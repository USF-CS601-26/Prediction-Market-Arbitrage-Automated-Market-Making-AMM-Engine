"""
engine/executors/simulator.py

SimulatedExecutor — fills orders against the real, live order book.

This is a mock venue, not a toy. It fills by walking the same OrderBook the
ingestion engines maintain, honouring the limit price level by level and
charging the venue's real fee function per fill. What it adds on top of
executable_spread.py's "what would this cost" is everything that makes
execution different from pricing:

  latency        submit_order returns after a venue round trip, during which
                 the book has moved. A paired arbitrage that assumes
                 instantaneous fills never sees its own worst case.
  partial fills  the normal outcome, from two distinct causes that are kept
                 distinct: genuine depth/limit exhaustion (deterministic,
                 read off the book) and the venue filling only part of a
                 marketable order before the rest rests (stochastic).
  rejections     deterministic ones (bad size, bad price, off-tick, stale
                 book) and a configurable random rate for everything a venue
                 rejects for reasons we can't see.

Two deliberate modelling choices:

1. An unfilled remainder RESTS. A limit order that only partly fills is
   left OPEN or PARTIALLY_FILLED — not cancelled. That is what a venue does,
   and it is the whole reason ArbitrageStrategy has to cancel and unwind:
   if the mock silently cancelled remainders, the leg-mismatch code would
   never be exercised and the bug would be found in production instead.

2. By default a fill does NOT remove the depth it consumed
   (consume_liquidity=False). The feed owns the book and is its only
   mutator; writing our fills into it would corrupt the sequence/consistency
   invariants the ingestion engines check against the exchange (notably
   PolymarketFeed's check_top_mismatch, which compares our top of book with
   the exchange's own hint). The cost of that choice is that repeated orders
   against a frozen book can each fill at the same level; the pipeline
   evaluates at most one opportunity per book change, which bounds it.
   Tests that want depletion set consume_liquidity=True.

Randomness comes from a seeded random.Random, so an entire simulated session
replays identically — a prerequisite for using any of this in a test.
"""

from __future__ import annotations

import asyncio
import logging
from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import ROUND_FLOOR, Decimal
from random import Random

from order_book import OrderBook

from engine.books import ask_ladder, bid_ladder
from engine.executors.base import BaseExecutor, OrderNotFound
from engine.models import Fill, Order, OrderSide, OrderState, OrderStatus, utcnow

log = logging.getLogger(__name__)

ZERO = Decimal("0")

# (contracts, price) -> fee; the same FeeFn signature executable_spread uses
FeeFn = Callable[[Decimal, Decimal], Decimal]


def _no_fee(contracts: Decimal, price: Decimal) -> Decimal:
    return ZERO


@dataclass
class SimulationConfig:
    """
    How this mock venue misbehaves. Defaults are the benign case — no
    latency, no randomness — so a test only opts into the behaviour it is
    actually testing.
    """
    latency_ms: float = 0.0             # mean venue round trip for a submit
    latency_jitter_ms: float = 0.0      # uniform +/- jitter around the mean
    cancel_latency_ms: float = 0.0
    reject_prob: float = 0.0            # chance the venue rejects outright
    partial_fill_prob: float = 0.0      # chance a marketable order only partly fills
    partial_fill_min_fraction: float = 0.25   # floor on how little of it fills
    min_order_size: Decimal = Decimal("1")
    size_increment: Decimal = Decimal("1")    # 1 contract on Kalshi, 0.01 share on Polymarket
    tick_size: Decimal | None = Decimal("0.01")   # None disables the off-tick check
    consume_liquidity: bool = False     # see the module docstring
    seed: int | None = 0                # None = nondeterministic; anything else is repeatable
    stats: dict = field(default_factory=dict)


class SimulatedExecutor(BaseExecutor):
    """
    Venue-neutral fill simulation. Subclasses supply the venue name, the fee
    function, and how to find the book for an order's (market_id, outcome).
    """

    def __init__(self, venue: str, fee_fn: FeeFn = _no_fee,
                 config: SimulationConfig | None = None):
        super().__init__(venue=venue)
        self.fee_fn = fee_fn
        self.config = config or SimulationConfig()
        self._rng = Random(self.config.seed)
        self._orders: dict[str, OrderState] = {}
        # every latency actually waited, in ms, so a test can assert the
        # distribution without having to time the event loop
        self.latencies_ms: list[float] = []
        self.submitted = 0
        self.rejected = 0
        self.cancelled = 0

    # ---- what a subclass must provide ----

    @abstractmethod
    def book_for(self, order: Order) -> OrderBook | None:
        """
        The book this order trades against, already expressed as the outcome
        the order names (see engine/books.complement_book). None means we
        have no usable book, which is a rejection, not an empty fill.
        """

    def book_unavailable_reason(self, order: Order) -> str:
        """
        Why book_for returned None. Overridden per venue so a rejection says
        "book is stale after a disconnect" rather than a generic failure —
        the distinction between a stale book and an empty one is the
        difference between waiting and giving up.
        """
        return "no tradable order book for this market"

    def preflight(self, order: Order) -> str | None:
        """Both the venue-independent and the venue-specific static rules."""
        return self.validate(order) or self.venue_validate(order)

    def venue_validate(self, order: Order) -> str | None:
        """Venue-specific rules on top of BaseExecutor.validate."""
        cfg = self.config
        if order.size < cfg.min_order_size:
            return f"size {order.size} below {self.venue} minimum {cfg.min_order_size}"
        if cfg.tick_size is not None and order.limit_price % cfg.tick_size != 0:
            return f"limit price {order.limit_price} is not a multiple of tick {cfg.tick_size}"
        return None

    # ---- the BaseExecutor interface ----

    async def submit_order(self, order: Order) -> OrderState:
        state = OrderState(order=order, status=OrderStatus.PENDING)
        self._orders[order.order_id] = state
        self.submitted += 1

        # the venue round trip happens BEFORE anything is decided, so the
        # book this order fills against is the one that exists on arrival,
        # not the one the strategy looked at
        await self._wait(self.config.latency_ms, self.config.latency_jitter_ms)

        reason = self.validate(order) or self.venue_validate(order)
        if reason is None and self._rng.random() < self.config.reject_prob:
            reason = "simulated venue rejection"
        if reason is None and self.book_for(order) is None:
            reason = self.book_unavailable_reason(order)
        if reason is not None:
            return self._reject(state, reason)

        await self._fill(state)
        return state

    async def cancel_order(self, order_id: str) -> bool:
        state = self._orders.get(order_id)
        if state is None:
            raise OrderNotFound(order_id)
        await self._wait(self.config.cancel_latency_ms, 0.0)
        if state.status.is_terminal:
            # filled (or already pulled) while the cancel was in flight
            return False
        self._set_status(state, OrderStatus.CANCELLED)
        self.cancelled += 1
        return True

    async def get_status(self, order_id: str) -> OrderState:
        state = self._orders.get(order_id)
        if state is None:
            raise OrderNotFound(order_id)
        return state

    # ---- fill simulation ----

    async def _fill(self, state: OrderState) -> None:
        order = state.order
        book = self.book_for(order)
        assert book is not None      # checked in submit_order

        if order.side == OrderSide.BUY:
            ladder = ask_ladder(book, order.limit_price)
            side_map = book.asks
        else:
            ladder = bid_ladder(book, order.limit_price)
            side_map = book.bids

        # the REMAINING size, so a resting order re-matched by match_resting()
        # can never fill past its original size
        target = self._fill_target(state.remaining)
        filled = ZERO
        for price, available in ladder:
            if filled >= target:
                break
            take = self._quantize(min(available, target - filled))
            if take <= ZERO:
                continue
            fill = Fill(
                order_id=order.order_id, venue=self.venue, market_id=order.market_id,
                outcome=order.outcome, side=order.side, size=take, price=price,
                fee=self.fee_fn(take, price), timestamp=utcnow(),
                pair_id=order.pair_id, strategy_id=order.strategy_id,
            )
            state.fills.append(fill)
            state.filled_size += take
            state.fees += fill.fee
            filled += take
            if self.config.consume_liquidity:
                self._consume(side_map, price, take)
            await self._emit_fill(fill)

        if state.filled_size >= order.size:
            self._set_status(state, OrderStatus.FILLED)
        elif state.filled_size > ZERO:
            # remainder rests at the venue; the caller must cancel it
            self._set_status(state, OrderStatus.PARTIALLY_FILLED)
        else:
            self._set_status(state, OrderStatus.OPEN)

    def _fill_target(self, size: Decimal) -> Decimal:
        """
        How much of `size` the venue is willing to fill right now, before
        depth is consulted. Usually all of it; with probability
        partial_fill_prob the venue fills a random fraction and rests the
        rest, which is the stochastic half of partial fills.
        """
        if self.config.partial_fill_prob <= 0:
            return size
        if self._rng.random() >= self.config.partial_fill_prob:
            return size
        lo = self.config.partial_fill_min_fraction
        fraction = Decimal(str(self._rng.uniform(lo, 1.0)))
        capped = self._quantize(size * fraction)
        # a "partial" fill of zero is really a rejection; keep at least one increment
        return max(capped, self.config.size_increment)

    def _quantize(self, size: Decimal) -> Decimal:
        """Round DOWN to the venue's size increment: never fill more than exists."""
        inc = self.config.size_increment
        if inc <= ZERO:
            return size
        return (size / inc).to_integral_value(rounding=ROUND_FLOOR) * inc

    @staticmethod
    def _consume(side_map, price: Decimal, take: Decimal) -> None:
        remaining = side_map.get(price, ZERO) - take
        if remaining > ZERO:
            side_map[price] = remaining
        else:
            side_map.pop(price, None)

    # ---- plumbing ----

    def _reject(self, state: OrderState, reason: str) -> OrderState:
        state.reject_reason = reason
        self._set_status(state, OrderStatus.REJECTED)
        self.rejected += 1
        log.info("%s rejected %s: %s", self.venue, state.order.order_id, reason)
        return state

    @staticmethod
    def _set_status(state: OrderState, status: OrderStatus) -> None:
        state.status = status
        state.updated_at = utcnow()

    async def _wait(self, mean_ms: float, jitter_ms: float) -> None:
        if mean_ms <= 0 and jitter_ms <= 0:
            return
        ms = mean_ms
        if jitter_ms > 0:
            ms += self._rng.uniform(-jitter_ms, jitter_ms)
        ms = max(0.0, ms)
        self.latencies_ms.append(ms)
        await asyncio.sleep(ms / 1000.0)

    def open_orders(self) -> list[OrderState]:
        """Every order still live at this venue. Used by the unwind path."""
        return [s for s in self._orders.values() if not s.status.is_terminal]

    async def match_resting(self) -> int:
        """
        Re-match every resting order against the CURRENT book; returns how
        many orders received a fill.

        Without this a resting limit order could only ever fill at the moment
        it was submitted, which is fine for the arbitrage legs (whatever does
        not fill immediately is cancelled) but makes a market maker's quotes —
        which are resting orders by design — fill never. The model is the
        usual "trade-through" one for L2 data: a resting BUY at p fills once
        the book offers at or below p, a resting SELL once it bids at or above
        p, honouring the limit price level by level exactly as submit_order
        does. The pipeline calls this on every book change, before the
        strategies tick, the way a venue's matching engine runs before anyone
        reacts to the new prices.
        """
        touched = 0
        for state in self.open_orders():
            if state.status == OrderStatus.PENDING:
                continue                     # still in flight; submit_order owns it
            if self.book_for(state.order) is None:
                continue
            before = state.filled_size
            await self._fill(state)
            if state.filled_size > before:
                touched += 1
        return touched
