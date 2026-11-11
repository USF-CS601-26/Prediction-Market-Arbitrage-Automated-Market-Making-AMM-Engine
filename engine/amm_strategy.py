"""
engine/amm_strategy.py

AMMStrategy — an inventory- and sentiment-aware market maker for one binary
market on one venue, in the Avellaneda-Stoikov style.

The quote
---------
    reservation   r = s - q * gamma * sigma^2 * (T - t)  +  k * S * confidence
    spread        delta = gamma * sigma^2 * (T - t)  +  (2 / gamma) * ln(1 + gamma / kappa)
    quotes        bid = r - delta/2,   ask = r + delta/2

    s           mid of the YES book
    q           net YES-equivalent inventory from the PortfolioManager, in
                units of `inventory_unit` contracts (YES held minus NO held),
                across every venue trading this event by default: the
                arbitrage strategy's Polymarket-YES + Kalshi-NO pair is hedged,
                and an AMM that skewed on one leg alone would lean to unwind it
    gamma       risk aversion: how hard inventory pushes the quotes
    sigma       realized volatility of the mid, per sqrt(day)
    T - t       days to expiry, capped at `horizon_cap_days`
    kappa       order-arrival decay per dollar of distance from the mid; the
                (2/gamma) ln(1 + gamma/kappa) term is the spread that
                maximizes expected fill profit when there is no inventory
    S           signed sentiment level from the shared sentiment feed
    confidence  how much that level is worth trusting (see below)
    k           dollars of shift at S * confidence = 1

Each term moves the quotes the way it should:
  - long inventory (q > 0) lowers r, so the bid gets less aggressive and the
    ask more aggressive: the AMM sells down what it holds;
  - positive sentiment raises r: the AMM pays more for YES ahead of a
    repricing instead of being picked off by it;
  - more volatility or longer to expiry widens the spread and strengthens
    the inventory push.

Units and the horizon cap
-------------------------
Prices are dollars in (0, 1), time is days. Taken literally over a market
that expires in months, sigma^2 * (T - t) dwarfs every price in the book and
the AMM would never quote inside 0.01..0.99. The standard practical reading
is that T - t is the horizon over which the market maker must be flat, not
the contract's maturity, so it is min(time to expiry, horizon_cap_days).
Near expiry it shrinks with the real time left and the inventory term fades;
inside `stop_before_expiry` the AMM cancels everything and stops quoting,
because a binary contract about to resolve is a jump to 0 or 1, not a
diffusion the formula can price.

Confidence
----------
The shared sentiment stream (engine/sentiment.py, contract `sentiment:{pair_id}`)
carries a signed `score` per headline and no confidence field. Confidence is
therefore derived from the rolling window the feed already keeps:

    confidence = agreement x evidence
    agreement  = 1 - stdev(scores in the window)        (scores are in [-1, 1])
    evidence   = min(1, n / min_points_for_confidence)

Headlines that agree, and enough of them, move the quotes; a window of
contradictory headlines averages to a small S and also scores low agreement,
so it barely skews at all — the arbitrage threshold is what widens on that.

From quote to orders
--------------------
A prediction market has no "sell what you do not hold" on every venue, so
each side is expressed as the order that venue can actually take:

    bid side   BUY YES at bid            (or SELL NO at 1 - bid if NO is held)
    ask side   SELL YES at ask if held,  otherwise BUY NO at 1 - ask

Buying NO at 1 - ask is the same economic trade as offering YES at ask.
Quotes are post-only: a bid that would cross the best ask is pulled back a
tick behind it (and vice versa), because a crossing quote is a taker order.

Every order goes through PortfolioManager.check_order. A side the risk layer
vetoes is simply not quoted; when both are vetoed the AMM is flat out of
room and quotes nothing. Quotes are cancelled and replaced only when the
reservation price has moved by `requote_threshold` or a side has gone
(filled, cancelled, rejected) — re-sending identical quotes on every book
update would just burn the venue's rate limit.
"""

from __future__ import annotations

import logging
import math
import statistics
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from order_book import OrderBook
from pydantic import BaseModel, Field

from engine.executors.base import OrderNotFound
from engine.executors.factory import OrderExecutorFactory
from engine.models import Order, OrderSide, OrderState, OrderStatus, next_order_id, utcnow
from engine.strategy import BaseStrategy, StrategyResult

log = logging.getLogger(__name__)

ZERO = Decimal("0")
ONE = Decimal("1")
DAY = timedelta(days=1)

# Returns the market's YES book, or None when it must not be quoted against
BookProvider = Callable[[], OrderBook | None]


@dataclass
class AMMConfig:
    gamma: float = 2.0                       # risk aversion
    kappa: float = 100.0                     # order-arrival decay per $ from the mid
    k_sentiment: float = 0.02                # $ shift at S * confidence = 1
    quote_size: Decimal = Decimal("10")
    # contracts per unit of q. With the floor sigma (0.02) and gamma 2, one
    # unit moves r by 0.08c, so 1000 contracts (the default position limit)
    # lean the quotes by 8c — strong enough to matter well before the limit
    inventory_unit: Decimal = Decimal("10")
    # whose inventory q is: "event" = net YES-equivalent across every venue
    # registered under this market (an arbitrage pair's two legs cancel), or
    # "venue" = this venue's position alone
    inventory_scope: str = "event"
    sigma_floor: float = 0.02                # per sqrt(day); a quiet book is not riskless
    sigma_cap: float = 0.50
    vol_window: timedelta = timedelta(hours=1)
    min_vol_span: timedelta = timedelta(minutes=1)
    horizon_cap_days: float = 1.0
    expiry: datetime | None = None
    stop_before_expiry: timedelta = timedelta(hours=1)
    requote_threshold: Decimal = Decimal("0.005")
    tick_size: Decimal = Decimal("0.01")
    min_price: Decimal = Decimal("0.01")
    max_price: Decimal = Decimal("0.99")
    post_only: bool = True
    min_points_for_confidence: int = 1


class AMMQuote(BaseModel):
    """One tick's pricing, whether or not it led to new orders."""
    strategy_id: str
    timestamp: datetime = Field(default_factory=utcnow)
    venue: str
    market: str
    mid: Decimal
    reservation: Decimal
    spread: Decimal
    bid: Decimal | None = None
    ask: Decimal | None = None
    inventory: Decimal = ZERO                # net YES-equivalent contracts
    sigma: Decimal = ZERO
    tau_days: Decimal = ZERO
    sentiment: Decimal = ZERO
    confidence: Decimal = ZERO
    action: str = "hold"                     # hold | requote | cancel | stop
    reason: str = ""
    placed: list[OrderState] = Field(default_factory=list)
    cancelled: list[str] = Field(default_factory=list)


# --------------------------------------------------------------------------
# pure pieces, testable without an event loop
# --------------------------------------------------------------------------

class MidVolatility:
    """
    Realized variance of the mid per day over a rolling time window:
    sum of squared mid changes divided by the elapsed time. A book that has
    not moved, or a window too short to say anything, reads as sigma_floor.
    """

    def __init__(self, window: timedelta, min_span: timedelta,
                 floor: float, cap: float):
        self.window, self.min_span = window, min_span
        self.floor, self.cap = floor, cap
        self._points: deque[tuple[datetime, float]] = deque()

    def add(self, at: datetime, mid: Decimal) -> None:
        self._points.append((at, float(mid)))
        cutoff = at - self.window
        while len(self._points) > 2 and self._points[0][0] < cutoff:
            self._points.popleft()

    def sigma(self) -> float:
        if len(self._points) < 2:
            return self.floor
        span = self._points[-1][0] - self._points[0][0]
        if span < self.min_span:
            return self.floor
        days = span / DAY
        mids = [m for _, m in self._points]
        variance = sum((b - a) ** 2 for a, b in zip(mids, mids[1:])) / days
        return min(max(math.sqrt(variance), self.floor), self.cap)


def sentiment_features(feed, pair_id: str, min_points: int = 1) -> tuple[Decimal, Decimal, int]:
    """(level S, confidence, points in window) from a RollingSentiment-style feed."""
    if feed is None:
        return ZERO, ZERO, 0
    recent = feed.recent(pair_id) if hasattr(feed, "recent") else []
    scores = [float(p.score) for p in recent]
    if not scores:
        return ZERO, ZERO, 0
    level = statistics.fmean(scores)
    agreement = max(0.0, 1.0 - statistics.pstdev(scores))
    evidence = min(1.0, len(scores) / max(min_points, 1))
    return _dec(level), _dec(agreement * evidence), len(scores)


def reservation_and_spread(mid: Decimal, q_units: float, sigma: float, tau_days: float,
                           sentiment: Decimal, confidence: Decimal,
                           cfg: AMMConfig) -> tuple[Decimal, Decimal]:
    """The two formulas, exactly as written in the module docstring."""
    risk = cfg.gamma * sigma ** 2 * tau_days
    r = float(mid) - q_units * risk + cfg.k_sentiment * float(sentiment) * float(confidence)
    delta = risk + (2.0 / cfg.gamma) * math.log(1.0 + cfg.gamma / cfg.kappa)
    return _dec(r), _dec(delta)


def quote_prices(reservation: Decimal, spread: Decimal, cfg: AMMConfig,
                 best_bid: Decimal | None = None, best_ask: Decimal | None = None
                 ) -> tuple[Decimal | None, Decimal | None]:
    """
    Tick-align (bid down, ask up — never quote tighter than the model asked
    for), clamp to [min_price, max_price], keep bid < ask, and, when
    post-only, never cross the opposite best. A side that cannot be placed
    inside the bounds is returned as None.
    """
    tick = cfg.tick_size
    half = spread / 2
    bid = _to_tick(reservation - half, tick, ROUND_FLOOR)
    ask = _to_tick(reservation + half, tick, ROUND_CEILING)

    if cfg.post_only:
        if best_ask is not None and bid >= best_ask:
            bid = best_ask - tick
        if best_bid is not None and ask <= best_bid:
            ask = best_bid + tick

    bid = max(bid, cfg.min_price)
    ask = min(ask, cfg.max_price)
    if bid >= ask:
        # squeezed against a bound: keep one tick between them, giving way on
        # whichever side is not pinned
        if ask >= cfg.max_price:
            bid = ask - tick
        else:
            ask = bid + tick

    bid_out = bid if cfg.min_price <= bid <= cfg.max_price - tick else None
    ask_out = ask if cfg.min_price + tick <= ask <= cfg.max_price else None
    if bid_out is not None and best_ask is not None and cfg.post_only and bid_out >= best_ask:
        bid_out = None
    if ask_out is not None and best_bid is not None and cfg.post_only and ask_out <= best_bid:
        ask_out = None
    return bid_out, ask_out


def _to_tick(price: Decimal, tick: Decimal, rounding: str) -> Decimal:
    return (price / tick).to_integral_value(rounding=rounding) * tick


def _dec(x: float) -> Decimal:
    return Decimal(repr(round(x, 10)))


# --------------------------------------------------------------------------
# the strategy
# --------------------------------------------------------------------------

class AMMStrategy(BaseStrategy):
    """
    Args:
        pair_id:        sentiment key (the stream is sentiment:{pair_id}).
        venue:          the venue quoted on.
        market:         the logical market name the PortfolioManager knows
                        this venue's YES/NO ids by (register_market).
        yes_market_id / no_market_id:
                        what an order carries for each outcome — the same
                        Kalshi ticker twice, or Polymarket's two token ids.
        book_provider:  zero-argument callable returning the YES book, or
                        None while it is stale.
        factory:        OrderExecutorFactory, routed by venue.
        portfolio:      the PortfolioManager (needs check_order and
                        get_inventory; position() and release() are used
                        when present).
        sentiment:      the shared sentiment feed, or None.
    """

    def __init__(self, pair_id: str, venue: str, market: str,
                 yes_market_id: str, no_market_id: str,
                 book_provider: BookProvider,
                 factory: OrderExecutorFactory, portfolio,
                 sentiment=None, config: AMMConfig | None = None,
                 strategy_id: str | None = None, enabled: bool = True):
        super().__init__(strategy_id or f"amm:{venue}:{pair_id}", enabled=enabled)
        self.pair_id = pair_id
        self.venue = venue
        self.market = market
        self.yes_market_id = yes_market_id
        self.no_market_id = no_market_id
        self.book_provider = book_provider
        self.factory = factory
        self.portfolio = portfolio
        self.sentiment = sentiment
        self.config = config or AMMConfig()
        self.vol = MidVolatility(self.config.vol_window, self.config.min_vol_span,
                                 self.config.sigma_floor, self.config.sigma_cap)

        self.live: dict[str, OrderState] = {}       # "bid" / "ask" -> resting order
        self.live_reservation: Decimal | None = None
        self.last_quote: AMMQuote | None = None
        self.quotes: list[AMMQuote] = []             # every requote, for reports and tests
        self.requotes = 0
        self.cancels = 0
        self.risk_vetoes = 0
        self.stopped_reason: str | None = None

    # ---- the tick ----

    async def on_tick(self, now: datetime | None = None) -> StrategyResult:
        now = now or utcnow()
        cfg = self.config

        book = self.book_provider()
        if book is None:
            return await self._stand_down("book unavailable", now)
        best_bid, best_ask = book.get_top_of_book()
        if best_bid is None or best_ask is None:
            return await self._stand_down("one-sided book", now)
        mid = (best_bid.price + best_ask.price) / 2
        self.vol.add(now, mid)

        await self._refresh_live()

        tau_days = cfg.horizon_cap_days
        if cfg.expiry is not None:
            left = cfg.expiry - now
            if left <= cfg.stop_before_expiry:
                return await self._stand_down(
                    f"within {cfg.stop_before_expiry} of expiry; not quoting", now, stop=True)
            tau_days = min(left / DAY, cfg.horizon_cap_days)

        scope_venue = None if cfg.inventory_scope == "event" else self.venue
        inventory = self.portfolio.get_inventory(self.market, venue=scope_venue).net
        q_units = float(inventory / cfg.inventory_unit)
        level, confidence, _ = sentiment_features(self.sentiment, self.pair_id,
                                                  cfg.min_points_for_confidence)
        sigma = self.vol.sigma()
        r, delta = reservation_and_spread(mid, q_units, sigma, tau_days, level, confidence, cfg)
        bid, ask = quote_prices(r, delta, cfg, best_bid.price, best_ask.price)

        quote = AMMQuote(strategy_id=self.strategy_id, timestamp=now, venue=self.venue,
                         market=self.market, mid=mid, reservation=r, spread=delta,
                         bid=bid, ask=ask, inventory=inventory, sigma=_dec(sigma),
                         tau_days=_dec(tau_days), sentiment=level, confidence=confidence)
        self.stopped_reason = None

        if not self._needs_requote(quote):
            quote.reason = "quotes unchanged"
            self.last_quote = quote
            return StrategyResult(strategy_id=self.strategy_id, timestamp=now,
                                  acted=False, reason=quote.reason, detail=quote)

        quote.cancelled = await self.cancel_all()
        quote.action = "requote"
        vetoes: list[str] = []
        for side, price in (("bid", bid), ("ask", ask)):
            if price is None:
                continue
            state, why = await self._place(side, price, inventory)
            if state is not None:
                quote.placed.append(state)
            elif why:
                vetoes.append(f"{side}: {why}")

        self.live_reservation = r
        self.requotes += 1
        if quote.placed:
            quote.reason = (f"quoting {bid}/{ask} around r={r:.4f}"
                            + (f" ({'; '.join(vetoes)})" if vetoes else ""))
        else:
            quote.reason = "not quoting: " + ("; ".join(vetoes) or "no valid price inside bounds")
            quote.action = "stop" if vetoes else quote.action
        self.last_quote = quote
        self.quotes.append(quote)
        return StrategyResult(strategy_id=self.strategy_id, timestamp=now,
                              acted=bool(quote.placed or quote.cancelled),
                              reason=quote.reason, detail=quote)

    def _needs_requote(self, quote: AMMQuote) -> bool:
        if self.live_reservation is None:
            return True
        if abs(quote.reservation - self.live_reservation) >= self.config.requote_threshold:
            return True
        # a side the model wants but which is no longer resting (filled,
        # cancelled by the venue, rejected) has to be put back
        wanted = {s for s, p in (("bid", quote.bid), ("ask", quote.ask)) if p is not None}
        return not wanted.issubset(self.live)

    # ---- orders ----

    def _order_for(self, side: str, price: Decimal, size: Decimal, inventory: Decimal) -> Order:
        """Express one side of the quote as an order the venue can take."""
        held_yes = self._held(self.yes_market_id, "YES")
        held_no = self._held(self.no_market_id, "NO")
        if side == "bid":
            if held_no >= size:
                outcome, order_side, market_id, limit = "NO", OrderSide.SELL, self.no_market_id, ONE - price
            else:
                outcome, order_side, market_id, limit = "YES", OrderSide.BUY, self.yes_market_id, price
        else:
            if held_yes >= size:
                outcome, order_side, market_id, limit = "YES", OrderSide.SELL, self.yes_market_id, price
            else:
                outcome, order_side, market_id, limit = "NO", OrderSide.BUY, self.no_market_id, ONE - price
        return Order(order_id=next_order_id(f"amm-{side}"), venue=self.venue,
                     market_id=market_id, outcome=outcome, side=order_side,
                     size=size, limit_price=limit, pair_id=self.pair_id,
                     strategy_id=self.strategy_id)

    async def _place(self, side: str, price: Decimal, inventory: Decimal
                     ) -> tuple[OrderState | None, str]:
        order = self._order_for(side, price, self.config.quote_size, inventory)
        decision = await self.check_risk(self.portfolio, order)
        if not decision.approved:
            self.risk_vetoes += 1
            return None, f"risk: {decision.reason}"
        if decision.max_size is not None and decision.max_size < order.size:
            order.size = self._quantize(decision.max_size)
            if order.size <= ZERO:
                self._release(order.order_id)
                self.risk_vetoes += 1
                return None, "risk: no room for one increment"

        executor = self.factory.for_order(order)
        reason = executor.preflight(order)
        if reason is not None:
            self._release(order.order_id)
            return None, f"venue would reject: {reason}"

        state = await executor.submit_order(order)
        if state.status == OrderStatus.REJECTED:
            self._release(order.order_id)
            return None, f"rejected: {state.reject_reason}"
        if not state.status.is_terminal:
            self.live[side] = state
        return state, ""

    async def cancel_all(self) -> list[str]:
        """Pull every resting quote; returns the ids actually cancelled."""
        cancelled = []
        for side, state in list(self.live.items()):
            if not state.status.is_terminal:
                try:
                    if await self.factory.for_order(state.order).cancel_order(state.order.order_id):
                        cancelled.append(state.order.order_id)
                        self.cancels += 1
                except OrderNotFound:
                    log.warning("cancel for unknown order %s", state.order.order_id)
            self._release(state.order.order_id)
            del self.live[side]
        self.live_reservation = None
        return cancelled

    async def _refresh_live(self) -> None:
        """Forget sides whose order is finished (filled, or pulled by the venue)."""
        for side, state in list(self.live.items()):
            try:
                current = await self.factory.for_order(state.order).get_status(state.order.order_id)
            except OrderNotFound:
                current = None
            if current is None or current.status.is_terminal:
                self._release(state.order.order_id)
                del self.live[side]

    async def _stand_down(self, reason: str, now: datetime, stop: bool = False) -> StrategyResult:
        cancelled = await self.cancel_all() if self.live else []
        if stop:
            self.stopped_reason = reason
        detail = None
        if cancelled or stop:
            mid = self.last_quote.mid if self.last_quote else ZERO
            detail = AMMQuote(strategy_id=self.strategy_id, timestamp=now, venue=self.venue,
                              market=self.market, mid=mid, reservation=mid, spread=ZERO,
                              action="stop" if stop else "cancel", reason=reason,
                              cancelled=cancelled)
            self.quotes.append(detail)
        return StrategyResult(strategy_id=self.strategy_id, timestamp=now,
                              acted=bool(cancelled), reason=reason, detail=detail)

    # ---- helpers ----

    def _held(self, market_id: str, outcome: str) -> Decimal:
        if hasattr(self.portfolio, "position"):
            return self.portfolio.position(self.venue, market_id, outcome)
        return ZERO

    def _release(self, order_id: str) -> None:
        if hasattr(self.portfolio, "release"):
            self.portfolio.release(order_id)

    def _quantize(self, size: Decimal) -> Decimal:
        executor = self.factory.for_venue(self.venue)
        inc = getattr(getattr(executor, "config", None), "size_increment", ONE)
        if inc <= ZERO:
            return size
        return (size / inc).to_integral_value(rounding=ROUND_FLOOR) * inc

    def summary(self) -> str:
        q = self.last_quote
        tail = (f", last {q.bid}/{q.ask} around r={q.reservation:.4f} "
                f"(mid {q.mid}, inventory {q.inventory})") if q and q.action != "stop" else ""
        return (f"{self.strategy_id}: {self.ticks} ticks, {self.requotes} requotes, "
                f"{self.cancels} cancels, {self.risk_vetoes} risk vetoes{tail}"
                + (f", STOPPED: {self.stopped_reason}" if self.stopped_reason else ""))
