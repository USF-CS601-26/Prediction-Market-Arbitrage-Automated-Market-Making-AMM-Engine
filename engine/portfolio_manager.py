"""
engine/portfolio_manager.py

PortfolioManager — the one book of positions every strategy and every venue
shares, and the risk gate every order passes through.

It implements the RiskManager contract in engine/portfolio.py
(check_order / record_fill) and adds get_inventory: YES/NO inventory, average
cost and P&L per market. SimplePortfolioManager in that file stays as the reference
implementation the arbitrage unit tests were written against; the pipeline
runs on this one.

Why a singleton
---------------
Position state is a process-wide fact. If two strategies (anything registered
through pipeline.add_strategy) each held their own PortfolioManager, each would see room under the limits
that the other had already used, and together they would over-allocate.
`PortfolioManager()` therefore always returns the same instance; the
creation is lock-protected (double-checked in __new__) so two threads racing
to construct it still get one object. reset() re-initializes that instance,
which is what a pipeline does at start-up and what a test does in setup.

Thread safety
-------------
Every public method takes the instance's RLock. The pipeline itself runs on
one asyncio loop, but fill listeners and a future real venue client may call
record_fill from a callback thread, and check_order must see a consistent
snapshot of positions and reservations while it decides.

Reservations: how over-allocation is blocked
--------------------------------------------
check_order does not just compare the order against today's positions. An
approved BUY reserves the capacity it could consume (its size at its limit
price) until it fills, is released, or is found to be no longer open by
reconcile_reservations(). The next check_order — from any strategy, for any
venue — sees that reservation as if it had already filled. That is what
stops one strategy trading into room another is about to use, and what stops
two venues each being approved against the same total limit.

Exposure is worst-case loss, not notional
-----------------------------------------
For one binary market on one venue, holding Y YES and N NO contracts that
cost C in total pays Y if YES resolves and N if NO resolves, so the most it
can lose is

    exposure = max(0, C - min(Y, N))

A plain long position's exposure is its cost; buying the other outcome
against it REDUCES exposure (a YES+NO pair is a certain $1), so a hedging
order is recognized as one. Short positions are converted to their long equivalent
first (short q YES at p == long q NO at 1 - p). Positions on DIFFERENT venues
are not netted against each other: a Kalshi NO and a Polymarket YES on the
same event settle on different rails at different times, so the conservative
view counts both legs. That overstates the risk of a hedged arbitrage, which
is the safe direction to be wrong in.

Limits (RiskLimits)
-------------------
    max_position_per_market   |net YES-equivalent contracts| per venue market
    max_notional_per_order     size x limit price, per BUY order
    max_exposure_per_market    worst-case $ loss per venue market
    max_exposure_per_venue     summed over that venue's markets
    max_total_exposure         summed over every venue
    cash                       one shared pool, or a balance per venue

A breached limit shrinks the order to the largest size that fits
(RiskDecision.max_size) or vetoes it if nothing fits. An order that REDUCES
both the net position and the exposure is approved past the limits (only
cash is checked), because blocking it would strand exactly the risk the
limits exist to prevent; unwind orders are always approved, as in
SimplePortfolioManager.
"""

from __future__ import annotations

import copy
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Mapping

from pydantic import BaseModel

from engine.models import Fill, Order, OrderSide, RiskDecision, utcnow

log = logging.getLogger(__name__)

ZERO = Decimal("0")
ONE = Decimal("1")
QUANTUM = Decimal("0.01")      # finest size the risk layer resolves; strategies quantize further

PositionKey = tuple[str, str, str]     # (venue, market_id, outcome)
MarketKey = tuple[str, str]            # (venue, logical market)
SHARED = "*"                           # cash-pool key when cash is not split per venue


@dataclass(frozen=True)
class RiskLimits:
    """Defaults are sized for the mock venues and a $10,000 account."""
    max_position_per_market: Decimal = Decimal("1000")
    max_notional_per_order: Decimal = Decimal("1000")
    max_exposure_per_market: Decimal = Decimal("2500")
    max_exposure_per_venue: Decimal = Decimal("5000")
    max_total_exposure: Decimal = Decimal("8000")
    allow_unwind_over_limit: bool = True
    allow_risk_reducing: bool = True


# --------------------------------------------------------------------------
# positions
# --------------------------------------------------------------------------

@dataclass
class Position:
    """
    One (venue, market_id, outcome) holding with average-cost accounting.

    qty is signed (negative = short). avg_cost is the average entry price of
    the OPEN quantity, fees excluded; fees are charged to realized P&L when
    paid, because they are gone whether or not the position is ever closed.
    """
    venue: str
    market_id: str
    outcome: str
    qty: Decimal = ZERO
    avg_cost: Decimal = ZERO
    realized_pnl: Decimal = ZERO
    fees: Decimal = ZERO
    mark: Decimal | None = None

    def apply(self, side: OrderSide, size: Decimal, price: Decimal, fee: Decimal = ZERO) -> None:
        delta = size if side == OrderSide.BUY else -size
        if size == ZERO:
            pass
        elif self.qty == ZERO or (self.qty > ZERO) == (delta > ZERO):
            # opening or adding: the average moves toward the new price
            total = abs(self.qty) + size
            self.avg_cost = (abs(self.qty) * self.avg_cost + size * price) / total
            self.qty += delta
        else:
            # reducing: realize on the part that closes; any excess opens the
            # other way at this fill's price
            closing = min(size, abs(self.qty))
            direction = ONE if self.qty > ZERO else -ONE
            self.realized_pnl += closing * (price - self.avg_cost) * direction
            self.qty += delta
            if self.qty == ZERO:
                self.avg_cost = ZERO
            elif size > closing:
                self.avg_cost = price
        self.realized_pnl -= fee
        self.fees += fee

    @property
    def unrealized_pnl(self) -> Decimal:
        if self.mark is None or self.qty == ZERO:
            return ZERO
        return self.qty * (self.mark - self.avg_cost)


class Inventory(BaseModel):
    """What get_inventory returns: one logical market, on one venue or all of them."""
    market: str
    venue: str | None = None
    yes: Decimal = ZERO
    no: Decimal = ZERO
    yes_avg_cost: Decimal | None = None
    no_avg_cost: Decimal | None = None
    realized_pnl: Decimal = ZERO
    unrealized_pnl: Decimal = ZERO
    exposure: Decimal = ZERO

    @property
    def net(self) -> Decimal:
        """YES-equivalent position: one NO contract is minus one YES."""
        return self.yes - self.no


@dataclass
class Reservation:
    """Capacity an approved, not-yet-filled order may still consume."""
    order_id: str
    venue: str
    market_id: str
    outcome: str
    side: OrderSide
    size: Decimal
    price: Decimal
    strategy_id: str | None = None
    created_at: datetime = field(default_factory=utcnow)


# --------------------------------------------------------------------------
# the manager
# --------------------------------------------------------------------------

class PortfolioManager:
    """
    Usage:
        pm = PortfolioManager()                      # the process-wide instance
        pm.reset(RiskLimits(...), cash=Decimal("10000"))
        pm.register_market("polymarket", "netanyahu-out-2026",
                           {"YES": yes_token, "NO": no_token})
        executor.add_fill_listener(pm.record_fill)

        decision = pm.check_order(order)
        inv = pm.get_inventory("netanyahu-out-2026", venue="polymarket")
    """

    _instance: "PortfolioManager | None" = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:          # double-checked: another thread may have won
                    instance = super().__new__(cls)
                    instance._ready = False
                    cls._instance = instance
        return cls._instance

    def __init__(self, limits: RiskLimits | None = None,
                 cash: Decimal | Mapping[str, Decimal] | None = None):
        with type(self)._instance_lock:
            if self._ready:
                if limits is not None or cash is not None:
                    log.warning("PortfolioManager already exists; constructor arguments "
                                "ignored — call reset() to reconfigure it")
                return
            self._lock = threading.RLock()
            self._configure(limits, cash)
            self._ready = True

    @classmethod
    def instance(cls) -> "PortfolioManager":
        """The module-level accessor; equivalent to PortfolioManager()."""
        return cls()

    @classmethod
    def _discard_instance(cls) -> None:
        """Tests only: forget the singleton so the next construction is fresh."""
        with cls._instance_lock:
            cls._instance = None

    def reset(self, limits: RiskLimits | None = None,
              cash: Decimal | Mapping[str, Decimal] | None = None) -> "PortfolioManager":
        """Drop every position, fill and reservation and apply new limits."""
        with self._lock:
            self._configure(limits, cash)
        return self

    def _configure(self, limits, cash) -> None:
        self.limits = limits or RiskLimits()
        if cash is None:
            cash = Decimal("10000")
        if isinstance(cash, Mapping):
            self._cash = {venue: Decimal(str(v)) for venue, v in cash.items()}
        else:
            self._cash = {SHARED: Decimal(str(cash))}
        self._starting_cash = dict(self._cash)
        self._positions: dict[PositionKey, Position] = {}
        self._markets: dict[tuple[str, str], str] = {}        # (venue, market_id) -> logical market
        self._reservations: dict[str, Reservation] = {}
        self.fills: list[Fill] = []
        self.rejections: list[tuple[Order, str]] = []
        self.fees_paid = ZERO

    # ---- market registry ----

    def register_market(self, venue: str, market: str, outcome_ids: Mapping[str, str]) -> None:
        """
        Name the logical market a venue's ids belong to. Polymarket trades
        YES and NO as two token ids; registering both under one market is
        what lets get_inventory and the per-market limits see them together.
        Unregistered ids are their own market.
        """
        with self._lock:
            for market_id in outcome_ids.values():
                self._markets[(venue, market_id)] = market

    def market_of(self, venue: str, market_id: str) -> str:
        return self._markets.get((venue, market_id), market_id)

    # ---- fills ----

    def record_fill(self, fill: Fill) -> None:
        """Wire this as executor.add_fill_listener(pm.record_fill)."""
        with self._lock:
            position = self._position(fill.venue, fill.market_id, fill.outcome)
            position.apply(fill.side, fill.size, fill.price, fill.fee)
            self._cash[self._pool(fill.venue)] += fill.cash_flow
            self.fees_paid += fill.fee
            self.fills.append(fill)

            reservation = self._reservations.get(fill.order_id)
            if reservation is not None:
                reservation.size -= fill.size
                if reservation.size <= ZERO:
                    del self._reservations[fill.order_id]

    def _position(self, venue: str, market_id: str, outcome: str) -> Position:
        key = (venue, market_id, outcome)
        if key not in self._positions:
            self._positions[key] = Position(venue, market_id, outcome)
        return self._positions[key]

    # ---- reservations ----

    def release(self, order_id: str) -> None:
        """An order is finished (cancelled, rejected, done): free what it reserved."""
        with self._lock:
            self._reservations.pop(order_id, None)

    def reconcile_reservations(self, open_order_ids) -> int:
        """
        Drop every reservation whose order is no longer open at any venue.
        The pipeline calls this after each tick with the executors' open
        orders, which is what releases capacity reserved by orders that were
        cancelled, rejected, or never sent (a pair aborted after one leg's
        check). Returns how many were released.
        """
        open_ids = set(open_order_ids)
        with self._lock:
            stale = [oid for oid in self._reservations if oid not in open_ids]
            for oid in stale:
                del self._reservations[oid]
            return len(stale)

    @property
    def reservations(self) -> dict[str, Reservation]:
        with self._lock:
            return dict(self._reservations)

    # ---- marks ----

    def set_mark(self, venue: str, market_id: str, outcome: str, price: Decimal | None) -> None:
        """The price unrealized P&L is measured at, normally the book's mid."""
        with self._lock:
            key = (venue, market_id, outcome)
            if key in self._positions:
                self._positions[key].mark = price
            elif price is not None:
                self._position(venue, market_id, outcome).mark = price

    # ---- reads ----

    def position(self, venue: str, market_id: str, outcome: str) -> Decimal:
        with self._lock:
            p = self._positions.get((venue, market_id, outcome))
            return p.qty if p else ZERO

    def get_inventory(self, market: str, venue: str | None = None) -> Inventory:
        """
        YES/NO inventory, average cost, realized and unrealized P&L and
        exposure for one logical market — on one venue, or aggregated over
        every venue when `venue` is None. `market` may be the registered
        logical name or any id registered under it.
        """
        with self._lock:
            # accept a raw id as well as the logical name
            if venue is not None:
                market = self.market_of(venue, market)
            else:
                market = next((m for (_v, mid), m in self._markets.items() if mid == market), market)

            yes = no = realized = unrealized = exposure = ZERO
            yes_cost = no_cost = ZERO
            venues = set()
            for p in self._positions.values():
                if self.market_of(p.venue, p.market_id) != market:
                    continue
                if venue is not None and p.venue != venue:
                    continue
                venues.add(p.venue)
                realized += p.realized_pnl
                unrealized += p.unrealized_pnl
                if p.outcome == "YES":
                    yes += p.qty
                    yes_cost += p.qty * p.avg_cost
                else:
                    no += p.qty
                    no_cost += p.qty * p.avg_cost
            for v in venues:
                exposure += self._exposure(self._projected(v, market))
            return Inventory(
                market=market, venue=venue, yes=yes, no=no,
                yes_avg_cost=(yes_cost / yes) if yes else None,
                no_avg_cost=(no_cost / no) if no else None,
                realized_pnl=realized, unrealized_pnl=unrealized, exposure=exposure,
            )

    @property
    def cash(self) -> Decimal:
        with self._lock:
            return sum(self._cash.values(), ZERO)

    def cash_for(self, venue: str) -> Decimal:
        with self._lock:
            return self._cash[self._pool(venue)]

    def realized_pnl(self) -> Decimal:
        with self._lock:
            return sum((p.realized_pnl for p in self._positions.values()), ZERO)

    def unrealized_pnl(self) -> Decimal:
        with self._lock:
            return sum((p.unrealized_pnl for p in self._positions.values()), ZERO)

    def exposure(self, venue: str | None = None, market: str | None = None) -> Decimal:
        """
        Worst-case loss across the matching venue markets, counting pending
        orders on whichever side of each market would hurt more.
        """
        with self._lock:
            return sum((self._worst_exposure(v, m)
                        for v, m in self._venue_markets()
                        if (venue is None or v == venue) and (market is None or m == market)),
                       ZERO)

    def gross_exposure(self) -> Decimal:
        """Kept for SimplePortfolioManager compatibility: total exposure."""
        return self.exposure()

    # ---- the risk check ----

    def check_order(self, order: Order) -> RiskDecision:
        """
        Approve, shrink, or veto one order, and reserve what is approved.
        """
        with self._lock:
            if order.size <= ZERO:
                return self._veto(order, "size must be positive")
            if order.is_unwind and self.limits.allow_unwind_over_limit:
                return RiskDecision.ok()

            if self.limits.allow_risk_reducing and self._reduces_risk(order):
                # past the position and exposure limits, but never past the
                # per-order notional cap or the cash actually available
                if self._order_problem(order, order.size) is None:
                    self._reserve(order, order.size)
                    return RiskDecision.ok()
                # a hedge we cannot pay for in full: fall through and size it

            if self._violation(order, order.size) is None:
                self._reserve(order, order.size)
                return RiskDecision.ok()

            allowed = self._largest_allowed(order)
            if allowed <= ZERO:
                return self._veto(order, self._violation(order, min(order.size, QUANTUM))
                                  or self._violation(order, order.size) or "no room under the limits")
            self._reserve(order, allowed)
            return RiskDecision.ok(max_size=allowed)

    def _largest_allowed(self, order: Order) -> Decimal:
        """
        Bisection on size. Every limit is convex in the order's size (the
        exposure formula is a max of linear pieces minus a concave min), so
        the sizes that pass form an interval starting at zero.
        """
        lo, hi = ZERO, order.size
        if self._violation(order, QUANTUM) is not None:
            return ZERO
        lo = QUANTUM
        for _ in range(40):
            if hi - lo <= QUANTUM:
                break
            mid = ((lo + hi) / 2).quantize(QUANTUM)
            if mid <= lo:
                break
            if self._violation(order, mid) is None:
                lo = mid
            else:
                hi = mid
        return lo

    def _violation(self, order: Order, size: Decimal) -> str | None:
        """
        The first limit `order` breaks at `size`, or None if it fits.

        Pending orders count only when they push the SAME way as this one.
        A resting order on the other side might never fill, so letting it
        offset this order would understate the worst case — a strategy's own
        resting YES bid must not make its NO bid look like a hedge.
        """
        lim = self.limits
        problem = self._order_problem(order, size)
        if problem is not None:
            return problem

        market = self.market_of(order.venue, order.market_id)
        d = self._direction(order)
        before = self._projected(order.venue, market, direction=d)
        after = self._projected(order.venue, market, extra=(order, size), direction=d)

        net_after = self._net(after)
        if abs(net_after) > lim.max_position_per_market and abs(net_after) > abs(self._net(before)):
            return (f"position limit on {order.venue}/{market}: net {net_after} "
                    f"over {lim.max_position_per_market}")

        market_after = self._exposure(after)
        if market_after > lim.max_exposure_per_market and market_after > self._exposure(before):
            return (f"market exposure on {order.venue}/{market} would be "
                    f"{market_after:.2f}, limit {lim.max_exposure_per_market}")

        # venue and total limits sum each market's worst case; this order
        # changes only its own market's
        other_side = self._exposure(self._projected(order.venue, market, direction=-d))
        increase = (max(market_after, other_side)
                    - self._worst_exposure(order.venue, market))
        venue_after = self.exposure(venue=order.venue) + increase
        if increase > ZERO and venue_after > lim.max_exposure_per_venue:
            return (f"{order.venue} exposure would be {venue_after:.2f}, "
                    f"limit {lim.max_exposure_per_venue}")
        total_after = self.exposure() + increase
        if increase > ZERO and total_after > lim.max_total_exposure:
            return (f"total exposure would be {total_after:.2f}, "
                    f"limit {lim.max_total_exposure}")
        return None

    def _order_problem(self, order: Order, size: Decimal) -> str | None:
        """The limits no order is exempt from: per-order notional and cash."""
        if (order.side == OrderSide.BUY
                and size * order.limit_price > self.limits.max_notional_per_order):
            return (f"order notional {size * order.limit_price:.2f} over "
                    f"{self.limits.max_notional_per_order} per order")
        return self._cash_problem(order, size)

    def _cash_problem(self, order: Order, size: Decimal) -> str | None:
        needed = self._cash_needed(order, size)
        if needed <= ZERO:
            return None
        available = self._available_cash(order.venue)
        if needed > available:
            return f"needs {needed:.2f} cash, {available:.2f} available"
        return None

    def _reduces_risk(self, order: Order) -> bool:
        market = self.market_of(order.venue, order.market_id)
        d = self._direction(order)
        before = self._projected(order.venue, market, direction=d)
        after = self._projected(order.venue, market, extra=(order, order.size), direction=d)
        return (abs(self._net(after)) <= abs(self._net(before))
                and self._exposure(after) <= self._exposure(before)
                and (abs(self._net(after)) < abs(self._net(before))
                     or self._exposure(after) < self._exposure(before)))

    @staticmethod
    def _direction(order_or_reservation) -> int:
        """+1 if it adds YES-equivalent exposure (buy YES / sell NO), else -1."""
        adds_yes = (order_or_reservation.outcome == "YES") == (order_or_reservation.side == OrderSide.BUY)
        return 1 if adds_yes else -1

    def _worst_exposure(self, venue: str, market: str) -> Decimal:
        """A market's exposure if whichever side's pending orders hurt more all fill."""
        return max(self._exposure(self._projected(venue, market, direction=1)),
                   self._exposure(self._projected(venue, market, direction=-1)))

    def _reserve(self, order: Order, size: Decimal) -> None:
        self._reservations[order.order_id] = Reservation(
            order_id=order.order_id, venue=order.venue, market_id=order.market_id,
            outcome=order.outcome, side=order.side, size=size, price=order.limit_price,
            strategy_id=order.strategy_id)

    def _veto(self, order: Order, reason: str) -> RiskDecision:
        self.rejections.append((order, reason))
        log.info("risk veto on %s: %s", order.order_id, reason)
        return RiskDecision.veto(reason)

    # ---- projections ----

    def _venue_markets(self) -> set[MarketKey]:
        keys = {(p.venue, self.market_of(p.venue, p.market_id)) for p in self._positions.values()}
        keys |= {(r.venue, self.market_of(r.venue, r.market_id)) for r in self._reservations.values()}
        return keys

    def _projected(self, venue: str, market: str, extra: tuple[Order, Decimal] | None = None,
                   direction: int | None = None) -> dict[str, Position]:
        """
        The venue market's positions as they would stand if its pending
        reservations (only those pushing `direction`, when given) and
        `extra` all filled in full at their limit prices — the worst case
        the limits have to hold against.
        """
        projected: dict[str, Position] = {}
        for p in self._positions.values():
            if p.venue == venue and self.market_of(p.venue, p.market_id) == market:
                merged = projected.get(p.outcome)
                if merged is None:
                    projected[p.outcome] = copy.copy(p)
                elif p.qty != ZERO:                      # same outcome under two ids: merge
                    merged.apply(OrderSide.BUY if p.qty > 0 else OrderSide.SELL,
                                 abs(p.qty), p.avg_cost)

        pending = [(r.outcome, r.side, r.size, r.price) for r in self._reservations.values()
                   if r.venue == venue and self.market_of(r.venue, r.market_id) == market
                   and (direction is None or self._direction(r) == direction)]
        if extra is not None:
            order, size = extra
            pending.append((order.outcome, order.side, size, order.limit_price))
        for outcome, side, size, price in pending:
            if outcome not in projected:
                projected[outcome] = Position(venue, market, outcome)
            projected[outcome].apply(side, size, price)
        return projected

    @staticmethod
    def _net(positions: dict[str, Position]) -> Decimal:
        yes = positions["YES"].qty if "YES" in positions else ZERO
        no = positions["NO"].qty if "NO" in positions else ZERO
        return yes - no

    @staticmethod
    def _exposure(positions: dict[str, Position]) -> Decimal:
        """max(0, cost - min(YES payout, NO payout)), shorts as long-the-other-side."""
        long_yes = long_no = cost = ZERO
        for outcome, p in positions.items():
            if p.qty == ZERO:
                continue
            if p.qty > ZERO:
                qty, price, as_yes = p.qty, p.avg_cost, outcome == "YES"
            else:
                qty, price, as_yes = -p.qty, ONE - p.avg_cost, outcome != "YES"
            cost += qty * price
            if as_yes:
                long_yes += qty
            else:
                long_no += qty
        return max(ZERO, cost - min(long_yes, long_no))

    # ---- cash ----

    def _pool(self, venue: str) -> str:
        if SHARED in self._cash:
            return SHARED
        if venue not in self._cash:
            self._cash[venue] = ZERO
            self._starting_cash[venue] = ZERO
        return venue

    def _held(self, venue: str, market_id: str, outcome: str) -> Decimal:
        p = self._positions.get((venue, market_id, outcome))
        return p.qty if p else ZERO

    def _cash_needed(self, order: Order, size: Decimal) -> Decimal:
        """A BUY pays its notional; a SELL beyond what is held posts 1 - price collateral."""
        if order.side == OrderSide.BUY:
            return size * order.limit_price
        held = max(self._held(order.venue, order.market_id, order.outcome), ZERO)
        short = max(size - held, ZERO)
        return short * (ONE - order.limit_price)

    def _available_cash(self, venue: str) -> Decimal:
        pool = self._pool(venue)
        committed = ZERO
        for r in self._reservations.values():
            if self._pool(r.venue) != pool:
                continue
            if r.side == OrderSide.BUY:
                committed += r.size * r.price
            else:
                held = max(self._held(r.venue, r.market_id, r.outcome), ZERO)
                committed += max(r.size - held, ZERO) * (ONE - r.price)
        return self._cash[pool] - committed

    # ---- reporting ----

    def summary(self) -> str:
        with self._lock:
            held = {f"{p.venue}/{p.outcome}": str(p.qty)
                    for p in self._positions.values() if p.qty != ZERO}
            return (f"cash ${self.cash:.4f} (realized ${self.realized_pnl():+.4f}, "
                    f"unrealized ${self.unrealized_pnl():+.4f}), "
                    f"fees ${self.fees_paid:.4f}, {len(self.fills)} fills, "
                    f"exposure ${self.exposure():.2f}, positions {held or '{}'}")


def get_portfolio_manager() -> PortfolioManager:
    """Module-level accessor for the process-wide PortfolioManager."""
    return PortfolioManager.instance()
