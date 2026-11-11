"""
engine/portfolio.py

The risk seam between the strategies and the venues.

Person A owns the real PortfolioManager (it also backs the AMM's inventory
skew). This file defines the contract both sides agreed on, plus a working
reference implementation so the pipeline is runnable end to end before that
lands and so the arbitrage tests have something to check against.

The contract is two calls:

    check_order(order) -> RiskDecision     before dispatch, every order
    record_fill(fill)  -> None             after every fill, from the
                                           executor's fill listener

record_fill being driven by the executor's listener rather than by the
strategy is deliberate. A resting order can fill long after the strategy has
moved on, and an unwind path that throws will skip whatever bookkeeping came
after it; wiring the portfolio directly to the fill stream means position
state cannot silently diverge from what the venues actually did.

RiskDecision carries max_size, so the risk layer can shrink an order instead
of only vetoing it — the common real case is a genuine edge against a
position limit that allows part of it.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from decimal import Decimal
from typing import Protocol, runtime_checkable

from engine.models import Fill, Order, OrderSide, RiskDecision

log = logging.getLogger(__name__)

ZERO = Decimal("0")
ONE = Decimal("1")

# (venue, market_id, outcome) — a position is only fungible within one of these
PositionKey = tuple[str, str, str]


@runtime_checkable
class RiskManager(Protocol):
    """
    Implement this to replace SimplePortfolioManager with the real thing.

    check_order may be sync or async; the strategy awaits the result if it is
    awaitable, so Person A can make it hit a database without the strategy
    changing.
    """

    def check_order(self, order: Order) -> RiskDecision: ...

    def record_fill(self, fill: Fill) -> None: ...


class SimplePortfolioManager:
    """
    Reference implementation: in-memory positions, cash and three limits.

    What it is NOT: it does not model margin, settlement timing, or the fact
    that a Kalshi position and a Polymarket position settle on different
    rails at different times. A paired arbitrage is flat in exposure terms
    but not in capital terms, and nothing here prices that. The limits below
    are notional caps, which is the honest level of protection for a mock.
    """

    def __init__(self,
                 cash: Decimal = Decimal("10000"),
                 max_position_per_market: Decimal = Decimal("1000"),
                 max_notional_per_order: Decimal = Decimal("1000"),
                 max_gross_exposure: Decimal = Decimal("5000"),
                 allow_unwind_over_limit: bool = True):
        self.cash = cash
        self.starting_cash = cash
        self.max_position_per_market = max_position_per_market
        self.max_notional_per_order = max_notional_per_order
        self.max_gross_exposure = max_gross_exposure
        self.allow_unwind_over_limit = allow_unwind_over_limit

        self.positions: dict[PositionKey, Decimal] = defaultdict(lambda: ZERO)
        self.fees_paid = ZERO
        self.fills: list[Fill] = []
        self.rejections: list[tuple[Order, str]] = []

    # ---- reads ----

    @staticmethod
    def key(venue: str, market_id: str, outcome: str) -> PositionKey:
        return (venue, market_id, outcome)

    def position(self, venue: str, market_id: str, outcome: str) -> Decimal:
        return self.positions[self.key(venue, market_id, outcome)]

    def gross_exposure(self) -> Decimal:
        """
        Sum of |position| x $1 across every market: the most that could be
        lost if every contract held expired worthless. A paired arbitrage
        shows up here as exposure on both legs even though the pair is
        hedged, which overstates risk — the conservative direction.
        """
        return sum((abs(q) for q in self.positions.values()), ZERO)

    def realized_pnl(self) -> Decimal:
        """Cash moved so far. Open positions are not marked to market."""
        return self.cash - self.starting_cash

    # ---- the contract ----

    def check_order(self, order: Order) -> RiskDecision:
        """
        Approve, shrink, or veto one order.

        An unwind order is allowed past the position limit by default: it
        REDUCES exposure, and blocking it would strand a naked leg, which is
        the exact risk the limit exists to prevent.
        """
        if order.size <= ZERO:
            return RiskDecision.veto("size must be positive")

        if order.is_unwind and self.allow_unwind_over_limit:
            return RiskDecision.ok()

        allowed = order.size

        notional = order.size * order.limit_price
        if notional > self.max_notional_per_order:
            allowed = min(allowed, self.max_notional_per_order / order.limit_price)

        if order.side == OrderSide.BUY:
            held = self.position(order.venue, order.market_id, order.outcome)
            room = self.max_position_per_market - held
            if room <= ZERO:
                return self._veto(order, f"position limit reached on "
                                         f"{order.market_id}/{order.outcome}: {held}")
            allowed = min(allowed, room)

            exposure_room = self.max_gross_exposure - self.gross_exposure()
            if exposure_room <= ZERO:
                return self._veto(order, f"gross exposure limit reached "
                                         f"({self.gross_exposure()})")
            allowed = min(allowed, exposure_room)

            cost = allowed * order.limit_price
            if cost > self.cash:
                if self.cash <= ZERO:
                    return self._veto(order, "no cash")
                allowed = min(allowed, self.cash / order.limit_price)

        if allowed <= ZERO:
            return self._veto(order, "no room left after limits")
        return RiskDecision.ok(max_size=allowed if allowed < order.size else None)

    def record_fill(self, fill: Fill) -> None:
        """Wire this as executor.add_fill_listener(pm.record_fill)."""
        key = self.key(fill.venue, fill.market_id, fill.outcome)
        signed = fill.size if fill.side == OrderSide.BUY else -fill.size
        self.positions[key] += signed
        self.cash += fill.cash_flow
        self.fees_paid += fill.fee
        self.fills.append(fill)

    # ---- helpers ----

    def _veto(self, order: Order, reason: str) -> RiskDecision:
        self.rejections.append((order, reason))
        log.info("risk veto on %s: %s", order.order_id, reason)
        return RiskDecision.veto(reason)

    def summary(self) -> str:
        held = {f"{v}/{o}": str(q) for (v, _m, o), q in self.positions.items() if q != ZERO}
        return (f"cash ${self.cash:.4f} (realized ${self.realized_pnl():+.4f}), "
                f"fees ${self.fees_paid:.4f}, {len(self.fills)} fills, "
                f"gross exposure {self.gross_exposure()}, positions {held or '{}'}")


class AlwaysApprove:
    """
    A RiskManager that approves everything and still books fills. For tests
    that are about the arbitrage math rather than the limits — using the real
    manager there makes a test fail for two unrelated reasons at once.
    """

    def __init__(self) -> None:
        self.fills: list[Fill] = []
        self.checked: list[Order] = []

    def check_order(self, order: Order) -> RiskDecision:
        self.checked.append(order)
        return RiskDecision.ok()

    def record_fill(self, fill: Fill) -> None:
        self.fills.append(fill)
