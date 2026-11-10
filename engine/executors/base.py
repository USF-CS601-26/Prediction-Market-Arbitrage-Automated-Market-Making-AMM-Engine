"""
engine/executors/base.py

BaseExecutor — the only thing a strategy is allowed to know about a venue.

Three operations, because they are the three a paired arbitrage actually
needs:

    submit_order  send one leg
    cancel_order  pull the remainder when the other leg failed
    get_status    find out what we are actually holding

Everything is async. Not because the mocks need it, but because the real
implementations will be WebSocket clients, and a synchronous interface
would force the strategy to be rewritten the day a real venue is plugged in.

Fills are reported two ways on purpose:

  - submit_order returns the OrderState it reached, for the caller that
    wants to await its own leg;
  - registered fill listeners get every Fill as it happens, which is how
    the PortfolioManager learns about fills without the strategy having to
    remember to tell it. A resting order that fills minutes later has no
    caller left to return to, so the listener path is the authoritative one.

Listeners may be sync or async; _emit_fill handles both, mirroring
KalshiWSClient._fire so the two callback conventions in the repo match.
"""

from __future__ import annotations

import inspect
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from decimal import Decimal

from engine.models import Fill, Order, OrderSide, OrderState

log = logging.getLogger(__name__)

# on_fill(Fill) -> None | Awaitable[None]
FillListener = Callable[[Fill], None | Awaitable[None]]

ZERO = Decimal("0")
ONE = Decimal("1")


class OrderNotFound(KeyError):
    """Raised by cancel_order / get_status for an id this executor never saw."""


class BaseExecutor(ABC):
    """
    One venue's order interface.

    Subclasses own their fill simulation or wire protocol; this class owns
    the listener plumbing and the venue-independent order validation that
    every venue would reject anyway.
    """

    venue: str = "unknown"

    def __init__(self, venue: str | None = None):
        if venue is not None:
            self.venue = venue
        self._listeners: list[FillListener] = []

    # ---- fill reporting ----

    def add_fill_listener(self, listener: FillListener) -> None:
        """
        Register a callback for every Fill this executor produces. The
        PortfolioManager's record_fill is the intended first subscriber.
        """
        self._listeners.append(listener)

    async def _emit_fill(self, fill: Fill) -> None:
        """
        Notify every listener. A listener that raises is logged and skipped
        rather than allowed to kill the order's lifecycle — a bookkeeping
        bug must not leave a leg in an unknown state.
        """
        for listener in self._listeners:
            try:
                result = listener(fill)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                log.exception("fill listener failed on %s", fill.order_id)

    # ---- venue-independent validation ----

    def validate(self, order: Order) -> str | None:
        """
        Returns a rejection reason, or None if the order is sane.

        These are the checks every venue makes, so catching them here means
        a mock and a real venue reject the same orders for the same stated
        reason. Venue-specific rules (tick size, minimum size) belong in the
        subclass.
        """
        if order.venue != self.venue:
            return f"order routed to {self.venue} but addressed to {order.venue}"
        if order.size <= ZERO:
            return f"size must be positive, got {order.size}"
        if not (ZERO < order.limit_price < ONE):
            return f"limit price must be strictly between 0 and 1, got {order.limit_price}"
        return None

    def preflight(self, order: Order) -> str | None:
        """
        Would this order be rejected on sight? Returns the reason, or None.

        This is validation WITHOUT sending, which a paired strategy needs:
        dispatching a leg whose price is off-tick or whose size is below the
        venue minimum means one leg fills, the other is rejected, and the
        pair has to be unwound at a loss. Declining to trade is strictly
        better, and only the executor knows the venue's static rules.

        It cannot catch everything — a venue can still reject for reasons we
        cannot see from here, which is why the unwind path exists regardless.
        """
        return self.validate(order)

    # ---- the interface ----

    @abstractmethod
    async def submit_order(self, order: Order) -> OrderState:
        """
        Send one order and return the state it reached.

        The returned state is NOT necessarily terminal: a limit order whose
        remainder could not fill against current depth rests at the venue,
        so PARTIALLY_FILLED and OPEN are both normal outcomes and the caller
        is responsible for cancelling what it no longer wants.
        """

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an order's unfilled remainder. True if this call cancelled it,
        False if it was already terminal (filled, cancelled or rejected) —
        a race the unwind logic has to handle, not an error.
        """

    @abstractmethod
    async def get_status(self, order_id: str) -> OrderState:
        """Current state of one order. Raises OrderNotFound for an unknown id."""

    # ---- shared helpers ----

    @staticmethod
    def opposite(side: OrderSide) -> OrderSide:
        """The side that flattens a position opened on `side`."""
        return OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY

    def __repr__(self) -> str:
        return f"<{type(self).__name__} venue={self.venue}>"
