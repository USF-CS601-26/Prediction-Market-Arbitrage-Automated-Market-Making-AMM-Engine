"""
engine/executors/kalshi.py

MockKalshiExecutor — fills against the live Kalshi book.

Two pieces of Kalshi-specific knowledge live here and nowhere else:

1. There is only ONE book. kalshi_ws.py already normalizes it into a YES
   book (no-side bids become asks at 1 - price), so a NO order has to trade
   against the complement of that book. complement_book() does the
   reflection; see engine/books.py for why this is an identity rather than
   an approximation.

2. Contracts are integral and quoted in whole cents. A fractional size or an
   off-tick price is rejected before it reaches the fill logic.

Freshness is delegated to the feed, not re-derived. KalshiWSClient.
book_is_fresh is already false while disconnected, before the first
snapshot, and during a sequence-gap resync; passing it in as `is_fresh`
means the executor refuses to trade on a stale book for exactly the reasons
the ingestion engine already knows about.
"""

from __future__ import annotations

from decimal import Decimal

from order_book import OrderBook

from engine.books import complement_book
from engine.executors.simulator import FeeFn, SimulationConfig, SimulatedExecutor, _no_fee
from engine.models import Order

# Kalshi trades whole contracts, quoted in whole cents.
KALSHI_DEFAULTS = SimulationConfig(
    latency_ms=40.0,
    latency_jitter_ms=15.0,
    cancel_latency_ms=25.0,
    min_order_size=Decimal("1"),
    size_increment=Decimal("1"),
    tick_size=Decimal("0.01"),
)


class MockKalshiExecutor(SimulatedExecutor):
    """
    Args:
        ticker:   the market this executor trades; orders for anything else
                  are rejected rather than silently filled against the wrong
                  book.
        book:     the OrderBook KalshiWSClient maintains (a YES book).
        fee_fn:   (contracts, price) -> fee, normally
                  lambda c, p: kalshi_fees.taker_fee(c, p, schedule).
        is_fresh: callable returning whether the book is currently safe to
                  read. Pass `lambda: client.book_is_fresh` in the live
                  pipeline; the default trusts the book, which is what a
                  hand-built fixture wants.
    """

    venue = "kalshi"

    def __init__(self, ticker: str, book: OrderBook, fee_fn: FeeFn = _no_fee,
                 is_fresh=None, config: SimulationConfig | None = None):
        super().__init__(venue="kalshi", fee_fn=fee_fn,
                         config=config or KALSHI_DEFAULTS)
        self.ticker = ticker
        self.book = book
        self.is_fresh = is_fresh or (lambda: True)

    def book_for(self, order: Order) -> OrderBook | None:
        """
        YES trades the book as-is; NO trades its complement.

        The complement is rebuilt on every call rather than cached. Orders
        are orders of magnitude rarer than book updates, and a cache keyed on
        anything less than the full book state is a way to fill against a
        book that has already moved.
        """
        if not self.is_fresh() or not (self.book.bids or self.book.asks):
            return None
        return self.book if order.outcome == "YES" else complement_book(self.book, order.market_id)

    def book_unavailable_reason(self, order: Order) -> str:
        if not self.is_fresh():
            return (f"Kalshi book for {self.ticker} is not fresh "
                    "(disconnected, unseeded, or resyncing after a sequence gap)")
        return f"Kalshi book for {self.ticker} is empty on both sides"

    def venue_validate(self, order: Order) -> str | None:
        if order.market_id != self.ticker:
            return f"this executor trades {self.ticker}, not {order.market_id}"
        if order.size != order.size.to_integral_value():
            return f"Kalshi trades whole contracts, got size {order.size}"
        return super().venue_validate(order)
