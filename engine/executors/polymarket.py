"""
engine/executors/polymarket.py

MockPolymarketExecutor — fills against the live Polymarket books.

Polymarket is the easier venue to model and the harder one to trust:

  easier   each outcome has its own token book, so a NO order reads the NO
           book directly with no derivation (and must NOT also synthesize
           one from YES bids — on this CLOB those are the same resting
           orders, so merging would double-count; see engine/books.py).

  harder   there are no sequence numbers, so nothing in the protocol detects
           a dropped message. PolymarketFeed tracks validity itself, and this
           executor refuses to trade an outcome whose book `is_valid` reports
           as stale — before the first snapshot, or after a disconnect until
           the resubscribe snapshot lands.

Shares are fractional here, unlike Kalshi contracts, so the size increment
is 0.01 rather than 1.
"""

from __future__ import annotations

from decimal import Decimal

from order_book import OrderBook

from engine.books import OutcomeBooks
from engine.executors.simulator import FeeFn, SimulationConfig, SimulatedExecutor, _no_fee
from engine.models import Order

# Polymarket's CLOB is an off-exchange matching engine; round trips are
# slower and less predictable than Kalshi's.
POLYMARKET_DEFAULTS = SimulationConfig(
    latency_ms=120.0,
    latency_jitter_ms=60.0,
    cancel_latency_ms=80.0,
    min_order_size=Decimal("5"),          # CLOB minimum order size, in shares
    size_increment=Decimal("0.01"),
    tick_size=Decimal("0.01"),
)


class MockPolymarketExecutor(SimulatedExecutor):
    """
    Args:
        feed:     anything satisfying engine.books.OutcomeBooks — in the
                  pipeline this is the PolymarketFeed itself, in tests a
                  StaticOutcomeBooks.
        fee_fn:   (shares, price) -> fee, normally
                  polymarket.fees.fee_fn(pair.polymarket.fees).
        tokens:   optional {outcome: token_id}. When given, an order's
                  market_id is checked against it, so a YES order carrying
                  the NO token id is rejected instead of filling against the
                  book its outcome names.
    """

    venue = "polymarket"

    def __init__(self, feed: OutcomeBooks, fee_fn: FeeFn = _no_fee,
                 tokens: dict[str, str] | None = None,
                 config: SimulationConfig | None = None):
        super().__init__(venue="polymarket", fee_fn=fee_fn,
                         config=config or POLYMARKET_DEFAULTS)
        self.feed = feed
        self.tokens = tokens or {}

    def book_for(self, order: Order) -> OrderBook | None:
        if not self.feed.is_valid(order.outcome):
            return None
        book = self.feed.book(order.outcome)
        return book if (book.bids or book.asks) else None

    def book_unavailable_reason(self, order: Order) -> str:
        if not self.feed.is_valid(order.outcome):
            return (f"Polymarket {order.outcome} book is invalid "
                    "(no snapshot yet, or stale since a disconnect)")
        return f"Polymarket {order.outcome} book is empty on both sides"

    def venue_validate(self, order: Order) -> str | None:
        expected = self.tokens.get(order.outcome)
        if expected is not None and expected != order.market_id:
            return (f"{order.outcome} trades token {expected}, "
                    f"but the order names {order.market_id}")
        return super().venue_validate(order)
