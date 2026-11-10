"""
engine/books.py

Turning a venue's book into "what does it cost to BUY this outcome".

The two venues express the same thing differently, and the arbitrage math
only works once they are expressed the same way:

  Kalshi      one ticker, one book. kalshi_ws.py already folds the no-side
              bids into asks via price = 1 - no_price, so the OrderBook it
              maintains is a YES book: bids are YES bids, asks are YES asks.
              There is no NO book to read — buying NO at q is the same trade
              as selling YES at 1 - q, so the NO book has to be derived.

  Polymarket  two token books, one per outcome, each with its own bids and
              asks. Buying NO reads the NO book directly.

complement_book() does the derivation. It is an identity about the same
resting orders, not an approximation: a YES bid of 300 at 0.41 IS an offer
of 300 NO at 0.59, because YES + NO = $1 at settlement.

Why Polymarket's NO book is NOT also synthesized and merged: on Polymarket's
CLOB a NO ask and the mirroring YES bid are the same resting order, so
merging the real NO book with one synthesized from YES bids would count the
same liquidity twice and report double the executable size. Kalshi needs the
derivation precisely because its feed gives us only one book; Polymarket must
not get it because its feed gives us both.
"""

from decimal import Decimal
from typing import Protocol

from models import OrderBookSnapshot, PriceLevel
from order_book import OrderBook

ONE = Decimal("1")


def complement_book(book: OrderBook, market_id: str | None = None) -> OrderBook:
    """
    The NO book implied by a YES book (and vice versa — the operation is its
    own inverse).

        NO ask at (1 - p)  <- YES bid at p     (selling YES = buying NO)
        NO bid at (1 - p)  <- YES ask at p     (buying YES  = selling NO)

    Sizes are unchanged; only prices are reflected through 1. Note the sides
    swap, which is what makes the best YES bid (highest) the best NO ask
    (lowest), exactly as it should be.

    Returns a NEW OrderBook. The source book keeps being mutated by its
    feed, so this is a point-in-time derivation and has to be recomputed
    whenever the underlying book changes.
    """
    out = OrderBook(market_id=market_id or f"{book.market_id}:COMPLEMENT",
                    exchange=book.exchange)
    out.apply_snapshot(OrderBookSnapshot(
        market_id=out.market_id,
        exchange=book.exchange,
        timestamp="1970-01-01T00:00:00Z",   # derived state has no exchange timestamp
        bids=[PriceLevel(price=ONE - p, size=s) for p, s in book.asks.items()],
        asks=[PriceLevel(price=ONE - p, size=s) for p, s in book.bids.items()],
        sequence=book.last_sequence,
    ))
    return out


def ask_ladder(book: OrderBook, limit_price: Decimal | None = None) -> list[tuple[Decimal, Decimal]]:
    """
    (price, size) pairs to lift when BUYING, cheapest first, truncated at
    `limit_price` if given. This is the ladder every buy-side depth walk
    consumes.
    """
    levels = book.asks.items()
    if limit_price is None:
        return list(levels)
    return [(p, s) for p, s in levels if p <= limit_price]


def bid_ladder(book: OrderBook, limit_price: Decimal | None = None) -> list[tuple[Decimal, Decimal]]:
    """(price, size) pairs to hit when SELLING, highest first, floored at `limit_price`."""
    levels = reversed(book.bids.items())
    if limit_price is None:
        return list(levels)
    return [(p, s) for p, s in levels if p >= limit_price]


class OutcomeBooks(Protocol):
    """
    What the Polymarket-shaped executors and strategies need from a feed:
    one book per outcome, plus whether that book can be trusted right now.

    PolymarketFeed satisfies this as written. It is a Protocol rather than a
    base class so nothing in engine/ has to import the Polymarket feed (and
    with it websockets and an event loop) just to be type-checked against it.
    """

    def book(self, outcome: str) -> OrderBook: ...

    def is_valid(self, outcome: str) -> bool: ...


class StaticOutcomeBooks:
    """
    An OutcomeBooks backed by hand-built books. Used by tests and by the
    replay harness's fixtures, where there is no feed to ask.
    """

    def __init__(self, books: dict[str, OrderBook], valid: bool = True):
        self._books = books
        self.valid = valid

    def book(self, outcome: str) -> OrderBook:
        return self._books[outcome]

    def is_valid(self, outcome: str) -> bool:
        return self.valid and outcome in self._books
