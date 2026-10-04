"""
order_book.py

Core in-memory L2 order book engine. Maintains sorted bid/ask price levels
for a single market and applies snapshot/delta updates deterministically.

This file has zero network or I/O dependencies by design — it's pure state
logic, which makes it fully unit-testable offline (see tests/test_order_book.py)
before kalshi_ws.py ever touches a live connection.
"""

from sortedcontainers import SortedDict
from models import OrderBookSnapshot, OrderBookDelta, Side, PriceLevel


class OrderBook:
    """
    In-memory L2 order book for one market on one exchange.

    Bids and asks are each stored as a SortedDict (price -> size), which
    keeps price levels in sorted order automatically so top-of-book and
    depth reads don't require re-sorting on every call.
    """

    def __init__(self, market_id: str, exchange: str):
        self.market_id = market_id
        self.exchange = exchange
        self.bids: SortedDict = SortedDict()   # price -> size, sorted ascending by key
        self.asks: SortedDict = SortedDict()
        self.last_sequence: int | None = None  # tracks the last applied message's sequence number

    def apply_snapshot(self, snapshot: OrderBookSnapshot) -> None:
        """
        Full state replace. Called on initial WebSocket connect, or after
        a resync triggered by a detected sequence gap in apply_delta.
        Wipes any existing state rather than merging — a snapshot is
        authoritative.
        """
        self.bids.clear()
        self.asks.clear()
        for level in snapshot.bids:
            self.bids[level.price] = level.size
        for level in snapshot.asks:
            self.asks[level.price] = level.size
        self.last_sequence = snapshot.sequence

    def apply_delta(self, delta: OrderBookDelta) -> None:
        """
        Incremental update to a single price level. Raises ValueError if
        the delta's sequence number isn't exactly one more than the last
        applied message — this is the gap-detection mechanism that
        prevents the book from silently drifting out of sync with the
        exchange if a WebSocket message gets dropped.
        """
        if (self.last_sequence is not None
                and delta.sequence is not None
                and delta.sequence != self.last_sequence + 1):
            raise ValueError(
                f"Sequence gap on {self.market_id}: expected "
                f"{self.last_sequence + 1}, got {delta.sequence}"
            )

        # route the update to the correct side's SortedDict
        book = self.bids if delta.side == Side.BID else self.asks
        if delta.size == 0:
            # size == 0 is the convention (per models.py) for "remove this level"
            book.pop(delta.price, None)
        else:
            # insert new level or overwrite existing size at this price
            book[delta.price] = delta.size

        self.last_sequence = delta.sequence

    def get_top_of_book(self) -> tuple[PriceLevel | None, PriceLevel | None]:
        """
        Returns (best_bid, best_ask) — the highest bid and lowest ask
        currently in the book. Returns None for either side if that side
        is currently empty.
        """
        best_bid = None
        if self.bids:
            # SortedDict keeps keys ascending, so the highest bid is the last item
            price = self.bids.peekitem(-1)[0]
            best_bid = PriceLevel(price=price, size=self.bids[price])

        best_ask = None
        if self.asks:
            # the lowest ask is the first item
            price = self.asks.peekitem(0)[0]
            best_ask = PriceLevel(price=price, size=self.asks[price])

        return best_bid, best_ask

    def get_depth(self, levels: int = 10) -> tuple[list[PriceLevel], list[PriceLevel]]:
        """
        Returns up to `levels` price levels per side, ordered best-first
        (highest bid first, lowest ask first) — i.e. how depth is
        conventionally displayed in a trading UI.
        """
        # take the last `levels` bid entries (highest prices) and reverse
        # so the best (highest) bid comes first in the output list
        bid_items = list(self.bids.items())[-levels:]
        bid_levels = [PriceLevel(price=p, size=s) for p, s in reversed(bid_items)]

        # asks are already ascending, so the first `levels` entries are
        # already best-first (lowest price = best ask)
        ask_items = list(self.asks.items())[:levels]
        ask_levels = [PriceLevel(price=p, size=s) for p, s in ask_items]

        return bid_levels, ask_levels