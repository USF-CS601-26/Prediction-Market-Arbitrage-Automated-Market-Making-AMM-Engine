"""
models.py

Shared data contracts for the Kalshi ingestion pipeline.

These Pydantic models define the normalized shape that ALL market data gets
converted into, regardless of which exchange it came from. order_book.py,
kalshi_ws.py, and (eventually) Junyu's sentiment/strategy modules all import
these — so this is the single source of truth for field names and types.

Decimal is used instead of float for all price/size fields to avoid floating
point rounding errors in downstream spread/arbitrage calculations.
"""

from pydantic import BaseModel
from enum import Enum
from decimal import Decimal
from datetime import datetime


class Side(str, Enum):
    """
    Which side of the book a price level or delta belongs to.
    Inherits from str so it serializes cleanly to/from JSON as "bid"/"ask"
    rather than as a raw enum object.
    """
    BID = "bid"
    ASK = "ask"


class PriceLevel(BaseModel):
    """
    A single price/size pair on one side of the order book.
    Used both for full snapshots (list of these per side) and as the
    return type for top-of-book / depth queries in OrderBook.
    """
    price: Decimal
    size: Decimal


class OrderBookSnapshot(BaseModel):
    """
    A full point-in-time replace of the order book state for one market.
    Sent on initial WebSocket connect, or after a resync following a
    detected sequence gap (see OrderBookDelta.sequence below).
    """
    market_id: str                  # exchange-specific market/ticker identifier
    exchange: str                   # e.g. "kalshi" — lets downstream code tell venues apart
    timestamp: datetime             # when the exchange generated this snapshot
    bids: list[PriceLevel]          # all bid levels included in the snapshot
    asks: list[PriceLevel]          # all ask levels included in the snapshot
    sequence: int | None = None     # snapshot's position in the message sequence, if provided


class OrderBookDelta(BaseModel):
    """
    A single incremental update to one price level on one side of the book.
    Applied on top of the last known snapshot/delta state in order_book.py.
    """
    market_id: str
    exchange: str
    timestamp: datetime
    side: Side                      # which side (bid/ask) this update applies to
    price: Decimal                  # the price level being updated
    size: Decimal                   # new size at this price; size == 0 means "delete this level"
    sequence: int | None = None     # must be exactly last_sequence + 1, or OrderBook
                                     # raises ValueError — this is how dropped/missed
                                     # messages get detected instead of silently
                                     # corrupting the book's state