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

class Candle(BaseModel):
    """
    One OHLC period for a single market, from Kalshi's candlesticks
    endpoint. Used for backtesting and signal work — note this is
    historical *trade* data, a fundamentally different thing from the
    live resting orders in OrderBookSnapshot.

    Two independent price series live here:

      - open/high/low/close/mean describe prices that actually TRADED.
        They are all None in a period with no trades, which is the common
        case on a quiet market at 1-minute resolution, so every consumer
        has to handle None rather than assume a price exists.
      - yes_bid_* / yes_ask_* describe the QUOTE, which exists whether or
        not anyone traded. On an illiquid market this is usually the more
        informative series of the two.
    """
    market_id: str
    exchange: str
    end_ts: datetime                 # close of this candle's period
    interval_minutes: int            # 1, 60 or 1440 — Kalshi accepts no others

    open: Decimal | None = None      # traded-price OHLC; None when volume == 0
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    mean: Decimal | None = None      # volume-weighted mean traded price
    previous: Decimal | None = None  # last trade before this period, if any

    yes_bid_open: Decimal | None = None   # quote OHLC — present even with no trades
    yes_bid_close: Decimal | None = None
    yes_ask_open: Decimal | None = None
    yes_ask_close: Decimal | None = None

    volume: Decimal = Decimal(0)          # contracts traded during the period
    open_interest: Decimal = Decimal(0)   # contracts outstanding at period end


class FeeSchedule(BaseModel):
    """
    One venue's fee parameters for one series/market group.

    Kept venue-neutral so the executable spread engine can price Kalshi
    and Polymarket fills through the same code path — each venue
    supplies its own schedule and fee function.
    """
    venue: str
    series: str
    fee_type: str            # e.g. "quadratic", "quadratic_with_maker_fees"
    multiplier: Decimal      # per-series scaling; 0 means the series is fee-free


class Fill(BaseModel):
    """One price level consumed while walking the book."""
    price: Decimal
    size: Decimal
    fee: Decimal


class ExecutionQuote(BaseModel):
    """
    What it would actually cost to trade `requested` contracts right now,
    after walking real depth and applying real fees.

    This is the unit the arbitrage detector compares across venues. Note
    `filled` may be less than `requested`: a venue can simply not have
    the depth, and treating a partial fill as complete is how a detector
    reports size it could never actually get.
    """
    market_id: str
    exchange: str
    side: Side                   # BID = we are selling into bids, ASK = buying from asks
    requested: Decimal
    filled: Decimal
    fully_filled: bool
    gross: Decimal               # notional before fees
    fees: Decimal
    net: Decimal                 # cost to buy, or proceeds to sell, after fees
    avg_price: Decimal | None    # net / filled; None if nothing could fill
    fills: list[Fill] = []
