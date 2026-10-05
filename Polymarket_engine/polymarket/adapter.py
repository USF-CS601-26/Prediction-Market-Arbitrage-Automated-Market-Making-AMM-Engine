"""
polymarket/adapter.py

Converts parsed Polymarket events into the shared, exchange-neutral
contracts from models.py (OrderBookSnapshot / OrderBookDelta), so the same
OrderBook class and downstream spread engine work for both venues.

Mapping:
  book                -> one OrderBookSnapshot
  price_change        -> one OrderBookDelta per entry in price_changes
  BUY / SELL          -> Side.BID / Side.ASK
  asset_id (token id) -> market_id   (one OrderBook per outcome token)
  timestamp (ms str)  -> timezone-aware UTC datetime
  sequence            -> None        (Polymarket has no sequence numbers, so
                                      OrderBook's gap check is skipped)

Polymarket's `size` is already the new total at that price, which matches
the models.py convention, so no arithmetic is needed here.
"""

from datetime import datetime, timedelta, timezone

from models import OrderBookDelta, OrderBookSnapshot, PriceLevel, Side
from polymarket.messages import BookMsg, PolymarketEvent, PriceChangeMsg

EXCHANGE = "polymarket"

_SIDE = {"BUY": Side.BID, "SELL": Side.ASK}

BookUpdate = OrderBookSnapshot | OrderBookDelta


def ms_to_datetime(ms: int) -> datetime:
    # integer arithmetic keeps the millisecond exact (ms / 1000 would go through float)
    return datetime.fromtimestamp(ms // 1000, tz=timezone.utc) + timedelta(milliseconds=ms % 1000)


def book_to_snapshot(msg: BookMsg) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        market_id=msg.asset_id,
        exchange=EXCHANGE,
        timestamp=ms_to_datetime(msg.timestamp),
        # drop zero-size levels so they never show up as phantom liquidity
        bids=[PriceLevel(price=l.price, size=l.size) for l in msg.bids if l.size > 0],
        asks=[PriceLevel(price=l.price, size=l.size) for l in msg.asks if l.size > 0],
        sequence=None,
    )


def price_change_to_deltas(msg: PriceChangeMsg) -> list[OrderBookDelta]:
    ts = ms_to_datetime(msg.timestamp)
    return [
        OrderBookDelta(
            market_id=c.asset_id,
            exchange=EXCHANGE,
            timestamp=ts,
            side=_SIDE[c.side],
            price=c.price,
            size=c.size,
            sequence=None,
        )
        for c in msg.price_changes
    ]


def to_book_updates(event: PolymarketEvent) -> list[BookUpdate]:
    """Book-changing updates for one event; empty for events that don't touch L2 levels."""
    match event:
        case BookMsg():
            return [book_to_snapshot(event)]
        case PriceChangeMsg():
            return price_change_to_deltas(event)
        case _:
            # tick_size_change / last_trade_price don't change price levels
            return []
