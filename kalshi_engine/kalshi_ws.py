"""
kalshi_ws.py

Async WebSocket client that connects to Kalshi's live (demo) order book
feed, authenticates using kalshi_auth.py, subscribes to orderbook_delta
for one market, and applies incoming snapshot/delta messages to a local
OrderBook instance.

Wire format notes (Kalshi's fixed-point format; the legacy integer-cents
fields were removed from the API on 2026-03-12):

  - Prices arrive as decimal-dollar strings, e.g. "0.0800" = 8c.
  - Contract counts arrive as fixed-point strings, e.g. "300.00".
  - A snapshot carries msg.yes_dollars_fp / msg.no_dollars_fp, each a list
    of [price, count] string pairs.
  - A delta carries msg.price_dollars, msg.delta_fp and msg.side.
  - seq lives on the message ENVELOPE (data["seq"]), not inside msg.

Handles Kalshi's bid-only yes/no book structure by translating:
  - yes levels -> this OrderBook's bids directly
  - no levels  -> this OrderBook's asks, via price = 1 - no_price
                  (since bidding "no" at X is economically the same as
                  offering "yes" at 1 - X)
"""

import asyncio
import json
import os
import websockets
from datetime import datetime, timezone
from decimal import Decimal
from dotenv import load_dotenv

from kalshi_auth import generate_auth_headers
from order_book import OrderBook
from models import OrderBookSnapshot, OrderBookDelta, PriceLevel, Side

load_dotenv()

# Production market data over REST is public, but the production
# WebSocket requires production credentials (see kalshi_rest_feed.py for
# the no-credentials path). Switch with KALSHI_ENV=prod in .env.
WS_HOSTS = {
    "demo": "wss://demo-api.kalshi.co",
    "prod": "wss://api.elections.kalshi.com",
}
KALSHI_ENV = os.getenv("KALSHI_ENV", "demo").lower()
WS_URL = os.getenv("KALSHI_WS_URL") or f"{WS_HOSTS[KALSHI_ENV]}/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"
MARKET_TICKER = os.getenv("KALSHI_MARKET_TICKER")


def _to_ask_price(no_price: Decimal) -> Decimal:
    """
    Kalshi's book only carries bids on each side (yes, no).
    A no-side bid at price X is economically equivalent to a yes-side
    ask at (1 - X), so this is how we derive "asks" for our bid/ask
    OrderBook model from Kalshi's yes/no structure.
    """
    return Decimal("1") - no_price


def _event_time(msg: dict, envelope: dict) -> datetime:
    """
    Resolves an event timestamp from whichever field Kalshi populated.
    Deltas carry ts/ts_ms; snapshots often carry neither, in which case
    the envelope's sending_ts_ms is the best available clock.
    """
    if msg.get("ts"):
        return datetime.fromisoformat(str(msg["ts"]).replace("Z", "+00:00"))
    for ms_field, src in (("ts_ms", msg), ("sending_ts_ms", envelope)):
        if src.get(ms_field):
            return datetime.fromtimestamp(src[ms_field] / 1000, tz=timezone.utc)
    return datetime.now(tz=timezone.utc)


def _levels(msg: dict, *names: str) -> list:
    """
    Pulls a side's [[price, count], ...] array out of a snapshot msg.

    Kalshi spells these yes_dollars_fp / no_dollars_fp, but the same
    payload is served as orderbook_fp.{yes,no}_dollars on the REST
    endpoint, so both spellings are accepted rather than silently
    returning an empty book if we hit the other one. An empty book omits
    the key entirely, which is a legitimately empty side.
    """
    for name in names:
        if name in msg:
            return msg[name] or []
    nested = msg.get("orderbook_fp") or {}
    for name in names:
        if name in nested:
            return nested[name] or []
    return []


def _kalshi_snapshot_to_model(msg: dict, envelope: dict) -> OrderBookSnapshot:
    """
    Converts a raw Kalshi orderbook_snapshot message into our normalized
    OrderBookSnapshot.
    """
    yes_levels = _levels(msg, "yes_dollars_fp", "yes_dollars")
    no_levels = _levels(msg, "no_dollars_fp", "no_dollars")

    bids = [
        PriceLevel(price=Decimal(str(p)), size=Decimal(str(s)))
        for p, s in yes_levels
    ]
    asks = [
        PriceLevel(price=_to_ask_price(Decimal(str(p))), size=Decimal(str(s)))
        for p, s in no_levels
    ]

    return OrderBookSnapshot(
        market_id=msg["market_ticker"],
        exchange="kalshi",
        timestamp=_event_time(msg, envelope),
        bids=bids,
        asks=asks,
        sequence=envelope.get("seq"),
    )


def _parse_delta(msg: dict) -> tuple[Side, Decimal, Decimal]:
    """
    Pulls (side, price, size_change) out of a raw orderbook_delta msg,
    already translated into this OrderBook's bid/ask coordinate space.

    NOTE: delta_fp is an INCREMENTAL change to the count at that price
    (negative when size is leaving the book), whereas OrderBookDelta.size
    is the resulting ABSOLUTE size at that level. Converting between the
    two needs the book's current state, so that happens in the client —
    this function only reports the change.
    """
    raw_price = Decimal(str(msg["price_dollars"]))
    change = Decimal(str(msg["delta_fp"]))

    if msg["side"] == "yes":
        return Side.BID, raw_price, change
    return Side.ASK, _to_ask_price(raw_price), change


class KalshiWSClient:
    def __init__(self, market_ticker: str, order_book: OrderBook):
        self.market_ticker = market_ticker
        self.order_book = order_book
        self._ws = None
        self._message_id = 1
        self._sid = None
        # set while we've detected a sequence gap and are waiting on a
        # fresh snapshot; deltas that arrive in the meantime are dropped
        # because they'd apply on top of known-stale state
        self._resyncing = False

    async def connect_and_listen(self):
        auth_headers = generate_auth_headers("GET", WS_PATH)

        async with websockets.connect(WS_URL, additional_headers=auth_headers) as ws:
            self._ws = ws
            print(f"Connected to {WS_URL}. "
                  f"Subscribing to orderbook for {self.market_ticker}")
            await self._subscribe()

            async for raw_msg in ws:
                await self._handle_message(json.loads(raw_msg))

    async def _send(self, payload: dict):
        payload["id"] = self._message_id
        self._message_id += 1
        await self._ws.send(json.dumps(payload))

    async def _subscribe(self):
        await self._send({
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": [self.market_ticker],
            },
        })

    async def _request_snapshot(self):
        """
        Asks Kalshi to re-send a full snapshot on the existing
        subscription. Used to recover from a sequence gap without
        tearing down the connection. Callers set _resyncing first, so
        that deltas arriving before this send completes are dropped.
        """
        await self._send({
            "cmd": "update_subscription",
            "params": {
                "sids": [self._sid],
                "action": "get_snapshot",
                # required: Kalshi rejects get_snapshot without it
                # ({"code": 14, "msg": "Market Ticker required"})
                "market_tickers": [self.market_ticker],
            },
        })

    async def _handle_message(self, data: dict):
        msg_type = data.get("type")
        msg = data.get("msg", {})

        if msg_type == "subscribed":
            self._sid = msg.get("sid")
            print(f"Subscribed: sid={self._sid}")

        elif msg_type == "orderbook_snapshot":
            snapshot = _kalshi_snapshot_to_model(msg, data)
            self.order_book.apply_snapshot(snapshot)
            self._resyncing = False
            self._print_top_of_book("snapshot")

        elif msg_type == "orderbook_delta":
            if self._resyncing:
                # still waiting on the post-gap snapshot; this delta
                # would corrupt the book, so drop it
                return
            await self._apply_delta(msg, data)

        elif msg_type == "error":
            print(f"Feed error: {data}")

    async def _apply_delta(self, msg: dict, envelope: dict):
        side, price, change = _parse_delta(msg)

        # Kalshi sends a CHANGE in contract count; OrderBook wants the
        # resulting absolute size, so add the change to what we currently
        # hold at that (already translated) price level.
        book = self.order_book.bids if side == Side.BID else self.order_book.asks
        new_size = book.get(price, Decimal("0")) + change
        if new_size < 0:
            # should not happen on a correctly-sequenced feed; clamping
            # to 0 removes the level rather than storing a negative size
            new_size = Decimal("0")

        delta = OrderBookDelta(
            market_id=msg["market_ticker"],
            exchange="kalshi",
            timestamp=_event_time(msg, envelope),
            side=side,
            price=price,
            size=new_size,
            sequence=envelope.get("seq"),
        )

        try:
            self.order_book.apply_delta(delta)
        except ValueError as e:
            # sequence gap -> our state is stale, so resync instead of
            # letting the book drift silently out of sync. The flag is
            # set before awaiting so there's no window in which further
            # deltas get applied on top of the stale book.
            self._resyncing = True
            print(f"{e} — requesting fresh snapshot")
            await self._request_snapshot()
            return

        self._print_top_of_book("delta")

    def _print_top_of_book(self, label: str):
        bid, ask = self.order_book.get_top_of_book()
        bid_s = f"{bid.price} x {bid.size}" if bid else "-"
        ask_s = f"{ask.price} x {ask.size}" if ask else "-"
        print(f"[{label}] seq={self.order_book.last_sequence} "
              f"bid {bid_s} | ask {ask_s}")


if __name__ == "__main__":
    async def main():
        try:
            print("Starting connection attempt...", flush=True)
            book = OrderBook(market_id=MARKET_TICKER, exchange="kalshi")
            client = KalshiWSClient(market_ticker=MARKET_TICKER, order_book=book)
            await client.connect_and_listen()
        except Exception:
            import traceback
            traceback.print_exc()

    asyncio.run(main())
