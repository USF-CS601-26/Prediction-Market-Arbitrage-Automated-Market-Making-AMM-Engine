"""
kalshi_rest_feed.py

Polls Kalshi's PUBLIC production REST orderbook endpoint and replays each
response into an OrderBook, as an alternative source to kalshi_ws.py.

Why this exists: Kalshi's production WebSocket requires production API
credentials, and the demo environment the WS client points at has zero
quoted liquidity (every demo market returns an empty book). This module
needs no credentials at all — production market data is public — so it's
the only way to exercise the book against real depth today.

Trade-off vs. the WebSocket: REST returns full snapshots only, never
incremental deltas, so there's no sequence number and no gap detection.
Each poll is an authoritative state replace. For real-time work, generate
production API keys and point kalshi_ws.py at the production host instead.

Usage:
    python kalshi_rest_feed.py <MARKET_TICKER> [--interval SECONDS] [--once]
"""

import argparse
import json
import time
import urllib.request
from datetime import datetime, timezone

from order_book import OrderBook
from kalshi_ws import _kalshi_snapshot_to_model

PROD_HOST = "https://api.elections.kalshi.com"


def fetch_orderbook(ticker: str) -> dict:
    """
    Fetches one market's full orderbook. No authentication: Kalshi serves
    production market data publicly. Returns the raw JSON body, which
    carries orderbook_fp.{yes,no}_dollars as [price, count] string pairs.
    """
    url = f"{PROD_HOST}/trade-api/v2/markets/{ticker}/orderbook"
    req = urllib.request.Request(url, headers={"User-Agent": "kalshi-engine/0.1"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def snapshot_from_rest(ticker: str, body: dict):
    """
    Adapts a REST orderbook body into our OrderBookSnapshot.

    The REST shape (orderbook_fp.yes_dollars / no_dollars) is one of the
    spellings _kalshi_snapshot_to_model already accepts, so the yes/no ->
    bid/ask translation stays in exactly one place rather than being
    reimplemented here. REST omits the ticker from the body and has no
    sequence number, so both are supplied by the caller side.
    """
    msg = {"market_ticker": ticker, **body}
    envelope = {"sending_ts_ms": int(time.time() * 1000)}
    return _kalshi_snapshot_to_model(msg, envelope)


def poll(ticker: str, interval: float, once: bool = False):
    book = OrderBook(market_id=ticker, exchange="kalshi")

    while True:
        snapshot = snapshot_from_rest(ticker, fetch_orderbook(ticker))
        book.apply_snapshot(snapshot)

        bid, ask = book.get_top_of_book()
        ts = datetime.now(tz=timezone.utc).strftime("%H:%M:%S")
        bid_s = f"{bid.price} x {bid.size}" if bid else "-"
        ask_s = f"{ask.price} x {ask.size}" if ask else "-"
        spread = f"{ask.price - bid.price}" if bid and ask else "-"
        print(f"[{ts}] {len(book.bids)} bid lvls / {len(book.asks)} ask lvls | "
              f"bid {bid_s} | ask {ask_s} | spread {spread}")

        if once:
            return book
        time.sleep(interval)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("ticker")
    p.add_argument("--interval", type=float, default=2.0,
                   help="seconds between polls (default: 2)")
    p.add_argument("--once", action="store_true", help="fetch a single snapshot and exit")
    a = p.parse_args()
    try:
        poll(a.ticker, a.interval, a.once)
    except KeyboardInterrupt:
        print("\nstopped")
