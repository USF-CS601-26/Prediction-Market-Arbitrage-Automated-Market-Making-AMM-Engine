"""
kalshi_candles.py

Fetches historical OHLC candlesticks from Kalshi's PUBLIC production REST
API and normalizes them into models.Candle.

This is price HISTORY, which answers a different question from the order
book in order_book.py. The book tells you what you can trade right now and
at what depth; candles tell you where the price has been. Backtesting and
signal work want these; live spread/arbitrage math wants the book.

Like kalshi_rest_feed.py this needs no credentials — Kalshi serves
production market data publicly.

API constraints, confirmed against the live endpoint:
  - period_interval accepts ONLY 1 (minute), 60 (hour) and 1440 (day).
    Anything else is a 400.
  - A single request may span at most 5000 candles, so longer ranges are
    chunked and stitched here.
  - In a period with no trades the price OHLC keys are ABSENT (not null),
    while yes_bid/yes_ask are still populated.

Usage:
    python kalshi_candles.py <MARKET_TICKER> [--days N] [--interval 1|60|1440]
"""

import argparse
import json
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

from models import Candle

PROD_HOST = "https://api.elections.kalshi.com"
VALID_INTERVALS = {1: "1m", 60: "1h", 1440: "1d"}
MAX_CANDLES_PER_REQUEST = 5000


def series_ticker(market_ticker: str) -> str:
    """
    Derives the series ticker the candlesticks path needs from a market
    ticker: "KXLEADERSOUT-27JAN01-BNETISR" -> "KXLEADERSOUT".
    """
    return market_ticker.split("-")[0]


def _dec(d: dict | None, key: str) -> Decimal | None:
    """
    Reads one fixed-point string field. Returns None when the key is
    missing, which is how Kalshi reports "no trades in this period" —
    it omits the OHLC keys rather than sending nulls.
    """
    if not d or key not in d or d[key] is None:
        return None
    return Decimal(str(d[key]))


def _to_candle(raw: dict, ticker: str, interval: int) -> Candle:
    price = raw.get("price") or {}
    bid = raw.get("yes_bid") or {}
    ask = raw.get("yes_ask") or {}
    return Candle(
        market_id=ticker,
        exchange="kalshi",
        end_ts=datetime.fromtimestamp(raw["end_period_ts"], tz=timezone.utc),
        interval_minutes=interval,
        open=_dec(price, "open_dollars"),
        high=_dec(price, "high_dollars"),
        low=_dec(price, "low_dollars"),
        close=_dec(price, "close_dollars"),
        mean=_dec(price, "mean_dollars"),
        previous=_dec(price, "previous_dollars"),
        yes_bid_open=_dec(bid, "open_dollars"),
        yes_bid_close=_dec(bid, "close_dollars"),
        yes_ask_open=_dec(ask, "open_dollars"),
        yes_ask_close=_dec(ask, "close_dollars"),
        volume=_dec(raw, "volume_fp") or Decimal(0),
        open_interest=_dec(raw, "open_interest_fp") or Decimal(0),
    )


def _fetch_window(ticker: str, start_ts: int, end_ts: int, interval: int) -> list[dict]:
    url = (f"{PROD_HOST}/trade-api/v2/series/{series_ticker(ticker)}"
           f"/markets/{ticker}/candlesticks"
           f"?start_ts={start_ts}&end_ts={end_ts}&period_interval={interval}")
    req = urllib.request.Request(url, headers={"User-Agent": "kalshi-engine/0.1"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r).get("candlesticks", [])


def fetch_candles(ticker: str, days: float = 7, interval: int = 60,
                  end_ts: int | None = None) -> list[Candle]:
    """
    Fetches `days` of history at `interval` minutes, newest last.

    Ranges wider than the API's 5000-candle ceiling are split into
    successive windows and stitched back together, de-duplicated on
    end_period_ts in case the window edges overlap.
    """
    if interval not in VALID_INTERVALS:
        raise ValueError(
            f"interval must be one of {sorted(VALID_INTERVALS)} "
            f"({', '.join(VALID_INTERVALS.values())}); got {interval}"
        )

    end_ts = end_ts or int(time.time())
    start_ts = int(end_ts - days * 86400)
    # stay a little under the ceiling so an off-by-one on period
    # boundaries can't tip a request over it
    window = int(interval * 60 * (MAX_CANDLES_PER_REQUEST - 10))

    raw_by_ts: dict[int, dict] = {}
    cursor = start_ts
    while cursor < end_ts:
        chunk_end = min(cursor + window, end_ts)
        for c in _fetch_window(ticker, cursor, chunk_end, interval):
            raw_by_ts[c["end_period_ts"]] = c
        cursor = chunk_end

    return [_to_candle(raw_by_ts[ts], ticker, interval) for ts in sorted(raw_by_ts)]


def _fmt(d: Decimal | None) -> str:
    return f"{'-':>8}" if d is None else f"{d:>8}"


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("ticker")
    p.add_argument("--days", type=float, default=7)
    p.add_argument("--interval", type=int, default=60,
                   choices=sorted(VALID_INTERVALS), help="period in minutes")
    p.add_argument("--limit", type=int, default=20, help="rows to print")
    a = p.parse_args()

    candles = fetch_candles(a.ticker, a.days, a.interval)
    traded = [c for c in candles if c.volume > 0]
    print(f"{a.ticker}  —  {len(candles)} x {VALID_INTERVALS[a.interval]} candles "
          f"over {a.days}d ({len(traded)} with trades)\n")
    print(f"  {'period end (UTC)':<18}{'open':>8}{'high':>8}{'low':>8}{'close':>8}"
          f"{'bid':>8}{'ask':>8}{'volume':>12}")
    for c in candles[-a.limit:]:
        print(f"  {c.end_ts.strftime('%Y-%m-%d %H:%M'):<18}"
              f"{_fmt(c.open)}{_fmt(c.high)}{_fmt(c.low)}{_fmt(c.close)}"
              f"{_fmt(c.yes_bid_close)}{_fmt(c.yes_ask_close)}{c.volume:>12}")
    if traded:
        lo = min(c.low for c in traded); hi = max(c.high for c in traded)
        print(f"\n  traded range {lo} - {hi} | total volume "
              f"{sum(c.volume for c in candles)} | OI {candles[-1].open_interest}")
