"""
detect_pair.py

Milestone 1 end-to-end demo: load the hardcoded verified pair, build a
live L2 book for BOTH venues, and report the executable cross-venue
edge after fees and depth.

Both books are built from each venue's PUBLIC market-data endpoint, so
this runs with no credentials. kalshi_ws.py is the real-time path; REST
is used here so the demo is a single deterministic shot rather than a
stream.

Polymarket fees: until the branches merge, the fee parameters are read
straight from config/pair.toml ([polymarket.fees]) and applied with
Polymarket's published formula. After merge this should call
polymarket.fees.taker_fee directly instead — see NOTE below about
argument order.

Usage:
    python detect_pair.py [--size 500] [--pair-config PATH]
"""

import argparse
import json
import tomllib
import urllib.request
from decimal import Decimal
from pathlib import Path

from models import OrderBookSnapshot, PriceLevel
from order_book import OrderBook
from kalshi_fees import fetch_fee_schedule, taker_fee as kalshi_taker_fee
from kalshi_rest_feed import fetch_orderbook as kalshi_orderbook, snapshot_from_rest
from cross_venue_spread import best_edge, describe

# Searched in order. The first path is the post-merge layout; the second
# lets this branch run standalone before the merge lands.
PAIR_CONFIG_PATHS = [
    Path("../Polymarket_engine/config/pair.toml"),
    Path("config/pair.toml"),
]
POLYMARKET_CLOB = "https://clob.polymarket.com"


def load_pair(explicit: str | None = None) -> dict:
    paths = [Path(explicit)] if explicit else PAIR_CONFIG_PATHS
    for p in paths:
        if p.is_file():
            with open(p, "rb") as f:
                cfg = tomllib.load(f)
            cfg["_path"] = str(p)
            return cfg
    raise FileNotFoundError(
        "pair.toml not found. Looked in: "
        + ", ".join(str(p) for p in paths)
        + ". Pass --pair-config, or merge the Polymarket branch."
    )


def polymarket_book(token_id: str, market_id: str) -> OrderBook:
    """
    Builds an OrderBook from Polymarket's public CLOB book endpoint.

    Polymarket returns both sides worst-price-first, but OrderBook sorts
    on insert, so the incoming order does not matter here.
    """
    url = f"{POLYMARKET_CLOB}/book?token_id={token_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "kalshi-engine/0.1"})
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = json.load(r)

    book = OrderBook(market_id=market_id, exchange="polymarket")
    book.apply_snapshot(OrderBookSnapshot(
        market_id=market_id, exchange="polymarket",
        timestamp="1970-01-01T00:00:00Z",
        bids=[PriceLevel(price=Decimal(l["price"]), size=Decimal(l["size"]))
              for l in raw.get("bids", [])],
        asks=[PriceLevel(price=Decimal(l["price"]), size=Decimal(l["size"]))
              for l in raw.get("asks", [])],
    ))
    return book


def polymarket_fee_fn(fees_cfg: dict):
    """
    Stand-in for polymarket.fees.taker_fee until the branches merge.

    Polymarket's published formula is fee = C * rate * (p * (1-p))**exp,
    rounded to 5dp, takers only. This market has feesEnabled = false, so
    in practice it returns zero — but the parameters are read from
    config rather than assumed, because other series do charge.

    NOTE: exposed as (contracts, price) to match executable_spread.FeeFn
    and kalshi_fees.taker_fee. The Polymarket module currently takes
    (price, shares) — the orders must be reconciled before this is
    swapped for the real implementation, since transposing them fails
    silently rather than raising.
    """
    enabled = bool(fees_cfg.get("enabled", False))
    rate = Decimal(str(fees_cfg.get("rate", "0")))
    exponent = Decimal(str(fees_cfg.get("exponent", "1")))

    def fee(contracts: Decimal, price: Decimal) -> Decimal:
        if not enabled or rate == 0 or contracts <= 0:
            return Decimal("0")
        raw = contracts * rate * (price * (Decimal("1") - price)) ** exponent
        return raw.quantize(Decimal("0.00001"))

    return fee


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--size", type=Decimal, default=Decimal("100"))
    p.add_argument("--pair-config")
    p.add_argument("--kalshi-ticker",
                   help="overrides [kalshi].ticker, which is still a TODO in pair.toml")
    a = p.parse_args()

    cfg = load_pair(a.pair_config)
    pair_id = cfg.get("pair_id", "unknown")
    ticker = a.kalshi_ticker or cfg.get("kalshi", {}).get("ticker", "")
    if not ticker:
        raise SystemExit(
            f"[kalshi].ticker is empty in {cfg['_path']}. "
            f"Set it there, or pass --kalshi-ticker."
        )

    verified = cfg.get("verification", {}).get("criteria_match", False)
    pm = cfg["polymarket"]

    kb = OrderBook(market_id=ticker, exchange="kalshi")
    kb.apply_snapshot(snapshot_from_rest(ticker, kalshi_orderbook(ticker)))
    pb = polymarket_book(pm["yes_token"], pm["slug"])

    sched = fetch_fee_schedule(ticker)
    k_fee = lambda c, pr: kalshi_taker_fee(c, pr, sched)
    p_fee = polymarket_fee_fn(pm.get("fees", {}))

    print(f"pair: {pair_id}   (config: {cfg['_path']})")
    if not verified:
        print("  WARNING: verification.criteria_match is false — this pair has not "
              "been manually confirmed equivalent. Numbers below are indicative only.")
    for book, fees in ((kb, f"{sched.fee_type} x{sched.multiplier}"),
                       (pb, "enabled" if pm.get("fees", {}).get("enabled") else "fee-free")):
        b, s = book.get_top_of_book()
        print(f"  {book.exchange:<11} {len(book.bids):>3}b/{len(book.asks):<3}a  "
              f"bid {b.price} x {b.size:<10} ask {s.price} x {s.size:<10}  fees: {fees}")

    print(f"\nsize {a.size}:")
    for e in best_edge(pair_id, a.size, kb, k_fee, pb, p_fee):
        print(describe(e))
