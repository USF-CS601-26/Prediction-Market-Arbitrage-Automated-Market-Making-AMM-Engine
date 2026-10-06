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

Two modes:
    one-shot   a single sample, for checking the math
    --watch    poll both venues on an interval and log a time series,
               which is how you find out whether a pair actually moves
               and whether an edge persists or is a momentary artifact

Usage:
    python detect_pair.py [--size 500]
    python detect_pair.py --watch --duration 1800 --interval 10 --out run.csv
"""

import argparse
import csv
import json
import sys
import time
import tomllib
import urllib.error
import urllib.request
from datetime import datetime, timezone
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


def sample(ticker: str, pm: dict, pair_id: str, size: Decimal,
           k_fee, p_fee) -> tuple[OrderBook, OrderBook, list]:
    """One synchronized read of both venues plus the resulting edges."""
    kb = OrderBook(market_id=ticker, exchange="kalshi")
    kb.apply_snapshot(snapshot_from_rest(ticker, kalshi_orderbook(ticker)))
    pb = polymarket_book(pm["yes_token"], pm["slug"])
    return kb, pb, best_edge(pair_id, size, kb, k_fee, pb, p_fee)


CSV_COLUMNS = [
    "ts_utc", "kalshi_bid", "kalshi_ask", "poly_bid", "poly_ask",
    "size", "buy_venue", "sell_venue", "gross_edge", "fees",
    "net_edge", "net_per_contract", "executable",
]


def row_for(e, kb: OrderBook, pb: OrderBook) -> dict:
    kbid, kask = kb.get_top_of_book()
    pbid, pask = pb.get_top_of_book()
    return {
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "kalshi_bid": kbid.price if kbid else "", "kalshi_ask": kask.price if kask else "",
        "poly_bid": pbid.price if pbid else "", "poly_ask": pask.price if pask else "",
        "size": e.size, "buy_venue": e.buy_venue, "sell_venue": e.sell_venue,
        "gross_edge": e.gross_edge, "fees": e.total_fees, "net_edge": e.net_edge,
        "net_per_contract": e.net_per_contract if e.net_per_contract is not None else "",
        "executable": e.executable,
    }


def watch(ticker, pm, pair_id, size, k_fee, p_fee,
          duration: float, interval: float, out: str | None):
    """
    Polls both venues until `duration` elapses, printing one line per
    sample and optionally writing a CSV for charting.

    A failed poll is logged and skipped rather than ending the run — a
    30-minute session should survive a transient network blip, and a
    gap in the series is far better than losing the whole recording.
    """
    writer = None
    fh = None
    if out:
        fh = open(out, "w", newline="")
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()

    deadline = time.monotonic() + duration
    rows, errors, n = [], 0, 0
    print(f"watching for {duration/60:.0f} min, every {interval:.0f}s"
          f"{f', logging to {out}' if out else ''}\n")
    print(f"  {'time':<10}{'kalshi':>14}{'polymarket':>14}"
          f"{'direction':>14}{'net':>11}{'per ct':>9}")

    try:
        while time.monotonic() < deadline:
            tick = time.monotonic()
            try:
                # one quick retry: a single DNS/TCP blip shouldn't cost a
                # whole sample on an unattended 30-minute run
                try:
                    kb, pb, edges = sample(ticker, pm, pair_id, size, k_fee, p_fee)
                except (urllib.error.URLError, urllib.error.HTTPError, OSError):
                    time.sleep(1)
                    kb, pb, edges = sample(ticker, pm, pair_id, size, k_fee, p_fee)
                e = edges[0]
                r = row_for(e, kb, pb)
                rows.append(r)
                n += 1
                if writer:
                    writer.writerow(r)
                    fh.flush()      # survive a Ctrl+C mid-run
                direction = f"{e.buy_venue[:4]}->{e.sell_venue[:4]}"
                mark = "" if e.executable else " (thin)"
                per = (f"{e.net_per_contract * 100:+.2f}c"
                       if e.net_per_contract is not None else "-")
                kq = f"{r['kalshi_bid']}/{r['kalshi_ask']}"
                pq = f"{r['poly_bid']}/{r['poly_ask']}"
                print(f"  {r['ts_utc'][11:19]:<10}{kq:>14}{pq:>14}"
                      f"{direction:>14}{e.net_edge:>+11.4f}{per:>9}{mark}")
            except (urllib.error.URLError, urllib.error.HTTPError, OSError) as err:
                errors += 1
                print(f"  poll failed ({type(err).__name__}: {err}) — continuing")

            slack = interval - (time.monotonic() - tick)
            if slack > 0:
                time.sleep(min(slack, max(0.0, deadline - time.monotonic())))
    except KeyboardInterrupt:
        print("\n  stopped early")
    finally:
        if fh:
            fh.close()

    if not rows:
        print("\nno successful samples")
        return

    nets = [Decimal(str(r["net_edge"])) for r in rows]
    execs = sum(1 for r in rows if r["executable"])
    positive = sum(1 for v in nets if v > 0)
    moved = len({(r["kalshi_bid"], r["kalshi_ask"], r["poly_bid"], r["poly_ask"])
                 for r in rows})
    print(f"\n  {n} samples ({errors} failed)")
    print(f"  net edge   min {min(nets):+.4f}   max {max(nets):+.4f}   "
          f"mean {sum(nets)/len(nets):+.4f}")
    print(f"  positive   {positive}/{n} samples")
    print(f"  executable {execs}/{n} samples at size {size}")
    print(f"  distinct top-of-book states seen: {moved}"
          + ("   <- pair never moved; a longer window or a busier pair "
             "would make a better fixture" if moved <= 1 else ""))
    if out:
        print(f"  wrote {out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--size", type=Decimal, default=Decimal("100"))
    p.add_argument("--pair-config")
    p.add_argument("--kalshi-ticker",
                   help="overrides [kalshi].ticker, which is still a TODO in pair.toml")
    p.add_argument("--watch", action="store_true",
                   help="poll repeatedly instead of sampling once")
    p.add_argument("--duration", type=float, default=1800,
                   help="seconds to watch (default 1800 = 30 min)")
    p.add_argument("--interval", type=float, default=10,
                   help="seconds between polls (default 10)")
    p.add_argument("--out", help="write a CSV time series to this path")
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

    sched = fetch_fee_schedule(ticker)
    k_fee = lambda c, pr: kalshi_taker_fee(c, pr, sched)
    p_fee = polymarket_fee_fn(pm.get("fees", {}))

    print(f"pair: {pair_id}   (config: {cfg['_path']})")
    if not verified:
        print("  WARNING: verification.criteria_match is false — this pair has not "
              "been manually confirmed equivalent. Numbers below are indicative only.")

    if a.watch:
        watch(ticker, pm, pair_id, a.size, k_fee, p_fee,
              a.duration, a.interval, a.out)
    else:
        kb, pb, edges = sample(ticker, pm, pair_id, a.size, k_fee, p_fee)
        for book, fees in ((kb, f"{sched.fee_type} x{sched.multiplier}"),
                           (pb, "enabled" if pm.get("fees", {}).get("enabled")
                            else "fee-free")):
            b, s2 = book.get_top_of_book()
            print(f"  {book.exchange:<11} {len(book.bids):>3}b/{len(book.asks):<3}a  "
                  f"bid {b.price} x {b.size:<10} ask {s2.price} x {s2.size:<10}  fees: {fees}")
        print(f"\nsize {a.size}:")
        for e in edges:
            print(describe(e))
