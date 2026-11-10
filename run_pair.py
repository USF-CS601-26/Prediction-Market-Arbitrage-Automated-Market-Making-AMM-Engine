"""
run_pair.py

Milestone 1 combined entry point: streams BOTH venues for the hardcoded
pair and recomputes the cross-venue edge whenever either order book changes.

    Polymarket  Polymarket_engine/   PolymarketWsClient + PolymarketFeed (YES book)
    Kalshi      kalshi_engine/       KalshiWSClient (needs kalshi_engine/.env)
    Edge        kalshi_engine/       cross_venue_spread.best_edge: walks real
                                     depth on both books and charges both venues' fees

Usage (from the repo root):
    python run_pair.py                        live: print the edge as the books change
    python run_pair.py --record               live, and record both raw tapes
    python run_pair.py --replay PM_TAPE KS_TAPE
                                              offline: replay a Polymarket tape and a
                                              Kalshi tape merged on receive time
Options:
    --size N                   contracts per leg (default 100)
    --kalshi-fee-multiplier M  skip the fee-schedule lookup (makes replay network-free)

A result line is printed only when something visible changes (either top of
book, or the edge), so a quiet market stays quiet. Ctrl+C prints a summary.
"""

import argparse
import asyncio
import heapq
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent
KALSHI_DIR = ROOT / "kalshi_engine"
POLY_DIR = ROOT / "Polymarket_engine"
sys.path[:0] = [str(KALSHI_DIR), str(POLY_DIR)]

from cross_venue_spread import best_edge                                   # noqa: E402
from kalshi_fees import fetch_fee_schedule, series_ticker                  # noqa: E402
from kalshi_fees import taker_fee as kalshi_taker_fee                      # noqa: E402
from kalshi_ws import KalshiWSClient                                        # noqa: E402
from models import CrossVenueEdge                                           # noqa: E402
from models import FeeSchedule as KalshiFeeSchedule                         # noqa: E402
from order_book import OrderBook                                            # noqa: E402
import tape as kalshi_tape                                                  # noqa: E402
from polymarket import recorder as poly_tape                                # noqa: E402
from polymarket.config import PairConfig, load_pair_config                  # noqa: E402
from polymarket.feed import PolymarketFeed                                  # noqa: E402
from polymarket.fees import fee_fn as poly_fee_fn                           # noqa: E402
from polymarket.ws_client import PolymarketWsClient                         # noqa: E402

HEARTBEAT_SECONDS = 30


class QuietKalshiWSClient(KalshiWSClient):
    """
    KalshiWSClient without its per-message top-of-book printing (run_pair
    prints its own combined line). With offline=True, a sequence gap during a
    replay is reported instead of asking a non-existent socket for a snapshot;
    the book then stays stale until the tape's next snapshot, as it would live.
    """

    def __init__(self, *args, offline: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.offline = offline
        self.sequence_gaps = 0

    def _print_top_of_book(self, label: str) -> None:
        pass

    async def _request_snapshot(self):
        self.sequence_gaps += 1
        if self.offline:
            print("  (replay) Kalshi sequence gap: book stale until the next snapshot in the tape")
            return
        await super()._request_snapshot()


@dataclass
class Sample:
    ts: datetime
    edges: list[CrossVenueEdge]       # both directions, best first


class PairEngine:
    """
    Decides when the two books are usable, evaluates the edge when either one
    changed, prints changes, and keeps the numbers for the summary.
    """

    def __init__(self, pair_id: str, size: Decimal, kalshi: KalshiWSClient,
                 poly_feed: PolymarketFeed, k_fee, p_fee, keep_samples: bool = False):
        self.pair_id = pair_id
        self.size = size
        self.kalshi = kalshi
        self.kalshi_book = kalshi.order_book
        self.poly_feed = poly_feed
        self.k_fee = k_fee
        self.p_fee = p_fee
        self.kalshi_version = 0          # bumped by KalshiWSClient(on_update=...)
        self._seen = None                # versions at the last evaluation
        self._last_key = None            # what was last printed
        self._last_print = time.monotonic()
        self._last_wait_reason = None
        self.evaluations = 0
        self.executable_positive = 0
        self.best: tuple[CrossVenueEdge, datetime] | None = None
        # every evaluation, for tests; off in live mode so memory stays flat
        self.keep_samples = keep_samples
        self.samples: list[Sample] = []

    # wired as KalshiWSClient(on_update=...)
    def kalshi_changed(self, _client) -> None:
        self.kalshi_version += 1

    def _poly_version(self) -> int:
        return self.poly_feed.stats["snapshots"] + self.poly_feed.stats["deltas"]

    def not_ready_reason(self) -> str | None:
        if not self.kalshi.book_is_fresh:
            return "waiting for a fresh Kalshi book"
        if not self.poly_feed.is_valid("YES"):
            return "waiting for a valid Polymarket book"
        for name, book in (("Kalshi", self.kalshi_book), ("Polymarket", self.poly_feed.book("YES"))):
            bid, ask = book.get_top_of_book()
            if bid is None or ask is None:
                return f"{name} book has an empty side"
            if bid.price >= ask.price:
                # e.g. mid-way through a trade reported as several messages
                return f"{name} book is momentarily crossed"
        return None

    def evaluate_if_changed(self, ts: datetime) -> Sample | None:
        versions = (self.kalshi_version, self._poly_version())
        if versions == self._seen:
            return None
        self._seen = versions

        reason = self.not_ready_reason()
        if reason is not None:
            if reason != self._last_wait_reason:
                print(f"{ts:%H:%M:%S}  {reason}")
                self._last_wait_reason = reason
            return None
        self._last_wait_reason = None

        edges = best_edge(self.pair_id, self.size,
                          self.kalshi_book, self.k_fee,
                          self.poly_feed.book("YES"), self.p_fee)
        sample = Sample(ts=ts, edges=edges)
        self._record(sample)
        self._print_if_changed(sample)
        return sample

    def _record(self, s: Sample) -> None:
        self.evaluations += 1
        if self.keep_samples:
            self.samples.append(s)
        top = s.edges[0]
        if top.executable and top.net_edge > 0:
            self.executable_positive += 1
        if self.best is None or top.net_edge > self.best[0].net_edge:
            self.best = (top, s.ts)

    def _print_if_changed(self, s: Sample) -> None:
        kb, ka = self.kalshi_book.get_top_of_book()
        pb, pa = self.poly_feed.book("YES").get_top_of_book()
        # exactly what the line shows: top-of-book prices and the two net edges
        key = (kb.price, ka.price, pb.price, pa.price,
               tuple((e.buy_venue, e.net_edge, e.executable) for e in s.edges))
        if key == self._last_key:
            return
        self._last_key = key
        self._last_print = time.monotonic()
        print(f"{s.ts:%H:%M:%S}  Kalshi {kb.price:.2f}/{ka.price:.2f}  "
              f"Poly {pb.price:.2f}/{pa.price:.2f} | " + " | ".join(_fmt_edge(e) for e in s.edges))

    def heartbeat(self) -> None:
        if time.monotonic() - self._last_print >= HEARTBEAT_SECONDS:
            print(f"{datetime.now(timezone.utc):%H:%M:%S}  no change "
                  f"({self.evaluations} evaluations so far)")
            self._last_print = time.monotonic()

    async def watch(self, poll_seconds: float = 0.2) -> None:
        """Live mode: re-evaluate shortly after either book changes."""
        while True:
            await asyncio.sleep(poll_seconds)
            self.evaluate_if_changed(datetime.now(timezone.utc))
            self.heartbeat()

    def print_summary(self) -> None:
        print("\n=== summary ===")
        print(f"pair {self.pair_id}, {self.size} contracts per leg")
        print(f"edge evaluations: {self.evaluations}")
        print(f"executable positive net edges: {self.executable_positive}")
        if self.best is not None:
            e, ts = self.best
            print(f"best net edge: {_fmt_edge(e)} at {ts:%H:%M:%S} UTC")


def _fmt_edge(e: CrossVenueEdge) -> str:
    sign = "+" if e.net_edge > 0 else ""
    per = f" ({sign}{e.net_per_contract * 100:.2f}c/ct)" if e.net_per_contract is not None else ""
    flag = "" if e.executable else " PARTIAL"
    return f"buy {e.buy_venue}->sell {e.sell_venue} net {sign}{e.net_edge:.4f}{per}{flag}"


def fee_functions(pair: PairConfig, ticker: str, kalshi_multiplier: Decimal | None):
    """Both fee functions in executable_spread's (contracts, price) order."""
    if kalshi_multiplier is None:
        schedule = fetch_fee_schedule(ticker)
    else:
        schedule = KalshiFeeSchedule(venue="kalshi", series=series_ticker(ticker),
                                     fee_type="quadratic", multiplier=kalshi_multiplier)
    print(f"fees: Kalshi {schedule.fee_type} x{schedule.multiplier}, "
          f"Polymarket {'rate ' + str(pair.polymarket.fees.rate) if pair.polymarket.fees.enabled else 'off'}")

    def k_fee(contracts: Decimal, price: Decimal) -> Decimal:
        return kalshi_taker_fee(contracts, price, schedule)

    return k_fee, poly_fee_fn(pair.polymarket.fees)


async def run_live(pair: PairConfig, ticker: str, size: Decimal, k_fee, p_fee, record: bool) -> None:
    k_rec = p_rec = None
    if record:
        k_rec = kalshi_tape.TapeRecorder(kalshi_tape.default_tape_path(ticker, KALSHI_DIR / "fixtures"))
        p_rec = poly_tape.TapeRecorder(poly_tape.default_tape_path(pair.pair_id, POLY_DIR / "fixtures"))
        print(f"recording Kalshi to {k_rec.path}")
        print(f"recording Polymarket to {p_rec.path}")

    queue: asyncio.Queue = asyncio.Queue()
    poly_client = PolymarketWsClient(pair.asset_ids(), queue, on_raw=p_rec.write if p_rec else None)
    poly_feed = PolymarketFeed(pair, queue)
    kalshi = QuietKalshiWSClient(ticker, OrderBook(market_id=ticker, exchange="kalshi"),
                                 on_raw=k_rec.write if k_rec else None)
    engine = PairEngine(pair.pair_id, size, kalshi, poly_feed, k_fee, p_fee)
    kalshi.on_update = engine.kalshi_changed

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(kalshi.run_forever())
            tg.create_task(poly_client.run())
            tg.create_task(poly_feed.run())
            tg.create_task(engine.watch())
    finally:
        engine.print_summary()
        for rec in (k_rec, p_rec):
            if rec is not None:
                rec.close()
                print(f"wrote {rec.frames_written} frames to {rec.path}")


async def run_replay(pair: PairConfig, ticker: str, size: Decimal, k_fee, p_fee,
                     poly_tape_path: str, kalshi_tape_path: str,
                     keep_samples: bool = False) -> PairEngine:
    poly_feed = PolymarketFeed(pair, queue=None)
    kalshi = QuietKalshiWSClient(ticker, OrderBook(market_id=ticker, exchange="kalshi"), offline=True)
    kalshi.connected = True          # a replayed session counts as connected
    engine = PairEngine(pair.pair_id, size, kalshi, poly_feed, k_fee, p_fee, keep_samples)
    kalshi.on_update = engine.kalshi_changed

    # one timeline, ordered by local receive time; Polymarket first on a tie
    timeline = heapq.merge(
        ((f.recv_ts_ns, 0, f) for f in poly_tape.load_tape(poly_tape_path)),
        ((f.recv_ts_ns, 1, f) for f in kalshi_tape.load_tape(kalshi_tape_path)),
        key=lambda item: (item[0], item[1]),
    )
    frames = 0
    for recv_ts_ns, venue, frame in timeline:
        if venue == 0:
            poly_feed.handle(frame)
        else:
            await kalshi._handle_message(json.loads(frame.text))
        frames += 1
        engine.evaluate_if_changed(datetime.fromtimestamp(recv_ts_ns / 1e9, tz=timezone.utc))
    poly_feed.settle()
    engine.frames_replayed = frames

    print(f"\nreplayed {frames} frames "
          f"(Polymarket checks failed: {poly_feed.check_failures()})")
    engine.print_summary()
    return engine


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pair-config", default=str(POLY_DIR / "config" / "pair.toml"))
    p.add_argument("--size", type=Decimal, default=Decimal("100"))
    p.add_argument("--record", action="store_true", help="live mode: also record both raw tapes")
    p.add_argument("--replay", nargs=2, metavar=("POLYMARKET_TAPE", "KALSHI_TAPE"))
    p.add_argument("--kalshi-ticker", help="overrides [kalshi].ticker in the pair config")
    p.add_argument("--kalshi-fee-multiplier", type=Decimal,
                   help="use this Kalshi fee multiplier instead of looking it up")
    a = p.parse_args()

    pair = load_pair_config(a.pair_config)
    ticker = a.kalshi_ticker or pair.kalshi.ticker
    if not ticker:
        sys.exit("No Kalshi ticker: set [kalshi].ticker in the pair config or pass --kalshi-ticker.")
    print(f"pair {pair.pair_id}: Polymarket {pair.polymarket.slug} / Kalshi {ticker}")
    if not pair.verification.criteria_match:
        print("WARNING: verification.criteria_match is false; the pair is not confirmed equivalent.")

    k_fee, p_fee = fee_functions(pair, ticker, a.kalshi_fee_multiplier)
    try:
        if a.replay:
            asyncio.run(run_replay(pair, ticker, a.size, k_fee, p_fee, *a.replay))
        else:
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
            asyncio.run(run_live(pair, ticker, a.size, k_fee, p_fee, a.record))
    except KeyboardInterrupt:
        pass
    except BaseExceptionGroup as eg:
        # a task in the live TaskGroup failed for good, e.g. Kalshi rejected the credentials
        for e in eg.exceptions:
            print(f"stopped: {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
