"""
tests/test_pair_replay.py

Recorded-tape test from the proposal: "a little segment of raw WebSocket
information captured from both venues, which tests whether the project can
survive real data."

The two tapes below were recorded at the same time on 2026-10-08 (about an
hour, starting 06:28 UTC): Polymarket with `run_polymarket.py --record`,
Kalshi with `kalshi_ws.py --record`. run_pair.run_replay merges them on
receive time and feeds every frame through the same code the live run uses
(PolymarketFeed, KalshiWSClient's message handler, best_edge), so this checks
the whole pipeline on real data.

Fully offline: no network, no credentials. The Kalshi fee multiplier is fixed
at 1, the value the KXLEADERSOUT series had when the tapes were recorded.

Run from the repo root:  pytest tests
"""

import asyncio
import contextlib
import io
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import run_pair  # noqa: E402  (also puts both engines on the import path)

POLY_TAPE = ROOT / "Polymarket_engine/fixtures/tape_polymarket_netanyahu-out-2026_20261008T062810Z.jsonl"
KALSHI_TAPE = ROOT / "kalshi_engine/fixtures/tape_kalshi_KXLEADERSOUT-27JAN01-BNETISR_20261008T062814Z.jsonl"
SIZE = Decimal("100")


def replay() -> run_pair.PairEngine:
    pair = run_pair.load_pair_config(run_pair.POLY_DIR / "config" / "pair.toml")
    with contextlib.redirect_stdout(io.StringIO()):     # keep the test output readable
        k_fee, p_fee = run_pair.fee_functions(pair, pair.kalshi.ticker, Decimal("1"))
        return asyncio.run(run_pair.run_replay(
            pair, pair.kalshi.ticker, SIZE, k_fee, p_fee,
            str(POLY_TAPE), str(KALSHI_TAPE), keep_samples=True))


def frame_count(path: Path) -> int:
    with open(path, encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def first_kalshi_snapshot_time() -> datetime:
    with open(KALSHI_TAPE, encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            if json.loads(row["text"]).get("type") == "orderbook_snapshot":
                return datetime.fromtimestamp(row["recv_ts_ns"] / 1e9, tz=timezone.utc)
    raise AssertionError("no Kalshi snapshot in the tape")


@pytest.fixture(scope="module")
def engine() -> run_pair.PairEngine:
    return replay()


def summarize(e: run_pair.PairEngine) -> list:
    return [(s.ts, [(x.buy_venue, x.net_edge, x.executable) for x in s.edges]) for s in e.samples]


# 1. survives real data

def test_whole_tape_replays(engine):
    assert engine.frames_replayed == frame_count(POLY_TAPE) + frame_count(KALSHI_TAPE)
    assert engine.evaluations > 0
    assert len(engine.samples) == engine.evaluations


# 2. both order books stay consistent

def test_polymarket_checks_never_fail(engine):
    assert engine.poly_feed.check_failures() == 0


def test_kalshi_sequence_never_gaps(engine):
    # the two reconnects in this tape start a new subscription with a fresh
    # snapshot at seq=1; that is a reset, not a gap
    assert engine.kalshi.sequence_gaps == 0


# 3. no edge from a book that isn't ready

def test_no_edge_before_both_books_are_ready(engine):
    # the Polymarket snapshot arrives first; the edge can only start once the
    # Kalshi snapshot is in as well
    kalshi_ready = first_kalshi_snapshot_time()
    assert engine.samples[0].ts == kalshi_ready
    assert all(s.ts >= kalshi_ready for s in engine.samples)


# 4. reproducible

def test_replay_is_deterministic(engine):
    assert summarize(replay()) == summarize(engine)


# 5. the numbers are right: first evaluation, checked by hand

def test_first_edge_matches_hand_calculation(engine):
    # Books at 06:28:14: Kalshi YES 0.41 x 5.64 / 0.42 x 136.38 (next bid 0.40),
    # Polymarket YES 0.37 / 0.38. Polymarket fees are off for this market;
    # Kalshi taker fee = ceil_to_cent(0.07 * C * P * (1 - P)) per price level.
    best, other = engine.samples[0].edges

    # Buy 100 on Polymarket, sell 100 on Kalshi
    #   buy : 100 @ 0.38 = 38.00, fee 0
    #   sell: 5.64 @ 0.41 = 2.3124, fee ceil(0.07 * 5.64 * 0.41 * 0.59 = 0.0955) = 0.10
    #         94.36 @ 0.40 = 37.744, fee ceil(0.07 * 94.36 * 0.40 * 0.60 = 1.5852) = 1.59
    #   net = (2.3124 + 37.744 - 0.10 - 1.59) - 38.00 = +0.3664
    assert (best.buy_venue, best.sell_venue) == ("polymarket", "kalshi")
    assert [(f.size, f.price, f.fee) for f in best.buy.fills] == [(Decimal("100"), Decimal("0.38"), 0)]
    assert [(f.size, f.price, f.fee) for f in best.sell.fills] == [
        (Decimal("5.64"), Decimal("0.41"), Decimal("0.10")),
        (Decimal("94.36"), Decimal("0.40"), Decimal("1.59")),
    ]
    assert best.net_edge == Decimal("0.3664")
    assert best.executable

    # Buy 100 on Kalshi, sell 100 on Polymarket
    #   buy : 100 @ 0.42 = 42.00, fee ceil(0.07 * 100 * 0.42 * 0.58 = 1.7052) = 1.71
    #   sell: 100 @ 0.37 = 37.00, fee 0
    #   net = 37.00 - (42.00 + 1.71) = -6.71
    assert (other.buy_venue, other.sell_venue) == ("kalshi", "polymarket")
    assert other.net_edge == Decimal("-6.71")
