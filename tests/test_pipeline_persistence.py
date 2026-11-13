"""
tests/test_pipeline_persistence.py

Feature-6 persistence and the PortfolioManager inside the full pipeline, on
the same recorded tapes as test_pipeline_replay.py.

The arbitrage strategy trades up to a 200-contract limit while a +0.90
headline is injected, and every event family is written through
AsyncDbEventSink — the same schema and batch writer the PostgreSQL
deployment uses, here on sqlite+aiosqlite so the test stays offline.
"""

import asyncio
import contextlib
import io
from decimal import Decimal
from pathlib import Path

import pytest

from engine.db.sink import AsyncDbEventSink
from engine.pipeline import PipelineConfig, ScriptedHeadline, build_pipeline
from engine.portfolio_manager import PortfolioManager

D = Decimal
ROOT = Path(__file__).resolve().parent.parent
POLY_TAPE = ROOT / "Polymarket_engine/fixtures/tape_polymarket_netanyahu-out-2026_20261008T062810Z.jsonl"
KALSHI_TAPE = ROOT / "kalshi_engine/fixtures/tape_kalshi_KXLEADERSOUT-27JAN01-BNETISR_20261008T062814Z.jsonl"

pytestmark = pytest.mark.slow

HEADLINE = [ScriptedHeadline(after_frames=1200, score=D("0.90"),
                             text="Netanyahu expected to step down")]


async def read_run(url: str, run_id: str) -> dict[str, list[dict]]:
    reader = AsyncDbEventSink(url)
    await reader.start(mode="read")
    try:
        out = {}
        for table in ("trade_log", "market_snapshots", "spread_history",
                      "sentiment_features", "arb_opportunities", "arb_attempts"):
            out[table] = [r for r in await reader.fetch_table(table, run_only=False)
                          if r["run_id"] == run_id]
        return out
    finally:
        await reader.aclose()


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    url = f"sqlite+aiosqlite:///{(tmp_path_factory.mktemp('db') / 'events.db').as_posix()}"
    config = PipelineConfig(kalshi_fee_multiplier=D(1), max_position=D(200), db_url=url)
    pipeline = build_pipeline(config=config)
    with contextlib.redirect_stdout(io.StringIO()):
        report = asyncio.run(pipeline.run_replay(POLY_TAPE, KALSHI_TAPE, headlines=HEADLINE))
    tables = asyncio.run(read_run(url, pipeline.sink.run_id))
    yield report, pipeline, tables
    pipeline.close()


# --------------------------------------------------------------------------
# persistence
# --------------------------------------------------------------------------

def test_every_event_family_is_persisted_and_matches_the_report(run):
    report, pipeline, tables = run
    assert report.db_dropped == 0
    for table, rows_ in tables.items():
        assert len(rows_) == report.db_counts[table], table

    assert report.db_counts["arb_attempts"] == report.attempts
    fills = [r for r in tables["trade_log"] if r["kind"] == "fill"]
    orders = [r for r in tables["trade_log"] if r["kind"] == "order"]
    assert len(fills) == report.fills > 0
    expected_orders = sum(1 for a in pipeline.arbitrage.attempts
                          for s in (a.yes_state, a.no_state, *a.unwind_states) if s is not None)
    assert len(orders) == expected_orders == 2 * report.dispatched


def test_book_snapshots_cover_all_three_books_periodically_and_on_each_trade(run):
    _, _, tables = run
    snaps = tables["market_snapshots"]
    assert {(r["venue"], r["outcome"]) for r in snaps} == {
        ("kalshi", "YES"), ("polymarket", "YES"), ("polymarket", "NO")}
    assert {r["reason"] for r in snaps} == {"periodic", "event"}
    kalshi = next(r for r in snaps if r["venue"] == "kalshi")
    assert (kalshi["best_bid"], kalshi["best_ask"]) == (D("0.41"), D("0.42"))
    assert kalshi["bids"] and kalshi["asks"]


def test_spread_history_records_both_venues_and_the_best_direction(run):
    _, _, tables = run
    spreads = tables["spread_history"]
    assert spreads
    first = spreads[0]
    assert set(first["tops"]) == {"kalshi", "polymarket"}
    assert first["direction"] == "polymarket YES + kalshi NO"
    assert first["top_of_book_cost"] == D("0.97")


def test_the_headline_is_in_the_sentiment_feature_history(run):
    _, _, tables = run
    rows_ = tables["sentiment_features"]
    with_headline = [r for r in rows_ if r["latest_headline"]]
    assert with_headline and with_headline[0]["level"] == D("0.9")
    assert with_headline[0]["confidence"] == 1
    assert any(r["n_points"] == 0 for r in rows_)        # before the headline arrived


# --------------------------------------------------------------------------
# the PortfolioManager inside the pipeline
# --------------------------------------------------------------------------

def test_the_pipeline_runs_on_the_shared_portfolio_manager(run):
    report, pipeline, _ = run
    pm = pipeline.portfolio
    assert pm is PortfolioManager.instance()
    assert report.matched_pairs == 200                    # the per-market limit
    assert report.vetoed_by_risk > 0


def test_inventory_is_per_venue_but_the_arbitrage_pair_is_flat_for_the_event(run):
    _, pipeline, _ = run
    pm, pair_id = pipeline.portfolio, pipeline.pair.pair_id
    assert pm.get_inventory(pair_id, venue="polymarket").net == 200    # long YES
    assert pm.get_inventory(pair_id, venue="kalshi").net == -200       # long NO
    assert pm.get_inventory(pair_id).net == 0


def test_positions_are_marked_at_the_mid_and_no_reservation_is_left_behind(run):
    """
    200 Polymarket YES bought at 0.38 against a 0.375 mid, and 200 Kalshi NO
    at 0.59 against a 0.585 mid: half a cent under water on each leg.
    """
    _, pipeline, _ = run
    pm = pipeline.portfolio
    assert pm.unrealized_pnl() == D("-2.00")
    assert pm.reservations == {}          # every order is finished, so nothing stays reserved
