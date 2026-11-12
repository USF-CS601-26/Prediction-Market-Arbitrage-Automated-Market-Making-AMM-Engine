"""
tests/test_amm_pipeline.py

The AMM, the PortfolioManager and feature-6 persistence inside the full
pipeline, on the same recorded tapes as test_pipeline_replay.py.

This is the proposal's "quote skewing on a high-sentiment headline" use
case with the real AMMStrategy rather than a stand-in: a +0.90 headline is
injected into an hour in which neither book moves, so every change in the
AMM's reservation price is the sentiment term and nothing else.

Offline: the database is sqlite+aiosqlite through the same AsyncDbEventSink
and schema the PostgreSQL deployment uses.
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


def replay(config: PipelineConfig, headlines=None):
    pipeline = build_pipeline(config=config)
    with contextlib.redirect_stdout(io.StringIO()):
        report = asyncio.run(pipeline.run_replay(POLY_TAPE, KALSHI_TAPE, headlines=headlines))
    return report, pipeline


async def read_run(url: str, run_id: str) -> dict[str, list[dict]]:
    reader = AsyncDbEventSink(url)
    await reader.start(mode="read")
    try:
        out = {}
        for table in ("trade_log", "market_snapshots", "spread_history",
                      "sentiment_features", "amm_quotes", "arb_opportunities"):
            out[table] = [r for r in await reader.fetch_table(table, run_only=False)
                          if r["run_id"] == run_id]
        return out
    finally:
        await reader.aclose()


@pytest.fixture(scope="module")
def skew_run(tmp_path_factory):
    url = f"sqlite+aiosqlite:///{(tmp_path_factory.mktemp('db') / 'amm.db').as_posix()}"
    config = PipelineConfig(kalshi_fee_multiplier=D(1), dry_run=True,
                            amm_venues=("kalshi", "polymarket"), db_url=url)
    report, pipeline = replay(config, HEADLINE)
    tables = asyncio.run(read_run(url, pipeline.sink.run_id))
    yield report, pipeline, tables
    pipeline.close()


def test_the_real_amm_skews_its_quotes_on_a_high_sentiment_headline(skew_run):
    """
    USE CASE: "quote skewing on a high-sentiment headline", with the AMM.

    Before the headline the reservation price is the mid (flat inventory, no
    sentiment). After it, r = mid + k x S x confidence = mid + 0.02 x 0.9 x 1.
    When the headline ages out of the 5-minute window the AMM requotes back.
    """
    report, pipeline, _ = skew_run
    kalshi = next(a for a in pipeline.amms if a.venue == "kalshi")
    quotes = [q for q in kalshi.quotes if q.action == "requote"]
    assert len(quotes) >= 3

    first, skewed, last = quotes[0], quotes[1], quotes[-1]
    assert first.mid == D("0.415") and first.reservation == first.mid
    assert skewed.sentiment == D("0.9") and skewed.confidence == 1
    assert skewed.reservation - skewed.mid == D("0.018")
    assert skewed.bid > first.bid and skewed.ask > first.ask
    assert last.reservation == last.mid, "the skew should decay once the headline ages out"
    assert report.headlines_injected == 1


def test_both_venues_amms_quote_inside_the_bounds_without_crossing(skew_run):
    _, pipeline, _ = skew_run
    for amm in pipeline.amms:
        for q in amm.quotes:
            if q.action != "requote":
                continue
            assert D("0.01") <= q.bid < q.ask <= D("0.99")
            assert q.bid % D("0.01") == 0 and q.ask % D("0.01") == 0


def test_every_event_family_is_persisted_and_matches_the_report(skew_run):
    report, pipeline, tables = skew_run
    counts = report.db_counts
    assert report.db_dropped == 0
    for table, rows_ in tables.items():
        assert len(rows_) == counts[table], table

    assert counts["amm_quotes"] == sum(len(a.quotes) for a in pipeline.amms)
    assert counts["arb_opportunities"] > 0
    assert counts["market_snapshots"] > 0
    assert counts["spread_history"] > 0
    assert counts["trade_log"] > 0               # the AMM's resting orders and cancels

    # the headline is in the sentiment feature history, with the level it set
    headline_rows = [r for r in tables["sentiment_features"] if r["latest_headline"]]
    assert headline_rows and headline_rows[0]["level"] == D("0.9")

    # snapshots cover all three books: Kalshi YES, Polymarket YES and NO
    books = {(r["venue"], r["outcome"]) for r in tables["market_snapshots"]}
    assert books == {("kalshi", "YES"), ("polymarket", "YES"), ("polymarket", "NO")}


def test_the_persisted_amm_quotes_carry_the_skew(skew_run):
    _, _, tables = skew_run
    kalshi = [r for r in tables["amm_quotes"]
              if r["venue"] == "kalshi" and r["action"] == "requote"]
    shifts = {r["reservation"] - r["mid"] for r in kalshi}
    assert shifts == {D(0), D("0.018")}


def test_the_amm_and_the_arbitrage_strategy_share_one_set_of_limits():
    """
    One 200-contract limit on Polymarket YES, two strategies wanting it.

    The AMM's first quote rests a 10-lot YES bid, which reserves 10 of the
    200. The arbitrage strategy then buys Polymarket YES on every tick until
    the limit stops it — at 190, not 200, because the reservation counts as
    if the resting bid had filled. Position plus everything still reserved
    is exactly the limit, never more.

    The limits see the Polymarket leg on its own (+190 on that venue). The
    AMM's skew sees the event, where the arbitrage pair (Polymarket YES +
    Kalshi NO) is hedged: q = 0, so it does not lean to unwind half of it.
    """
    config = PipelineConfig(kalshi_fee_multiplier=D(1), max_position=D(200),
                            amm_venues=("polymarket",))
    report, pipeline = replay(config)
    try:
        pm = pipeline.portfolio
        assert pm is PortfolioManager.instance()
        token = pipeline.pair.polymarket.yes_token
        yes = pm.position("polymarket", token, "YES")
        reserved_yes = sum(r.size for r in pm.reservations.values()
                           if r.market_id == token and r.side.value == "buy")

        assert report.matched_pairs == 190 == yes
        assert yes + reserved_yes == 200

        assert pm.get_inventory(pipeline.pair.pair_id, venue="polymarket").net == 190
        assert pm.get_inventory(pipeline.pair.pair_id).net == 0      # hedged across venues

        amm = pipeline.amms[0]
        assert amm.last_quote.inventory == 0
        assert amm.last_quote.reservation == amm.last_quote.mid
        # a static book and a flat event: one quote, held for the whole hour
        assert amm.requotes == 1
    finally:
        pipeline.close()


def test_an_amm_scoped_to_its_venue_leans_against_that_venues_leg():
    """
    The same run with inventory_scope="venue": the Polymarket AMM now treats
    the arbitrage leg as its own long inventory, leans its quotes down as it
    builds, and requotes only when r has moved by the threshold.
    """
    from engine.amm_strategy import AMMConfig
    config = PipelineConfig(kalshi_fee_multiplier=D(1), max_position=D(200),
                            amm_venues=("polymarket",),
                            amm=AMMConfig(inventory_scope="venue"))
    report, pipeline = replay(config)
    try:
        amm = pipeline.amms[0]
        assert amm.last_quote.inventory == 190
        assert amm.last_quote.reservation < amm.last_quote.mid
        requotes = [q for q in amm.quotes if q.action == "requote"]
        reservations = [q.reservation for q in requotes]
        assert len(requotes) >= 3
        assert reservations == sorted(reservations, reverse=True)
        assert all(a - b >= amm.config.requote_threshold
                   for a, b in zip(reservations, reservations[1:]))
        # holding YES on this venue, it offers by selling it rather than buying NO
        assert any(s.order.side.value == "sell" and s.order.outcome == "YES"
                   for s in requotes[-1].placed)
    finally:
        pipeline.close()
