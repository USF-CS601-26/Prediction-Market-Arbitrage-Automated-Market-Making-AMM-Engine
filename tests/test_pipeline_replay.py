"""
tests/test_pipeline_replay.py

End-to-end integration on the recorded tapes: both feeds, sentiment,
strategies, mock execution, risk and the database, driven by real data.

The tapes were recorded simultaneously on 2026-10-08 starting 06:28 UTC
(Polymarket with run_polymarket.py --record, Kalshi with kalshi_ws.py
--record) and are the same pair of files tests/test_pair_replay.py uses.
Every frame goes through the code a live run uses — PolymarketFeed for one
venue, KalshiWSClient's message handler for the other — so this exercises
the whole stack rather than a stub of it.

Fully offline: no network, no credentials, no Redis. The Kalshi fee
multiplier is pinned at 1, the value the KXLEADERSOUT series carried when
the tapes were recorded, which is what removes the last HTTP call.

What the tapes actually contain, established in these tests rather than
assumed: for the whole hour Kalshi quoted 0.41/0.42 and Polymarket 0.37/0.38,
a persistent 3c/pair dislocation on an illiquid geopolitical market. That is
why the opportunity count is high — not because the detector is generous.

Run from the repo root:  pytest tests
"""

import asyncio
import contextlib
import io
from decimal import Decimal
from pathlib import Path

import pytest

from engine.arbitrage import DynamicThreshold, SlippageModel
from engine.pipeline import ArbitragePipeline, PipelineConfig, ScriptedHeadline, build_pipeline
from engine.strategy import BaseStrategy, StrategyResult

D = Decimal
ROOT = Path(__file__).resolve().parent.parent
POLY_TAPE = ROOT / "Polymarket_engine/fixtures/tape_polymarket_netanyahu-out-2026_20261008T062810Z.jsonl"
KALSHI_TAPE = ROOT / "kalshi_engine/fixtures/tape_kalshi_KXLEADERSOUT-27JAN01-BNETISR_20261008T062814Z.jsonl"

TOTAL_FRAMES = 2569          # the two tapes merged

pytestmark = pytest.mark.slow


def run(config: PipelineConfig | None = None, headlines=None, **kwargs):
    """Replay both tapes through a fresh pipeline and return (report, pipeline)."""
    config = config or PipelineConfig(kalshi_fee_multiplier=D(1), **kwargs)
    pipeline = build_pipeline(config=config)
    with contextlib.redirect_stdout(io.StringIO()):     # keep test output readable
        report = asyncio.run(pipeline.run_replay(POLY_TAPE, KALSHI_TAPE,
                                                 headlines=headlines))
    return report, pipeline


@pytest.fixture(scope="module")
def baseline():
    """One dry-run replay, shared by the tests that only read from it."""
    report, pipeline = run(PipelineConfig(kalshi_fee_multiplier=D(1), dry_run=True))
    yield report, pipeline
    pipeline.close()


# --------------------------------------------------------------------------
# the pipeline runs, and the ingestion engines stay healthy under it
# --------------------------------------------------------------------------

def test_every_frame_is_replayed(baseline):
    report, _ = baseline
    assert report.frames_replayed == TOTAL_FRAMES


def test_the_books_stay_consistent_through_the_whole_replay(baseline):
    """
    The strategy layer reads the books but must never corrupt them. A
    Polymarket consistency failure or a Kalshi sequence gap here would mean
    the trading side had written into a feed it only owns read access to.
    """
    report, _ = baseline
    assert report.polymarket_check_failures == 0
    assert report.kalshi_sequence_gaps == 0


def test_the_strategy_only_evaluates_once_both_books_are_usable(baseline):
    """
    2569 frames produce fewer evaluations: nothing is priced before Kalshi's
    first snapshot and Polymarket's two.
    """
    report, _ = baseline
    assert 0 < report.evaluations < report.frames_replayed
    assert report.ticks >= report.evaluations


def test_the_tapes_contain_the_dislocation_these_tests_assume(baseline):
    """
    Pins the actual market state, so a future change to the fixtures cannot
    quietly invalidate every number below. Kalshi 0.41/0.42 against
    Polymarket 0.37/0.38 is a 3c/pair edge buying Polymarket YES and Kalshi
    NO (= 1 - 0.41 = 0.59).
    """
    _, pipeline = baseline
    kalshi_bid, kalshi_ask = pipeline.kalshi_book.get_top_of_book()
    poly_bid, poly_ask = pipeline.poly_feed.book("YES").get_top_of_book()
    assert (kalshi_bid.price, kalshi_ask.price) == (D("0.4100"), D("0.4200"))
    assert (poly_bid.price, poly_ask.price) == (D("0.37"), D("0.38"))

    best = pipeline.arbitrage.last_opportunities[0]
    assert best.direction == "polymarket YES + kalshi NO"
    assert best.top_of_book_cost == D("0.97")


def test_polymarkets_no_book_mirrors_its_yes_book_on_real_data(baseline):
    """
    engine/books.py refuses to synthesize a NO book for Polymarket on the
    grounds that a NO ask and the mirroring YES bid are the same resting
    order, so merging would double-count. This asserts that claim against the
    tape instead of trusting the documentation.
    """
    _, pipeline = baseline
    _, no_ask = pipeline.poly_feed.book("NO").get_top_of_book()
    yes_bid, _ = pipeline.poly_feed.book("YES").get_top_of_book()
    assert no_ask.price == D(1) - yes_bid.price


# --------------------------------------------------------------------------
# dry run vs execution
# --------------------------------------------------------------------------

def test_a_dry_run_prices_opportunities_without_trading(baseline):
    report, _ = baseline
    assert report.opportunities > 0
    assert report.attempts == 0
    assert report.fills == 0
    assert report.matched_pairs == 0


def test_execution_matches_pairs_and_locks_a_positive_profit():
    """
    The whole system, trading. Position limits bound how much of a persistent
    edge can be taken, which is why matched pairs are capped well below what
    was priced.
    """
    report, pipeline = run(PipelineConfig(kalshi_fee_multiplier=D(1),
                                          max_position=D(1000)))
    try:
        assert report.dispatched > 0
        assert report.matched_pairs == 1000          # the per-market position cap
        assert report.locked_profit > 0
        assert report.naked_size == 0                # nothing left unhedged
        assert report.fills > 0
    finally:
        pipeline.close()


def test_the_locked_profit_equals_the_gross_edge_minus_the_fees_paid():
    """
    1000 pairs at a 3c gross edge is $30.00; Kalshi's quadratic taker fee near
    0.41 takes $18.00 of it, leaving $12.00. Pinning the identity rather than
    just the total means a fee bug cannot hide inside a plausible number.
    """
    report, pipeline = run(PipelineConfig(kalshi_fee_multiplier=D(1),
                                          max_position=D(1000)))
    try:
        gross = report.matched_pairs * D("0.03")
        assert gross == D("30.00")
        assert pipeline.portfolio.fees_paid == D("18.00")
        assert report.locked_profit == gross - pipeline.portfolio.fees_paid
        assert report.locked_profit == D("12.00")
    finally:
        pipeline.close()


def test_the_position_limit_is_what_stops_the_trading():
    report, pipeline = run(PipelineConfig(kalshi_fee_multiplier=D(1),
                                          max_position=D(200)))
    try:
        assert report.matched_pairs == 200
        assert report.vetoed_by_risk > 0
        assert pipeline.portfolio.position("polymarket",
                                           pipeline.pair.polymarket.yes_token,
                                           "YES") == 200
    finally:
        pipeline.close()


def test_replaying_the_same_tapes_twice_gives_the_same_result():
    """
    Seeded executor RNGs and a pinned fee multiplier: a replay is a test only
    if it is reproducible.
    """
    first, p1 = run(PipelineConfig(kalshi_fee_multiplier=D(1), max_position=D(500)))
    second, p2 = run(PipelineConfig(kalshi_fee_multiplier=D(1), max_position=D(500)))
    try:
        assert first.model_dump(exclude={"thresholds"}) == second.model_dump(exclude={"thresholds"})
    finally:
        p1.close()
        p2.close()


# --------------------------------------------------------------------------
# use case 1 from the proposal: a widened threshold under volatility
# --------------------------------------------------------------------------

def test_a_news_surge_mid_replay_widens_the_threshold_and_stops_the_trading():
    """
    USE CASE: "a widened arbitrage threshold under volatility", on real data.

    The tape's edge is a steady 3c/pair. Four contradictory headlines are
    injected 800 frames in; the sentiment standard deviation pushes dP_min
    past 3c, and the SAME unchanged book stops being tradable. Nothing about
    the market data differs between the two halves of this run — only the
    threshold does, which is what makes it evidence about the threshold.
    """
    headlines = [
        ScriptedHeadline(after_frames=800, score=D("0.95"), text="Netanyahu to resign - wire"),
        ScriptedHeadline(after_frames=805, score=D("-0.90"), text="Office denies resignation"),
        ScriptedHeadline(after_frames=810, score=D("0.90"), text="Coalition partner confirms"),
        ScriptedHeadline(after_frames=815, score=D("-0.85"), text="Second denial issued"),
    ]
    config = PipelineConfig(kalshi_fee_multiplier=D(1), dry_run=True,
                            threshold=DynamicThreshold(base=D("0.005"), lam=D("0.05")))
    report, pipeline = run(config, headlines=headlines)
    try:
        assert report.headlines_injected == 4

        # the threshold really did widen, from the base to well past the edge
        assert report.min_threshold == D("0.005")
        assert report.max_threshold > D("0.03")

        # and the strategy declined the same book it had been pricing happily
        assert report.declined_by_threshold > 0
        assert report.opportunities > 0

        calm = [t for t in report.thresholds if t == D("0.005")]
        surged = [t for t in report.thresholds if t > D("0.03")]
        assert calm and surged
    finally:
        pipeline.close()


def test_without_the_surge_the_same_replay_never_declines_anything():
    """The control for the test above: same tapes, same threshold, no headlines."""
    config = PipelineConfig(kalshi_fee_multiplier=D(1), dry_run=True,
                            threshold=DynamicThreshold(base=D("0.005"), lam=D("0.05")))
    report, pipeline = run(config)
    try:
        assert report.headlines_injected == 0
        assert report.max_threshold == D("0.005")
        assert report.declined_by_threshold == 0
    finally:
        pipeline.close()


# --------------------------------------------------------------------------
# use case 2 from the proposal: quote skewing on a high-sentiment headline
# --------------------------------------------------------------------------

class SkewProbe(BaseStrategy):
    """
    A minimal quoting strategy, standing in for Person A's AMM.

    It is NOT the AMM — it only computes the skew the AMM would apply, which
    is what this test can honestly verify: that a strategy registered through
    add_strategy() receives the same sentiment feed the arbitrage threshold
    reads, and sees the signed LEVEL (which an AMM skews on) rather than the
    volatility (which the threshold widens on). The real AMM drops into this
    same seam.

    Each quote is recorded with the replay time and the sentiment level that
    produced it, because the skew is expected to appear AND then decay as the
    headline ages out of the window.
    """

    def __init__(self, pair_id, sentiment, book, skew_cents=D("0.02")):
        super().__init__("skew-probe")
        self.pair_id = pair_id
        self.sentiment = sentiment
        self.book = book
        self.skew_cents = skew_cents
        self.quotes: list[dict] = []

    async def on_tick(self, now=None) -> StrategyResult:
        bid, ask = self.book.get_top_of_book()
        if bid is None or ask is None:
            return self.skip("no two-sided book", now)
        level = self.sentiment.level(self.pair_id)
        # positive sentiment favours YES, so both quotes move up
        skew = level * self.skew_cents
        self.quotes.append({"at": now, "level": level, "skew": skew,
                            "bid": bid.price + skew, "ask": ask.price + skew,
                            "raw_bid": bid.price, "raw_ask": ask.price})
        return StrategyResult(strategy_id=self.strategy_id, timestamp=now,
                              acted=False, reason=f"skew {skew}")


def test_a_high_sentiment_headline_skews_a_registered_quoting_strategy():
    """
    USE CASE: "quote skewing on a high-sentiment headline".

    A +0.90 headline is injected 1200 frames into the tape. The underlying
    Kalshi book does not move for the whole hour, so any change in the quotes
    is the sentiment skew and nothing else: +0.90 x 2c = +1.8c on both sides.

    Scope: the AMM quoter itself is Person A's module and is not in this
    repository yet, so what is verified here is the integration contract — a
    strategy registered on the pipeline gets the same scored headlines, reads
    the signed level, and skews off it.
    """
    config = PipelineConfig(kalshi_fee_multiplier=D(1), dry_run=True)
    pipeline = build_pipeline(config=config)
    probe = SkewProbe(pipeline.pair.pair_id, pipeline.sentiment, pipeline.kalshi_book)
    pipeline.add_strategy(probe)

    headlines = [ScriptedHeadline(after_frames=1200, score=D("0.90"),
                                  text="Netanyahu expected to step down")]
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            report = asyncio.run(pipeline.run_replay(POLY_TAPE, KALSHI_TAPE,
                                                     headlines=headlines))
        assert report.headlines_injected == 1
        assert len(probe.quotes) > 100

        # the book really is static, so the skew is the only moving part
        assert {q["raw_bid"] for q in probe.quotes} == {D("0.4100")}

        before = [q for q in probe.quotes if q["level"] == 0]
        skewed = [q for q in probe.quotes if q["level"] != 0]
        assert before and skewed, "expected quotes both before and after the headline"

        # unskewed quotes are exactly the book
        assert before[0]["bid"] == before[0]["raw_bid"]

        # skewed ones are the book plus 0.90 x 2c, on both sides
        assert skewed[0]["level"] == D("0.9")
        assert skewed[0]["skew"] == D("0.018")
        assert skewed[0]["bid"] == D("0.4100") + D("0.018")
        assert skewed[0]["ask"] == D("0.4200") + D("0.018")

        # and the skew only applies while the headline is inside the window
        assert probe.quotes[0]["at"] < skewed[0]["at"]
        assert probe.quotes[-1]["level"] == 0, "the headline should age out"
    finally:
        pipeline.close()


def test_a_registered_strategy_shares_the_executors_and_the_portfolio():
    """The seam Person A's AMM plugs into: same factory, same risk, same sentiment."""
    pipeline = build_pipeline(config=PipelineConfig(kalshi_fee_multiplier=D(1),
                                                    dry_run=True))
    try:
        probe = SkewProbe(pipeline.pair.pair_id, pipeline.sentiment, pipeline.kalshi_book)
        pipeline.add_strategy(probe)
        assert pipeline.strategies == [pipeline.arbitrage, probe]
        assert pipeline.factory.venues() == ["kalshi", "polymarket"]
        assert probe.sentiment is pipeline.sentiment
    finally:
        pipeline.close()


# --------------------------------------------------------------------------
# persistence
# --------------------------------------------------------------------------

def test_events_and_fills_are_persisted_and_queryable(tmp_path):
    db = tmp_path / "arb.db"
    report, pipeline = run(PipelineConfig(kalshi_fee_multiplier=D(1),
                                          max_position=D(300), db_path=str(db)))
    try:
        assert report.db_counts["arb_opportunities"] > 0
        assert report.db_counts["arb_attempts"] == report.attempts
        assert report.db_counts["fills"] == report.fills

        # Decimals must survive the round trip as text, not as floats
        rows = pipeline.sink.query(
            "SELECT direction, size, cost_per_pair FROM arb_opportunities LIMIT 1")
        direction, size, cost = rows[0]
        assert direction == "polymarket YES + kalshi NO"
        assert D(cost) < D("1.00")
        assert D(size) > 0

        total = pipeline.sink.query("SELECT SUM(CAST(size AS REAL)) FROM fills")[0][0]
        assert total > 0
    finally:
        pipeline.close()


def test_slippage_reduces_the_profit_on_the_same_tapes():
    """One adverse tick per leg on a 3c edge leaves 1c, before fees."""
    clean, p1 = run(PipelineConfig(kalshi_fee_multiplier=D(1), max_position=D(300)))
    slipped, p2 = run(PipelineConfig(
        kalshi_fee_multiplier=D(1), max_position=D(300),
        slippage=SlippageModel(adverse_ticks=D(1), tick_size=D("0.01"))))
    try:
        assert clean.locked_profit > slipped.locked_profit
    finally:
        p1.close()
        p2.close()
