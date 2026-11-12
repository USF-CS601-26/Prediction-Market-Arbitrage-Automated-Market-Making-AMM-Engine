"""
tests/test_persistence_db.py

Feature 6: AsyncDbEventSink and the schema behind it.

Everything here runs on sqlite+aiosqlite — the same SQLAlchemy Core tables
and the same batch writer the PostgreSQL deployment uses, minus the server.
The one test that needs PostgreSQL itself is skipped unless TEST_DATABASE_URL
points at one, e.g.

    docker compose up -d postgres
    TEST_DATABASE_URL=postgresql+asyncpg://arb:arb@localhost:5432/arb pytest tests/test_persistence_db.py
"""

import asyncio
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from engine.amm_strategy import AMMQuote
from engine.arbitrage import ArbitrageOpportunity, kalshi_quotes
from engine.db import rows
from engine.db.schema import EVENT_TABLES
from engine.db.sink import AsyncDbEventSink
from engine.models import Fill, Order, OrderSide, OrderState, OrderStatus
from engine.persistence import NullEventSink, SqliteEventSink, make_sink
from engine.sentiment import SentimentSnapshot

from conftest import make_book

pytestmark = pytest.mark.asyncio

D = Decimal
TS = datetime(2026, 10, 8, 6, 30, tzinfo=timezone.utc)
EXACT = D("0.123456789012345678901234")      # more digits than a float can hold


def a_fill(i=0, price=EXACT):
    return Fill(order_id=f"o-{i}", venue="kalshi", market_id="KX", outcome="YES",
                side=OrderSide.BUY, size=D(10), price=price, fee=D("0.07"), timestamp=TS)


def an_order_state():
    order = Order(order_id="o-1", venue="polymarket", market_id="tok", outcome="NO",
                  side=OrderSide.BUY, size=D(20), limit_price=D("0.62"), pair_id="P",
                  strategy_id="amm:polymarket:P")
    state = OrderState(order=order, status=OrderStatus.PARTIALLY_FILLED,
                       filled_size=D(5), fees=D(0), updated_at=TS)
    state.fills.append(Fill(order_id="o-1", venue="polymarket", market_id="tok", outcome="NO",
                            side=OrderSide.BUY, size=D(5), price=D("0.62"), fee=D(0)))
    return state


def an_opportunity():
    return ArbitrageOpportunity(pair_id="P", timestamp=TS, yes_venue="polymarket",
                                yes_market_id="PY", no_venue="kalshi", no_market_id="KX",
                                size=D(100), yes_avg_price=D("0.38"), no_avg_price=D("0.59"),
                                gross_cost=D("97"), fees=D("1.80"), net_profit=D("1.20"),
                                top_of_book_cost=D("0.97"))


def a_quote():
    return AMMQuote(strategy_id="amm:kalshi:P", timestamp=TS, venue="kalshi", market="P",
                    mid=D("0.415"), reservation=D("0.433"), spread=D("0.0206"),
                    bid=D("0.42"), ask=D("0.45"), sigma=D("0.02"), tau_days=D(1),
                    sentiment=D("0.9"), confidence=D(1), action="requote", reason="test")


@pytest.fixture
def url(tmp_path):
    return f"sqlite+aiosqlite:///{(tmp_path / 'events.db').as_posix()}"


async def record_one_of_each(sink):
    book = make_book("kalshi", [("0.41", "300"), ("0.40", "100")],
                     [("0.42", "200"), ("0.43", "50")], "KX")
    sink.record_fill(a_fill())
    sink.record_order(an_order_state())
    sink.record_opportunity(an_opportunity(), D("0.005"), D("0"))
    sink.record_book_snapshot(TS, "kalshi", "KX", "YES", book)
    quotes = {"kalshi": kalshi_quotes("KX", book)}
    sink.record_spread(TS, "P", quotes, [an_opportunity()], D("0.005"), D("0"))
    sink.record_sentiment(TS, "P", D("0.9"), D("0"), D("1"), 1,
                          SentimentSnapshot(pair_id="P", score=D("0.9"), timestamp=TS,
                                            headline="expected to step down", source="wire"),
                          D("0.005"))
    sink.record_amm_quote(a_quote())


# --------------------------------------------------------------------------
# rows
# --------------------------------------------------------------------------

async def test_every_row_builder_fills_every_column_of_its_table():
    book = make_book("kalshi", [("0.41", "300")], [("0.42", "200")], "KX")
    built = {
        "trade_log": [rows.fill_row("r", a_fill()), rows.order_row("r", an_order_state())],
        "arb_opportunities": [rows.opportunity_row("r", an_opportunity(), D("0.005"), None)],
        "market_snapshots": [rows.snapshot_row("r", TS, "kalshi", "KX", "YES", book)],
        "spread_history": [rows.spread_row("r", TS, "P", {"kalshi": kalshi_quotes("KX", book)},
                                           [], None, None)],
        "sentiment_features": [rows.sentiment_row("r", TS, "P", D(0), D(0), D(0), 0)],
        "amm_quotes": [rows.amm_quote_row("r", a_quote())],
    }
    for table, built_rows in built.items():
        columns = {c.name for c in EVENT_TABLES[table].columns} - {"id"}
        for row in built_rows:
            assert set(row) == columns, table


async def test_a_snapshot_copies_the_levels_so_later_book_changes_do_not_leak_in():
    book = make_book("kalshi", [("0.41", "300"), ("0.40", "100")], [("0.42", "200")], "KX")
    row = rows.snapshot_row("r", TS, "kalshi", "KX", "YES", book, depth=10)
    book.bids.clear()
    assert row["bids"] == [["0.41", "300"], ["0.40", "100"]]       # best first
    assert row["best_bid"] == D("0.41") and row["mid"] == D("0.415")
    assert row["bid_depth"] == D(400)


async def test_naive_timestamps_are_taken_as_utc():
    assert rows.utc(datetime(2026, 1, 1, 12)).tzinfo == timezone.utc


# --------------------------------------------------------------------------
# the sink against a real (SQLite) database
# --------------------------------------------------------------------------

async def test_every_event_type_lands_in_its_table(url):
    sink = AsyncDbEventSink(url)
    await sink.start(mode="test", pair_id="P")
    await record_one_of_each(sink)
    await sink.aclose()

    assert sink.counts() == {"trade_log": 2, "market_snapshots": 1, "spread_history": 1,
                             "sentiment_features": 1, "amm_quotes": 1,
                             "arb_opportunities": 1, "arb_attempts": 0}
    assert sink.dropped == 0

    reader = AsyncDbEventSink(url)          # a fresh engine (and run) on the same file
    await reader.start(mode="read")
    try:
        trade_log = await reader.fetch_table("trade_log", run_only=False)
        kinds = sorted(r["kind"] for r in trade_log if r["run_id"] == sink.run_id)
        assert kinds == ["fill", "order"]
        runs = await reader.fetch("SELECT mode, finished_at FROM runs WHERE run_id = :r",
                                  {"r": sink.run_id})
        assert runs[0][0] == "test" and runs[0][1] is not None
    finally:
        await reader.aclose()


async def test_decimals_round_trip_exactly(url):
    sink = AsyncDbEventSink(url)
    await sink.start(mode="test", pair_id="P")
    sink.record_fill(a_fill(price=EXACT))
    await sink.flush()
    (row,) = await sink.fetch_table("trade_log")
    await sink.aclose()
    assert row["price"] == EXACT and isinstance(row["price"], Decimal)
    assert row["cash_flow"] == -(D(10) * EXACT + D("0.07"))
    assert row["ts"] == TS and row["ts"].tzinfo is not None


async def test_snapshot_and_sentiment_rows_read_back_intact(url):
    sink = AsyncDbEventSink(url)
    await sink.start(mode="test", pair_id="P")
    await record_one_of_each(sink)
    await sink.flush()
    (snap,) = await sink.fetch_table("market_snapshots")
    (senti,) = await sink.fetch_table("sentiment_features")
    (spread,) = await sink.fetch_table("spread_history")
    (quote,) = await sink.fetch_table("amm_quotes")
    await sink.aclose()
    assert snap["asks"] == [["0.42", "200"], ["0.43", "50"]]
    assert senti["latest_headline"] == "expected to step down" and senti["level"] == D("0.9")
    assert spread["tops"]["kalshi"]["yes_ask"] == "0.42"
    assert quote["reservation"] == D("0.433") and quote["action"] == "requote"


async def test_rows_recorded_before_start_are_written_once_it_starts(url):
    sink = AsyncDbEventSink(url)
    sink.record_fill(a_fill())
    assert sink.pending == 1
    await sink.start(mode="test", pair_id="P")
    await sink.aclose()
    assert sink.counts()["trade_log"] == 1


async def test_rows_are_written_in_batches(url):
    sink = AsyncDbEventSink(url, batch_size=50, flush_interval=5.0)
    await sink.start(mode="test", pair_id="P")
    for i in range(500):
        sink.record_fill(a_fill(i))
    await sink.aclose()
    assert sink.counts()["trade_log"] == 500
    assert sink.batches == 10


# --------------------------------------------------------------------------
# logging never blocks the strategy loop
# --------------------------------------------------------------------------

async def test_recording_does_not_wait_for_a_slow_database(url):
    sink = AsyncDbEventSink(url, batch_size=100)
    await sink.start(mode="test", pair_id="P")
    real_write = sink._write

    async def slow_write(batch):
        await asyncio.sleep(0.3)            # a database taking 300ms per batch
        await real_write(batch)
    sink._write = slow_write

    started = time.perf_counter()
    for i in range(1000):
        sink.record_fill(a_fill(i))
    elapsed = time.perf_counter() - started
    assert elapsed < 0.25, f"recording 1000 rows took {elapsed:.3f}s"
    assert sink.pending > 0                 # still queued, not yet written

    await sink.aclose()
    assert sink.counts()["trade_log"] == 1000


async def test_a_full_queue_drops_and_counts_instead_of_blocking(url):
    sink = AsyncDbEventSink(url, max_queue=10)
    await sink.start(mode="test", pair_id="P")
    for i in range(50):                     # the writer gets no turn in between
        sink.record_fill(a_fill(i))
    assert sink.dropped == 40
    await sink.aclose()
    assert sink.counts()["trade_log"] == 10


async def test_a_failing_database_never_raises_into_the_caller(url):
    sink = AsyncDbEventSink(url, max_retries=2, retry_backoff=0.0)
    await sink.start(mode="test", pair_id="P")

    class Broken:
        def begin(self):
            raise ConnectionError("database went away")
    engine, sink.engine = sink.engine, Broken()

    sink.record_fill(a_fill())             # must not raise
    await sink.flush()
    assert sink.failed_batches == 1 and sink.dropped == 1
    sink.engine = engine
    await sink.aclose()


async def test_recording_after_close_is_a_counted_no_op(url):
    sink = AsyncDbEventSink(url)
    await sink.start(mode="test", pair_id="P")
    await sink.aclose()
    sink.record_fill(a_fill())
    assert sink.dropped == 1


# --------------------------------------------------------------------------
# choosing a sink
# --------------------------------------------------------------------------

async def test_make_sink_prefers_the_url(tmp_path, url):
    assert isinstance(make_sink(), NullEventSink)
    sqlite = make_sink(db_path=str(tmp_path / "a.db"))
    assert isinstance(sqlite, SqliteEventSink)
    sqlite.close()
    assert isinstance(make_sink(db_path="ignored.db", db_url=url), AsyncDbEventSink)


async def test_the_older_sinks_accept_the_new_event_types():
    """The pipeline records snapshots etc. through whatever sink it has."""
    for sink in (NullEventSink(), SqliteEventSink(":memory:")):
        await record_one_of_each(sink)
        sink.close()


# --------------------------------------------------------------------------
# PostgreSQL itself
# --------------------------------------------------------------------------

PG_URL = os.environ.get("TEST_DATABASE_URL")


@pytest.mark.postgres
@pytest.mark.skipif(not PG_URL, reason="set TEST_DATABASE_URL to a postgresql+asyncpg:// URL")
async def test_postgres_round_trip():
    sink = AsyncDbEventSink(PG_URL)
    await sink.start(mode="test", pair_id="P")
    await record_one_of_each(sink)
    sink.record_fill(a_fill(1, price=EXACT))
    await sink.flush()
    fills = [r for r in await sink.fetch_table("trade_log") if r["kind"] == "fill"]
    total = await sink.fetch(
        "SELECT SUM(size * price) FROM trade_log WHERE run_id = :r AND kind = 'fill'",
        {"r": sink.run_id})
    await sink.aclose()

    assert sink.counts()["trade_log"] == 3 and sink.dropped == 0
    assert any(r["price"] == EXACT for r in fills)          # NUMERIC keeps every digit
    assert total[0][0] == D(10) * EXACT + D(10) * EXACT     # aggregates exactly, in SQL
