"""
engine/db/sink.py

AsyncDbEventSink — the EventSink for README feature 6: PostgreSQL through
SQLAlchemy's async engine and asyncpg, with async batching so that logging
never blocks the strategy loop.

How it stays out of the strategy's way
--------------------------------------
Every record_*() call is synchronous and does exactly two things: build the
row (pure, microseconds) and put it on a bounded asyncio.Queue with
put_nowait. It never awaits the database. A background writer task drains
the queue in batches — up to `batch_size` rows, or whatever arrived within
`flush_interval` seconds of the first — and writes each batch in ONE
transaction, one executemany per table.

What happens when the database cannot keep up or is down:

  queue full     the row is dropped and counted (`dropped`); the strategy
                 carries on. A trading loop that stalls because its audit log
                 is slow has its priorities backwards.
  write fails    the batch is retried `max_retries` times with backoff, then
                 dropped and counted (`failed_batches`). Nothing is raised to
                 the caller — persistence must never take trading down, the
                 same rule engine/persistence.py states.

Lifecycle
---------
    sink = AsyncDbEventSink("postgresql+asyncpg://arb:arb@localhost:5432/arb")
    await sink.start(mode="live", pair_id=..., config={...})   # creates tables, inserts the run
    ... record_*() from anywhere on the loop ...
    await sink.aclose()                                         # drains the queue, marks the run finished

Rows recorded before start() are buffered and written once it runs, so the
pipeline can construct the sink before its event loop exists.

The same class runs on `sqlite+aiosqlite:///path.db`, which is how the test
suite exercises the batching, the schema and the Decimal round trip without a
PostgreSQL server.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from engine.db import rows
from engine.db.schema import EVENT_TABLES, metadata, runs

log = logging.getLogger(__name__)

_STOP = object()


class AsyncDbEventSink:

    def __init__(self, url: str, batch_size: int = 200, flush_interval: float = 0.25,
                 max_queue: int = 100_000, max_retries: int = 3, retry_backoff: float = 0.2,
                 snapshot_depth: int = 10, echo: bool = False):
        self.url = url
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_queue = max_queue
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.snapshot_depth = snapshot_depth
        self.echo = echo

        self.run_id = str(uuid.uuid4())
        self.engine: AsyncEngine | None = None
        self._queue: asyncio.Queue | None = None
        self._writer: asyncio.Task | None = None
        self._early: list[tuple[str, dict]] = []
        self._closed = False

        self.written: Counter[str] = Counter()
        self.recorded: Counter[str] = Counter()
        self.dropped = 0
        self.failed_batches = 0
        self.batches = 0

    # ---- lifecycle ----

    @property
    def started(self) -> bool:
        return self._writer is not None

    async def start(self, mode: str = "live", pair_id: str = "", config: dict | None = None
                    ) -> "AsyncDbEventSink":
        if self.started:
            return self
        self.engine = create_async_engine(self.url, echo=self.echo)
        async with self.engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
            await conn.execute(runs.insert().values(
                run_id=self.run_id, started_at=datetime.now(tz=timezone.utc),
                mode=mode, pair_id=pair_id,
                config={k: str(v) for k, v in (config or {}).items()},
            ))
        self._queue = asyncio.Queue(maxsize=self.max_queue)
        for item in self._early:
            self._enqueue(item)
        self._early.clear()
        self._writer = asyncio.create_task(self._write_loop(), name="db-writer")
        return self

    async def flush(self) -> None:
        """Wait until everything recorded so far has been written (or dropped)."""
        if self._queue is not None and self._writer is not None and not self._writer.done():
            await self._queue.join()

    async def aclose(self) -> None:
        """Drain the queue, mark the run finished and release the connections."""
        if self._closed:
            return
        self._closed = True
        if self._writer is not None:
            await self._queue.put(_STOP)
            await self._writer
        if self.engine is not None:
            try:
                async with self.engine.begin() as conn:
                    await conn.execute(update(runs).where(runs.c.run_id == self.run_id)
                                       .values(finished_at=datetime.now(tz=timezone.utc)))
            except Exception:
                log.exception("could not mark run %s finished", self.run_id)
            await self.engine.dispose()

    def close(self) -> None:
        """
        Synchronous close, for the EventSink protocol. The real shutdown is
        aclose(), which the pipeline awaits at the end of a run; anything
        still queued here can no longer be written.
        """
        if not self._closed:
            pending = self._queue.qsize() if self._queue is not None else len(self._early)
            if pending:
                log.warning("AsyncDbEventSink closed without aclose(); %d rows not written", pending)
            self._closed = True

    # ---- recording: synchronous, never awaits the database ----

    def record_fill(self, fill) -> None:
        self._put("trade_log", rows.fill_row(self.run_id, fill))

    def record_order(self, state, ts: datetime | None = None) -> None:
        self._put("trade_log", rows.order_row(self.run_id, state, ts))

    def record_opportunity(self, opportunity, threshold: Decimal | None = None,
                           volatility: Decimal | None = None) -> None:
        self._put("arb_opportunities",
                  rows.opportunity_row(self.run_id, opportunity, threshold, volatility))

    def record_attempt(self, attempt) -> None:
        self._put("arb_attempts", rows.attempt_row(self.run_id, attempt))

    def record_book_snapshot(self, ts: datetime, venue: str, market_id: str, outcome: str,
                             book, reason: str = "periodic") -> None:
        self._put("market_snapshots", rows.snapshot_row(
            self.run_id, ts, venue, market_id, outcome, book, self.snapshot_depth, reason))

    def record_spread(self, ts: datetime, pair_id: str, quotes: dict, opportunities,
                      threshold: Decimal | None = None, volatility: Decimal | None = None) -> None:
        self._put("spread_history", rows.spread_row(
            self.run_id, ts, pair_id, quotes, opportunities, threshold, volatility))

    def record_sentiment(self, ts: datetime, pair_id: str, level: Decimal, volatility: Decimal,
                         confidence: Decimal, n_points: int, latest=None,
                         threshold: Decimal | None = None) -> None:
        self._put("sentiment_features", rows.sentiment_row(
            self.run_id, ts, pair_id, level, volatility, confidence, n_points, latest, threshold))

    def _put(self, table: str, row: dict) -> None:
        if self._closed:
            self.dropped += 1
            return
        self.recorded[table] += 1
        if self._queue is None:
            self._early.append((table, row))
        else:
            self._enqueue((table, row))

    def _enqueue(self, item) -> None:
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            self.dropped += 1
            if self.dropped == 1 or self.dropped % 1000 == 0:
                log.warning("persistence queue full; %d rows dropped so far", self.dropped)

    # ---- the writer ----

    async def _write_loop(self) -> None:
        loop = asyncio.get_running_loop()
        stopping = False
        while not stopping:
            item = await self._queue.get()
            if item is _STOP:
                self._queue.task_done()
                break
            batch = [item]
            deadline = loop.time() + self.flush_interval
            while len(batch) < self.batch_size:
                try:
                    item = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    timeout = deadline - loop.time()
                    if timeout <= 0:
                        break
                    try:
                        item = await asyncio.wait_for(self._queue.get(), timeout)
                    except TimeoutError:
                        break
                if item is _STOP:
                    self._queue.task_done()
                    stopping = True
                    break
                batch.append(item)
            await self._write(batch)
            for _ in batch:
                self._queue.task_done()

    async def _write(self, batch: list[tuple[str, dict]]) -> None:
        grouped: dict[str, list[dict]] = defaultdict(list)
        for table, row in batch:
            grouped[table].append(row)
        for attempt in range(1, self.max_retries + 1):
            try:
                async with self.engine.begin() as conn:
                    for table, table_rows in grouped.items():
                        await conn.execute(EVENT_TABLES[table].insert(), table_rows)
                for table, table_rows in grouped.items():
                    self.written[table] += len(table_rows)
                self.batches += 1
                return
            except Exception:
                log.exception("persistence batch of %d rows failed (attempt %d/%d)",
                              len(batch), attempt, self.max_retries)
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_backoff * attempt)
        self.failed_batches += 1
        self.dropped += len(batch)

    # ---- reads, for reports and tests ----

    def counts(self) -> dict[str, int]:
        """Rows written per table in this run."""
        return {name: self.written[name] for name in EVENT_TABLES}

    @property
    def pending(self) -> int:
        return self._queue.qsize() if self._queue is not None else len(self._early)

    async def fetch(self, sql_or_stmt, params: dict | None = None) -> list[tuple]:
        """Run a read query (SQL text or a SQLAlchemy select) on the sink's engine."""
        stmt = text(sql_or_stmt) if isinstance(sql_or_stmt, str) else sql_or_stmt
        async with self.engine.connect() as conn:
            result = await conn.execute(stmt, params or {})
            return [tuple(r) for r in result.fetchall()]

    async def fetch_table(self, table: str, run_only: bool = True) -> list[dict]:
        t = EVENT_TABLES[table] if table in EVENT_TABLES else runs
        stmt = select(t)
        if run_only and "run_id" in t.c:
            stmt = stmt.where(t.c.run_id == self.run_id)
        if "id" in t.c:
            stmt = stmt.order_by(t.c.id)
        async with self.engine.connect() as conn:
            result = await conn.execute(stmt)
            return [dict(r._mapping) for r in result.fetchall()]
