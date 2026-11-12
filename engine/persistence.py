"""
engine/persistence.py

Where arbitrage events and fills go so they can be looked at afterwards.

Scope note: Feature 6 in the README is "Event & Snapshot Persistence:
PostgreSQL integration via SQLAlchemy/asyncpg". That is engine/db/
(AsyncDbEventSink, selected with --db-url). This module is the sink interface
it implements, plus the original events-only SQLite sink (--db), which needs
no extra dependency.

SQLite rather than JSON lines because the questions worth asking afterwards
are aggregate ones — how often did an edge exist, what did the threshold
decline, what did unwinds cost — and those are one SELECT in a database and
a script over a log file. The schema is deliberately the same shape a
Postgres version would use (same tables, same columns, DECIMAL stored as
TEXT so no value is rounded on the way in), so swapping the driver is a
rewrite of this file and nothing else.

Decimals are stored as TEXT, not REAL. A float column would silently undo
the entire reason models.py uses Decimal.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol

log = logging.getLogger(__name__)


class EventSink(Protocol):
    """
    What the pipeline needs from a persistence layer.

    The first three calls are the arbitrage events this file was written
    for. The rest were added with feature 6 (engine/db/sink.py): the
    TradeLog's order rows, book snapshots, spread history and sentiment
    features. Every call is synchronous and must not block — a sink
    that talks to a server queues the row and writes it from its own task.
    A sink that has nowhere to put one of these simply ignores it.
    """

    def record_opportunity(self, opportunity, threshold: Decimal, volatility: Decimal) -> None: ...

    def record_attempt(self, attempt) -> None: ...

    def record_fill(self, fill) -> None: ...

    def record_order(self, state, ts=None) -> None: ...

    def record_book_snapshot(self, ts, venue: str, market_id: str, outcome: str,
                             book, reason: str = "periodic") -> None: ...

    def record_spread(self, ts, pair_id: str, quotes: dict, opportunities,
                      threshold=None, volatility=None) -> None: ...

    def record_sentiment(self, ts, pair_id: str, level, volatility, confidence,
                         n_points: int, latest=None, threshold=None) -> None: ...

    def close(self) -> None: ...


class _IgnoresFeature6Events:
    """No-op versions of the calls added with feature 6."""

    def record_order(self, state, ts=None) -> None:
        pass

    def record_book_snapshot(self, ts, venue, market_id, outcome, book, reason="periodic") -> None:
        pass

    def record_spread(self, ts, pair_id, quotes, opportunities, threshold=None,
                      volatility=None) -> None:
        pass

    def record_sentiment(self, ts, pair_id, level, volatility, confidence, n_points,
                         latest=None, threshold=None) -> None:
        pass


class NullEventSink(_IgnoresFeature6Events):
    """Records nothing. The default, so no test needs a database on disk."""

    def record_opportunity(self, opportunity, threshold=None, volatility=None) -> None:
        pass

    def record_attempt(self, attempt) -> None:
        pass

    def record_fill(self, fill) -> None:
        pass

    def close(self) -> None:
        pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS arb_opportunities (
    id              INTEGER PRIMARY KEY,
    ts              TEXT NOT NULL,
    pair_id         TEXT NOT NULL,
    direction       TEXT NOT NULL,      -- "<venue> YES + <venue> NO"
    size            TEXT NOT NULL,      -- executable pairs after depth + threshold
    yes_avg_price   TEXT,
    no_avg_price    TEXT,
    cost_per_pair   TEXT,               -- all-in, fees included; profitable below 1.00
    fees            TEXT NOT NULL,
    net_profit      TEXT NOT NULL,
    threshold       TEXT NOT NULL,      -- the dP_min it was judged against
    volatility      TEXT,               -- sentiment stdev that set the threshold
    top_of_book_cost TEXT               -- the naive number, for comparison
);

CREATE TABLE IF NOT EXISTS arb_attempts (
    id              INTEGER PRIMARY KEY,
    ts              TEXT NOT NULL,
    pair_id         TEXT NOT NULL,
    direction       TEXT NOT NULL,
    dispatched      INTEGER NOT NULL,
    reason          TEXT,
    requested_size  TEXT NOT NULL,
    matched_pairs   TEXT NOT NULL,
    unwound_size    TEXT NOT NULL,
    naked_size      TEXT NOT NULL,
    cash_spent      TEXT NOT NULL,
    locked_profit   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fills (
    id              INTEGER PRIMARY KEY,
    ts              TEXT NOT NULL,
    order_id        TEXT NOT NULL,
    pair_id         TEXT,
    strategy_id     TEXT,
    venue           TEXT NOT NULL,
    market_id       TEXT NOT NULL,
    outcome         TEXT NOT NULL,
    side            TEXT NOT NULL,
    size            TEXT NOT NULL,
    price           TEXT NOT NULL,
    fee             TEXT NOT NULL,
    cash_flow       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_opportunities_pair ON arb_opportunities(pair_id, ts);
CREATE INDEX IF NOT EXISTS idx_fills_pair ON fills(pair_id, ts);
"""


def _t(value) -> str | None:
    """Decimal/datetime -> TEXT, losslessly."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class SqliteEventSink(_IgnoresFeature6Events):
    """
    Usage:
        with SqliteEventSink("arb.db") as sink:
            ...
    Pass ":memory:" for a throwaway database, which is what the tests use.

    Arbitrage events only. For the full feature-6 schema (trade log, book
    snapshots, spread history, sentiment features) use
    engine.db.sink.AsyncDbEventSink, which also runs on SQLite via
    `sqlite+aiosqlite:///file.db`.
    """

    def __init__(self, path: str | Path = "arb_events.db"):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self.written = 0

    def record_opportunity(self, opportunity, threshold: Decimal | None = None,
                           volatility: Decimal | None = None) -> None:
        o = opportunity
        self._insert("arb_opportunities", {
            "ts": _t(o.timestamp), "pair_id": o.pair_id, "direction": o.direction,
            "size": _t(o.size), "yes_avg_price": _t(o.yes_avg_price),
            "no_avg_price": _t(o.no_avg_price), "cost_per_pair": _t(o.cost_per_pair),
            "fees": _t(o.fees), "net_profit": _t(o.net_profit),
            "threshold": _t(threshold if threshold is not None else o.threshold),
            "volatility": _t(volatility), "top_of_book_cost": _t(o.top_of_book_cost),
        })

    def record_attempt(self, attempt) -> None:
        a = attempt
        self._insert("arb_attempts", {
            "ts": _t(a.opportunity.timestamp), "pair_id": a.opportunity.pair_id,
            "direction": a.opportunity.direction, "dispatched": int(a.dispatched),
            "reason": a.reason, "requested_size": _t(a.opportunity.size),
            "matched_pairs": _t(a.matched_pairs), "unwound_size": _t(a.unwound_size),
            "naked_size": _t(a.naked_size), "cash_spent": _t(a.cash_spent),
            "locked_profit": _t(a.locked_profit),
        })

    def record_fill(self, fill) -> None:
        f = fill
        self._insert("fills", {
            "ts": _t(f.timestamp), "order_id": f.order_id, "pair_id": f.pair_id,
            "strategy_id": f.strategy_id, "venue": f.venue, "market_id": f.market_id,
            "outcome": f.outcome, "side": f.side.value, "size": _t(f.size),
            "price": _t(f.price), "fee": _t(f.fee), "cash_flow": _t(f.cash_flow),
        })

    def _insert(self, table: str, row: dict) -> None:
        columns = ", ".join(row)
        placeholders = ", ".join("?" for _ in row)
        try:
            self.conn.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                              tuple(row.values()))
            self.conn.commit()
            self.written += 1
        except sqlite3.Error:
            # persistence must never take the trading loop down with it
            log.exception("failed to persist a row into %s", table)

    def counts(self) -> dict[str, int]:
        return {t: self.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                for t in ("arb_opportunities", "arb_attempts", "fills")}

    def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        return self.conn.execute(sql, params).fetchall()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def make_sink(db_path: str | None = None, db_url: str | None = None, **kwargs) -> EventSink:
    """
    --db-url (an SQLAlchemy async URL, normally postgresql+asyncpg://...)
    wins over --db (a SQLite file for the events-only sink); neither means
    NullEventSink.
    """
    if db_url:
        from engine.db.sink import AsyncDbEventSink    # noqa: PLC0415  (SQLAlchemy is optional)
        return AsyncDbEventSink(db_url, **kwargs)
    if db_path:
        return SqliteEventSink(db_path)
    return NullEventSink()


def dump_json(obj) -> str:
    """Pydantic model -> JSON, Decimals as strings. For ad-hoc debugging."""
    return json.dumps(obj.model_dump(mode="json"), indent=2, default=str)
