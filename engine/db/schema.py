"""
engine/db/schema.py

The database schema, as SQLAlchemy Core tables.

Written once, run on two drivers:

    postgresql+asyncpg://...      the real deployment (README feature 6)
    sqlite+aiosqlite:///file.db   offline runs and the test suite

Two column types make that portable without giving up exactness:

  ExactDecimal  NUMERIC on PostgreSQL, which is exact and aggregates with
                SUM/AVG directly. SQLite has no exact decimal type (its
                NUMERIC affinity is a float), so there the value is stored as
                TEXT — the same choice engine/persistence.py made — and read
                back as Decimal either way. A float anywhere in this path
                would undo the reason every model in the repo uses Decimal.

  UTCDateTime   TIMESTAMPTZ on PostgreSQL. Values are normalized to UTC on
                the way in (a naive datetime is taken to be UTC) and come back
                timezone-aware on both drivers.

Tables
------
    runs                one row per pipeline run; every other row carries run_id
    trade_log           executions: one row per order (its final state) and
                        one row per fill — the TradeLog
    market_snapshots    L2 book snapshots, top N levels per side — the MarketSnapshot
    spread_history      cross-venue top of book and the best priced edge over time
    sentiment_features  sentiment level, volatility, confidence and headline over time
    arb_opportunities   priced arbitrage opportunities (same columns as the SQLite sink)
    arb_attempts        what each execution attempt actually did
"""

from __future__ import annotations

from datetime import timezone
from decimal import Decimal

from sqlalchemy import (JSON, BigInteger, Boolean, Column, DateTime, ForeignKey, Index,
                        Integer, MetaData, Numeric, String, Table, Text)
from sqlalchemy.types import TypeDecorator

metadata = MetaData()


class ExactDecimal(TypeDecorator):
    """NUMERIC on PostgreSQL, TEXT on SQLite, Decimal in Python on both."""
    impl = Numeric
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "sqlite":
            return dialect.type_descriptor(String(64))
        return dialect.type_descriptor(Numeric(asdecimal=True))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        value = value if isinstance(value, Decimal) else Decimal(str(value))
        return str(value) if dialect.name == "sqlite" else value

    def process_result_value(self, value, dialect):
        return None if value is None else Decimal(str(value))


class UTCDateTime(TypeDecorator):
    """TIMESTAMPTZ; always stored as UTC and returned timezone-aware."""
    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def process_result_value(self, value, dialect):
        if value is not None and value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value


def _id() -> Column:
    # BIGSERIAL on PostgreSQL; SQLite only autoincrements an INTEGER key
    return Column("id", BigInteger().with_variant(Integer, "sqlite"),
                  primary_key=True, autoincrement=True)


def _run_id() -> Column:
    return Column("run_id", String(36), ForeignKey("runs.run_id"), nullable=False)


runs = Table(
    "runs", metadata,
    Column("run_id", String(36), primary_key=True),
    Column("started_at", UTCDateTime, nullable=False),
    Column("finished_at", UTCDateTime),
    Column("mode", String(16), nullable=False),           # live | replay | test
    Column("pair_id", String(128), nullable=False),
    Column("config", JSON),
)

trade_log = Table(
    "trade_log", metadata,
    _id(), _run_id(),
    Column("ts", UTCDateTime, nullable=False),
    Column("kind", String(8), nullable=False),            # order | fill
    Column("order_id", String(64), nullable=False),
    Column("venue", String(32), nullable=False),
    Column("market_id", String(128), nullable=False),
    Column("outcome", String(3), nullable=False),
    Column("side", String(4), nullable=False),
    Column("size", ExactDecimal, nullable=False),         # order size, or fill size
    Column("price", ExactDecimal),                        # limit price, or fill price
    Column("fee", ExactDecimal),
    Column("cash_flow", ExactDecimal),                    # fills only
    Column("status", String(20)),                         # orders only
    Column("filled_size", ExactDecimal),                  # orders only
    Column("avg_fill_price", ExactDecimal),               # orders only
    Column("reason", Text),
    Column("strategy_id", String(64)),
    Column("pair_id", String(128)),
    Column("is_unwind", Boolean, nullable=False, default=False),
)

market_snapshots = Table(
    "market_snapshots", metadata,
    _id(), _run_id(),
    Column("ts", UTCDateTime, nullable=False),
    Column("venue", String(32), nullable=False),
    Column("market_id", String(128), nullable=False),
    Column("outcome", String(3), nullable=False),
    Column("sequence", BigInteger),
    Column("best_bid", ExactDecimal),
    Column("best_ask", ExactDecimal),
    Column("mid", ExactDecimal),
    Column("bid_depth", ExactDecimal),                    # total size in the stored levels
    Column("ask_depth", ExactDecimal),
    Column("bids", JSON, nullable=False),                 # [[price, size], ...] best first, as strings
    Column("asks", JSON, nullable=False),
    Column("reason", String(16), nullable=False),         # periodic | event
)

spread_history = Table(
    "spread_history", metadata,
    _id(), _run_id(),
    Column("ts", UTCDateTime, nullable=False),
    Column("pair_id", String(128), nullable=False),
    Column("tops", JSON, nullable=False),                 # {venue: {yes_bid, yes_ask, no_bid, no_ask}}
    Column("direction", String(64)),                      # the best direction this tick
    Column("top_of_book_cost", ExactDecimal),             # YES ask + NO ask, best direction
    Column("top_of_book_edge", ExactDecimal),             # 1 - that cost
    Column("executable_size", ExactDecimal),              # pairs that clear depth, fees and dP_min
    Column("net_profit", ExactDecimal),
    Column("net_per_pair", ExactDecimal),
    Column("threshold", ExactDecimal),
    Column("volatility", ExactDecimal),
)

sentiment_features = Table(
    "sentiment_features", metadata,
    _id(), _run_id(),
    Column("ts", UTCDateTime, nullable=False),
    Column("pair_id", String(128), nullable=False),
    Column("level", ExactDecimal, nullable=False),        # mean signed score in the window
    Column("volatility", ExactDecimal, nullable=False),   # stdev in the window
    Column("confidence", ExactDecimal, nullable=False),
    Column("n_points", Integer, nullable=False),
    Column("latest_score", ExactDecimal),
    Column("latest_headline", Text),
    Column("latest_source", String(64)),
    Column("threshold", ExactDecimal),                    # dP_min this produced
)

arb_opportunities = Table(
    "arb_opportunities", metadata,
    _id(), _run_id(),
    Column("ts", UTCDateTime, nullable=False),
    Column("pair_id", String(128), nullable=False),
    Column("direction", String(64), nullable=False),
    Column("size", ExactDecimal, nullable=False),
    Column("yes_avg_price", ExactDecimal),
    Column("no_avg_price", ExactDecimal),
    Column("cost_per_pair", ExactDecimal),
    Column("fees", ExactDecimal, nullable=False),
    Column("net_profit", ExactDecimal, nullable=False),
    Column("threshold", ExactDecimal, nullable=False),
    Column("volatility", ExactDecimal),
    Column("top_of_book_cost", ExactDecimal),
)

arb_attempts = Table(
    "arb_attempts", metadata,
    _id(), _run_id(),
    Column("ts", UTCDateTime, nullable=False),
    Column("pair_id", String(128), nullable=False),
    Column("direction", String(64), nullable=False),
    Column("dispatched", Boolean, nullable=False),
    Column("reason", Text),
    Column("requested_size", ExactDecimal, nullable=False),
    Column("matched_pairs", ExactDecimal, nullable=False),
    Column("unwound_size", ExactDecimal, nullable=False),
    Column("naked_size", ExactDecimal, nullable=False),
    Column("cash_spent", ExactDecimal, nullable=False),
    Column("locked_profit", ExactDecimal, nullable=False),
)

# time-ordered reads per run are the common query on every event table
for _table in (trade_log, market_snapshots, spread_history, sentiment_features,
               arb_opportunities, arb_attempts):
    Index(f"ix_{_table.name}_run_ts", _table.c.run_id, _table.c.ts)
Index("ix_trade_log_order", trade_log.c.order_id)

EVENT_TABLES = {t.name: t for t in (trade_log, market_snapshots, spread_history,
                                     sentiment_features, arb_opportunities, arb_attempts)}
