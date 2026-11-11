"""
engine/db/

README feature 6: event and snapshot persistence on PostgreSQL through
SQLAlchemy (async) and asyncpg.

    schema.py   the tables, portable between PostgreSQL and SQLite
    rows.py     domain object -> row, one pure function per table
    sink.py     AsyncDbEventSink: non-blocking record_*(), batched async writer

The URL comes from --db-url or DATABASE_URL, e.g.
    postgresql+asyncpg://arb:arb@localhost:5432/arb
docker-compose.yml at the repo root starts a matching PostgreSQL.
"""
