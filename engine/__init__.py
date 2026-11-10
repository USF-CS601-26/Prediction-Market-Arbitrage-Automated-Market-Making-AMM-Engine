"""
engine/

Venue-neutral layer that sits on top of the two ingestion engines:

    kalshi_engine/       Kalshi feed, fee schedule, L2 book
    Polymarket_engine/   Polymarket feed, fee schedule, L2 books (YES + NO)
    engine/              <- execution, strategies, risk and the pipeline

Nothing in here talks to an exchange's wire format. It consumes the
normalized OrderBook / ExecutionQuote contracts from kalshi_engine/models.py
and produces orders, so the same strategy code runs against a live feed, a
recorded tape or a hand-built fixture.
"""

from engine import _paths  # noqa: F401  (import for its sys.path side effect)
