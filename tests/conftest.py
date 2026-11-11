"""
tests/conftest.py

Shared fixtures for the cross-venue suite.

Books are built by hand here rather than loaded from a venue, so every
number in these tests is one a reader can check with a calculator. The
recorded-tape tests in test_pipeline_replay.py are the counterweight: real
data, no hand-built anything.
"""

from decimal import Decimal

import pytest
from models import FeeSchedule, OrderBookSnapshot, PriceLevel
from order_book import OrderBook

from engine.executors.simulator import SimulationConfig

D = Decimal

# No fees: isolates the depth/threshold arithmetic from the fee arithmetic.
FREE = lambda contracts, price: D("0")

# Kalshi's real quadratic schedule at the default multiplier.
KALSHI_SCHEDULE = FeeSchedule(venue="kalshi", series="TEST",
                              fee_type="quadratic", multiplier=D(1))

# Mock venues with no latency and no randomness: a test should fail for the
# reason it is testing, not because an RNG rolled badly.
DETERMINISTIC = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                                 tick_size=D("0.01"))


def make_book(exchange: str, bids, asks, market_id: str = "M") -> OrderBook:
    """
    Build a book from (price, size) string pairs, best-first or not — the
    OrderBook sorts them. Prices and sizes are strings so they become
    Decimals without passing through float.
    """
    book = OrderBook(market_id=market_id, exchange=exchange)
    book.apply_snapshot(OrderBookSnapshot(
        market_id=market_id, exchange=exchange,
        timestamp="2026-10-08T06:28:00Z",
        bids=[PriceLevel(price=D(p), size=D(s)) for p, s in bids],
        asks=[PriceLevel(price=D(p), size=D(s)) for p, s in asks],
    ))
    return book


@pytest.fixture
def book():
    return make_book
