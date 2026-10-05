"""
tests/test_fees_and_spread.py

Offline unit tests for the fee schedule and the executable spread
engine. No network: fee schedules are constructed directly and books
are built by hand, so these pin the arithmetic rather than the API.
"""

from decimal import Decimal

import pytest

from models import FeeSchedule, OrderBookSnapshot, PriceLevel, Side
from order_book import OrderBook
from kalshi_fees import taker_fee, maker_fee, TAKER_RATE
from executable_spread import quote_buy, quote_sell, round_trip_edge

D = Decimal

STANDARD = FeeSchedule(venue="kalshi", series="X", fee_type="quadratic", multiplier=D(1))
HALF     = FeeSchedule(venue="kalshi", series="X", fee_type="quadratic", multiplier=D("0.5"))
FREE     = FeeSchedule(venue="kalshi", series="X", fee_type="quadratic", multiplier=D(0))
MAKERFEE = FeeSchedule(venue="kalshi", series="X",
                       fee_type="quadratic_with_maker_fees", multiplier=D(1))


def book_with(bids, asks):
    b = OrderBook(market_id="M", exchange="kalshi")
    b.apply_snapshot(OrderBookSnapshot(
        market_id="M", exchange="kalshi", timestamp="2026-10-05T00:00:00Z",
        bids=[PriceLevel(price=D(p), size=D(s)) for p, s in bids],
        asks=[PriceLevel(price=D(p), size=D(s)) for p, s in asks],
    ))
    return b


# ---------------------------------------------------------------- fees

def test_fee_matches_published_formula():
    """ceil_to_cent(0.07 * C * P * (1-P)) — exact, no rounding needed here."""
    assert taker_fee(D(100), D("0.50"), STANDARD) == D("1.75")


@pytest.mark.parametrize("price,expected", [
    ("0.05", "0.34"),   # 0.07*100*0.05*0.95 = 0.3325 -> ceil 0.34
    ("0.25", "1.32"),   # 0.07*100*0.25*0.75 = 1.3125 -> ceil 1.32
    ("0.75", "1.32"),
    ("0.95", "0.34"),
])
def test_fee_rounds_up_to_next_cent(price, expected):
    assert taker_fee(D(100), D(price), STANDARD) == D(expected)


def test_fee_peaks_at_fifty_cents():
    """
    The quadratic term is maximal at P=0.50. This is the property that
    makes a flat-percentage fee model wrong: identical gross spreads
    cost most to capture in the middle of the book.
    """
    fees = {p: taker_fee(D(100), D(p), STANDARD)
            for p in ("0.10", "0.30", "0.50", "0.70", "0.90")}
    assert max(fees, key=fees.get) == "0.50"
    assert fees["0.10"] == fees["0.90"]      # symmetric about 0.50
    assert fees["0.30"] == fees["0.70"]


def test_rounding_up_makes_tiny_fills_expensive():
    """A sub-cent fee still costs a full cent, which can kill a thin arb."""
    raw = TAKER_RATE * D(1) * D("0.50") * D("0.50")   # 0.0175
    assert raw < D("0.02")
    assert taker_fee(D(1), D("0.50"), STANDARD) == D("0.02")


def test_series_multiplier_is_applied():
    assert taker_fee(D(100), D("0.50"), HALF) == D("0.88")   # ceil(0.875)
    assert taker_fee(D(100), D("0.50"), FREE) == D("0.00")


def test_maker_fee_only_on_maker_fee_series():
    assert maker_fee(D(100), D("0.50"), STANDARD) == D("0.00")
    assert maker_fee(D(100), D("0.50"), MAKERFEE) == D("0.44")  # ceil(0.4375)


# ------------------------------------------------- executable spread

def fee_fn(c, p):
    return taker_fee(c, p, STANDARD)


def test_buy_walks_cheapest_asks_first():
    b = book_with(bids=[("0.40", "100")], asks=[("0.50", "10"), ("0.60", "10")])
    q = quote_buy(b, D(15), fee_fn)
    assert [f.price for f in q.fills] == [D("0.50"), D("0.60")]
    assert q.filled == D(15)
    assert q.gross == D("0.50") * 10 + D("0.60") * 5


def test_sell_walks_highest_bids_first():
    b = book_with(bids=[("0.40", "10"), ("0.30", "10")], asks=[("0.60", "100")])
    q = quote_sell(b, D(15), fee_fn)
    assert [f.price for f in q.fills] == [D("0.40"), D("0.30")]


def test_fees_add_to_buy_cost_and_subtract_from_sale_proceeds():
    b = book_with(bids=[("0.40", "100")], asks=[("0.60", "100")])
    buy, sell = quote_buy(b, D(50), fee_fn), quote_sell(b, D(50), fee_fn)
    assert buy.net == buy.gross + buy.fees
    assert sell.net == sell.gross - sell.fees
    assert buy.avg_price > D("0.60")   # paid worse than the quote
    assert sell.avg_price < D("0.40")  # received worse than the quote


def test_fee_is_charged_per_level_not_on_average_price():
    """
    Because the fee is quadratic, charging it on a blended average price
    gives a different number than charging per fill. This pins the
    per-level behaviour.
    """
    b = book_with(bids=[("0.10", "100")], asks=[("0.10", "100"), ("0.90", "100")])
    q = quote_buy(b, D(200), fee_fn)
    per_level = taker_fee(D(100), D("0.10"), STANDARD) + taker_fee(D(100), D("0.90"), STANDARD)
    blended = taker_fee(D(200), D("0.50"), STANDARD)   # what the naive model charges
    assert q.fees == per_level
    assert q.fees != blended


def test_partial_fill_is_reported_not_silently_rounded():
    b = book_with(bids=[("0.40", "5")], asks=[("0.60", "5")])
    q = quote_buy(b, D(100), fee_fn)
    assert q.filled == D(5)
    assert q.fully_filled is False
    assert round_trip_edge(b, D(100), fee_fn) is None


def test_empty_book_yields_no_depth():
    b = book_with(bids=[], asks=[])
    q = quote_buy(b, D(10), fee_fn)
    assert q.filled == 0 and q.avg_price is None and q.fully_filled is False


def test_round_trip_edge_is_negative_on_a_sane_book():
    """You cannot make money crossing the spread both ways on one venue."""
    b = book_with(bids=[("0.40", "1000")], asks=[("0.42", "1000")])
    assert round_trip_edge(b, D(100), fee_fn) < 0


def test_deeper_size_gets_a_worse_average_price():
    """The whole point of depth-aware quoting."""
    b = book_with(bids=[("0.40", "1000")],
                  asks=[("0.50", "10"), ("0.60", "10"), ("0.70", "1000")])
    small = quote_buy(b, D(10), fee_fn)
    large = quote_buy(b, D(500), fee_fn)
    assert large.avg_price > small.avg_price
