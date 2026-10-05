"""
tests/test_cross_venue.py

Offline tests for the cross-venue edge calculation. Books are built by
hand so these pin the arbitrage arithmetic without touching either
venue's API.
"""

from decimal import Decimal

from models import OrderBookSnapshot, PriceLevel
from order_book import OrderBook
from kalshi_fees import taker_fee
from models import FeeSchedule
from cross_venue_spread import edge_for_direction, best_edge

D = Decimal
FREE = lambda c, p: D("0")
KALSHI = FeeSchedule(venue="kalshi", series="X", fee_type="quadratic", multiplier=D(1))
KFEE = lambda c, p: taker_fee(c, p, KALSHI)


def book(venue, bids, asks):
    b = OrderBook(market_id="M", exchange=venue)
    b.apply_snapshot(OrderBookSnapshot(
        market_id="M", exchange=venue, timestamp="2026-10-05T00:00:00Z",
        bids=[PriceLevel(price=D(p), size=D(s)) for p, s in bids],
        asks=[PriceLevel(price=D(p), size=D(s)) for p, s in asks],
    ))
    return b


def test_buy_cheap_sell_dear_is_a_positive_edge():
    cheap = book("polymarket", [("0.38", "1000")], [("0.39", "1000")])
    dear = book("kalshi", [("0.45", "1000")], [("0.46", "1000")])
    e = edge_for_direction("p", D(100), cheap, FREE, dear, FREE)
    assert e.executable
    assert e.gross_edge == D("0.45") * 100 - D("0.39") * 100
    assert e.net_edge > 0
    assert e.net_per_contract == e.net_edge / 100


def test_net_edge_is_sell_net_minus_buy_net():
    a = book("polymarket", [("0.38", "1000")], [("0.39", "1000")])
    b = book("kalshi", [("0.45", "1000")], [("0.46", "1000")])
    e = edge_for_direction("p", D(100), a, KFEE, b, KFEE)
    assert e.net_edge == e.sell.net - e.buy.net
    assert e.total_fees == e.buy.fees + e.sell.fees


def test_fees_can_erase_a_gross_edge():
    """A 2c gross edge at P~0.5 is mostly fees on 100 contracts."""
    cheap = book("polymarket", [("0.49", "1000")], [("0.49", "1000")])
    dear = book("kalshi", [("0.51", "1000")], [("0.51", "1000")])
    free = edge_for_direction("p", D(100), cheap, FREE, dear, FREE)
    fees = edge_for_direction("p", D(100), cheap, FREE, dear, KFEE)
    assert free.net_edge > fees.net_edge
    assert fees.total_fees > 0


def test_edge_degrades_as_size_exhausts_depth():
    """The core depth lesson: a thin edge does not scale."""
    cheap = book("polymarket", [("0.38", "10000")], [("0.39", "100"), ("0.60", "10000")])
    dear = book("kalshi", [("0.45", "100"), ("0.20", "10000")], [("0.46", "10000")])
    small = edge_for_direction("p", D(100), cheap, FREE, dear, FREE)
    large = edge_for_direction("p", D(1000), cheap, FREE, dear, FREE)
    assert small.net_per_contract > large.net_per_contract
    assert small.net_edge > 0 and large.net_edge < 0


def test_partial_depth_is_not_executable():
    thin = book("polymarket", [("0.38", "5")], [("0.39", "5")])
    deep = book("kalshi", [("0.45", "1000")], [("0.46", "1000")])
    e = edge_for_direction("p", D(100), thin, FREE, deep, FREE)
    assert e.buy.fully_filled is False
    assert e.executable is False


def test_best_edge_returns_both_directions_best_first():
    a = book("polymarket", [("0.38", "1000")], [("0.39", "1000")])
    b = book("kalshi", [("0.45", "1000")], [("0.46", "1000")])
    out = best_edge("p", D(100), a, FREE, b, FREE)
    assert len(out) == 2
    assert out[0].net_edge >= out[1].net_edge
    assert out[0].buy_venue == "polymarket" and out[0].sell_venue == "kalshi"


def test_aligned_books_offer_no_arbitrage():
    a = book("polymarket", [("0.40", "1000")], [("0.42", "1000")])
    b = book("kalshi", [("0.40", "1000")], [("0.42", "1000")])
    assert all(e.net_edge < 0 for e in best_edge("p", D(100), a, FREE, b, FREE))
