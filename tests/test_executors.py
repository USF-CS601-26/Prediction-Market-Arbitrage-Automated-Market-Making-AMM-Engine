"""
tests/test_executors.py

The executor layer: fills against real depth, partial fills, rejections,
latency, cancellation, and factory routing.

Latency is asserted on the values the executor recorded rather than by timing
the event loop. A test that measures wall-clock time on a loaded CI machine
fails for reasons that have nothing to do with the code.
"""

import asyncio
from decimal import Decimal

import pytest
from conftest import DETERMINISTIC, FREE, KALSHI_SCHEDULE, make_book
from kalshi_fees import taker_fee

from engine.books import StaticOutcomeBooks
from engine.executors import (MockKalshiExecutor, MockPolymarketExecutor,
                             OrderExecutorFactory, UnknownVenueError)
from engine.executors.base import OrderNotFound
from engine.executors.simulator import SimulationConfig
from engine.models import Order, OrderSide, OrderStatus

D = Decimal
KFEE = lambda c, p: taker_fee(c, p, KALSHI_SCHEDULE)
pytestmark = pytest.mark.asyncio


def buy(size, limit, outcome="YES", venue="kalshi", market="T"):
    return Order(venue=venue, market_id=market, outcome=outcome,
                 side=OrderSide.BUY, size=D(size), limit_price=D(limit))


def kalshi_executor(bids=(("0.41", "300"),), asks=(("0.43", "150"), ("0.44", "500")),
                    fee=FREE, config=DETERMINISTIC, is_fresh=None):
    book = make_book("kalshi", bids, asks, "T")
    return MockKalshiExecutor("T", book, fee, is_fresh=is_fresh, config=config), book


# --------------------------------------------------------------------------
# filling against real depth
# --------------------------------------------------------------------------

async def test_a_buy_walks_several_levels_and_reports_the_average():
    """150 @ 0.43 + 50 @ 0.44 = 66.50 on 200 contracts -> avg 0.3325 x 1000... no:
    gross 64.50 + 22.00 = 86.50, avg 0.4325."""
    ex, _ = kalshi_executor()
    state = await ex.submit_order(buy(200, "0.44"))
    assert state.status == OrderStatus.FILLED
    assert state.filled_size == 200
    assert [f.size for f in state.fills] == [D(150), D(50)]
    assert state.gross == D("86.50")
    assert state.avg_price == D("0.4325")


async def test_a_limit_price_truncates_the_ladder():
    """
    A 0.43 limit can only reach the 150 resting there. The remaining 50 RESTS
    rather than filling worse — and rather than being cancelled, which is the
    behaviour the unwind logic depends on.
    """
    ex, _ = kalshi_executor()
    state = await ex.submit_order(buy(200, "0.43"))
    assert state.status == OrderStatus.PARTIALLY_FILLED
    assert state.filled_size == 150
    assert state.remaining == 50
    assert not state.status.is_terminal


async def test_an_unmarketable_limit_rests_unfilled():
    ex, _ = kalshi_executor()
    state = await ex.submit_order(buy(100, "0.40"))
    assert state.status == OrderStatus.OPEN
    assert state.filled_size == 0
    assert state.avg_price is None


async def test_fees_are_charged_per_level_at_that_level_s_price():
    """
    ceil(0.07 x 150 x 0.43 x 0.57) = ceil(2.5736) = 2.58
    ceil(0.07 x  50 x 0.44 x 0.56) = ceil(0.8624) = 0.87
    """
    ex, _ = kalshi_executor(fee=KFEE)
    state = await ex.submit_order(buy(200, "0.44"))
    assert [f.fee for f in state.fills] == [D("2.58"), D("0.87")]
    assert state.fees == D("3.45")
    assert state.net_cash == -(D("86.50") + D("3.45"))


async def test_buying_no_fills_against_the_complement_of_the_yes_book():
    """A YES bid of 300 @ 0.41 is an offer of 300 NO @ 0.59."""
    ex, _ = kalshi_executor()
    state = await ex.submit_order(buy(100, "0.60", outcome="NO"))
    assert state.status == OrderStatus.FILLED
    assert state.fills[0].price == D("0.59")
    assert state.fills[0].outcome == "NO"


# --------------------------------------------------------------------------
# partial fills and liquidity consumption
# --------------------------------------------------------------------------

async def test_a_simulated_partial_fill_leaves_the_remainder_resting():
    """partial_fill_prob=1 forces the stochastic partial path every time."""
    config = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                              tick_size=D("0.01"), partial_fill_prob=1.0,
                              partial_fill_min_fraction=0.25, seed=7)
    ex, _ = kalshi_executor(config=config)
    state = await ex.submit_order(buy(100, "0.44"))
    assert state.status == OrderStatus.PARTIALLY_FILLED
    assert 0 < state.filled_size < 100
    assert state.filled_size + state.remaining == 100


async def test_a_partial_fill_is_never_zero_sized():
    """A 'partial' fill of nothing is a rejection; at least one increment fills."""
    config = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                              tick_size=D("0.01"), partial_fill_prob=1.0,
                              partial_fill_min_fraction=0.0, seed=3)
    ex, _ = kalshi_executor(config=config)
    for _ in range(20):
        state = await ex.submit_order(buy(2, "0.44"))
        assert state.filled_size >= 1


async def test_consume_liquidity_depletes_the_book_for_the_next_order():
    """Off by default (the feed owns the book); on, a fill removes the depth."""
    config = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                              tick_size=D("0.01"), consume_liquidity=True)
    ex, book = kalshi_executor(config=config)
    await ex.submit_order(buy(150, "0.43"))
    assert D("0.43") not in book.asks
    second = await ex.submit_order(buy(150, "0.43"))
    assert second.filled_size == 0


async def test_liquidity_is_left_alone_by_default():
    ex, book = kalshi_executor()
    await ex.submit_order(buy(150, "0.43"))
    assert book.asks[D("0.43")] == 150


# --------------------------------------------------------------------------
# rejections
# --------------------------------------------------------------------------

@pytest.mark.parametrize("order, fragment", [
    (buy("1.5", "0.43"), "whole contracts"),
    (buy(1, "0.435"), "not a multiple of tick"),
    (buy(0, "0.43"), "size must be positive"),
    (buy(1, "1.00"), "between 0 and 1"),
    (buy(1, "0.43", market="OTHER"), "trades T, not OTHER"),
    (Order(venue="polymarket", market_id="T", outcome="YES", side=OrderSide.BUY,
           size=D(1), limit_price=D("0.43")), "addressed to polymarket"),
])
async def test_bad_orders_are_rejected_with_a_specific_reason(order, fragment):
    ex, _ = kalshi_executor()
    state = await ex.submit_order(order)
    assert state.status == OrderStatus.REJECTED
    assert fragment in state.reject_reason
    # preflight must agree with submit_order, or the strategy's pre-check is useless
    assert ex.preflight(order) is None or fragment in ex.preflight(order)


async def test_a_stale_book_is_refused_rather_than_traded():
    """
    KalshiWSClient.book_is_fresh is false while disconnected, before the first
    snapshot, and during a sequence-gap resync. Trading then is how a book
    that has silently drifted becomes a position.
    """
    ex, _ = kalshi_executor(is_fresh=lambda: False)
    state = await ex.submit_order(buy(100, "0.44"))
    assert state.status == OrderStatus.REJECTED
    assert "not fresh" in state.reject_reason


async def test_an_empty_book_is_refused_and_says_so():
    ex, _ = kalshi_executor(bids=(), asks=())
    state = await ex.submit_order(buy(100, "0.44"))
    assert state.status == OrderStatus.REJECTED
    assert "empty on both sides" in state.reject_reason


async def test_the_random_rejection_rate_is_honoured():
    config = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                              tick_size=D("0.01"), reject_prob=1.0)
    ex, _ = kalshi_executor(config=config)
    state = await ex.submit_order(buy(100, "0.44"))
    assert state.status == OrderStatus.REJECTED
    assert state.reject_reason == "simulated venue rejection"
    # and a random rejection is NOT something preflight can predict
    assert ex.preflight(buy(100, "0.44")) is None


# --------------------------------------------------------------------------
# latency
# --------------------------------------------------------------------------

async def test_latency_is_drawn_within_the_configured_band():
    config = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                              tick_size=D("0.01"), latency_ms=40.0,
                              latency_jitter_ms=15.0, seed=1)
    ex, _ = kalshi_executor(config=config)
    for _ in range(25):
        await ex.submit_order(buy(1, "0.44"))
    assert len(ex.latencies_ms) == 25
    assert all(25.0 <= ms <= 55.0 for ms in ex.latencies_ms)
    assert len(set(ex.latencies_ms)) > 1          # jitter actually varies


async def test_a_seeded_executor_replays_identically():
    """Required for any of this to be usable in a test."""
    config = lambda: SimulationConfig(min_order_size=D(1), size_increment=D(1),
                                      tick_size=D("0.01"), latency_ms=40.0,
                                      latency_jitter_ms=15.0, partial_fill_prob=0.5,
                                      seed=99)
    results = []
    for _ in range(2):
        ex, _ = kalshi_executor(config=config())
        filled = [(await ex.submit_order(buy(50, "0.44"))).filled_size for _ in range(10)]
        results.append((filled, ex.latencies_ms))
    assert results[0] == results[1]


# --------------------------------------------------------------------------
# cancellation and status
# --------------------------------------------------------------------------

async def test_cancel_pulls_a_resting_remainder():
    ex, _ = kalshi_executor()
    state = await ex.submit_order(buy(200, "0.43"))
    assert await ex.cancel_order(state.order.order_id) is True
    assert (await ex.get_status(state.order.order_id)).status == OrderStatus.CANCELLED
    assert state.filled_size == 150          # the fills are kept


async def test_cancelling_a_terminal_order_returns_false_rather_than_raising():
    """A fill that beat the cancel is a race, not an error."""
    ex, _ = kalshi_executor()
    state = await ex.submit_order(buy(100, "0.44"))
    assert state.status == OrderStatus.FILLED
    assert await ex.cancel_order(state.order.order_id) is False


async def test_unknown_order_ids_raise():
    ex, _ = kalshi_executor()
    with pytest.raises(OrderNotFound):
        await ex.cancel_order("nope")
    with pytest.raises(OrderNotFound):
        await ex.get_status("nope")


async def test_open_orders_lists_only_live_ones():
    ex, _ = kalshi_executor()
    await ex.submit_order(buy(100, "0.44"))        # fills
    resting = await ex.submit_order(buy(100, "0.40"))   # rests
    assert [s.order.order_id for s in ex.open_orders()] == [resting.order.order_id]


# --------------------------------------------------------------------------
# fill listeners
# --------------------------------------------------------------------------

async def test_every_fill_reaches_both_sync_and_async_listeners():
    ex, _ = kalshi_executor()
    sync_seen, async_seen = [], []

    async def async_listener(fill):
        async_seen.append(fill)

    ex.add_fill_listener(sync_seen.append)
    ex.add_fill_listener(async_listener)
    await ex.submit_order(buy(200, "0.44"))
    assert len(sync_seen) == 2 and len(async_seen) == 2


async def test_a_raising_listener_does_not_break_the_order():
    """A bookkeeping bug must not leave a leg in an unknown state."""
    ex, _ = kalshi_executor()
    good = []
    ex.add_fill_listener(lambda f: (_ for _ in ()).throw(RuntimeError("boom")))
    ex.add_fill_listener(good.append)
    state = await ex.submit_order(buy(200, "0.44"))
    assert state.status == OrderStatus.FILLED
    assert len(good) == 2


# --------------------------------------------------------------------------
# Polymarket specifics
# --------------------------------------------------------------------------

async def test_polymarket_reads_the_no_book_directly():
    feed = StaticOutcomeBooks({
        "YES": make_book("polymarket", [("0.37", "400")], [("0.38", "400")], "PY"),
        "NO": make_book("polymarket", [("0.62", "400")], [("0.63", "400")], "PN"),
    })
    ex = MockPolymarketExecutor(feed, FREE, tokens={"YES": "PY", "NO": "PN"},
                                config=DETERMINISTIC)
    state = await ex.submit_order(Order(venue="polymarket", market_id="PN", outcome="NO",
                                        side=OrderSide.BUY, size=D(100),
                                        limit_price=D("0.63")))
    assert state.status == OrderStatus.FILLED
    assert state.fills[0].price == D("0.63")


async def test_polymarket_rejects_an_order_carrying_the_wrong_token():
    feed = StaticOutcomeBooks({
        "YES": make_book("polymarket", [("0.37", "400")], [("0.38", "400")], "PY"),
        "NO": make_book("polymarket", [("0.62", "400")], [("0.63", "400")], "PN"),
    })
    ex = MockPolymarketExecutor(feed, FREE, tokens={"YES": "PY", "NO": "PN"},
                                config=DETERMINISTIC)
    state = await ex.submit_order(Order(venue="polymarket", market_id="PN", outcome="YES",
                                        side=OrderSide.BUY, size=D(100),
                                        limit_price=D("0.38")))
    assert state.status == OrderStatus.REJECTED
    assert "YES trades token PY" in state.reject_reason


async def test_polymarket_refuses_an_invalid_book():
    """PolymarketFeed has no sequence numbers, so validity is all there is."""
    feed = StaticOutcomeBooks({
        "YES": make_book("polymarket", [("0.37", "400")], [("0.38", "400")], "PY"),
    }, valid=False)
    ex = MockPolymarketExecutor(feed, FREE, config=DETERMINISTIC)
    state = await ex.submit_order(Order(venue="polymarket", market_id="PY", outcome="YES",
                                        side=OrderSide.BUY, size=D(100),
                                        limit_price=D("0.38")))
    assert state.status == OrderStatus.REJECTED
    assert "invalid" in state.reject_reason


async def test_polymarket_enforces_its_minimum_order_size():
    """The CLOB minimum is 5 shares; a 3-pair arbitrage cannot be executed there."""
    feed = StaticOutcomeBooks({
        "YES": make_book("polymarket", [("0.37", "400")], [("0.38", "400")], "PY"),
    })
    ex = MockPolymarketExecutor(feed, FREE, config=None)   # venue defaults
    order = Order(venue="polymarket", market_id="PY", outcome="YES",
                  side=OrderSide.BUY, size=D(3), limit_price=D("0.38"))
    assert "below polymarket minimum" in ex.preflight(order)


# --------------------------------------------------------------------------
# factory routing
# --------------------------------------------------------------------------

async def test_the_factory_routes_an_order_by_its_venue():
    kalshi, _ = kalshi_executor()
    feed = StaticOutcomeBooks({"YES": make_book("polymarket", [], [("0.38", "400")], "PY")})
    poly = MockPolymarketExecutor(feed, FREE, config=DETERMINISTIC)
    factory = OrderExecutorFactory()
    factory.register_instance(kalshi)
    factory.register_instance(poly)

    assert factory.for_order(buy(1, "0.43")) is kalshi
    assert factory.for_order(Order(venue="polymarket", market_id="PY", outcome="YES",
                                   side=OrderSide.BUY, size=D(1),
                                   limit_price=D("0.38"))) is poly
    assert factory.venues() == ["kalshi", "polymarket"]


async def test_an_unregistered_venue_raises_rather_than_defaulting():
    factory = OrderExecutorFactory()
    with pytest.raises(UnknownVenueError, match="no executor registered"):
        factory.for_venue("nasdaq")


async def test_executors_are_built_once_and_cached():
    """
    An executor owns its order table; a fresh one per call would lose the
    history cancel_order and get_status depend on.
    """
    built = []

    def builder():
        ex, _ = kalshi_executor()
        built.append(ex)
        return ex

    factory = OrderExecutorFactory()
    factory.register("kalshi", builder)
    first = factory.for_venue("kalshi")
    assert factory.for_venue("kalshi") is first
    assert len(built) == 1


async def test_re_registering_a_venue_drops_the_cached_executor():
    factory = OrderExecutorFactory()
    first, _ = kalshi_executor()
    second, _ = kalshi_executor()
    factory.register_instance(first)
    assert factory.for_venue("kalshi") is first
    factory.register_instance(second)
    assert factory.for_venue("kalshi") is second


async def test_one_listener_can_be_attached_to_every_venue_at_once():
    kalshi, _ = kalshi_executor()
    feed = StaticOutcomeBooks({"YES": make_book("polymarket", [], [("0.38", "400")], "PY")})
    poly = MockPolymarketExecutor(feed, FREE, config=DETERMINISTIC)
    factory = OrderExecutorFactory()
    factory.register_instance(kalshi)
    factory.register_instance(poly)

    seen = []
    factory.add_fill_listener_to_all(seen.append)
    await kalshi.submit_order(buy(100, "0.44"))
    await poly.submit_order(Order(venue="polymarket", market_id="PY", outcome="YES",
                                  side=OrderSide.BUY, size=D(100), limit_price=D("0.38")))
    assert {f.venue for f in seen} == {"kalshi", "polymarket"}
