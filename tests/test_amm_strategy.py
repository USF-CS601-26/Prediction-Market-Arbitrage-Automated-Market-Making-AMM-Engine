"""
tests/test_amm_strategy.py

AMMStrategy: the reservation-price and spread formulas, quote clamping, and
the strategy's behaviour against hand-built books and deterministic mock
venues — inventory skew, sentiment skew, cancel-and-replace, risk vetoes,
expiry, and resting quotes that fill when the book trades through them.
"""

import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from engine.amm_strategy import (AMMConfig, AMMStrategy, MidVolatility, quote_prices,
                                 reservation_and_spread, sentiment_features)
from engine.books import StaticOutcomeBooks
from engine.executors import MockKalshiExecutor, MockPolymarketExecutor, OrderExecutorFactory
from engine.models import Fill, OrderSide, OrderStatus
from engine.portfolio_manager import PortfolioManager, RiskLimits
from engine.sentiment import InMemorySentimentFeed

from conftest import DETERMINISTIC, FREE, make_book

D = Decimal
T0 = datetime(2026, 10, 8, 6, 30, tzinfo=timezone.utc)
CFG = AMMConfig()


@pytest.fixture(autouse=True)
def fresh_singleton():
    PortfolioManager._discard_instance()
    yield
    PortfolioManager._discard_instance()


# --------------------------------------------------------------------------
# the formulas
# --------------------------------------------------------------------------

def test_the_spread_matches_the_formula_by_hand():
    # gamma 2, kappa 100, sigma 0.02, tau 1:  2 x 0.0004 + (2/2) ln(1.02)
    r, delta = reservation_and_spread(D("0.50"), 0.0, 0.02, 1.0, D(0), D(0), CFG)
    assert r == D("0.5")
    assert float(delta) == pytest.approx(0.0008 + math.log(1.02), abs=1e-9)


@pytest.mark.parametrize("q_units, direction", [(2.0, -1), (-2.0, 1), (0.0, 0)])
def test_inventory_skews_the_reservation_price_against_the_position(q_units, direction):
    """Long inventory lowers r (sell it down); short raises it; flat leaves r at mid."""
    r, _ = reservation_and_spread(D("0.50"), q_units, 0.05, 1.0, D(0), D(0), CFG)
    assert (r > D("0.50")) - (r < D("0.50")) == direction
    # q x gamma x sigma^2 x tau = 2 x 2 x 0.0025 x 1 = 0.01
    if q_units:
        assert abs(r - D("0.50")) == D("0.01")


@pytest.mark.parametrize("sentiment, direction", [(D("0.9"), 1), (D("-0.9"), -1)])
def test_sentiment_shifts_the_reservation_price_in_its_own_direction(sentiment, direction):
    r, _ = reservation_and_spread(D("0.50"), 0.0, 0.02, 1.0, sentiment, D(1), CFG)
    assert (r > D("0.50")) - (r < D("0.50")) == direction
    assert abs(r - D("0.50")) == D("0.018")           # k 0.02 x 0.9 x 1


def test_zero_confidence_means_no_sentiment_shift():
    r, _ = reservation_and_spread(D("0.50"), 0.0, 0.02, 1.0, D("0.9"), D(0), CFG)
    assert r == D("0.5")


def test_a_shorter_horizon_narrows_the_spread_and_weakens_the_inventory_push():
    r_far, d_far = reservation_and_spread(D("0.5"), 2.0, 0.05, 1.0, D(0), D(0), CFG)
    r_near, d_near = reservation_and_spread(D("0.5"), 2.0, 0.05, 0.1, D(0), D(0), CFG)
    assert d_near < d_far
    assert abs(r_near - D("0.5")) < abs(r_far - D("0.5"))


# --------------------------------------------------------------------------
# turning a quote into prices
# --------------------------------------------------------------------------

def test_quotes_are_tick_aligned_outward():
    bid, ask = quote_prices(D("0.5"), D("0.0206"), CFG)
    assert (bid, ask) == (D("0.48"), D("0.52"))       # 0.4897 down, 0.5103 up


def test_quotes_are_clamped_to_the_prediction_market_bounds():
    bid, ask = quote_prices(D("0.995"), D("0.02"), CFG)
    assert ask == D("0.99") and bid == D("0.98")
    bid, ask = quote_prices(D("0.004"), D("0.02"), CFG)
    assert bid == D("0.01") and ask == D("0.02")


def test_post_only_quotes_never_cross_the_book():
    # the model wants to bid 0.43 into a 0.41/0.42 book: pulled back to 0.41
    bid, ask = quote_prices(D("0.44"), D("0.02"), CFG, best_bid=D("0.41"), best_ask=D("0.42"))
    assert bid == D("0.41")
    assert ask == D("0.45")


# --------------------------------------------------------------------------
# inputs: volatility and sentiment
# --------------------------------------------------------------------------

def test_a_static_mid_reads_as_the_volatility_floor():
    vol = MidVolatility(timedelta(hours=1), timedelta(minutes=1), floor=0.02, cap=0.5)
    for i in range(30):
        vol.add(T0 + timedelta(minutes=i), D("0.5"))
    assert vol.sigma() == 0.02


def test_realized_volatility_per_day():
    """A 1c move every minute is (0.01^2 x 1440) per day -> sigma = 0.01 x sqrt(1440)."""
    vol = MidVolatility(timedelta(hours=2), timedelta(minutes=1), floor=0.0, cap=10.0)
    for i in range(61):
        vol.add(T0 + timedelta(minutes=i), D("0.50") + D("0.01") * (i % 2))
    assert vol.sigma() == pytest.approx(0.01 * math.sqrt(1440), rel=1e-9)


def test_confidence_is_high_for_agreeing_headlines_and_low_for_contradictory_ones():
    feed = InMemorySentimentFeed(now_fn=lambda: T0)
    for s in ("0.8", "0.9", "0.85"):
        feed.push("P", s, timestamp=T0)
    level, agreeing, n = sentiment_features(feed, "P")
    assert n == 3 and level > D("0.8") and agreeing > D("0.9")

    noisy = InMemorySentimentFeed(now_fn=lambda: T0)
    for s in ("0.9", "-0.9", "0.85", "-0.85"):
        noisy.push("P", s, timestamp=T0)
    level, contradictory, _ = sentiment_features(noisy, "P")
    assert abs(level) < D("0.05") and contradictory < D("0.2")


# --------------------------------------------------------------------------
# the strategy against mock venues
# --------------------------------------------------------------------------

TICKER = "KX"


class Rig:
    """A Kalshi book, its deterministic mock venue, the PortfolioManager and an AMM."""

    def __init__(self, bids=(("0.48", "500"),), asks=(("0.52", "500"),),
                 limits: RiskLimits | None = None, **cfg):
        self.book = make_book("kalshi", list(bids), list(asks), TICKER)
        self.pm = PortfolioManager().reset(limits or RiskLimits())
        self.pm.register_market("kalshi", "PAIR", {"YES": TICKER, "NO": TICKER})
        self.venue = MockKalshiExecutor(TICKER, self.book, FREE, config=DETERMINISTIC)
        self.venue.add_fill_listener(self.pm.record_fill)
        self.factory = OrderExecutorFactory()
        self.factory.register_instance(self.venue)
        self.sentiment = InMemorySentimentFeed(now_fn=lambda: T0)
        self.amm = AMMStrategy("PAIR", "kalshi", "PAIR", TICKER, TICKER, lambda: self.book,
                               self.factory, self.pm, sentiment=self.sentiment,
                               config=AMMConfig(**cfg))

    async def tick(self, at=T0):
        return await self.amm.run_tick(at)

    def resting(self):
        return {s.order.order_id: s for s in self.venue.open_orders()}


@pytest.mark.asyncio
async def test_a_flat_amm_quotes_both_sides_around_the_mid():
    rig = Rig()
    result = await rig.tick()
    quote = result.detail
    assert result.acted and quote.action == "requote"
    assert quote.reservation == D("0.5")
    assert (quote.bid, quote.ask) == (D("0.48"), D("0.52"))

    orders = sorted((s.order for s in rig.resting().values()), key=lambda o: o.outcome == "NO")
    bid_order, ask_order = orders
    assert (bid_order.outcome, bid_order.side, bid_order.limit_price) == ("YES", OrderSide.BUY, D("0.48"))
    # offering YES at 0.52 without holding any = bidding for NO at 0.48
    assert (ask_order.outcome, ask_order.side, ask_order.limit_price) == ("NO", OrderSide.BUY, D("0.48"))


@pytest.mark.asyncio
async def test_long_inventory_lowers_both_quotes_and_sells_what_it_holds():
    flat = (await Rig().tick()).detail
    rig = Rig()
    rig.pm.record_fill(Fill(order_id="seed", venue="kalshi", market_id=TICKER, outcome="YES",
                            side=OrderSide.BUY, size=D(500), price=D("0.50"), fee=D(0)))
    long = (await rig.tick()).detail
    assert long.inventory == 500
    assert long.reservation < flat.reservation
    assert long.ask <= flat.ask and long.bid <= flat.bid
    ask_order = next(s.order for s in rig.resting().values() if s.order.side == OrderSide.SELL)
    assert (ask_order.outcome, ask_order.limit_price) == ("YES", long.ask)


@pytest.mark.asyncio
async def test_a_hedged_cross_venue_position_is_not_inventory_to_skew_on():
    """
    The arbitrage strategy holds Kalshi NO against Polymarket YES. Per venue
    that is -100 and +100; for the event it is flat. By default the AMM skews
    on the event, so it does not lean to unwind one leg of a hedge.
    """
    def seed(pm):
        pm.register_market("polymarket", "PAIR", {"YES": "PY", "NO": "PN"})
        for venue, market, outcome in (("kalshi", TICKER, "NO"), ("polymarket", "PY", "YES")):
            pm.record_fill(Fill(order_id=f"arb-{venue}", venue=venue, market_id=market,
                                outcome=outcome, side=OrderSide.BUY, size=D(100),
                                price=D("0.5"), fee=D(0)))

    event = Rig()
    seed(event.pm)
    assert (await event.tick()).detail.inventory == 0

    PortfolioManager._discard_instance()
    venue = Rig(inventory_scope="venue")
    seed(venue.pm)
    quote = (await venue.tick()).detail
    assert quote.inventory == -100 and quote.reservation > quote.mid


@pytest.mark.asyncio
async def test_a_positive_headline_raises_the_quotes():
    calm = (await Rig().tick()).detail
    rig = Rig()
    rig.sentiment.push("PAIR", "0.9", headline="expected to step down", timestamp=T0)
    skewed = (await rig.tick()).detail
    assert skewed.sentiment == D("0.9") and skewed.confidence == 1
    assert skewed.reservation - calm.reservation == D("0.018")
    assert skewed.bid > calm.bid


@pytest.mark.asyncio
async def test_quotes_are_replaced_only_when_the_reservation_moves_past_the_threshold():
    rig = Rig()
    await rig.tick()
    first_ids = set(rig.resting())

    result = await rig.tick(T0 + timedelta(seconds=1))          # nothing changed
    assert not result.acted and result.reason == "quotes unchanged"
    assert set(rig.resting()) == first_ids

    rig.sentiment.push("PAIR", "0.9", timestamp=T0)              # r moves 1.8c
    result = await rig.tick(T0 + timedelta(seconds=2))
    assert result.acted and len(result.detail.cancelled) == 2
    assert set(rig.resting()).isdisjoint(first_ids)
    assert rig.amm.cancels == 2


@pytest.mark.asyncio
async def test_a_side_the_risk_layer_vetoes_is_not_quoted():
    """At the position limit the AMM stops adding (no bid) but keeps offering to sell down."""
    rig = Rig(limits=RiskLimits(max_position_per_market=D(100)))
    rig.pm.record_fill(Fill(order_id="seed", venue="kalshi", market_id=TICKER, outcome="YES",
                            side=OrderSide.BUY, size=D(100), price=D("0.50"), fee=D(0)))
    quote = (await rig.tick()).detail
    sides = {(s.order.outcome, s.order.side) for s in quote.placed}
    assert sides == {("YES", OrderSide.SELL)}
    assert "bid: risk: position limit" in quote.reason
    assert rig.amm.risk_vetoes == 1


@pytest.mark.asyncio
async def test_when_every_side_is_vetoed_the_amm_quotes_nothing():
    rig = Rig(limits=RiskLimits(max_total_exposure=D(0)))
    result = await rig.tick()
    assert not result.detail.placed
    assert result.detail.action == "stop"
    assert result.detail.reason.startswith("not quoting")
    assert not rig.resting()


@pytest.mark.asyncio
async def test_the_risk_layer_can_shrink_a_quote():
    rig = Rig(limits=RiskLimits(max_notional_per_order=D("2.40")))   # 5 @ 0.48
    quote = (await rig.tick()).detail
    assert {s.order.size for s in quote.placed} == {D(5)}


@pytest.mark.asyncio
async def test_the_amm_stops_quoting_and_cancels_near_expiry():
    expiry = T0 + timedelta(hours=3)
    rig = Rig(expiry=expiry, stop_before_expiry=timedelta(hours=1))
    await rig.tick()
    assert len(rig.resting()) == 2

    result = await rig.tick(expiry - timedelta(minutes=30))
    assert result.acted and result.detail.action == "stop"
    assert not rig.resting()
    assert "expiry" in rig.amm.stopped_reason


@pytest.mark.asyncio
async def test_time_to_expiry_shortens_the_horizon():
    expiry = T0 + timedelta(hours=6)
    rig = Rig(expiry=expiry)
    quote = (await rig.tick()).detail
    assert quote.tau_days == D("0.25")


@pytest.mark.asyncio
async def test_a_resting_bid_fills_when_the_book_trades_through_it_and_the_amm_then_leans_short():
    rig = Rig()
    first = (await rig.tick()).detail                  # bids 0.48 for 10
    # the offer side collapses to 0.47: our 0.48 bid is now marketable
    rig.book.asks.clear()
    rig.book.asks[D("0.47")] = D("500")
    assert await rig.venue.match_resting() == 1
    assert rig.pm.get_inventory("PAIR", venue="kalshi").net == 10

    rig.book.bids.clear()
    rig.book.bids[D("0.46")] = D("500")
    second = (await rig.tick(T0 + timedelta(seconds=5))).detail
    assert second.inventory == 10
    # 10 contracts = 1 unit: r sits below the new mid by 1 x 2 x 0.0004
    assert second.mid - second.reservation == D("0.0008")
    assert first.inventory == 0


@pytest.mark.asyncio
async def test_a_stale_book_cancels_every_quote():
    rig = Rig()
    await rig.tick()
    rig.amm.book_provider = lambda: None
    result = await rig.tick(T0 + timedelta(seconds=1))
    assert result.reason == "book unavailable"
    assert not rig.resting()


@pytest.mark.asyncio
async def test_on_polymarket_the_ask_side_bids_for_the_no_token():
    pm = PortfolioManager().reset()
    pm.register_market("polymarket", "PAIR", {"YES": "PY", "NO": "PN"})
    feed = StaticOutcomeBooks({
        "YES": make_book("polymarket", [("0.37", "400")], [("0.38", "400")], "PY"),
        "NO": make_book("polymarket", [("0.62", "400")], [("0.63", "400")], "PN"),
    })
    venue = MockPolymarketExecutor(feed, FREE, tokens={"YES": "PY", "NO": "PN"},
                                   config=DETERMINISTIC)
    factory = OrderExecutorFactory()
    factory.register_instance(venue)
    amm = AMMStrategy("PAIR", "polymarket", "PAIR", "PY", "PN", lambda: feed.book("YES"),
                      factory, pm)
    quote = (await amm.run_tick(T0)).detail
    by_outcome = {s.order.outcome: s.order for s in quote.placed}
    assert by_outcome["YES"].market_id == "PY" and by_outcome["YES"].limit_price == quote.bid
    assert by_outcome["NO"].market_id == "PN" and by_outcome["NO"].limit_price == 1 - quote.ask
    assert all(s.status == OrderStatus.OPEN for s in quote.placed)
