"""
tests/test_arbitrage_strategy.py

The decision and execution half: risk gating, paired dispatch, and what
happens when a pair breaks.

The leg-mismatch cases are the point of this file. A paired arbitrage is
only risk-free while both legs exist, and every one of these scenarios turns
it into a naked directional position unless the unwind path does its job.
"""

import asyncio
from decimal import Decimal

import pytest
from conftest import DETERMINISTIC, FREE, make_book

from engine.arbitrage import DynamicThreshold, kalshi_quotes, polymarket_quotes
from engine.arbitrage_strategy import ArbitrageStrategy
from engine.books import StaticOutcomeBooks
from engine.executors import MockKalshiExecutor, MockPolymarketExecutor, OrderExecutorFactory
from engine.executors.simulator import SimulationConfig
from engine.models import OrderSide, OrderStatus, RiskDecision
from engine.portfolio import AlwaysApprove, SimplePortfolioManager
from engine.sentiment import InMemorySentimentFeed

D = Decimal
pytestmark = pytest.mark.asyncio

NO_THRESHOLD = DynamicThreshold(base=D(0), lam=D(0))

# A 5c/pair edge: Kalshi YES 0.40 x200, Polymarket NO 0.55 x200.
KALSHI_BOOK = ([("0.30", "200")], [("0.40", "200")])
POLY_BOOKS = {"YES": ([("0.40", "200")], [("0.46", "200")]),
              "NO": ([("0.50", "200")], [("0.55", "200")])}


def build(kalshi_book=KALSHI_BOOK, poly_books=POLY_BOOKS, risk=None,
          kalshi_sim=DETERMINISTIC, poly_sim=DETERMINISTIC, sentiment=None,
          threshold=NO_THRESHOLD, max_size=D(100), **kwargs):
    """Wire a complete strategy over hand-built books."""
    kbook = make_book("kalshi", *kalshi_book, "K")
    feed = StaticOutcomeBooks({o: make_book("polymarket", b, a, f"P{o}")
                              for o, (b, a) in poly_books.items()})
    kalshi = MockKalshiExecutor("K", kbook, FREE, config=kalshi_sim)
    poly = MockPolymarketExecutor(feed, FREE, tokens={"YES": "PYES", "NO": "PNO"},
                                  config=poly_sim)
    factory = OrderExecutorFactory()
    factory.register_instance(kalshi)
    factory.register_instance(poly)

    risk = risk or AlwaysApprove()
    factory.add_fill_listener_to_all(risk.record_fill)

    strategy = ArbitrageStrategy(
        "pair",
        lambda: kalshi_quotes("K", kbook, FREE),
        lambda: polymarket_quotes("PYES", "PNO", feed, FREE),
        factory, risk, sentiment=sentiment, threshold=threshold,
        max_size=max_size, **kwargs,
    )
    return strategy, factory, risk, kalshi, poly, kbook, feed


# --------------------------------------------------------------------------
# the happy path
# --------------------------------------------------------------------------

async def test_a_clean_pair_matches_both_legs_and_locks_the_edge():
    strategy, *_ = build()
    result = await strategy.run_tick()
    attempt = result.detail

    assert result.acted and attempt.clean
    assert attempt.matched_pairs == 100
    assert attempt.locked_profit == D("5.00")       # 100 pairs x 5c
    assert attempt.naked_size == 0
    assert attempt.unwound_size == 0


async def test_both_legs_are_dispatched_concurrently_not_one_after_the_other():
    """
    Sequential dispatch would leave the first leg exposed for a full round
    trip. With 100ms of latency on each venue, concurrent dispatch finishes in
    about 100ms and sequential in about 200.
    """
    slow = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                            tick_size=D("0.01"), latency_ms=100.0)
    strategy, *_ = build(kalshi_sim=slow, poly_sim=slow)
    start = asyncio.get_running_loop().time()
    await strategy.run_tick()
    elapsed_ms = (asyncio.get_running_loop().time() - start) * 1000
    assert elapsed_ms < 180


async def test_the_dispatched_limits_are_the_worst_levels_walked():
    strategy, *_ = build()
    attempt = (await strategy.run_tick()).detail
    assert attempt.yes_state.order.limit_price == D("0.40")
    assert attempt.no_state.order.limit_price == D("0.55")
    assert attempt.yes_state.order.side == OrderSide.BUY
    assert attempt.no_state.order.side == OrderSide.BUY


async def test_positions_reach_the_portfolio_through_the_fill_stream():
    risk = SimplePortfolioManager(max_position_per_market=D(10000),
                                  max_notional_per_order=D(10000))
    strategy, *_ = build(risk=risk)
    await strategy.run_tick()
    assert risk.position("kalshi", "K", "YES") == 100
    assert risk.position("polymarket", "PNO", "NO") == 100
    assert risk.cash == D("10000") - D("95")        # 100 pairs at 0.95


# --------------------------------------------------------------------------
# risk gating
# --------------------------------------------------------------------------

async def test_a_veto_on_either_leg_dispatches_neither():
    """
    Sending only the approved leg leaves exactly the naked position the risk
    check just refused — the one case where partially honouring a decision is
    worse than ignoring it.
    """
    class VetoNo:
        def check_order(self, order):
            if order.outcome == "NO":
                return RiskDecision.veto("no appetite for NO")
            return RiskDecision.ok()

        def record_fill(self, fill):
            pass

    strategy, _, _, kalshi, poly, *_ = build(risk=VetoNo())
    attempt = (await strategy.run_tick()).detail
    assert not attempt.dispatched
    assert "risk vetoed the NO leg" in attempt.reason
    assert kalshi.submitted == 0 and poly.submitted == 0


async def test_risk_shrinks_both_legs_to_the_same_size():
    """A shrunk pair is still a pair; shrinking one leg alone would not be."""
    risk = SimplePortfolioManager(max_position_per_market=D(40),
                                  max_notional_per_order=D(10000))
    strategy, *_ = build(risk=risk)
    attempt = (await strategy.run_tick()).detail
    assert attempt.yes_state.order.size == 40
    assert attempt.no_state.order.size == 40
    assert attempt.matched_pairs == 40
    assert attempt.dispatched_size == 40
    # 40 bought against 40 sold is perfectly hedged: a position limit doing its
    # job is not an execution failure
    assert attempt.clean
    assert attempt.locked_profit == D("2.00")


async def test_an_exhausted_position_limit_stops_trading_entirely():
    risk = SimplePortfolioManager(max_position_per_market=D(100),
                                  max_notional_per_order=D(10000))
    strategy, *_ = build(risk=risk)
    first = (await strategy.run_tick()).detail
    assert first.matched_pairs == 100
    second = (await strategy.run_tick()).detail
    assert not second.dispatched
    assert "position limit reached" in second.reason


async def test_an_async_check_order_is_awaited():
    """Person A's risk layer may hit a database; the strategy must not care."""
    class AsyncRisk:
        def __init__(self):
            self.calls = 0

        async def check_order(self, order):
            self.calls += 1
            await asyncio.sleep(0)
            return RiskDecision.ok()

        def record_fill(self, fill):
            pass

    risk = AsyncRisk()
    strategy, *_ = build(risk=risk)
    attempt = (await strategy.run_tick()).detail
    assert risk.calls == 2 and attempt.matched_pairs == 100


# --------------------------------------------------------------------------
# pre-flight: refuse a leg the venue would certainly reject
# --------------------------------------------------------------------------

async def test_a_leg_below_the_venue_minimum_aborts_the_pair():
    """
    Polymarket's CLOB minimum is 5 shares. A 3-pair trade would fill on Kalshi
    and be rejected on Polymarket, i.e. a guaranteed unwind. Declining is
    strictly better than trading and then repairing.
    """
    poly_defaults = None        # use MockPolymarketExecutor's real venue profile
    strategy, _, _, kalshi, poly, *_ = build(poly_sim=poly_defaults, max_size=D(3))
    attempt = (await strategy.run_tick()).detail
    assert not attempt.dispatched
    assert "would be rejected" in attempt.reason
    assert "below polymarket minimum" in attempt.reason
    assert kalshi.submitted == 0 and poly.submitted == 0
    assert strategy.preflight_rejections == 1


# --------------------------------------------------------------------------
# leg mismatch: the reason this strategy needs an unwind path at all
# --------------------------------------------------------------------------

async def test_a_rejected_leg_is_unwound_and_the_loss_is_booked():
    """
    The NO leg is rejected outright, so 100 YES are held naked. The unwind
    sells them back into the YES bids (0.30, two ticks of slack -> 0.28) for a
    real loss: bought at 0.40, sold at 0.30, = -$10.00.
    """
    reject = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                              tick_size=D("0.01"), reject_prob=1.0)
    strategy, *_ = build(poly_sim=reject)
    attempt = (await strategy.run_tick()).detail

    assert attempt.dispatched
    assert attempt.no_state.status == OrderStatus.REJECTED
    assert attempt.matched_pairs == 0
    assert attempt.unwound_size == 100
    assert attempt.naked_size == 0
    assert attempt.locked_profit == D("-10.00")
    assert not attempt.clean


async def test_the_unwind_sells_the_right_outcome_on_the_right_venue():
    reject = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                              tick_size=D("0.01"), reject_prob=1.0)
    strategy, *_ = build(poly_sim=reject)
    attempt = (await strategy.run_tick()).detail
    unwind = attempt.unwind_states[0].order
    assert unwind.venue == "kalshi" and unwind.outcome == "YES"
    assert unwind.side == OrderSide.SELL
    assert unwind.is_unwind
    assert unwind.limit_price == D("0.28")      # best bid 0.30, 2 ticks of slack


async def test_only_the_unmatched_excess_is_unwound():
    """
    The NO book holds only 60, so the pair is sized to 60 and nothing is
    unwound. This separates depth-limited sizing (handled before dispatch)
    from a genuine mismatch (handled after).
    """
    poly = {"YES": ([("0.40", "200")], [("0.46", "200")]),
            "NO": ([("0.50", "200")], [("0.55", "60")])}
    strategy, *_ = build(poly_books=poly)
    attempt = (await strategy.run_tick()).detail
    assert attempt.opportunity.size == 60
    assert attempt.matched_pairs == 60
    assert attempt.unwound_size == 0 and attempt.clean


async def test_a_partial_leg_is_unwound_down_to_the_matched_size():
    """
    A forced stochastic partial on the Polymarket leg leaves YES over-filled.
    Whatever the split, the invariant holds: matched + unwound + naked covers
    the over-filled leg exactly, and matched pairs are the only hedged ones.
    """
    partial = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                               tick_size=D("0.01"), partial_fill_prob=1.0,
                               partial_fill_min_fraction=0.3, seed=11)
    strategy, *_ = build(poly_sim=partial)
    attempt = (await strategy.run_tick()).detail

    yes_filled = attempt.yes_state.filled_size
    no_filled = attempt.no_state.filled_size
    assert no_filled < yes_filled                       # the partial happened
    assert attempt.matched_pairs == no_filled
    assert attempt.unwound_size + attempt.naked_size == yes_filled - no_filled


async def test_an_unwind_with_no_bid_to_sell_into_is_reported_as_naked():
    """
    An empty bid side means the excess cannot be flattened at all. It must be
    recorded, not silently dropped: the position exists either way, and
    naked_size is what tells an operator about it.
    """
    reject = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                              tick_size=D("0.01"), reject_prob=1.0)
    strategy, *_ = build(kalshi_book=([], [("0.40", "200")]), poly_sim=reject)
    attempt = (await strategy.run_tick()).detail
    assert attempt.matched_pairs == 0
    assert attempt.naked_size == 100
    assert attempt.naked_leg == "kalshi/YES"
    assert attempt.unwind_states == []


async def test_a_resting_remainder_is_cancelled():
    """Nothing may be left live once an attempt is reconciled."""
    thin = {"YES": ([("0.40", "200")], [("0.46", "200")]),
            "NO": ([("0.50", "200")], [("0.55", "60"), ("0.99", "500")])}
    partial = SimulationConfig(min_order_size=D(1), size_increment=D(1),
                               tick_size=D("0.01"), partial_fill_prob=1.0,
                               partial_fill_min_fraction=0.3, seed=5)
    strategy, factory, *_ = build(poly_books=thin, poly_sim=partial)
    await strategy.run_tick()
    for venue in factory.venues():
        assert factory.for_venue(venue).open_orders() == []


# --------------------------------------------------------------------------
# no edge, no action
# --------------------------------------------------------------------------

async def test_no_edge_means_no_orders():
    expensive = ([("0.30", "200")], [("0.50", "200")])      # 0.50 + 0.55 = 1.05
    strategy, _, _, kalshi, poly, *_ = build(kalshi_book=expensive)
    result = await strategy.run_tick()
    assert not result.acted and result.reason == "no edge"
    assert kalshi.submitted == 0 and poly.submitted == 0


async def test_an_untradable_venue_stops_the_tick_before_pricing():
    strategy, *_ = build()
    strategy.quotes_b = lambda: None
    result = await strategy.run_tick()
    assert not result.acted
    assert "not tradable" in result.reason
    assert strategy.evaluations == 0


async def test_dry_run_prices_the_edge_but_sends_nothing():
    strategy, _, _, kalshi, poly, *_ = build(dry_run=True)
    result = await strategy.run_tick()
    assert not result.acted and result.reason == "dry run"
    assert result.detail.size == 100 and result.detail.profitable
    assert kalshi.submitted == 0 and poly.submitted == 0
    assert strategy.opportunities_seen == 1


async def test_a_raising_strategy_is_contained_rather_than_killing_the_loop():
    strategy, *_ = build()
    strategy.quotes_a = lambda: (_ for _ in ()).throw(RuntimeError("feed exploded"))
    result = await strategy.run_tick()
    assert not result.acted
    assert "feed exploded" in result.reason


# --------------------------------------------------------------------------
# the dynamic threshold, end to end through the strategy
# --------------------------------------------------------------------------

async def test_a_news_surge_widens_the_threshold_and_declines_a_thin_edge():
    """
    Use case from the proposal, at the strategy level.

    The book is unchanged throughout: Kalshi YES 0.44 + Polymarket NO 0.55 =
    0.99, a 1c/pair edge. Calm sentiment accepts it; four contradictory
    headlines push the sentiment standard deviation up, dP_min past 1c, and
    the same book is declined.
    """
    sentiment = InMemorySentimentFeed()
    thin = ([("0.30", "200")], [("0.44", "200")])
    strategy, _, _, kalshi, poly, *_ = build(
        kalshi_book=thin, sentiment=sentiment,
        threshold=DynamicThreshold(base=D("0.002"), lam=D("0.05")),
        risk=SimplePortfolioManager(max_position_per_market=D(100000),
                                    max_notional_per_order=D(100000),
                                    max_gross_exposure=D(100000)),
    )

    calm = await strategy.run_tick()
    assert calm.acted, "a 1c edge should trade when sentiment is quiet"
    assert strategy.last_threshold == D("0.002")

    for score in ("0.9", "-0.8", "0.85", "-0.75"):
        sentiment.push("pair", score, headline="contradictory wire copy")

    surged = await strategy.run_tick()
    assert not surged.acted
    assert strategy.last_threshold > D("0.01")
    assert "dP_min" in surged.reason
    assert strategy.declined_by_threshold == 1


async def test_steady_sentiment_does_not_widen_the_threshold():
    """
    It is the VOLATILITY that widens dP_min, not the level. A market that has
    been confidently bullish all morning is not dangerous to arbitrage.
    """
    sentiment = InMemorySentimentFeed()
    for _ in range(6):
        sentiment.push("pair", "0.9", headline="same steady view")
    strategy, *_ = build(sentiment=sentiment,
                         threshold=DynamicThreshold(base=D("0.002"), lam=D("0.05")))
    result = await strategy.run_tick()
    assert result.acted
    assert strategy.last_threshold == D("0.002")


async def test_no_sentiment_feed_leaves_the_threshold_at_its_base():
    """Absence of sentiment data is not evidence of calm, but it is not panic either."""
    strategy, *_ = build(sentiment=None,
                         threshold=DynamicThreshold(base=D("0.002"), lam=D("0.05")))
    await strategy.run_tick()
    assert strategy.last_threshold == D("0.002")
