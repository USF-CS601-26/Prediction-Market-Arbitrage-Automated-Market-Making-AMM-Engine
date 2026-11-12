"""
tests/test_portfolio_manager.py

The PortfolioManager: singleton behaviour under concurrent access, position
accounting (average cost, realized and unrealized P&L), inventory per market,
and every limit — including the reservations that stop two strategies or two
venues being approved against the same room.

Numbers are small and hand-checkable.
"""

import threading
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest

from engine.models import Fill, Order, OrderSide
from engine.portfolio_manager import (Inventory, PortfolioManager, RiskLimits,
                                      get_portfolio_manager)

D = Decimal
BUY, SELL = OrderSide.BUY, OrderSide.SELL


@pytest.fixture(autouse=True)
def fresh_singleton():
    PortfolioManager._discard_instance()
    yield
    PortfolioManager._discard_instance()


def order(venue="kalshi", market="T", outcome="YES", side=BUY, size="10", price="0.40", **kw):
    return Order(venue=venue, market_id=market, outcome=outcome, side=side,
                 size=D(size), limit_price=D(price), **kw)


def fill(venue="kalshi", market="T", outcome="YES", side=BUY, size="10", price="0.40",
         fee="0", order_id="o-1"):
    return Fill(order_id=order_id, venue=venue, market_id=market, outcome=outcome,
                side=side, size=D(size), price=D(price), fee=D(fee))


def pm(**limits) -> PortfolioManager:
    cash = limits.pop("cash", D("10000"))
    return PortfolioManager().reset(RiskLimits(**{k: D(str(v)) for k, v in limits.items()}),
                                    cash=cash)


# --------------------------------------------------------------------------
# singleton
# --------------------------------------------------------------------------

def test_every_construction_returns_the_same_instance():
    a = PortfolioManager()
    assert PortfolioManager() is a
    assert PortfolioManager.instance() is a
    assert get_portfolio_manager() is a


def test_concurrent_construction_from_many_threads_yields_one_instance():
    """32 threads released at once all race through __new__; one object survives."""
    barrier = threading.Barrier(32)

    def build(_):
        barrier.wait()
        return id(PortfolioManager())

    with ThreadPoolExecutor(max_workers=32) as pool:
        ids = set(pool.map(build, range(32)))
    assert len(ids) == 1


def test_later_constructor_arguments_are_ignored_and_reset_applies_them(caplog):
    first = PortfolioManager(RiskLimits(max_position_per_market=D(5)))
    second = PortfolioManager(RiskLimits(max_position_per_market=D(99)))
    assert second is first
    assert first.limits.max_position_per_market == 5
    assert "ignored" in caplog.text
    first.reset(RiskLimits(max_position_per_market=D(99)))
    assert first.limits.max_position_per_market == 99


def test_concurrent_fills_from_many_threads_are_all_booked_exactly():
    m = pm()
    n_threads, per_thread = 8, 250

    def book_fills(t):
        for i in range(per_thread):
            m.record_fill(fill(size="1", price="0.40", order_id=f"t{t}-{i}"))

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        list(pool.map(book_fills, range(n_threads)))

    total = n_threads * per_thread
    assert m.position("kalshi", "T", "YES") == total
    assert len(m.fills) == total
    assert m.cash == D("10000") - D("0.40") * total


def test_concurrent_risk_checks_cannot_over_allocate():
    """
    50 threads each ask for 10 contracts against a 100-contract limit at the
    same moment. Reservations make the approvals add up to the limit exactly,
    never more — without them every thread would see an empty book and all 50
    would be approved.
    """
    m = pm(max_position_per_market=100)
    barrier = threading.Barrier(50)

    def ask(i):
        barrier.wait()
        decision = m.check_order(order(size="10"))
        if not decision.approved:
            return D(0)
        return decision.max_size if decision.max_size is not None else D(10)

    with ThreadPoolExecutor(max_workers=50) as pool:
        approved = list(pool.map(ask, range(50)))
    assert sum(approved) == 100


# --------------------------------------------------------------------------
# accounting
# --------------------------------------------------------------------------

def test_average_cost_realized_and_unrealized_pnl():
    m = pm()
    m.record_fill(fill(size="100", price="0.40", order_id="a"))
    m.record_fill(fill(size="100", price="0.50", order_id="b"))
    inv = m.get_inventory("T", venue="kalshi")
    assert inv.yes == 200
    assert inv.yes_avg_cost == D("0.45")

    m.record_fill(fill(side=SELL, size="50", price="0.60", order_id="c"))
    assert m.realized_pnl() == D("7.50")             # 50 x (0.60 - 0.45)

    m.set_mark("kalshi", "T", "YES", D("0.55"))
    assert m.unrealized_pnl() == D("15.00")          # 150 x (0.55 - 0.45)
    assert m.get_inventory("T", venue="kalshi").yes_avg_cost == D("0.45")


def test_fees_are_realized_when_paid_and_reduce_cash():
    m = pm()
    m.record_fill(fill(size="10", price="0.40", fee="0.17"))
    assert m.realized_pnl() == D("-0.17")
    assert m.fees_paid == D("0.17")
    assert m.cash == D("10000") - D("4.00") - D("0.17")


def test_selling_through_zero_opens_a_short_at_the_fill_price():
    m = pm()
    m.record_fill(fill(size="10", price="0.40", order_id="a"))
    m.record_fill(fill(side=SELL, size="15", price="0.50", order_id="b"))
    assert m.position("kalshi", "T", "YES") == -5
    assert m.realized_pnl() == D("1.00")             # 10 x (0.50 - 0.40)
    assert m.get_inventory("T", venue="kalshi").yes_avg_cost == D("0.50")


def test_inventory_joins_a_polymarket_markets_two_tokens():
    m = pm()
    m.register_market("polymarket", "PAIR", {"YES": "tok-yes", "NO": "tok-no"})
    m.register_market("kalshi", "PAIR", {"YES": "KX", "NO": "KX"})
    m.record_fill(fill("polymarket", "tok-yes", "YES", size="30", price="0.38", order_id="a"))
    m.record_fill(fill("polymarket", "tok-no", "NO", size="10", price="0.62", order_id="b"))
    m.record_fill(fill("kalshi", "KX", "NO", size="20", price="0.59", order_id="c"))

    poly = m.get_inventory("PAIR", venue="polymarket")
    assert isinstance(poly, Inventory)
    assert (poly.yes, poly.no, poly.net) == (30, 10, 20)
    # a raw token id resolves to its registered market
    assert m.get_inventory("tok-no", venue="polymarket").net == 20

    everywhere = m.get_inventory("PAIR")
    assert (everywhere.yes, everywhere.no) == (30, 30)


def test_exposure_is_worst_case_loss_so_a_yes_no_pair_is_nearly_riskless():
    m = pm()
    m.record_fill(fill(outcome="YES", size="100", price="0.40", order_id="a"))
    assert m.exposure() == D("40.00")
    m.record_fill(fill(outcome="NO", size="100", price="0.55", order_id="b"))
    # paid 95 for a certain 100
    assert m.exposure() == 0


# --------------------------------------------------------------------------
# limits
# --------------------------------------------------------------------------

def test_the_position_limit_shrinks_an_order_to_what_fits():
    m = pm(max_position_per_market=100)
    m.record_fill(fill(size="80"))
    decision = m.check_order(order(size="50"))
    assert decision.approved and decision.max_size == 20


def test_a_full_position_vetoes_further_buys_with_a_reason():
    m = pm(max_position_per_market=100)
    m.record_fill(fill(size="100"))
    decision = m.check_order(order(size="10"))
    assert not decision.approved
    assert "position limit" in decision.reason
    assert m.rejections and m.rejections[-1][1] == decision.reason


def test_the_per_order_notional_limit_shrinks_the_order():
    m = pm(max_notional_per_order=20)
    decision = m.check_order(order(size="100", price="0.40"))    # $40 asked
    assert decision.max_size == 50


def test_the_market_exposure_limit():
    m = pm(max_exposure_per_market=30)
    decision = m.check_order(order(size="100", price="0.40"))    # $40 at risk
    assert decision.max_size == 75                                # $30 / 0.40


def test_the_venue_exposure_limit_spans_that_venues_markets():
    m = pm(max_exposure_per_venue=50)
    m.record_fill(fill(market="A", size="100", price="0.40"))     # $40 on kalshi
    decision = m.check_order(order(market="B", size="100", price="0.40"))
    assert decision.max_size == 25                                # $10 left / 0.40


def test_the_total_limit_blocks_over_allocation_across_exchanges():
    """
    Kalshi's order is approved and reserves its $40. Polymarket's, checked a
    moment later and before Kalshi's has filled, only gets the $10 left —
    the reservation, not a fill, is what it sees.
    """
    m = pm(max_total_exposure=50)
    assert m.check_order(order("kalshi", "K", size="100", price="0.40")).max_size is None
    decision = m.check_order(order("polymarket", "P", size="100", price="0.40"))
    assert decision.max_size == 25


def test_a_fill_consumes_its_reservation_instead_of_counting_twice():
    m = pm(max_total_exposure=50)
    o = order(size="100", price="0.40")
    m.check_order(o)
    m.record_fill(fill(size="100", price="0.40", order_id=o.order_id))
    assert o.order_id not in m.reservations
    assert m.exposure() == D("40.00")


def test_a_resting_order_on_one_side_does_not_make_the_other_side_look_hedged():
    """
    A strategy rests a bid for 100 YES and another for NO. Either can fill
    without the other, so the NO order is judged as if the YES bid never
    fills: 200 NO would leave net -200 against a 100 limit.
    """
    m = pm(max_position_per_market=100)
    assert m.check_order(order(outcome="YES", size="100", price="0.48")).max_size is None
    decision = m.check_order(order(outcome="NO", size="200", price="0.48"))
    assert decision.max_size == 100


def test_reconciling_releases_reservations_of_orders_that_are_gone():
    m = pm(max_total_exposure=50)
    live, gone = order(market="A", size="50"), order(market="B", size="50")
    m.check_order(live)
    m.check_order(gone)
    assert m.exposure() == D("40.00")
    released = m.reconcile_reservations({live.order_id})
    assert released == 1
    assert m.exposure() == D("20.00")


def test_a_risk_reducing_order_passes_a_breached_limit():
    """At the position limit, selling down or buying the hedge must still be allowed."""
    m = pm(max_position_per_market=100, max_exposure_per_market=40)
    m.record_fill(fill(size="100", price="0.40"))
    assert not m.check_order(order(size="10")).approved          # adding is blocked
    assert m.check_order(order(side=SELL, size="10", price="0.45")).approved
    assert m.check_order(order(outcome="NO", size="10", price="0.55")).approved


def test_unwind_orders_are_always_approved():
    m = pm(max_position_per_market=1)
    m.record_fill(fill(size="1"))
    assert m.check_order(order(size="500", is_unwind=True)).approved


def test_cash_can_be_split_per_venue():
    m = pm(cash={"kalshi": D("10"), "polymarket": D("1000")})
    assert m.check_order(order("kalshi", size="100", price="0.40")).max_size == 25
    assert m.check_order(order("polymarket", "P", size="100", price="0.40")).max_size is None


def test_shared_cash_counts_pending_buys():
    m = pm(cash=D("50"))
    m.check_order(order("kalshi", "K", size="100", price="0.40"))    # reserves $40
    decision = m.check_order(order("polymarket", "P", size="100", price="0.40"))
    assert decision.max_size == 25
    assert "cash" not in (decision.reason or "")
