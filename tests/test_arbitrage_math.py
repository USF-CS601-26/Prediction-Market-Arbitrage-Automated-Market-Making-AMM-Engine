"""
tests/test_arbitrage_math.py

The complementary-pair arithmetic, on fixtures small enough to check by hand.

Every expected number in this file is derived in a comment, because the whole
value of this module is that it says no to trades that look profitable. A
test that only asserts "net_profit > 0" would pass against a detector that
systematically overstates the edge.
"""

from decimal import Decimal

import pytest
from conftest import FREE, KALSHI_SCHEDULE, make_book
from cross_venue_spread import edge_for_direction
from kalshi_fees import taker_fee

from engine.arbitrage import (DynamicThreshold, OutcomeQuote, SlippageModel, evaluate,
                             evaluate_directions, kalshi_quotes, polymarket_quotes)
from engine.books import StaticOutcomeBooks, complement_book

D = Decimal
KFEE = lambda c, p: taker_fee(c, p, KALSHI_SCHEDULE)


def yes_quote(asks, venue="kalshi", fee=FREE, bids=()):
    return OutcomeQuote(venue, "Y", "YES", make_book(venue, bids, asks, "Y"), fee)


def no_quote(asks, venue="polymarket", fee=FREE, bids=()):
    return OutcomeQuote(venue, "N", "NO", make_book(venue, bids, asks, "N"), fee)


# --------------------------------------------------------------------------
# the basic identity: a matched pair pays exactly $1
# --------------------------------------------------------------------------

def test_cost_below_a_dollar_is_the_profit_per_pair():
    """YES 0.40 + NO 0.55 = 0.95/pair -> 5c/pair on 100 pairs = $5.00."""
    opp = evaluate("p", yes_quote([("0.40", "100")]), no_quote([("0.55", "100")]), D(100))
    assert opp.size == 100
    assert opp.cost_per_pair == D("0.95")
    assert opp.net_profit == D("5.00")
    assert opp.net_per_pair == D("0.05")
    assert opp.profitable


def test_cost_above_a_dollar_is_not_a_trade():
    """0.45 + 0.58 = 1.03: paying $1.03 for a $1.00 payout."""
    opp = evaluate("p", yes_quote([("0.45", "100")]), no_quote([("0.58", "100")]), D(100))
    assert opp.size == 0
    assert not opp.profitable
    assert opp.top_of_book_edge == D("-0.03")


def test_exactly_a_dollar_is_sized_but_not_profitable():
    """
    0.40 + 0.60 = exactly $1.00 for a $1.00 payout.

    dP_min is a MINIMUM, so a chunk earning exactly the threshold clears it:
    at threshold 0 the walk sizes the trade. What stops it is the separate
    `profitable` gate, which requires strictly positive profit and is what
    ArbitrageStrategy keys off. Both halves are asserted here because the
    split is the thing worth pinning: sizing says what COULD be done, the
    profitability gate says whether to do it.
    """
    opp = evaluate("p", yes_quote([("0.40", "100")]), no_quote([("0.60", "100")]), D(100))
    assert opp.size == 100
    assert opp.net_profit == D("0")
    assert not opp.profitable

    # and with any real threshold it is not even sized
    base = DynamicThreshold().value()
    assert evaluate("p", yes_quote([("0.40", "100")]), no_quote([("0.60", "100")]),
                    D(100), threshold=base).size == 0


# --------------------------------------------------------------------------
# depth: the top of book is a lie at size
# --------------------------------------------------------------------------

def test_executable_size_stops_at_the_level_that_kills_the_edge():
    """
    YES: 40 @ 0.40 then 200 @ 0.49.  NO: 100 @ 0.55.
      pairs 1..40   cost 0.95  -> +5c each  = $2.00
      pair  41      cost 1.04  -> declined, and the walk stops there
    Top of book still claims 5c, which is exactly the trap being tested.
    """
    opp = evaluate("p", yes_quote([("0.40", "40"), ("0.49", "200")]),
                   no_quote([("0.55", "100")]), D(100))
    assert opp.size == 40
    assert opp.net_profit == D("2.00")
    assert opp.top_of_book_edge == D("0.05")        # the naive number
    assert opp.net_per_pair == D("0.05")


def test_size_is_capped_by_the_thinner_leg():
    """NO has only 25 contracts; 100 pairs cannot exist however deep YES is."""
    opp = evaluate("p", yes_quote([("0.40", "5000")]), no_quote([("0.55", "25")]), D(100))
    assert opp.size == 25
    assert opp.net_profit == D("1.25")              # 25 x 5c


def test_a_level_split_across_two_chunks_still_prices_correctly():
    """
    YES: 30 @ 0.40, 70 @ 0.42.  NO: 50 @ 0.55, 50 @ 0.56.
      chunk 1: 30 pairs  0.40+0.55 = 0.95  -> 30 x 5c = 1.50
      chunk 2: 20 pairs  0.42+0.55 = 0.97  -> 20 x 3c = 0.60
      chunk 3: 50 pairs  0.42+0.56 = 0.98  -> 50 x 2c = 1.00
      total 100 pairs, $3.10
    The NO level at 0.55 is consumed across two chunks, which is the case a
    naive single-pass walk gets wrong.
    """
    opp = evaluate("p", yes_quote([("0.40", "30"), ("0.42", "70")]),
                   no_quote([("0.55", "50"), ("0.56", "50")]), D(100))
    assert opp.size == 100
    assert opp.net_profit == D("3.10")
    assert len(opp.fills) == 3
    assert [f.size for f in opp.fills] == [D(30), D(20), D(50)]


def test_limit_prices_are_the_worst_level_touched():
    """The dispatched limit must cover every level walked, or the walk lied."""
    opp = evaluate("p", yes_quote([("0.40", "30"), ("0.42", "70")]),
                   no_quote([("0.55", "50"), ("0.56", "50")]), D(100))
    assert opp.yes_limit_price == D("0.42")
    assert opp.no_limit_price == D("0.56")


def test_empty_book_yields_nothing_rather_than_raising():
    opp = evaluate("p", yes_quote([]), no_quote([("0.55", "100")]), D(100))
    assert opp.size == 0
    assert opp.top_of_book_cost is None
    assert opp.top_of_book_edge is None
    assert opp.cost_per_pair is None


def test_sub_contract_depth_is_not_tradable_at_an_increment_of_one():
    """0.4 of a Kalshi contract is not a trade; the walk ends rather than rounding up."""
    opp = evaluate("p", yes_quote([("0.40", "0.4")]), no_quote([("0.55", "100")]), D(100))
    assert opp.size == 0


def test_requested_size_caps_the_walk():
    opp = evaluate("p", yes_quote([("0.40", "5000")]), no_quote([("0.55", "5000")]), D(10))
    assert opp.size == 10
    assert opp.requested_size == 10
    assert opp.net_profit == D("0.50")


# --------------------------------------------------------------------------
# fees: quadratic, per fill, and capable of erasing the whole edge
# --------------------------------------------------------------------------

def test_kalshi_fees_erase_a_two_cent_edge_near_fifty_cents():
    """
    100 @ 0.49 with a free NO leg at 0.49: gross edge 2c/pair = $2.00.
    Kalshi fee = ceil(0.07 x 100 x 0.49 x 0.51) = ceil(1.7493) = $1.75.
    Net $0.25 — an 87% haircut, at the price where the quadratic peaks.
    """
    opp = evaluate("p", yes_quote([("0.49", "100")], fee=KFEE),
                   no_quote([("0.49", "100")]), D(100))
    assert opp.fees == D("1.75")
    assert opp.net_profit == D("0.25")


def test_the_same_edge_survives_at_the_wing_where_the_quadratic_is_small():
    """
    Same 2c gross edge, but at 0.05/0.93 instead of 0.49/0.49.
    Kalshi fee = ceil(0.07 x 100 x 0.05 x 0.95) = ceil(0.3325) = $0.34.
    Net $1.66 vs $0.25 near the middle: the SAME spread, 6.6x the profit.
    """
    opp = evaluate("p", yes_quote([("0.05", "100")], fee=KFEE),
                   no_quote([("0.93", "100")]), D(100))
    assert opp.fees == D("0.34")
    assert opp.net_profit == D("1.66")


def test_fees_are_charged_per_chunk_not_on_a_blended_price():
    """
    Two chunks at different prices pay two separately rounded fees. Charging
    one fee on the average price gives a different number, because the fee is
    quadratic and rounded up per fill.
      30 @ 0.40: ceil(0.07 x 30 x 0.40 x 0.60) = ceil(0.504) = 0.51
      70 @ 0.42: ceil(0.07 x 70 x 0.42 x 0.58) = ceil(1.19364) = 1.20
    """
    opp = evaluate("p", yes_quote([("0.40", "30"), ("0.42", "70")], fee=KFEE),
                   no_quote([("0.50", "200")]), D(100))
    assert [f.fees for f in opp.fills] == [D("0.51"), D("1.20")]
    assert opp.fees == D("1.71")


# --------------------------------------------------------------------------
# threshold and slippage
# --------------------------------------------------------------------------

def test_threshold_declines_an_edge_too_thin_to_be_worth_the_latency():
    """0.445 + 0.55 = 0.995, i.e. 0.5c/pair. dP_min 1c declines it."""
    yes, no = yes_quote([("0.445", "100")]), no_quote([("0.55", "100")])
    assert evaluate("p", yes, no, D(100), threshold=D("0.004")).size == 100
    assert evaluate("p", yes, no, D(100), threshold=D("0.01")).size == 0


def test_threshold_is_applied_per_chunk_not_to_the_average():
    """
    30 pairs at 5c and 70 at 1c average 2.2c/pair, which clears a 2c
    threshold on average. Per chunk, the 70 do not — and averaging a bad
    chunk against good ones is how a detector talks itself into size.
    """
    yes = yes_quote([("0.40", "30"), ("0.44", "70")])
    no = no_quote([("0.55", "200")])
    assert evaluate("p", yes, no, D(100), threshold=D("0")).size == 100
    assert evaluate("p", yes, no, D(100), threshold=D("0.02")).size == 30


def test_slippage_costs_one_tick_per_leg():
    """1 adverse tick on each leg of a 5c edge leaves 3c."""
    yes, no = yes_quote([("0.40", "100")]), no_quote([("0.55", "100")])
    slip = SlippageModel(adverse_ticks=D(1), tick_size=D("0.01"))
    assert evaluate("p", yes, no, D(100)).net_profit == D("5.00")
    assert evaluate("p", yes, no, D(100), slippage=slip).net_profit == D("3.00")


def test_fixed_cost_per_pair_is_charged_on_every_pair():
    yes, no = yes_quote([("0.40", "100")]), no_quote([("0.55", "100")])
    slip = SlippageModel(fixed_cost_per_pair=D("0.01"))
    assert evaluate("p", yes, no, D(100), slippage=slip).net_profit == D("4.00")


# --------------------------------------------------------------------------
# both directions
# --------------------------------------------------------------------------

def test_both_directions_are_evaluated_and_the_better_one_comes_first():
    """
    Kalshi 0.41/0.43, Polymarket YES 0.50/0.52 and NO 0.45/0.54.
      kalshi YES + poly NO : 0.43 + 0.54 = 0.97  -> 3c/pair
      poly YES  + kalshi NO: 0.52 + (1 - 0.41) = 1.11  -> no trade
    """
    kalshi = kalshi_quotes("K", make_book("kalshi", [("0.41", "300")], [("0.43", "200")], "K"))
    poly = polymarket_quotes("PY", "PN", StaticOutcomeBooks({
        "YES": make_book("polymarket", [("0.50", "100")], [("0.52", "300")], "PY"),
        "NO": make_book("polymarket", [("0.45", "100")], [("0.54", "300")], "PN"),
    }))
    best, worse = evaluate_directions("p", kalshi, poly, D(100))
    assert best.direction == "kalshi YES + polymarket NO"
    assert best.size == 100 and best.net_profit == D("3.00")
    assert worse.direction == "polymarket YES + kalshi NO"
    assert worse.size == 0


# --------------------------------------------------------------------------
# cross-check against the independently written same-outcome engine
# --------------------------------------------------------------------------

@pytest.mark.parametrize("kalshi_bids, kalshi_asks, poly_bids, poly_asks, fee", [
    # the shape the recorded tapes actually had on 2026-10-08
    ([("0.41", "300"), ("0.40", "500")], [("0.42", "200")],
     [("0.37", "400")], [("0.38", "400")], FREE),
    # several levels on both sides
    ([("0.45", "40"), ("0.44", "60"), ("0.40", "500")], [("0.47", "100")],
     [("0.37", "25"), ("0.38", "50")], [("0.38", "30"), ("0.39", "200")], FREE),
    # with Kalshi's quadratic fee on the Kalshi leg
    ([("0.41", "300"), ("0.40", "500")], [("0.42", "200")],
     [("0.37", "400")], [("0.38", "400")], KFEE),
])
def test_complementary_pair_agrees_with_the_same_outcome_engine(
        kalshi_bids, kalshi_asks, poly_bids, poly_asks, fee):
    """
    On a binary CLOB a NO ask IS the mirror of a YES bid, so

        buy YES on A + buy NO on B   ==   buy YES on A + sell YES on B
        ask_A + (1 - bid_B) < 1      <=>  ask_A < bid_B

    This asserts that identity against cross_venue_spread.edge_for_direction,
    which was written independently and walks each leg separately rather than
    pairing them. Two code paths agreeing on quadratic fees and multi-level
    depth is the strongest evidence available that either is right.

    It also confirms the fee symmetry the NO derivation depends on: charging
    Kalshi's quadratic fee at the NO price (1 - p) must give the same number
    as charging it at the YES price p, because p(1-p) is symmetric about 0.50.
    """
    kalshi = make_book("kalshi", kalshi_bids, kalshi_asks, "K")
    poly = make_book("polymarket", poly_bids, poly_asks, "P")

    same = edge_for_direction("p", D(100), poly, FREE, kalshi, fee)
    mine = evaluate("p",
                    OutcomeQuote("polymarket", "P", "YES", poly, FREE),
                    OutcomeQuote("kalshi", "K", "NO", complement_book(kalshi, "K"), fee),
                    D(100), threshold=D("-1"))      # no threshold: raw comparison

    assert mine.net_profit == same.net_edge
    assert mine.fees == same.total_fees


# --------------------------------------------------------------------------
# the book derivation the Kalshi NO side depends on
# --------------------------------------------------------------------------
def test_the_complement_reflects_both_sides_through_one_dollar():
    book = make_book("kalshi", [("0.41", "300"), ("0.40", "500")], [("0.43", "200")])
    no = complement_book(book)
    assert list(no.asks.items()) == [(D("0.59"), D(300)), (D("0.60"), D(500))]
    assert list(no.bids.items()) == [(D("0.57"), D(200))]


def test_the_complement_is_its_own_inverse():
    book = make_book("kalshi", [("0.41", "300")], [("0.43", "200")])
    back = complement_book(complement_book(book))
    assert list(back.bids.items()) == list(book.bids.items())
    assert list(back.asks.items()) == list(book.asks.items())


def test_the_best_yes_bid_becomes_the_best_no_ask():
    """Highest YES bid -> lowest NO ask, which is what makes the ladder order right."""
    book = make_book("kalshi", [("0.41", "300"), ("0.35", "500")], [])
    _, best_no_ask = complement_book(book).get_top_of_book()
    assert best_no_ask.price == D("0.59")
