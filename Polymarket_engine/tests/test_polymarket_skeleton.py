"""
tests/test_polymarket_skeleton.py

Smoke tests for the Polymarket pipeline: parse_frame -> adapter -> feed ->
OrderBook. The frames follow the shape of a live capture from 2026-10-04,
shortened to a few levels so expected values can be checked by hand.

Run with: pytest tests/ -v
"""

import json
from decimal import Decimal

import pytest

from models import Side
from polymarket.adapter import to_book_updates
from polymarket.config import PairConfig
from polymarket.feed import PolymarketFeed
from polymarket.messages import BookMsg, PriceChangeMsg, parse_frame
from polymarket.ws_client import Disconnected, RawFrame

YES = "111"
NO = "222"
MARKET = "0xabc"


def frame(payload) -> RawFrame:
    return RawFrame(recv_ts_ns=0, text=json.dumps(payload))


def book_msg(asset_id, bids, asks, ts="1791140646263"):
    # levels are given in wire order: bids ascending, asks descending (best last)
    return {
        "event_type": "book", "market": MARKET, "asset_id": asset_id, "timestamp": ts,
        "hash": "h", "tick_size": "0.001",
        "bids": [{"price": p, "size": s} for p, s in bids],
        "asks": [{"price": p, "size": s} for p, s in asks],
    }


def price_change_msg(*changes, ts="1791140701568"):
    return {
        "event_type": "price_change", "market": MARKET, "timestamp": ts,
        "price_changes": [
            {"asset_id": a, "price": p, "size": s, "side": side, "hash": "h"}
            for a, p, s, side in changes
        ],
    }


INITIAL_SNAPSHOT = [
    book_msg(YES, bids=[("0.040", "100"), ("0.042", "28.4")], asks=[("0.060", "200"), ("0.050", "5800")]),
    book_msg(NO, bids=[("0.940", "200"), ("0.950", "5800")], asks=[("0.960", "100"), ("0.958", "28.4")]),
]


@pytest.fixture
def feed() -> PolymarketFeed:
    pair = PairConfig.model_validate({
        "pair_id": "test",
        "polymarket": {
            "slug": "s", "condition_id": MARKET, "yes_token": YES, "no_token": NO,
            "tick_size": "0.001", "fees": {"enabled": True, "rate": "0.04"},
        },
    })
    return PolymarketFeed(pair, queue=None)


# ---- parsing ----

def test_initial_frame_is_array_of_books():
    events = parse_frame(json.dumps(INITIAL_SNAPSHOT))
    assert [type(e) for e in events] == [BookMsg, BookMsg]
    assert events[0].bids[-1].price == Decimal("0.042")


def test_legacy_price_change_format_is_upgraded():
    legacy = {
        "event_type": "price_change", "market": MARKET, "asset_id": YES, "timestamp": "1",
        "changes": [{"price": "0.043", "size": "10", "side": "BUY"}],
    }
    (event,) = parse_frame(json.dumps(legacy))
    assert isinstance(event, PriceChangeMsg)
    assert event.price_changes[0].asset_id == YES


def test_bad_input_is_skipped_not_raised():
    assert parse_frame("INVALID OPERATION") == []
    assert parse_frame(json.dumps({"event_type": "something_new"})) == []
    assert parse_frame(json.dumps({"event_type": "book", "asset_id": YES})) == []   # missing fields


def test_empty_best_bid_is_tolerated():
    msg = price_change_msg((YES, "0.043", "10", "BUY"))
    msg["price_changes"][0]["best_bid"] = ""
    (event,) = parse_frame(json.dumps(msg))
    assert event.price_changes[0].best_bid is None


# ---- adapter ----

def test_price_change_maps_sides_and_assets():
    (event,) = parse_frame(json.dumps(price_change_msg(
        (NO, "0.001", "1024.18", "BUY"),
        (YES, "0.999", "1024.18", "SELL"),
    )))
    d_no, d_yes = to_book_updates(event)
    assert (d_no.market_id, d_no.side, d_no.exchange) == (NO, Side.BID, "polymarket")
    assert (d_yes.market_id, d_yes.side, d_yes.size) == (YES, Side.ASK, Decimal("1024.18"))
    assert d_yes.sequence is None
    assert d_yes.timestamp.isoformat() == "2026-10-04T19:05:01.568000+00:00"


# ---- feed + OrderBook ----

def test_snapshot_builds_both_books(feed):
    feed.handle(frame(INITIAL_SNAPSHOT))
    bid, ask = feed.book("YES").get_top_of_book()
    assert (bid.price, bid.size) == (Decimal("0.042"), Decimal("28.4"))
    assert (ask.price, ask.size) == (Decimal("0.050"), Decimal("5800"))
    bid, ask = feed.book("NO").get_top_of_book()
    assert (bid.price, ask.price) == (Decimal("0.950"), Decimal("0.958"))
    assert feed.is_valid("YES") and feed.is_valid("NO")


def test_price_change_updates_and_removes_levels(feed):
    feed.handle(frame(INITIAL_SNAPSHOT))
    feed.handle(frame(price_change_msg(
        (YES, "0.045", "300", "BUY"),     # new best bid
        (YES, "0.050", "0", "SELL"),      # best ask removed -> 0.060 becomes best
    )))
    bid, ask = feed.book("YES").get_top_of_book()
    assert (bid.price, bid.size) == (Decimal("0.045"), Decimal("300"))
    assert (ask.price, ask.size) == (Decimal("0.060"), Decimal("200"))


def test_delta_before_snapshot_is_dropped(feed):
    feed.handle(frame(price_change_msg((YES, "0.045", "300", "BUY"))))
    assert feed.book("YES").get_top_of_book() == (None, None)
    assert feed.stats["dropped_before_snapshot"] == 1
    assert not feed.is_valid("YES")


def test_disconnect_invalidates_until_next_snapshot(feed):
    feed.handle(frame(INITIAL_SNAPSHOT))
    feed.handle(Disconnected("test"))
    assert not feed.is_valid("YES")
    feed.handle(frame(price_change_msg((YES, "0.045", "300", "BUY"))))
    assert feed.stats["dropped_before_snapshot"] == 1
    feed.handle(frame(INITIAL_SNAPSHOT))
    assert feed.is_valid("YES")
    assert feed.book("YES").get_top_of_book()[0].price == Decimal("0.042")


def test_unknown_asset_is_ignored(feed):
    feed.handle(frame([book_msg("999", bids=[("0.5", "1")], asks=[("0.6", "1")])]))
    assert feed.stats["unknown_asset"] == 1
    assert not feed.is_valid("YES")


# ---- consistency checks ----
# Crossed-book and exchange-hint checks run when a group of same-timestamp
# messages is complete, so these tests call feed.settle() before asserting.

def test_clean_updates_raise_no_check_failures(feed):
    feed.handle(frame(INITIAL_SNAPSHOT))
    msg = price_change_msg((YES, "0.045", "300", "BUY"), (NO, "0.955", "300", "SELL"))
    msg["price_changes"][0].update(best_bid="0.045", best_ask="0.050")
    msg["price_changes"][1].update(best_bid="0.950", best_ask="0.955")
    feed.handle(frame(msg))
    feed.settle()
    assert feed.check_failures() == 0


def test_crossed_book_is_flagged(feed):
    feed.handle(frame(INITIAL_SNAPSHOT))
    feed.handle(frame(price_change_msg((YES, "0.055", "10", "BUY"))))   # bid above best ask 0.050
    feed.settle()
    assert feed.stats["check_crossed"] == 1


# A trade as the exchange reports it (pattern from a recorded tape, frames 874-876):
# a sell at 0.39 hits the resting 0.39 bid. The exchange first adds the unfilled
# remainder as an ask at 0.39, then removes the used-up bid, all with one timestamp.
TRADE_SNAPSHOT = [book_msg(YES, bids=[("0.38", "5000"), ("0.39", "100")], asks=[("0.40", "300")],
                           ts="1791172168000")]


def _trade_step(asset, price, size, side, ts):
    msg = price_change_msg((asset, price, size, side), ts=ts)
    msg["price_changes"][0].update(best_bid="0.38", best_ask="0.39")   # exchange's view after the trade
    return frame(msg)


def test_book_crossed_in_the_middle_of_a_trade_is_not_flagged(feed):
    feed.handle(frame(TRADE_SNAPSHOT))
    feed.handle(_trade_step(YES, "0.39", "184", "SELL", ts="1791172168793"))   # book briefly crossed here
    feed.handle(_trade_step(YES, "0.39", "0", "BUY", ts="1791172168793"))      # same timestamp: fixed
    feed.settle()
    assert feed.check_failures() == 0
    bids, asks = feed.book("YES").get_depth()
    assert [(l.price, l.size) for l in bids] == [(Decimal("0.38"), Decimal("5000"))]
    assert [(l.price, l.size) for l in asks] == [(Decimal("0.39"), Decimal("184")), (Decimal("0.40"), Decimal("300"))]


def test_book_still_crossed_when_the_timestamp_moves_on_is_flagged(feed):
    feed.handle(frame(TRADE_SNAPSHOT))
    feed.handle(_trade_step(YES, "0.39", "184", "SELL", ts="1791172168793"))
    # the bid removal never arrives; a message with a newer timestamp closes the group
    feed.handle(frame(price_change_msg((YES, "0.10", "5", "BUY"), ts="1791172168818")))
    assert feed.stats["check_crossed"] == 1
    assert feed.stats["check_top_mismatch"] == 1


def test_out_of_range_price_is_rejected(feed):
    feed.handle(frame(INITIAL_SNAPSHOT))
    feed.handle(frame(price_change_msg((YES, "1.5", "10", "SELL"))))
    assert feed.stats["check_bad_level"] == 1
    assert Decimal("1.5") not in feed.book("YES").asks


def test_off_tick_price_is_flagged_but_applied(feed):
    feed.handle(frame(INITIAL_SNAPSHOT))
    feed.handle(frame(price_change_msg((YES, "0.0415", "10", "BUY"))))  # tick is 0.001
    assert feed.stats["check_off_tick"] == 1
    assert feed.book("YES").bids[Decimal("0.0415")] == Decimal("10")


def test_top_of_book_mismatch_with_exchange_is_flagged(feed):
    feed.handle(frame(INITIAL_SNAPSHOT))
    msg = price_change_msg((YES, "0.030", "5", "BUY"))
    msg["price_changes"][0].update(best_bid="0.043", best_ask="0.050")  # exchange says 0.043, we have 0.042
    feed.handle(frame(msg))
    feed.settle()
    assert feed.stats["check_top_mismatch"] == 1
