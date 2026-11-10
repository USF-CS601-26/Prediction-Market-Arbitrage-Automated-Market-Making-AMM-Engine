"""
tests/test_sentiment_threshold.py

The dynamic threshold and the sentiment statistics it is built from.

    dP_min = base + lambda * sentiment_volatility     (capped)

The distinction that matters throughout: it is the VOLATILITY of sentiment
that widens the threshold, not its level. A steadily extreme view is not a
dangerous book to arbitrage; one that is being repriced is.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from engine.arbitrage import DynamicThreshold
from engine.sentiment import (InMemorySentimentFeed, RedisSentimentFeed, RollingSentiment,
                             SentimentSnapshot)

D = Decimal
T0 = datetime(2026, 10, 8, 6, 28, tzinfo=timezone.utc)


def at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


# --------------------------------------------------------------------------
# the threshold formula
# --------------------------------------------------------------------------

def test_zero_volatility_gives_the_base_threshold():
    assert DynamicThreshold(base=D("0.005"), lam=D("0.05")).value(D(0)) == D("0.005")


def test_the_threshold_grows_linearly_with_volatility():
    """0.005 + 0.05 x 0.4 = 0.025, i.e. 2.5c/pair demanded instead of 0.5c."""
    t = DynamicThreshold(base=D("0.005"), lam=D("0.05"), cap=D(1))
    assert t.value(D("0.4")) == D("0.025")
    assert t.value(D("0.8")) == D("0.045")


def test_the_cap_stops_a_spike_from_turning_the_strategy_off():
    """
    Without a cap, one contradictory headline pair demands an edge no real
    book shows, silently disabling the strategy for the length of the window.
    """
    t = DynamicThreshold(base=D("0.005"), lam=D("0.5"), cap=D("0.05"))
    assert t.value(D("1")) == D("0.05")
    assert "capped" in t.describe(D("1"))
    assert "capped" not in t.describe(D("0"))


def test_negative_volatility_is_treated_as_zero():
    """A standard deviation cannot be negative, but a bad feed can send one."""
    assert DynamicThreshold(base=D("0.005")).value(D("-3")) == D("0.005")


def test_a_zero_lambda_disables_the_news_premium():
    t = DynamicThreshold(base=D("0.005"), lam=D(0))
    assert t.value(D("0.9")) == D("0.005")


# --------------------------------------------------------------------------
# the rolling window
# --------------------------------------------------------------------------

def test_a_single_point_has_no_volatility():
    """One observation says nothing about dispersion."""
    feed = InMemorySentimentFeed()
    feed.push("p", "0.9")
    assert feed.volatility("p") == D(0)


def test_an_unknown_pair_has_no_volatility_and_no_latest():
    feed = InMemorySentimentFeed()
    assert feed.volatility("nobody") == D(0)
    assert feed.latest("nobody") is None


def test_a_steady_feed_has_zero_volatility_at_any_level():
    feed = InMemorySentimentFeed()
    for _ in range(5):
        feed.push("p", "0.95")
    assert feed.volatility("p") == D(0)
    assert feed.level("p") == D("0.95")


def test_contradictory_headlines_produce_volatility():
    feed = InMemorySentimentFeed()
    for score in ("0.9", "-0.8", "0.85", "-0.75"):
        feed.push("p", score)
    assert feed.volatility("p") > D("0.5")
    assert abs(feed.level("p")) < D("0.2")      # the level roughly cancels out


def test_the_window_is_a_time_span_not_a_count():
    """
    Ten headlines in a minute and ten over an hour are different conditions,
    and a fixed-length deque cannot tell them apart.
    """
    feed = InMemorySentimentFeed(window=timedelta(minutes=5))
    feed.push("p", "0.9", timestamp=at(0))
    feed.push("p", "-0.9", timestamp=at(30))
    assert feed.volatility("p", now=at(60)) > D("0.5")      # both inside the window
    assert feed.volatility("p", now=at(3600)) == D(0)       # both aged out


def test_an_aged_out_surge_stops_widening_the_threshold():
    feed = InMemorySentimentFeed(window=timedelta(minutes=5))
    t = DynamicThreshold(base=D("0.005"), lam=D("0.05"))
    for i, score in enumerate(("0.9", "-0.8", "0.85", "-0.75")):
        feed.push("p", score, timestamp=at(i * 10))

    during = t.value(feed.volatility("p", now=at(60)))
    after = t.value(feed.volatility("p", now=at(3600)))
    assert during > D("0.03")
    assert after == D("0.005")


def test_latest_returns_the_most_recent_snapshot():
    feed = InMemorySentimentFeed()
    feed.push("p", "0.1", headline="first")
    feed.push("p", "0.7", headline="second", source="wire")
    latest = feed.latest("p")
    assert latest.score == D("0.7")
    assert latest.headline == "second" and latest.source == "wire"


def test_pairs_are_tracked_independently():
    feed = InMemorySentimentFeed()
    feed.push("a", "0.9")
    feed.push("a", "-0.9")
    feed.push("b", "0.5")
    assert feed.volatility("a") > D(0)
    assert feed.volatility("b") == D(0)


def test_the_point_buffer_is_bounded():
    """Memory must stay flat on a pair that is scored all day."""
    feed = InMemorySentimentFeed(max_points=10)
    for i in range(100):
        feed.push("p", "0.5", timestamp=at(i))
    assert len(feed.recent("p", now=at(100))) <= 10


# --------------------------------------------------------------------------
# the Redis transport
# --------------------------------------------------------------------------

class FakeRedis:
    """
    Stands in for redis.asyncio. Returns the stream shape xread really
    returns, so the parsing is tested rather than a mock of the parsing.
    """

    def __init__(self, batches):
        self.batches = list(batches)
        self.calls = []

    async def xread(self, streams, **kwargs):
        self.calls.append((dict(streams), kwargs))
        return self.batches.pop(0) if self.batches else []


def test_redis_entries_become_snapshots_and_advance_the_cursor():
    now = datetime.now(tz=timezone.utc).timestamp()
    feed = RedisSentimentFeed(["p"], client=FakeRedis([
        [("sentiment:p", [
            ("1700000000000-0", {"score": "0.8", "headline": "wire A",
                                 "source": "reuters", "ts": str(now - 30)}),
            ("1700000000001-0", {"score": "-0.6", "headline": "wire B"}),
        ])],
    ]))
    assert asyncio.run(feed.drain()) == 2
    assert feed.volatility("p") > D("0.5")
    assert feed.latest("p").headline == "wire B"
    assert feed.latest("p").source is None          # absent fields stay absent
    assert feed._cursors["p"] == "1700000000001-0"


def test_a_headline_older_than_the_window_is_consumed_but_not_counted():
    """
    The producer's `ts` is honoured, not the time we happened to read it. A
    backlog drained after a restart must not widen today's threshold with
    news from last year.
    """
    feed = RedisSentimentFeed(["p"], window=timedelta(minutes=5), client=FakeRedis([
        [("sentiment:p", [
            ("1-0", {"score": "0.9", "ts": "1760000000"}),      # 2025, long stale
            ("2-0", {"score": "-0.9", "ts": "1760000060"}),
        ])],
    ]))
    assert asyncio.run(feed.drain()) == 2       # both consumed
    assert feed.volatility("p") == D(0)         # neither inside the window
    assert feed.latest("p").score == D("-0.9")  # but still readable as the latest


def test_a_malformed_entry_is_dropped_rather_than_raised():
    """
    The producer is a separate process. One bad message must not take the
    trading loop down with it.
    """
    feed = RedisSentimentFeed(["p"], client=FakeRedis([
        [("sentiment:p", [
            ("1-0", {"score": "not-a-number"}),
            ("2-0", {"headline": "no score at all"}),
            ("3-0", {"score": "0.4"}),
        ])],
    ]))
    assert asyncio.run(feed.drain()) == 1
    assert feed.latest("p").score == D("0.4")


def test_a_redis_failure_leaves_the_window_unchanged():
    """A sentiment outage must degrade to the base threshold, not to a crash."""
    class Broken:
        async def xread(self, streams, **kwargs):
            raise ConnectionError("redis is down")

    feed = RedisSentimentFeed(["p"], client=Broken())
    feed.add(SentimentSnapshot(pair_id="p", score=D("0.5")))
    assert asyncio.run(feed.drain()) == 0
    assert feed.latest("p").score == D("0.5")


def test_the_cursor_starts_at_now_so_stale_headlines_are_not_replayed():
    """A restart must not widen today's threshold with yesterday's news."""
    feed = RedisSentimentFeed(["p", "q"], client=FakeRedis([]))
    assert feed._cursors == {"p": "$", "q": "$"}


def test_json_payloads_are_accepted_as_well_as_flat_fields():
    feed = RedisSentimentFeed(["p"], client=FakeRedis([
        [("sentiment:p", [("1-0", {"json": '{"score": 0.33}'})])],
    ]))
    assert asyncio.run(feed.drain()) == 1
    assert feed.latest("p").score == D("0.33")
