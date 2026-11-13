"""
engine/sentiment.py

The sentiment side of the strategy input, and the volatility number the
dynamic arbitrage threshold is built on.

Why an arbitrage detector cares about sentiment at all: the threshold is not
a risk preference, it is a latency hedge. Between seeing an edge and both
legs filling there is a venue round trip (40ms on Kalshi, 120ms on
Polymarket in the mock defaults), and during a news surge the book can move
further in that window than the entire edge is worth. Requiring a wider edge
when sentiment is churning is how the strategy declines trades whose
apparent profit is really just a stale quote.

Note it is the VOLATILITY of sentiment that widens the threshold, not its
level. A market that has been confidently bullish all morning is not
dangerous to arbitrage; one that is being repriced is. Person A's AMM uses
the same feed for the opposite purpose — the signed level, to skew quotes —
which is why both numbers are exposed here.

Transport contract agreed with Person A (the news/sentiment producer):

    Redis stream   sentiment:{pair_id}
    fields         score     float in [-1, 1], signed sentiment
                   headline  str, optional
                   source    str, optional
                   ts        unix seconds, float, optional (defaults to now)

A stream rather than a plain key because the threshold needs the recent
HISTORY to compute a standard deviation; a key that only ever holds the
latest value cannot answer "how much has this moved".
"""

from __future__ import annotations

import json
import logging
import statistics
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Protocol

from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

ZERO = Decimal("0")


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class SentimentSnapshot(BaseModel):
    """One scored news item (or one periodic re-score) for one pair."""
    pair_id: str
    score: Decimal                      # signed, [-1, 1]; positive favours YES
    timestamp: datetime = Field(default_factory=utcnow)
    headline: str | None = None
    source: str | None = None


class SentimentFeed(Protocol):
    """
    What the strategies need. Implemented by InMemorySentimentFeed (tests and
    replay) and RedisSentimentFeed (live).
    """

    def latest(self, pair_id: str) -> SentimentSnapshot | None: ...

    def volatility(self, pair_id: str) -> Decimal: ...


class RollingSentiment:
    """
    A bounded window of recent scores per pair, and the two statistics built
    from it.

    `window` is a time span, not a count, because the thing being measured is
    a rate of news flow: ten headlines in a minute and ten over an hour are
    very different conditions and a fixed-length deque cannot tell them
    apart. `max_points` is only a memory bound.

    volatility() is the population standard deviation of the scores in the
    window, which is zero for a single point and zero for a feed that is
    steady at any level — including a steadily extreme one. That is the
    intended behaviour: see the module docstring.

    `now_fn` is how "now" is decided, and it has to be injectable. During a
    replay the headlines carry the tape's historical timestamps, so a window
    measured against wall-clock time contains none of them and every
    statistic silently reads zero. The pipeline points this at the replay
    clock; live, it stays at utcnow.
    """

    def __init__(self, window: timedelta = timedelta(minutes=5), max_points: int = 512,
                 now_fn=utcnow):
        self.window = window
        self.max_points = max_points
        self.now_fn = now_fn
        self._points: dict[str, deque[SentimentSnapshot]] = defaultdict(
            lambda: deque(maxlen=max_points))

    def add(self, snapshot: SentimentSnapshot) -> None:
        self._points[snapshot.pair_id].append(snapshot)

    def latest(self, pair_id: str) -> SentimentSnapshot | None:
        points = self._points.get(pair_id)
        return points[-1] if points else None

    def recent(self, pair_id: str, now: datetime | None = None) -> list[SentimentSnapshot]:
        """Snapshots inside the window, oldest first. Expiry is lazy: the
        window is applied on read, so a quiet pair costs nothing to maintain."""
        points = self._points.get(pair_id)
        if not points:
            return []
        cutoff = (now or self.now_fn()) - self.window
        return [p for p in points if p.timestamp >= cutoff]

    def volatility(self, pair_id: str, now: datetime | None = None) -> Decimal:
        scores = [float(p.score) for p in self.recent(pair_id, now)]
        if len(scores) < 2:
            return ZERO
        return Decimal(str(statistics.pstdev(scores)))

    def level(self, pair_id: str, now: datetime | None = None) -> Decimal:
        """Mean signed sentiment in the window — the number an AMM skews on."""
        scores = [float(p.score) for p in self.recent(pair_id, now)]
        if not scores:
            return ZERO
        return Decimal(str(statistics.fmean(scores)))


class InMemorySentimentFeed(RollingSentiment):
    """
    A SentimentFeed driven by direct push. The replay harness injects
    headlines from a recorded tape through this, which is what makes the
    "threshold widens under a news surge" scenario a repeatable test rather
    than something that has to be observed live.
    """

    def push(self, pair_id: str, score: Decimal | str | float,
             headline: str | None = None, source: str | None = None,
             timestamp: datetime | None = None) -> SentimentSnapshot:
        snap = SentimentSnapshot(
            pair_id=pair_id, score=Decimal(str(score)), headline=headline,
            source=source, timestamp=timestamp or self.now_fn(),
        )
        self.add(snap)
        return snap


class RedisSentimentFeed(RollingSentiment):
    """
    Consumes the shared Redis sentiment stream into a rolling window.

    redis is imported lazily so that nothing in engine/ requires a Redis
    install to be importable — the replay tests and every unit test run
    against InMemorySentimentFeed, and a missing optional dependency must not
    break collection of tests that never touch it.

    drain() is non-blocking and returns how many new entries it consumed, so
    the pipeline can call it on its existing evaluation tick instead of
    needing a second task. run() is the blocking-read alternative for a
    deployment that wants sentiment on its own task.
    """

    STREAM_PREFIX = "sentiment:"

    def __init__(self, pair_ids: list[str], url: str = "redis://localhost:6379/0",
                 window: timedelta = timedelta(minutes=5), client=None,
                 now_fn=utcnow):
        super().__init__(window=window, now_fn=now_fn)
        self.pair_ids = list(pair_ids)
        self.url = url
        self._client = client
        # "$" means "only entries added from now on"; a restart deliberately
        # does not replay history, because a stale headline must not widen
        # today's threshold
        self._cursors: dict[str, str] = {p: "$" for p in self.pair_ids}
        self.consumed = 0

    @property
    def client(self):
        if self._client is None:
            try:
                import redis.asyncio as aioredis       # noqa: PLC0415
            except ImportError as exc:                 # pragma: no cover
                raise RuntimeError(
                    "RedisSentimentFeed needs the redis package "
                    "(pip install redis); use InMemorySentimentFeed offline"
                ) from exc
            self._client = aioredis.from_url(self.url, decode_responses=True)
        return self._client

    def stream(self, pair_id: str) -> str:
        return f"{self.STREAM_PREFIX}{pair_id}"

    async def drain(self, count: int = 100) -> int:
        """Consume whatever is waiting, without blocking. Returns entry count."""
        return await self._read(count=count, block_ms=None)

    async def run(self, block_ms: int = 1000) -> None:
        """Blocking-read loop, for running sentiment as its own task."""
        while True:
            await self._read(count=100, block_ms=block_ms)

    async def _read(self, count: int, block_ms: int | None) -> int:
        streams = {self.stream(p): self._cursors[p] for p in self.pair_ids}
        kwargs = {"count": count}
        if block_ms is not None:
            kwargs["block"] = block_ms
        try:
            response = await self.client.xread(streams, **kwargs)
        except Exception:
            log.exception("sentiment xread failed; leaving the window unchanged")
            return 0

        n = 0
        for stream_name, entries in response or []:
            pair_id = str(stream_name).removeprefix(self.STREAM_PREFIX)
            for entry_id, fields in entries:
                self._cursors[pair_id] = entry_id
                snap = self._parse(pair_id, fields)
                if snap is not None:
                    self.add(snap)
                    n += 1
        self.consumed += n
        return n

    def _parse(self, pair_id: str, fields: dict) -> SentimentSnapshot | None:
        """
        A malformed entry is logged and dropped, never raised. The producer
        is a separate process; one bad message must not take the trading loop
        down with it.
        """
        try:
            raw = fields.get("score")
            if raw is None and "json" in fields:
                raw = json.loads(fields["json"]).get("score")
            ts = fields.get("ts")
            return SentimentSnapshot(
                pair_id=pair_id,
                score=Decimal(str(raw)),
                headline=fields.get("headline"),
                source=fields.get("source"),
                timestamp=(datetime.fromtimestamp(float(ts), tz=timezone.utc)
                           if ts else self.now_fn()),
            )
        except Exception:
            log.warning("dropping malformed sentiment entry on %s: %r", pair_id, fields)
            return None


def sentiment_features(feed, pair_id: str, min_points: int = 1) -> tuple[Decimal, Decimal, int]:
    """
    (level, confidence, points in the window) for the sentiment_features table.

    The stream carries no confidence field, so it is derived from the window:
    agreement (1 - stdev of the scores, which lie in [-1, 1]) times evidence
    (min(1, n / min_points)). Headlines that agree score high; contradictory
    ones average to a small level and also score low agreement.
    """
    if feed is None:
        return ZERO, ZERO, 0
    recent = feed.recent(pair_id) if hasattr(feed, "recent") else []
    scores = [float(p.score) for p in recent]
    if not scores:
        return ZERO, ZERO, 0
    level = statistics.fmean(scores)
    agreement = max(0.0, 1.0 - statistics.pstdev(scores))
    evidence = min(1.0, len(scores) / max(min_points, 1))
    return (Decimal(repr(round(level, 10))), Decimal(repr(round(agreement * evidence, 10))),
            len(scores))
