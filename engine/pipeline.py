"""
engine/pipeline.py

The whole system in one place: both venues' feeds, the sentiment feed, the
strategies, the executors, the risk layer and the database.

Two modes, deliberately sharing every component except the frame source:

    live    both WebSocket clients, Redis sentiment, mock execution against
            the live books
    replay  two recorded tapes merged on local receive time, scripted
            headlines, the same mock execution

Replay is the mode that matters for review. "It worked when I ran it" is not
a result anyone can check; a recorded tape plus a seeded RNG is. Everything
that could make a run non-deterministic is pinned: the executors' RNGs are
seeded, the Kalshi fee multiplier can be fixed so no network lookup is
needed, and sentiment arrives at a stated frame index rather than whenever a
news API felt like it.

The strategy list is open. ArbitrageStrategy is registered by default;
Person A's AMM quoter registers through add_strategy() and gets the same
tick, the same executors, the same PortfolioManager and the same sentiment
feed. The pipeline does not know or care what a strategy does with them.

Run it:
    python -m engine.pipeline --replay <POLYMARKET_TAPE> <KALSHI_TAPE>
    python -m engine.pipeline --live
"""

from __future__ import annotations

import asyncio
import dataclasses
import heapq
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from pydantic import BaseModel, Field

import engine  # noqa: F401  (sys.path bootstrap for both venue engines)
from engine.amm_strategy import AMMConfig, AMMQuote, AMMStrategy, sentiment_features
from engine.arbitrage import DynamicThreshold, SlippageModel, kalshi_quotes, polymarket_quotes
from engine.arbitrage_strategy import ArbitrageStrategy
from engine.executors import MockKalshiExecutor, MockPolymarketExecutor, OrderExecutorFactory
from engine.executors.base import OrderNotFound
from engine.executors.simulator import SimulationConfig
from engine.models import Fill
from engine.persistence import EventSink, make_sink
from engine.portfolio_manager import PortfolioManager, RiskLimits
from engine.sentiment import InMemorySentimentFeed, RedisSentimentFeed
from engine.strategy import BaseStrategy, StrategyResult

# both venue engines, reached via the engine package's path bootstrap
from kalshi_fees import fetch_fee_schedule, series_ticker
from kalshi_fees import taker_fee as kalshi_taker_fee
from models import FeeSchedule as KalshiFeeSchedule
from order_book import OrderBook
from polymarket import recorder as poly_tape
from polymarket.config import PairConfig, load_pair_config
from polymarket.feed import PolymarketFeed
from polymarket.fees import fee_fn as poly_fee_fn
from polymarket.ws_client import PolymarketWsClient
import tape as kalshi_tape

# run_pair.py owns the offline-safe KalshiWSClient subclass (it suppresses
# per-message printing and turns a sequence gap during a replay into a
# reported stale book instead of a snapshot request against a socket that
# does not exist). Importing it beats maintaining a second copy of behaviour
# that subtle.
from run_pair import QuietKalshiWSClient

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
ZERO = Decimal("0")


@dataclass
class ScriptedHeadline:
    """
    A sentiment event injected at a known point in a replay.

    Keyed on a frame count rather than a wall-clock time because a recorded
    tape's timestamps are historical: "60 seconds in" is ambiguous after the
    fact, "once 4000 frames have been applied" is not.
    """
    after_frames: int
    score: Decimal
    text: str = ""
    source: str = "scripted"


@dataclass
class PipelineConfig:
    size: Decimal = Decimal("100")              # pairs per attempt, before depth/risk
    threshold: DynamicThreshold = field(default_factory=DynamicThreshold)
    slippage: SlippageModel = field(default_factory=SlippageModel)
    kalshi_fee_multiplier: Decimal | None = None   # set to skip the network lookup
    dry_run: bool = False
    db_path: str | None = None                  # SQLite file, arbitrage events only
    db_url: str | None = None                   # feature 6: postgresql+asyncpg://... (wins over db_path)
    snapshot_interval: timedelta = timedelta(seconds=5)   # periodic book/spread/sentiment rows
    snapshot_depth: int = 10                    # levels per side in a book snapshot
    redis_url: str | None = None                # None = InMemorySentimentFeed
    sentiment_window: timedelta = timedelta(minutes=5)
    seed: int | None = 0                        # executor RNG seed; None = random
    # mock venue behaviour. Defaults are each venue's own profile from
    # engine/executors/{kalshi,polymarket}.py, overridden here only for tests.
    kalshi_sim: SimulationConfig | None = None
    polymarket_sim: SimulationConfig | None = None
    max_position: Decimal = Decimal("1000")
    cash: Decimal = Decimal("10000")
    # the full limit set; when None, RiskLimits defaults with max_position applied
    risk_limits: RiskLimits | None = None
    # venues to run an AMM quoter on ("kalshi", "polymarket"); empty = arbitrage only
    amm_venues: tuple[str, ...] = ()
    amm: AMMConfig = field(default_factory=AMMConfig)


class PipelineReport(BaseModel):
    """What a run did. Everything a test or a reviewer would want to assert on."""
    pair_id: str
    frames_replayed: int = 0
    ticks: int = 0
    evaluations: int = 0
    opportunities: int = 0
    declined_by_threshold: int = 0
    declined_by_preflight: int = 0
    attempts: int = 0            # opportunities taken as far as a risk check
    dispatched: int = 0          # attempts that actually sent both legs
    vetoed_by_risk: int = 0
    matched_pairs: Decimal = ZERO
    locked_profit: Decimal = ZERO
    naked_size: Decimal = ZERO
    fills: int = 0
    thresholds: list[Decimal] = Field(default_factory=list)   # dP_min per evaluation
    headlines_injected: int = 0
    polymarket_check_failures: int = 0
    kalshi_sequence_gaps: int = 0
    db_counts: dict[str, int] = Field(default_factory=dict)
    db_dropped: int = 0
    portfolio: str = ""
    amm: list[str] = Field(default_factory=list)       # one summary line per AMM
    errors: list[str] = Field(default_factory=list)   # live mode: tasks that died

    @property
    def max_threshold(self) -> Decimal:
        return max(self.thresholds) if self.thresholds else ZERO

    @property
    def min_threshold(self) -> Decimal:
        return min(self.thresholds) if self.thresholds else ZERO

    def describe(self) -> str:
        return "\n".join([
            "=== pipeline report ===",
            f"pair {self.pair_id}",
            f"frames replayed:        {self.frames_replayed}",
            f"strategy ticks:         {self.ticks}",
            f"edge evaluations:       {self.evaluations}",
            f"opportunities priced:   {self.opportunities}",
            f"declined by dP_min:     {self.declined_by_threshold}",
            f"declined by pre-flight: {self.declined_by_preflight}",
            f"execution attempts:     {self.attempts}",
            f"  dispatched:           {self.dispatched}",
            f"  vetoed by risk:       {self.vetoed_by_risk}",
            f"pairs matched:          {self.matched_pairs}",
            f"locked profit:          ${self.locked_profit:+.4f}",
            f"naked after unwind:     {self.naked_size}",
            f"fills booked:           {self.fills}",
            f"dP_min range:           {self.min_threshold:.4f} .. {self.max_threshold:.4f}",
            f"headlines injected:     {self.headlines_injected}",
            f"Polymarket check fails: {self.polymarket_check_failures}",
            f"Kalshi sequence gaps:   {self.kalshi_sequence_gaps}",
            f"persisted:              {self.db_counts}"
            + (f"  ({self.db_dropped} dropped)" if self.db_dropped else ""),
            f"portfolio:              {self.portfolio}",
        ] + [f"AMM:                    {line}" for line in self.amm]
          + ([f"ERRORS:                 {'; '.join(self.errors)}"] if self.errors else []))


def _mid(book: OrderBook) -> Decimal | None:
    bid, ask = book.get_top_of_book()
    if bid is None or ask is None:
        return None
    return (bid.price + ask.price) / 2


def kalshi_fee_function(ticker: str, multiplier: Decimal | None):
    """
    Kalshi's taker fee as (contracts, price) -> fee.

    A fixed multiplier skips fetch_fee_schedule's HTTP call, which is what
    makes a replay test fully offline. 1.0 is the value the KXLEADERSOUT
    series carried when the repo's tapes were recorded.
    """
    if multiplier is None:
        schedule = fetch_fee_schedule(ticker)
    else:
        schedule = KalshiFeeSchedule(venue="kalshi", series=series_ticker(ticker),
                                     fee_type="quadratic", multiplier=multiplier)

    def fee(contracts: Decimal, price: Decimal) -> Decimal:
        return kalshi_taker_fee(contracts, price, schedule)

    return fee, schedule


class ArbitragePipeline:
    """
    Owns every component and the wiring between them. Construct it, then call
    run_replay() or run_live().
    """

    def __init__(self, pair: PairConfig, ticker: str,
                 config: PipelineConfig | None = None, offline: bool = True):
        self.pair = pair
        self.ticker = ticker
        self.config = config or PipelineConfig()
        self.offline = offline

        # --- market data ---
        self.kalshi_book = OrderBook(market_id=ticker, exchange="kalshi")
        self.kalshi = QuietKalshiWSClient(ticker, self.kalshi_book, offline=offline)
        self.poly_queue: asyncio.Queue | None = None if offline else asyncio.Queue()
        self.poly_feed = PolymarketFeed(pair, self.poly_queue)

        # --- fees ---
        self.kalshi_fee, self.kalshi_schedule = kalshi_fee_function(
            ticker, self.config.kalshi_fee_multiplier)
        self.poly_fee = poly_fee_fn(pair.polymarket.fees)

        # --- sentiment: the same feed both strategy families read ---
        # `now` is the replay clock. The sentiment window is measured against
        # it rather than wall-clock time, because a tape's headlines carry
        # historical timestamps: measured against the real clock the window
        # would contain none of them and every statistic would read zero.
        self.now: datetime | None = None
        if self.config.redis_url:
            self.sentiment = RedisSentimentFeed([pair.pair_id], self.config.redis_url,
                                                window=self.config.sentiment_window,
                                                now_fn=self.clock)
        else:
            self.sentiment = InMemorySentimentFeed(window=self.config.sentiment_window,
                                                   now_fn=self.clock)

        # --- risk and persistence ---
        # The process-wide PortfolioManager, reset for this run: one book of
        # positions for every strategy and venue is what stops the arbitrage
        # strategy and the AMM over-allocating against each other. Both
        # venues' YES/NO ids are registered under the pair's id, so per-market
        # limits and get_inventory see a Polymarket market's two tokens as one.
        limits = self.config.risk_limits or RiskLimits(
            max_position_per_market=self.config.max_position)
        self.portfolio = PortfolioManager.instance().reset(limits, cash=self.config.cash)
        self.portfolio.register_market("kalshi", pair.pair_id, {"YES": ticker, "NO": ticker})
        self.portfolio.register_market("polymarket", pair.pair_id,
                                       {"YES": pair.polymarket.yes_token,
                                        "NO": pair.polymarket.no_token})
        self.sink: EventSink = make_sink(
            self.config.db_path, self.config.db_url,
            **({"snapshot_depth": self.config.snapshot_depth} if self.config.db_url else {}))

        # --- execution ---
        self.factory = OrderExecutorFactory()
        self._register_executors()

        # --- strategies ---
        self.strategies: list[BaseStrategy] = []
        self.arbitrage = self._build_arbitrage_strategy()
        self.add_strategy(self.arbitrage)
        self.amms: list[AMMStrategy] = [self._build_amm(v) for v in self.config.amm_venues]
        for amm in self.amms:
            self.add_strategy(amm)

        # --- throttles for the periodic persistence rows ---
        self._last_snapshot: datetime | None = None
        self._last_spread: tuple | None = None
        self._last_spread_at: datetime | None = None
        self._last_sentiment: tuple | None = None
        self._last_sentiment_at: datetime | None = None

        # change detection, same mechanism run_pair uses
        self.kalshi_version = 0
        self._seen: tuple[int, int] | None = None
        self.kalshi.on_update = self._kalshi_changed

        self.ticks = 0
        self._attempts_persisted = 0
        self.headlines_injected = 0
        self.thresholds: list[Decimal] = []

    def clock(self) -> datetime:
        """Replay time while replaying, wall-clock time otherwise."""
        return self.now or datetime.now(tz=timezone.utc)

    # ---- wiring ----

    def _sim(self, override: SimulationConfig | None,
             default: SimulationConfig) -> SimulationConfig:
        """Venue defaults, with the run's seed applied unless overridden."""
        cfg = override or default
        if override is None and self.config.seed is not None:
            cfg = SimulationConfig(**{**cfg.__dict__, "seed": self.config.seed})
        return cfg

    def _register_executors(self) -> None:
        from engine.executors.kalshi import KALSHI_DEFAULTS
        from engine.executors.polymarket import POLYMARKET_DEFAULTS

        kalshi_sim = self._sim(self.config.kalshi_sim, KALSHI_DEFAULTS)
        poly_sim = self._sim(self.config.polymarket_sim, POLYMARKET_DEFAULTS)

        self.factory.register("kalshi", lambda: MockKalshiExecutor(
            self.ticker, self.kalshi_book, self.kalshi_fee,
            is_fresh=lambda: self.kalshi.book_is_fresh, config=kalshi_sim))
        self.factory.register("polymarket", lambda: MockPolymarketExecutor(
            self.poly_feed, self.poly_fee,
            tokens={"YES": self.pair.polymarket.yes_token,
                    "NO": self.pair.polymarket.no_token},
            config=poly_sim))

        # the PortfolioManager learns about fills from the executors directly,
        # not from the strategy: a resting order can fill after the strategy
        # has moved on, and an unwind that throws must not skip bookkeeping
        self.factory.add_fill_listener_to_all(self._on_fill)

    def _on_fill(self, fill: Fill) -> None:
        self.portfolio.record_fill(fill)
        self.sink.record_fill(fill)

    def _build_arbitrage_strategy(self) -> ArbitrageStrategy:
        pm = self.pair.polymarket

        def kalshi_side():
            # rebuilt every tick: Kalshi's NO book is a reflection of its YES
            # book, so a cached one is a quote against a book that has moved
            if not self.kalshi.book_is_fresh:
                return None
            return kalshi_quotes(self.ticker, self.kalshi_book, self.kalshi_fee)

        def polymarket_side():
            if not (self.poly_feed.is_valid("YES") and self.poly_feed.is_valid("NO")):
                return None
            return polymarket_quotes(pm.yes_token, pm.no_token, self.poly_feed, self.poly_fee)

        return ArbitrageStrategy(
            self.pair.pair_id, kalshi_side, polymarket_side,
            self.factory, self.portfolio, sentiment=self.sentiment,
            threshold=self.config.threshold, slippage=self.config.slippage,
            max_size=self.config.size, dry_run=self.config.dry_run,
            tick_size=pm.tick_size,
        )

    def _build_amm(self, venue: str) -> AMMStrategy:
        """
        An AMM quoter on one venue's YES book. It shares the factory, the
        PortfolioManager (its inventory and limits) and the sentiment feed
        with the arbitrage strategy, and uses the market's end date for T - t.
        """
        pm = self.pair.polymarket
        expiry = self.config.amm.expiry or pm.end_date
        if venue == "kalshi":
            def book():
                return self.kalshi_book if self.kalshi.book_is_fresh else None
            yes_id = no_id = self.ticker
            tick = Decimal("0.01")
        elif venue == "polymarket":
            def book():
                # the ask side buys NO, so both token books must be usable
                if self.poly_feed.is_valid("YES") and self.poly_feed.is_valid("NO"):
                    return self.poly_feed.book("YES")
                return None
            yes_id, no_id = pm.yes_token, pm.no_token
            tick = pm.tick_size
        else:
            raise ValueError(f"no AMM support for venue {venue!r}")
        config = dataclasses.replace(self.config.amm, tick_size=tick, expiry=expiry)
        return AMMStrategy(self.pair.pair_id, venue, self.pair.pair_id, yes_id, no_id,
                           book, self.factory, self.portfolio, sentiment=self.sentiment,
                           config=config)

    def add_strategy(self, strategy: BaseStrategy) -> None:
        """
        Register another strategy on the same tick — this is the seam Person
        A's AMM quoter plugs into. It gets self.factory, self.portfolio and
        self.sentiment, i.e. the same executors, the same position state and
        the same scored headlines the arbitrage threshold widens on.
        """
        self.strategies.append(strategy)

    # ---- the tick ----

    def _kalshi_changed(self, _client) -> None:
        self.kalshi_version += 1

    def _books_changed(self) -> bool:
        versions = (self.kalshi_version,
                    self.poly_feed.stats["snapshots"] + self.poly_feed.stats["deltas"])
        if versions == self._seen:
            return False
        self._seen = versions
        return True

    async def tick(self, now: datetime) -> None:
        """
        One book change: let the mock venues match resting orders against the
        new prices, give every strategy one chance to act, persist what they
        saw, and release risk capacity held by orders that are no longer live.
        """
        self.ticks += 1
        await self._match_resting()
        self._update_marks()
        volatility = self.sentiment.volatility(self.pair.pair_id)
        for strategy in self.strategies:
            result = await strategy.run_tick(now)
            if result is None:
                continue
            await self._persist(strategy, result, now, volatility)
        self.portfolio.reconcile_reservations(self._open_order_ids())
        self._record_periodic(now, volatility)

    async def _match_resting(self) -> None:
        for executor in list(self.factory.built()):
            match = getattr(executor, "match_resting", None)
            if match is not None:
                await match()

    def _open_order_ids(self) -> set[str]:
        ids = set()
        for executor in self.factory.built():
            for state in getattr(executor, "open_orders", lambda: [])():
                ids.add(state.order.order_id)
        return ids

    def _update_marks(self) -> None:
        """Mark every position at its book's mid, for unrealized P&L."""
        if self.kalshi.book_is_fresh:
            mid = _mid(self.kalshi_book)
            self.portfolio.set_mark("kalshi", self.ticker, "YES", mid)
            self.portfolio.set_mark("kalshi", self.ticker, "NO",
                                    None if mid is None else Decimal(1) - mid)
        pm = self.pair.polymarket
        for outcome, token in (("YES", pm.yes_token), ("NO", pm.no_token)):
            if self.poly_feed.is_valid(outcome):
                self.portfolio.set_mark("polymarket", token, outcome,
                                        _mid(self.poly_feed.book(outcome)))

    async def _persist(self, strategy: BaseStrategy, result: StrategyResult,
                       now: datetime, volatility: Decimal) -> None:
        if strategy is self.arbitrage:
            self._persist_arbitrage(now, volatility)
        elif isinstance(result.detail, AMMQuote) and result.detail.action != "hold":
            await self._persist_amm(result.detail, now)

    def _persist_arbitrage(self, now: datetime, volatility: Decimal) -> None:
        arb = self.arbitrage
        self.thresholds.append(arb.last_threshold)
        for opportunity in arb.last_opportunities:
            if opportunity.size > 0:
                self.sink.record_opportunity(opportunity, arb.last_threshold, volatility)

        # spread history: whenever the cross-venue top of book or the
        # threshold changes, and on the periodic interval otherwise
        if arb.last_quotes:
            from engine.db.rows import tops_of                  # noqa: PLC0415
            key = (json.dumps(tops_of(arb.last_quotes), sort_keys=True), arb.last_threshold)
            if key != self._last_spread or self._due(self._last_spread_at, now):
                self.sink.record_spread(now, self.pair.pair_id, arb.last_quotes,
                                        arb.last_opportunities, arb.last_threshold, volatility)
                self._last_spread, self._last_spread_at = key, now

        if len(arb.attempts) > self._attempts_persisted:
            for attempt in arb.attempts[self._attempts_persisted:]:
                self.sink.record_attempt(attempt)
                for state in (attempt.yes_state, attempt.no_state, *attempt.unwind_states):
                    if state is not None:
                        self.sink.record_order(state, now)
                if attempt.dispatched:
                    self._record_books(now, "event")
            self._attempts_persisted = len(arb.attempts)

    async def _persist_amm(self, quote: AMMQuote, now: datetime) -> None:
        self.sink.record_amm_quote(quote)
        for state in quote.placed:
            self.sink.record_order(state, now)
        for order_id in quote.cancelled:
            for executor in self.factory.built():
                try:
                    self.sink.record_order(await executor.get_status(order_id), now)
                    break
                except OrderNotFound:
                    continue
        if quote.action == "requote":
            self._record_books(now, "event")

    def _due(self, last: datetime | None, now: datetime) -> bool:
        return last is None or now - last >= self.config.snapshot_interval

    def _record_periodic(self, now: datetime, volatility: Decimal) -> None:
        """Book snapshots on the interval; sentiment features when they change."""
        if self._due(self._last_snapshot, now):
            self._record_books(now, "periodic")
            self._last_snapshot = now

        level, confidence, n = sentiment_features(self.sentiment, self.pair.pair_id,
                                                  self.config.amm.min_points_for_confidence)
        latest = self.sentiment.latest(self.pair.pair_id)
        key = (level, volatility, confidence, n, latest.timestamp if latest else None)
        if key != self._last_sentiment or self._due(self._last_sentiment_at, now):
            self.sink.record_sentiment(now, self.pair.pair_id, level, volatility, confidence,
                                       n, latest if n else None, self.arbitrage.last_threshold)
            self._last_sentiment, self._last_sentiment_at = key, now

    def _record_books(self, now: datetime, reason: str) -> None:
        if self.kalshi.book_is_fresh and (self.kalshi_book.bids or self.kalshi_book.asks):
            self.sink.record_book_snapshot(now, "kalshi", self.ticker, "YES",
                                           self.kalshi_book, reason)
        pm = self.pair.polymarket
        for outcome, token in (("YES", pm.yes_token), ("NO", pm.no_token)):
            if self.poly_feed.is_valid(outcome):
                self.sink.record_book_snapshot(now, "polymarket", token, outcome,
                                               self.poly_feed.book(outcome), reason)

    # ---- sink lifecycle ----

    async def _open_sink(self, mode: str) -> None:
        start = getattr(self.sink, "start", None)
        if start is not None:
            await start(mode=mode, pair_id=self.pair.pair_id, config={
                "ticker": self.ticker, "size": self.config.size,
                "dry_run": self.config.dry_run, "amm_venues": ",".join(self.config.amm_venues),
                "threshold_base": self.config.threshold.base,
                "threshold_lam": self.config.threshold.lam,
                "max_position": self.portfolio.limits.max_position_per_market,
            })

    async def _close_sink(self) -> None:
        aclose = getattr(self.sink, "aclose", None)
        if aclose is not None:
            await aclose()

    async def _yield_to_writer(self) -> None:
        """A replay never sleeps; give the batch writer a turn when rows pile up."""
        if getattr(self.sink, "pending", 0) >= getattr(self.sink, "batch_size", 1 << 30):
            await asyncio.sleep(0)

    # ---- replay ----

    async def run_replay(self, poly_tape_path: str | Path, kalshi_tape_path: str | Path,
                         headlines: list[ScriptedHeadline] | None = None,
                         verbose: bool = False) -> PipelineReport:
        """
        Merge both tapes on local receive time and drive the full pipeline.

        Frames go through exactly the code a live run uses — PolymarketFeed
        for one venue, KalshiWSClient's message handler for the other — so
        this is an integration test of the whole stack, not of a stub.
        """
        self.kalshi.connected = True          # a replayed session owns its book
        headlines = sorted(headlines or [], key=lambda h: h.after_frames)
        next_headline = 0
        await self._open_sink("replay")
        try:
            frames = await self._replay_frames(poly_tape_path, kalshi_tape_path,
                                               headlines, next_headline, verbose)
        finally:
            await self._close_sink()
        return self.report(frames)

    async def _replay_frames(self, poly_tape_path, kalshi_tape_path,
                             headlines: list[ScriptedHeadline], next_headline: int,
                             verbose: bool) -> int:

        timeline = heapq.merge(
            ((f.recv_ts_ns, 0, f) for f in poly_tape.load_tape(str(poly_tape_path))),
            ((f.recv_ts_ns, 1, f) for f in kalshi_tape.load_tape(str(kalshi_tape_path))),
            key=lambda item: (item[0], item[1]),
        )

        frames = 0
        for recv_ts_ns, venue, frame in timeline:
            if venue == 0:
                self.poly_feed.handle(frame)
            else:
                await self.kalshi._handle_message(json.loads(frame.text))
            frames += 1
            now = datetime.fromtimestamp(recv_ts_ns / 1e9, tz=timezone.utc)
            self.now = now          # advance the clock the sentiment window uses

            while next_headline < len(headlines) and headlines[next_headline].after_frames <= frames:
                h = headlines[next_headline]
                self.sentiment.push(self.pair.pair_id, h.score, headline=h.text,
                                    source=h.source, timestamp=now)
                self.headlines_injected += 1
                next_headline += 1
                if verbose:
                    print(f"{now:%H:%M:%S}  headline (score {h.score}): {h.text}")

            if self._books_changed():
                await self.tick(now)
                await self._yield_to_writer()

        self.poly_feed.settle()               # run the end-of-group checks once more
        return frames

    # ---- live ----

    async def run_live(self, poll_seconds: float = 0.2,
                       max_seconds: float | None = None) -> PipelineReport:
        """
        Both WebSocket clients plus a strategy loop. Needs kalshi_engine/.env
        for Kalshi credentials; Polymarket's public CLOB feed needs none.
        """
        if self.poly_queue is None:
            raise RuntimeError("construct the pipeline with offline=False to run live")

        poly_client = PolymarketWsClient(self.pair.asset_ids(), self.poly_queue)
        stop = asyncio.Event()

        async def strategy_loop() -> None:
            while not stop.is_set():
                await asyncio.sleep(poll_seconds)
                self.now = None     # live: the sentiment window follows the real clock
                if isinstance(self.sentiment, RedisSentimentFeed):
                    await self.sentiment.drain()
                if self._books_changed():
                    await self.tick(datetime.now(tz=timezone.utc))

        async def timer() -> None:
            if max_seconds is not None:
                await asyncio.sleep(max_seconds)
                stop.set()

        # A failed task (Kalshi rejecting the credentials, say) surfaces as an
        # exception group. It is recorded on the report rather than raised,
        # because a run that traded before it died still has results worth
        # returning — but it must NOT be swallowed silently, which is what a
        # bare `return` inside a `finally` would do.
        errors: list[str] = []
        await self._open_sink("live")
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.kalshi.run_forever())
                tg.create_task(poly_client.run())
                tg.create_task(self.poly_feed.run())
                tg.create_task(strategy_loop())
                if max_seconds is not None:
                    tg.create_task(timer())
        except* Exception as group:
            for exc in group.exceptions:
                log.error("live run stopped: %s: %s", type(exc).__name__, exc)
                errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            for amm in self.amms:               # leave nothing resting at the venues
                await amm.cancel_all()
            await self._close_sink()

        report = self.report(0)
        report.errors = errors
        return report

    # ---- reporting ----

    def report(self, frames: int) -> PipelineReport:
        arb = self.arbitrage
        attempts = arb.attempts
        counts = self.sink.counts() if hasattr(self.sink, "counts") else {}
        return PipelineReport(
            pair_id=self.pair.pair_id,
            frames_replayed=frames,
            ticks=self.ticks,
            evaluations=arb.evaluations,
            opportunities=arb.opportunities_seen,
            declined_by_threshold=arb.declined_by_threshold,
            declined_by_preflight=arb.preflight_rejections,
            attempts=len(attempts),
            dispatched=sum(1 for a in attempts if a.dispatched),
            vetoed_by_risk=sum(1 for a in attempts
                               if not a.dispatched and "risk" in a.reason),
            matched_pairs=sum((a.matched_pairs for a in attempts), ZERO),
            locked_profit=sum((a.locked_profit for a in attempts), ZERO),
            naked_size=sum((a.naked_size for a in attempts), ZERO),
            fills=len(self.portfolio.fills),
            thresholds=self.thresholds,
            headlines_injected=self.headlines_injected,
            polymarket_check_failures=self.poly_feed.check_failures(),
            kalshi_sequence_gaps=getattr(self.kalshi, "sequence_gaps", 0),
            db_counts=counts,
            db_dropped=getattr(self.sink, "dropped", 0),
            portfolio=self.portfolio.summary(),
            amm=[amm.summary() for amm in self.amms],
        )

    def close(self) -> None:
        self.sink.close()


def build_pipeline(pair_config_path: str | Path | None = None,
                   ticker: str | None = None,
                   config: PipelineConfig | None = None,
                   offline: bool = True) -> ArbitragePipeline:
    """Load the pair config and construct a pipeline from it."""
    path = pair_config_path or (ROOT / "Polymarket_engine" / "config" / "pair.toml")
    pair = load_pair_config(path)
    return ArbitragePipeline(pair, ticker or pair.kalshi.ticker, config, offline=offline)


def main() -> None:
    import argparse

    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pair-config")
    p.add_argument("--kalshi-ticker")
    p.add_argument("--size", type=Decimal, default=Decimal("100"))
    p.add_argument("--replay", nargs=2, metavar=("POLYMARKET_TAPE", "KALSHI_TAPE"))
    p.add_argument("--live", action="store_true")
    p.add_argument("--max-seconds", type=float, help="live mode: stop after this long")
    p.add_argument("--db", help="write arbitrage events to this SQLite file")
    p.add_argument("--db-url", default=os.environ.get("DATABASE_URL"),
                   help="feature 6: full persistence to this SQLAlchemy async URL, e.g. "
                        "postgresql+asyncpg://arb:arb@localhost:5432/arb (default: $DATABASE_URL)")
    p.add_argument("--amm", action="append", default=[], choices=["kalshi", "polymarket"],
                   help="run an AMM quoter on this venue (repeatable)")
    p.add_argument("--amm-gamma", type=float, default=AMMConfig.gamma)
    p.add_argument("--amm-kappa", type=float, default=AMMConfig.kappa)
    p.add_argument("--amm-k", type=float, default=AMMConfig.k_sentiment,
                   help="AMM sentiment drift: $ shift at S x confidence = 1")
    p.add_argument("--amm-size", type=Decimal, default=AMMConfig.quote_size)
    p.add_argument("--redis-url", help="live mode: consume sentiment from this Redis")
    p.add_argument("--dry-run", action="store_true", help="price and log, dispatch nothing")
    p.add_argument("--kalshi-fee-multiplier", type=Decimal,
                   help="skip the fee-schedule lookup (makes replay network-free)")
    p.add_argument("--base-threshold", type=Decimal, default=Decimal("0.005"))
    p.add_argument("--lam", type=Decimal, default=Decimal("0.05"),
                   help="sentiment-volatility coefficient in dP_min")
    p.add_argument("--headline", action="append", default=[],
                   metavar="FRAMES:SCORE:TEXT",
                   help="replay: inject a scored headline after N frames (repeatable)")
    a = p.parse_args()

    if not (a.replay or a.live):
        p.error("choose --replay or --live")

    config = PipelineConfig(
        size=a.size, dry_run=a.dry_run, db_path=a.db, db_url=a.db_url, redis_url=a.redis_url,
        kalshi_fee_multiplier=a.kalshi_fee_multiplier,
        threshold=DynamicThreshold(base=a.base_threshold, lam=a.lam),
        amm_venues=tuple(dict.fromkeys(a.amm)),
        amm=AMMConfig(gamma=a.amm_gamma, kappa=a.amm_kappa, k_sentiment=a.amm_k,
                      quote_size=a.amm_size),
    )
    pipeline = build_pipeline(a.pair_config, a.kalshi_ticker, config, offline=not a.live)
    print(f"pair {pipeline.pair.pair_id}: Polymarket {pipeline.pair.polymarket.slug} "
          f"/ Kalshi {pipeline.ticker}")
    print(f"fees: Kalshi {pipeline.kalshi_schedule.fee_type} "
          f"x{pipeline.kalshi_schedule.multiplier}, Polymarket "
          f"{'rate ' + str(pipeline.pair.polymarket.fees.rate) if pipeline.pair.polymarket.fees.enabled else 'off'}")
    if not pipeline.pair.verification.criteria_match:
        print("WARNING: verification.criteria_match is false; the pair is not "
              "confirmed equivalent, so every edge below is provisional.")

    headlines = []
    for spec in a.headline:
        frames, score, text = spec.split(":", 2)
        headlines.append(ScriptedHeadline(after_frames=int(frames),
                                          score=Decimal(score), text=text))

    try:
        if a.replay:
            report = asyncio.run(pipeline.run_replay(a.replay[0], a.replay[1],
                                                     headlines=headlines, verbose=True))
        else:
            logging.basicConfig(level=logging.INFO,
                                format="%(levelname)s %(name)s: %(message)s")
            report = asyncio.run(pipeline.run_live(max_seconds=a.max_seconds))
        print()
        print(report.describe())
        print()
        print(pipeline.arbitrage.summary())
        if getattr(pipeline.sink, "run_id", None):
            print(f"database run_id: {pipeline.sink.run_id}")
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
