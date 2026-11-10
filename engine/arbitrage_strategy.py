"""
engine/arbitrage_strategy.py

ArbitrageStrategy — the decision and execution half of the complementary-pair
arbitrage. The pricing half is engine/arbitrage.py, kept separate because it
is pure arithmetic and should be testable without an event loop, an executor
or a risk manager.

One tick does five things:

    1. read both venues' quotes (and refuse to act if either book is stale)
    2. widen the threshold by the current sentiment volatility
    3. price both directions against real depth
    4. ask the PortfolioManager, which may shrink the size or veto outright
    5. dispatch both legs together, then reconcile what actually happened

Step 5 is where the real work is. A paired arbitrage is only an arbitrage
while both legs exist. Three things can break the pair, and all three happen:

  - one leg is REJECTED outright (bad price, venue error, random rejection)
  - one leg fills PARTIALLY, because depth moved during the round trip
  - one leg fills fully and the other rests unfilled

In every case the result is a naked directional position, which is the exact
risk this strategy exists not to take. reconcile() cancels every remainder
and then flattens the unmatched excess, accepting a known loss now instead
of carrying an unknown one.

Why flatten rather than complete the pair: when a NO leg fails, the two
repairs are to sell the excess YES back into its bids, or to buy the missing
NO at whatever it now costs. Completing is sometimes cheaper and is what a
production system would price both ways. Flattening is chosen here because
its cost is bounded by the spread we can see, whereas completing is
unbounded when the leg failed BECAUSE the depth disappeared — and an unwind
path is the last place to put an unbounded cost. The alternative is noted
rather than implemented.

Both legs are dispatched with asyncio.gather, i.e. concurrently rather than
sequentially. Sequential dispatch would mean the second leg is sent after a
full round trip of exposure on the first, which roughly doubles the window
the threshold is paying for.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

from engine.arbitrage import (ArbitrageOpportunity, DynamicThreshold, OutcomeQuote,
                              SlippageModel, VenueQuotes, _quantize, evaluate_directions)
from engine.executors.base import OrderNotFound
from engine.executors.factory import OrderExecutorFactory
from engine.models import (Order, OrderSide, OrderState, OrderStatus, next_order_id,
                           utcnow)
from engine.strategy import BaseStrategy, StrategyResult

log = logging.getLogger(__name__)

ZERO = Decimal("0")
ONE = Decimal("1")

# Returns this venue's current quotes, or None when its book must not be
# traded (disconnected, unseeded, mid-resync, or failing its own checks).
QuotesProvider = Callable[[], VenueQuotes | None]


class ArbitrageAttempt(BaseModel):
    """
    What one execution attempt actually did, as opposed to what it hoped to.

    `matched_pairs` is the number that matters: pairs with both legs filled,
    which are the only ones that carry the $1 certain payout. Everything else
    on this model describes the gap between that and the opportunity.
    """
    opportunity: ArbitrageOpportunity
    dispatched: bool = False
    reason: str = ""
    yes_state: OrderState | None = None
    no_state: OrderState | None = None
    matched_pairs: Decimal = ZERO
    cancelled: list[str] = Field(default_factory=list)
    unwind_states: list[OrderState] = Field(default_factory=list)
    unwound_size: Decimal = ZERO
    naked_size: Decimal = ZERO       # excess we could NOT flatten
    naked_leg: str | None = None

    @property
    def cash_spent(self) -> Decimal:
        """
        Net cash across every order this attempt sent, unwinds included.
        Negative while the position is open: the $1 per matched pair arrives
        at settlement, not now.
        """
        states = [s for s in (self.yes_state, self.no_state) if s is not None]
        return sum((s.net_cash for s in states + self.unwind_states), ZERO)

    @property
    def locked_profit(self) -> Decimal:
        """
        Realized profit assuming the matched pairs settle as designed:
        $1 per matched pair, plus the (negative) cash already spent. This is
        the honest P&L of the attempt, and it goes negative when a leg
        mismatch had to be unwound at a loss.
        """
        return self.matched_pairs * ONE + self.cash_spent

    @property
    def dispatched_size(self) -> Decimal:
        """
        The size actually sent, which is the opportunity's size unless the
        risk layer shrank it.
        """
        return self.yes_state.order.size if self.yes_state is not None else ZERO

    @property
    def clean(self) -> bool:
        """
        Both dispatched legs filled in full, nothing left over, nothing naked.

        Measured against the DISPATCHED size rather than the opportunity's
        size: a trade the risk layer shrank from 100 pairs to 40 and then
        filled 40-for-40 is perfectly hedged, and calling that unclean would
        conflate a position limit doing its job with an execution failure.
        """
        return (self.dispatched
                and self.unwound_size == ZERO
                and self.naked_size == ZERO
                and self.matched_pairs == self.dispatched_size)

    def describe(self) -> str:
        if not self.dispatched:
            return f"  not dispatched: {self.reason}"
        bits = [f"  matched {self.matched_pairs} pairs",
                f"locked ${self.locked_profit:+.4f}"]
        if self.unwound_size:
            bits.append(f"unwound {self.unwound_size}")
        if self.naked_size:
            bits.append(f"NAKED {self.naked_size} on {self.naked_leg}")
        if self.cancelled:
            bits.append(f"cancelled {len(self.cancelled)}")
        return ", ".join(bits)


class ArbitrageStrategy(BaseStrategy):
    """
    Args:
        pair_id:      the verified contract pair this strategy trades.
        quotes_a/b:   zero-argument callables returning each venue's
                      VenueQuotes, or None when that venue is untradable.
                      Callables rather than books so the strategy never holds
                      a stale derived book: Kalshi's NO side is a reflection
                      of its YES book and has to be rebuilt every tick.
        factory:      OrderExecutorFactory; orders are routed by venue name.
        risk:         anything implementing engine.portfolio.RiskManager.
        sentiment:    optional SentimentFeed. Without one the threshold stays
                      at its base value, which is the correct degraded
                      behaviour: no sentiment data is not evidence of calm.
        max_size:     pairs per attempt, before depth and risk shrink it.
        dry_run:      price and log, dispatch nothing. The replay harness uses
                      this to measure how often an edge existed without
                      pretending to have traded it.
    """

    def __init__(self, pair_id: str,
                 quotes_a: QuotesProvider, quotes_b: QuotesProvider,
                 factory: OrderExecutorFactory, risk,
                 sentiment=None,
                 threshold: DynamicThreshold | None = None,
                 slippage: SlippageModel | None = None,
                 max_size: Decimal = Decimal("100"),
                 size_increment: Decimal = ONE,
                 unwind_slack_ticks: Decimal = Decimal("2"),
                 tick_size: Decimal = Decimal("0.01"),
                 dry_run: bool = False,
                 strategy_id: str | None = None,
                 enabled: bool = True):
        super().__init__(strategy_id or f"arb:{pair_id}", enabled=enabled)
        self.pair_id = pair_id
        self.quotes_a = quotes_a
        self.quotes_b = quotes_b
        self.factory = factory
        self.risk = risk
        self.sentiment = sentiment
        self.threshold = threshold or DynamicThreshold()
        self.slippage = slippage or SlippageModel()
        self.max_size = max_size
        self.size_increment = size_increment
        self.unwind_slack_ticks = unwind_slack_ticks
        self.tick_size = tick_size
        self.dry_run = dry_run

        # observability: a replay is only useful if it can be summarized
        self.evaluations = 0
        self.opportunities_seen = 0          # priced profitable at the threshold
        self.declined_by_threshold = 0       # positive top of book, no executable size
        self.preflight_rejections = 0         # a leg a venue would reject on sight
        self.attempts: list[ArbitrageAttempt] = []
        self.last_opportunities: list[ArbitrageOpportunity] = []
        self.last_quotes: dict[str, VenueQuotes] = {}
        self.last_threshold = self.threshold.base

    # ---- pricing ----

    def current_threshold(self) -> Decimal:
        """dP_min for this tick, widened by current sentiment volatility."""
        volatility = ZERO
        if self.sentiment is not None:
            volatility = self.sentiment.volatility(self.pair_id)
        return self.threshold.value(volatility)

    def price(self) -> list[ArbitrageOpportunity] | None:
        """
        Both directions, best first, or None if either venue is untradable.
        Pure: no orders, no risk calls. Separated from on_tick so a replay can
        measure opportunities without an execution path.
        """
        a, b = self.quotes_a(), self.quotes_b()
        if a is None or b is None:
            self.last_quotes = {}
            return None
        self.last_quotes = {a.venue: a, b.venue: b}
        self.last_threshold = self.current_threshold()
        self.evaluations += 1
        opportunities = evaluate_directions(
            self.pair_id, a, b, self.max_size, self.last_threshold,
            self.slippage, self.size_increment,
        )
        self.last_opportunities = opportunities
        return opportunities

    # ---- the tick ----

    async def on_tick(self, now: datetime | None = None) -> StrategyResult:
        now = now or utcnow()
        opportunities = self.price()
        if opportunities is None:
            return self.skip("one or both venues are not tradable", now)

        best = opportunities[0]
        if not best.profitable:
            # distinguish "nothing there" from "there, but declined": only the
            # second says anything about the threshold
            if any(o.top_of_book_edge and o.top_of_book_edge > ZERO for o in opportunities):
                self.declined_by_threshold += 1
                return self.skip(
                    f"top of book shows an edge but nothing clears "
                    f"dP_min {self.last_threshold:.4f} after depth and fees", now)
            return self.skip("no edge", now)

        self.opportunities_seen += 1
        if self.dry_run:
            return StrategyResult(strategy_id=self.strategy_id, timestamp=now,
                                  acted=False, reason="dry run", detail=best)

        # the quotes this opportunity was priced against, NOT a fresh read:
        # an unwind has to be priced against the same book the decision used
        attempt = await self.execute(best, self.last_quotes)
        self.attempts.append(attempt)
        return StrategyResult(strategy_id=self.strategy_id, timestamp=now,
                              acted=attempt.dispatched,
                              reason=attempt.reason or "dispatched",
                              detail=attempt)

    # ---- execution ----

    async def execute(self, opp: ArbitrageOpportunity,
                      quotes: dict[str, VenueQuotes]) -> ArbitrageAttempt:
        """
        Risk-check, dispatch both legs concurrently, then reconcile.

        The two orders are limit orders priced at the WORST level the depth
        walk touched. That is what keeps a moving book from turning into a
        worse fill: if the book has moved, we fill less, not worse. A market
        order here would be an offer to pay away the entire edge.
        """
        attempt = ArbitrageAttempt(opportunity=opp)

        yes_order = Order(
            order_id=next_order_id("yes"), venue=opp.yes_venue,
            market_id=opp.yes_market_id, outcome="YES", side=OrderSide.BUY,
            size=opp.size, limit_price=opp.yes_limit_price,
            pair_id=self.pair_id, strategy_id=self.strategy_id,
        )
        no_order = Order(
            order_id=next_order_id("no"), venue=opp.no_venue,
            market_id=opp.no_market_id, outcome="NO", side=OrderSide.BUY,
            size=opp.size, limit_price=opp.no_limit_price,
            pair_id=self.pair_id, strategy_id=self.strategy_id,
        )

        size = await self._risk_sized(yes_order, no_order, attempt)
        if size is None:
            return attempt
        # Shrinking is safe without re-pricing: the walk accepted every chunk
        # on its own marginal merit, so any prefix of it also clears dP_min.
        yes_order.size = no_order.size = size

        # Pre-flight AFTER sizing: shrinking can itself break a venue rule,
        # most obviously by dropping below a minimum order size. A leg that
        # is certain to be rejected must abort the pair, not be sent and then
        # unwound.
        for order in (yes_order, no_order):
            reason = self.factory.for_order(order).preflight(order)
            if reason is not None:
                attempt.reason = f"{order.outcome} leg would be rejected: {reason}"
                self.preflight_rejections += 1
                log.info("not dispatching %s: %s", self.pair_id, attempt.reason)
                return attempt

        attempt.dispatched = True
        yes_state, no_state = await asyncio.gather(
            self.factory.for_order(yes_order).submit_order(yes_order),
            self.factory.for_order(no_order).submit_order(no_order),
        )
        attempt.yes_state, attempt.no_state = yes_state, no_state
        await self.reconcile(attempt, quotes)
        return attempt

    async def _risk_sized(self, yes_order: Order, no_order: Order,
                          attempt: ArbitrageAttempt) -> Decimal | None:
        """
        Both legs through check_order. Returns the size to trade, or None to
        abort.

        A veto on EITHER leg aborts BOTH. Dispatching the approved leg alone
        would leave exactly the naked position the risk check just refused —
        the one case where partially honouring a decision is worse than
        ignoring it.
        """
        sizes = []
        for order in (yes_order, no_order):
            decision = await self.check_risk(self.risk, order)
            if not decision.approved:
                attempt.reason = f"risk vetoed the {order.outcome} leg: {decision.reason}"
                return None
            sizes.append(decision.max_size if decision.max_size is not None else order.size)

        size = self._quantize(min(sizes))
        if size <= ZERO:
            attempt.reason = "risk shrank the trade to zero"
            return None
        if size < yes_order.size:
            attempt.reason = f"risk shrank {yes_order.size} -> {size}"
        return size

    async def reconcile(self, attempt: ArbitrageAttempt,
                        quotes: dict[str, VenueQuotes]) -> None:
        """
        Make the position match what we are willing to hold.

        Order of operations matters. Remainders are cancelled FIRST, so that
        nothing else fills while we are deciding what to unwind; only then is
        the excess measured and flattened. Doing it the other way round can
        unwind against a leg that fills again a moment later.
        """
        yes_state, no_state = attempt.yes_state, attempt.no_state
        await self._cancel_remainder(yes_state, attempt)
        await self._cancel_remainder(no_state, attempt)

        yes_filled = yes_state.filled_size if yes_state else ZERO
        no_filled = no_state.filled_size if no_state else ZERO
        attempt.matched_pairs = min(yes_filled, no_filled)

        if yes_filled == no_filled:
            return

        # the leg that filled more is the one carrying naked risk
        if yes_filled > no_filled:
            excess, state, quote = yes_filled - no_filled, yes_state, self._quote_for(quotes, attempt, "YES")
        else:
            excess, state, quote = no_filled - yes_filled, no_state, self._quote_for(quotes, attempt, "NO")

        log.warning("leg mismatch on %s: YES %s vs NO %s; flattening %s",
                    self.pair_id, yes_filled, no_filled, excess)
        await self._unwind(attempt, state, quote, excess)

    def _quote_for(self, quotes: dict[str, VenueQuotes],
                   attempt: ArbitrageAttempt, outcome: str) -> OutcomeQuote | None:
        opp = attempt.opportunity
        venue = opp.yes_venue if outcome == "YES" else opp.no_venue
        vq = quotes.get(venue)
        if vq is None:
            return None
        return vq.yes if outcome == "YES" else vq.no

    async def _cancel_remainder(self, state: OrderState | None,
                                attempt: ArbitrageAttempt) -> None:
        """
        Pull an order's unfilled remainder. A False return means it reached a
        terminal state first, which is a race rather than a failure, so it is
        not treated as an error.
        """
        if state is None or state.status.is_terminal:
            return
        try:
            if await self.factory.for_order(state.order).cancel_order(state.order.order_id):
                attempt.cancelled.append(state.order.order_id)
        except OrderNotFound:
            log.warning("cancel for unknown order %s", state.order.order_id)

    async def _unwind(self, attempt: ArbitrageAttempt, state: OrderState | None,
                      quote: OutcomeQuote | None, excess: Decimal) -> None:
        """
        Sell the unmatched excess back into its own book.

        The limit is set `unwind_slack_ticks` BELOW the best bid, deliberately
        giving up a couple of ticks. An unwind priced at the touch does not
        fill when the book is moving, and a half-unwound leg is worse than a
        slightly expensive one.

        If the excess cannot be flattened — no bid, no book, or the unwind
        itself only partly fills — the remainder is recorded as `naked_size`
        rather than quietly dropped. The PortfolioManager still has the
        position from the fill stream; this is what tells an operator it is
        there.
        """
        if state is None or quote is None:
            attempt.naked_size = excess
            attempt.naked_leg = "unknown"
            log.error("cannot unwind %s on %s: no book available", excess, self.pair_id)
            return

        outcome = quote.outcome
        bid, _ = quote.book.get_top_of_book()
        if bid is None:
            attempt.naked_size = excess
            attempt.naked_leg = f"{quote.venue}/{outcome}"
            log.error("cannot unwind %s %s %s: no bid in the book",
                      excess, quote.venue, outcome)
            return

        limit = max(bid.price - self.unwind_slack_ticks * self.tick_size, self.tick_size)
        unwind = Order(
            order_id=next_order_id("unwind"), venue=quote.venue,
            market_id=state.order.market_id, outcome=outcome, side=OrderSide.SELL,
            size=self._quantize(excess), limit_price=limit,
            pair_id=self.pair_id, strategy_id=self.strategy_id, is_unwind=True,
        )

        decision = await self.check_risk(self.risk, unwind)
        if not decision.approved:
            # a risk layer that blocks an unwind is misconfigured: the order
            # reduces exposure. Record the naked size loudly and move on.
            attempt.naked_size = excess
            attempt.naked_leg = f"{quote.venue}/{outcome}"
            log.error("risk blocked the unwind of %s %s: %s",
                      excess, outcome, decision.reason)
            return

        result = await self.factory.for_order(unwind).submit_order(unwind)
        attempt.unwind_states.append(result)
        attempt.unwound_size = result.filled_size
        if result.status == OrderStatus.PARTIALLY_FILLED or result.filled_size < excess:
            await self._cancel_remainder(result, attempt)
            attempt.naked_size = excess - result.filled_size
            attempt.naked_leg = f"{quote.venue}/{outcome}"
            log.error("unwound only %s of %s %s %s; %s left naked",
                      result.filled_size, excess, quote.venue, outcome,
                      attempt.naked_size)

    def _quantize(self, size: Decimal) -> Decimal:
        return _quantize(size, self.size_increment)

    # ---- reporting ----

    def summary(self) -> str:
        matched = sum((a.matched_pairs for a in self.attempts), ZERO)
        locked = sum((a.locked_profit for a in self.attempts), ZERO)
        naked = sum((a.naked_size for a in self.attempts), ZERO)
        return (f"{self.strategy_id}: {self.evaluations} evaluations, "
                f"{self.opportunities_seen} opportunities, "
                f"{self.declined_by_threshold} declined by dP_min, "
                f"{self.preflight_rejections} declined by pre-flight, "
                f"{len(self.attempts)} attempts "
                f"({sum(1 for a in self.attempts if a.dispatched)} dispatched), "
                f"{matched} pairs matched, "
                f"locked ${locked:+.4f}"
                + (f", {naked} NAKED" if naked else ""))
