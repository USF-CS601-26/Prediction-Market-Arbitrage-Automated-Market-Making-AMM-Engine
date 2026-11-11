"""
engine/arbitrage.py

The arbitrage itself: buy YES on one venue and NO on the other, so that
exactly one of the two contracts pays $1 at settlement.

    profit per pair = $1 - yes_ask - no_ask - fees

This is a different trade from cross_venue_spread.py, which buys and sells
the SAME outcome across the two venues. That one needs the two venues to
disagree about the price; this one only needs their quotes to sum to less
than a dollar, which happens for a mechanical reason — the two books are
maintained by different crowds and neither is obliged to agree with the
other's complement. The payout is certain either way: whichever side of the
event occurs, one leg pays $1.00 and the other pays $0.00.

Three things make this harder than the formula suggests, and all three are
modelled here:

1. DEPTH. The top of each book is a lie at size. The executable size is
   found by walking both ladders in lockstep and pairing contracts level by
   level, stopping at the first chunk whose own marginal profit falls below
   the threshold. Stopping (rather than skipping) is the only correct
   behaviour: you cannot reach a deeper level without consuming the one in
   front of it.

2. FEES. Both venues charge a fee that is quadratic in price and rounded per
   fill, so fees per pair are neither constant nor proportional, and a fee
   model that takes an average price gets a different answer. Each chunk is
   charged at its own price, and a level split into two chunks pays two
   roundings — because it really is two fills.

3. THRESHOLD. A positive number is not a reason to trade. Between the
   decision and the fills there is a venue round trip, and the threshold is
   what pays for that risk. See engine/sentiment.py for why it widens with
   sentiment volatility rather than with sentiment level.

Not modelled, and worth stating plainly: the two legs settle on different
rails on different timelines, so capital is tied up asymmetrically; the
Polymarket leg settles in USDC, so a positive edge is gross of any
USDC/USD basis; and the whole thing assumes the two contracts resolve
identically, which is what config/pair.toml's criteria_match exists to
confirm and which is currently still false for the test pair.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_FLOOR, Decimal

from order_book import OrderBook
from pydantic import BaseModel, Field

from engine.books import ask_ladder, complement_book
from engine.executors.simulator import FeeFn, _no_fee
from engine.models import utcnow

ZERO = Decimal("0")
ONE = Decimal("1")

# A matched pair pays exactly this much, once, whichever way the event goes.
PAYOUT = ONE


@dataclass
class SlippageModel:
    """
    What we expect to lose to the book moving between the decision and the
    fill — on top of the depth already walked.

    `adverse_ticks` is the honest version of a latency model: assume each leg
    fills that many ticks worse than the book currently shows. One tick on
    each leg of a pair is 2c, which is more than most of these edges, so this
    defaults to zero and is turned on deliberately. It is applied to the
    price used for BOTH the cost and the fee, since a worse fill price is
    also the price the venue charges its fee on.

    `fixed_cost_per_pair` covers everything per-contract that is not a venue
    fee — gas, transfer costs, the cost of carrying two settlement rails.
    """
    adverse_ticks: Decimal = ZERO
    tick_size: Decimal = Decimal("0.01")
    fixed_cost_per_pair: Decimal = ZERO

    def adjust_buy(self, price: Decimal) -> Decimal:
        """A buyer slips upward: you pay more than you saw."""
        slipped = price + self.adverse_ticks * self.tick_size
        # a price at or above $1 is not a trade, it is a guaranteed loss
        return min(slipped, ONE)


@dataclass
class DynamicThreshold:
    """
    The minimum net profit per pair required to trade:

        dP_min = base + lambda * sentiment_volatility     (capped)

    base is the standing cost of being wrong about a quiet book. The lambda
    term is the news premium: when scored headlines are churning, the book
    can move further during the venue round trip than the whole edge is
    worth, so the edge demanded has to grow with the churn.

    The cap matters more than it looks. Volatility is a standard deviation of
    scores in [-1, 1], so it is bounded by 1 in theory but in practice a
    single contradictory headline pair can spike it; without a cap a brief
    spike would demand an edge no real book ever shows, silently turning the
    strategy off for the length of the window.
    """
    base: Decimal = Decimal("0.005")        # half a cent per pair
    lam: Decimal = Decimal("0.05")          # per unit of sentiment stdev
    cap: Decimal = Decimal("0.05")          # never demand more than 5c per pair

    def value(self, volatility: Decimal = ZERO) -> Decimal:
        if volatility < ZERO:
            volatility = ZERO
        return min(self.base + self.lam * volatility, self.cap)

    def describe(self, volatility: Decimal = ZERO) -> str:
        return (f"dP_min = {self.base} + {self.lam} x {volatility:.4f} "
                f"= {self.value(volatility):.4f}/pair"
                + (" (capped)" if self.base + self.lam * volatility > self.cap else ""))


@dataclass(frozen=True)
class OutcomeQuote:
    """
    Where to buy one outcome: which venue, which market, and the book and fee
    function to price it against.

    `book` must already be expressed as the named outcome's own book, i.e.
    its asks are what it costs to BUY that outcome. For Polymarket that is
    the token's book as the feed maintains it; for Kalshi the NO side has to
    be derived first (see kalshi_quotes below).
    """
    venue: str
    market_id: str
    outcome: str
    book: OrderBook
    fee_fn: FeeFn = _no_fee

    def best_ask(self) -> Decimal | None:
        _, ask = self.book.get_top_of_book()
        return ask.price if ask else None


@dataclass(frozen=True)
class VenueQuotes:
    """Both outcomes on one venue."""
    venue: str
    yes: OutcomeQuote
    no: OutcomeQuote


def kalshi_quotes(ticker: str, book: OrderBook, fee_fn: FeeFn = _no_fee) -> VenueQuotes:
    """
    Kalshi has one book, already normalized to YES by kalshi_ws.py, so the NO
    book is derived by reflecting it through $1.

    The complement is a point-in-time copy, so this must be called afresh on
    every evaluation. The strategy does exactly that rather than caching,
    because a cached complement is a quote against a book that has moved.
    """
    return VenueQuotes(
        venue="kalshi",
        yes=OutcomeQuote("kalshi", ticker, "YES", book, fee_fn),
        no=OutcomeQuote("kalshi", ticker, "NO", complement_book(book, ticker), fee_fn),
    )


def polymarket_quotes(yes_token: str, no_token: str, feed, fee_fn: FeeFn = _no_fee) -> VenueQuotes:
    """
    Polymarket publishes a book per outcome, so both sides are read directly.
    `feed` is anything satisfying engine.books.OutcomeBooks — PolymarketFeed
    in the pipeline, StaticOutcomeBooks in tests.
    """
    return VenueQuotes(
        venue="polymarket",
        yes=OutcomeQuote("polymarket", yes_token, "YES", feed.book("YES"), fee_fn),
        no=OutcomeQuote("polymarket", no_token, "NO", feed.book("NO"), fee_fn),
    )


class PairFill(BaseModel):
    """One chunk of matched contracts, priced on both legs."""
    size: Decimal
    yes_price: Decimal          # slippage-adjusted, i.e. what we expect to pay
    no_price: Decimal
    fees: Decimal               # both legs' fees for this chunk
    net_profit: Decimal         # for this chunk, after fees and fixed costs

    @property
    def net_per_pair(self) -> Decimal:
        return self.net_profit / self.size if self.size else ZERO


class ArbitrageOpportunity(BaseModel):
    """
    One direction of a complementary-pair arbitrage, sized against real depth.

    `size` is the executable size: how many pairs can be bought while every
    marginal pair still clears the threshold. A size of 0 with a positive
    `top_of_book_edge` is the common and important case — the top of book
    looks mispriced but there is nothing behind it.
    """
    pair_id: str
    timestamp: datetime = Field(default_factory=utcnow)

    yes_venue: str
    yes_market_id: str
    no_venue: str
    no_market_id: str

    size: Decimal = ZERO
    yes_avg_price: Decimal | None = None
    no_avg_price: Decimal | None = None
    yes_limit_price: Decimal | None = None   # worst price touched on the YES leg
    no_limit_price: Decimal | None = None
    gross_cost: Decimal = ZERO               # what both legs cost, before fees
    fees: Decimal = ZERO
    net_profit: Decimal = ZERO               # payout - gross_cost - fees - fixed costs
    threshold: Decimal = ZERO                # the dP_min this was judged against
    requested_size: Decimal = ZERO
    fills: list[PairFill] = Field(default_factory=list)

    # the naive number, kept for comparison: best ask + best ask, no fees, no depth
    top_of_book_cost: Decimal | None = None

    @property
    def direction(self) -> str:
        return f"{self.yes_venue} YES + {self.no_venue} NO"

    @property
    def net_per_pair(self) -> Decimal | None:
        return (self.net_profit / self.size) if self.size > 0 else None

    @property
    def cost_per_pair(self) -> Decimal | None:
        """All-in cost of one pair, fees included. Profitable below $1.00."""
        return ((self.gross_cost + self.fees) / self.size) if self.size > 0 else None

    @property
    def top_of_book_edge(self) -> Decimal | None:
        """$1 - (best yes ask + best no ask). Pre-fee, pre-depth: indicative only."""
        return None if self.top_of_book_cost is None else PAYOUT - self.top_of_book_cost

    @property
    def profitable(self) -> bool:
        return self.size > 0 and self.net_profit > ZERO

    def describe(self) -> str:
        if self.size == 0:
            tob = ("-" if self.top_of_book_edge is None
                   else f"{self.top_of_book_edge * 100:+.2f}c")
            return (f"  {self.direction}: nothing executable at "
                    f"dP_min {self.threshold:.4f} (top of book {tob}/pair)")
        return (f"  {self.direction}: {self.size} pairs @ "
                f"{self.yes_avg_price:.4f} + {self.no_avg_price:.4f} "
                f"= {self.cost_per_pair:.4f}/pair all-in"
                f"   net ${self.net_profit:+.4f} "
                f"({self.net_per_pair * 100:+.2f}c/pair, dP_min {self.threshold:.4f})")


def evaluate(pair_id: str, yes: OutcomeQuote, no: OutcomeQuote,
             max_size: Decimal, threshold: Decimal = ZERO,
             slippage: SlippageModel | None = None,
             size_increment: Decimal = ONE) -> ArbitrageOpportunity:
    """
    Walk both ladders in lockstep and return the largest profitable pair
    trade, up to `max_size`.

    size_increment defaults to 1 because a Kalshi contract cannot be split;
    pairing 0.37 of a contract against 0.37 of a Polymarket share is not a
    trade either venue would accept. A chunk that quantizes to zero ends the
    walk.
    """
    slip = slippage or SlippageModel()
    opp = ArbitrageOpportunity(
        pair_id=pair_id,
        yes_venue=yes.venue, yes_market_id=yes.market_id,
        no_venue=no.venue, no_market_id=no.market_id,
        threshold=threshold, requested_size=max_size,
    )

    yes_best, no_best = yes.best_ask(), no.best_ask()
    if yes_best is not None and no_best is not None:
        opp.top_of_book_cost = yes_best + no_best

    # mutable copies: a level is consumed across however many chunks the
    # other leg's levels force it to be split into
    yes_levels = [[p, s] for p, s in ask_ladder(yes.book)]
    no_levels = [[p, s] for p, s in ask_ladder(no.book)]

    i = j = 0
    taken = ZERO
    gross = ZERO
    fees = ZERO
    net = ZERO
    yes_notional = no_notional = ZERO
    worst_yes = worst_no = None

    while i < len(yes_levels) and j < len(no_levels) and taken < max_size:
        yes_price, yes_avail = yes_levels[i]
        no_price, no_avail = no_levels[j]

        chunk = _quantize(min(yes_avail, no_avail, max_size - taken), size_increment)
        if chunk <= ZERO:
            break

        yes_at = slip.adjust_buy(yes_price)
        no_at = slip.adjust_buy(no_price)
        chunk_fees = yes.fee_fn(chunk, yes_at) + no.fee_fn(chunk, no_at)
        chunk_cost = chunk * (yes_at + no_at)
        chunk_net = (chunk * PAYOUT) - chunk_cost - chunk_fees - (chunk * slip.fixed_cost_per_pair)

        # the marginal test: this chunk must clear the threshold on its own.
        # Averaging it against earlier, better chunks is how a detector talks
        # itself into trading size it should not.
        if chunk_net / chunk < threshold:
            break

        opp.fills.append(PairFill(size=chunk, yes_price=yes_at, no_price=no_at,
                                  fees=chunk_fees, net_profit=chunk_net))
        taken += chunk
        gross += chunk_cost
        fees += chunk_fees
        net += chunk_net
        yes_notional += chunk * yes_at
        no_notional += chunk * no_at
        worst_yes = yes_at if worst_yes is None else max(worst_yes, yes_at)
        worst_no = no_at if worst_no is None else max(worst_no, no_at)

        yes_levels[i][1] -= chunk
        no_levels[j][1] -= chunk
        if yes_levels[i][1] <= ZERO:
            i += 1
        if no_levels[j][1] <= ZERO:
            j += 1

    if taken > ZERO:
        opp.size = taken
        opp.gross_cost = gross
        opp.fees = fees
        opp.net_profit = net
        opp.yes_avg_price = yes_notional / taken
        opp.no_avg_price = no_notional / taken
        opp.yes_limit_price = worst_yes
        opp.no_limit_price = worst_no
    return opp


def evaluate_directions(pair_id: str, a: VenueQuotes, b: VenueQuotes,
                        max_size: Decimal, threshold: Decimal = ZERO,
                        slippage: SlippageModel | None = None,
                        size_increment: Decimal = ONE) -> list[ArbitrageOpportunity]:
    """
    Both ways round, best first.

    Both are always returned, even the unprofitable one, so a log can record
    the road not taken — which is the only way to tell afterwards whether a
    missed trade was missed because it was not there or because the
    threshold declined it.
    """
    directions = [
        evaluate(pair_id, a.yes, b.no, max_size, threshold, slippage, size_increment),
        evaluate(pair_id, b.yes, a.no, max_size, threshold, slippage, size_increment),
    ]
    return sorted(directions, key=lambda o: (o.net_profit, o.size), reverse=True)


def _quantize(size: Decimal, increment: Decimal) -> Decimal:
    """Round DOWN to a tradable size. Never round up: the depth is not there."""
    if increment <= ZERO:
        return size
    return (size / increment).to_integral_value(rounding=ROUND_FLOOR) * increment
