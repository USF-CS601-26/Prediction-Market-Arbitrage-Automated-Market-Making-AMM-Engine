# System architecture

Three layers. Market data flows up, orders flow down, and nothing in a lower
layer knows what a higher one is doing.

```
                    ┌─────────────────────────────────────────────┐
  news / Redis ───▶ │  engine/sentiment.py                        │
                    │    RedisSentimentFeed  (stream sentiment:*)  │
                    │    RollingSentiment    (window, volatility)  │
                    └───────┬──────────────────────┬──────────────┘
                   level    │                      │  volatility
             (AMM skews)    │                      │  (threshold widens)
                            ▼                      ▼
┌──────────────────┐   ┌──────────────────────────────────────────┐
│ kalshi_engine/   │   │  engine/  — venue-neutral trading layer   │
│  kalshi_ws       │──▶│                                          │
│  order_book  ────┼──▶│  arbitrage.py          pricing (pure)     │
│  kalshi_fees     │   │  arbitrage_strategy.py decide + execute   │
└──────────────────┘   │  strategy.py           BaseStrategy       │
                       │  portfolio.py          risk / positions   │
┌──────────────────┐   │  books.py              outcome books      │
│ Polymarket_engine│──▶│  executors/            venue adapters     │
│  ws_client       │   │  persistence.py        SQLite event sink   │
│  feed (YES + NO) │   │  pipeline.py           wires it all up    │
│  fees            │   └───────────────┬──────────────────────────┘
└──────────────────┘                   │ orders
                                       ▼
                         ┌──────────────────────────────┐
                         │ MockKalshiExecutor            │
                         │ MockPolymarketExecutor        │
                         │   latency, partials, rejects  │
                         └──────────────────────────────┘
```

`kalshi_engine/models.py` owns the **market data** contracts (OrderBook
snapshots and deltas, ExecutionQuote, CrossVenueEdge). `engine/models.py`
owns the **order** contracts (Order, Fill, OrderState, RiskDecision). The
split is deliberate: the ingestion engines have no dependency on the trading
side, so they stay usable on their own.

---

## The trade

Buy YES on one venue and NO on the other. Exactly one of the two contracts
pays $1.00 at settlement, whichever way the event goes, so:

```
profit per pair = $1.00 − yes_ask − no_ask − fees
```

### Expressing both venues the same way

The two venues publish the same information in different shapes, and the
arithmetic only works once they match (`engine/books.py`):

| | what the feed gives us | how we buy NO |
|---|---|---|
| **Kalshi** | one book, already normalized to YES by `kalshi_ws.py` (no-side bids folded into asks at `1 − price`) | derived: `complement_book()` reflects the YES book through $1 |
| **Polymarket** | two token books, one per outcome | read the NO book directly |

`complement_book()` is an identity about the same resting orders, not an
approximation: a YES bid of 300 at 0.41 **is** an offer of 300 NO at 0.59.

Polymarket deliberately does *not* get the same derivation. On its CLOB a NO
ask and the mirroring YES bid are the same resting order, so merging a real
NO book with one synthesized from YES bids would count the same liquidity
twice and report double the executable size.
`test_polymarkets_no_book_mirrors_its_yes_book_on_real_data` checks that
mirror property against the recorded tape rather than trusting the docs — it
held on all 2 567 ticks where both books were valid.

### This is the same trade as the same-outcome spread

Because a NO ask is the mirror of a YES bid on *both* venues:

```
ask_A + (1 − bid_B) < 1    ⇔    ask_A < bid_B
buy YES on A + buy NO on B  ==  buy YES on A + sell YES on B
```

So the complementary-pair formulation and `cross_venue_spread.py`'s
same-outcome formulation are two ways of writing one trade. That is a useful
fact rather than a redundancy: the two modules were written independently and
walk the books differently (one pairs contracts, the other quotes each leg on
its own), so they can be checked against each other.
`test_complementary_pair_agrees_with_the_same_outcome_engine` asserts they
produce identical net edges and identical fees across multi-level depth and
Kalshi's quadratic fee schedule.

It also confirms the symmetry the Kalshi NO derivation relies on: charging
Kalshi's fee at the NO price `1 − p` gives the same number as charging it at
the YES price `p`, because `p(1−p)` is symmetric about 0.50.

### Sizing against real depth

Top of book is a lie at size. `arbitrage.evaluate()` walks both ask ladders in
lockstep, pairing contracts level by level:

1. take `chunk = min(yes_level_remaining, no_level_remaining, size_remaining)`
2. price that chunk at its own two prices and charge both venues' fees **on
   that chunk**
3. if the chunk's own marginal profit per pair is below `ΔP_min`, **stop**

Stopping rather than skipping is the only correct behaviour: you cannot reach
a deeper level without consuming the one in front of it. Testing each chunk on
its own marginal merit — rather than averaging it against earlier, better
chunks — is what stops the detector talking itself into size it should not
take.

A level is consumed across as many chunks as the other leg's levels force it
into, and each chunk pays its own rounded fee, because each chunk really is a
separate fill.

### Fees are not a percentage

Both venues charge a fee that is quadratic in price:

```
Kalshi      ceil_to_cent(0.07 × multiplier × C × p × (1 − p))
Polymarket  round_5dp(C × rate × (p × (1 − p)) ** exponent)
```

The `p(1−p)` term peaks at 0.50. The same 2c gross edge is worth $0.25 on 100
pairs at 0.49 and $1.66 at 0.05 — a 6.6× difference from the fee curve alone
(`test_kalshi_fees_erase_a_two_cent_edge_near_fifty_cents` and its sibling).
A detector that models fees as flat basis points invents arbitrage in the
middle of the book and misses it at the wings.

### The dynamic threshold

```
ΔP_min = base + λ · sentiment_volatility        (capped)
```

The threshold is a **latency hedge**, not a risk preference. Between seeing an
edge and both legs filling there is a venue round trip — 40 ms on Kalshi, 120
ms on Polymarket in the mock defaults — and during a news surge the book can
move further in that window than the whole edge is worth.

It is the **volatility** of sentiment that widens it, not the level. A market
that has been confidently bullish all morning is not dangerous to arbitrage;
one that is being repriced is. Person A's AMM reads the signed *level* from
the same feed for the opposite purpose, which is why `RollingSentiment`
exposes both.

The cap matters more than it looks: without it a single contradictory
headline pair would demand an edge no real book shows, silently switching the
strategy off for the length of the window.

---

## Execution

### Why a mock venue is not a stub

`SimulatedExecutor` fills by walking the same `OrderBook` the ingestion
engines maintain, honouring the limit price level by level and charging the
venue's real fee function per fill. On top of that it models what makes
execution different from pricing:

- **latency** — the round trip happens *before* anything is decided, so the
  book an order fills against is the one that exists on arrival.
- **partial fills**, from two causes kept distinct: genuine depth/limit
  exhaustion (deterministic, read off the book) and the venue filling only
  part of a marketable order (stochastic, `partial_fill_prob`).
- **rejections** — deterministic ones (bad size, off-tick price, stale book,
  below venue minimum) and a configurable random rate for everything a venue
  rejects for reasons we cannot see.

Two deliberate choices:

**An unfilled remainder rests.** A partly filled limit order is left `OPEN` or
`PARTIALLY_FILLED`, not cancelled. That is what a venue does, and it is the
whole reason the unwind path gets exercised — if the mock silently cancelled
remainders the leg-mismatch code would never run in a test and the bug would
be found in production.

**A fill does not remove the depth it consumed** (`consume_liquidity=False` by
default). The feed owns the book and is its only mutator; writing our fills
into it would corrupt the invariants the ingestion engines check against the
exchange, notably `PolymarketFeed`'s `check_top_mismatch`, which compares our
top of book with the exchange's own hint. The cost is that repeated orders
against a frozen book can each fill at the same level; the pipeline evaluates
at most one opportunity per book change, which bounds it. Tests that want
depletion opt in.

Every random draw comes from a seeded `random.Random`, so a whole simulated
session replays identically.

### Routing

A strategy holds an `OrderExecutorFactory`, not a set of executors. Swapping a
mock for a real venue client is a factory registration, not a strategy change.
Executors are built lazily and cached per venue, because an executor owns
per-venue state — the book it fills against, its order table, its seeded RNG —
and a fresh one per call would lose the history `cancel_order` and
`get_status` depend on.

### Keeping the pair a pair

A paired arbitrage is only an arbitrage while both legs exist. Three things
break it, and all three happen:

- one leg is rejected outright
- one leg fills partially because depth moved during the round trip
- one leg fills in full and the other rests unfilled

`ArbitrageStrategy.execute()` handles them in this order:

1. **Risk-check both legs.** A veto on *either* aborts *both*. Dispatching
   only the approved leg leaves exactly the naked position the risk check just
   refused — the one case where partially honouring a decision is worse than
   ignoring it. `RiskDecision.max_size` lets the risk layer shrink instead of
   only vetoing; shrinking is safe without re-pricing, because the walk
   accepted every chunk on its own merit, so any prefix of it also clears
   `ΔP_min`.
2. **Pre-flight both legs**, after sizing. `BaseExecutor.preflight()` is
   validation without sending: a leg that is certain to be rejected (off-tick
   price, below the venue minimum — which shrinking can itself cause) aborts
   the pair rather than being sent and then repaired. Declining is strictly
   better than a loss-making unwind.
3. **Dispatch concurrently** with `asyncio.gather`. Sequential dispatch would
   expose the first leg for a full round trip, roughly doubling the window the
   threshold is paying for.
4. **Cancel every remainder first**, then measure the excess. The other order
   would let an unwind race against a leg that fills again a moment later.
5. **Flatten the excess**, selling it back into its own book two ticks below
   the touch. An unwind priced at the touch does not fill when the book is
   moving, and a half-unwound leg is worse than a slightly expensive one.

Limit prices are set at the **worst level the depth walk touched**, so a book
that has moved means we fill *less*, not *worse*. A market order on an
arbitrage leg is an offer to pay away the entire edge.

**Flatten rather than complete.** When the NO leg fails, the two repairs are
to sell the excess YES back, or to buy the missing NO at whatever it now
costs. Completing is sometimes cheaper and a production system would price
both. Flattening is chosen because its cost is bounded by the spread we can
see, whereas completing is unbounded exactly when the leg failed *because* the
depth disappeared — and an unwind path is the last place to put an unbounded
cost.

If the excess cannot be flattened at all (no bid, no book, or the unwind
itself only partly fills), the remainder is recorded as `naked_size` rather
than quietly dropped. The position exists either way; this is what tells an
operator about it.

---

## Risk

`engine/portfolio.py` defines the `RiskManager` protocol both sides agreed on:

```python
check_order(order) -> RiskDecision     # before dispatch, every order
record_fill(fill)  -> None             # after every fill
```

The pipeline runs on `PortfolioManager` (`engine/portfolio_manager.py`): a
thread-safe singleton shared by every strategy, with per-market, per-venue and
total limits and reservations that block over-allocation across exchanges —
see [RISK_AND_PERSISTENCE.md](RISK_AND_PERSISTENCE.md).
`SimplePortfolioManager` remains the reference implementation the arbitrage
unit tests use.
`check_order` may be sync or async — the strategy awaits an awaitable result —
so it can grow a database call without the strategy changing.

`record_fill` is driven by the **executor's fill listener**, not by the
strategy. A resting order can fill long after the strategy has moved on, and
an unwind path that throws would skip whatever bookkeeping came after it;
wiring the portfolio directly to the fill stream means position state cannot
silently diverge from what the venues actually did.

An unwind order is allowed past the position limit by default: it *reduces*
exposure, and blocking it would strand a naked leg — the exact risk the limit
exists to prevent.

---

## Sentiment transport

Agreed with Person A, the news/sentiment producer:

```
Redis stream   sentiment:{pair_id}
fields         score     float in [-1, 1], signed; positive favours YES
               headline  str, optional
               source    str, optional
               ts        unix seconds, optional (defaults to now)
```

A stream, not a plain key, because the threshold needs recent *history* to
compute a standard deviation — a key holding only the latest value cannot
answer "how much has this moved".

The consumer starts its cursor at `$` (entries from now on): a restart must
not widen today's threshold with yesterday's news. A malformed entry is logged
and dropped, and a Redis outage degrades to the base threshold, because the
producer is a separate process and one bad message must not take the trading
loop down.

`now_fn` is injectable. During a replay the headlines carry the tape's
historical timestamps, so a window measured against wall-clock time contains
none of them and every statistic silently reads zero. The pipeline points it
at the replay clock.

---

## Persistence

`engine/persistence.py` is the sink interface that README feature 6
(PostgreSQL via SQLAlchemy/asyncpg) plugs into, plus a SQLite implementation
so the pipeline has a real, queryable database today without adding a service
dependency.

SQLite rather than JSON lines because the questions worth asking afterwards
are aggregate — how often did an edge exist, what did the threshold decline,
what did unwinds cost — and those are one `SELECT` in a database and a script
over a log file. Tables: `arb_opportunities`, `arb_attempts`, `fills`. The
schema is the shape a Postgres version would use, so swapping the driver is a
rewrite of that one file.

Decimals are stored as `TEXT`, not `REAL`. A float column would silently undo
the entire reason `models.py` uses `Decimal`.

Feature 6 itself is `engine/db/`: `AsyncDbEventSink` on PostgreSQL through
SQLAlchemy's async engine and asyncpg (`--db-url`). It adds the trade log, book
snapshots, spread history and sentiment features, keeps these tables' columns,
and stores Decimals as exact `NUMERIC`. Recording is a non-blocking enqueue; a
background task writes in batches. Schema and design:
[RISK_AND_PERSISTENCE.md](RISK_AND_PERSISTENCE.md).

---

## The pipeline, and the seam for the AMM

`engine/pipeline.py` owns every component and the wiring. Two modes share
everything except the frame source:

- **live** — both WebSocket clients, Redis sentiment, mock execution against
  the live books.
- **replay** — two recorded tapes merged on local receive time, scripted
  headlines, the same mock execution.

Replay is the mode that matters for review. Everything that could make a run
non-deterministic is pinned: executor RNGs are seeded, the Kalshi fee
multiplier can be fixed so no network lookup is needed, and sentiment arrives
at a stated frame index rather than whenever a news API felt like it.

The strategy list is open. `ArbitrageStrategy` is registered by default;
anything implementing `BaseStrategy` registers through `add_strategy()` and
gets the same tick, the same `OrderExecutorFactory`, the same
`PortfolioManager` and the same sentiment feed. **This is the seam an AMM
quoter would plug into** — `test_a_high_sentiment_headline_skews_a_registered_quoting_strategy`
drives a minimal stand-in through it to prove the contract holds.

After the strategies run on each tick, the pipeline releases risk reservations
held by orders that are no longer open.

`on_tick` is called after any book change, not on a fixed schedule, so a
strategy must be cheap when there is nothing to do and must not assume a
regular interval. A strategy that raises is logged and contained: one
strategy failing must not stop the others or kill the ingestion tasks it
shares a loop with.

---

## What the recorded tapes actually show

Both tapes were recorded simultaneously on 2026-10-08 from 06:28 UTC, about
an hour, 2 569 frames merged. For the whole hour:

```
Kalshi      0.4100 / 0.4200          →  NO ask = 1 − 0.41 = 0.59
Polymarket  0.37   / 0.38            →  YES ask = 0.38
                                        0.38 + 0.59 = 0.97  →  3c/pair
```

A persistent 3c/pair dislocation on an illiquid geopolitical market. That is
why the opportunity count in a replay is high — the edge really was there all
hour, not because the detector is generous. Trading it with a 1 000-contract
position limit matches 1 000 pairs:

```
gross   1000 × 3c                   = $30.00
fees    Kalshi quadratic near 0.41  = $18.00
net                                 = $12.00
```

`test_the_locked_profit_equals_the_gross_edge_minus_the_fees_paid` pins that
identity rather than just the total, so a fee bug cannot hide inside a
plausible-looking number.

---

## What is deliberately not modelled

- **Settlement and capital.** The legs settle on different rails on different
  timelines, so a matched pair is flat in exposure terms but not in capital
  terms. A positive net edge is gross of carry.
- **USDC basis.** The Polymarket leg settles in USDC; any USDC/USD basis is
  ignored.
- **Resolution risk.** Everything assumes the two contracts resolve
  identically. That is what `Polymarket_engine/config/pair.toml`'s
  `criteria_match` exists to confirm, and it is **still `false`** for the test
  pair — one open question about whether a caretaker period after an election
  counts as still in office. Every number in a run is provisional until that
  is settled, and the runner prints a warning saying so.
- **Re-entry pacing.** The strategy re-evaluates on every book change and will
  keep adding to a position while an edge persists. The position limit is the
  only control; a production system would add a cooldown and an in-flight
  guard.
- **Maker quoting.** Every leg here takes liquidity. Resting an arbitrage leg
  is cheaper on fees and strictly riskier, and is not attempted.
