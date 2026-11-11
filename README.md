# Prediction-Market-Arbitrage-Automated-Market-Making-AMM-Engine

Required Features:
1.	Dual-Venue Market Data Ingestion: Asynchronous WebSocket handlers for raw exchange order streams.
2.	L2 Order Book Reconstruction: Single-threaded, in-memory order book engines maintaining real-time bid/ask depth.
3.	Pair Mapping & Manual Verification: Configuration pipeline to map and verify equivalent contracts across platforms.
4.	Subscription Lifecycle Monitor: Resilience manager that handles connection drops, heartbeat checks, and auto-resubscriptions.
5.	Executable Spread & Fee Engine: Calculates net spreads incorporating non-linear taker fee schedules and depth constraints.
6.	Event & Snapshot Persistence: PostgreSQL integration via SQLAlchemy/asyncpg for logging arbitrage events and book snapshots.

Milestone 1 status: Features 1, 2, 4, 5 are done. Feature 3 is partially done. Feature 6 is open.

Final status: Feature 6 is implemented (`engine/db/`, see below). Feature 3 is still
partially done: the pair config and its verification record exist, but
`criteria_match` is still `false` for the one configured pair.

Milestone 2 adds the trading side on top of that ingestion layer:

7. **Execution layer:** `BaseExecutor` plus mock Kalshi and Polymarket venues that
   fill against the live order book with latency, partial fills and rejections,
   routed by an `OrderExecutorFactory`.
8. **Arbitrage strategy:** complementary-pair detection (buy YES on one venue, NO on
   the other, for a certain $1.00 payout), sized by walking real depth, priced through
   both venues' non-linear fee schedules, and gated by a sentiment-driven threshold.
9. **Risk seam:** the `RiskManager` protocol (`check_order` / `record_fill`) and a
   reference `SimplePortfolioManager`.
10. **End-to-end pipeline:** both feeds, sentiment, strategies, mock execution and a
    SQLite event log, runnable live or as a repeatable replay over recorded tapes.

Market making, risk and persistence:

11. **PortfolioManager** (`engine/portfolio_manager.py`): a thread-safe singleton
    holding one book of positions for every strategy and venue — YES/NO inventory,
    average cost, realized and unrealized P&L — behind `check_order`, `record_fill`
    and `get_inventory`. Limits per market, per venue and in total, with
    reservations so two strategies or two venues can never be approved against the
    same room.
12. **AMMStrategy** (`engine/amm_strategy.py`): Avellaneda-Stoikov quotes,
    `r = s − q·γ·σ²·(T−t) + k·S·confidence` and
    `δ = γσ²(T−t) + (2/γ)·ln(1+γ/κ)`, skewed by inventory and by the shared
    sentiment feed, clamped to 0.01–0.99 on the tick grid, cancelled and replaced
    only when the reservation price moves, and stopped near expiry or when the risk
    limits are hit.
13. **Feature 6, persistence** (`engine/db/`): PostgreSQL via SQLAlchemy's async
    engine and asyncpg. The tables are a trade log, book snapshots, spread history,
    sentiment features, AMM quotes and the arbitrage events. Writes are batched in
    the background, so logging never blocks the strategy loop.

See **[docs/MARKET_MAKING_RISK_PERSISTENCE.md](docs/MARKET_MAKING_RISK_PERSISTENCE.md)**
for the AMM maths, the risk limits and the database schema.

## Architecture

See **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** for the component map, the
arbitrage math, and the design decisions behind the execution and unwind paths.

The short version — three layers, with market data flowing up and orders flowing down:

| Folder | What it owns |
|---|---|
| `kalshi_engine/` | Kalshi WebSocket feed, L2 book, quadratic fee schedule, depth-walking spread engine |
| `Polymarket_engine/` | Polymarket CLOB feed, one L2 book per outcome token, fee schedule |
| `engine/` | Venue-neutral trading layer: executors, arbitrage strategy, AMM, PortfolioManager, sentiment, persistence (`engine/db/`), pipeline |

`kalshi_engine/models.py` holds the shared **market data** contracts;
`engine/models.py` holds the **order** contracts. The ingestion engines have no
dependency on the trading side.

## Setup

Python 3.12 or newer (the code uses `asyncio.TaskGroup` and `BaseExceptionGroup`).

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt      # both venue engines + SQLAlchemy, asyncpg, aiosqlite, redis
```

Everything offline — the replay pipeline and every test in `tests/` — runs with just
that; the database tests use SQLite through the same schema. Optional extras:

| You want | You need |
|---|---|
| PostgreSQL persistence (feature 6) | `docker compose up -d postgres`, then `--db-url postgresql+asyncpg://arb:arb@localhost:5432/arb` (or set `DATABASE_URL`) |
| Live Kalshi data | `kalshi_engine/.env` with `KALSHI_ENV=prod`, a production Key ID and a private key path (see `kalshi_engine/.env.example`). Polymarket's public feed needs no credentials. |
| Live sentiment from Redis | `docker compose up -d redis`, and a producer writing to the `sentiment:{pair_id}` stream. Without it the pipeline uses an in-memory feed and the threshold stays at its base value. |

## How to use it:

run run_pair.py

| Option | What it does | Example |
|---|---|---|
| `--size N` | Number of contracts per leg used to compute the spread (default: 100) | `--size 500` |
| `--record` | Also records both venues' raw WebSocket messages while running | `--record` |
| `--kalshi-ticker T` | Uses a different Kalshi market for this run, without editing `pair.toml` | `--kalshi-ticker KXLEADERSOUT-27JAN01-BNETISR` |
| `--pair-config PATH` | Uses a different pair configuration file | `--pair-config Polymarket_engine/config/pair.toml` |

| What you see | Meaning | Action needed? |
|---|---|---|
| `pair ...`, `fees: ...` | Startup info: the pair and both venues' fee settings | No |
| `INFO ... connected` / `Connected to wss://api.elections.kalshi.com...` | Both venues are connected | No |
| `waiting for a fresh Kalshi book` (or similar) | One side's order book isn't ready yet (just connected, or disconnected), so the spread calculation is paused | Usually recovers on its own within seconds |
| `06:28:14  Kalshi 0.41/0.42  Poly 0.37/0.38 \| buy ... net ...` | Main result: best prices on both venues and the net edge in both directions. Printed only when something changes | Read the result |
| `PARTIAL` | One side doesn't have enough depth to fill the requested size | Try a smaller `--size` |
| `no change (N evaluations so far)` | Nothing changed for 30 seconds; the program is still running | No |
| `Disconnected ... reconnecting in ...` / `WARNING ... disconnected` | Connection dropped; reconnecting automatically | Usually no; it recovers on its own |
| `WARNING ... check ... failed` | A Polymarket order book consistency check failed | Ignore an occasional one; investigate if frequent |
| `stopped: ... Kalshi rejected the credentials` | The Kalshi key was rejected and the program stopped | Check the Key ID and private key path in `.env` |

| What you want to do | Command |
|---|---|
| Replay the tapes and get the same results as the live run | `python run_pair.py --replay <POLYMARKET_TAPE> <KALSHI_TAPE>` |
| Inspect a Polymarket tape message by message | `python -m polymarket.inspect_tape <TAPE>` (run inside the `Polymarket_engine` folder) |
| Add the tapes to the repo as test data | `git add` only the two `.jsonl` files; before committing, run `git status` to make sure `.env` isn't included |

## Running the arbitrage pipeline

`engine/pipeline.py` is the Milestone 2 entry point: both feeds, sentiment,
strategies, mock execution and the event log, in one process.

```bash
# replay both recorded tapes through the full stack (offline, no credentials)
python -m engine.pipeline \
  --replay Polymarket_engine/fixtures/tape_polymarket_netanyahu-out-2026_20261008T062810Z.jsonl \
           kalshi_engine/fixtures/tape_kalshi_KXLEADERSOUT-27JAN01-BNETISR_20261008T062814Z.jsonl \
  --kalshi-fee-multiplier 1

# price the edge without trading it
python -m engine.pipeline --replay <PM_TAPE> <KS_TAPE> --kalshi-fee-multiplier 1 --dry-run

# inject scored headlines and watch the threshold widen
python -m engine.pipeline --replay <PM_TAPE> <KS_TAPE> --kalshi-fee-multiplier 1 \
  --headline "800:0.95:Netanyahu to resign - wire" \
  --headline "805:-0.90:Office denies resignation"

# add AMM quoters on both venues and log everything to PostgreSQL
docker compose up -d postgres
python -m engine.pipeline --replay <PM_TAPE> <KS_TAPE> --kalshi-fee-multiplier 1 \
  --amm kalshi --amm polymarket \
  --headline "1200:0.90:Netanyahu expected to step down" \
  --db-url postgresql+asyncpg://arb:arb@localhost:5432/arb

# live, both venues, stopping after a minute
python -m engine.pipeline --live --max-seconds 60 --db-url postgresql+asyncpg://arb:arb@localhost:5432/arb
```

| Option | What it does |
|---|---|
| `--replay PM_TAPE KS_TAPE` | Offline: merge both tapes on receive time and drive the whole pipeline |
| `--live` | Both WebSocket clients (needs `kalshi_engine/.env`) |
| `--size N` | Pairs per attempt, before depth and risk shrink it (default 100) |
| `--dry-run` | Price and log every opportunity, dispatch nothing |
| `--kalshi-fee-multiplier M` | Skip the fee-schedule lookup, making a replay network-free. Use `1` for the recorded tapes. |
| `--base-threshold D` / `--lam L` | `ΔP_min = base + λ · sentiment_volatility` (defaults 0.005 and 0.05) |
| `--headline FRAMES:SCORE:TEXT` | Replay only: inject a scored headline after N frames. Repeatable. |
| `--amm VENUE` | Run an AMM quoter on `kalshi` or `polymarket` (repeatable) |
| `--amm-gamma G` / `--amm-kappa K` / `--amm-k K` / `--amm-size N` | AMM risk aversion, order-arrival decay, sentiment drift, quote size (defaults 2, 100, 0.02, 10) |
| `--db-url URL` | Feature 6: write every event family to this SQLAlchemy async URL (default `$DATABASE_URL`) |
| `--db PATH` | Older events-only sink: opportunities, attempts and fills to a SQLite file |
| `--redis-url URL` | Live only: consume sentiment from the `sentiment:{pair_id}` stream |
| `--max-seconds S` | Live only: stop after S seconds |

What the report tells you:

| Line | Meaning |
|---|---|
| `edge evaluations` | Ticks where both books were fresh enough to price |
| `opportunities priced` | Evaluations where a profitable pair existed after depth and fees |
| `declined by dP_min` | Top of book showed an edge, but nothing cleared the threshold — the threshold doing its job |
| `declined by pre-flight` | A leg a venue would have rejected on sight, so the pair was not sent |
| `dispatched` / `vetoed by risk` | How many attempts reached the venues vs were stopped by a position limit |
| `pairs matched` | Both legs filled — the only pairs carrying the certain $1.00 payout |
| `locked profit` | $1.00 per matched pair minus everything actually paid, unwinds included |
| `naked after unwind` | Excess that could **not** be flattened. Should be 0; anything else needs a look. |
| `persisted` | Rows written per table; `(N dropped)` appears only if the database fell behind or failed |
| `AMM` | Per quoter: requotes, cancels, risk vetoes, and the last quote with its mid and inventory |

> The pipeline prints a warning on every run because
> `Polymarket_engine/config/pair.toml` still has `criteria_match = false`. Until both
> rule texts are confirmed equivalent, every edge it reports is provisional.

## Tests

```bash
# cross-venue suite: executors, arbitrage math, strategy, replay integration
python -m pytest tests                  # 194 tests (1 skipped without PostgreSQL), ~2.5 min; the replays dominate
python -m pytest tests -m "not slow"    # 172 tests, ~10s, no tape replays

# the per-venue suites, run from their own folders
cd kalshi_engine     && python -m pytest tests   # 33 tests; reaches the live Kalshi API
cd Polymarket_engine && python -m pytest tests
```

| File | Covers |
|---|---|
| `tests/test_arbitrage_math.py` | Pair arithmetic on hand-calculated fixtures, depth-walk edge cases, the quadratic fee curve, threshold and slippage, and a cross-check that the complementary-pair engine agrees exactly with `cross_venue_spread.py` |
| `tests/test_executors.py` | Depth walking, limit truncation, partial fills, every rejection reason, latency bands, seeded determinism, cancellation races, fill listeners, factory routing |
| `tests/test_arbitrage_strategy.py` | Paired dispatch, risk veto and shrink, pre-flight, and every leg-mismatch path including naked-position reporting |
| `tests/test_sentiment_threshold.py` | Threshold formula and cap, rolling-window statistics, Redis stream parsing and failure handling |
| `tests/test_pipeline_replay.py` | The whole stack over the recorded tapes, plus both proposal use cases |
| `tests/test_pair_replay.py` | Milestone 1's recorded-tape test for the same-outcome spread |
| `tests/test_portfolio_manager.py` | Singleton under 32 racing threads, concurrent fills and risk checks, average cost and P&L, inventory across Polymarket's two tokens, every limit, reservations |
| `tests/test_amm_strategy.py` | The formulas by hand, inventory and sentiment skew direction, clamping and post-only, volatility and confidence, cancel/replace, risk vetoes, expiry, resting fills |
| `tests/test_persistence_db.py` | Every table and row builder, exact Decimal round trip, batching, non-blocking under a slow or failing database; a PostgreSQL round trip when `TEST_DATABASE_URL` is set |
| `tests/test_amm_pipeline.py` | The real AMM on the tapes: quote skew on a headline and its decay, every event family persisted, AMM and arbitrage sharing one set of limits |

Everything in `tests/` is offline: no network, no credentials, no Redis. The one
PostgreSQL test is skipped unless `TEST_DATABASE_URL` points at a server:

```bash
docker compose up -d postgres
TEST_DATABASE_URL=postgresql+asyncpg://arb:arb@localhost:5432/arb python -m pytest tests -m postgres
```

## AI Use

### Junyu:
**Main types of tasks**
- **Scaffolding:** the structure of `Polymarket_engine`  and the combined entry point `run_pair.py`.
- **API integration:** Polymarket's Gamma API and CLOB WebSocket, connecting my feed to
  the shared `models.py` / `OrderBook`, and wiring both venues' streams into the spread engine.
- **Generating tests:** unit tests, the synthetic-fixture runner, fee tests based on
  Polymarket's published fee table, and the replay tests on our recorded tapes.
- **Debugging and explanations:** reading teammate code and diagnosing
  problems found on real data.

**Examples of substantial AI-assisted parts**
1. **Polymarket ingestion and L2 book (`Polymarket_engine/polymarket/`):** the AI drafted
   the WebSocket client, parser, adapter and consistency checks. 
2. **Combined entry point (`run_pair.py`) and replay tests (`tests/test_pair_replay.py`):**
   runs the Kalshi and Polymarket streams together and recomputes the edge on every change,
   with a replay mode for the simultaneously recorded tapes.

**How I reviewed, tested and modified the code**
- Made the design decisions myself: 
  e.g. using live WebSocket streams instead of REST snapshots for the demo, and importing the shared
  files instead of copying them.
- write synthetic test cases 
- Modified AI code where it didn't fit: I changed `taker_fee` to `(shares, price, fees)`
  to match the Kalshi fee functions. 

### Joshua:
**Main types of tasks**
- **Debugging against the live API:** diagnosing a 401 that blocked all Kalshi
  access, and finding where our assumptions about Kalshi's wire format had gone
  stale.
- **Completing and hardening the Kalshi ingestion** (`kalshi_engine/kalshi_ws.py`):
  the snapshot/delta handler, sequence-gap detection and resync, and the
  reconnect/backoff layer.
- **Fee and spread engine** (`kalshi_fees.py`, `executable_spread.py`,
  `cross_venue_spread.py`): Kalshi's quadratic fee schedule and the depth-walking
  logic that turns two order books into a net executable edge.
- **Generating tests:** the 33 offline unit tests covering the fee curve, depth
  walking, cross-venue edge calculation, callback hooks and tape replay.

**Examples of substantial AI-assisted parts**
1. **Kalshi order book ingestion (`kalshi_engine/kalshi_ws.py`, `kalshi_auth.py`):**
   the AI drafted the Ed25519/RSA request signing, the fixed-point snapshot/delta
   parsing, and the reconnect/resync lifecycle. Several real defects surfaced only
   because it tested against the live exchange rather than reasoning from the docs:
   `delta_fp` is an *incremental* change while our `OrderBookDelta.size` is an
   absolute size, `seq` lives on the message envelope rather than inside `msg`, and
   our `get_snapshot` resync command was silently rejected by Kalshi with
   `{"code": 14, "msg": "Market Ticker required"}` — which would have frozen the
   book permanently on any real sequence gap.
2. **Fee schedule and executable spread (`kalshi_fees.py`, `executable_spread.py`):**
   rather than hardcoding Kalshi's 0.07 taker rate, the AI surveyed ~9200 live
   series and found four distinct fee configurations, including nine series that
   charge nothing at all. The fee schedule is now fetched per series. Fees are
   charged per fill level rather than on a blended average price, because the fee
   is quadratic in price and the two genuinely differ.

**How I reviewed, tested and modified the code**
- Made the design decisions myself: getting production Kalshi API credentials
  instead of staying on the demo environment (demo has zero liquidity, so no
  delta traffic exists there to test against), and choosing what to build in what
  order against the milestone spec rather than building everything suggested.
- Insisted on validation against live data instead of only synthetic fixtures.
  Two checks I consider the real evidence the book is correct: after 528 live
  deltas the delta-accumulated book matched an in-band snapshot exactly across
  279 price levels, and a 15,142-frame recorded tape replays into a byte-identical
  book (130 bid / 149 ask levels, matching sequence numbers).
- Modified AI code where it didn't fit: the first version of the gap-resync set
  its `_resyncing` flag inside a fire-and-forget task, which left a window where
  stale deltas were still applied; it now sets the flag synchronously before
  awaiting. I also kept `book_is_fresh` as a public gate at Junyu's request so the
  combined runner can tell when the Kalshi book is safe to read.
- Agreed with Junyu's review that the demo should use live WebSocket streams
  rather than the one-shot REST sampling I had used to prove the fee/depth maths,
  and added the `on_raw` / `on_update` hooks his combined entry point needed.

#### Milestone 2 (arbitrage, execution and integration)

**Main types of tasks**
- **Scaffolding:** the `engine/` layer — the executor interface and mock venues,
  the strategy base class, the risk seam, and the pipeline that wires both
  venues' feeds to it.
- **Arbitrage math:** the complementary-pair depth walk, the per-chunk fee and
  slippage model, and the sentiment-driven threshold.
- **Generating tests:** the 123 offline tests in `tests/`, including the
  hand-calculated arbitrage fixtures and the recorded-tape integration run.
- **Debugging:** three defects that only surfaced by running the pipeline on the
  real tapes rather than on fixtures (below).

**Examples of substantial AI-assisted parts**
1. **Execution layer (`engine/executors/`):** the AI drafted the fill simulation
   that walks the live book honouring the limit price, the latency/partial-fill/
   rejection model, and the factory. The two choices worth calling out are both
   about *not* being a stub: an unfilled remainder RESTS rather than being
   cancelled, which is the only reason the leg-mismatch code gets exercised at
   all; and a fill does NOT remove the depth it consumed, because the feed owns
   the book and writing our fills into it would break the consistency checks
   `PolymarketFeed` runs against the exchange's own top-of-book hints.
2. **Arbitrage engine and strategy (`engine/arbitrage.py`,
   `engine/arbitrage_strategy.py`):** pairing contracts level by level, testing
   each chunk on its own marginal profit rather than an average, and the
   cancel-then-flatten unwind path.

**How I reviewed, tested and modified the code**
- **Checked the premise before trusting the module.** On a binary CLOB a NO ask
  is the mirror of a YES bid, so buying YES on A and NO on B is *the same trade*
  as buying YES on A and selling YES on B — meaning this strategy is a
  reformulation of the Milestone 1 same-outcome spread, not an independent one.
  Rather than leave that unsaid, I turned it into a cross-check:
  `test_complementary_pair_agrees_with_the_same_outcome_engine` asserts my
  depth-pairing walk and `cross_venue_spread.py`'s separate-leg walk produce
  identical net edges and identical fees. Two independently written code paths
  agreeing across multi-level depth and a quadratic fee schedule is the
  strongest evidence available that either is right.
- **Validated a design claim against real data instead of the docs.** The AI's
  first draft synthesized a NO book for *both* venues. That is right for Kalshi
  (its feed gives one book) and wrong for Polymarket (its feed gives two, and a
  NO ask and the mirroring YES bid are the same resting order, so merging would
  double-count the liquidity and report twice the executable size). I checked
  the mirror property on the recorded tape before relying on it: it held on all
  2 567 ticks where both books were valid, and that check is now a test.
- **Found three real defects by running the pipeline, not the fixtures.**
  (a) The strategy would dispatch a leg a venue was *certain* to reject — an
  off-tick price, or a size under Polymarket's 5-share minimum after the risk
  layer shrank it — filling one leg and then unwinding at a guaranteed loss. I
  added `BaseExecutor.preflight()` so a doomed leg aborts the pair instead; in
  the fixture that exposed it, that turned a $5.00 unwind loss into no trade.
  (b) The rolling sentiment window was measured against wall-clock time, so
  headlines injected into a replay (which carry the tape's historical
  timestamps) were always outside the window and every statistic read zero —
  the threshold use case silently passed through doing nothing. `now_fn` is now
  injectable and the pipeline points it at the replay clock.
  (c) A `set()` declared as a class attribute on the pipeline was shared by
  every instance, so a second pipeline in the same process would skip persisting
  attempts the first had seen.
- **Made the design decisions myself.** Unwinding flattens the excess rather
  than completing the pair: completing is sometimes cheaper, but its cost is
  unbounded exactly when the leg failed *because* the depth disappeared, and an
  unwind path is the last place to put an unbounded cost. A risk veto on either
  leg aborts both, because sending only the approved leg leaves precisely the
  naked position the check just refused. `record_fill` is driven by the
  executor's fill listener rather than by the strategy, so a resting order that
  fills after the strategy has moved on still reaches the portfolio.
- **Corrected the reporting when it flattered the result.** The first pipeline
  report called every `ArbitrageAttempt` a "dispatch attempt", showing 2 563 of
  them on an hour of tape when only 200 reached a venue and 2 363 were stopped
  by the position limit. Dispatched and vetoed are now counted separately.
