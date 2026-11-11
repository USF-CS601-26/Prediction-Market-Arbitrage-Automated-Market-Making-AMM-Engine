# Market making, risk and persistence

This document covers the three modules that sit beside the arbitrage strategy:

| Module | File | What it owns |
|---|---|---|
| PortfolioManager | `engine/portfolio_manager.py` | One book of positions for every strategy and venue; the risk gate on every order |
| AMMStrategy | `engine/amm_strategy.py` | Inventory- and sentiment-aware two-sided quotes on one venue's market |
| Persistence | `engine/db/` | Feature 6: PostgreSQL via SQLAlchemy (async) + asyncpg, batched so logging never blocks |

[ARCHITECTURE.md](ARCHITECTURE.md) has the system-level picture. This file covers the maths and the contracts.

---

## 1. The AMM

### The two formulas

```
reservation   r     = s − q·γ·σ²·(T−t) + k·S·confidence
spread        δ     = γ·σ²·(T−t) + (2/γ)·ln(1 + γ/κ)
quotes        bid   = r − δ/2        ask = r + δ/2
```

| Symbol | Meaning | Source | Default |
|---|---|---|---|
| `s` | mid of the YES book | the venue's L2 book | — |
| `q` | net YES-equivalent inventory ÷ `inventory_unit` | `PortfolioManager.get_inventory(market).net` (YES − NO), across venues | unit = 10 contracts |
| `γ` | risk aversion | config | 2.0 |
| `σ` | realized volatility of the mid, per √day | `MidVolatility` (below) | floor 0.02, cap 0.50 |
| `T−t` | days to expiry, capped at the horizon | pair config `end_date` | cap 1 day |
| `κ` | order-arrival decay per $ of distance from the mid | config | 100 |
| `S` | signed sentiment level (mean score in the window) | shared sentiment feed | — |
| `confidence` | how far to trust `S` (below) | derived from the same window | — |
| `k` | $ of shift at `S·confidence = 1` | config | 0.02 |

The `(2/γ)·ln(1+γ/κ)` term is the spread that maximizes expected fill profit with zero inventory. With the defaults it is `ln(1.02) ≈ 1.98¢`. The `γσ²(T−t)` term is the extra width the market maker charges for carrying risk until the horizon.

### Which way each term pushes

| Situation | Effect on `r` | Why |
|---|---|---|
| Long inventory (`q > 0`) | down | The bid backs off and the ask gets aggressive, so the AMM sells down what it holds |
| Short inventory | up | It buys back |
| Positive sentiment | up | It pays more for YES ahead of the repricing instead of being picked off by it |
| More volatility or longer horizon | wider `δ`, stronger inventory push | More risk carried per contract |

These directions are each pinned by a test in `tests/test_amm_strategy.py`.

### Whose inventory `q` is

By default (`inventory_scope="event"`), `q` is the net YES-equivalent position across every venue trading the event. The arbitrage strategy's Polymarket YES + Kalshi NO pair nets to zero there, so the AMM does not lean to unwind one leg of a hedge.

The first version used each venue's leg alone. On the tapes the Kalshi AMM then saw −990 and the Polymarket AMM +990, and both leaned about 8¢ toward unwinding the arbitrage. `inventory_scope="venue"` keeps that behaviour available. The risk limits stay per venue either way.

### Units and the horizon cap

Prices are dollars in (0, 1) and time is in days. If `T−t` were the contract's real maturity (months), `σ²(T−t)` would exceed every price in the book and the AMM could never quote inside 0.01–0.99. The usual practical reading is that `T−t` is the horizon over which the market maker must be flat, so it is `min(time to expiry, horizon_cap_days)`. As expiry approaches, it shrinks with the real time left and the inventory term fades. Inside `stop_before_expiry` (1 hour by default) the AMM cancels everything and stops quoting. A binary contract about to resolve jumps to 0 or 1, which is not a diffusion the formula can price.

`end_date` comes from the pair config's `[polymarket]` section; `python -m polymarket.gamma <slug>` prints it.

### σ: realized volatility of the mid

`MidVolatility` keeps `(time, mid)` points over a rolling window (1 hour) and computes

```
σ² per day = Σ (Δmid)² / (elapsed time in days)
```

It is clamped to `[sigma_floor, sigma_cap]`. A book that has not moved, or a window shorter than a minute, reads as the floor: a quiet book is not a riskless one.

### Sentiment and confidence

The sentiment transport is the one in `engine/sentiment.py`: a Redis stream `sentiment:{pair_id}` with fields `score` (signed, in [-1, 1]), `headline`, `source` and `ts`. It has **no confidence field**, so confidence is derived from the rolling window the feed already keeps:

```
confidence = agreement × evidence
agreement  = 1 − stdev(scores in the window)
evidence   = min(1, n / min_points_for_confidence)      (min_points defaults to 1)
```

Headlines that agree move the quotes. A window of contradictory headlines averages to a small `S` and also scores low agreement, so it barely skews at all; the arbitrage threshold is what widens on that.

### From a quote to orders

A prediction market has no "sell what you do not hold" on every venue, so each side is sent as an order the venue can take:

| Side | Order |
|---|---|
| bid at `p` | BUY YES at `p`, or SELL NO at `1−p` if NO is held |
| ask at `p` | SELL YES at `p` if YES is held, otherwise BUY NO at `1−p` |

Buying NO at `1−p` is the same trade as offering YES at `p`. On Polymarket the NO leg uses the NO token; on Kalshi the mock venue fills it against the complement of the YES book.

Before sending, the prices go through these steps:

1. **Tick alignment, outward.** The bid rounds down and the ask rounds up, so the AMM never quotes tighter than the model asked.
2. **Bounds.** Prices are clamped to [0.01, 0.99] and the bid is kept below the ask.
3. **Post-only.** A bid that would cross the best ask is pulled back one tick behind it, and vice versa. A crossing quote would be a taker order.
4. **Risk.** `PortfolioManager.check_order` can shrink a side or veto it. A vetoed side is not quoted; if both are vetoed, the AMM quotes nothing.
5. **Venue pre-flight.** The venue's static rules are checked: minimum size, tick, token id.

### Cancel and replace

Quotes are replaced only when one of these happens:
- the reservation price has moved by `requote_threshold` (0.5¢) since the live quotes were placed;
- a side has gone (filled, or cancelled or rejected by the venue);
- the book became unusable: stale or one-sided, which cancels everything.

Re-sending identical quotes on every book update would only burn the venue's rate limit. Cancelling a quote releases its risk reservation immediately.

### Fills against resting quotes

The mock venues used to fill an order only at the moment it was submitted. `SimulatedExecutor.match_resting()` now re-matches open orders on every book change, before the strategies tick. A resting BUY at `p` fills once the book offers at or below `p` (a "trade-through" fill model for L2 data). The arbitrage strategy is unaffected, because it never leaves orders resting.

---

## 2. Risk limits: the PortfolioManager

### Contract

```python
check_order(order) -> RiskDecision      # approved / rejected(reason), optional max_size to shrink
record_fill(fill)  -> None              # wired as every executor's fill listener
get_inventory(market, venue=None) -> Inventory   # yes, no, net, avg costs, realized/unrealized P&L, exposure
```

`check_order` honours the `RiskManager` protocol in `engine/portfolio.py`, so `ArbitrageStrategy` uses it unchanged. `SimplePortfolioManager` stays in that file as the reference implementation the arbitrage unit tests were written against; the pipeline now runs on `PortfolioManager`.

### Singleton

`PortfolioManager()` always returns the same instance. Creation is double-checked under a lock in `__new__`, so threads racing to construct it get one object (tested with 32 threads released together). `PortfolioManager.instance()` and `get_portfolio_manager()` are accessors for the same object. `reset(limits, cash)` re-initializes it; the pipeline calls it at start-up and tests call it in setup. Every public method takes the instance's `RLock`.

**Why it is a singleton:** if the arbitrage strategy and the AMM each had their own, each would see room the other had already used.

### Positions and P&L

Positions are kept per `(venue, market_id, outcome)` with signed quantity and average cost:
- **Average cost** excludes fees. Adding to a position moves it toward the fill price; selling through zero opens the other way at the fill price.
- **Realized P&L** is `closed quantity × (exit − average cost)`. Fees are charged to it when paid.
- **Unrealized P&L** is `quantity × (mark − average cost)`. The pipeline marks every position at its book's mid on each tick.

`register_market(venue, market, {"YES": id, "NO": id})` groups ids into one logical market. That matters on Polymarket, where YES and NO are separate tokens. The pipeline registers both venues under the pair id.

### Exposure is worst-case loss

For one binary market on one venue, holding `Y` YES and `N` NO that cost `C`:

```
exposure = max(0, C − min(Y, N))
```

- A plain long position's exposure is its cost.
- Buying the other outcome against it *reduces* exposure, because a YES+NO pair is a certain $1. That is exactly how the AMM flattens.
- Shorts are converted to their long equivalent: short q YES at p is the same as long q NO at 1−p.
- Positions on different venues are **not** netted: they settle on different rails at different times. This overstates the risk of a hedged arbitrage, which is the safe direction.

### Limits

| Limit (`RiskLimits`) | Measured as | Default |
|---|---|---|
| `max_position_per_market` | \|net YES-equivalent\| per venue market | 1000 contracts |
| `max_notional_per_order` | size × limit price, BUY orders | $1,000 |
| `max_exposure_per_market` | worst-case loss per venue market | $2,500 |
| `max_exposure_per_venue` | summed over that venue's markets | $5,000 |
| `max_total_exposure` | summed over every venue | $8,000 |
| cash | one shared pool, or `{"kalshi": …, "polymarket": …}` | $10,000 shared |

Outcomes:
- **Breached limit:** the order is shrunk to the largest size that fits (found by bisection; every limit is convex in size), or vetoed with the binding reason.
- **Risk-reducing order:** if an order reduces the net position or the exposure without increasing the other, it passes the position and exposure limits. Only the per-order notional and cash still apply. Blocking it would strand the risk the limits exist to prevent.
- **Unwind:** always approved.

### Reservations: how over-allocation is blocked

An approved order reserves what it could consume (its size at its limit price) until one of these happens:
- it fills (each fill uses up its share of the reservation);
- it is released (`release(order_id)`, which the AMM calls on cancel);
- `reconcile_reservations(open_order_ids)` finds it is no longer open at any venue. The pipeline calls this after every tick.

Every later `check_order`, from any strategy and for any venue, counts these reservations as if they had filled. Only the ones pushing **the same direction** as the new order count: a resting order on the other side might never fill, so the AMM's own bid must not make its ask look like a hedge.

On the recorded tapes, with a 200-contract limit, the AMM's resting 10-lot YES bid holds 10 contracts of room. The arbitrage strategy stops at 190, and position plus reservation is exactly 200 (`tests/test_amm_pipeline.py`).

---

## 3. Persistence (feature 6)

### Running it

```bash
docker compose up -d postgres
python -m engine.pipeline --replay <PM_TAPE> <KS_TAPE> --kalshi-fee-multiplier 1 \
    --amm kalshi --amm polymarket \
    --db-url postgresql+asyncpg://arb:arb@localhost:5432/arb
```

`--db-url` defaults to `$DATABASE_URL`. The same code runs on `sqlite+aiosqlite:///file.db`, which is how the tests exercise it without a server. `--db file.db` still selects the older events-only SQLite sink.

### Never blocking the strategy loop

1. Every `record_*()` call builds its row (pure, microseconds) and does a `put_nowait` onto a bounded `asyncio.Queue`. It never awaits the database.
2. A background writer task drains the queue in batches (up to 200 rows, or whatever arrived within 0.25 s of the first) and writes each batch in one transaction, one `executemany` per table.
3. **Queue full:** the row is dropped and counted (`dropped`, shown in the pipeline report).
4. **Write fails:** the batch is retried 3 times with backoff, then dropped and counted (`failed_batches`). Nothing is raised to the caller.
5. **End of a run:** `aclose()` drains the queue and marks the run finished.

Tests pin this: 1,000 rows are recorded in under 0.25 s against a database taking 300 ms per batch, and all 1,000 are written by the time the sink closes.

### Schema

All tables are SQLAlchemy Core in `engine/db/schema.py`. Decimals are `NUMERIC` on PostgreSQL (exact, aggregatable in SQL) and text on SQLite; timestamps are `TIMESTAMPTZ`, normalized to UTC. Every event row carries `run_id`.

| Table | One row per | Key columns |
|---|---|---|
| `runs` | pipeline run | `run_id`, `mode`, `pair_id`, `started_at`, `finished_at`, `config` (JSON) |
| `trade_log` | order (its state after the attempt/requote) and fill | `kind` (order/fill), `order_id`, `venue`, `market_id`, `outcome`, `side`, `size`, `price`, `fee`, `cash_flow`, `status`, `filled_size`, `avg_fill_price`, `strategy_id`, `is_unwind` |
| `market_snapshots` | book, every `snapshot_interval` (5 s) and on each trade/requote | `venue`, `market_id`, `outcome`, `sequence`, `best_bid`, `best_ask`, `mid`, `bid_depth`, `ask_depth`, `bids`/`asks` (top 10 levels, JSON), `reason` (periodic/event) |
| `spread_history` | change in cross-venue top of book or threshold | `tops` (JSON per venue: YES/NO bid/ask), best `direction`, `top_of_book_cost`, `top_of_book_edge`, `executable_size`, `net_profit`, `net_per_pair`, `threshold`, `volatility` |
| `sentiment_features` | change in sentiment state | `level`, `volatility`, `confidence`, `n_points`, `latest_score`, `latest_headline`, `latest_source`, `threshold` |
| `amm_quotes` | AMM requote / cancel / stop | `mid`, `reservation`, `spread`, `bid`, `ask`, `inventory`, `sigma`, `tau_days`, `sentiment`, `confidence`, `orders_placed`, `orders_cancelled`, `reason` |
| `arb_opportunities` | priced opportunity | as in the SQLite sink |
| `arb_attempts` | execution attempt | as in the SQLite sink |

### Example queries

```sql
-- the AMM's skew over a run: how far sentiment and inventory pushed r off the mid
SELECT ts, venue, mid, reservation, reservation - mid AS skew, inventory, sentiment, confidence
FROM amm_quotes WHERE run_id = :run AND action = 'requote' ORDER BY ts;

-- realized cash per strategy, fees included (exact: NUMERIC)
SELECT strategy_id, SUM(cash_flow) AS cash, SUM(fee) AS fees
FROM trade_log WHERE run_id = :run AND kind = 'fill' GROUP BY strategy_id;

-- how often the edge cleared the threshold
SELECT COUNT(*) FILTER (WHERE executable_size > 0) * 1.0 / COUNT(*) FROM spread_history WHERE run_id = :run;
```
