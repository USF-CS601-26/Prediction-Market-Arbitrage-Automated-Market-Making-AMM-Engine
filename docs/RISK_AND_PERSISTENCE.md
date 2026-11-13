# Risk and persistence

This document covers the two modules that sit beside the arbitrage strategy:

| Module | File | What it owns |
|---|---|---|
| PortfolioManager | `engine/portfolio_manager.py` | One book of positions for every strategy and venue; the risk gate on every order |
| Persistence | `engine/db/` | Feature 6: PostgreSQL via SQLAlchemy (async) + asyncpg, batched so logging never blocks |

[ARCHITECTURE.md](ARCHITECTURE.md) has the system-level picture. This file covers the contracts and the schema.

---

## 1. Risk limits: the PortfolioManager

### Contract

```python
check_order(order) -> RiskDecision      # approved / rejected(reason), optional max_size to shrink
record_fill(fill)  -> None              # wired as every executor's fill listener
get_inventory(market, venue=None) -> Inventory   # yes, no, net, avg costs, realized/unrealized P&L, exposure
```

`check_order` honours the `RiskManager` protocol in `engine/portfolio.py`, so `ArbitrageStrategy` uses it unchanged. `SimplePortfolioManager` stays in that file as the reference implementation the arbitrage unit tests were written against; the pipeline now runs on `PortfolioManager`.

### Singleton

`PortfolioManager()` always returns the same instance. Creation is double-checked under a lock in `__new__`, so threads racing to construct it get one object (tested with 32 threads released together). `PortfolioManager.instance()` and `get_portfolio_manager()` are accessors for the same object. `reset(limits, cash)` re-initializes it; the pipeline calls it at start-up and tests call it in setup. Every public method takes the instance's `RLock`.

**Why it is a singleton:** if two strategies each had their own, each would see room the other had already used.

### Positions and P&L

Positions are kept per `(venue, market_id, outcome)` with signed quantity and average cost:
- **Average cost** excludes fees. Adding to a position moves it toward the fill price; selling through zero opens the other way at the fill price.
- **Realized P&L** is `closed quantity × (exit − average cost)`. Fees are charged to it when paid.
- **Unrealized P&L** is `quantity × (mark − average cost)`. The pipeline marks every position at its book's mid on each tick.

`register_market(venue, market, {"YES": id, "NO": id})` groups ids into one logical market. That matters on Polymarket, where YES and NO are separate tokens. The pipeline registers both venues under the pair id, so `get_inventory(pair_id, venue=...)` gives one venue's position and `get_inventory(pair_id)` gives the net across both. The arbitrage strategy's Polymarket YES + Kalshi NO pair nets to zero there.

### Exposure is worst-case loss

For one binary market on one venue, holding `Y` YES and `N` NO that cost `C`:

```
exposure = max(0, C − min(Y, N))
```

- A plain long position's exposure is its cost.
- Buying the other outcome against it *reduces* exposure, because a YES+NO pair is a certain $1.
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
- it is released (`release(order_id)`);
- `reconcile_reservations(open_order_ids)` finds it is no longer open at any venue. The pipeline calls this after every tick.

Every later `check_order`, from any strategy and for any venue, counts these reservations as if they had filled. Only the ones pushing **the same direction** as the new order count: a resting order on the other side might never fill, so it must not make a new order look like a hedge.

---

## 2. Persistence (feature 6)

### Running it

```bash
docker compose up -d postgres
python -m engine.pipeline --replay <PM_TAPE> <KS_TAPE> --kalshi-fee-multiplier 1 \
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
| `trade_log` | order (its state after the attempt) and fill | `kind` (order/fill), `order_id`, `venue`, `market_id`, `outcome`, `side`, `size`, `price`, `fee`, `cash_flow`, `status`, `filled_size`, `avg_fill_price`, `strategy_id`, `is_unwind` |
| `market_snapshots` | book, every `snapshot_interval` (5 s) and on each dispatched trade | `venue`, `market_id`, `outcome`, `sequence`, `best_bid`, `best_ask`, `mid`, `bid_depth`, `ask_depth`, `bids`/`asks` (top 10 levels, JSON), `reason` (periodic/event) |
| `spread_history` | change in cross-venue top of book or threshold | `tops` (JSON per venue: YES/NO bid/ask), best `direction`, `top_of_book_cost`, `top_of_book_edge`, `executable_size`, `net_profit`, `net_per_pair`, `threshold`, `volatility` |
| `sentiment_features` | change in sentiment state | `level`, `volatility`, `confidence`, `n_points`, `latest_score`, `latest_headline`, `latest_source`, `threshold` |
| `arb_opportunities` | priced opportunity | as in the SQLite sink |
| `arb_attempts` | execution attempt | as in the SQLite sink |

The sentiment stream (`sentiment:{pair_id}`) carries no confidence field, so `confidence` is derived from the rolling window: `(1 − stdev of the scores) × min(1, n / min_points)`.

### Example queries

```sql
-- realized cash per strategy, fees included (exact: NUMERIC)
SELECT strategy_id, SUM(cash_flow) AS cash, SUM(fee) AS fees
FROM trade_log WHERE run_id = :run AND kind = 'fill' GROUP BY strategy_id;

-- how often the edge cleared the threshold
SELECT COUNT(*) FILTER (WHERE executable_size > 0) * 1.0 / COUNT(*) FROM spread_history WHERE run_id = :run;

-- the book at each trade
SELECT ts, venue, outcome, best_bid, best_ask, bid_depth, ask_depth
FROM market_snapshots WHERE run_id = :run AND reason = 'event' ORDER BY ts;
```
