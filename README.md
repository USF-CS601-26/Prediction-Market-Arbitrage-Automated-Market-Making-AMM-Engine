# Prediction-Market-Arbitrage-Automated-Market-Making-AMM-Engine

Required Features:
1.	Dual-Venue Market Data Ingestion: Asynchronous WebSocket handlers for raw exchange order streams.
2.	L2 Order Book Reconstruction: Single-threaded, in-memory order book engines maintaining real-time bid/ask depth.
3.	Pair Mapping & Manual Verification: Configuration pipeline to map and verify equivalent contracts across platforms.
4.	Subscription Lifecycle Monitor: Resilience manager that handles connection drops, heartbeat checks, and auto-resubscriptions.
5.	Executable Spread & Fee Engine: Calculates net spreads incorporating non-linear taker fee schedules and depth constraints.
6.	Event & Snapshot Persistence: PostgreSQL integration via SQLAlchemy/asyncpg for logging arbitrage events and book snapshots.

Milestone 1 status: Features 1, 2, 4, 5 are done. Featrue 3 is partially done. Featrue 6 is open.

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
