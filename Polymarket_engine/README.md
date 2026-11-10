# Polymarket engine (CS601 arbitrage project, Junyu's part)

The Polymarket side of our Kalshi / Polymarket cross-venue arbitrage engine.
It connects to Polymarket's public WebSocket, keeps a live in-memory L2 order
book for both outcome tokens (YES and NO) of one market, checks those books
for consistency, records raw data for offline testing, and computes
Polymarket taker fees.

It builds on the shared data contracts in `../kalshi_engine/` (Joshua's part):
every Polymarket message is converted into the same `OrderBookSnapshot` /
`OrderBookDelta` models (`models.py`) and applied to the same `OrderBook`
class (`order_book.py`). Those two files are imported from `kalshi_engine/`,
not copied, so this folder must stay next to `kalshi_engine/` in the repo.

Current test pair: Polymarket "Netanyahu out by end of 2026?" (see
`config/pair.toml`; the Kalshi ticker is still to be filled in).

## Milestone 1 progress

| Step | Status |
|---|---|
| 1-6 Config, WebSocket, heartbeat, parsing, adapter, order books | Done |
| 7 Reconnect | Implemented; manual test (turn Wi-Fi off and on) still to do |
| 8 Consistency checks | Done; a 114-minute recorded tape replays with zero failures |
| 9 Tape recording | Done |
| 10 Hand-written test cases | Runner and 3 examples; more cases to add |
| 11 Replay test | To do |
| 12 Taker fee function | Done |
| 13 Spread engine, 14 demo | Not started |

## Setup

Python 3.11 or newer is required (`tomllib`, `asyncio.TaskGroup`).
Create the virtual environment outside this folder, so its thousands of
files stay out of Git (and OneDrive):

Windows (PowerShell):

```
python -m venv $HOME\.venvs\cs601
$HOME\.venvs\cs601\Scripts\python.exe -m pip install -r requirements.txt
```

macOS / Linux:

```
python3 -m venv ~/.venvs/cs601
~/.venvs/cs601/bin/python -m pip install -r requirements.txt
```

The commands below say `python`: activate the venv first, or use the venv's
python directly.

## Usage

Run from this folder. Stop live runs with Ctrl+C.

| Task | Command |
|---|---|
| Stream the test pair live and print the top of book | `python run_polymarket.py` |
| Same, and record a tape into `fixtures/` | `python run_polymarket.py --record` |
| Read a tape in human-readable form | `python -m polymarket.inspect_tape fixtures/<tape>.jsonl` |
| Show one raw frame of a tape | `python -m polymarket.inspect_tape fixtures/<tape>.jsonl --raw 874` |
| Look up a market and print its config section | `python -m polymarket.gamma <market-slug>` |
| Run the tests | `pytest` |

## Layout

```
config/pair.toml          the hardcoded test pair: market ids, tick size, fees, verification notes
polymarket/
  config.py               loads pair.toml into validated models
  gamma.py                market lookup on the Gamma API (command-line tool)
  ws_client.py            WebSocket connection, subscription, PING/PONG heartbeat, reconnect
  messages.py             pydantic models for Polymarket's raw messages; parse_frame()
  adapter.py              raw messages -> shared OrderBookSnapshot / OrderBookDelta
  feed.py                 owns the YES and NO OrderBooks; validity tracking; consistency checks
  fees.py                 taker fee for one fill
  recorder.py             records raw frames to a .jsonl tape; load_tape() for replays
  inspect_tape.py         tape viewer (command-line tool)
run_polymarket.py         live entry point
fixtures/
  tape_*.jsonl            recorded tapes: raw WebSocket frames with receive times
  synthetic/*.json        hand-written test cases with hand-calculated answers
tests/                    pytest suite
```

## Tests

- `tests/test_polymarket_skeleton.py`: parser, adapter, feed and consistency checks.
- `tests/test_synthetic_fixtures.py`: runs every case in `fixtures/synthetic/`.
  To add a case, copy an example, write the input frames, work out the
  expected book by hand *before* running it, and save it. The file format is
  documented at the top of the test file.
- `tests/test_polymarket_fees.py`: the fee function. The first group of
  expected values is Polymarket's own fee table.

## Design notes

- **polymarket.com, not polymarket.us.** We read the public market channel of
  the international Polymarket, which needs no account or API key.
  Polymarket US is a separate exchange with a different, authenticated API.
- **Two books per market.** YES and NO are separate tokens with their own
  books, and they mirror each other: a YES ask at 0.42 is the same order as a
  NO bid at 0.58.
- **No sequence numbers.** Polymarket can't tell us that we missed a message,
  so books are marked invalid on disconnect until a fresh snapshot arrives,
  and updates that come before the first snapshot are dropped.
- **Checks run per timestamp group.** The exchange reports one trade as
  several messages with the same timestamp, and the book is briefly crossed
  in between. See the docstring of `polymarket/feed.py`.
- **Fees.** `fee = shares * feeRate * p * (1 - p)` in USDC, charged to takers
  only, rounded to 5 decimal places
  ([Polymarket docs](https://docs.polymarket.com/trading/fees)). The current
  test market has fees disabled.
- **Prices and sizes are `Decimal`, never `float`.**
- **Shared files are imported, not copied.** `pytest.ini` and
  `run_polymarket.py` add `../kalshi_engine` to the import path, so a change
  to `models.py` or `order_book.py` reaches both venues at once.
