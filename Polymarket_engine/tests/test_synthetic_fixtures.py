"""
tests/test_synthetic_fixtures.py

Runs every hand-written case in fixtures/synthetic/*.json through
PolymarketFeed and compares the final books with the hand-calculated answer.

A case file looks like this (see the examples in fixtures/synthetic/):

{
  "description": "what this case checks",
  "frames": [
    [ {"event_type": "book", "asset_id": "YES", "bids": [...], "asks": [...]} ],
    {"event_type": "price_change", "price_changes": [
        {"asset_id": "YES", "price": "0.45", "size": "300", "side": "BUY"}
    ]},
    "DISCONNECT"
  ],
  "expected": {
    "YES": {"valid": true,
            "bids": [["0.45", "300"], ["0.42", "28.4"]],   <- full book, best price first
            "asks": [["0.60", "200"]]},
    "stats": {"dropped_before_snapshot": 1},              <- optional
    "check_failures": 0                                   <- optional, defaults to 0
  }
}

Each entry in "frames" is one WebSocket frame, written in Polymarket's real
message format, so the parser is tested too. To keep the files short:
  - use "YES" / "NO" as the asset_id (the test pair maps them to the outcomes);
  - "market" and "timestamp" may be left out (filled in automatically, a
    different timestamp per frame). Give several frames the same explicit
    "timestamp" to model one trade reported in several messages; the
    crossed-book and exchange-hint checks only judge the book once that
    group is complete;
  - the string "DISCONNECT" simulates a dropped connection.
"""

import json
from decimal import Decimal
from pathlib import Path

import pytest

from polymarket.config import PairConfig
from polymarket.feed import PolymarketFeed
from polymarket.ws_client import Disconnected, RawFrame

CASE_DIR = Path(__file__).parent.parent / "fixtures" / "synthetic"
CASES = sorted(CASE_DIR.glob("*.json"))

PAIR = PairConfig.model_validate({
    "pair_id": "synthetic",
    "polymarket": {
        "slug": "synthetic", "condition_id": "0xtest", "yes_token": "YES", "no_token": "NO",
        "tick_size": "0.01", "fees": {"enabled": False},
    },
})


def _to_queue_item(entry, index: int):
    if entry == "DISCONNECT":
        return Disconnected("synthetic disconnect")
    for msg in entry if isinstance(entry, list) else [entry]:
        msg.setdefault("market", "0xtest")
        msg.setdefault("timestamp", str(1_791_000_000_000 + index))
    return RawFrame(recv_ts_ns=index, text=json.dumps(entry))


def _as_levels(rows) -> list[tuple[Decimal, Decimal]]:
    return [(Decimal(price), Decimal(size)) for price, size in rows]


@pytest.mark.parametrize("path", CASES, ids=[p.stem for p in CASES])
def test_synthetic_case(path: Path):
    case = json.loads(path.read_text(encoding="utf-8"))
    feed = PolymarketFeed(PAIR, queue=None)
    for i, entry in enumerate(case["frames"]):
        feed.handle(_to_queue_item(entry, i))
    feed.settle()   # run the end-of-group checks for the last group

    expected = case["expected"]
    for outcome in ("YES", "NO"):
        if outcome not in expected:
            continue
        want = expected[outcome]
        if "valid" in want:
            assert feed.is_valid(outcome) == want["valid"], f"{outcome} validity"
        bids, asks = feed.book(outcome).get_depth(levels=1000)
        if "bids" in want:
            assert [(l.price, l.size) for l in bids] == _as_levels(want["bids"]), f"{outcome} bids"
        if "asks" in want:
            assert [(l.price, l.size) for l in asks] == _as_levels(want["asks"]), f"{outcome} asks"

    for key, count in expected.get("stats", {}).items():
        assert feed.stats[key] == count, f"stats[{key}]"
    assert feed.check_failures() == expected.get("check_failures", 0), f"check failures: {dict(feed.stats)}"
