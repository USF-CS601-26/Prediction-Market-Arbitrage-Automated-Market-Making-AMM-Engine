"""
mock_kalshi_feed.py

Generates synthetic Kalshi-style order book events (one snapshot followed
by a stream of deltas) for offline testing of OrderBook, without needing
a live network connection to Kalshi.

Running this file directly writes the generated events to
fixtures/mock_feed.json, which tests/test_order_book.py replays to verify
OrderBook's apply_snapshot/apply_delta logic end-to-end.
"""

import json
import random
from decimal import Decimal
from datetime import datetime, timezone


def generate_mock_snapshot(market_id="KXPRES-24", mid=Decimal("0.55")) -> dict:
    """
    Builds one fake snapshot: 5 bid levels and 5 ask levels around a
    midpoint price, separated by `spread` so bids and asks never touch
    or cross (a crossed book is unrealistic and would fail downstream
    sanity checks like bid.price < ask.price).
    """
    spread = Decimal("0.01")
    return {
        "market_id": market_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sequence": 1,
        "bids": [
            # walk down from (mid - spread) in 1-cent steps: 0.54, 0.53, ...
            {"price": str(mid - spread - Decimal("0.01") * i), "size": str(100 * (i + 1))}
            for i in range(5)
        ],
        "asks": [
            # walk up from (mid + spread) in 1-cent steps: 0.56, 0.57, ...
            {"price": str(mid + spread + Decimal("0.01") * i), "size": str(100 * (i + 1))}
            for i in range(5)
        ],
    }


def generate_mock_delta_stream(market_id="KXPRES-24", n=20, start_seq=2):
    """
    Generator yielding `n` fake delta messages with sequentially
    increasing sequence numbers (continuing on from the snapshot's
    sequence=1, so start_seq defaults to 2).

    Bid deltas are constrained to stay below 0.55 and ask deltas above
    0.55, so randomly generated deltas can never cross the book the
    same way the snapshot's levels can't.
    """
    seq = start_seq
    for _ in range(n):
        side = random.choice(["bid", "ask"])
        if side == "bid":
            price = round(random.uniform(0.50, 0.54), 2)   # bids stay below mid
        else:
            price = round(random.uniform(0.56, 0.60), 2)   # asks stay above mid
        yield {
            "market_id": market_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sequence": seq,
            "side": side,
            "price": str(price),
            "size": str(random.choice([0, 50, 100, 150])),  # 0 exercises the "remove level" path
        }
        seq += 1


if __name__ == "__main__":
    # build the full event list: one snapshot, then n deltas, each tagged
    # with a "type" field so the test harness knows which OrderBook method
    # to call when replaying
    events = [{"type": "snapshot", **generate_mock_snapshot()}]
    events += [{"type": "delta", **d} for d in generate_mock_delta_stream()]

    with open("fixtures/mock_feed.json", "w") as f:
        json.dump(events, f, indent=2)
    print(f"Wrote {len(events)} events to fixtures/mock_feed.json")