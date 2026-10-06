"""
tests/test_hooks_and_tape.py

Offline tests for the on_raw / on_update hooks and the tape
record-replay cycle. Fully synthetic — no network, no credentials.
"""

import asyncio
import json

import pytest

from decimal import Decimal
from order_book import OrderBook
from kalshi_ws import KalshiWSClient, RawFrame
from tape import TapeRecorder, load_tape, replay_tape

D = Decimal


def envelope(t, seq, msg):
    return {"type": t, "sid": 1, "seq": seq, "sending_ts_ms": 1669149841234, "msg": msg}


SNAPSHOT = envelope("orderbook_snapshot", 1, {
    "market_ticker": "M",
    "yes_dollars_fp": [["0.4000", "100.00"], ["0.4100", "50.00"]],
    "no_dollars_fp": [["0.5600", "80.00"]],
})
DELTAS = [
    envelope("orderbook_delta", 2, {"market_ticker": "M", "price_dollars": "0.4100",
                                    "delta_fp": "25.00", "side": "yes"}),
    envelope("orderbook_delta", 3, {"market_ticker": "M", "price_dollars": "0.5600",
                                    "delta_fp": "-80.00", "side": "no"}),
]


def fresh_client(**kw):
    book = OrderBook(market_id="M", exchange="kalshi")
    return KalshiWSClient("M", book, **kw), book


def drive(client, messages):
    async def run():
        for m in messages:
            await client._handle_message(m)
    asyncio.run(run())


# ------------------------------------------------------------ hooks

def test_on_update_fires_for_snapshot_and_each_delta():
    seen = []
    client, book = fresh_client(on_update=lambda c: seen.append(c.order_book.last_sequence))
    drive(client, [SNAPSHOT] + DELTAS)
    assert seen == [1, 2, 3]


def test_on_update_receives_the_client_so_consumers_can_read_state():
    captured = {}

    def cb(c):
        captured["fresh"] = c.book_is_fresh
        captured["top"] = c.order_book.get_top_of_book()

    client, book = fresh_client(on_update=cb)
    client.connected = True            # replay/live both set this
    drive(client, [SNAPSHOT])
    assert captured["fresh"] is True
    bid, ask = captured["top"]
    assert bid.price == D("0.4100")
    assert ask.price == D("1") - D("0.5600")


def test_async_callbacks_are_awaited():
    seen = []

    async def cb(c):
        await asyncio.sleep(0)
        seen.append(c.order_book.last_sequence)

    client, _ = fresh_client(on_update=cb)
    drive(client, [SNAPSHOT] + DELTAS)
    assert seen == [1, 2, 3]


def test_a_raising_callback_does_not_kill_ingestion():
    """
    Ingestion staying up matters more than any one consumer: a dropped
    connection would lose book state too.
    """
    def boom(c):
        raise ValueError("consumer bug")

    client, book = fresh_client(on_update=boom)
    drive(client, [SNAPSHOT] + DELTAS)
    assert book.last_sequence == 3                 # all messages still applied
    assert client._callback_errors == 3            # and the failures were counted


# ------------------------------------------------------------- tape

def test_tape_round_trip_preserves_frames(tmp_path):
    path = tmp_path / "t.jsonl"
    with TapeRecorder(path) as rec:
        for i, m in enumerate([SNAPSHOT] + DELTAS):
            rec.write(RawFrame(recv_ts_ns=1000 + i, text=json.dumps(m)))
        assert rec.frames_written == 3

    frames = list(load_tape(path))
    assert [f.recv_ts_ns for f in frames] == [1000, 1001, 1002]
    assert json.loads(frames[0].text)["type"] == "orderbook_snapshot"


def test_replay_rebuilds_the_same_book_as_live(tmp_path):
    """The property that makes recorded tapes usable as test fixtures."""
    live_client, live_book = fresh_client()
    drive(live_client, [SNAPSHOT] + DELTAS)

    path = tmp_path / "t.jsonl"
    with TapeRecorder(path) as rec:
        for i, m in enumerate([SNAPSHOT] + DELTAS):
            rec.write(RawFrame(recv_ts_ns=1000 + i, text=json.dumps(m)))

    replay_client, replay_book = fresh_client()
    n = asyncio.run(replay_tape(path, replay_client))

    assert n == 3
    assert dict(replay_book.bids) == dict(live_book.bids)
    assert dict(replay_book.asks) == dict(live_book.asks)
    assert replay_book.last_sequence == live_book.last_sequence


def test_frames_are_recorded_unparsed(tmp_path):
    """
    A tape must survive a parser bug, so frames are stored as received.
    Unparseable text records fine and only fails on replay.
    """
    path = tmp_path / "t.jsonl"
    with TapeRecorder(path) as rec:
        rec.write(RawFrame(recv_ts_ns=1, text="{not json"))
    assert list(load_tape(path))[0].text == "{not json"
