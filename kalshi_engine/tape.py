"""
tape.py

Records raw Kalshi WebSocket frames to a JSON Lines "tape" and replays
them offline through the order book.

The format is deliberately identical to the Polymarket side's recorder,
so one replay harness and one fixture convention cover both venues:

    {"recv_ts_ns": 1791140701568123456, "text": "<raw frame text>"}

Frames are stored UNPARSED. If the parser has a bug, the tape is still a
faithful copy of what the exchange sent, so replaying it after the fix
reproduces the original session exactly. A tape parsed at record time
would bake the bug in permanently.

Record:
    python kalshi_ws.py --record                # writes fixtures/tape_kalshi_<ticker>_<ts>.jsonl

Replay (offline, no network, no credentials):
    python tape.py fixtures/tape_kalshi_...jsonl
"""

import json
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from kalshi_ws import RawFrame


class TapeRecorder:
    """Pass `recorder.write` as KalshiWSClient(on_raw=...)."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # line-buffered so every frame reaches disk immediately and a
        # Ctrl+C mid-session loses nothing
        self._file = open(self.path, "a", encoding="utf-8", buffering=1)
        self.frames_written = 0

    def write(self, frame: RawFrame) -> None:
        self._file.write(json.dumps({"recv_ts_ns": frame.recv_ts_ns,
                                     "text": frame.text}) + "\n")
        self.frames_written += 1

    def close(self) -> None:
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def default_tape_path(label: str, directory: str | Path = "fixtures") -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path(directory) / f"tape_kalshi_{label}_{stamp}.jsonl"


def load_tape(path: str | Path) -> Iterator[RawFrame]:
    """Yields recorded frames in order."""
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                row = json.loads(line)
                yield RawFrame(recv_ts_ns=row["recv_ts_ns"], text=row["text"])


async def replay_tape(path: str | Path, client) -> int:
    """
    Feeds a recorded tape through a client's message handler, rebuilding
    the book exactly as the live session did. No network and no
    credentials, which is what makes recorded tapes usable as tests.

    The client is driven directly rather than through run_forever, so
    nothing tries to open a socket. Frames are applied as fast as they
    can be parsed; inter-frame timing is preserved in recv_ts_ns for
    anyone who wants to pace a replay, but is not honoured here.
    """
    # a replayed session is authoritative for its own book state
    client.connected = True
    n = 0
    for frame in load_tape(path):
        await client._handle_message(json.loads(frame.text))
        n += 1
    return n


if __name__ == "__main__":
    import argparse
    import asyncio

    from order_book import OrderBook
    import kalshi_ws

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("tape")
    p.add_argument("--ticker", default="REPLAY")
    a = p.parse_args()

    book = OrderBook(market_id=a.ticker, exchange="kalshi")
    client = kalshi_ws.KalshiWSClient(a.ticker, book)

    frames = asyncio.run(replay_tape(a.tape, client))
    bid, ask = book.get_top_of_book()
    print(f"\nreplayed {frames} frames from {a.tape}")
    print(f"  final book: {len(book.bids)} bid / {len(book.asks)} ask levels, "
          f"seq={book.last_sequence}")
    print(f"  top: bid {bid.price} x {bid.size} | ask {ask.price} x {ask.size}"
          if bid and ask else "  top: one side empty")
