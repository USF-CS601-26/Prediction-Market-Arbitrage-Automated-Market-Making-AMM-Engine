"""
polymarket/recorder.py

Records raw WebSocket frames to a JSON Lines "tape" so real market data can
be replayed offline through the parser and order books (the recorded-tape
test set from the proposal).

Each line is one frame exactly as received, plus the local receive time:
    {"recv_ts_ns": 1791140701568123456, "text": "<raw frame text>"}

Frames are stored unparsed on purpose: if the parser has a bug, the tape is
still a faithful copy of what the exchange sent, and replaying it after the
fix reproduces the original session.
"""

import json
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from polymarket.ws_client import RawFrame


class TapeRecorder:
    """Pass `recorder.write` as PolymarketWsClient(on_raw=...)."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # line-buffered, so every frame reaches disk immediately and a Ctrl+C loses nothing
        self._file = open(self.path, "a", encoding="utf-8", buffering=1)
        self.frames_written = 0

    def write(self, frame: RawFrame) -> None:
        self._file.write(json.dumps({"recv_ts_ns": frame.recv_ts_ns, "text": frame.text}) + "\n")
        self.frames_written += 1

    def close(self) -> None:
        self._file.close()


def default_tape_path(pair_id: str, directory: str | Path = "fixtures") -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path(directory) / f"tape_polymarket_{pair_id}_{stamp}.jsonl"


def load_tape(path: str | Path) -> Iterator[RawFrame]:
    """Yield the recorded frames in order, ready to feed to PolymarketFeed.handle()."""
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                row = json.loads(line)
                yield RawFrame(recv_ts_ns=row["recv_ts_ns"], text=row["text"])
