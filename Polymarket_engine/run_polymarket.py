"""
run_polymarket.py

Runs the Polymarket side live for the configured test pair and prints the
top of book for both outcome tokens every few seconds.

Usage:
    python run_polymarket.py [config/pair.toml] [--record]
--record also writes every raw frame to fixtures/tape_polymarket_<pair>_<time>.jsonl.
Stop with Ctrl+C.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# the shared models.py / order_book.py live in kalshi_engine/, next to this folder
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "kalshi_engine"))

from polymarket.config import load_pair_config
from polymarket.feed import PolymarketFeed
from polymarket.recorder import TapeRecorder, default_tape_path
from polymarket.ws_client import PolymarketWsClient


def _fmt(level) -> str:
    return "-" if level is None else f"{level.price} x {level.size}"


async def print_top_of_book(feed: PolymarketFeed, recorder: TapeRecorder | None, every: float = 3.0) -> None:
    while True:
        await asyncio.sleep(every)
        for outcome in ("YES", "NO"):
            if not feed.is_valid(outcome):
                print(f"{outcome:>3}  (no valid book yet)")
                continue
            bid, ask = feed.book(outcome).get_top_of_book()
            ts = feed.last_update_ts[feed._asset(outcome)]
            print(f"{outcome:>3}  bid {_fmt(bid):<20} ask {_fmt(ask):<20} updated {ts:%H:%M:%S} UTC")
        recorded = f"  recorded {recorder.frames_written} frames" if recorder else ""
        print(f"     stats {dict(feed.stats)}{recorded}")


async def main(config_path: str, record: bool) -> None:
    pair = load_pair_config(config_path)
    recorder = None
    if record:
        recorder = TapeRecorder(default_tape_path(pair.pair_id))
        print(f"recording raw frames to {recorder.path}")
    queue: asyncio.Queue = asyncio.Queue()
    client = PolymarketWsClient(pair.asset_ids(), queue, on_raw=recorder.write if recorder else None)
    feed = PolymarketFeed(pair, queue)
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(client.run())
            tg.create_task(feed.run())
            tg.create_task(print_top_of_book(feed, recorder))
    finally:
        if recorder:
            recorder.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config", nargs="?", default="config/pair.toml")
    parser.add_argument("--record", action="store_true", help="write raw frames to a tape in fixtures/")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(main(args.config, args.record))
    except KeyboardInterrupt:
        pass
