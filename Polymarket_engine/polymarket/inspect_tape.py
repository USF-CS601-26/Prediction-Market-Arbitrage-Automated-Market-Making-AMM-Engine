"""
polymarket/inspect_tape.py

Prints a recorded tape in human-readable form: one line per book snapshot
or price change, with token ids replaced by YES/NO, followed by a summary.

Usage:
    python -m polymarket.inspect_tape <tape.jsonl> [--config config/pair.toml] [--raw N]

--raw N pretty-prints frame N (1-based, as in the left column) exactly as received.
"""

import argparse
import json
from collections import Counter
from datetime import datetime, timezone

from polymarket.config import load_pair_config
from polymarket.messages import BookMsg, LastTradePriceMsg, PriceChangeMsg, TickSizeChangeMsg, parse_frame
from polymarket.recorder import load_tape


def _clock(recv_ts_ns: int) -> str:
    return datetime.fromtimestamp(recv_ts_ns / 1e9, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("tape")
    parser.add_argument("--config", default="config/pair.toml")
    parser.add_argument("--raw", type=int, metavar="N", help="pretty-print frame N as received")
    args = parser.parse_args()

    pair = load_pair_config(args.config)

    def name(asset_id: str) -> str:
        try:
            return pair.outcome_of(asset_id)
        except KeyError:
            return f"..{asset_id[-6:]}"

    frames = list(load_tape(args.tape))

    if args.raw is not None:
        frame = frames[args.raw - 1]
        print(f"frame {args.raw} received {_clock(frame.recv_ts_ns)} UTC")
        print(json.dumps(json.loads(frame.text), indent=2))
        return

    counts: Counter[str] = Counter()
    last_top: dict[str, tuple] = {}
    top_moves: Counter[str] = Counter()

    for i, frame in enumerate(frames, start=1):
        prefix = f"{i:>5}  {_clock(frame.recv_ts_ns)}"
        for event in parse_frame(frame.text):
            counts[event.event_type] += 1
            match event:
                case BookMsg():
                    best_bid = max(event.bids, key=lambda l: l.price, default=None)
                    best_ask = min(event.asks, key=lambda l: l.price, default=None)
                    top = (f"{best_bid.price} x {best_bid.size}" if best_bid else "-",
                           f"{best_ask.price} x {best_ask.size}" if best_ask else "-")
                    print(f"{prefix}  book    {name(event.asset_id):<3}  "
                          f"{len(event.bids)} bids / {len(event.asks)} asks   best {top[0]} | {top[1]}")
                case PriceChangeMsg():
                    for c in event.price_changes:
                        print(f"{prefix}  change  {name(c.asset_id):<3}  {c.side:<4} {c.price:<6} -> {c.size:<12}"
                              f"exchange top {c.best_bid} / {c.best_ask}")
                        prefix = " " * len(prefix)
                        top = (c.best_bid, c.best_ask)
                        if c.asset_id in last_top and last_top[c.asset_id] != top:
                            top_moves[name(c.asset_id)] += 1
                        last_top[c.asset_id] = top
                case TickSizeChangeMsg():
                    print(f"{prefix}  tick    {name(event.asset_id):<3}  {event.old_tick_size} -> {event.new_tick_size}")
                case LastTradePriceMsg():
                    print(f"{prefix}  trade   {name(event.asset_id):<3}  {event.side:<4} {event.size} @ {event.price}")
            prefix = " " * len(prefix)

    if frames:
        duration = (frames[-1].recv_ts_ns - frames[0].recv_ts_ns) / 1e9
        print()
        print(f"frames: {len(frames)}   duration: {duration / 60:.1f} min   events: {dict(counts)}")
        print(f"top-of-book moves (from exchange hints): {dict(top_moves) or 'none'}")


if __name__ == "__main__":
    main()
