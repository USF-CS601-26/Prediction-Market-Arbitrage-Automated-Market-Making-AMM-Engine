"""
polymarket/feed.py

The single consumer task for one Polymarket pair. It owns the two OrderBooks
(YES token and NO token) and is the only code that mutates them:

    queue -> parse_frame -> to_book_updates -> OrderBook.apply_snapshot / apply_delta

Because Polymarket has no sequence numbers, OrderBook's gap check can't
protect us, so this class tracks validity itself:
  - deltas that arrive before a token's first snapshot are dropped;
  - on Disconnected, every book is marked invalid until the snapshot sent
    after resubscribing arrives.

Consistency checks (each failure is logged and counted in stats["check_*"]):
  - check_bad_level:     price outside (0, 1) or negative size -> update rejected
  - check_off_tick:      price not a multiple of the tick size  -> applied, flagged
  - check_crossed:       best bid >= best ask
  - check_top_mismatch:  our top of book differs from the best_bid/best_ask
                         the exchange attached to its price_change messages

The last two run once per group of messages that share an exchange
timestamp, not after every message. The exchange reports one matching event
as several messages with the same timestamp: when an order trades against a
resting level, it first reports the order's unfilled remainder and only in
the next message removes the level it used up. In between, the book is
briefly crossed (seen in a recorded tape on 2026-10-05). settle() runs these
checks; it is called automatically when a message with a different timestamp
arrives, and a replay should call it once at the end.

Failures other than bad levels don't invalidate the book: the only way to
resync is a fresh snapshot, which needs a resubscribe. Triggering that is
left to the Subscription Lifecycle Monitor.

handle() is synchronous on purpose, so tests can drive it directly without
an event loop.
"""

import asyncio
import logging
from collections import Counter
from datetime import datetime
from decimal import Decimal

from models import OrderBookSnapshot
from order_book import OrderBook
from polymarket.adapter import EXCHANGE, BookUpdate, to_book_updates
from polymarket.config import Outcome, PairConfig
from polymarket.messages import PriceChange, PriceChangeMsg, TickSizeChangeMsg, parse_frame
from polymarket.ws_client import Disconnected, QueueItem

log = logging.getLogger(__name__)

_ZERO = Decimal(0)
_ONE = Decimal(1)


class PolymarketFeed:
    def __init__(self, pair: PairConfig, queue: asyncio.Queue[QueueItem]):
        self.pair = pair
        self.queue = queue
        assets = pair.asset_ids()
        self.books: dict[str, OrderBook] = {a: OrderBook(market_id=a, exchange=EXCHANGE) for a in assets}
        self.tick_size: dict[str, Decimal] = {a: pair.polymarket.tick_size for a in assets}
        self.last_update_ts: dict[str, datetime | None] = {a: None for a in assets}
        self._valid: dict[str, bool] = {a: False for a in assets}
        self.stats: Counter[str] = Counter()
        # the current group: everything applied since the last settle()
        self._group_ts: int | None = None
        self._touched: set[str] = set()
        self._hints: dict[str, PriceChange] = {}

    # ---- read side, used by the spread engine ----

    def _asset(self, outcome: Outcome) -> str:
        return self.pair.polymarket.yes_token if outcome == "YES" else self.pair.polymarket.no_token

    def book(self, outcome: Outcome) -> OrderBook:
        return self.books[self._asset(outcome)]

    def is_valid(self, outcome: Outcome) -> bool:
        """False until the first snapshot, and again after a disconnect until the next one."""
        return self._valid[self._asset(outcome)]

    def check_failures(self) -> int:
        return sum(n for key, n in self.stats.items() if key.startswith("check_"))

    # ---- write side ----

    async def run(self) -> None:
        while True:
            item = await self.queue.get()
            self.handle(item)

    def handle(self, item: QueueItem) -> None:
        if isinstance(item, Disconnected):
            self._clear_group()     # a group cut off by a disconnect can't be judged
            self._invalidate_all(item.reason)
            return

        self.stats["frames"] += 1
        for event in parse_frame(item.text):
            if event.timestamp != self._group_ts:
                self.settle()
                self._group_ts = event.timestamp
            if isinstance(event, TickSizeChangeMsg):
                if event.asset_id in self.tick_size:
                    log.info("tick size %s -> %s on %s", event.old_tick_size, event.new_tick_size, event.asset_id)
                    self.tick_size[event.asset_id] = event.new_tick_size
                continue
            for update in to_book_updates(event):
                self._apply(update)
            if isinstance(event, PriceChangeMsg):
                for change in event.price_changes:
                    if change.asset_id in self.books:
                        self._hints[change.asset_id] = change     # the latest hint wins

    def settle(self) -> None:
        """Run the group-level checks on everything applied since the last call."""
        for asset in self._touched:
            if self._valid[asset]:
                self._check_not_crossed(asset)
        for asset, hint in self._hints.items():
            if self._valid[asset]:
                self._check_against_exchange(asset, hint)
        self._clear_group()

    def _apply(self, update: BookUpdate) -> None:
        asset = update.market_id
        book = self.books.get(asset)
        if book is None:
            self.stats["unknown_asset"] += 1
            return

        if isinstance(update, OrderBookSnapshot):
            levels = update.bids + update.asks
            if not all(self._level_ok(asset, l.price, l.size) for l in levels):
                return
            book.apply_snapshot(update)
            self._valid[asset] = True
            self._hints.pop(asset, None)    # the snapshot supersedes earlier hints in this group
            self.stats["snapshots"] += 1
        else:
            if not self._valid[asset]:
                # no snapshot yet (or stale after a disconnect): applying would corrupt the book
                self.stats["dropped_before_snapshot"] += 1
                return
            if not self._level_ok(asset, update.price, update.size):
                return
            book.apply_delta(update)
            self.stats["deltas"] += 1

        self.last_update_ts[asset] = update.timestamp
        self._touched.add(asset)

    def _clear_group(self) -> None:
        self._group_ts = None
        self._touched.clear()
        self._hints.clear()

    # ---- consistency checks ----

    def _fail(self, check: str, asset: str, detail: str) -> None:
        self.stats[f"check_{check}"] += 1
        log.warning("check %s failed on %s: %s", check, self.pair.outcome_of(asset), detail)

    def _level_ok(self, asset: str, price: Decimal, size: Decimal) -> bool:
        """Rejects impossible levels; flags (but keeps) prices off the tick grid."""
        if not (_ZERO < price < _ONE) or size < 0:
            self._fail("bad_level", asset, f"price={price} size={size} (update rejected)")
            return False
        if price % self.tick_size[asset] != 0:
            self._fail("off_tick", asset, f"price={price} tick={self.tick_size[asset]}")
        return True

    def _check_not_crossed(self, asset: str) -> None:
        bid, ask = self.books[asset].get_top_of_book()
        if bid is not None and ask is not None and bid.price >= ask.price:
            self._fail("crossed", asset, f"best bid {bid.price} >= best ask {ask.price}")

    def _check_against_exchange(self, asset: str, hint: PriceChange) -> None:
        bid, ask = self.books[asset].get_top_of_book()
        ours = (bid.price if bid else None, ask.price if ask else None)
        if not (_matches(ours[0], hint.best_bid, empty=_ZERO)
                and _matches(ours[1], hint.best_ask, empty=_ONE)):
            self._fail("top_mismatch", asset,
                       f"ours {ours[0]} / {ours[1]}, exchange {hint.best_bid} / {hint.best_ask}")

    def _invalidate_all(self, reason: str) -> None:
        log.warning("marking all books invalid: %s", reason)
        for asset in self._valid:
            self._valid[asset] = False
        self.stats["disconnects"] += 1


def _matches(ours: Decimal | None, exchange: Decimal | None, empty: Decimal) -> bool:
    """Our best price vs the exchange's hint. An empty side may be reported as 0 (bids) or 1 (asks)."""
    if exchange is None:
        return True     # no hint, nothing to compare
    if ours is None:
        return exchange == empty
    return ours == exchange
