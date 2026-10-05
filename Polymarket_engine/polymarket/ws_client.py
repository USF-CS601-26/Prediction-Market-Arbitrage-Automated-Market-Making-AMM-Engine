"""
polymarket/ws_client.py

Network side only: connects to Polymarket's market channel, subscribes to
the pair's token ids, keeps the connection alive with application-level
PING/PONG, and pushes every raw text frame onto an asyncio.Queue.

It never parses or touches order books. The single consumer in feed.py
does that, which keeps all book mutation in one task (no locks needed).

When the connection drops, a Disconnected marker is put on the same queue,
so the consumer sees it in order relative to the frames that preceded it.
Reconnect here is the basic version (exponential backoff with jitter); the
Subscription Lifecycle Monitor will harden it later.
"""

import asyncio
import json
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException

log = logging.getLogger(__name__)

POLYMARKET_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass(frozen=True, slots=True)
class RawFrame:
    recv_ts_ns: int     # local receive time (time.time_ns()); used for the recorded tape and latency stats
    text: str


@dataclass(frozen=True, slots=True)
class Disconnected:
    reason: str


QueueItem = RawFrame | Disconnected


class PolymarketWsClient:
    def __init__(
        self,
        asset_ids: list[str],
        queue: asyncio.Queue[QueueItem],
        *,
        url: str = POLYMARKET_MARKET_WS,
        ping_interval: float = 10.0,
        pong_timeout: float = 30.0,
        max_backoff: float = 30.0,
        on_raw: Callable[[RawFrame], None] | None = None,   # hook for the tape recorder
    ):
        self.asset_ids = asset_ids
        self.queue = queue
        self.url = url
        self.ping_interval = ping_interval
        self.pong_timeout = pong_timeout
        self.max_backoff = max_backoff
        self.on_raw = on_raw
        self._last_pong = 0.0
        self._close_reason: str | None = None

    async def run(self) -> None:
        """Connect, stream, and reconnect forever (until cancelled)."""
        backoff = 1.0
        while True:
            self._close_reason = None
            try:
                # max_size raised from the 1 MiB default: a deep book snapshot can be large
                async with connect(self.url, max_size=2**24) as ws:
                    log.info("connected to %s", self.url)
                    backoff = 1.0
                    await self._run_session(ws)
                reason = self._close_reason or "server closed the connection"
            except (OSError, WebSocketException) as exc:
                reason = f"{type(exc).__name__}: {exc}"

            await self.queue.put(Disconnected(reason))
            delay = backoff * random.uniform(0.5, 1.0)
            log.warning("disconnected (%s); reconnecting in %.1fs", reason, delay)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, self.max_backoff)

    async def _run_session(self, ws: ClientConnection) -> None:
        await ws.send(json.dumps({"type": "market", "assets_ids": self.asset_ids}))
        log.info("subscribed to %d assets", len(self.asset_ids))
        self._last_pong = time.monotonic()
        heartbeat = asyncio.create_task(self._heartbeat(ws))
        try:
            async for message in ws:
                text = message.decode() if isinstance(message, bytes) else message
                if text == "PONG":
                    self._last_pong = time.monotonic()
                    continue
                frame = RawFrame(recv_ts_ns=time.time_ns(), text=text)
                if self.on_raw is not None:
                    self.on_raw(frame)
                await self.queue.put(frame)
        finally:
            heartbeat.cancel()

    async def _heartbeat(self, ws: ClientConnection) -> None:
        """Send "PING" every ping_interval; close the socket if PONGs stop coming back."""
        try:
            while True:
                await asyncio.sleep(self.ping_interval)
                silent_for = time.monotonic() - self._last_pong
                if silent_for > self.pong_timeout:
                    self._close_reason = f"no PONG for {silent_for:.0f}s"
                    # closing ends the `async for` in _run_session, which triggers a reconnect
                    await ws.close()
                    return
                await ws.send("PING")
        except ConnectionClosed:
            return
