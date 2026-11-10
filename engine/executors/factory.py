"""
engine/executors/factory.py

OrderExecutorFactory — venue name to executor, resolved once at startup.

A strategy holds a factory, not a set of executors. That is the seam that
lets the same ArbitrageStrategy run against mocks in a replay test and
against real venue clients in production without touching strategy code: the
pipeline registers different builders, and nothing downstream notices.

Executors are built lazily and cached per venue, because an executor owns
per-venue state — the order book it fills against, its order table, its
seeded RNG. Handing out a fresh one per call would lose the order history
that cancel_order and get_status depend on.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from engine.executors.base import BaseExecutor

# A builder takes no arguments: everything it needs was captured when it was
# registered. Keeping it zero-argument means the factory never has to know
# what a given venue's executor needs to be constructed.
ExecutorBuilder = Callable[[], BaseExecutor]


class UnknownVenueError(KeyError):
    """Raised when an order names a venue nothing is registered for."""


class OrderExecutorFactory:
    """
    Usage:

        factory = OrderExecutorFactory()
        factory.register("kalshi", lambda: MockKalshiExecutor(ticker, book, k_fee))
        factory.register("polymarket", lambda: MockPolymarketExecutor(feed, p_fee))

        await factory.for_venue("kalshi").submit_order(order)
    """

    def __init__(self, builders: dict[str, ExecutorBuilder] | None = None):
        self._builders: dict[str, ExecutorBuilder] = dict(builders or {})
        self._cache: dict[str, BaseExecutor] = {}

    def register(self, venue: str, builder: ExecutorBuilder) -> None:
        """
        Register (or replace) a venue's builder. Replacing one drops the
        cached executor, so a pipeline that swaps a mock for a live client
        mid-session does not keep routing to the old one.
        """
        self._builders[venue] = builder
        self._cache.pop(venue, None)

    def register_instance(self, executor: BaseExecutor) -> None:
        """Register an already-built executor under its own venue name."""
        self.register(executor.venue, lambda: executor)

    def for_venue(self, venue: str) -> BaseExecutor:
        if venue not in self._builders:
            raise UnknownVenueError(
                f"no executor registered for venue {venue!r}; "
                f"have {sorted(self._builders)}"
            )
        if venue not in self._cache:
            self._cache[venue] = self._builders[venue]()
        return self._cache[venue]

    def for_order(self, order) -> BaseExecutor:
        """Route by the order's own venue field."""
        return self.for_venue(order.venue)

    def venues(self) -> list[str]:
        return sorted(self._builders)

    def built(self) -> Iterable[BaseExecutor]:
        """Executors constructed so far — for wiring fill listeners or stats."""
        return self._cache.values()

    def add_fill_listener_to_all(self, listener) -> None:
        """
        Attach one listener to every registered venue, building each
        executor now. The PortfolioManager needs to hear about fills from
        venues the strategy has not happened to touch yet, so this
        deliberately forces construction rather than waiting.
        """
        for venue in self._builders:
            self.for_venue(venue).add_fill_listener(listener)
