"""
engine/strategy.py

BaseStrategy — the Strategy Pattern seam the pipeline drives.

The pipeline knows three things about a strategy: its id, whether it is
enabled, and that calling `await strategy.on_tick(now)` is how you give it a
chance to act. It does not know whether that strategy is hunting
complementary-pair arbitrage, quoting a market as an AMM, or doing nothing
at all. That is the whole point: the arbitrage strategy and Person A's AMM
quoter are registered the same way and run on the same tick, and adding a
third needs no pipeline change.

on_tick is called after any book change (and on a periodic heartbeat), not
on a fixed schedule. A strategy must therefore be cheap when there is
nothing to do and must not assume a regular interval.

Returning a StrategyResult rather than nothing lets the pipeline log and
test what a strategy decided, including the decisions NOT to trade — which
are the interesting ones when a threshold is doing its job.
"""

from __future__ import annotations

import inspect
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from engine.models import Order, RiskDecision, utcnow

log = logging.getLogger(__name__)


class StrategyResult(BaseModel):
    """
    What one tick decided. `acted` means orders were dispatched; everything
    else is a reason, which is deliberately a free-text string so a new
    strategy does not have to extend an enum to explain itself.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    strategy_id: str
    timestamp: datetime = Field(default_factory=utcnow)
    acted: bool = False
    reason: str = ""
    detail: Any = None          # strategy-specific payload, e.g. an ArbitrageAttempt


class BaseStrategy(ABC):
    """
    Subclasses implement on_tick. Everything else here is bookkeeping the
    pipeline relies on being present.
    """

    def __init__(self, strategy_id: str, enabled: bool = True):
        self.strategy_id = strategy_id
        self.enabled = enabled
        self.ticks = 0
        self.actions = 0
        self.last_result: StrategyResult | None = None

    @abstractmethod
    async def on_tick(self, now: datetime | None = None) -> StrategyResult:
        """Evaluate, and act if the strategy's own criteria are met."""

    async def run_tick(self, now: datetime | None = None) -> StrategyResult | None:
        """
        What the pipeline actually calls. Skips a disabled strategy, counts
        ticks, and contains exceptions: one strategy raising must not stop
        the others or kill the ingestion tasks it shares a loop with.
        """
        if not self.enabled:
            return None
        self.ticks += 1
        try:
            result = await self.on_tick(now or utcnow())
        except Exception as exc:
            log.exception("%s raised on tick", self.strategy_id)
            result = StrategyResult(strategy_id=self.strategy_id,
                                    reason=f"error: {type(exc).__name__}: {exc}")
        self.last_result = result
        if result.acted:
            self.actions += 1
        return result

    def skip(self, reason: str, now: datetime | None = None) -> StrategyResult:
        """A tick that decided not to trade, with the reason recorded."""
        return StrategyResult(strategy_id=self.strategy_id,
                              timestamp=now or utcnow(), acted=False, reason=reason)

    async def check_risk(self, risk, order: Order) -> RiskDecision:
        """
        Call the PortfolioManager's check_order, tolerating a sync or an
        async implementation. Person A's may end up hitting a database; the
        strategy should not have to be rewritten when it does.
        """
        decision = risk.check_order(order)
        if inspect.isawaitable(decision):
            decision = await decision
        return decision
