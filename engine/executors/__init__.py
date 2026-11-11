"""
engine/executors/

The venue-facing half of the trading side.

    base.py        BaseExecutor: the interface every venue implements
    simulator.py   SimulatedExecutor: fills against a real order book, with
                   latency, partial fills and rejections
    kalshi.py      MockKalshiExecutor
    polymarket.py  MockPolymarketExecutor
    factory.py     OrderExecutorFactory: venue -> executor

Strategies depend only on base.BaseExecutor, so swapping a mock for a real
venue client is a factory registration, not a strategy change.
"""

from engine.executors.base import BaseExecutor, FillListener
from engine.executors.factory import OrderExecutorFactory, UnknownVenueError
from engine.executors.kalshi import MockKalshiExecutor
from engine.executors.polymarket import MockPolymarketExecutor
from engine.executors.simulator import SimulatedExecutor, SimulationConfig

__all__ = [
    "BaseExecutor", "FillListener",
    "OrderExecutorFactory", "UnknownVenueError",
    "MockKalshiExecutor", "MockPolymarketExecutor",
    "SimulatedExecutor", "SimulationConfig",
]
