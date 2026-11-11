"""
engine/models.py

Data contracts for the execution and strategy layer.

kalshi_engine/models.py owns the *market data* contracts (OrderBook
snapshots/deltas, ExecutionQuote, CrossVenueEdge). This file owns the
*order* contracts: what we send, what comes back, and what the risk layer
decides. The split keeps the ingestion engines free of any dependency on
the trading side.

Decimal everywhere, for the same reason as in models.py: a cent of float
drift is the difference between an arbitrage and a loss.

Two naming notes:

  - `OrderSide` is BUY/SELL, deliberately NOT the existing `Side` (BID/ASK)
    from models.py. Side describes which half of a book a level sits on;
    OrderSide describes what we are doing. Conflating them is how you end
    up selling when you meant to lift an offer.
  - `outcome` is "YES" or "NO", matching polymarket/config.Outcome. Kalshi
    has one ticker with a yes/no book and Polymarket has two token books,
    so the outcome has to travel with the order rather than being implied
    by the market id.
"""

from __future__ import annotations

import itertools
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

Outcome = Literal["YES", "NO"]

ZERO = Decimal("0")
ONE = Decimal("1")

_order_ids = itertools.count(1)


def next_order_id(prefix: str = "ord") -> str:
    """Client-side order id. Monotonic so a log reads in dispatch order."""
    return f"{prefix}-{next(_order_ids):06d}"


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    """
    Only limit orders are modelled. An arbitrage leg must never be a market
    order: the whole edge is a couple of cents, and a market order is a
    standing offer to pay any price in the book.
    """
    LIMIT = "limit"


class OrderStatus(str, Enum):
    """
    Lifecycle of one order at one venue.

    PENDING is the gap between submit_order returning and the venue
    acknowledging — short, but real, and the window in which a paired
    strategy is most exposed.
    """
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"

    @property
    def is_terminal(self) -> bool:
        """True once the venue will send no further fills for this order."""
        return self in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED)


class Order(BaseModel):
    """
    One leg, as sent to one venue.

    `limit_price` is the worst price we will accept, always: for a BUY it is
    a ceiling, for a SELL a floor. The mock executors enforce it while
    walking the book, which is what makes a partial fill the normal outcome
    rather than an error.
    """
    order_id: str = Field(default_factory=next_order_id)
    venue: str                      # "kalshi" | "polymarket"
    market_id: str                  # Kalshi ticker, or Polymarket token id
    outcome: Outcome
    side: OrderSide
    size: Decimal                   # contracts / shares
    limit_price: Decimal            # dollars, 0 < price < 1
    order_type: OrderType = OrderType.LIMIT
    created_at: datetime = Field(default_factory=utcnow)

    # provenance, so a fill in the log can be traced back to the decision
    pair_id: str | None = None
    strategy_id: str | None = None
    # set on the flattening order sent when one leg of a pair fails
    is_unwind: bool = False


class Fill(BaseModel):
    """
    One execution against one price level, reported back by an executor.

    Fees are per fill rather than per order because both venues price fees
    as a non-linear function of the fill price (see kalshi_fees.py), so a
    blended average price gives a different — wrong — number.
    """
    order_id: str
    venue: str
    market_id: str
    outcome: Outcome
    side: OrderSide
    size: Decimal
    price: Decimal
    fee: Decimal
    timestamp: datetime = Field(default_factory=utcnow)
    pair_id: str | None = None
    strategy_id: str | None = None

    @property
    def cash_flow(self) -> Decimal:
        """
        Signed effect on cash. Negative when buying (money leaves, fees add
        to the bill), positive when selling (money arrives, fees come out of
        the proceeds).
        """
        notional = self.size * self.price
        return -(notional + self.fee) if self.side == OrderSide.BUY else (notional - self.fee)


class OrderState(BaseModel):
    """
    An executor's current view of one order: the aggregate of its fills plus
    its status. Returned by get_status and carried on OrderResult.
    """
    order: Order
    status: OrderStatus = OrderStatus.PENDING
    filled_size: Decimal = ZERO
    fees: Decimal = ZERO
    fills: list[Fill] = Field(default_factory=list)
    reject_reason: str | None = None
    updated_at: datetime = Field(default_factory=utcnow)

    @property
    def remaining(self) -> Decimal:
        return self.order.size - self.filled_size

    @property
    def gross(self) -> Decimal:
        """Notional traded, before fees."""
        return sum((f.size * f.price for f in self.fills), ZERO)

    @property
    def avg_price(self) -> Decimal | None:
        """
        Size-weighted average fill price, fees excluded. None if nothing
        filled. Fees are reported separately rather than folded in here, so
        that this stays comparable with a book price.
        """
        return (self.gross / self.filled_size) if self.filled_size > 0 else None

    @property
    def net_cash(self) -> Decimal:
        return sum((f.cash_flow for f in self.fills), ZERO)


class RiskDecision(BaseModel):
    """
    The PortfolioManager's verdict on one order.

    `max_size` lets the risk layer shrink an order instead of only
    approving or vetoing it, which is the common real case: the edge is
    genuine but the position limit allows less than the strategy asked for.
    An approved decision with max_size < order.size means "trade this much".
    """
    approved: bool
    reason: str = ""
    max_size: Decimal | None = None

    @classmethod
    def ok(cls, max_size: Decimal | None = None) -> "RiskDecision":
        return cls(approved=True, max_size=max_size)

    @classmethod
    def veto(cls, reason: str) -> "RiskDecision":
        return cls(approved=False, reason=reason)
