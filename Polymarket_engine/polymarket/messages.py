"""
polymarket/messages.py

Pydantic models for the raw messages on Polymarket's public market channel
(wss://ws-subscriptions-clob.polymarket.com/ws/market), plus parse_frame(),
which turns one WebSocket text frame into a list of typed events.

These models describe Polymarket's wire format only. adapter.py converts
them into the exchange-neutral contracts in models.py.

Wire-format facts (checked against a live capture on 2026-10-04):
  - A frame is either one JSON object or a JSON array of objects. The
    initial snapshot arrives as an array with one "book" per subscribed token.
  - "book" levels are NOT best-first: bids come ascending (best last) and
    asks descending (best last). OrderBook sorts them, so order doesn't matter.
  - "price_change" carries a list of changes; each one has its own asset_id,
    and `size` is the NEW total size at that price (0 = level removed), not
    an increment. A single trade usually produces one change for the YES
    token and a mirrored change for the NO token in the same message.
  - Prices are decimal strings with a tick size of 0.01 or 0.001.
  - Timestamps are milliseconds since epoch, sent as strings.
"""

import json
import logging
from decimal import Decimal
from typing import Annotated, Literal, Union

from pydantic import (
    AliasChoices,
    BaseModel,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

log = logging.getLogger(__name__)


class PolyLevel(BaseModel):
    price: Decimal
    size: Decimal


class BookMsg(BaseModel):
    """Full snapshot of one token's book. Replaces any existing state."""
    event_type: Literal["book"]
    asset_id: str
    market: str
    timestamp: int
    # older docs call the sides "buys"/"sells"
    bids: list[PolyLevel] = Field(validation_alias=AliasChoices("bids", "buys"))
    asks: list[PolyLevel] = Field(validation_alias=AliasChoices("asks", "sells"))
    hash: str | None = None
    tick_size: Decimal | None = None


class PriceChange(BaseModel):
    asset_id: str
    price: Decimal
    size: Decimal                       # new total at this price; 0 = remove level
    side: Literal["BUY", "SELL"]        # BUY = bid side, SELL = ask side
    hash: str | None = None
    # the exchange's own top of book after this change; useful as a cross-check
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None

    @field_validator("best_bid", "best_ask", mode="before")
    @classmethod
    def _empty_to_none(cls, v):
        # an empty side may be reported as "" rather than omitted
        return None if v == "" else v


class PriceChangeMsg(BaseModel):
    event_type: Literal["price_change"]
    market: str
    timestamp: int
    price_changes: list[PriceChange]

    @model_validator(mode="before")
    @classmethod
    def _upgrade_legacy_format(cls, data):
        # Older format: a single top-level asset_id plus a "changes" list.
        # Rewrite it into the current shape so the rest of the code sees one format.
        if isinstance(data, dict) and "price_changes" not in data and "changes" in data:
            asset_id = data.get("asset_id")
            data = {**data, "price_changes": [{"asset_id": asset_id, **c} for c in data["changes"]]}
        return data


class TickSizeChangeMsg(BaseModel):
    event_type: Literal["tick_size_change"]
    asset_id: str
    market: str
    old_tick_size: Decimal
    new_tick_size: Decimal
    timestamp: int


class LastTradePriceMsg(BaseModel):
    event_type: Literal["last_trade_price"]
    asset_id: str
    market: str
    price: Decimal
    size: Decimal
    side: Literal["BUY", "SELL"]
    timestamp: int
    fee_rate_bps: Decimal | None = None


PolymarketEvent = Annotated[
    Union[BookMsg, PriceChangeMsg, TickSizeChangeMsg, LastTradePriceMsg],
    Field(discriminator="event_type"),
]
_event_adapter: TypeAdapter[PolymarketEvent] = TypeAdapter(PolymarketEvent)

KNOWN_EVENT_TYPES = frozenset({"book", "price_change", "tick_size_change", "last_trade_price"})


def parse_frame(text: str) -> list[PolymarketEvent]:
    """
    Parse one WebSocket text frame. Never raises on bad input: malformed
    JSON, unknown event types and messages that fail validation are logged
    and skipped, so one odd message can't kill the feed.
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        log.warning("non-JSON frame ignored: %.200s", text)
        return []

    items = payload if isinstance(payload, list) else [payload]
    events: list[PolymarketEvent] = []
    for item in items:
        event_type = item.get("event_type") if isinstance(item, dict) else None
        if event_type not in KNOWN_EVENT_TYPES:
            log.debug("ignoring event_type=%r", event_type)
            continue
        try:
            events.append(_event_adapter.validate_python(item))
        except ValidationError as exc:
            log.warning("invalid %s message ignored: %s", event_type, exc)
    return events
