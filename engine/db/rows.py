"""
engine/db/rows.py

Domain object -> row dict, one function per table. Pure, so the mapping is
tested without a database and both drivers receive identical rows.

A row is built at the moment it is recorded, never later: an OrderBook keeps
being mutated by its feed, so a snapshot that held a reference to the book
instead of a copy of its levels would record whatever the book looked like
when the batch writer got round to it.

Every builder returns every column of its table, so a batch is always
uniform — executemany requires that.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

ZERO = Decimal("0")


def utc(ts: datetime | None) -> datetime:
    if ts is None:
        return datetime.now(tz=timezone.utc)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _s(value) -> str | None:
    return None if value is None else str(value)


# ---- trade_log ----

def fill_row(run_id: str, fill) -> dict:
    return {
        "run_id": run_id, "ts": utc(fill.timestamp), "kind": "fill",
        "order_id": fill.order_id, "venue": fill.venue, "market_id": fill.market_id,
        "outcome": fill.outcome, "side": fill.side.value, "size": fill.size,
        "price": fill.price, "fee": fill.fee, "cash_flow": fill.cash_flow,
        "status": None, "filled_size": None, "avg_fill_price": None, "reason": None,
        "strategy_id": fill.strategy_id, "pair_id": fill.pair_id, "is_unwind": False,
    }


def order_row(run_id: str, state, ts: datetime | None = None) -> dict:
    """An order's state as of `ts` (normally its final state after reconcile)."""
    o = state.order
    return {
        "run_id": run_id, "ts": utc(ts or state.updated_at), "kind": "order",
        "order_id": o.order_id, "venue": o.venue, "market_id": o.market_id,
        "outcome": o.outcome, "side": o.side.value, "size": o.size,
        "price": o.limit_price, "fee": state.fees, "cash_flow": None,
        "status": state.status.value, "filled_size": state.filled_size,
        "avg_fill_price": state.avg_price, "reason": state.reject_reason,
        "strategy_id": o.strategy_id, "pair_id": o.pair_id, "is_unwind": o.is_unwind,
    }


# ---- market_snapshots ----

def snapshot_row(run_id: str, ts: datetime, venue: str, market_id: str, outcome: str,
                 book, depth: int = 10, reason: str = "periodic") -> dict:
    bids = list(reversed(book.bids.items()))[:depth]      # best (highest) first
    asks = list(book.asks.items())[:depth]                # best (lowest) first
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    mid = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
    return {
        "run_id": run_id, "ts": utc(ts), "venue": venue, "market_id": market_id,
        "outcome": outcome, "sequence": getattr(book, "last_sequence", None),
        "best_bid": best_bid, "best_ask": best_ask, "mid": mid,
        "bid_depth": sum((s for _, s in bids), ZERO),
        "ask_depth": sum((s for _, s in asks), ZERO),
        "bids": [[str(p), str(s)] for p, s in bids],
        "asks": [[str(p), str(s)] for p, s in asks],
        "reason": reason,
    }


# ---- spread_history ----

def _top(book) -> tuple[Decimal | None, Decimal | None]:
    bid, ask = book.get_top_of_book()
    return (bid.price if bid else None), (ask.price if ask else None)


def tops_of(quotes: dict) -> dict:
    """{venue: {yes_bid, yes_ask, no_bid, no_ask}} as strings, from VenueQuotes."""
    out = {}
    for venue, vq in sorted(quotes.items()):
        yes_bid, yes_ask = _top(vq.yes.book)
        no_bid, no_ask = _top(vq.no.book)
        out[venue] = {"yes_bid": _s(yes_bid), "yes_ask": _s(yes_ask),
                      "no_bid": _s(no_bid), "no_ask": _s(no_ask)}
    return out


def spread_row(run_id: str, ts: datetime, pair_id: str, quotes: dict, opportunities,
               threshold: Decimal | None, volatility: Decimal | None) -> dict:
    best = opportunities[0] if opportunities else None
    return {
        "run_id": run_id, "ts": utc(ts), "pair_id": pair_id, "tops": tops_of(quotes),
        "direction": best.direction if best else None,
        "top_of_book_cost": best.top_of_book_cost if best else None,
        "top_of_book_edge": best.top_of_book_edge if best else None,
        "executable_size": best.size if best else None,
        "net_profit": best.net_profit if best else None,
        "net_per_pair": best.net_per_pair if best else None,
        "threshold": threshold, "volatility": volatility,
    }


# ---- sentiment_features ----

def sentiment_row(run_id: str, ts: datetime, pair_id: str, level: Decimal,
                  volatility: Decimal, confidence: Decimal, n_points: int,
                  latest=None, threshold: Decimal | None = None) -> dict:
    return {
        "run_id": run_id, "ts": utc(ts), "pair_id": pair_id, "level": level,
        "volatility": volatility, "confidence": confidence, "n_points": n_points,
        "latest_score": latest.score if latest else None,
        "latest_headline": latest.headline if latest else None,
        "latest_source": latest.source if latest else None,
        "threshold": threshold,
    }


# ---- arbitrage ----

def opportunity_row(run_id: str, opportunity, threshold: Decimal | None,
                    volatility: Decimal | None) -> dict:
    o = opportunity
    return {
        "run_id": run_id, "ts": utc(o.timestamp), "pair_id": o.pair_id,
        "direction": o.direction, "size": o.size, "yes_avg_price": o.yes_avg_price,
        "no_avg_price": o.no_avg_price, "cost_per_pair": o.cost_per_pair, "fees": o.fees,
        "net_profit": o.net_profit,
        "threshold": threshold if threshold is not None else o.threshold,
        "volatility": volatility, "top_of_book_cost": o.top_of_book_cost,
    }


def attempt_row(run_id: str, attempt) -> dict:
    a = attempt
    return {
        "run_id": run_id, "ts": utc(a.opportunity.timestamp), "pair_id": a.opportunity.pair_id,
        "direction": a.opportunity.direction, "dispatched": bool(a.dispatched),
        "reason": a.reason, "requested_size": a.opportunity.size,
        "matched_pairs": a.matched_pairs, "unwound_size": a.unwound_size,
        "naked_size": a.naked_size, "cash_spent": a.cash_spent,
        "locked_profit": a.locked_profit,
    }
