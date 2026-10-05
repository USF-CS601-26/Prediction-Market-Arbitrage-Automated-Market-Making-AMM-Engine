"""
cross_venue_spread.py

The cross-venue half of the detection engine: given the same outcome
trading on two venues, decide what a round trip is actually worth after
depth and fees.

Both venues' feeds normalize into the shared models in models.py, so an
OrderBook is an OrderBook regardless of where it came from. That makes
this module venue-agnostic — it takes two books and two fee functions,
and never needs to know which exchange produced either.

Direction matters. Buying on Kalshi and selling on Polymarket is a
different trade from the reverse, with different fills and different
fees, so both are evaluated and the better one reported.

What this does NOT model (and a real system would):
  - settlement and capital costs; the legs settle on different rails
    and on different timelines, so a positive net_edge is gross of
    carry, not free money
  - resolution risk, i.e. the two contracts disagreeing on an edge case.
    That is what the manual pair verification in config/pair.toml is
    for, and why criteria_match has to be confirmed before trusting any
    number this module prints.
"""

from decimal import Decimal

from models import CrossVenueEdge, ExecutionQuote
from order_book import OrderBook
from executable_spread import quote_buy, quote_sell, FeeFn, _no_fee

ZERO = Decimal("0")


def edge_for_direction(pair_id: str, size: Decimal,
                       buy_book: OrderBook, buy_fee: FeeFn,
                       sell_book: OrderBook, sell_fee: FeeFn) -> CrossVenueEdge:
    """
    Buy `size` on buy_book, sell the same size on sell_book.

    Both legs are quoted independently against real depth. The trade is
    only `executable` if BOTH fully fill — a one-legged arbitrage is an
    outright position, not an arbitrage, and reporting it as an
    opportunity is how a detector talks you into directional risk.
    """
    buy = quote_buy(buy_book, size, buy_fee)
    sell = quote_sell(sell_book, size, sell_fee)

    net_edge = sell.net - buy.net
    executable = buy.fully_filled and sell.fully_filled
    filled = min(buy.filled, sell.filled)

    return CrossVenueEdge(
        pair_id=pair_id, size=size,
        buy_venue=buy_book.exchange, sell_venue=sell_book.exchange,
        buy=buy, sell=sell,
        gross_edge=sell.gross - buy.gross,
        total_fees=buy.fees + sell.fees,
        net_edge=net_edge,
        net_per_contract=(net_edge / filled) if filled > 0 else None,
        executable=executable,
    )


def best_edge(pair_id: str, size: Decimal,
              book_a: OrderBook, fee_a: FeeFn,
              book_b: OrderBook, fee_b: FeeFn) -> list[CrossVenueEdge]:
    """
    Evaluates both directions and returns them sorted best-first.
    Callers normally want [0], but both are returned so a logger can
    record the road not taken.
    """
    directions = [
        edge_for_direction(pair_id, size, book_a, fee_a, book_b, fee_b),
        edge_for_direction(pair_id, size, book_b, fee_b, book_a, fee_a),
    ]
    return sorted(directions, key=lambda e: e.net_edge, reverse=True)


def describe(e: CrossVenueEdge) -> str:
    flag = "EXECUTABLE" if e.executable else "not executable (insufficient depth)"
    sign = "+" if e.net_edge > 0 else ""
    lines = [
        f"  buy {e.buy_venue} / sell {e.sell_venue}   [{flag}]",
        f"    buy : {e.buy.filled} @ avg {e.buy.avg_price:.4f}"
        f"   gross ${e.buy.gross:.4f}  fees ${e.buy.fees:.4f}"
        if e.buy.filled else "    buy : no depth",
        f"    sell: {e.sell.filled} @ avg {e.sell.avg_price:.4f}"
        f"   gross ${e.sell.gross:.4f}  fees ${e.sell.fees:.4f}"
        if e.sell.filled else "    sell: no depth",
        f"    gross edge ${e.gross_edge:.4f}   fees ${e.total_fees:.4f}"
        f"   NET {sign}${e.net_edge:.4f}"
        + (f"  ({sign}{e.net_per_contract * 100:.2f}c/contract)"
           if e.net_per_contract is not None else ""),
    ]
    return "\n".join(lines)
