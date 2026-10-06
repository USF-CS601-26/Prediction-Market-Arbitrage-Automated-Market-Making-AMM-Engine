"""
executable_spread.py

Turns an OrderBook into what a trade would ACTUALLY cost: walk real
depth, charge real fees, report the size you could genuinely get.

This is deliberately venue-neutral. It takes an OrderBook and a fee
function, so Kalshi (kalshi_fees.taker_fee) and Polymarket plug into the
same code path and produce comparable ExecutionQuotes. The cross-venue
detector then diffs two quotes rather than two raw prices.

Why not just compare best bid/ask: top-of-book is a lie at size. A 2c
gross edge on 1000 contracts can evaporate because (a) only 150
contracts rest at the best price and the rest fill worse, and (b) fees
are quadratic in price and charged per fill. Both effects are priced in
here.

Fee rounding note: Kalshi rounds each fee UP to the next cent. Where a
fill spans several price levels it is not documented whether the
exchange rounds per level or once per order, so this rounds PER LEVEL —
the more conservative reading. That can overstate cost by a cent or so
per level, which biases the detector toward missing marginal
opportunities rather than inventing ones. For an arbitrage system that
is the correct direction to be wrong in.
"""

from decimal import Decimal
from typing import Callable, Iterable

from models import ExecutionQuote, Fill, Side
from order_book import OrderBook

# (contracts, price) -> fee in dollars
FeeFn = Callable[[Decimal, Decimal], Decimal]

ZERO = Decimal("0")


def _no_fee(contracts: Decimal, price: Decimal) -> Decimal:
    return ZERO


def _walk(levels: Iterable[tuple[Decimal, Decimal]], contracts: Decimal,
          fee_fn: FeeFn) -> tuple[list[Fill], Decimal, Decimal, Decimal]:
    """
    Consumes price levels best-first until `contracts` is filled or depth
    runs out. Returns (fills, filled, gross, fees).
    """
    fills: list[Fill] = []
    filled = gross = fees = ZERO

    for price, size in levels:
        if filled >= contracts:
            break
        take = min(size, contracts - filled)
        if take <= 0:
            continue
        fee = fee_fn(take, price)
        fills.append(Fill(price=price, size=take, fee=fee))
        filled += take
        gross += price * take
        fees += fee

    return fills, filled, gross, fees


def quote_buy(book: OrderBook, contracts: Decimal,
              fee_fn: FeeFn = _no_fee) -> ExecutionQuote:
    """
    Cost to BUY `contracts` by lifting asks, cheapest first.
    net = gross + fees, because fees add to what you pay.
    """
    levels = book.asks.items()            # SortedDict: ascending = best ask first
    fills, filled, gross, fees = _walk(levels, contracts, fee_fn)
    net = gross + fees
    return ExecutionQuote(
        market_id=book.market_id, exchange=book.exchange, side=Side.ASK,
        requested=contracts, filled=filled, fully_filled=filled >= contracts,
        gross=gross, fees=fees, net=net,
        avg_price=(net / filled) if filled > 0 else None,
        fills=fills,
    )


def quote_sell(book: OrderBook, contracts: Decimal,
               fee_fn: FeeFn = _no_fee) -> ExecutionQuote:
    """
    Proceeds from SELLING `contracts` into bids, highest first.
    net = gross - fees, because fees reduce what you receive.
    """
    levels = reversed(book.bids.items())  # descending = best bid first
    fills, filled, gross, fees = _walk(levels, contracts, fee_fn)
    net = gross - fees
    return ExecutionQuote(
        market_id=book.market_id, exchange=book.exchange, side=Side.BID,
        requested=contracts, filled=filled, fully_filled=filled >= contracts,
        gross=gross, fees=fees, net=net,
        avg_price=(net / filled) if filled > 0 else None,
        fills=fills,
    )


def round_trip_edge(book: OrderBook, contracts: Decimal,
                    fee_fn: FeeFn = _no_fee) -> Decimal | None:
    """
    Net result of buying and immediately selling `contracts` on this one
    venue. Always negative on a sane book — you pay the spread twice
    plus fees — so it is the cost floor any cross-venue edge has to beat
    on this leg. Returns None if either side lacks the depth.
    """
    buy, sell = quote_buy(book, contracts, fee_fn), quote_sell(book, contracts, fee_fn)
    if not (buy.fully_filled and sell.fully_filled):
        return None
    return sell.net - buy.net


def describe(q: ExecutionQuote, label: str) -> str:
    if q.filled == 0:
        return f"  {label}: NO DEPTH"
    fill_note = "" if q.fully_filled else f"  PARTIAL ({q.filled}/{q.requested})"
    return (f"  {label}: {q.filled} contracts across {len(q.fills)} level(s)"
            f"{fill_note}\n"
            f"      gross ${q.gross:.4f}  fees ${q.fees:.2f}  net ${q.net:.4f}"
            f"  avg {q.avg_price:.4f}/contract")


if __name__ == "__main__":
    import argparse
    from kalshi_fees import fetch_fee_schedule, taker_fee
    from kalshi_rest_feed import fetch_orderbook, snapshot_from_rest

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("ticker")
    p.add_argument("--size", type=Decimal, default=Decimal("100"))
    a = p.parse_args()

    book = OrderBook(market_id=a.ticker, exchange="kalshi")
    book.apply_snapshot(snapshot_from_rest(a.ticker, fetch_orderbook(a.ticker)))
    sched = fetch_fee_schedule(a.ticker)
    fee_fn = lambda c, pr: taker_fee(c, pr, sched)

    bid, ask = book.get_top_of_book()
    print(f"{a.ticker}   fees: {sched.fee_type} x{sched.multiplier}")
    print(f"  top of book: bid {bid.price} x {bid.size} | ask {ask.price} x {ask.size}"
          f"   gross spread {ask.price - bid.price}\n")
    for size in (Decimal("10"), a.size, Decimal("5000")):
        b = quote_buy(book, size, fee_fn)
        s = quote_sell(book, size, fee_fn)
        edge = round_trip_edge(book, size, fee_fn)
        print(f"--- size {size} ---")
        print(describe(b, "BUY "))
        print(describe(s, "SELL"))
        print(f"      round-trip edge: "
              f"{'insufficient depth' if edge is None else f'${edge:.4f}'}\n")
