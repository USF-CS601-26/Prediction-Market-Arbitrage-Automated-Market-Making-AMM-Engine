"""
kalshi_fees.py

Kalshi's taker/maker fee schedule — the venue-specific half of the
executable spread engine. The venue-neutral depth walking lives in
executable_spread.py so the same machinery can take Polymarket's
schedule.

Kalshi's fee is QUADRATIC in price, not a flat basis-point charge:

    fee = ceil_to_cent( rate * multiplier * C * P * (1 - P) )

where C is the contract count and P the price in dollars. The P*(1-P)
term peaks at P = 0.50 (where it equals 0.25) and falls to zero at both
ends, so the SAME gross spread is far more expensive to capture near 50c
than near 5c or 95c. Any cross-venue detector that models fees as a flat
percentage will systematically invent arbitrage near the middle of the
book and miss it at the edges.

Rates are fixed by the exchange, but each SERIES carries its own
fee_type and fee_multiplier, which the API exposes. Surveying ~9200
live series turns up real variation:

    quadratic                  x1.0   9021 series
    quadratic_with_maker_fees  x1.0    151 series
    quadratic                  x0.5     18 series
    quadratic                  x0.0      9 series   <- no fees at all

So the schedule is fetched per series rather than hardcoded. Assuming
0.07 everywhere would misprice ~180 series, including nine where the
true net spread is strictly better than the model thinks.
"""

from decimal import Decimal, ROUND_CEILING
import json
import urllib.request

from models import FeeSchedule

PROD_HOST = "https://api.elections.kalshi.com"

# Exchange-wide rates. The series-level fee_multiplier scales these.
TAKER_RATE = Decimal("0.07")
MAKER_RATE = Decimal("0.0175")

# fee_type values that charge a maker fee on resting orders. Everything
# else charges takers only, and resting liquidity is free.
MAKER_FEE_TYPES = {"quadratic_with_maker_fees"}

_CACHE: dict[str, FeeSchedule] = {}


def series_ticker(market_ticker: str) -> str:
    """"KXLEADERSOUT-27JAN01-BNETISR" -> "KXLEADERSOUT"."""
    return market_ticker.split("-")[0]


def fetch_fee_schedule(market_ticker: str) -> FeeSchedule:
    """
    Looks up the fee schedule for a market's series. Public endpoint, no
    credentials. Cached per series, since the schedule is static config
    and a detector will ask for it on every book update.
    """
    series = series_ticker(market_ticker)
    if series in _CACHE:
        return _CACHE[series]

    url = f"{PROD_HOST}/trade-api/v2/series/{series}"
    req = urllib.request.Request(url, headers={"User-Agent": "kalshi-engine/0.1"})
    with urllib.request.urlopen(req, timeout=20) as r:
        s = json.load(r)["series"]

    schedule = FeeSchedule(
        venue="kalshi",
        series=series,
        fee_type=s.get("fee_type", "quadratic"),
        multiplier=Decimal(str(s.get("fee_multiplier", 1))),
    )
    _CACHE[series] = schedule
    return schedule


def _quadratic(rate: Decimal, schedule: FeeSchedule,
               contracts: Decimal, price: Decimal) -> Decimal:
    """
    ceil_to_cent(rate * multiplier * C * P * (1 - P)).

    Kalshi rounds fees UP to the next cent, so a fee is never zero on a
    nonzero charge — which is why tiny fills are disproportionately
    expensive and why an arb that looks good on 1 contract can die on
    rounding alone.
    """
    if contracts <= 0 or schedule.multiplier == 0:
        return Decimal("0.00")
    raw = rate * schedule.multiplier * contracts * price * (Decimal("1") - price)
    return raw.quantize(Decimal("0.01"), rounding=ROUND_CEILING)


def taker_fee(contracts: Decimal, price: Decimal,
              schedule: FeeSchedule) -> Decimal:
    """Fee for crossing the spread (removing liquidity)."""
    return _quadratic(TAKER_RATE, schedule, contracts, price)


def maker_fee(contracts: Decimal, price: Decimal,
              schedule: FeeSchedule) -> Decimal:
    """
    Fee for resting an order that later fills. Zero on most series —
    only those with fee_type "quadratic_with_maker_fees" charge it, and
    a resting order that is cancelled before filling is never charged.
    """
    if schedule.fee_type not in MAKER_FEE_TYPES:
        return Decimal("0.00")
    return _quadratic(MAKER_RATE, schedule, contracts, price)


if __name__ == "__main__":
    import sys
    t = sys.argv[1] if len(sys.argv) > 1 else "KXLEADERSOUT-27JAN01-BNETISR"
    sched = fetch_fee_schedule(t)
    print(f"{t}\n  {sched.fee_type} x{sched.multiplier} "
          f"(maker fees: {'yes' if sched.fee_type in MAKER_FEE_TYPES else 'no'})\n")
    print(f"  taker fee on 100 contracts, by price:")
    for p in ("0.05", "0.25", "0.50", "0.75", "0.95"):
        f = taker_fee(Decimal("100"), Decimal(p), sched)
        print(f"    P={p}  fee=${f:>5}   ({f / Decimal('100') * 100:.2f}c per contract)")
