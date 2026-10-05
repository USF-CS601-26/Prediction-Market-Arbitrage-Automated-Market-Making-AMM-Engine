"""
polymarket/fees.py

Polymarket taker fee for one fill, in USDC, per
https://docs.polymarket.com/trading/fees (checked 2026-10-05):

    fee = C * feeRate * p * (1 - p)

    C        shares filled
    p        fill price, 0 < p < 1
    feeRate  the market's fee rate (FeeSchedule.rate), e.g. 0.04 for politics

Only takers pay; makers are never charged. An arbitrage leg always takes
liquidity (it buys at the best ask), so this is the fee the spread engine needs.

The fee is largest at p = 0.50 and symmetric around it: a fill at 0.30 pays
the same as one at 0.70. It is rounded to 5 decimal places, so a fee below
0.00001 USDC is zero. The docs don't name the rounding mode; we use ordinary
half-up rounding, which can differ from the exchange by at most 0.00001 USDC
per fill.

Gamma's feeSchedule also has an `exponent`, documented only as "applied to the
price component of the fee curve". We apply it as (p * (1 - p)) ** exponent;
for exponent = 1, the only value seen so far, this is exactly the formula above.

When a buy walks several price levels, each level is a separate fill: compute
the fee for each level and add them up.
"""

from decimal import ROUND_HALF_UP, Decimal

from polymarket.config import FeeSchedule

FEE_QUANTUM = Decimal("0.00001")    # fees are rounded to 5 decimal places

_ZERO = Decimal(0)
_ONE = Decimal(1)


def taker_fee(price: Decimal, shares: Decimal, fees: FeeSchedule) -> Decimal:
    """USDC fee charged to the taker for filling `shares` at `price`."""
    if not (_ZERO < price < _ONE):
        raise ValueError(f"price must be strictly between 0 and 1, got {price}")
    if shares < 0:
        raise ValueError(f"shares must not be negative, got {shares}")
    if not fees.enabled:
        return _ZERO
    raw = shares * fees.rate * (price * (_ONE - price)) ** fees.exponent
    return raw.quantize(FEE_QUANTUM, rounding=ROUND_HALF_UP)
