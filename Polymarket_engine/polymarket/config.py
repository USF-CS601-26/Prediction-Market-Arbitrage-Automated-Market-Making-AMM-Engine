"""
polymarket/config.py

Loads the hardcoded test pair from a TOML file into a validated pydantic
model. TOML is used because tomllib ships with Python 3.11+, so no extra
dependency is needed.

Prices and fees are written as strings in the TOML file so they are parsed
straight into Decimal and never pass through float.
"""

import tomllib
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

Outcome = Literal["YES", "NO"]


class FeeSchedule(BaseModel):
    """
    A market's taker fee parameters, copied from Gamma's `feesEnabled` and
    `feeSchedule` fields. Used by polymarket/fees.py.
    """
    enabled: bool                       # Gamma feesEnabled
    rate: Decimal = Decimal(0)          # feeSchedule.rate, e.g. 0.04 for politics
    exponent: Decimal = Decimal(1)      # feeSchedule.exponent; 1 in every market seen so far


class PolymarketMarketConfig(BaseModel):
    slug: str
    condition_id: str           # the market id; WebSocket messages carry it as "market"
    yes_token: str              # CLOB token ids are ~77-digit integers: keep them as str
    no_token: str
    tick_size: Decimal
    fees: FeeSchedule           # required on purpose: a missing fee config must not silently mean "free"


class KalshiMarketConfig(BaseModel):
    ticker: str = ""


class VerificationConfig(BaseModel):
    """Written record of the manual check that both contracts resolve identically."""
    verified_by: list[str] = Field(default_factory=list)
    verified_on: str | None = None
    criteria_match: bool = False
    notes: list[str] = Field(default_factory=list)


class PairConfig(BaseModel):
    pair_id: str
    polymarket: PolymarketMarketConfig
    kalshi: KalshiMarketConfig = Field(default_factory=KalshiMarketConfig)
    verification: VerificationConfig = Field(default_factory=VerificationConfig)

    def asset_ids(self) -> list[str]:
        """Token ids to subscribe to: one order book per outcome."""
        return [self.polymarket.yes_token, self.polymarket.no_token]

    def outcome_of(self, asset_id: str) -> Outcome:
        if asset_id == self.polymarket.yes_token:
            return "YES"
        if asset_id == self.polymarket.no_token:
            return "NO"
        raise KeyError(f"asset_id {asset_id} is not part of pair {self.pair_id}")


def load_pair_config(path: str | Path) -> PairConfig:
    with open(path, "rb") as f:
        return PairConfig.model_validate(tomllib.load(f))
