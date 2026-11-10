"""
polymarket/gamma.py

Looks up a market on Polymarket's Gamma API (the read-only market catalogue)
and prints a [polymarket] section ready to paste into config/pair.toml,
followed by the market's rules text for the manual verification step.

Usage:
    python -m polymarket.gamma <market-slug>

Note that `outcomes` and `clobTokenIds` come back as JSON-encoded strings,
not lists, so each needs a second json.loads. The two lists are index-aligned:
outcomes[i] is the outcome for clobTokenIds[i].
"""

import json
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen

GAMMA_URL = "https://gamma-api.polymarket.com"


def fetch_market(slug: str) -> dict:
    url = f"{GAMMA_URL}/markets?{urlencode({'slug': slug})}"
    req = Request(url, headers={"User-Agent": "cs601-arb-engine"})
    with urlopen(req, timeout=15) as resp:
        markets = json.load(resp)
    if not markets:
        raise LookupError(
            f"no market with slug {slug!r}; if this is an event slug, "
            f"try {GAMMA_URL}/events?slug={slug} and pick a market from it"
        )
    return markets[0]


def token_ids_by_outcome(market: dict) -> dict[str, str]:
    outcomes = json.loads(market["outcomes"])
    token_ids = json.loads(market["clobTokenIds"])
    return dict(zip(outcomes, token_ids))


def to_toml_section(slug: str, market: dict) -> str:
    tokens = token_ids_by_outcome(market)
    fees = market.get("feeSchedule") or {}
    return "\n".join([
        "[polymarket]",
        f'slug = "{slug}"',
        f'condition_id = "{market["conditionId"]}"',
        f'yes_token = "{tokens["Yes"]}"',
        f'no_token = "{tokens["No"]}"',
        f'tick_size = "{market["orderPriceMinTickSize"]}"',
        "",
        "[polymarket.fees]",
        f'enabled = {str(bool(market.get("feesEnabled"))).lower()}',
        f'rate = "{fees.get("rate", 0)}"',
        f'exponent = "{fees.get("exponent", 1)}"',
    ])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python -m polymarket.gamma <market-slug>")
    slug = sys.argv[1]
    m = fetch_market(slug)
    print(to_toml_section(slug, m))
    print()
    print(f"# question : {m.get('question')}")
    print(f"# endDate  : {m.get('endDate')}")
    print(f"# active={m.get('active')} closed={m.get('closed')} "
          f"enableOrderBook={m.get('enableOrderBook')}")
    print(f"# bestBid={m.get('bestBid')} bestAsk={m.get('bestAsk')} spread={m.get('spread')}")
    print(f"# feesEnabled={m.get('feesEnabled')} feeType={m.get('feeType')} feeSchedule={m.get('feeSchedule')}")
    print("# rules:")
    for line in (m.get("description") or "").splitlines():
        print(f"#   {line}")
