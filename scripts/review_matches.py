#!/usr/bin/env python3
"""Permanent debugging and market pairing inspection tool.

Fetches current active/open markets from Kalshi and Polymarket (or uses offline sample
markets if network/credentials are unavailable), runs the `MarketMatcher` pipeline,
and prints a formatted breakdown of manual overrides and fuzzy candidate matches.

Usage:
    python scripts/review_matches.py
    python scripts/review_matches.py --min-similarity 0.80 --limit 50
    python scripts/review_matches.py --sample
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
import sys
from typing import Any

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.config_loader import load_config
from core.market_matcher import MarketMatcher, load_market_overrides
from kalshi_client.models import Market as KalshiMarket
from kalshi_client.rest import KalshiClient
from polymarket_client.models import Market as PolyMarket
from polymarket_client.rest import PolymarketClient

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger("review_matches")


# ============================================================================
# Offline Sample Datasets for Demo / Testing / Offline Mode
# ============================================================================

SAMPLE_KALSHI_MARKETS = [
    {
        "ticker": "FED-26MAY-T500",
        "title": "Will the Federal Reserve cut interest rates in May 2026?",
        "yes_bid": 44,
        "yes_ask": 46,
        "volume": 24500,
    },
    {
        "ticker": "PRES-2028-DEM",
        "title": "Democratic Party nominee for 2028 US Presidential Election",
        "yes_bid": 52,
        "yes_ask": 55,
        "volume": 120000,
    },
    {
        "ticker": "BTC-100K-2026",
        "title": "Bitcoin price reaches $100,000 before end of 2026",
        "yes_bid": 68,
        "yes_ask": 71,
        "volume": 89000,
    },
    {
        "ticker": "CPI-26APR-T30",
        "title": "US CPI annual inflation rate 3.0% or higher for April 2026",
        "yes_bid": 33,
        "yes_ask": 36,
        "volume": 15400,
    },
    {
        "ticker": "ETH-5K-2026",
        "title": "Ethereum price reaches $5,000 in 2026",
        "yes_bid": 40,
        "yes_ask": 43,
        "volume": 31000,
    },
    {
        "ticker": "KX-OSCARS-2026-PIC",
        "title": "Oscars 2026 Best Picture Winner",
        "yes_bid": 25,
        "yes_ask": 28,
        "volume": 8500,
    },
    {
        "ticker": "KX-SUPERBOWL-2027",
        "title": "Super Bowl LXI Winner in 2027",
        "yes_bid": 15,
        "yes_ask": 18,
        "volume": 52000,
    },
    {
        "ticker": "KX-RECESSION-2026",
        "title": "US enters economic recession in 2026",
        "yes_bid": 22,
        "yes_ask": 26,
        "volume": 41000,
    },
]

SAMPLE_POLYMARKET_MARKETS = [
    {
        "condition_id": "0x8f34a123bc901e4a77d562145b23e89fbc321456a7d8e9f0123456789abcdef0",
        "title": "Fed interest rate target cut at May 2026 FOMC meeting",
        "yes_bid": 45,
        "yes_ask": 47,
        "volume": 35000,
    },
    {
        "condition_id": "0x1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b",
        "title": "2028 Democratic Presidential Nominee",
        "yes_bid": 53,
        "yes_ask": 56,
        "volume": 240000,
    },
    {
        "condition_id": "0x3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d",
        "title": "Will Bitcoin hit $100,000 by end of 2026?",
        "yes_bid": 69,
        "yes_ask": 72,
        "volume": 150000,
    },
    {
        "condition_id": "0x5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f",
        "title": "US Annual CPI Inflation >= 3.0% in April 2026?",
        "yes_bid": 34,
        "yes_ask": 37,
        "volume": 22000,
    },
    {
        "condition_id": "0x7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b",
        "title": "Will Ethereum reach $5,000 in 2026?",
        "yes_bid": 41,
        "yes_ask": 44,
        "volume": 68000,
    },
    {
        "condition_id": "0x9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d",
        "title": "2026 Academy Awards: Best Picture Winner",
        "yes_bid": 26,
        "yes_ask": 29,
        "volume": 14000,
    },
    {
        "condition_id": "0xb1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2",
        "title": "Super Bowl LXI Champion 2027",
        "yes_bid": 16,
        "yes_ask": 19,
        "volume": 84000,
    },
    {
        "condition_id": "0xd3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4",
        "title": "US Economic Recession in 2026",
        "yes_bid": 23,
        "yes_ask": 27,
        "volume": 53000,
    },
]


# ============================================================================
# Live Market Fetching
# ============================================================================


async def fetch_live_markets(
    limit: int = 100,
) -> tuple[list[Any], list[Any], str]:
    """Fetch live market lists from Kalshi and Polymarket REST APIs.

    Falls back to sample dataset if APIs are unreachable or offline.
    """
    config = load_config()
    kalshi_markets: list[Any] = []
    poly_markets: list[Any] = []
    source_desc = "live"

    try:
        async with PolymarketClient.from_config(config) as poly_client:
            poly_markets = await poly_client.list_markets(status="open", limit=limit, max_pages=1)
    except Exception as e:
        logger.debug("Could not fetch live Polymarket markets: %s", e)

    try:
        async with KalshiClient.from_config(config) as kalshi_client:
            kalshi_markets = await kalshi_client.list_markets(status="open", limit=limit, max_pages=1)
    except Exception as e:
        logger.debug("Could not fetch live Kalshi markets: %s", e)

    if not kalshi_markets or not poly_markets:
        source_desc = "sample (live APIs unavailable/offline)"
        kalshi_markets = [KalshiMarket.model_validate(m) for m in SAMPLE_KALSHI_MARKETS]
        poly_markets = [PolyMarket.model_validate(m) for m in SAMPLE_POLYMARKET_MARKETS]

    return kalshi_markets, poly_markets, source_desc


# ============================================================================
# Formatting & Presentation
# ============================================================================


def print_matches_report(
    matches: list[Any],
    k_count: int,
    p_count: int,
    min_similarity: float,
    overrides_path: Path,
    source_name: str,
) -> None:
    """Print a clean, structured CLI report of matched market pairs."""
    sep = "=" * 80
    subsep = "-" * 80

    print(f"\n{sep}")
    print(" 🎯 CROSS-PLATFORM PREDICTION MARKET MATCH REPORT")
    print(f"{sep}")
    print(f" Source Data:      {source_name}")
    print(f" Kalshi Markets:   {k_count}")
    print(f" Polymarket Mkt:   {p_count}")
    print(f" Min Similarity:   {min_similarity:.2f}")
    print(f" Overrides File:   {overrides_path} ({'found' if overrides_path.exists() else 'not found'})")
    print(f" Total Matched:    {len(matches)}")
    print(f"{sep}\n")

    if not matches:
        print(" [!] No candidate pairs met the matching criteria.")
        print("     Try lowering --min-similarity (e.g. --min-similarity 0.70)\n")
        return

    override_count = sum(1 for m in matches if m.is_override)
    fuzzy_count = len(matches) - override_count

    print(f" Found {len(matches)} matched market pair(s):")
    print(f"   - Manual Overrides: {override_count}")
    print(f"   - Fuzzy Matches:    {fuzzy_count}\n")
    print(subsep)

    for idx, match in enumerate(matches, 1):
        badge = "✨ OVERRIDE" if match.is_override else f"🔍 FUZZY ({match.confidence * 100:.1f}%)"
        print(f" [{idx:02d}] {badge}")
        print(f"      Kalshi:     [{match.kalshi_ticker}]")
        print(f"                  {match.kalshi_title}")
        print(f"      Polymarket: [{match.polymarket_condition_id[:18]}...]")
        print(f"                  {match.polymarket_title}")
        if match.notes:
            print(f"      Notes:      {match.notes}")
        print(subsep)

    print(f"\n{sep}")
    print(" 💡 To add a new confirmed pair to config/market_overrides.yaml:")
    print("    overrides:")
    print('      - kalshi_ticker: "<KALSHI_TICKER>"')
    print('        polymarket_condition_id: "<POLYMARKET_CONDITION_ID>"')
    print('        notes: "<Brief reason or context>"')
    print(f"{sep}\n")


# ============================================================================
# CLI Entrypoint
# ============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Review candidate cross-platform prediction market matches."
    )
    parser.add_argument(
        "--min-similarity",
        type=float,
        default=None,
        help="Minimum similarity threshold (0.0 to 1.0). Defaults to config value (0.85).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max number of markets to fetch per platform (default: 100).",
    )
    parser.add_argument(
        "--overrides-file",
        type=str,
        default="config/market_overrides.yaml",
        help="Path to market overrides YAML file.",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use built-in representative sample dataset instead of querying APIs.",
    )

    args = parser.parse_args()

    config = load_config()
    min_similarity = (
        args.min_similarity
        if args.min_similarity is not None
        else config.matching.min_similarity
    )
    overrides_path = Path(args.overrides_file)

    if args.sample:
        k_markets = [KalshiMarket.model_validate(m) for m in SAMPLE_KALSHI_MARKETS]
        p_markets = [PolyMarket.model_validate(m) for m in SAMPLE_POLYMARKET_MARKETS]
        source_name = "offline sample dataset"
    else:
        k_markets, p_markets, source_name = asyncio.run(fetch_live_markets(limit=args.limit))

    matcher = MarketMatcher(
        min_similarity=min_similarity,
        overrides_path=overrides_path,
    )

    matches = matcher.match(k_markets, p_markets)

    print_matches_report(
        matches=matches,
        k_count=len(k_markets),
        p_count=len(p_markets),
        min_similarity=min_similarity,
        overrides_path=overrides_path,
        source_name=source_name,
    )


if __name__ == "__main__":
    main()
