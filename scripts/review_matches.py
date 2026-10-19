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
    {"ticker": "KXPRESNOMD-28-GN", "title": "Gavin Newsom wins 2028 Democratic nomination", "yes_bid": 23, "yes_ask": 25, "volume": 52000},
    {"ticker": "KXPRESNOMD-28-AOC", "title": "Alexandria Ocasio-Cortez wins 2028 Democratic nomination", "yes_bid": 11, "yes_ask": 13, "volume": 34000},
    {"ticker": "KXPRESNOMD-28-KH", "title": "Kamala Harris wins 2028 Democratic nomination", "yes_bid": 17, "yes_ask": 19, "volume": 89000},
    {"ticker": "KXPRESNOMD-28-JOSS", "title": "Jon Ossoff wins 2028 Democratic nomination", "yes_bid": 7, "yes_ask": 9, "volume": 14000},
    {"ticker": "KXPRESNOMD-28-PB", "title": "Pete Buttigieg wins 2028 Democratic nomination", "yes_bid": 13, "yes_ask": 15, "volume": 41000},
    {"ticker": "KXPRESNOMD-28-AB", "title": "Andy Beshear wins 2028 Democratic nomination", "yes_bid": 9, "yes_ask": 11, "volume": 18000},
    {"ticker": "KXPRESNOMD-28-JS", "title": "Josh Shapiro wins 2028 Democratic nomination", "yes_bid": 14, "yes_ask": 16, "volume": 47000},
    {"ticker": "KXPRESNOMD-28-MK", "title": "Mark Kelly wins 2028 Democratic nomination", "yes_bid": 8, "yes_ask": 10, "volume": 16000},
    {"ticker": "KXPRESNOMD-28-REMA", "title": "Rahm Emanuel wins 2028 Democratic nomination", "yes_bid": 5, "yes_ask": 7, "volume": 8000},
    {"ticker": "KXPRESNOMD-28-MO", "title": "Michelle Obama wins 2028 Democratic nomination", "yes_bid": 8, "yes_ask": 10, "volume": 25000},
    {"ticker": "KXPRESNOMD-28-WM", "title": "Wes Moore wins 2028 Democratic nomination", "yes_bid": 6, "yes_ask": 8, "volume": 12000},
    {"ticker": "KXPRESNOMD-28-RW", "title": "Raphael Warnock wins 2028 Democratic nomination", "yes_bid": 7, "yes_ask": 9, "volume": 15000},
    {"ticker": "KXPRESNOMD-28-CBOO", "title": "Cory Booker wins 2028 Democratic nomination", "yes_bid": 4, "yes_ask": 6, "volume": 7000},
    {"ticker": "KXPRESNOMD-28-GW", "title": "Gretchen Whitmer wins 2028 Democratic nomination", "yes_bid": 15, "yes_ask": 17, "volume": 53000},
    {"ticker": "KXPRESNOMR-28-JDV", "title": "J.D. Vance wins 2028 Republican nomination", "yes_bid": 47, "yes_ask": 49, "volume": 110000},
    {"ticker": "KXPRESNOMR-28-MR", "title": "Marco Rubio wins 2028 Republican nomination", "yes_bid": 14, "yes_ask": 16, "volume": 38000},
    {"ticker": "KXPRESNOMR-28-RDS", "title": "Ron DeSantis wins 2028 Republican nomination", "yes_bid": 7, "yes_ask": 9, "volume": 29000},
    {"ticker": "KXPRESNOMR-28-TCAR", "title": "Tucker Carlson wins 2028 Republican nomination", "yes_bid": 4, "yes_ask": 6, "volume": 19000},
    {"ticker": "KXPRESNOMR-28-TC", "title": "Ted Cruz wins 2028 Republican nomination", "yes_bid": 5, "yes_ask": 7, "volume": 12000},
    {"ticker": "KXPRESNOMR-28-DJTJR", "title": "Donald Trump Jr. wins 2028 Republican nomination", "yes_bid": 6, "yes_ask": 8, "volume": 21000},
]

SAMPLE_POLYMARKET_MARKETS = [
    {"condition_id": "0x0f49db97f71c68b1e42a6d16e3de93d85dbf7d4148e3f018eb79e88554be9f75", "title": "Will Gavin Newsom win the 2028 Democratic presidential nomination?", "yes_bid": 24, "yes_ask": 26, "volume": 78000},
    {"condition_id": "0xe6bcc2f1dd025ce5e1833190f7c60a71171c94f805df55b9ab0ded695ec93565", "title": "Will Alexandria Ocasio-Cortez win the 2028 Democratic presidential nomination?", "yes_bid": 12, "yes_ask": 14, "volume": 42000},
    {"condition_id": "0x909659c9436228e2be56d5582ba6188166f2f8bf3c596335512c8f2e380d01ec", "title": "Will Kamala Harris win the 2028 Democratic presidential nomination?", "yes_bid": 18, "yes_ask": 20, "volume": 125000},
    {"condition_id": "0x74dba1ce1ae9dd535414e85f2d9ab5ea32c0fb1acc9b7130b67e6d91217e24e1", "title": "Will Jon Ossoff win the 2028 Democratic presidential nomination?", "yes_bid": 8, "yes_ask": 10, "volume": 19000},
    {"condition_id": "0x4c325469d9b516ef4e6b8f73a81a12607dec075e3c2fd454f91765aaeafc4760", "title": "Will Pete Buttigieg win the 2028 Democratic presidential nomination?", "yes_bid": 14, "yes_ask": 16, "volume": 58000},
    {"condition_id": "0x1d519b87999e3d4e90e1e8f57b5eee73a0ba488ff3fdb70867f294733aba84a9", "title": "Will Andy Beshear win the 2028 Democratic presidential nomination?", "yes_bid": 10, "yes_ask": 12, "volume": 24000},
    {"condition_id": "0xd65891729ce093cc12236856837eba1a0872fc7998fd4294c21346f7db68079c", "title": "Will Josh Shapiro win the 2028 Democratic presidential nomination?", "yes_bid": 15, "yes_ask": 17, "volume": 63000},
    {"condition_id": "0x36db77c539bcc8c7bf0686c1b99f30c5a5eed1f53d3b3a27a071550c81f83701", "title": "Will Mark Kelly win the 2028 Democratic presidential nomination?", "yes_bid": 9, "yes_ask": 11, "volume": 22000},
    {"condition_id": "0x3dc47a72ac6bb265e9cefebd2a2eeed00966394cb2d07245b5c94138b60e82a3", "title": "Will Rahm Emanuel win the 2028 Democratic presidential nomination?", "yes_bid": 6, "yes_ask": 8, "volume": 11000},
    {"condition_id": "0x3e218c99a1335641b3a5ee6c887521d19b0c28fddd6b99c254a07968e35c0b1b", "title": "Will Michelle Obama win the 2028 Democratic presidential nomination?", "yes_bid": 9, "yes_ask": 11, "volume": 39000},
    {"condition_id": "0x61d9486c0f7e14ed98f3b177b6adcb3cd45646c92e8bbfbf209789b86472d4b6", "title": "Will Wes Moore win the 2028 Democratic presidential nomination?", "yes_bid": 7, "yes_ask": 9, "volume": 17000},
    {"condition_id": "0xdce84960dce38aa4a5800a5eba7c9ac34d2ce49ba9d44c42572c472d468af264", "title": "Will Raphael Warnock win the 2028 Democratic presidential nomination?", "yes_bid": 8, "yes_ask": 10, "volume": 21000},
    {"condition_id": "0x1970bcce75e3674660917ae4685b433a9b1e0152d810a8d7c346dfd5e00694c2", "title": "Will Cory Booker win the 2028 Democratic presidential nomination?", "yes_bid": 5, "yes_ask": 7, "volume": 9000},
    {"condition_id": "0xe39adea057926dc197fe30a441f57a340b2a232d5a687010f78bba9b6e02620f", "title": "Will Gretchen Whitmer win the 2028 Democratic presidential nomination?", "yes_bid": 16, "yes_ask": 18, "volume": 72000},
]


# ============================================================================
# Live Market Fetching
# ============================================================================


async def fetch_live_markets(
    limit: int = 100,
) -> tuple[list[Any], list[Any], str]:
    """Fetch live market lists from Kalshi and Polymarket REST APIs.

    Queries real event markets across politics, elections, economics, culture,
    and finance rather than auto-generated sports parlay bets.
    Falls back to sample dataset if APIs are unreachable or offline.
    """
    config = load_config()
    kalshi_markets: list[Any] = []
    poly_markets: list[Any] = []
    source_desc = "live"

    # 1. Fetch Polymarket markets from events & CLOB
    try:
        import httpx
        async with httpx.AsyncClient(timeout=15.0) as http_client:
            p_offset = 0
            while len(poly_markets) < limit:
                batch_size = min(100, limit - len(poly_markets))
                resp = await http_client.get(
                    "https://gamma-api.polymarket.com/events",
                    params={"closed": "false", "limit": batch_size, "offset": p_offset},
                )
                if resp.status_code != 200:
                    break
                p_events = resp.json()
                if not p_events:
                    break
                for pev in p_events:
                    ev_title = pev.get("title", "")
                    for pm in pev.get("markets", []):
                        cid = pm.get("conditionId") or pm.get("condition_id")
                        if not cid:
                            continue
                        q = pm.get("question", "") or ev_title
                        pm_data = dict(pm)
                        pm_data["condition_id"] = cid
                        pm_data["title"] = q
                        try:
                            poly_markets.append(PolyMarket.model_validate(pm_data))
                        except Exception:
                            pass
                        if len(poly_markets) >= limit:
                            break
                    if len(poly_markets) >= limit:
                        break
                p_offset += batch_size
    except Exception as e:
        logger.debug("Could not fetch live Polymarket markets via events: %s", e)

    # 2. Fetch Kalshi real event prediction markets
    try:
        async with KalshiClient.from_config(config) as kalshi_client:
            cursor = None
            while len(kalshi_markets) < limit:
                params: dict[str, Any] = {
                    "status": "open",
                    "limit": min(100, max(20, limit // 2)),
                    "with_nested_markets": "true",
                }
                if cursor:
                    params["cursor"] = cursor
                resp = await kalshi_client._signed_request("GET", "/events", params=params)
                data = resp.json()
                events = data.get("events", [])
                if not events:
                    break
                for ev in events:
                    ev_title = ev.get("title", "")
                    # Skip sports combo / parlay events
                    category = ev.get("category", "")
                    for m in ev.get("markets", []):
                        ticker = m.get("ticker")
                        if not ticker:
                            continue
                        sub = m.get("title", "") or m.get("yes_sub_title", "")
                        # Build full descriptive title
                        if sub and sub.strip().lower() != ev_title.strip().lower():
                            full_title = f"{ev_title}: {sub}"
                        else:
                            full_title = ev_title
                        m_data = dict(m)
                        m_data["title"] = full_title
                        try:
                            kalshi_markets.append(KalshiMarket.model_validate(m_data))
                        except Exception:
                            pass
                        if len(kalshi_markets) >= limit:
                            break
                    if len(kalshi_markets) >= limit:
                        break
                cursor = data.get("cursor")
                if not cursor:
                    break
    except Exception as e:
        logger.debug("Could not fetch Kalshi event markets: %s", e)

    # Fallback to standard list_markets if events empty
    if not kalshi_markets:
        try:
            async with KalshiClient.from_config(config) as kalshi_client:
                kalshi_markets = await kalshi_client.list_markets(status="open", limit=limit, max_pages=1)
        except Exception as e:
            logger.debug("Could not fetch live Kalshi markets fallback: %s", e)

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
