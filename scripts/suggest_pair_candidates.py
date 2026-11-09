#!/usr/bin/env python3
"""Suggest market pair candidates using fuzzy title matching.

Emits unverified candidate YAML templates with `status: excluded` and
clear justification, ready for human comparability verification.

Usage:
    python scripts/suggest_pair_candidates.py --category politics
    python scripts/suggest_pair_candidates.py --min-similarity 0.75
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
import re
import sys
from typing import Any, Optional

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
import yaml

from core.config_loader import load_config
from core.market_matcher import MarketMatcher, MatchedPair
from kalshi_client.rest import KalshiClient
from polymarket_client.rest import PolymarketClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("suggest_pair_candidates")


def slugify(text: str) -> str:
    """Generate a clean identifier slug from a title."""
    s = text.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_-]+", "-", s)
    return s[:40].strip("-")


async def fetch_markets(config: Any) -> tuple[list[Any], list[Any]]:
    """Fetch active markets from Kalshi and Polymarket."""
    k_client = KalshiClient.from_config(config, is_sandbox=False)
    p_client = PolymarketClient.from_config(config)

    kalshi_markets: list[Any] = []
    poly_markets: list[Any] = []

    try:
        async with k_client, p_client:
            logger.info("Fetching Kalshi markets...")
            k_res = await k_client.get_markets(limit=200, status="open")
            kalshi_markets = getattr(k_res, "markets", []) or []

            logger.info("Fetching Polymarket markets...")
            poly_markets = await p_client.get_markets(limit=200, active=True, closed=False)
    except Exception as e:
        logger.warning("Could not fetch live markets (network or credentials unavailable): %s", e)

    return kalshi_markets, poly_markets


def suggest_candidates(
    matches: list[MatchedPair],
    category: str = "politics",
) -> list[dict[str, Any]]:
    """Format MatchedPair objects into unverified candidate dicts."""
    candidates = []
    for match in matches:
        if match.is_override:
            continue

        k_title = match.kalshi_title or ""
        p_title = match.polymarket_title or ""
        base_slug = slugify(k_title) or f"pair-{match.kalshi_ticker.lower()}"
        pair_id = f"{category}-{base_slug}"

        cand = {
            "pair_id": pair_id,
            "category": category,
            "kalshi_ticker": match.kalshi_ticker,
            "polymarket_condition_id": match.polymarket_condition_id,
            "polymarket_yes_token_id": None,
            "polymarket_no_token_id": None,
            "status": "excluded",
            "exclusion_reason": (
                f"Candidate proposed by automated similarity matching (confidence={match.confidence:.2f}); "
                "pending human verification of rules and resolution criteria"
            ),
            "notes": f"Kalshi: '{k_title}' | Polymarket: '{p_title}'",
        }
        candidates.append(cand)
    return candidates


async def main_async() -> None:
    load_dotenv()
    config = load_config()

    parser = argparse.ArgumentParser(
        description="Discover candidate Kalshi-Polymarket pairs and generate candidate YAML definitions."
    )
    parser.add_argument(
        "--category",
        type=str,
        default="politics",
        help="Category for discovered pairs (default: politics)",
    )
    parser.add_argument(
        "--min-similarity",
        type=float,
        default=0.75,
        help="Minimum similarity score threshold (0.0 - 1.0)",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default=None,
        help="File to append or write candidate YAML blocks to",
    )
    args = parser.parse_args()

    kalshi_mkts, poly_mkts = await fetch_markets(config)
    if not kalshi_mkts or not poly_mkts:
        print("# No live markets could be fetched. Ensure API credentials and network connectivity.")
        return

    matcher = MarketMatcher(
        min_similarity=args.min_similarity,
        overrides_only=False,
    )
    matches = matcher.match_markets(kalshi_mkts, poly_mkts)
    candidates = suggest_candidates(matches, category=args.category)

    yaml_text = yaml.dump({"pairs": candidates}, sort_keys=False)
    print(yaml_text)

    if args.output_file:
        out_path = Path(args.output_file)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(yaml_text)
        logger.info("Saved %d candidates to %s", len(candidates), args.output_file)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
