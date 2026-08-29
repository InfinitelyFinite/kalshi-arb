#!/usr/bin/env python3
"""Verification script to test Polymarket list_markets() via Gamma API and get_orderbook() via CLOB API.

Fetches 20 open markets, retrieves their orderbooks, and displays the top markets
with both raw bids/asks and derived prices mirroring the Kalshi equivalent.

Usage:
    python scripts/test_polymarket_orderbooks.py
"""

import asyncio
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from core.config_loader import load_config
from polymarket_client.rest import PolymarketClient


async def main() -> int:
    load_dotenv()
    print("=" * 80)
    print("  Polymarket Market Discovery & Orderbook Verification")
    print("=" * 80)

    config = load_config()
    client = PolymarketClient.from_config(config)

    print(f"Gamma API URL: {client.gamma_url}")
    print(f"CLOB API URL:  {client.clob_url}\n")

    async with client:
        # 1. Fetch first page of open markets via Gamma API
        print("1. Fetching open markets via list_markets(limit=20, max_pages=1)...")
        markets = await client.list_markets(status="open", limit=20, max_pages=1)
        print(f"   --> Found {len(markets)} open markets.\n")

        if not markets:
            print("[WARN] No open markets returned. Trying status=None...")
            markets = await client.list_markets(status=None, limit=20, max_pages=1)
            print(f"   --> Found {len(markets)} total markets.\n")

        if not markets:
            print("[ERROR] No markets available to fetch orderbooks.")
            return 1

        # Select first 20 markets
        target_markets = markets[:20]
        market_map = {}
        token_to_market = {}

        for m in target_markets:
            market_map[m.condition_id] = m
            # Track YES token
            if m.yes_token_id:
                token_to_market[m.yes_token_id] = m

        yes_token_ids = [m.yes_token_id for m in target_markets if m.yes_token_id]
        print(f"2. Fetching orderbooks for {len(yes_token_ids)} YES tokens via get_orderbooks()...")
        orderbooks = await client.get_orderbooks(yes_token_ids)
        print(f"   --> Retrieved {len(orderbooks)} orderbooks.\n")

        # 3. Print Market Summary Table (Mirrors Kalshi table shape)
        print("=" * 100)
        print(f"{'#':<3} {'CONDITION / TOKEN ID':<28} {'YES BID':<9} {'YES ASK':<10} {'NO BID (der)':<14} {'NO ASK (der)':<14} {'SPREAD'}")
        print("-" * 100)

        for idx, m in enumerate(target_markets, 1):
            token_id = m.yes_token_id
            ob = orderbooks.get(token_id) if token_id else None

            identifier = (m.slug[:26] if m.slug else m.condition_id[:26])

            if not ob:
                print(f"{idx:<3} {identifier:<28} {'<No Orderbook>':<50}")
                continue

            yes_b = f"{ob.best_yes_bid}c" if ob.best_yes_bid is not None else "--"
            yes_a = f"{ob.best_yes_ask}c" if ob.best_yes_ask is not None else "--"
            no_b = f"{ob.best_no_bid}c" if ob.best_no_bid is not None else "--"
            no_a = f"{ob.best_no_ask}c" if ob.best_no_ask is not None else "--"
            spread = f"{ob.yes_spread}c" if ob.yes_spread is not None else "--"

            print(f"{idx:<3} {identifier:<28} {yes_b:<9} {yes_a:<10} {no_b:<14} {no_a:<14} {spread}")

        print("=" * 100)

        # 4. Detailed Book Breakdown for sample active markets
        active_obs = [ob for ob in orderbooks.values() if ob.yes_bids or ob.yes_asks]
        samples = active_obs[:3] if active_obs else list(orderbooks.values())[:2]

        print("\n" + "=" * 80)
        print("  Detailed Orderbook Snapshots (Verification Samples)")
        print("=" * 80)

        for ob in samples:
            m = token_to_market.get(ob.token_id)
            title = m.title if m else ob.token_id
            cond_id = m.condition_id if m else ob.condition_id

            print(f"\nCondition ID: [{cond_id}]")
            print(f"Token ID:     [{ob.token_id}]")
            print(f"Title:        {title}")
            print("-" * 60)
            print("  RAW BIDS & ASKS (from Polymarket CLOB):")
            print(f"    YES Bids (Price, Size): {ob.yes_bids[:5]}")
            print(f"    YES Asks (Price, Size): {ob.yes_asks[:5]}")
            print("  DERIVED / CALCULATED METRICS:")
            yes_b_str = f"{ob.best_yes_bid}c" if ob.best_yes_bid is not None else "--"
            yes_a_str = f"{ob.best_yes_ask}c" if ob.best_yes_ask is not None else "--"
            no_b_str = f"{ob.best_no_bid}c" if ob.best_no_bid is not None else "--"
            no_a_str = f"{ob.best_no_ask}c" if ob.best_no_ask is not None else "--"
            spread_str = f"{ob.yes_spread}c" if ob.yes_spread is not None else "--"

            print(f"    Best YES Bid: {yes_b_str}  |  Best YES Ask: {yes_a_str}  |  Spread: {spread_str}")
            print(f"    Derived Best NO Bid: {no_b_str}  |  Derived Best NO Ask: {no_a_str}")
            print("-" * 60)

    print("\n[SUCCESS] Polymarket list_markets() and get_orderbooks() verification complete.")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
