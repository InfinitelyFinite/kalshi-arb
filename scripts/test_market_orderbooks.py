#!/usr/bin/env python3
"""Verification script to test list_markets() and batched get_orderbooks() with derived asks.

Fetches open markets, retrieves their orderbooks in batch, and displays the top markets
with both raw bids and derived asks (Ask = 100 - Opposite Bid).

Usage:
    python scripts/test_market_orderbooks.py
"""

import asyncio
import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from core.config_loader import load_config
from kalshi_client.rest import KalshiClient


async def main() -> int:
    load_dotenv()
    print("=" * 80)
    print("  Kalshi Market Discovery & Batched Orderbook Verification")
    print("=" * 80)

    config = load_config()
    # Default to sandbox for safety
    is_sandbox = config.mode.trading_mode.value.lower() in ("sandbox", "dry_run")
    client = KalshiClient.from_config(config, is_sandbox=is_sandbox)

    print(f"Environment: {'SANDBOX' if is_sandbox else 'PRODUCTION'}")
    print(f"Target URL:  {client.base_url}\n")

    async with client:
        # 1. Fetch first page of open markets
        print("1. Fetching open markets via list_markets(limit=20, max_pages=1)...")
        markets = await client.list_markets(status="open", limit=20, max_pages=1)
        print(f"   --> Found {len(markets)} open markets.\n")

        if not markets:
            print("[WARN] No open markets returned. Trying status=None or active...")
            markets = await client.list_markets(status="", limit=20, max_pages=1)
            print(f"   --> Found {len(markets)} total markets.\n")

        if not markets:
            print("[ERROR] No markets available to fetch orderbooks.")
            return 1

        # Select first 20 tickers
        target_markets = markets[:20]
        tickers = [m.ticker for m in target_markets]
        market_map = {m.ticker: m for m in target_markets}

        # 2. Fetch orderbooks in batch
        print(f"2. Fetching orderbooks in batch for {len(tickers)} tickers via get_orderbooks()...")
        orderbooks = await client.get_orderbooks(tickers)
        print(f"   --> Retrieved {len(orderbooks)} orderbooks.\n")

        # 3. Print Market Summary Table
        print("=" * 100)
        print(f"{'#':<3} {'TICKER':<28} {'YES BID':<9} {'YES ASK (der)':<15} {'NO BID':<8} {'NO ASK (der)':<14} {'SPREAD'}")
        print("-" * 100)

        for idx, ticker in enumerate(tickers, 1):
            ob = orderbooks.get(ticker)
            if not ob:
                print(f"{idx:<3} {ticker:<28} {'<No Orderbook>':<50}")
                continue

            yes_b = f"{ob.best_yes_bid}c" if ob.best_yes_bid is not None else "--"
            yes_a = f"{ob.best_yes_ask}c" if ob.best_yes_ask is not None else "--"
            no_b = f"{ob.best_no_bid}c" if ob.best_no_bid is not None else "--"
            no_a = f"{ob.best_no_ask}c" if ob.best_no_ask is not None else "--"
            spread = f"{ob.yes_spread}c" if ob.yes_spread is not None else "--"

            print(f"{idx:<3} {ticker:<28} {yes_b:<9} {yes_a:<15} {no_b:<8} {no_a:<14} {spread}")

        print("=" * 100)

        # 4. Detailed Book Breakdown for 2-3 sample active markets
        active_obs = [ob for ob in orderbooks.values() if ob.yes_bids or ob.no_bids]
        samples = active_obs[:3] if active_obs else list(orderbooks.values())[:2]

        print("\n" + "=" * 80)
        print("  Detailed Orderbook Snapshots & Derived Asks (Verification Samples)")
        print("=" * 80)

        for ob in samples:
            m = market_map.get(ob.ticker)
            title = m.title if m else ob.ticker
            print(f"\nMarket: [{ob.ticker}]")
            print(f"Title:  {title}")
            print("-" * 60)
            print("  RAW BIDS (from Kalshi API):")
            print(f"    YES Bids (Price, Size): {ob.yes_bids}")
            print(f"    NO  Bids (Price, Size): {ob.no_bids}")
            print("  DERIVED ASKS (Calculated as 100 - Opposite Bid):")
            yes_b_str = f"{ob.best_yes_bid}c" if ob.best_yes_bid is not None else "--"
            yes_a_str = f"{ob.best_yes_ask}c" if ob.best_yes_ask is not None else "--"
            no_b_str = f"{ob.best_no_bid}c" if ob.best_no_bid is not None else "--"
            no_a_str = f"{ob.best_no_ask}c" if ob.best_no_ask is not None else "--"

            print(f"    Best YES Bid: {yes_b_str}  |  Derived Best YES Ask: {yes_a_str} (from best NO bid {no_b_str})")
            print(f"    Best NO  Bid: {no_b_str}   |  Derived Best NO  Ask: {no_a_str} (from best YES bid {yes_b_str})")
            print(f"    All Implied YES Asks: {ob.yes_asks}")
            print(f"    All Implied NO  Asks: {ob.no_asks}")
            print("-" * 60)

    print("\n[SUCCESS] list_markets() and get_orderbooks() verification complete.")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
