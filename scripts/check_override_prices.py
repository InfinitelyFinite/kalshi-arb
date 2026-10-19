#!/usr/bin/env python3
"""Inspect live orderbook buy/sell prices and arbitrage status for market_overrides.yaml.

Usage:
    # Query Kalshi Sandbox (default)
    .venv/bin/python scripts/check_override_prices.py

    # Query Kalshi Production
    .venv/bin/python scripts/check_override_prices.py --prod
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
from core.config_loader import load_config
from core.market_matcher import load_market_overrides
from kalshi_client.rest import KalshiClient


async def main():
    parser = argparse.ArgumentParser(description="Check live orderbook prices for market overrides")
    parser.add_argument("--prod", action="store_true", help="Use Kalshi Production API instead of Sandbox")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    is_sandbox = not args.prod
    api_name = "SANDBOX (demo-api.kalshi.co)" if is_sandbox else "PRODUCTION (trading-api.kalshi.com)"

    overrides_path = "config/market_overrides.yaml"
    overrides = load_market_overrides(overrides_path)
    if not overrides:
        print(f"[ERROR] No overrides found in {overrides_path}")
        return

    tickers = [ov.kalshi_ticker for ov in overrides if ov.kalshi_ticker]

    print("=" * 105)
    print(f"  Live Market Overrides Price & Arbitrage Inspector")
    print(f"  Target API: {api_name}")
    print(f"  Loaded:     {len(overrides)} markets from {overrides_path}")
    print("=" * 105)

    try:
        client = KalshiClient.from_config(config, is_sandbox=is_sandbox)
        async with client:
            print(f"\nFetching live orderbooks for {len(tickers)} Kalshi tickers...")
            orderbooks = await client.get_orderbooks(tickers)
    except Exception as e:
        print(f"\n[ERROR] Failed to connect to Kalshi: {e}")
        return

    print("\n" + "-" * 105)
    header = (
        f"{'TICKER':<20} | {'YES BID (SELL)':<14} | {'YES ASK (BUY)':<13} | "
        f"{'NO BID (SELL)':<13} | {'NO ASK (BUY)':<12} | {'INTRA COST':<10} | {'CANDIDATE / EVENT'}"
    )
    print(header)
    print("-" * 105)

    intra_opps = []

    for ov in overrides:
        t = ov.kalshi_ticker
        ob = orderbooks.get(t)
        notes = ov.notes or t

        if ob:
            y_bid = ob.best_yes_bid
            y_ask = ob.best_yes_ask
            n_bid = ob.best_no_bid
            n_ask = ob.best_no_ask

            cost_str = "--"
            if y_ask is not None and n_ask is not None:
                intra_cost = y_ask + n_ask
                cost_str = f"{intra_cost}¢"
                if intra_cost < 100:
                    edge = 100 - intra_cost
                    intra_opps.append((t, edge, y_ask, n_ask, notes))

            y_bid_str = f"{y_bid}¢" if y_bid is not None else "--"
            y_ask_str = f"{y_ask}¢" if y_ask is not None else "--"
            n_bid_str = f"{n_bid}¢" if n_bid is not None else "--"
            n_ask_str = f"{n_ask}¢" if n_ask is not None else "--"

            print(
                f"{t:<20} | {y_bid_str:<14} | {y_ask_str:<13} | "
                f"{n_bid_str:<13} | {n_ask_str:<12} | {cost_str:<10} | {notes}"
            )
        else:
            print(f"{t:<20} | {'--':<14} | {'--':<13} | {'--':<13} | {'--':<12} | {'--':<10} | {notes} (No Book)")

    print("-" * 105)

    print("\n--- Summary ---")
    if intra_opps:
        print(f"🔥 Found {len(intra_opps)} intra-market arbitrage opportunity(ies):")
        for t, edge, y_ask, n_ask, notes in intra_opps:
            print(f"   - {t} ({notes}): Gross Edge = +{edge}¢ (YES Ask={y_ask}¢ + NO Ask={n_ask}¢ = {y_ask + n_ask}¢ < 100¢)")
    else:
        print("✅ No intra-market arbitrage opportunities currently open on Kalshi (all YES Ask + NO Ask >= 100¢).")

    print("\nNote: Polymarket condition IDs in market_overrides.yaml link to these candidates for cross-platform matching.")


if __name__ == "__main__":
    asyncio.run(main())
