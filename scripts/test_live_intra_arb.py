#!/usr/bin/env python3
"""Live Intra-Market Arbitrage Scanner & Verification Tool.

Connects to live/demo prediction markets (Kalshi Sandbox/Prod and Polymarket CLOB),
fetches real-time orderbooks, and evaluates them with `find_intra_market_opportunities()`.

Usage:
    # Scan Polymarket live orderbooks (public API, no keys required)
    python scripts/test_live_intra_arb.py --polymarket --limit 30

    # Scan Kalshi Demo/Sandbox orderbooks
    python scripts/test_live_intra_arb.py --kalshi --sandbox --limit 30

    # Scan both with a simulated test market to verify live alert handling
    python scripts/test_live_intra_arb.py --polymarket --simulate-edge
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
from core.intra_market_arb import find_intra_market_opportunities
from core.models import Market
from kalshi_client.rest import KalshiClient
from polymarket_client.rest import PolymarketClient


async def scan_kalshi(limit: int, is_sandbox: bool, config) -> list:
    """Fetch live or sandbox Kalshi orderbooks."""
    print(f"\n[Kalshi] Connecting to {'SANDBOX' if is_sandbox else 'PRODUCTION'} API...")
    try:
        client = KalshiClient.from_config(config, is_sandbox=is_sandbox)
        async with client:
            markets = await client.list_markets(status="open", limit=limit, max_pages=1)
            if not markets:
                markets = await client.list_markets(status="", limit=limit, max_pages=1)
            if not markets:
                print("   [WARN] No Kalshi markets found.")
                return []

            target_tickers = [m.ticker for m in markets[:limit]]
            print(f"   --> Found {len(target_tickers)} markets. Fetching orderbooks in batch...")
            orderbooks = await client.get_orderbooks(target_tickers)
            print(f"   --> Retrieved {len(orderbooks)} orderbooks.")
            return list(orderbooks.values())
    except Exception as e:
        print(f"   [ERROR] Failed to query Kalshi: {e}")
        return []


async def scan_polymarket(limit: int, config) -> list:
    """Fetch live Polymarket CLOB orderbooks."""
    print(f"\n[Polymarket] Connecting to live Gamma & CLOB APIs...")
    try:
        client = PolymarketClient.from_config(config)
        async with client:
            markets = await client.list_markets(status="open", limit=limit, max_pages=1)
            if not markets:
                markets = await client.list_markets(status=None, limit=limit, max_pages=1)
            if not markets:
                print("   [WARN] No Polymarket markets found.")
                return []

            # Map token_id to market
            token_map = {}
            token_ids = []
            for m in markets[:limit]:
                if m.yes_token_id:
                    token_ids.append(m.yes_token_id)
                    token_map[m.yes_token_id] = m

            print(f"   --> Found {len(token_ids)} active YES tokens. Fetching CLOB orderbooks...")
            orderbooks = await client.get_orderbooks(token_ids)
            print(f"   --> Retrieved {len(orderbooks)} orderbooks.")

            # Attach friendly slug to ticker
            for tid, ob in orderbooks.items():
                if tid in token_map:
                    m = token_map[tid]
                    if m.slug:
                        ob.ticker = m.slug
                    elif m.ticker:
                        ob.ticker = m.ticker

            return list(orderbooks.values())
    except Exception as e:
        print(f"   [ERROR] Failed to query Polymarket: {e}")
        return []


async def main():
    parser = argparse.ArgumentParser(description="Live/Demo Intra-Market Arbitrage Scanner")
    parser.add_argument("--kalshi", action="store_true", help="Scan Kalshi markets")
    parser.add_argument("--polymarket", action="store_true", help="Scan Polymarket markets")
    parser.add_argument("--prod", action="store_true", help="Use Kalshi production API instead of sandbox")
    parser.add_argument("--sandbox", action="store_true", default=True, help="Use Kalshi sandbox API")
    parser.add_argument("--limit", type=int, default=25, help="Number of markets to fetch per platform")
    parser.add_argument("--min-edge", type=float, default=None, help="Custom min edge threshold (e.g. 0.01 for 1 cent)")
    parser.add_argument("--simulate-edge", action="store_true", help="Inject a synthetic 3.5c edge to verify alert triggering")
    args = parser.parse_args()

    # Default to scanning Polymarket + Kalshi if neither specified
    if not args.kalshi and not args.polymarket:
        args.polymarket = True
        args.kalshi = True

    load_dotenv()
    config = load_config()

    if args.min_edge is not None:
        config.trading.min_edge = args.min_edge

    print("=" * 80)
    print("  Intra-Market Arbitrage Engine — Live & Demo Market Verification")
    print("=" * 80)
    min_cents = config.trading.min_edge * 100 if config.trading.min_edge < 1.0 else config.trading.min_edge
    print(f"Trading Threshold: min_edge = {min_cents:.2f} cents ({config.trading.min_edge})")
    print(f"Fee Model:         taker_fee_bps = {config.trading.taker_fee_bps} bps")

    all_markets: list = []

    if args.kalshi:
        is_sandbox = not args.prod
        kalshi_obs = await scan_kalshi(limit=args.limit, is_sandbox=is_sandbox, config=config)
        all_markets.extend(kalshi_obs)

    if args.polymarket:
        poly_obs = await scan_polymarket(limit=args.limit, config=config)
        all_markets.extend(poly_obs)

    if args.simulate_edge:
        print("\n[SIMULATION] Injecting synthetic mispriced test market (YES=47c, NO=49.5c -> 3.5c edge)...")
        synthetic_market = Market(
            platform="demo_exchange",
            ticker="DEMO-SIM-ARBITRAGE-3.5C",
            title="Synthetic Mispriced Market for Live Alert Verification",
            yes_ask=47,
            no_ask=49,
            yes_bid=45,
            no_bid=48,
            volume=50000,
        )
        all_markets.append(synthetic_market)

    print("\n" + "=" * 80)
    print(f"  Scanning {len(all_markets)} total orderbooks with find_intra_market_opportunities()...")
    print("=" * 80)

    # Run the intra-market arb engine
    opportunities = find_intra_market_opportunities(all_markets, config=config)

    # Display live pricing summary for top orderbooks
    print("\nOrderbook Price Inspection (Sample of live orderbooks):")
    print(f"{'PLATFORM':<12} {'TICKER':<32} {'YES ASK':<10} {'NO ASK':<10} {'TOTAL COST':<12} {'EDGE':<10}")
    print("-" * 88)

    ranked_by_spread = []
    empty_orderbooks = []
    for m in all_markets:
        y_ask = getattr(m, "best_yes_ask", None) or getattr(m, "yes_ask", None)
        n_ask = getattr(m, "best_no_ask", None) or getattr(m, "no_ask", None)
        plat = getattr(m, "platform", None)
        if not plat:
            plat = "polymarket" if (hasattr(m, "token_id") or hasattr(m, "condition_id")) else "kalshi"
        tick = getattr(m, "ticker", "")[:30]
        if y_ask is not None and n_ask is not None:
            cost = y_ask + n_ask
            edge = 100 - cost
            ranked_by_spread.append((plat, tick, y_ask, n_ask, cost, edge))
        else:
            empty_orderbooks.append((plat, tick))

    # Sort by total cost ascending (closest to arb first)
    ranked_by_spread.sort(key=lambda x: x[4])

    for plat, tick, y_ask, n_ask, cost, edge in ranked_by_spread[:15]:
        edge_str = f"+{edge}c" if edge > 0 else f"{edge}c"
        print(f"{plat:<12} {tick:<32} {y_ask:>3}c       {n_ask:>3}c       {cost:>3}c         {edge_str:>6}")

    if empty_orderbooks and not ranked_by_spread:
        for plat, tick in empty_orderbooks[:10]:
            print(f"{plat:<12} {tick:<32} {'--':>4}       {'--':>4}       {'--':>4}         {'Empty Book':>10}")
        print("-" * 88)
        print("  [NOTE] Kalshi Sandbox returns open test markets, but their orderbooks are empty (no bids/asks).")
        print("         Kalshi Sandbox does not run automated market makers quoting prices.")
        print("         To verify the detection engine with simulated spreads, add `--simulate-edge`.")
    else:
        print("-" * 88)

    # Display detected opportunities
    print("\n" + "=" * 80)
    if opportunities:
        print(f"  [OPPORTUNITY FOUND] {len(opportunities)} Intra-Market Arbitrage Opportunity(ies) Detected!")
        print("=" * 80)
        for idx, opp in enumerate(opportunities, 1):
            print(f"\nOpportunity #{idx}:")
            print(f"  Platform:         {opp.platform.upper()}")
            print(f"  Ticker:           {opp.ticker}")
            print(f"  Side Actions:     {opp.side_actions} (Simultaneous Long YES + Long NO)")
            print(f"  Net Edge:         +{opp.edge_cents:.2f} cents per contract")
            print(f"  Gross Edge:       +{opp.gross_edge_cents:.2f} cents")
            print(f"  Estimated Fees:   {opp.estimated_fees_cents:.2f} cents")
            print(f"  Detected At:      {opp.timestamp.isoformat()}")
            print(f"  Orderbook Snapshot for Audit:")
            print(f"    - YES Ask:      {opp.orderbook_snapshot.get('yes_ask')}c")
            print(f"    - NO  Ask:      {opp.orderbook_snapshot.get('no_ask')}c")
            print(f"    - YES Bid:      {opp.orderbook_snapshot.get('yes_bid')}c")
            print(f"    - NO  Bid:      {opp.orderbook_snapshot.get('no_bid')}c")
    else:
        print("  [RESULT] No live intra-market arbitrage opportunities detected currently.")
        print(f"  (Market asks sum to >= {100 - min_cents:.1f}c; efficient book pricing prevents same-exchange mispricing).")
        print("=" * 80)

    print("\n[SUCCESS] Live scanner verification complete.")


if __name__ == "__main__":
    asyncio.run(main())
