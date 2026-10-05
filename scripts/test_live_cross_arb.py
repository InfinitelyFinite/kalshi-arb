#!/usr/bin/env python3
"""Live Cross-Platform Arbitrage Scanner & Verification Tool.

Queries matched pairs from manual overrides (config/market_overrides.yaml) or event matching,
fetches real-time orderbooks from Kalshi and Polymarket, and evaluates both trading directions:
    (a) Buy Kalshi YES + Buy Polymarket NO (Cost = Kalshi YES ask + Polymarket NO ask)
    (b) Buy Polymarket YES + Buy Kalshi NO (Cost = Polymarket YES ask + Kalshi NO ask)

Usage:
    # Scan live matched markets on Kalshi Sandbox and Polymarket CLOB
    .venv/bin/python scripts/test_live_cross_arb.py --kalshi --sandbox --limit 20 --min-edge 0.005

    # Scan with simulated edge to verify cross-platform detection and fee accounting
    .venv/bin/python scripts/test_live_cross_arb.py --kalshi --sandbox --limit 20 --min-edge 0.005 --simulate-edge
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
from core.cross_platform_arb import find_cross_platform_opportunities
from core.market_matcher import MarketMatch, MarketMatcher, load_market_overrides
from core.models import Market
from kalshi_client.models import Orderbook as KalshiOrderbook
from kalshi_client.rest import KalshiClient
from polymarket_client.models import Orderbook as PolyOrderbook
from polymarket_client.rest import PolymarketClient


async def fetch_kalshi_markets_and_books(limit: int, is_sandbox: bool, config) -> tuple[list, dict]:
    """Fetch Kalshi markets and orderbooks."""
    print(f"\n[Kalshi] Connecting to {'SANDBOX' if is_sandbox else 'PRODUCTION'} API...")
    try:
        client = KalshiClient.from_config(config, is_sandbox=is_sandbox)
        async with client:
            all_markets = []
            if not is_sandbox:
                for s in ["KXINX", "KXBTC", "KXETH", "KXNDX"]:
                    try:
                        mkts = await client.list_markets(series_ticker=s, status="open", limit=max(5, limit // 4), max_pages=1)
                        all_markets.extend(mkts)
                    except Exception:
                        pass
            if not all_markets:
                all_markets = await client.list_markets(status="open", limit=limit, max_pages=1)
            if not all_markets:
                all_markets = await client.list_markets(status="", limit=limit, max_pages=1)
            if not all_markets:
                print("   [WARN] No Kalshi markets found.")
                return [], {}

            filtered = [m for m in all_markets if not m.ticker.startswith("KXMVECROSSCATEGORY")]
            if filtered:
                all_markets = filtered

            tickers = [m.ticker for m in all_markets[:limit]]
            print(f"   --> Found {len(tickers)} Kalshi markets. Fetching orderbooks...")
            orderbooks = await client.get_orderbooks(tickers)
            print(f"   --> Retrieved {len(orderbooks)} Kalshi orderbooks.")
            return all_markets[:limit], orderbooks
    except Exception as e:
        print(f"   [ERROR] Failed to query Kalshi: {e}")
        return [], {}


async def fetch_polymarket_markets_and_books(limit: int, config) -> tuple[list, dict]:
    """Fetch Polymarket markets and orderbooks."""
    print(f"\n[Polymarket] Connecting to live Gamma & CLOB APIs...")
    try:
        client = PolymarketClient.from_config(config)
        async with client:
            markets = await client.list_markets(status="open", limit=limit, max_pages=1)
            if not markets:
                markets = await client.list_markets(status=None, limit=limit, max_pages=1)
            if not markets:
                print("   [WARN] No Polymarket markets found.")
                return [], {}

            token_map = {}
            token_ids = []
            for m in markets[:limit]:
                if m.yes_token_id:
                    token_ids.append(m.yes_token_id)
                    token_map[m.yes_token_id] = m

            print(f"   --> Found {len(token_ids)} active tokens. Fetching CLOB orderbooks...")
            orderbooks = await client.get_orderbooks(token_ids)
            print(f"   --> Retrieved {len(orderbooks)} Polymarket orderbooks.")

            # Map by condition_id and token_id
            by_id = {}
            for tid, ob in orderbooks.items():
                by_id[tid] = ob
                if tid in token_map:
                    m = token_map[tid]
                    if m.condition_id:
                        by_id[m.condition_id] = ob
                    if m.slug:
                        ob.ticker = m.slug

            return markets[:limit], by_id
    except Exception as e:
        print(f"   [ERROR] Failed to query Polymarket: {e}")
        return [], {}


async def main():
    parser = argparse.ArgumentParser(description="Live Cross-Platform Arbitrage Scanner")
    parser.add_argument("--kalshi", action="store_true", default=True, help="Include Kalshi in scan")
    parser.add_argument("--polymarket", action="store_true", default=True, help="Include Polymarket in scan")
    parser.add_argument("--prod", action="store_true", help="Use Kalshi production API")
    parser.add_argument("--sandbox", action="store_true", default=True, help="Use Kalshi sandbox API")
    parser.add_argument("--limit", type=int, default=20, help="Number of markets to fetch per platform")
    parser.add_argument("--min-edge", type=float, default=None, help="Custom min edge threshold (e.g. 0.005 for 0.5c)")
    parser.add_argument("--simulate-edge", action="store_true", help="Inject simulated cross-platform mispricings")
    parser.add_argument("--taker-fee-bps", type=float, default=None, help="Taker fee in basis points (e.g. 50.0 for 0.5%)")
    parser.add_argument("--gas-cents", type=float, default=None, help="Polygon network gas estimate in cents (e.g. 1.0)")
    parser.add_argument("--sample", action="store_true", help="Use built-in representative sample dataset with populated books")
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    if args.min_edge is not None:
        config.trading.min_edge = args.min_edge
    if args.taker_fee_bps is not None:
        config.trading.taker_fee_bps = args.taker_fee_bps
    if args.gas_cents is not None:
        config.trading.polygon_gas_estimate_cents = args.gas_cents

    print("=" * 80)
    print("  Cross-Platform Arbitrage Engine — Live & Demo Market Verification")
    print("=" * 80)
    min_cents = config.trading.min_edge * 100 if config.trading.min_edge < 1.0 else config.trading.min_edge
    print(f"Trading Threshold: min_edge = {min_cents:.2f} cents ({config.trading.min_edge})")
    print(f"Fee Model:         taker_fee_bps = {config.trading.taker_fee_bps} bps, Polygon gas = {config.trading.polygon_gas_estimate_cents:.2f}¢")

    if args.sample:
        from scripts.review_matches import SAMPLE_KALSHI_MARKETS, SAMPLE_POLYMARKET_MARKETS
        from kalshi_client.models import Market as KalshiMarket
        from polymarket_client.models import Market as PolyMarket
        print("\n[SAMPLE MODE] Using offline sample dataset with populated orderbooks...")
        kalshi_markets = [KalshiMarket.model_validate(m) for m in SAMPLE_KALSHI_MARKETS]
        poly_markets = [PolyMarket.model_validate(m) for m in SAMPLE_POLYMARKET_MARKETS]
        kalshi_books = {}
        for m in SAMPLE_KALSHI_MARKETS:
            kalshi_books[m["ticker"]] = {
                "yes_bid": m["yes_bid"],
                "yes_ask": m["yes_ask"],
                "no_bid": 100 - m["yes_ask"],
                "no_ask": 100 - m["yes_bid"],
            }
        poly_books = {}
        for m in SAMPLE_POLYMARKET_MARKETS:
            poly_books[m["condition_id"]] = {
                "yes_bid": m["yes_bid"],
                "yes_ask": m["yes_ask"],
                "no_bid": 100 - m["yes_ask"],
                "no_ask": 100 - m["yes_bid"],
            }
    else:
        is_sandbox = not args.prod
        kalshi_markets, kalshi_books = await fetch_kalshi_markets_and_books(
            limit=args.limit, is_sandbox=is_sandbox, config=config
        )
        poly_markets, poly_books = await fetch_polymarket_markets_and_books(
            limit=args.limit, config=config
        )

    # Load manual overrides
    overrides = load_market_overrides("config/market_overrides.yaml")
    matched_pairs: list[Any] = []
    seen_pair_keys: set[tuple[str, str]] = set()

    # Map overrides
    for ov in overrides:
        pair_key = (ov.kalshi_ticker, ov.polymarket_condition_id)
        if pair_key not in seen_pair_keys:
            seen_pair_keys.add(pair_key)
            matched_pairs.append(
                MarketMatch(
                    kalshi_ticker=ov.kalshi_ticker,
                    polymarket_condition_id=ov.polymarket_condition_id,
                    kalshi_title=ov.notes or ov.kalshi_ticker,
                    polymarket_title=ov.notes or ov.polymarket_condition_id,
                    confidence=1.0,
                    match_source="override",
                )
            )

    # Also fuzzy match fetched markets if available
    if kalshi_markets and poly_markets:
        matcher = MarketMatcher(min_similarity=config.matching.min_similarity)
        fuzzy_matches = matcher.match(kalshi_markets, poly_markets)
        for fm in fuzzy_matches:
            pair_key = (fm.kalshi_ticker, fm.polymarket_condition_id)
            if pair_key not in seen_pair_keys:
                seen_pair_keys.add(pair_key)
                matched_pairs.append(fm)

    # Synthetic simulation markets
    if args.simulate_edge:
        print("\n[SIMULATION] Injecting synthetic mispriced cross-platform pairs:")
        print("   1. Direction (a) edge: Kalshi YES=42¢ + Poly NO=46¢ (Cost=88¢ -> +12¢ gross edge)")
        print("   2. Direction (b) edge: Poly YES=43¢ + Kalshi NO=41¢ (Cost=84¢ -> +16¢ gross edge)")

        sim_pair_a = MarketMatch(
            kalshi_ticker="SIM-KXFED-26",
            polymarket_condition_id="0xsim_fed_26",
            kalshi_title="Fed Rate Decision 2026 (Simulated)",
            polymarket_title="Fed Rate Decision 2026 (Simulated)",
            confidence=1.0,
            match_source="simulation",
        )
        kalshi_books["SIM-KXFED-26"] = {"yes_ask": 42, "no_ask": 60, "yes_bid": 40, "no_bid": 58}
        poly_books["0xsim_fed_26"] = {"yes_ask": 56, "no_ask": 46, "yes_bid": 54, "no_bid": 44}
        matched_pairs.append(sim_pair_a)

        sim_pair_b = MarketMatch(
            kalshi_ticker="SIM-KXBTC-100K",
            polymarket_condition_id="0xsim_btc_100k",
            kalshi_title="Bitcoin $100k (Simulated)",
            polymarket_title="Bitcoin $100k (Simulated)",
            confidence=1.0,
            match_source="simulation",
        )
        kalshi_books["SIM-KXBTC-100K"] = {"yes_ask": 62, "no_ask": 41, "yes_bid": 59, "no_bid": 38}
        poly_books["0xsim_btc_100k"] = {"yes_ask": 43, "no_ask": 61, "yes_bid": 39, "no_bid": 57}
        matched_pairs.append(sim_pair_b)

    print("\n" + "=" * 80)
    print(f"  Scanning {len(matched_pairs)} matched pairs across orderbooks...")
    print("=" * 80)

    # Run cross-platform arbitrage detection
    opportunities = find_cross_platform_opportunities(
        matched_pairs=matched_pairs,
        kalshi_orderbooks=kalshi_books,
        polymarket_orderbooks=poly_books,
        config=config,
    )

    # Orderbook summary table
    print("\nPair Price Inspection (Sample of evaluated pairs):")
    print(f"{'PAIR / KALSHI':<22} {'POLYMARKET ID':<22} {'DIR (a) COST':<14} {'DIR (b) COST':<14} {'STATUS':<15}")
    print("-" * 87)

    evaluated_rows = []
    for p in matched_pairs:
        k_id = getattr(p, "kalshi_ticker", "")
        poly_id = getattr(p, "polymarket_condition_id", "")
        k_ob = kalshi_books.get(k_id)
        p_ob = poly_books.get(poly_id)

        k_yes = getattr(k_ob, "best_yes_ask", None) if k_ob else None
        if k_yes is None and isinstance(k_ob, dict):
            k_yes = k_ob.get("yes_ask")
        k_no = getattr(k_ob, "best_no_ask", None) if k_ob else None
        if k_no is None and isinstance(k_ob, dict):
            k_no = k_ob.get("no_ask")

        p_yes = getattr(p_ob, "best_yes_ask", None) if p_ob else None
        if p_yes is None and isinstance(p_ob, dict):
            p_yes = p_ob.get("yes_ask")
        p_no = getattr(p_ob, "best_no_ask", None) if p_ob else None
        if p_no is None and isinstance(p_ob, dict):
            p_no = p_ob.get("no_ask")

        cost_a_str = f"{k_yes + p_no}¢" if (k_yes is not None and p_no is not None) else "--"
        cost_b_str = f"{p_yes + k_no}¢" if (p_yes is not None and k_no is not None) else "--"

        has_both_books = (k_ob is not None and p_ob is not None)
        if not has_both_books:
            status_str = "Missing Book"
        elif (k_yes is not None and p_no is not None) or (p_yes is not None and k_no is not None):
            status_str = "Priced"
        elif k_yes is not None or p_no is not None or p_yes is not None or k_no is not None:
            status_str = "One-Sided"
        else:
            status_str = "Empty Book"

        evaluated_rows.append({
            "k_id": k_id,
            "poly_id": poly_id,
            "cost_a": cost_a_str,
            "cost_b": cost_b_str,
            "status": status_str,
            "has_books": has_both_books,
        })

    # Prioritize pairs with available orderbooks at the top of the table
    evaluated_rows.sort(key=lambda r: (not r["has_books"], r["status"] == "Empty Book"))

    for row in evaluated_rows[:15]:
        print(f"{row['k_id'][:20]:<22} {row['poly_id'][:20]:<22} {row['cost_a']:<14} {row['cost_b']:<14} {row['status']:<15}")
    print("-" * 87)

    missing_count = sum(1 for r in evaluated_rows if not r["has_books"])
    if missing_count > 0:
        print(f"  Note: {missing_count} pair(s) marked 'Missing Book' are static override templates")
        print("        from config/market_overrides.yaml whose contracts are not currently")
        print("        listed on active live exchanges.")

    if not opportunities:
        print("\n" + "=" * 80)
        print("  [RESULT] No live cross-platform arbitrage opportunities detected currently.")
        print(f"  (Matched leg asks sum to >= 100¢ minus fees).")
        if not args.simulate_edge:
            print("  To verify detection with simulated spreads, add `--simulate-edge`.")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print(f"  [OPPORTUNITY FOUND] {len(opportunities)} Cross-Platform Arbitrage Opportunity(ies) Detected!")
        print("=" * 80)
        for i, opp in enumerate(opportunities, 1):
            dir_str = opp.orderbook_snapshot.get("direction", "cross_platform")
            print(f"\nOpportunity #{i}:")
            print(f"  Direction:        {dir_str}")
            print(f"  Primary Platform: {opp.platform.upper()} ({opp.ticker})")
            print(f"  Target Platform:  {opp.target_platform.upper()} ({opp.target_ticker})")
            print(f"  Gross Edge:       +{opp.gross_edge_cents:.2f} cents")
            print(f"  Estimated Fees:   {opp.estimated_fees_cents:.2f} cents")
            print(f"  Net Edge:         +{opp.edge_cents:.2f} cents per contract bundle")
            print("  Execution Legs:")
            for leg_idx, leg in enumerate(opp.legs, 1):
                print(f"    Leg {leg_idx}: {leg.platform.upper():<10} {leg.side.upper():<8} @ {leg.price_cents:.0f}¢ (fee = {leg.fee_cents:.2f}¢)")

    print("\n[SUCCESS] Cross-platform scanner verification complete.")


if __name__ == "__main__":
    asyncio.run(main())
