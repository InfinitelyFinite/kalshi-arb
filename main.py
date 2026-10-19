#!/usr/bin/env python3
"""Main entry point for kalshi-arb detection and execution engine.

Orchestrates:
1. Configuration loading and validation (`config.yaml`).
2. Unified DataFeed ingestion (Kalshi & Polymarket, supporting simulation and live).
3. NLP & Fuzzy Market Event Matching (`MarketMatcher`).
4. Step 7 Intra-Market Arbitrage Detection (`find_intra_market_opportunities`).
5. Step 8 Cross-Platform Arbitrage Detection (`find_cross_platform_opportunities`).
6. DuckDB historical orderbook snapshot storage.

Usage:
    # Run in simulation mode (produces streaming detected opportunities with 0 network calls):
    python main.py

    # Run for 60 seconds with 1.0s polling interval:
    python main.py --duration 60 --poll-interval 1.0
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import logging
from pathlib import Path
import signal
import sys
import time
from typing import Optional

# Ensure project root is on Python module search path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.config_loader import DataMode, TradingMode, load_config
from core.cross_platform_arb import find_cross_platform_opportunities
from core.data_feed import DataFeed
from core.intra_market_arb import find_intra_market_opportunities
from core.market_matcher import MarketMatcher

# Setup formatted console logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("kalshi_arb.main")


def print_banner(
    data_mode: str,
    trading_mode: str,
    poll_interval: float,
    duration: float,
    duckdb_path: str,
) -> None:
    """Print visually structured runtime configuration banner."""
    dur_str = "Continuous (Ctrl+C to stop)" if duration <= 0 else f"{duration:.1f}s"
    print("\n" + "=" * 90)
    print("  ⚡ kalshi-arb: Prediction Market Arbitrage Detection Engine")
    print("=" * 90)
    print(f"  Data Mode:         {data_mode.upper()} {'(SYNTHETIC / NO NETWORK)' if data_mode.lower() == 'simulation' else '(LIVE)'}")
    print(f"  Trading Mode:      {trading_mode.upper()}")
    print(f"  Scan Interval:     {poll_interval:.1f}s")
    print(f"  Run Duration:      {dur_str}")
    print(f"  DuckDB Snapshots:  {duckdb_path}")
    print("=" * 90 + "\n")


async def run_pipeline(
    duration: float = 0.0,
    poll_interval: Optional[float] = None,
    config_path: Optional[str] = None,
    duckdb_path: Optional[str] = None,
) -> int:
    """Run unified arbitrage detection loop against DataFeed."""
    config = load_config(config_path)

    data_mode_str = (
        config.mode.data_mode.value
        if hasattr(config.mode.data_mode, "value")
        else str(config.mode.data_mode)
    )
    trading_mode_str = (
        config.mode.trading_mode.value
        if hasattr(config.mode.trading_mode, "value")
        else str(config.mode.trading_mode)
    )

    is_sim = data_mode_str.lower() == "simulation"

    # Default poll interval: 1.5s in simulation for rapid detection, 5.0s in real
    actual_poll = poll_interval if poll_interval is not None else (1.5 if is_sim else 5.0)
    target_duckdb = duckdb_path or "data/snapshots.duckdb"

    print_banner(
        data_mode=data_mode_str,
        trading_mode=trading_mode_str,
        poll_interval=actual_poll,
        duration=duration,
        duckdb_path=target_duckdb,
    )

    feed = DataFeed(
        config=config,
        duckdb_path=target_duckdb,
        poll_interval=actual_poll,
        snapshot_interval=30.0 if is_sim else 60.0,
        enable_ws=not is_sim,
    )

    matcher = MarketMatcher.from_config(config)

    logger.info("Initializing DataFeed and auto-discovering markets...")
    await feed.start()

    logger.info(
        "DataFeed operational. Tracking %d Kalshi markets and %d Polymarket tokens.",
        len(feed.kalshi_tickers),
        len(feed.polymarket_tokens),
    )
    logger.info("Arbitrage detection engines active. Scanning for mispricings...\n")

    start_time = time.time()
    scan_count = 0
    total_intra_opps = 0
    total_cross_opps = 0

    stop_event = asyncio.Event()

    # Handle graceful termination signals
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: stop_event.set())
        except (NotImplementedError, RuntimeError):
            pass

    try:
        while not stop_event.is_set():
            if duration > 0 and (time.time() - start_time) >= duration:
                break

            scan_count += 1
            all_states = list(feed.market_state.values())

            # -------------------------------------------------------------
            # Step 7: Intra-Market Arbitrage Scan
            # -------------------------------------------------------------
            intra_opps = find_intra_market_opportunities(all_states, config=config)
            for opp in intra_opps:
                total_intra_opps += 1
                y_ask = opp.orderbook_snapshot.get("yes_ask", "--")
                n_ask = opp.orderbook_snapshot.get("no_ask", "--")
                logger.info(
                    "🔥 [OPPORTUNITY] [INTRA-MARKET] Platform=%s | Ticker=%s | Net Edge=+%.2f¢ | Gross Edge=+%.2f¢ | YES Ask=%s¢ | NO Ask=%s¢",
                    opp.platform.upper(),
                    opp.ticker,
                    opp.edge_cents,
                    opp.gross_edge_cents or opp.edge_cents,
                    y_ask,
                    n_ask,
                )

            # -------------------------------------------------------------
            # Step 8: Cross-Platform Arbitrage Scan
            # -------------------------------------------------------------
            kalshi_states = [s for s in all_states if s.platform == "kalshi"]
            poly_states = [s for s in all_states if s.platform == "polymarket"]

            if kalshi_states and poly_states:
                matches = matcher.match(kalshi_states, poly_states)

                kalshi_books = {
                    s.ticker: s.raw_orderbook
                    for s in kalshi_states
                    if s.raw_orderbook is not None
                }
                poly_books = {
                    s.ticker: s.raw_orderbook
                    for s in poly_states
                    if s.raw_orderbook is not None
                }

                cross_opps = find_cross_platform_opportunities(
                    matched_pairs=matches,
                    kalshi_orderbooks=kalshi_books,
                    polymarket_orderbooks=poly_books,
                    config=config,
                )

                for opp in cross_opps:
                    total_cross_opps += 1
                    logger.info(
                        "⚡ [OPPORTUNITY] [CROSS-PLATFORM] Kalshi=%s <-> Polymarket=%s | Net Edge=+%.2f¢ | Gross Edge=+%.2f¢ | Fees=%.2f¢",
                        opp.ticker,
                        opp.target_ticker,
                        opp.edge_cents,
                        opp.gross_edge_cents or opp.edge_cents,
                        opp.estimated_fees_cents or 0.0,
                    )
                    for i, leg in enumerate(opp.legs, 1):
                        logger.info(
                            "     └─ Leg %d: Buy %s %s @ %.1f¢ on %s",
                            i,
                            leg.side.replace("buy_", "").upper(),
                            leg.ticker,
                            leg.price_cents,
                            leg.platform.upper(),
                        )

            # Status heartbeat every 15 scan cycles
            if scan_count % 15 == 0:
                elapsed = time.time() - start_time
                logger.info(
                    "📊 Status: %d scan cycles (%.1fs elapsed) | Intra-Market Opps: %d | Cross-Platform Opps: %d | Tracked Markets: %d",
                    scan_count,
                    elapsed,
                    total_intra_opps,
                    total_cross_opps,
                    len(all_states),
                )

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=actual_poll)
            except asyncio.TimeoutError:
                pass

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown requested by user.")
    finally:
        logger.info("Stopping DataFeed and flushing snapshot buffers...")
        await feed.close()

    total_time = time.time() - start_time
    print("\n" + "=" * 90)
    print("  Simulation / Pipeline Execution Summary")
    print("=" * 90)
    print(f"  Total Duration:                   {total_time:.1f}s")
    print(f"  Scan Cycles Completed:            {scan_count}")
    print(f"  Intra-Market Arbitrages Found:    {total_intra_opps}")
    print(f"  Cross-Platform Arbitrages Found:  {total_cross_opps}")
    print("=" * 90 + "\n")
    return 0


def main() -> None:
    """CLI entry point for main.py."""
    parser = argparse.ArgumentParser(
        description="kalshi-arb: Prediction market arbitrage detection engine."
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Run duration in seconds (default: 0 = continuous)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=None,
        help="Polling scan interval in seconds (default: 1.5s in simulation, 5.0s in real)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to custom config.yaml file",
    )
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default=None,
        help="Path to DuckDB database file for orderbook snapshots",
    )
    args = parser.parse_args()

    try:
        sys.exit(
            asyncio.run(
                run_pipeline(
                    duration=args.duration,
                    poll_interval=args.poll_interval,
                    config_path=args.config,
                    duckdb_path=args.duckdb_path,
                )
            )
        )
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
