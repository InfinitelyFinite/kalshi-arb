#!/usr/bin/env python3
"""Run unified DataFeed streaming and polling against Kalshi & Polymarket, snapshotting to DuckDB.

Usage:
    # Run for 5 minutes with 60s snapshots (Default definition of done):
    python scripts/run_data_feed.py --duration 300 --snapshot-interval 60 --sandbox

    # Quick test (15s run with 5s snapshots):
    python scripts/run_data_feed.py --duration 15 --snapshot-interval 5 --sandbox
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
import sys
import time

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
import duckdb

from core.config_loader import load_config
from core.data_feed import DataFeed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_data_feed")


def print_banner(duration: float, snapshot_interval: float, duckdb_path: str, is_sandbox: bool) -> None:
    print("=" * 95)
    print("  Unified DataFeed (Kalshi & Polymarket) + DuckDB Orderbook Snapshots")
    print("=" * 95)
    print(f"Environment:       {'SANDBOX' if is_sandbox else 'PRODUCTION'}")
    print(f"Duration:          {duration:.1f}s")
    print(f"Snapshot Interval: {snapshot_interval:.1f}s")
    print(f"DuckDB Output:     {duckdb_path}")
    print("=" * 95 + "\n")


async def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Stream and snapshot Kalshi & Polymarket market data to DuckDB.")
    parser.add_argument("--duration", type=float, default=300.0, help="Total run duration in seconds (default: 300s / 5 min)")
    parser.add_argument("--snapshot-interval", type=float, default=60.0, help="DuckDB snapshot interval in seconds (default: 60s)")
    parser.add_argument("--poll-interval", type=float, default=10.0, help="REST polling interval in seconds (default: 10s)")
    parser.add_argument("--duckdb-path", type=str, default="data/snapshots.duckdb", help="Target DuckDB file path")
    parser.add_argument("--sandbox", action="store_true", help="Force Kalshi sandbox mode")
    parser.add_argument("--prod", action="store_true", help="Force Kalshi production mode")
    parser.add_argument("--no-ws", action="store_true", help="Disable WebSocket streaming (poll-only)")
    parser.add_argument("--kalshi-tickers", type=str, default="", help="Comma-separated Kalshi tickers (auto-discovered if empty)")
    parser.add_argument("--polymarket-tokens", type=str, default="", help="Comma-separated Polymarket token IDs (auto-discovered if empty)")
    args = parser.parse_args()

    config = load_config()

    if args.prod:
        is_sandbox = False
    elif args.sandbox:
        is_sandbox = True
    else:
        is_sandbox = config.mode.trading_mode.value.lower() in ("sandbox", "dry_run")

    print_banner(args.duration, args.snapshot_interval, args.duckdb_path, is_sandbox)

    k_tickers = [t.strip() for t in args.kalshi_tickers.split(",") if t.strip()]
    p_tokens = [t.strip() for t in args.polymarket_tokens.split(",") if t.strip()]

    feed = DataFeed(
        config=config,
        kalshi_tickers=k_tickers,
        polymarket_tokens=p_tokens,
        duckdb_path=args.duckdb_path,
        snapshot_interval=args.snapshot_interval,
        poll_interval=args.poll_interval,
        is_sandbox=is_sandbox,
        enable_ws=not args.no_ws,
    )

    start_time = time.time()

    try:
        print("[1/3] Initializing and discovering markets...")
        await feed.start()

        print(f"\n[2/3] Feed active! Tracking {len(feed.kalshi_tickers)} Kalshi markets and {len(feed.polymarket_tokens)} Polymarket markets.")
        print("     Snapshots are recorded to DuckDB on interval. Press Ctrl+C to stop early.\n")

        last_display = 0.0
        while (time.time() - start_time) < args.duration:
            await asyncio.sleep(1.0)
            elapsed = time.time() - start_time

            # Print live state summary every 15 seconds
            if elapsed - last_display >= 15.0:
                last_display = elapsed
                print(f"[{elapsed:5.1f}s / {args.duration:5.1f}s] Active Market States ({len(feed.market_state)} total):")
                sample_states = list(feed.market_state.values())[:6]
                for s in sample_states:
                    y_b = f"{s.yes_bid}¢" if s.yes_bid is not None else "--"
                    y_a = f"{s.yes_ask}¢" if s.yes_ask is not None else "--"
                    print(f"   [{s.platform.upper():<10}] {s.ticker:<30} YES: {y_b:>4} / {y_a:<4} | Vol: {s.volume}")
                print()

    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nFeed stopped by user.")
    finally:
        print("\n[3/3] Shutting down DataFeed...")
        await feed.close()

    total_elapsed = time.time() - start_time
    print(f"\nCompleted run in {total_elapsed:.1f} seconds.")

    # Query and display DuckDB verification report
    db_file = Path(args.duckdb_path)
    if not db_file.exists():
        print(f"[WARN] DuckDB file {args.duckdb_path} was not created.")
        return 1

    print("\n" + "=" * 95)
    print("  DuckDB Verification: orderbook_snapshots")
    print("=" * 95)

    conn = duckdb.connect(str(db_file))
    try:
        print("\n--- Summary Table (Market Snapshot Row Counts) ---")
        conn.sql("""
            SELECT 
                platform,
                ticker,
                count(*) AS snapshot_count,
                min(ts) AS first_snapshot,
                max(ts) AS latest_snapshot
            FROM orderbook_snapshots
            GROUP BY platform, ticker
            ORDER BY platform, snapshot_count DESC;
        """).show()

        print("\n--- Sample Snapshot Records (duckdb.sql('select * from orderbook_snapshots limit 5')) ---")
        conn.sql("SELECT * FROM orderbook_snapshots ORDER BY ts DESC LIMIT 5;").show()

        total_rows = conn.sql("SELECT count(*) FROM orderbook_snapshots;").fetchone()[0]
        print(f"\nTotal Orderbook Snapshot Rows: {total_rows}")
    finally:
        conn.close()

    print("\n[SUCCESS] DataFeed DuckDB snapshotting complete.")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
