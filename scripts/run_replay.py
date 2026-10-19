#!/usr/bin/env python3
"""CLI runner for historical arbitrage replay engine.

Replays DuckDB historical orderbook snapshots through the prediction market arbitrage
detection engine at a configurable speed multiplier, reporting edge size distribution
and opportunity persistence statistics.

Usage:
    # Replay at 100x speed for a specific date range:
    python scripts/run_replay.py --speed 100 --from 2026-08-01 --to 2026-08-15

    # Run instantaneous full backtest across all recorded history:
    python scripts/run_replay.py --speed 0

    # Save summary report as JSON:
    python scripts/run_replay.py --speed 0 --output-json data/replay_report.json
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
import sys

# Ensure repository root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.config_loader import load_config
from simulation.replay_engine import ReplayEngine, ReplaySummary


def setup_logging(verbose: bool = False) -> None:
    """Configure structured logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Historical Prediction Market Arbitrage Replay Engine",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Core CLI parameters matching Step 10 specification
    parser.add_argument(
        "--speed",
        type=float,
        default=100.0,
        help="Replay speed multiplier relative to real-time (e.g. 100 = 100x real-time; 0 or -1 = instantaneous)",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        default=None,
        help="Replay start date/timestamp (e.g. '2026-08-01' or '2026-08-01 12:00:00')",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=str,
        default=None,
        help="Replay end date/timestamp (e.g. '2026-08-15' or '2026-08-15 23:59:59')",
    )

    # Database and configuration
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default="data/snapshots.duckdb",
        help="Path to DuckDB database file containing orderbook_snapshots",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config.yaml configuration file",
    )

    # Strategy and filtering controls
    parser.add_argument(
        "--strategy",
        type=str,
        choices=["all", "intra", "cross"],
        default="all",
        help="Arbitrage strategies to evaluate ('all', 'intra', 'cross')",
    )
    parser.add_argument(
        "--min-edge",
        type=float,
        default=None,
        help="Minimum net arbitrage profit edge threshold in cents",
    )
    parser.add_argument(
        "--overrides-only",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Restrict cross-platform matching strictly to config/market_overrides.yaml",
    )
    parser.add_argument(
        "--limit-snapshots",
        type=int,
        default=None,
        help="Maximum number of historical snapshots to process",
    )

    # Output options
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Path to save full summary report and opportunity records as JSON",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable detailed debug logging during replay",
    )

    return parser.parse_args()


async def main_async() -> int:
    """Async main routine."""
    args = parse_args()
    setup_logging(args.verbose)

    config = load_config(args.config)

    include_intra = args.strategy in ("all", "intra")
    include_cross = args.strategy in ("all", "cross")

    engine = ReplayEngine(
        duckdb_path=args.duckdb_path,
        speed=args.speed,
        start_date=args.from_date,
        end_date=args.to_date,
        config=config,
        min_edge=args.min_edge,
        overrides_only=args.overrides_only,
        include_intra=include_intra,
        include_cross=include_cross,
        limit_snapshots=args.limit_snapshots,
    )

    summary: ReplaySummary = await engine.run()

    # Print formatted text report
    report_text = summary.format_report()
    print(report_text)

    # Optional JSON output
    if args.output_json:
        summary.save_json(args.output_json)
        print(f"✅ Report saved to {args.output_json}")

    return 0


def main() -> None:
    """Synchronous entry point."""
    try:
        sys.exit(asyncio.run(main_async()))
    except KeyboardInterrupt:
        print("\n[INFO] Replay interrupted by user.")
        sys.exit(0)
    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
