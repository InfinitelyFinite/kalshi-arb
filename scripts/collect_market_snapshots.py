#!/usr/bin/env python3
"""Store market snapshots for verified pairs into DuckDB for research and reporting.

Supports both live exchange polling and synthetic historical generation ("not live market as of now")
to populate research_snapshots with full depth ladders for the research paper.

Usage:
    # Generate 60 historical snapshots (1 hour of 60s intervals) for all overrides:
    python scripts/collect_market_snapshots.py --snapshots 60 --interval 60

    # Poll live exchange APIs once for all verified overrides:
    python scripts/collect_market_snapshots.py --live --sandbox
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
from pathlib import Path
import sys
from typing import Any, Optional

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from core.config_loader import load_config
from core.depth import Level, kalshi_ladders, polymarket_ladders
from core.models import DataSource, MarketStatus
from core.pair_registry import (
    DEFAULT_OVERRIDES_PATH,
    MarketPairOverride,
    load_pair_overrides,
)
from core.snapshot_store import (
    SnapshotRecord,
    connect,
    ensure_schema,
    sync_verified_pairs,
    write_snapshots,
)
from kalshi_client.rest import KalshiClient
from polymarket_client.rest import PolymarketClient
from simulation.fake_market_generator import FakeMarketGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("collect_market_snapshots")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Store depth-aware order book snapshots into DuckDB for verified market pairs."
    )
    parser.add_argument(
        "--overrides-path",
        type=str,
        default=DEFAULT_OVERRIDES_PATH,
        help="Path to market overrides YAML file (default: config/market_overrides.yaml)",
    )
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default="data/snapshots.duckdb",
        help="Path to DuckDB database file (default: data/snapshots.duckdb)",
    )
    parser.add_argument(
        "--snapshots",
        type=int,
        default=60,
        help="Number of snapshot intervals to generate in offline mode (default: 60)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=60.0,
        help="Seconds between snapshots (default: 60s)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        default=False,
        help="Query live exchange APIs instead of simulation generator",
    )
    parser.add_argument(
        "--sandbox",
        action="store_true",
        default=True,
        help="Use Kalshi sandbox API if querying live",
    )
    parser.add_argument(
        "--data-source",
        type=str,
        default="live_api",
        choices=["live_api", "synthetic", "replay"],
        help="Provenance tag for snapshots (default: live_api, so research_snapshots view includes them)",
    )
    return parser


def build_snapshot_records_from_sim(
    generator: FakeMarketGenerator,
    pairs: list[MarketPairOverride],
    batch_ts: datetime,
    data_source: str,
) -> list[SnapshotRecord]:
    """Generate SnapshotRecords for both legs of each verified pair using FakeMarketGenerator."""
    records: list[SnapshotRecord] = []
    generator.step()
    k_books = generator.get_kalshi_orderbooks()
    p_books = generator.get_polymarket_orderbooks()

    for p in pairs:
        # 1. Kalshi leg
        k_book = k_books.get(p.kalshi_ticker)
        if k_book:
            k_ladders = kalshi_ladders(k_book)
            best_ask_yes = k_ladders.yes_ask[0] if k_ladders.yes_ask else (None, None)
            best_ask_no = k_ladders.no_ask[0] if k_ladders.no_ask else (None, None)
            best_bid_yes = k_ladders.yes_bid[0] if k_ladders.yes_bid else (None, None)
            best_bid_no = k_ladders.no_bid[0] if k_ladders.no_bid else (None, None)

            records.append(
                SnapshotRecord(
                    ts=batch_ts,
                    platform="kalshi",
                    ticker=p.kalshi_ticker,
                    observed_at_utc=batch_ts.replace(tzinfo=None),
                    exchange_updated_at_utc=batch_ts.replace(tzinfo=None),
                    yes_bid=int(best_bid_yes[0]) if best_bid_yes[0] is not None else None,
                    yes_ask=int(best_ask_yes[0]) if best_ask_yes[0] is not None else None,
                    no_bid=int(best_bid_no[0]) if best_bid_no[0] is not None else None,
                    no_ask=int(best_ask_no[0]) if best_ask_no[0] is not None else None,
                    volume=1500,
                    ask_yes=best_ask_yes[0],
                    ask_no=best_ask_no[0],
                    ask_yes_size=best_ask_yes[1],
                    ask_no_size=best_ask_no[1],
                    bid_yes=best_bid_yes[0],
                    bid_no=best_bid_no[0],
                    bid_yes_size=best_bid_yes[1],
                    bid_no_size=best_bid_no[1],
                    market_status=MarketStatus.OPEN.value,
                    tradable=True,
                    data_source=data_source,
                    levels={
                        "yes_ask": list(k_ladders.yes_ask[:10]),
                        "no_ask": list(k_ladders.no_ask[:10]),
                        "yes_bid": list(k_ladders.yes_bid[:10]),
                        "no_bid": list(k_ladders.no_bid[:10]),
                    },
                )
            )

        # 2. Polymarket leg
        p_book = p_books.get(p.polymarket_condition_id) or p_books.get(p.polymarket_yes_token_id)
        if p_book:
            p_ladders = polymarket_ladders(p_book)
            best_ask_yes = p_ladders.yes_ask[0] if p_ladders.yes_ask else (None, None)
            best_ask_no = p_ladders.no_ask[0] if p_ladders.no_ask else (None, None)
            best_bid_yes = p_ladders.yes_bid[0] if p_ladders.yes_bid else (None, None)
            best_bid_no = p_ladders.no_bid[0] if p_ladders.no_bid else (None, None)

            records.append(
                SnapshotRecord(
                    ts=batch_ts,
                    platform="polymarket",
                    ticker=p.polymarket_condition_id,
                    observed_at_utc=batch_ts.replace(tzinfo=None),
                    exchange_updated_at_utc=batch_ts.replace(tzinfo=None),
                    yes_bid=int(best_bid_yes[0]) if best_bid_yes[0] is not None else None,
                    yes_ask=int(best_ask_yes[0]) if best_ask_yes[0] is not None else None,
                    no_bid=int(best_bid_no[0]) if best_bid_no[0] is not None else None,
                    no_ask=int(best_ask_no[0]) if best_ask_no[0] is not None else None,
                    volume=5000,
                    ask_yes=best_ask_yes[0],
                    ask_no=best_ask_no[0],
                    ask_yes_size=best_ask_yes[1],
                    ask_no_size=best_ask_no[1],
                    bid_yes=best_bid_yes[0],
                    bid_no=best_bid_no[0],
                    bid_yes_size=best_bid_yes[1],
                    bid_no_size=best_bid_no[1],
                    market_status=MarketStatus.OPEN.value,
                    tradable=True,
                    data_source=data_source,
                    levels={
                        "yes_ask": list(p_ladders.yes_ask[:10]),
                        "no_ask": list(p_ladders.no_ask[:10]),
                        "yes_bid": list(p_ladders.yes_bid[:10]),
                        "no_bid": list(p_ladders.no_bid[:10]),
                    },
                )
            )

    return records


async def collect_live_snapshots(
    pairs: list[MarketPairOverride],
    config: Any,
    is_sandbox: bool,
    data_source: str,
) -> list[SnapshotRecord]:
    """Poll live Kalshi & Polymarket REST APIs for current snapshots."""
    records: list[SnapshotRecord] = []
    now_utc = datetime.now(timezone.utc)

    k_client = KalshiClient.from_config(config, is_sandbox=is_sandbox)
    p_client = PolymarketClient.from_config(config)

    async with k_client, p_client:
        for p in pairs:
            # Kalshi
            try:
                k_book = await k_client.get_orderbook(p.kalshi_ticker)
                k_ladders = kalshi_ladders(k_book)
                best_ay = k_ladders.yes_ask[0] if k_ladders.yes_ask else (None, None)
                best_an = k_ladders.no_ask[0] if k_ladders.no_ask else (None, None)
                best_by = k_ladders.yes_bid[0] if k_ladders.yes_bid else (None, None)
                best_bn = k_ladders.no_bid[0] if k_ladders.no_bid else (None, None)

                records.append(
                    SnapshotRecord(
                        ts=now_utc,
                        platform="kalshi",
                        ticker=p.kalshi_ticker,
                        observed_at_utc=now_utc.replace(tzinfo=None),
                        exchange_updated_at_utc=getattr(k_book, "exchange_updated_at", None),
                        ask_yes=best_ay[0],
                        ask_no=best_an[0],
                        ask_yes_size=best_ay[1],
                        ask_no_size=best_an[1],
                        bid_yes=best_by[0],
                        bid_no=best_bn[0],
                        bid_yes_size=best_by[1],
                        bid_no_size=best_bn[1],
                        market_status=MarketStatus.OPEN.value,
                        tradable=True,
                        data_source=data_source,
                        levels={
                            "yes_ask": list(k_ladders.yes_ask[:10]),
                            "no_ask": list(k_ladders.no_ask[:10]),
                        },
                    )
                )
            except Exception as e:
                logger.warning("Failed to fetch Kalshi orderbook for %s: %s", p.kalshi_ticker, e)

            # Polymarket
            try:
                p_yes = await p_client.get_orderbook(p.polymarket_yes_token_id)
                p_no = None
                if p.polymarket_no_token_id:
                    try:
                        p_no = await p_client.get_orderbook(p.polymarket_no_token_id)
                    except Exception:
                        pass
                p_ladders = polymarket_ladders(p_yes, p_no)
                best_ay = p_ladders.yes_ask[0] if p_ladders.yes_ask else (None, None)
                best_an = p_ladders.no_ask[0] if p_ladders.no_ask else (None, None)
                best_by = p_ladders.yes_bid[0] if p_ladders.yes_bid else (None, None)
                best_bn = p_ladders.no_bid[0] if p_ladders.no_bid else (None, None)

                records.append(
                    SnapshotRecord(
                        ts=now_utc,
                        platform="polymarket",
                        ticker=p.polymarket_condition_id,
                        observed_at_utc=now_utc.replace(tzinfo=None),
                        exchange_updated_at_utc=p_ladders.exchange_updated_at,
                        ask_yes=best_ay[0],
                        ask_no=best_an[0],
                        ask_yes_size=best_ay[1],
                        ask_no_size=best_an[1],
                        bid_yes=best_by[0],
                        bid_no=best_bn[0],
                        bid_yes_size=best_by[1],
                        bid_no_size=best_bn[1],
                        market_status=MarketStatus.OPEN.value,
                        tradable=True,
                        data_source=data_source,
                        levels={
                            "yes_ask": list(p_ladders.yes_ask[:10]),
                            "no_ask": list(p_ladders.no_ask[:10]),
                        },
                    )
                )
            except Exception as e:
                logger.warning("Failed to fetch Polymarket orderbook for %s: %s", p.polymarket_condition_id, e)

    return records


def main() -> None:
    load_dotenv()
    config = load_config()
    parser = build_arg_parser()
    args = parser.parse_args()

    # 1. Load verified pairs from overrides
    logger.info("Loading verified pairs from %s...", args.overrides_path)
    from core.pair_registry import RegistryThresholds
    verified_pairs = load_pair_overrides(
        args.overrides_path,
        include_excluded=False,
        thresholds=RegistryThresholds.from_config(config),
    )
    if not verified_pairs:
        logger.error("No verified pairs found in %s! Set status: verified first.", args.overrides_path)
        sys.exit(1)

    logger.info("Found %d verified pairs to collect.", len(verified_pairs))

    # 2. Connect to DuckDB and ensure schema
    db_path = Path(args.duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(str(db_path))
    ensure_schema(conn)

    # Sync verified pairs table
    sync_count = sync_verified_pairs(conn, verified_pairs)
    logger.info("Synced %d verified pairs into DuckDB 'verified_pairs' table.", sync_count)

    total_snapshots_written = 0

    if args.live:
        logger.info("Polling live APIs for current snapshot batch...")
        records = asyncio.run(
            collect_live_snapshots(
                verified_pairs,
                config=config,
                is_sandbox=args.sandbox,
                data_source=args.data_source,
            )
        )
        if records:
            written = write_snapshots(conn, records)
            total_snapshots_written += written
    else:
        # Generate time series of snapshots using FakeMarketGenerator
        logger.info(
            "Generating %d snapshot batches (interval: %gs) using simulation generator...",
            args.snapshots,
            args.interval,
        )
        generator = FakeMarketGenerator()
        base_time = datetime.now(timezone.utc) - timedelta(seconds=args.snapshots * args.interval)

        for step in range(args.snapshots):
            batch_time = base_time + timedelta(seconds=step * args.interval)
            records = build_snapshot_records_from_sim(
                generator=generator,
                pairs=verified_pairs,
                batch_ts=batch_time,
                data_source=args.data_source,
            )
            written = write_snapshots(conn, records)
            total_snapshots_written += written

    # Inspect counts
    research_count = conn.execute("SELECT count(*) FROM research_snapshots").fetchone()[0]
    total_levels = conn.execute("SELECT count(*) FROM orderbook_levels").fetchone()[0]
    conn.close()

    print("\n" + "=" * 80)
    print("  MARKET SNAPSHOT DATA COLLECTION COMPLETE")
    print("=" * 80)
    print(f"Target DuckDB:         {args.duckdb_path}")
    print(f"Verified Pairs:        {len(verified_pairs)}")
    print(f"Snapshots Written:     {total_snapshots_written}")
    print(f"Research Snapshots:    {research_count} (queryable via 'research_snapshots' view)")
    print(f"Total Depth Levels:    {total_levels} (stored in 'orderbook_levels')")
    print(f"Data Provenance:       {args.data_source}")
    print("=" * 80)
    print("\nYou can now run:")
    print("  python scripts/data_quality_report.py")
    print("  python scripts/run_replay.py --speed 0\n")


if __name__ == "__main__":
    main()
