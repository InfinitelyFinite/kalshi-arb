#!/usr/bin/env python3
"""Sustained real-market snapshot collection script for research studies.

Collects high-frequency (e.g. 5-second) depth-aware order book snapshots from
real Kalshi and Polymarket APIs across verified market pairs for sustained durations
(24-hour pilot, 7-day research study, or configurable periods).

Key Features:
- Configurable duration (--duration-hours) and snapshot interval (--snapshot-interval).
- Restricts collection to verified pairs (--overrides-only) from market_overrides.yaml.
- Robust network error recovery with exponential backoff and rate limit handling.
- Atomic DuckDB persistence with top-N depth ladder storage.
- Session metadata tracking (study_sessions table) and resume capability (--resume).
- Periodic inline data-quality checks.
- Clean signal handling (SIGINT / SIGTERM) for safe interruption and resumption.

Usage:
    # 24-hour pilot at 5s interval:
    python scripts/collect_study.py --duration-hours 24 --snapshot-interval 5 --overrides-only

    # 7-day (168-hour) full study:
    python scripts/collect_study.py --duration-hours 168 --snapshot-interval 5 --overrides-only

    # Resume an interrupted study:
    python scripts/collect_study.py --resume --duration-hours 168 --snapshot-interval 5
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
import math
import os
from pathlib import Path
import signal
import sys
import time
from typing import Any, Optional
import uuid

# Ensure repository root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
import duckdb

from core.config_loader import AppConfig, load_config
from core.depth import Level, kalshi_ladders, polymarket_ladders
from core.models import DataSource, MarketStatus
from core.pair_registry import (
    DEFAULT_OVERRIDES_PATH,
    MarketPairOverride,
    RegistryThresholds,
    load_pair_overrides,
)
from core.snapshot_store import (
    SnapshotRecord,
    connect,
    ensure_schema,
    sync_verified_pairs,
    to_naive_utc,
    write_snapshots,
)
from kalshi_client.rest import KalshiAPIError, KalshiClient
from polymarket_client.rest import PolymarketAPIError, PolymarketClient
from scripts.data_quality_report import DataQualityAnalyzer

logger = logging.getLogger("collect_study")


def setup_logging(log_file: Optional[str] = None, verbose: bool = False) -> None:
    """Configure structured console and optional file logging."""
    level = logging.DEBUG if verbose else logging.INFO
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(level=level, format=log_format, datefmt=date_format, handlers=handlers, force=True)


def build_arg_parser() -> argparse.ArgumentParser:
    """Construct CLI argument parser for sustained study collection."""
    parser = argparse.ArgumentParser(
        description="Sustained real-market snapshot collection for prediction market research.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--duration-hours",
        type=float,
        default=24.0,
        help="Target collection duration in hours (e.g. 24 for pilot, 168 for 7-day study)",
    )
    parser.add_argument(
        "--snapshot-interval",
        type=float,
        default=5.0,
        help="Nominal interval between snapshot batches in seconds (e.g. 5.0s)",
    )
    parser.add_argument(
        "--overrides-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Restrict collection strictly to verified pairs from market_overrides.yaml",
    )
    parser.add_argument(
        "--overrides-path",
        type=str,
        default=DEFAULT_OVERRIDES_PATH,
        help="Path to YAML market pair overrides registry",
    )
    parser.add_argument(
        "--duckdb-path",
        "--db",
        dest="duckdb_path",
        type=str,
        default="data/snapshots.duckdb",
        help="Target DuckDB database file path",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="Resume the most recent running or interrupted collection session",
    )
    parser.add_argument(
        "--quality-check-interval-mins",
        type=float,
        default=60.0,
        help="Run inline data quality audit every N minutes (0 to disable)",
    )
    parser.add_argument(
        "--sandbox",
        action="store_true",
        default=False,
        help="Force Kalshi sandbox API mode",
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        default=False,
        help="Force production APIs mode",
    )
    parser.add_argument(
        "--data-source",
        type=str,
        default="live_api",
        help="Data source provenance tag (must be 'live_api' for research views)",
    )
    parser.add_argument(
        "--depth-levels",
        type=int,
        default=10,
        help="Top-N depth levels per side stored in orderbook_levels",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default="data/collect_study.log",
        help="Path to write structured collection logs",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retry attempts on transient network/API failures",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Enable verbose DEBUG logging",
    )
    return parser


def ensure_sessions_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Ensure study_sessions table exists for tracking session metadata."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS study_sessions (
            session_id VARCHAR PRIMARY KEY,
            started_at_utc TIMESTAMP,
            ended_at_utc TIMESTAMP,
            target_duration_hours DOUBLE,
            snapshot_interval_seconds DOUBLE,
            overrides_path VARCHAR,
            pairs_count INTEGER,
            total_batches INTEGER,
            total_snapshots INTEGER,
            total_levels INTEGER,
            error_count INTEGER,
            status VARCHAR,
            notes VARCHAR
        )
        """
    )


def register_session(
    conn: duckdb.DuckDBPyConnection,
    session_id: str,
    target_hours: float,
    interval_s: float,
    overrides_path: str,
    pairs_count: int,
) -> None:
    """Register a new collection session in the metadata table."""
    conn.execute(
        """
        INSERT INTO study_sessions (
            session_id, started_at_utc, target_duration_hours,
            snapshot_interval_seconds, overrides_path, pairs_count,
            total_batches, total_snapshots, total_levels, error_count,
            status, notes
        ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 'RUNNING', '')
        """,
        [
            session_id,
            datetime.now(timezone.utc).replace(tzinfo=None),
            target_hours,
            interval_s,
            overrides_path,
            pairs_count,
        ],
    )


def update_session_progress(
    conn: duckdb.DuckDBPyConnection,
    session_id: str,
    batches: int,
    snapshots: int,
    levels: int,
    errors: int,
    status: str = "RUNNING",
    notes: str = "",
) -> None:
    """Update progress counters and status for the active collection session."""
    conn.execute(
        """
        UPDATE study_sessions
        SET total_batches = ?,
            total_snapshots = ?,
            total_levels = ?,
            error_count = ?,
            status = ?,
            notes = ?,
            ended_at_utc = CASE WHEN ? IN ('COMPLETED', 'INTERRUPTED', 'FAILED') THEN ? ELSE ended_at_utc END
        WHERE session_id = ?
        """,
        [
            batches,
            snapshots,
            levels,
            errors,
            status,
            notes,
            status,
            datetime.now(timezone.utc).replace(tzinfo=None),
            session_id,
        ],
    )


def find_resumable_session(conn: duckdb.DuckDBPyConnection) -> Optional[dict[str, Any]]:
    """Look up the most recent incomplete or interrupted study collection session."""
    try:
        row = conn.execute(
            """
            SELECT session_id, started_at_utc, target_duration_hours,
                   snapshot_interval_seconds, pairs_count, total_batches,
                   total_snapshots, total_levels, error_count, status
            FROM study_sessions
            WHERE status IN ('RUNNING', 'INTERRUPTED')
            ORDER BY started_at_utc DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        return {
            "session_id": row[0],
            "started_at_utc": row[1],
            "target_duration_hours": row[2],
            "snapshot_interval_seconds": row[3],
            "pairs_count": row[4],
            "total_batches": row[5],
            "total_snapshots": row[6],
            "total_levels": row[7],
            "error_count": row[8],
            "status": row[9],
        }
    except Exception:
        return None


class SustainedStudyCollector:
    """Orchestrates sustained real-market data gathering from Kalshi and Polymarket."""

    def __init__(
        self,
        config: AppConfig,
        pairs: list[MarketPairOverride],
        duckdb_path: str,
        duration_hours: float,
        snapshot_interval: float,
        overrides_path: str,
        is_sandbox: bool = False,
        data_source: str = "live_api",
        depth_levels: int = 10,
        quality_check_interval_mins: float = 60.0,
        max_retries: int = 3,
        resume: bool = False,
    ) -> None:
        self.config = config
        self.pairs = pairs
        self.duckdb_path = duckdb_path
        self.duration_hours = duration_hours
        self.snapshot_interval = snapshot_interval
        self.overrides_path = overrides_path
        self.is_sandbox = is_sandbox
        self.data_source = data_source
        self.depth_levels = depth_levels
        self.quality_check_interval_mins = quality_check_interval_mins
        self.max_retries = max_retries
        self.resume = resume

        self._shutdown_requested = False
        self.session_id = f"study-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        self.total_batches = 0
        self.total_snapshots = 0
        self.total_levels = 0
        self.total_errors = 0
        self.start_wall_time = time.time()
        self.target_seconds = duration_hours * 3600.0

        # Pre-extract platform tickers
        self.kalshi_tickers = [p.kalshi_ticker for p in self.pairs if p.kalshi_ticker]
        self.polymarket_tokens: list[str] = []
        for p in self.pairs:
            if p.polymarket_yes_token_id:
                self.polymarket_tokens.append(p.polymarket_yes_token_id)
            if p.polymarket_no_token_id:
                self.polymarket_tokens.append(p.polymarket_no_token_id)

    def request_shutdown(self, signum: int, frame: Any) -> None:
        """Handle interrupt signal gracefully."""
        sig_name = signal.Signals(signum).name
        logger.warning("Received signal %s. Initiating graceful study collection shutdown...", sig_name)
        self._shutdown_requested = True

    async def _fetch_kalshi_books(
        self,
        client: KalshiClient,
    ) -> dict[str, Any]:
        """Fetch orderbooks for Kalshi tickers with retry and exponential backoff."""
        for attempt in range(1, self.max_retries + 1):
            try:
                books = await client.get_orderbooks(self.kalshi_tickers)
                return books
            except Exception as e:
                backoff = min(8.0, (2 ** (attempt - 1)) * 0.5)
                logger.warning(
                    "Kalshi orderbook fetch attempt %d/%d failed: %s. Backing off %.1fs...",
                    attempt,
                    self.max_retries,
                    e,
                    backoff,
                )
                if attempt == self.max_retries:
                    self.total_errors += 1
                    return {}
                await asyncio.sleep(backoff)
        return {}

    async def _fetch_polymarket_books(
        self,
        client: PolymarketClient,
    ) -> dict[str, Any]:
        """Fetch orderbooks for Polymarket tokens with retry and exponential backoff."""
        for attempt in range(1, self.max_retries + 1):
            try:
                books = await client.get_orderbooks(self.polymarket_tokens)
                return books
            except Exception as e:
                backoff = min(8.0, (2 ** (attempt - 1)) * 0.5)
                logger.warning(
                    "Polymarket orderbook fetch attempt %d/%d failed: %s. Backing off %.1fs...",
                    attempt,
                    self.max_retries,
                    e,
                    backoff,
                )
                if attempt == self.max_retries:
                    self.total_errors += 1
                    return {}
                await asyncio.sleep(backoff)
        return {}

    def _build_snapshot_records(
        self,
        batch_ts: datetime,
        k_books: dict[str, Any],
        p_books: dict[str, Any],
    ) -> list[SnapshotRecord]:
        """Transform raw orderbook payloads into validated SnapshotRecords."""
        records: list[SnapshotRecord] = []
        naive_ts = batch_ts.replace(tzinfo=None)

        for p in self.pairs:
            # 1. Kalshi leg
            k_book = k_books.get(p.kalshi_ticker)
            if k_book:
                ladders = kalshi_ladders(k_book)
                best_ay = ladders.yes_ask[0] if ladders.yes_ask else (None, None)
                best_an = ladders.no_ask[0] if ladders.no_ask else (None, None)
                best_by = ladders.yes_bid[0] if ladders.yes_bid else (None, None)
                best_bn = ladders.no_bid[0] if ladders.no_bid else (None, None)

                records.append(
                    SnapshotRecord(
                        ts=batch_ts,
                        platform="kalshi",
                        ticker=p.kalshi_ticker,
                        observed_at_utc=naive_ts,
                        exchange_updated_at_utc=to_naive_utc(ladders.exchange_updated_at),
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
                        data_source=self.data_source,
                        levels={
                            "yes_ask": list(ladders.yes_ask[: self.depth_levels]),
                            "no_ask": list(ladders.no_ask[: self.depth_levels]),
                            "yes_bid": list(ladders.yes_bid[: self.depth_levels]),
                            "no_bid": list(ladders.no_bid[: self.depth_levels]),
                        },
                    )
                )

            # 2. Polymarket leg
            p_yes = p_books.get(p.polymarket_yes_token_id)
            p_no = p_books.get(p.polymarket_no_token_id) if p.polymarket_no_token_id else None

            if p_yes:
                ladders = polymarket_ladders(p_yes, p_no)
                best_ay = ladders.yes_ask[0] if ladders.yes_ask else (None, None)
                best_an = ladders.no_ask[0] if ladders.no_ask else (None, None)
                best_by = ladders.yes_bid[0] if ladders.yes_bid else (None, None)
                best_bn = ladders.no_bid[0] if ladders.no_bid else (None, None)

                records.append(
                    SnapshotRecord(
                        ts=batch_ts,
                        platform="polymarket",
                        ticker=p.polymarket_condition_id,
                        observed_at_utc=naive_ts,
                        exchange_updated_at_utc=to_naive_utc(ladders.exchange_updated_at),
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
                        data_source=self.data_source,
                        levels={
                            "yes_ask": list(ladders.yes_ask[: self.depth_levels]),
                            "no_ask": list(ladders.no_ask[: self.depth_levels]),
                            "yes_bid": list(ladders.yes_bid[: self.depth_levels]),
                            "no_bid": list(ladders.no_bid[: self.depth_levels]),
                        },
                    )
                )

        return records

    def _run_inline_quality_audit(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Run periodic inline data quality check and log audit statistics."""
        try:
            logger.info("Running periodic inline data quality audit...")
            analyzer = DataQualityAnalyzer(
                conn=conn,
                config=self.config,
                snapshot_interval=self.snapshot_interval,
                use_raw_table=False,
            )
            audit = analyzer.analyze()
            snap_counts = audit.get("snapshot_counts", {})
            gaps = audit.get("gaps", {})
            freshness = audit.get("quote_freshness", {})
            logger.info(
                "[DATA QUALITY AUDIT] Reliability: %s | Snapshots: %s (Coverage: %.1f%%) | Gaps > 300s: %d | Stale: %.1f%%",
                audit.get("overall_reliability", "N/A").upper(),
                snap_counts.get("total_analyzed", 0),
                snap_counts.get("coverage_pct", 0.0),
                gaps.get("count", 0),
                freshness.get("stale_quote_pct", 0.0),
            )
        except Exception as e:
            logger.warning("Inline data quality audit encountered a warning: %s", e)

    async def run(self) -> dict[str, Any]:
        """Execute the sustained market data collection loop."""
        db_path = Path(self.duckdb_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = connect(str(db_path))
        ensure_schema(conn)
        ensure_sessions_schema(conn)
        sync_verified_pairs(conn, self.pairs)

        # Handle session resumption if requested
        if self.resume:
            existing = find_resumable_session(conn)
            if existing:
                self.session_id = existing["session_id"]
                self.total_batches = existing["total_batches"]
                self.total_snapshots = existing["total_snapshots"]
                self.total_levels = existing["total_levels"]
                self.total_errors = existing["error_count"]
                logger.info(
                    "Resuming active session %s: already recorded %d batches (%d snapshots).",
                    self.session_id,
                    self.total_batches,
                    self.total_snapshots,
                )
            else:
                logger.info("No prior resumable session found; starting fresh session %s.", self.session_id)
                register_session(
                    conn,
                    self.session_id,
                    self.duration_hours,
                    self.snapshot_interval,
                    self.overrides_path,
                    len(self.pairs),
                )
        else:
            register_session(
                conn,
                self.session_id,
                self.duration_hours,
                self.snapshot_interval,
                self.overrides_path,
                len(self.pairs),
            )

        logger.info(
            "Starting sustained study collection | Session: %s | Duration: %.1fh (%.1fs) | Interval: %.1fs | Pairs: %d",
            self.session_id,
            self.duration_hours,
            self.target_seconds,
            self.snapshot_interval,
            len(self.pairs),
        )

        k_client = KalshiClient.from_config(self.config, is_sandbox=self.is_sandbox)
        p_client = PolymarketClient.from_config(self.config)

        last_quality_check = time.time()
        status = "COMPLETED"

        async with k_client, p_client:
            while not self._shutdown_requested:
                elapsed = time.time() - self.start_wall_time
                if elapsed >= self.target_seconds:
                    logger.info("Target duration of %.2f hours reached.", self.duration_hours)
                    break

                step_start = time.time()
                batch_ts = datetime.now(timezone.utc)

                # Fetch books from both exchanges concurrently
                k_task = self._fetch_kalshi_books(k_client)
                p_task = self._fetch_polymarket_books(p_client)
                k_books, p_books = await asyncio.gather(k_task, p_task)

                records = self._build_snapshot_records(batch_ts, k_books, p_books)
                if records:
                    written = write_snapshots(conn, records)
                    self.total_snapshots += written
                    levels_in_batch = sum(len(lvls) for r in records for lvls in r.levels.values())
                    self.total_levels += levels_in_batch
                    self.total_batches += 1

                # Update session table every 10 batches
                if self.total_batches % 10 == 0:
                    update_session_progress(
                        conn,
                        self.session_id,
                        self.total_batches,
                        self.total_snapshots,
                        self.total_levels,
                        self.total_errors,
                        status="RUNNING",
                    )

                # Periodic quality check
                if self.quality_check_interval_mins > 0:
                    if (time.time() - last_quality_check) >= (self.quality_check_interval_mins * 60.0):
                        self._run_inline_quality_audit(conn)
                        last_quality_check = time.time()

                # Pace the snapshot interval
                iteration_duration = time.time() - step_start
                sleep_sec = max(0.0, self.snapshot_interval - iteration_duration)
                if sleep_sec > 0:
                    try:
                        await asyncio.sleep(sleep_sec)
                    except asyncio.CancelledError:
                        break

            if self._shutdown_requested:
                status = "INTERRUPTED"

        # Finalize session
        update_session_progress(
            conn,
            self.session_id,
            self.total_batches,
            self.total_snapshots,
            self.total_levels,
            self.total_errors,
            status=status,
            notes=f"Finished with status {status}. Total batches: {self.total_batches}",
        )
        conn.close()

        total_elapsed = time.time() - self.start_wall_time
        summary = {
            "session_id": self.session_id,
            "status": status,
            "duration_hours": round(total_elapsed / 3600.0, 3),
            "target_hours": self.duration_hours,
            "snapshot_interval": self.snapshot_interval,
            "total_batches": self.total_batches,
            "total_snapshots": self.total_snapshots,
            "total_levels": self.total_levels,
            "error_count": self.total_errors,
            "duckdb_path": self.duckdb_path,
        }
        return summary


def print_summary_banner(summary: dict[str, Any]) -> None:
    """Print structured summary box at conclusion of collection."""
    print("\n" + "=" * 85)
    print("  🏁 SUSTAINED MARKET DATA COLLECTION SUMMARY")
    print("=" * 85)
    print(f"  Session ID:          {summary['session_id']}")
    print(f"  Final Status:        {summary['status']}")
    print(f"  Elapsed Time:        {summary['duration_hours']:.2f} hours (target: {summary['target_hours']:.1f}h)")
    print(f"  Snapshot Interval:   {summary['snapshot_interval']:.1f}s")
    print(f"  Total Batches:       {summary['total_batches']:,}")
    print(f"  Snapshots Written:   {summary['total_snapshots']:,}")
    print(f"  Depth Levels Stored: {summary['total_levels']:,}")
    print(f"  Network/API Errors:  {summary['error_count']}")
    print(f"  Target Database:     {summary['duckdb_path']}")
    print("=" * 85)
    print("\nNext recommended steps:")
    print("  # Inspect data quality:")
    print(f"  python scripts/data_quality_report.py --db {summary['duckdb_path']} --out results/")
    print("  # Run empirical study analysis across parameter scenarios:")
    print(f"  python scripts/analyze_study.py --db {summary['duckdb_path']} --out results/\n")


def main() -> None:
    load_dotenv()
    config = load_config()
    parser = build_arg_parser()
    args = parser.parse_args()

    setup_logging(log_file=args.log_file, verbose=args.verbose)

    # Determine execution environment (sandbox vs prod)
    if args.prod:
        is_sandbox = False
    elif args.sandbox:
        is_sandbox = True
    else:
        is_sandbox = config.mode.trading_mode.value.lower() in ("sandbox", "dry_run")

    # Load verified overrides
    logger.info("Loading verified market pairs from %s...", args.overrides_path)
    thresholds = RegistryThresholds.from_config(config)
    verified_pairs = load_pair_overrides(
        args.overrides_path,
        include_excluded=False,
        thresholds=thresholds,
    )
    if not verified_pairs:
        logger.error("No verified pairs found in %s! Halting collection.", args.overrides_path)
        sys.exit(1)

    logger.info("Found %d verified pairs available for sustained collection.", len(verified_pairs))

    collector = SustainedStudyCollector(
        config=config,
        pairs=verified_pairs,
        duckdb_path=args.duckdb_path,
        duration_hours=args.duration_hours,
        snapshot_interval=args.snapshot_interval,
        overrides_path=args.overrides_path,
        is_sandbox=is_sandbox,
        data_source=args.data_source,
        depth_levels=args.depth_levels,
        quality_check_interval_mins=args.quality_check_interval_mins,
        max_retries=args.max_retries,
        resume=args.resume,
    )

    # Attach signal handlers for graceful stop
    signal.signal(signal.SIGINT, collector.request_shutdown)
    signal.signal(signal.SIGTERM, collector.request_shutdown)

    try:
        summary = asyncio.run(collector.run())
        print_summary_banner(summary)
    except KeyboardInterrupt:
        logger.info("Collection process stopped by user.")


if __name__ == "__main__":
    main()
