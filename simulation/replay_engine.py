"""Historical replay engine for prediction market arbitrage backtesting.

Reads orderbook snapshot rows from DuckDB (populated over real runtime by Step 5 DataFeed),
ordered chronologically by timestamp, and replays them through the DataFeed and
arbitrage detection engines at a configurable speed multiplier (e.g. 100x real-time).

Key Features:
- Replays historical market states at real-time, scaled (e.g. 100x), or instantaneous speeds.
- Evaluates Intra-Market and Cross-Platform arbitrage detectors on each snapshot.
- Tracks opportunity lifecycles: appearance, persistence across consecutive snapshots,
  disappearance, and half-life / duration.
- Computes comprehensive quantitative summary statistics: edge distribution (min, max,
  mean, median, percentiles, histogram) and duration distribution.
- Emits formatted reports and JSON summaries for decision-making.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
import math
from pathlib import Path
import statistics
import sys
import time
from typing import Any, AsyncIterator, Callable, Optional, Sequence, Union

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from core.config_loader import AppConfig, load_config
from core.cross_platform_arb import (
    build_cross_platform_executability_inputs,
    evaluate_cross_platform_opportunities,
    find_cross_platform_opportunities,
)
from core.data_feed import DataFeed, MarketState
from core.depth import Level
from core.executability import (
    Classification,
    Episode,
    EpisodeTracker,
    ExecutabilityParams,
    LatencyStatus,
    OpportunityEvaluation,
    evaluate_opportunity,
)
from core.intra_market_arb import (
    build_intra_market_executability_input,
    evaluate_intra_market_opportunities,
    find_intra_market_opportunities,
)
from core.market_matcher import MarketMatcher, load_market_overrides
from core.models import Opportunity
from core.opportunity_store import (
    ensure_executability_schema,
    save_param_set,
    write_episodes_and_latencies,
    write_evaluations_and_counts,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Domain Models for Replay Tracking
# ============================================================================


class OpportunityRecord(BaseModel):
    """Lifecycle record for an arbitrage opportunity during historical replay.

    Tracks when an opportunity first appeared, how many consecutive snapshots it
    persisted across, when it disappeared, and edge statistics over its lifetime.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    opportunity_id: str = Field(description="Unique fingerprint for this opportunity")
    strategy: str = Field(description="Strategy type ('intra_market' or 'cross_platform')")
    platform: str = Field(description="Primary execution platform")
    ticker: str = Field(description="Primary market ticker / identifier")
    target_platform: Optional[str] = Field(default=None, description="Secondary platform for cross-platform")
    target_ticker: Optional[str] = Field(default=None, description="Secondary ticker for cross-platform")
    side_actions: list[str] = Field(default_factory=list, description="Side actions, e.g. ['buy_yes', 'buy_no']")

    start_ts: datetime = Field(description="Snapshot timestamp when opportunity was first detected")
    last_seen_ts: datetime = Field(description="Snapshot timestamp when opportunity was last detected")
    end_ts: Optional[datetime] = Field(default=None, description="Snapshot timestamp when opportunity disappeared")

    duration_seconds: float = Field(default=0.0, description="Total persistence duration in seconds (end_ts - start_ts)")
    persistence_span_seconds: float = Field(
        default=0.0,
        description="Duration between first and last confirmed detection (last_seen_ts - start_ts)",
    )
    snapshots_count: int = Field(default=1, description="Number of consecutive snapshots where opportunity persisted")

    initial_edge_cents: float = Field(description="Net edge in cents when first detected")
    min_edge_cents: float = Field(description="Minimum net edge in cents observed")
    max_edge_cents: float = Field(description="Maximum net edge in cents observed")
    mean_edge_cents: float = Field(description="Average net edge in cents observed")
    edge_samples: list[float] = Field(default_factory=list, description="All observed edge samples across snapshots")

    gross_edge_cents: Optional[float] = Field(default=None, description="Gross edge before fees in cents")
    estimated_fees_cents: Optional[float] = Field(default=0.0, description="Estimated transaction fees in cents")
    legs: list[dict[str, Any]] = Field(default_factory=list, description="Execution legs details")
    is_active: bool = Field(default=True, description="Whether opportunity is currently active in replay")

    # Step 12 executability extensions
    classification: Optional[str] = Field(default=None, description="Classification level (e.g. EXECUTABLE_CANDIDATE)")
    primary_reject_reason: Optional[str] = Field(default=None, description="Primary rejection reason if not candidate")
    size_q: Optional[int] = Field(default=None, description="Trade size q")
    vwap_edge_cents: Optional[float] = Field(default=None, description="Net edge computed with depth VWAP")
    duration_lower_s: Optional[float] = Field(default=None, description="Lower bound on persistence duration")
    duration_upper_s: Optional[float] = Field(default=None, description="Upper bound on persistence duration")
    censored_left: bool = Field(default=False, description="Whether episode was already open at replay start")
    censored_right: bool = Field(default=False, description="Whether episode was still open at replay end")
    latency_results: dict[str, str] = Field(default_factory=dict, description="Latency status per latency L")

    def record_update(self, opp: Opportunity, current_ts: datetime) -> None:
        """Update opportunity record when observed in consecutive snapshot."""
        self.last_seen_ts = current_ts
        self.snapshots_count += 1
        edge = float(opp.edge_cents)
        self.edge_samples.append(edge)
        self.min_edge_cents = min(self.min_edge_cents, edge)
        self.max_edge_cents = max(self.max_edge_cents, edge)
        self.mean_edge_cents = sum(self.edge_samples) / len(self.edge_samples)
        if opp.gross_edge_cents is not None:
            self.gross_edge_cents = float(opp.gross_edge_cents)
        if opp.estimated_fees_cents is not None:
            self.estimated_fees_cents = float(opp.estimated_fees_cents)
        self.persistence_span_seconds = max(0.0, (self.last_seen_ts - self.start_ts).total_seconds())

    def close(self, disappeared_ts: datetime) -> None:
        """Close opportunity record upon disappearance."""
        self.end_ts = disappeared_ts
        self.is_active = False
        self.duration_seconds = max(0.0, (self.end_ts - self.start_ts).total_seconds())
        self.persistence_span_seconds = max(0.0, (self.last_seen_ts - self.start_ts).total_seconds())


class ReplaySummary(BaseModel):
    """Quantitative summary report of historical replay execution."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    total_snapshots: int = Field(default=0, description="Total distinct snapshot timestamps processed")
    total_rows: int = Field(default=0, description="Total DuckDB orderbook rows ingested")
    start_ts: Optional[datetime] = Field(default=None, description="Start timestamp of replayed data")
    end_ts: Optional[datetime] = Field(default=None, description="End timestamp of replayed data")
    simulated_duration_seconds: float = Field(default=0.0, description="Total simulated time span in seconds")
    wall_clock_duration_seconds: float = Field(default=0.0, description="Execution time in wall-clock seconds")
    speed_multiplier: float = Field(default=1.0, description="Replay speed multiplier relative to real-time")

    total_opportunities_found: int = Field(default=0, description="Total unique opportunity episodes detected")
    total_opportunity_instances: int = Field(default=0, description="Total opportunity detections summed over snapshots")
    intra_market_count: int = Field(default=0, description="Intra-market opportunities count")
    cross_platform_count: int = Field(default=0, description="Cross-platform opportunities count")

    edge_stats: dict[str, Any] = Field(default_factory=dict, description="Edge size statistical distribution")
    duration_stats: dict[str, Any] = Field(default_factory=dict, description="Persistence duration statistical distribution")
    opportunities: list[OpportunityRecord] = Field(default_factory=list, description="All completed opportunity records")

    # Step 12 executability extensions
    evaluations_count: int = Field(default=0, description="Total opportunity evaluations across sizes and parameter sets")
    episodes_count: int = Field(default=0, description="Total distinct episodes tracked")
    executable_episodes_count: int = Field(default=0, description="Total executable candidate episodes tracked")
    param_set_id: Optional[str] = Field(default=None, description="Param set identifier for persisted run")

    def format_report(self) -> str:
        """Format an ASCII report detailing replay results and statistics."""
        lines: list[str] = []
        lines.append("\n" + "=" * 90)
        lines.append("  📊 HISTORICAL REPLAY ARBITRAGE REPORT")
        lines.append("=" * 90)

        # 1. Execution Scope
        lines.append("  [1] Replay Execution Scope")
        lines.append(f"      Total Snapshots Processed:  {self.total_snapshots:,}")
        lines.append(f"      Total Rows Evaluated:       {self.total_rows:,}")
        start_str = self.start_ts.strftime("%Y-%m-%d %H:%M:%S UTC") if self.start_ts else "N/A"
        end_str = self.end_ts.strftime("%Y-%m-%d %H:%M:%S UTC") if self.end_ts else "N/A"
        lines.append(f"      Time Window:                {start_str}  -->  {end_str}")
        sim_hrs = self.simulated_duration_seconds / 3600.0
        lines.append(f"      Simulated Time Span:        {self.simulated_duration_seconds:,.1f}s ({sim_hrs:.2f} hours)")
        lines.append(f"      Wall-Clock Time:            {self.wall_clock_duration_seconds:.2f}s")
        speed_lbl = "Instantaneous" if self.speed_multiplier <= 0 or math.isinf(self.speed_multiplier) else f"{self.speed_multiplier:.1f}x"
        lines.append(f"      Replay Speed Multiplier:    {speed_lbl}")
        lines.append("-" * 90)

        # 2. Opportunities Found
        lines.append("  [2] Opportunities Overview")
        lines.append(f"      Total Unique Opportunities: {self.total_opportunities_found:,}")
        lines.append(f"      Total Snapshot Detections:  {self.total_opportunity_instances:,}")
        lines.append(f"      Intra-Market Opportunities: {self.intra_market_count:,}")
        lines.append(f"      Cross-Platform Arbitrages:  {self.cross_platform_count:,}")
        lines.append("-" * 90)

        # 3. Edge Size Distribution
        lines.append("  [3] Edge Size Distribution (cents per contract / $1 payout)")
        if self.edge_stats and self.total_opportunities_found > 0:
            e = self.edge_stats
            lines.append(
                f"      Mean: {e.get('mean', 0.0):.2f}¢ | Median: {e.get('median', 0.0):.2f}¢ | "
                f"Min: {e.get('min', 0.0):.2f}¢ | Max: {e.get('max', 0.0):.2f}¢ | Std: {e.get('stdev', 0.0):.2f}¢"
            )
            pcts = e.get("percentiles", {})
            lines.append(
                f"      Percentiles: 25th={pcts.get('p25', 0.0):.2f}¢ | 50th={pcts.get('p50', 0.0):.2f}¢ | "
                f"75th={pcts.get('p75', 0.0):.2f}¢ | 90th={pcts.get('p90', 0.0):.2f}¢ | 95th={pcts.get('p95', 0.0):.2f}¢"
            )
            hist = e.get("histogram", {})
            if hist:
                lines.append("      Edge Size Buckets:")
                for b_label, count in hist.items():
                    pct = (count / self.total_opportunities_found) * 100.0
                    bar = "█" * int(round(pct / 4.0))
                    lines.append(f"        {b_label:<16} : {count:>5} ({pct:>5.1f}%) {bar}")
        else:
            lines.append("      No arbitrage opportunities detected within this historical sample.")
        lines.append("-" * 90)

        # 4. Persistence / Duration Distribution
        lines.append("  [4] Opportunity Persistence & Duration (Half-Life)")
        if self.duration_stats and self.total_opportunities_found > 0:
            d = self.duration_stats
            lines.append(
                f"      Mean Duration:   {d.get('mean', 0.0):.1f}s | Median: {d.get('median', 0.0):.1f}s | "
                f"Min: {d.get('min', 0.0):.1f}s | Max: {d.get('max', 0.0):.1f}s"
            )
            lines.append(
                f"      Mean Snapshots:  {d.get('mean_snapshots', 0.0):.1f} consecutive snapshots | "
                f"Max Snapshots: {d.get('max_snapshots', 0)}"
            )
            pcts = d.get("percentiles", {})
            lines.append(
                f"      Percentiles:     25th={pcts.get('p25', 0.0):.1f}s | 50th={pcts.get('p50', 0.0):.1f}s | "
                f"75th={pcts.get('p75', 0.0):.1f}s | 90th={pcts.get('p90', 0.0):.1f}s"
            )
            hist = d.get("histogram", {})
            if hist:
                lines.append("      Duration Buckets:")
                for b_label, count in hist.items():
                    pct = (count / self.total_opportunities_found) * 100.0
                    bar = "█" * int(round(pct / 4.0))
                    lines.append(f"        {b_label:<16} : {count:>5} ({pct:>5.1f}%) {bar}")
        else:
            lines.append("      N/A (No opportunities detected).")
        lines.append("-" * 90)

        # 5. Top Opportunities Ranked by Edge
        if self.opportunities:
            lines.append("  [5] Top Detected Opportunities (Ranked by Max Edge)")
            top_opps = sorted(self.opportunities, key=lambda o: o.max_edge_cents, reverse=True)[:10]
            lines.append(
                f"      {'STRATEGY':<15} | {'MARKET(S)':<38} | {'MAX EDGE':<9} | {'MEAN EDGE':<10} | {'DURATION':<10} | {'SNAPS'}"
            )
            lines.append("      " + "-" * 82)
            for opp in top_opps:
                if opp.strategy == "intra_market":
                    m_label = f"{opp.platform.upper()}:{opp.ticker}"
                else:
                    m_label = f"{opp.ticker} <-> {opp.target_ticker or 'N/A'}"
                if len(m_label) > 38:
                    m_label = m_label[:35] + "..."
                dur_str = f"{opp.duration_seconds:.1f}s" if opp.duration_seconds > 0 else f"{opp.persistence_span_seconds:.1f}s"
                lines.append(
                    f"      {opp.strategy:<15} | {m_label:<38} | +{opp.max_edge_cents:>6.2f}¢ | +{opp.mean_edge_cents:>7.2f}¢ | {dur_str:>9} | {opp.snapshots_count:>5}"
                )
            lines.append("-" * 90)

        lines.append("=" * 90 + "\n")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Convert summary to JSON-serializable dictionary."""
        return {
            "total_snapshots": self.total_snapshots,
            "total_rows": self.total_rows,
            "start_ts": self.start_ts.isoformat() if self.start_ts else None,
            "end_ts": self.end_ts.isoformat() if self.end_ts else None,
            "simulated_duration_seconds": self.simulated_duration_seconds,
            "wall_clock_duration_seconds": self.wall_clock_duration_seconds,
            "speed_multiplier": self.speed_multiplier,
            "total_opportunities_found": self.total_opportunities_found,
            "total_opportunity_instances": self.total_opportunity_instances,
            "intra_market_count": self.intra_market_count,
            "cross_platform_count": self.cross_platform_count,
            "edge_stats": self.edge_stats,
            "duration_stats": self.duration_stats,
            "opportunities": [opp.model_dump(mode="json") for opp in self.opportunities],
        }

    def save_json(self, output_path: Union[str, Path]) -> None:
        """Save summary report to a JSON file."""
        p = Path(output_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info("Saved replay summary JSON to %s", p)


# ============================================================================
# Replay Engine Core
# ============================================================================


class ReplayEngine:
    """Historical market replay engine.

    Reads timestamped orderbook snapshot rows from DuckDB and replays them
    through the DataFeed state manager and Arbitrage detectors at configurable speed.
    """

    DEFAULT_DUCKDB_PATH = "data/snapshots.duckdb"

    def __init__(
        self,
        duckdb_path: Union[str, Path] = DEFAULT_DUCKDB_PATH,
        speed: float = 100.0,
        start_date: Optional[Union[datetime, str]] = None,
        end_date: Optional[Union[datetime, str]] = None,
        config: Optional[AppConfig] = None,
        min_edge: Optional[float] = None,
        overrides_only: Optional[bool] = None,
        max_sleep_per_step: float = 2.0,
        include_intra: bool = True,
        include_cross: bool = True,
        limit_snapshots: Optional[int] = None,
        on_opportunity: Optional[Callable[[Opportunity], None]] = None,
        allow_unverified: bool = False,
        executability_params: Optional[ExecutabilityParams] = None,
        tau_fresh_sweep: Optional[Sequence[float]] = None,
        latency_sweep_s: Optional[Sequence[float]] = None,
        persist_evaluations: bool = True,
        param_set_id: Optional[str] = None,
    ) -> None:
        """Initialize ReplayEngine.

        Args:
            duckdb_path: Path to DuckDB database file containing `orderbook_snapshots`.
            speed: Speed multiplier (e.g. 100.0 for 100x real-time; <=0 or inf for instantaneous).
            start_date: Start date/time filter (inclusive).
            end_date: End date/time filter (inclusive).
            config: Optional AppConfig instance. If None, loaded via `load_config()`.
            min_edge: Minimum net edge in cents or fraction. Overrides config if supplied.
            overrides_only: If True, only scans manual market overrides. If None, inherits config.
            max_sleep_per_step: Maximum sleep in seconds per step to prevent stalling on data gaps.
            include_intra: Whether to scan for intra-market arbitrage.
            include_cross: Whether to scan for cross-platform arbitrage.
            limit_snapshots: Optional limit on the number of snapshots to replay.
            on_opportunity: Optional callback invoked when an opportunity is detected.
            allow_unverified: If True, queries orderbook_snapshots directly instead of research_snapshots.
            executability_params: ExecutabilityParams thresholds and cost settings.
            tau_fresh_sweep: Quote age sensitivity thresholds in seconds.
            latency_sweep_s: Execution latency survival thresholds in seconds.
            persist_evaluations: If True, persists evaluations and episodes into DuckDB.
            param_set_id: Identifier for stored parameter set.
        """
        self.duckdb_path = Path(duckdb_path)
        self.speed = float(speed)
        self.start_date = self._parse_datetime(start_date)
        self.end_date = self._parse_datetime(end_date, is_end=True)
        self.config = config or load_config()
        self.min_edge = min_edge
        self.overrides_only = overrides_only
        self.max_sleep_per_step = max(0.0, float(max_sleep_per_step))
        self.include_intra = include_intra
        self.include_cross = include_cross
        self.limit_snapshots = limit_snapshots
        self.on_opportunity = on_opportunity
        self.allow_unverified = bool(allow_unverified)
        self.persist_evaluations = persist_evaluations
        self.param_set_id = param_set_id or "default_run"

        # Executability parameters
        if executability_params is not None:
            self.executability_params = executability_params
        elif hasattr(self.config, "executability"):
            ec = self.config.executability
            self.executability_params = ExecutabilityParams(
                sizes=list(ec.sizes),
                min_edge_cents=Decimal(str(ec.min_edge_cents)),
                tau_fresh_s=Decimal(str(ec.tau_fresh)),
                tau_skew_s=Decimal(str(ec.tau_skew)),
                clock_tolerance_s=Decimal(str(ec.clock_tolerance_s)),
                kalshi_taker_rate=Decimal(str(ec.kalshi_taker_rate)),
                kalshi_multiplier=Decimal(str(ec.kalshi_multiplier)),
                polymarket_taker_rate=Decimal(str(ec.polymarket_taker_rate)),
                polygon_gas_cents=Decimal(str(ec.polygon_gas_cents)),
                slippage_cents_per_bundle=Decimal(str(ec.slippage_cents)),
            )
        else:
            self.executability_params = ExecutabilityParams()

        self.tau_fresh_sweep = list(tau_fresh_sweep) if tau_fresh_sweep is not None else (
            list(self.config.executability.tau_fresh_sweep) if hasattr(self.config, "executability") else [5.0, 10.0, 30.0]
        )
        self.latency_sweep_s = list(latency_sweep_s) if latency_sweep_s is not None else (
            list(self.config.executability.latency_sweep_s) if hasattr(self.config, "executability") else [1.0, 2.0, 5.0, 10.0, 30.0]
        )

        # Replay DataFeed (in-memory, network disabled)
        self.feed = DataFeed(
            config=self.config,
            duckdb_path=":memory:",
            enable_ws=False,
        )

        # Market Matcher
        self.matcher = MarketMatcher.from_config(self.config)
        if self.overrides_only is not None:
            self.matcher.overrides_only = bool(self.overrides_only)

        # Pre-cache known override metadata for quick title/condition lookup
        self._override_lookup: dict[str, Any] = {}
        for ov in self.matcher.overrides:
            if ov.kalshi_ticker:
                self._override_lookup[ov.kalshi_ticker] = ov
            if ov.polymarket_condition_id:
                self._override_lookup[ov.polymarket_condition_id] = ov
                self._override_lookup[ov.polymarket_condition_id.lower()] = ov

        self._levels_by_sid: dict[int, dict[str, list[Level]]] = {}

    @staticmethod
    def _parse_datetime(val: Optional[Union[datetime, str]], is_end: bool = False) -> Optional[datetime]:
        """Parse string or datetime into UTC-aware datetime."""
        if val is None:
            return None
        if isinstance(val, datetime):
            if val.tzinfo is None:
                return val.replace(tzinfo=timezone.utc)
            return val

        val_str = str(val).strip()
        if not val_str:
            return None

        # If string is date only (YYYY-MM-DD or YYYY/MM/DD), set appropriate start/end of day
        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                dt = datetime.strptime(val_str, fmt)
                if is_end:
                    dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc)
                else:
                    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass

        # Try ISO parsing
        try:
            dt = datetime.fromisoformat(val_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

        # Try date + time YYYY-MM-DD HH:MM:SS
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                dt = datetime.strptime(val_str, fmt)
                dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass

        raise ValueError(f"Unable to parse datetime string: '{val_str}'")

    def _query_snapshots(self) -> tuple[duckdb.DuckDBPyConnection, list[tuple[Any, ...]]]:
        """Connect to DuckDB and query ordered snapshot records according to filters."""
        if not self.duckdb_path.exists():
            raise FileNotFoundError(f"Snapshots database file not found at: {self.duckdb_path}")

        read_only = not self.persist_evaluations
        try:
            conn = duckdb.connect(str(self.duckdb_path), read_only=read_only)
        except Exception:
            conn = duckdb.connect(str(self.duckdb_path), read_only=True)
            self.persist_evaluations = False

        if self.persist_evaluations:
            ensure_executability_schema(conn)

        table_name = "orderbook_snapshots"
        if not self.allow_unverified:
            try:
                view_check = conn.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'research_snapshots'"
                ).fetchone()
                if view_check and view_check[0] > 0:
                    table_name = "research_snapshots"
            except Exception:
                pass

        cols_in_table = {
            r[0].lower()
            for r in conn.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
            ).fetchall()
        }

        # Query levels if available
        self._levels_by_sid = {}
        try:
            has_levels = conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'orderbook_levels'"
            ).fetchone()[0] > 0
            if has_levels:
                level_rows = conn.execute(
                    "SELECT snapshot_id, side, price, size FROM orderbook_levels ORDER BY snapshot_id, level_idx"
                ).fetchall()
                for sid, side, price, size in level_rows:
                    if sid not in self._levels_by_sid:
                        self._levels_by_sid[sid] = {}
                    side_str = str(side)
                    if side_str not in self._levels_by_sid[sid]:
                        self._levels_by_sid[sid][side_str] = []
                    self._levels_by_sid[sid][side_str].append((Decimal(str(price)), Decimal(str(size))))
        except Exception as e:
            logger.debug("Orderbook levels query skipped: %s", e)

        sid_expr = "snapshot_id" if "snapshot_id" in cols_in_table else "NULL AS snapshot_id"
        obs_expr = "observed_at_utc::VARCHAR" if "observed_at_utc" in cols_in_table else "NULL AS observed_at_utc"
        exch_expr = "exchange_updated_at_utc::VARCHAR" if "exchange_updated_at_utc" in cols_in_table else "NULL AS exchange_updated_at_utc"
        status_expr = "market_status" if "market_status" in cols_in_table else "'open' AS market_status"
        tradable_expr = "tradable" if "tradable" in cols_in_table else "TRUE AS tradable"
        ds_expr = "data_source" if "data_source" in cols_in_table else "'live_api' AS data_source"
        trunc_expr = "depth_truncated" if "depth_truncated" in cols_in_table else "FALSE AS depth_truncated"
        pair_expr = "pair_id" if "pair_id" in cols_in_table else "NULL AS pair_id"

        query = [
            f"SELECT ts::VARCHAR, platform, ticker, yes_bid, yes_ask, no_bid, no_ask, volume, {sid_expr}, {obs_expr}, {exch_expr}, {status_expr}, {tradable_expr}, {ds_expr}, {trunc_expr}, {pair_expr}",
            f"FROM {table_name}",
            "WHERE 1=1",
        ]
        params: list[Any] = []

        if self.start_date is not None:
            query.append("AND ts >= ?")
            params.append(self.start_date)

        if self.end_date is not None:
            query.append("AND ts <= ?")
            params.append(self.end_date)

        query.append("ORDER BY ts ASC, platform ASC, ticker ASC")
        sql = " ".join(query)
        logger.debug("Executing DuckDB snapshot replay query: %s | params=%s", sql, params)

        rel = conn.execute(sql, params)
        rows = rel.fetchall()
        return conn, rows

    def _group_rows_by_timestamp(
        self,
        rows: Sequence[tuple[Any, ...]],
    ) -> list[tuple[datetime, list[tuple[Any, ...]]]]:
        """Group flat rows into batches by identical snapshot timestamp."""
        batches: list[tuple[datetime, list[tuple[Any, ...]]]] = []
        if not rows:
            return batches

        curr_ts: Optional[datetime] = None
        curr_batch: list[tuple[Any, ...]] = []

        for row in rows:
            raw_ts = row[0]
            if isinstance(raw_ts, str):
                ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            elif isinstance(raw_ts, datetime):
                ts = raw_ts
            else:
                ts = datetime.now(timezone.utc)

            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            if curr_ts is None or ts == curr_ts:
                curr_ts = ts
                curr_batch.append(row)
            else:
                batches.append((curr_ts, curr_batch))
                curr_ts = ts
                curr_batch = [row]

        if curr_ts is not None and curr_batch:
            batches.append((curr_ts, curr_batch))

        if self.limit_snapshots is not None and self.limit_snapshots > 0:
            batches = batches[: self.limit_snapshots]

        return batches

    @staticmethod
    def _get_opportunity_key(opp: Opportunity) -> str:
        """Compute stable unique key identifying an opportunity across snapshots."""
        if opp.strategy == "intra_market":
            return f"intra:{opp.platform.lower()}:{opp.ticker.lower()}"

        legs_repr = ":".join(f"{leg.platform.lower()}_{leg.side.lower()}" for leg in opp.legs)
        tgt = (opp.target_ticker or "").lower()
        return f"cross:{opp.ticker.lower()}:{tgt}:{legs_repr}"

    async def run(self) -> ReplaySummary:
        """Execute historical replay and return structured quantitative summary report."""
        wall_start_time = time.time()
        conn, rows = self._query_snapshots()
        try:
            batches = self._group_rows_by_timestamp(rows)

            total_rows = len(rows)
            total_snapshots = len(batches)

            if total_snapshots == 0:
                logger.warning("No snapshot records found matching specified criteria.")
                return ReplaySummary(
                    total_snapshots=0,
                    total_rows=0,
                    start_ts=self.start_date,
                    end_ts=self.end_date,
                    simulated_duration_seconds=0.0,
                    wall_clock_duration_seconds=time.time() - wall_start_time,
                    speed_multiplier=self.speed,
                    total_opportunities_found=0,
                )

            start_ts = batches[0][0]
            end_ts = batches[-1][0]
            sim_duration = max(0.0, (end_ts - start_ts).total_seconds())

            logger.info(
                "Replaying %d snapshots (%d rows) from %s to %s at %.1fx speed...",
                total_snapshots,
                total_rows,
                start_ts,
                end_ts,
                self.speed,
            )

            # Setup episode trackers (primary and sweeps)
            trackers: dict[str, tuple[ExecutabilityParams, EpisodeTracker]] = {}
            sampling_interval = getattr(self.config.data_quality, "snapshot_interval_seconds", 60.0)
            gap_tolerance = getattr(self.config.executability, "episode_gap_tolerance", 1.5)

            primary_tracker = EpisodeTracker(
                sampling_interval_s=sampling_interval,
                episode_gap_tolerance=gap_tolerance,
                latencies_s=self.latency_sweep_s,
            )
            trackers[self.param_set_id] = (self.executability_params, primary_tracker)

            # Tau fresh sweeps
            for tf in self.tau_fresh_sweep:
                tf_dec = Decimal(str(tf))
                if tf_dec != self.executability_params.tau_fresh_s:
                    sw_id = f"{self.param_set_id}_tau_{int(tf)}"
                    sw_params = replace(self.executability_params, tau_fresh_s=tf_dec)
                    sw_tracker = EpisodeTracker(
                        sampling_interval_s=sampling_interval,
                        episode_gap_tolerance=gap_tolerance,
                        latencies_s=self.latency_sweep_s,
                    )
                    trackers[sw_id] = (sw_params, sw_tracker)

            active_opps: dict[str, OpportunityRecord] = {}
            completed_opps: list[OpportunityRecord] = []
            total_instances = 0
            intra_count = 0
            cross_count = 0

            prev_ts: Optional[datetime] = None

            for snapshot_idx, (snapshot_ts, batch_rows) in enumerate(batches, 1):
                # Speed pacing / sleep delay
                if prev_ts is not None and self.speed > 0 and not math.isinf(self.speed):
                    sim_step_seconds = max(0.0, (snapshot_ts - prev_ts).total_seconds())
                    if sim_step_seconds > 0:
                        real_delay = min(sim_step_seconds / self.speed, self.max_sleep_per_step)
                        if real_delay > 0.001:
                            await asyncio.sleep(real_delay)

                prev_ts = snapshot_ts

                current_batch_markets: list[dict[str, Any]] = []

                # Ingest batch rows into DataFeed market state
                for row in batch_rows:
                    ts, platform, ticker, yes_bid, yes_ask, no_bid, no_ask, volume, sid, obs_str, exch_str, status, tradable, data_source, trunc, pair_id = row
                    p_key = str(platform).lower()
                    t_key = str(ticker)

                    obs_at = datetime.fromisoformat(obs_str.replace("Z", "+00:00")) if obs_str else snapshot_ts
                    if obs_at.tzinfo is None:
                        obs_at = obs_at.replace(tzinfo=timezone.utc)
                    exch_at = datetime.fromisoformat(exch_str.replace("Z", "+00:00")) if exch_str else None
                    if exch_at and exch_at.tzinfo is None:
                        exch_at = exch_at.replace(tzinfo=timezone.utc)

                    levels = self._levels_by_sid.get(sid, {}) if sid is not None else {}

                    m_dict = {
                        "snapshot_id": sid,
                        "platform": p_key,
                        "ticker": t_key,
                        "ts": snapshot_ts,
                        "observed_at_utc": obs_at,
                        "exchange_updated_at_utc": exch_at,
                        "yes_bid": yes_bid,
                        "yes_ask": yes_ask,
                        "no_bid": no_bid,
                        "no_ask": no_ask,
                        "volume": int(volume) if volume is not None else 0,
                        "market_status": status,
                        "tradable": tradable,
                        "data_source": data_source,
                        "depth_truncated": trunc,
                        "levels": levels,
                        "pair_id": pair_id,
                    }
                    current_batch_markets.append(m_dict)

                    # Metadata enrichment from overrides or known tables
                    ov = self._override_lookup.get(t_key)
                    title = ov.notes or ov.kalshi_title if ov else t_key
                    cid = t_key if (t_key.startswith("0x") or (ov and ov.polymarket_condition_id == t_key)) else None

                    self.feed.update_market_state(
                        platform=p_key,
                        ticker=t_key,
                        title=title,
                        yes_bid=yes_bid,
                        yes_ask=yes_ask,
                        no_bid=no_bid,
                        no_ask=no_ask,
                        volume=int(volume) if volume is not None else 0,
                        ts=snapshot_ts,
                        condition_id=cid,
                    )

                # 1. Legacy Opportunity detection
                current_detected: list[Opportunity] = []
                all_states = list(self.feed.market_state.values())

                if self.include_intra:
                    intra_opps = find_intra_market_opportunities(
                        all_states,
                        config=self.config,
                        min_edge=self.min_edge,
                    )
                    current_detected.extend(intra_opps)

                if self.include_cross:
                    kalshi_states = [s for s in all_states if s.platform == "kalshi"]
                    poly_states = [s for s in all_states if s.platform == "polymarket"]
                    if kalshi_states and poly_states:
                        matches = self.matcher.match(kalshi_states, poly_states)
                        k_books = {s.ticker: s for s in kalshi_states}
                        p_books = {}
                        for s in poly_states:
                            p_books[s.ticker] = s
                            if s.condition_id:
                                p_books[s.condition_id] = s

                        cross_opps = find_cross_platform_opportunities(
                            matched_pairs=matches,
                            kalshi_orderbooks=k_books,
                            polymarket_orderbooks=p_books,
                            config=self.config,
                            min_edge=self.min_edge,
                        )
                        current_detected.extend(cross_opps)

                # 2. Executability evaluations and episode tracking
                for p_id, (params, tracker) in trackers.items():
                    batch_evals: list[OpportunityEvaluation] = []
                    if self.include_intra:
                        intra_evals = evaluate_intra_market_opportunities(
                            current_batch_markets,
                            params=params,
                            t_eval=snapshot_ts,
                        )
                        batch_evals.extend(intra_evals)

                    if self.include_cross:
                        k_books = {m["ticker"]: m for m in current_batch_markets if m["platform"] == "kalshi"}
                        p_books = {m["ticker"]: m for m in current_batch_markets if m["platform"] == "polymarket"}
                        for m in current_batch_markets:
                            if m["platform"] == "polymarket" and m.get("pair_id"):
                                p_books[m["pair_id"]] = m

                        kalshi_states = [s for s in all_states if s.platform == "kalshi"]
                        poly_states = [s for s in all_states if s.platform == "polymarket"]
                        if kalshi_states and poly_states:
                            matches = self.matcher.match(kalshi_states, poly_states)
                            cross_evals = evaluate_cross_platform_opportunities(
                                matched_pairs=matches,
                                kalshi_orderbooks=k_books,
                                polymarket_orderbooks=p_books,
                                params=params,
                                t_eval=snapshot_ts,
                                pair_verified=True,
                            )
                            batch_evals.extend(cross_evals)

                    tracker.step(snapshot_ts, batch_evals)
                    if self.persist_evaluations:
                        write_evaluations_and_counts(conn, p_id, batch_evals, snapshot_ts)

                # Map current detected opportunities by key
                detected_by_key: dict[str, Opportunity] = {}
                for opp in current_detected:
                    opp_key = self._get_opportunity_key(opp)
                    detected_by_key[opp_key] = opp
                    total_instances += 1
                    if self.on_opportunity:
                        try:
                            self.on_opportunity(opp)
                        except Exception as e:
                            logger.warning("Error in on_opportunity callback: %s", e)

                # Reconcile with active opportunities (Persistence & Disappearance Tracking)
                current_keys = set(detected_by_key.keys())
                active_keys = set(active_opps.keys())

                # A. Disappeared opportunities
                disappeared_keys = active_keys - current_keys
                for d_key in disappeared_keys:
                    rec = active_opps.pop(d_key)
                    rec.close(disappeared_ts=snapshot_ts)
                    completed_opps.append(rec)
                    logger.debug("Opportunity %s closed at %s after %.1fs", d_key, snapshot_ts, rec.duration_seconds)

                # B. Continued opportunities
                persisted_keys = active_keys & current_keys
                for p_key in persisted_keys:
                    active_opps[p_key].record_update(detected_by_key[p_key], current_ts=snapshot_ts)

                # C. Newly appeared opportunities
                new_keys = current_keys - active_keys
                for n_key in new_keys:
                    opp = detected_by_key[n_key]
                    rec = OpportunityRecord(
                        opportunity_id=n_key,
                        strategy=opp.strategy,
                        platform=opp.platform,
                        ticker=opp.ticker,
                        target_platform=opp.target_platform,
                        target_ticker=opp.target_ticker,
                        side_actions=opp.side_actions,
                        start_ts=snapshot_ts,
                        last_seen_ts=snapshot_ts,
                        snapshots_count=1,
                        initial_edge_cents=float(opp.edge_cents),
                        min_edge_cents=float(opp.edge_cents),
                        max_edge_cents=float(opp.edge_cents),
                        mean_edge_cents=float(opp.edge_cents),
                        edge_samples=[float(opp.edge_cents)],
                        gross_edge_cents=float(opp.gross_edge_cents) if opp.gross_edge_cents is not None else None,
                        estimated_fees_cents=float(opp.estimated_fees_cents) if opp.estimated_fees_cents is not None else 0.0,
                        legs=[leg.model_dump(mode="json") for leg in opp.legs],
                        is_active=True,
                    )
                    active_opps[n_key] = rec
                    if opp.strategy == "intra_market":
                        intra_count += 1
                    else:
                        cross_count += 1

            # Finalize remaining active opportunities
            for rec in active_opps.values():
                rec.is_active = False
                completed_opps.append(rec)

            # Finalize episode trackers and persist
            total_episodes = 0
            total_exec_episodes = 0
            for p_id, (params, tracker) in trackers.items():
                eps = tracker.finalize()
                if p_id == self.param_set_id:
                    total_episodes = len(eps)
                    total_exec_episodes = sum(1 for ep in eps if ep.level == "executable")
                if self.persist_evaluations:
                    save_param_set(conn, p_id, params)
                    write_episodes_and_latencies(conn, p_id, tracker, eps)

            edge_stats = self._calculate_edge_distribution(completed_opps)
            dur_stats = self._calculate_duration_distribution(completed_opps)

            return ReplaySummary(
                total_snapshots=total_snapshots,
                total_rows=total_rows,
                start_ts=start_ts,
                end_ts=end_ts,
                simulated_duration_seconds=sim_duration,
                wall_clock_duration_seconds=time.time() - wall_start_time,
                speed_multiplier=self.speed,
                total_opportunities_found=len(completed_opps),
                total_opportunity_instances=total_instances,
                intra_market_count=intra_count,
                cross_platform_count=cross_count,
                edge_stats=edge_stats,
                duration_stats=dur_stats,
                opportunities=completed_opps,
                evaluations_count=total_instances,
                episodes_count=total_episodes,
                executable_episodes_count=total_exec_episodes,
                param_set_id=self.param_set_id,
            )
        finally:
            conn.close()

    def run_sync(self) -> ReplaySummary:
        """Synchronous wrapper around `run()`."""
        return asyncio.run(self.run())

    @staticmethod
    def _calculate_edge_distribution(opportunities: Sequence[OpportunityRecord]) -> dict[str, Any]:
        """Compute statistical distribution metrics for opportunity edge sizes."""
        if not opportunities:
            return {}

        # Collect peak (max) edge observed per opportunity episode
        edges = [o.max_edge_cents for o in opportunities]
        edges.sort()
        n = len(edges)

        mean_val = float(statistics.mean(edges))
        median_val = float(statistics.median(edges))
        stdev_val = float(statistics.stdev(edges)) if n > 1 else 0.0

        def get_percentile(p: float) -> float:
            k = (n - 1) * p
            f = math.floor(k)
            c = math.ceil(k)
            if f == c:
                return float(edges[int(k)])
            return float(edges[f] * (c - k) + edges[c] * (k - f))

        # Histogram buckets
        buckets = {
            "< 0.50¢": 0,
            "0.50¢ - 1.00¢": 0,
            "1.00¢ - 2.00¢": 0,
            "2.00¢ - 5.00¢": 0,
            "5.00¢ - 10.00¢": 0,
            ">= 10.00¢": 0,
        }
        for e in edges:
            if e < 0.50:
                buckets["< 0.50¢"] += 1
            elif e < 1.00:
                buckets["0.50¢ - 1.00¢"] += 1
            elif e < 2.00:
                buckets["1.00¢ - 2.00¢"] += 1
            elif e < 5.00:
                buckets["2.00¢ - 5.00¢"] += 1
            elif e < 10.00:
                buckets["5.00¢ - 10.00¢"] += 1
            else:
                buckets[">= 10.00¢"] += 1

        return {
            "count": n,
            "min": round(edges[0], 2),
            "max": round(edges[-1], 2),
            "mean": round(mean_val, 2),
            "median": round(median_val, 2),
            "stdev": round(stdev_val, 2),
            "percentiles": {
                "p25": round(get_percentile(0.25), 2),
                "p50": round(median_val, 2),
                "p75": round(get_percentile(0.75), 2),
                "p90": round(get_percentile(0.90), 2),
                "p95": round(get_percentile(0.95), 2),
                "p99": round(get_percentile(0.99), 2),
            },
            "histogram": buckets,
        }

    @staticmethod
    def _calculate_duration_distribution(opportunities: Sequence[OpportunityRecord]) -> dict[str, Any]:
        """Compute statistical distribution metrics for opportunity persistence durations."""
        if not opportunities:
            return {}

        durations = [o.duration_seconds if o.duration_seconds > 0 else o.persistence_span_seconds for o in opportunities]
        durations.sort()
        n = len(durations)

        snapshots = [o.snapshots_count for o in opportunities]

        mean_dur = float(statistics.mean(durations))
        median_dur = float(statistics.median(durations))
        stdev_dur = float(statistics.stdev(durations)) if n > 1 else 0.0

        def get_percentile(p: float) -> float:
            k = (n - 1) * p
            f = math.floor(k)
            c = math.ceil(k)
            if f == c:
                return float(durations[int(k)])
            return float(durations[f] * (c - k) + durations[c] * (k - f))

        # Duration buckets in seconds
        buckets = {
            "< 10s": 0,
            "10s - 30s": 0,
            "30s - 60s": 0,
            "1m - 5m": 0,
            "5m - 15m": 0,
            ">= 15m": 0,
        }
        for d in durations:
            if d < 10.0:
                buckets["< 10s"] += 1
            elif d < 30.0:
                buckets["10s - 30s"] += 1
            elif d < 60.0:
                buckets["30s - 60s"] += 1
            elif d < 300.0:
                buckets["1m - 5m"] += 1
            elif d < 900.0:
                buckets["5m - 15m"] += 1
            else:
                buckets[">= 15m"] += 1

        return {
            "count": n,
            "min": round(durations[0], 2),
            "max": round(durations[-1], 2),
            "mean": round(mean_dur, 2),
            "median": round(median_dur, 2),
            "stdev": round(stdev_dur, 2),
            "mean_snapshots": round(sum(snapshots) / float(n), 2),
            "max_snapshots": max(snapshots),
            "percentiles": {
                "p25": round(get_percentile(0.25), 2),
                "p50": round(median_dur, 2),
                "p75": round(get_percentile(0.75), 2),
                "p90": round(get_percentile(0.90), 2),
            },
            "histogram": buckets,
        }
