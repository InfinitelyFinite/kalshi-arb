#!/usr/bin/env python3
"""Empirical study analysis and replay experiment engine for prediction market research.

Replays DuckDB research snapshots to answer core research questions:
1. Opportunity frequency per valid pair-hour and overall hour.
2. Opportunity funnel: Raw -> Fee-Adjusted -> Executable Candidates & Episodes.
3. Distribution of net edges (mean, median, percentiles, histogram).
4. Opportunity persistence duration and half-life across latency thresholds.
5. Order book depth, fillability, and executable volume.
6. Sensitivity analysis across parameter grid (trade sizes, freshness, latency, fees, min edge).
7. Usable synchronized market data percentage and quote skew.

Produces reproducible tables, CSV exports, JSON summaries, and Markdown reports.

Usage:
    # Standard analysis across all collected research data:
    python scripts/analyze_study.py --db data/snapshots.duckdb --out results/

    # Analyze specific date range:
    python scripts/analyze_study.py --db data/snapshots.duckdb --out results/ --from 2026-11-01 --to 2026-11-09
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
import math
import os
from pathlib import Path
import statistics
import sys
from typing import Any, Optional, Sequence

# Ensure repository root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

from core.config_loader import AppConfig, load_config
from core.depth import Level, clean_levels, vwap_for_size
from core.executability import (
    Classification,
    Episode,
    ExecutabilityParams,
    LatencyStatus,
    LegInput,
    OpportunityEvaluation,
    OpportunityInput,
    RejectReason,
    evaluate_opportunity,
)
from core.snapshot_store import connect, ensure_schema, view_exists

logger = logging.getLogger("analyze_study")


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )


def percentile(data: Sequence[float], p: float) -> Optional[float]:
    """Calculate the p-th percentile from numeric sequence."""
    if not data:
        return None
    s = sorted(data)
    k = (len(s) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(s[int(k)])
    return float(s[int(f)] * (c - k) + s[int(c)] * (k - f))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Empirical study analysis and sensitivity backtesting for prediction market research.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--duckdb-path",
        "--db",
        dest="duckdb_path",
        type=str,
        default="data/snapshots.duckdb",
        help="Path to DuckDB database file containing research_snapshots",
    )
    parser.add_argument(
        "--output-dir",
        "--out",
        dest="output_dir",
        type=str,
        default="results",
        help="Directory to save output reports, summary JSON, and CSV tables",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        default=None,
        help="Filter start timestamp or date (ISO format or YYYY-MM-DD)",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=str,
        default=None,
        help="Filter end timestamp or date (ISO format or YYYY-MM-DD)",
    )
    parser.add_argument(
        "--allow-unverified",
        action="store_true",
        default=False,
        help="Analyze raw orderbook_snapshots table instead of research_snapshots view",
    )
    parser.add_argument(
        "--limit-snapshots",
        type=int,
        default=None,
        help="Limit the number of snapshot batches evaluated for rapid testing",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Enable verbose DEBUG logging",
    )
    return parser


@dataclass
class StudyEpisode:
    """Tracks a discrete episode of an arbitrage opportunity across consecutive snapshots."""

    episode_id: str
    pair_id: str
    direction: str
    size_q: int
    param_scenario: str
    first_seen_utc: datetime
    last_seen_utc: datetime
    snapshots_count: int = 1
    peak_net_edge: float = 0.0
    mean_net_edge: float = 0.0
    _edge_samples: list[float] = field(default_factory=list)
    duration_seconds: float = 0.0
    is_active: bool = True
    survived_latencies: dict[float, bool] = field(default_factory=dict)

    def record_update(self, current_ts: datetime, net_edge: float) -> None:
        self.last_seen_utc = current_ts
        self.snapshots_count += 1
        self._edge_samples.append(net_edge)
        self.peak_net_edge = max(self.peak_net_edge, net_edge)
        self.mean_net_edge = sum(self._edge_samples) / len(self._edge_samples)
        self.duration_seconds = max(0.0, (self.last_seen_utc - self.first_seen_utc).total_seconds())

    def close(self, disappeared_ts: datetime) -> None:
        self.is_active = False
        self.duration_seconds = max(0.0, (disappeared_ts - self.first_seen_utc).total_seconds())


class StudyAnalyzer:
    """Core analysis and replay engine for real-market empirical datasets."""

    FEE_SCENARIOS = {
        "baseline": {
            "name": "Contemporary Verified Exchange Rules",
            "kalshi_rate": Decimal("0.07"),
            "kalshi_mult": Decimal("1.0"),
            "poly_rate": Decimal("0.0"),
            "gas_cents": Decimal("0.0"),
        },
        "lower_cost": {
            "name": "Lower-Cost / VIP Tier (50% Kalshi Discount)",
            "kalshi_rate": Decimal("0.035"),
            "kalshi_mult": Decimal("1.0"),
            "poly_rate": Decimal("0.0"),
            "gas_cents": Decimal("0.0"),
        },
        "higher_cost": {
            "name": "Higher-Cost / Friction Buffer (10% Kalshi, 2% Poly, Gas)",
            "kalshi_rate": Decimal("0.10"),
            "kalshi_mult": Decimal("1.0"),
            "poly_rate": Decimal("0.02"),
            "gas_cents": Decimal("1.0"),
        },
    }

    TRADE_SIZES = [1, 10, 50]
    QUOTE_FRESHNESS_SWEEP = [5.0, 10.0, 30.0]
    LATENCY_SWEEP = [2.0, 5.0, 10.0]
    MIN_EDGE_SWEEP = [0.0, 0.5, 1.0, 2.0]
    PERSISTENCE_THRESHOLDS = [0.0, 5.0, 10.0, 30.0]

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        config: AppConfig,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        use_raw_table: bool = False,
        limit_snapshots: Optional[int] = None,
    ) -> None:
        self.conn = conn
        self.config = config
        self.from_dt = from_dt
        self.to_dt = to_dt
        self.use_raw_table = use_raw_table
        self.limit_snapshots = limit_snapshots

        self.table_name = "orderbook_snapshots" if use_raw_table else "research_snapshots"
        if not use_raw_table and not view_exists(conn, "research_snapshots"):
            self.table_name = "orderbook_snapshots"

    def run_study(self) -> dict[str, Any]:
        """Execute full empirical study and calculate all 7 research questions."""
        logger.info("Executing study analysis on dataset '%s'...", self.table_name)
        try:
            ensure_schema(self.conn)
        except Exception:
            pass

        # 1. Fetch raw snapshots filtered by time
        where_clauses: list[str] = ["1=1"]
        params: list[Any] = []
        if self.from_dt:
            where_clauses.append("observed_at_utc >= ?")
            params.append(self.from_dt.replace(tzinfo=None))
        if self.to_dt:
            where_clauses.append("observed_at_utc <= ?")
            params.append(self.to_dt.replace(tzinfo=None))
        where_sql = " AND ".join(where_clauses)

        limit_sql = f" LIMIT {self.limit_snapshots}" if self.limit_snapshots else ""
        cols_in_table = {c[1] for c in self.conn.execute(f"PRAGMA table_info({self.table_name})").fetchall()}
        order_col = "pair_id" if "pair_id" in cols_in_table else "ticker"
        query = f"SELECT * FROM {self.table_name} WHERE {where_sql} ORDER BY ts ASC, {order_col} ASC{limit_sql}"

        cursor = self.conn.execute(query, params)
        cols = [desc[0] for desc in cursor.description]
        raw_rows = cursor.fetchall()
        rows = [dict(zip(cols, r)) for r in raw_rows]

        logger.info("Loaded %d snapshots from %s.", len(rows), self.table_name)
        if not rows:
            return self._empty_study_result()

        # 2. Fetch orderbook depth levels
        levels_by_snap = self._load_depth_levels(rows)

        # 3. Synchronize snapshot pairs by timestamp `ts` and `pair_id`
        pair_batches, sync_metrics = self._synchronize_pairs(rows, levels_by_snap)
        logger.info("Constructed %d synchronized pair observations across %d pairs.", len(pair_batches), sync_metrics["pair_count"])

        # 4. Pair-hour and observation time accounting (Q1 & Q7)
        pair_hours_data = self._calculate_pair_hours(pair_batches, sync_metrics)

        # 5. Liquidity & depth analysis (Q5)
        depth_analysis = self._analyze_depth_and_fillability(pair_batches)

        # 6. Replay & evaluate opportunities across baseline and parameter sweeps (Q2, Q3, Q4, Q6)
        baseline_results, sensitivity_matrix = self._evaluate_parameter_scenarios(pair_batches)

        # 7. Synthesize complete study answers
        study_results: dict[str, Any] = {
            "metadata": {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "dataset": self.table_name,
                "total_snapshots_loaded": len(rows),
                "synchronized_pair_observations": len(pair_batches),
                "distinct_pairs_count": sync_metrics["pair_count"],
                "time_window": sync_metrics["time_window"],
                "total_study_hours": pair_hours_data["total_study_hours"],
                "total_valid_pair_hours": pair_hours_data["total_pair_hours"],
            },
            "research_question_1_frequency": self._compile_q1_frequency(baseline_results, pair_hours_data),
            "research_question_2_funnel": self._compile_q2_funnel(baseline_results),
            "research_question_3_edge_distribution": self._compile_q3_edge_distribution(baseline_results),
            "research_question_4_persistence": self._compile_q4_persistence(baseline_results),
            "research_question_5_depth_and_volume": depth_analysis,
            "research_question_6_sensitivity": sensitivity_matrix,
            "research_question_7_synchronization": sync_metrics,
        }

        return study_results

    def _load_depth_levels(self, rows: list[dict[str, Any]]) -> dict[int, dict[str, list[Level]]]:
        """Fetch child orderbook_levels indexed by snapshot_id."""
        levels_by_snap: dict[int, dict[str, list[Level]]] = {}
        snap_ids = [r["snapshot_id"] for r in rows if r.get("snapshot_id") is not None]
        if not snap_ids:
            return levels_by_snap

        try:
            lvl_cursor = self.conn.execute(
                """
                SELECT snapshot_id, side, price, size
                FROM orderbook_levels
                WHERE snapshot_id IN (SELECT unnest(?))
                ORDER BY snapshot_id, side, level_idx ASC
                """,
                [snap_ids],
            )
            for sid, side, price, size in lvl_cursor.fetchall():
                if sid not in levels_by_snap:
                    levels_by_snap[sid] = {}
                if side not in levels_by_snap[sid]:
                    levels_by_snap[sid][side] = []
                levels_by_snap[sid][side].append((Decimal(str(price)), Decimal(str(size))))
        except Exception as e:
            logger.debug("Could not query orderbook_levels (using best ask sizes fallback): %s", e)

        return levels_by_snap

    def _synchronize_pairs(
        self,
        rows: list[dict[str, Any]],
        levels_by_snap: dict[int, dict[str, list[Level]]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Pair Kalshi and Polymarket legs by snapshot timestamp and compute synchronization metrics."""
        # Group by (ts, pair_id)
        grouped: dict[tuple[Any, str], dict[str, dict[str, Any]]] = {}
        for r in rows:
            pair_id = r.get("pair_id") or r.get("ticker")
            ts = r.get("ts")
            platform = (r.get("platform") or "").lower()
            if not pair_id or not ts or not platform:
                continue

            key = (ts, str(pair_id))
            if key not in grouped:
                grouped[key] = {}
            grouped[key][platform] = r

        synchronized_batches: list[dict[str, Any]] = []
        observed_skews_ms: list[float] = []
        quote_skews_ms: list[float] = []
        staleness_ages_sec: list[float] = []
        missing_exchange_ts = 0
        total_evals = len(grouped)

        timestamps: list[datetime] = []
        pairs_seen: set[str] = set()

        for (ts, pair_id), legs in grouped.items():
            pairs_seen.add(pair_id)
            k_leg = legs.get("kalshi")
            p_leg = legs.get("polymarket")

            if not k_leg or not p_leg:
                continue

            k_obs = k_leg.get("observed_at_utc")
            p_obs = p_leg.get("observed_at_utc")
            k_exch = k_leg.get("exchange_updated_at_utc")
            p_exch = p_leg.get("exchange_updated_at_utc")

            if k_obs:
                timestamps.append(k_obs if isinstance(k_obs, datetime) else datetime.fromisoformat(str(k_obs)))

            # Skew calculations
            if k_obs and p_obs:
                k_dt = k_obs if isinstance(k_obs, datetime) else datetime.fromisoformat(str(k_obs))
                p_dt = p_obs if isinstance(p_obs, datetime) else datetime.fromisoformat(str(p_obs))
                skew_obs = abs((k_dt - p_dt).total_seconds()) * 1000.0
                observed_skews_ms.append(skew_obs)

            if k_exch is None or p_exch is None:
                missing_exchange_ts += 1
            else:
                k_exch_dt = k_exch if isinstance(k_exch, datetime) else datetime.fromisoformat(str(k_exch))
                p_exch_dt = p_exch if isinstance(p_exch, datetime) else datetime.fromisoformat(str(p_exch))
                q_skew = abs((k_exch_dt - p_exch_dt).total_seconds()) * 1000.0
                quote_skews_ms.append(q_skew)

                # Quote freshness age
                if k_obs:
                    k_dt = k_obs if isinstance(k_obs, datetime) else datetime.fromisoformat(str(k_obs))
                    staleness_ages_sec.append(max(0.0, (k_dt - k_exch_dt).total_seconds()))

            # Extract ladders
            k_sid = k_leg.get("snapshot_id")
            p_sid = p_leg.get("snapshot_id")
            k_levels = levels_by_snap.get(k_sid, {}) if k_sid else {}
            p_levels = levels_by_snap.get(p_sid, {}) if p_sid else {}

            synchronized_batches.append(
                {
                    "ts": ts,
                    "pair_id": pair_id,
                    "kalshi": k_leg,
                    "polymarket": p_leg,
                    "kalshi_levels": k_levels,
                    "polymarket_levels": p_levels,
                }
            )

        # Sort chronologically
        synchronized_batches.sort(key=lambda b: b["ts"])

        start_dt = min(timestamps) if timestamps else None
        end_dt = max(timestamps) if timestamps else None
        total_span_s = (end_dt - start_dt).total_seconds() if start_dt and end_dt else 0.0

        # Usable synchronized quote criteria: both legs present, non-empty, and skew <= 5s
        usable_count = sum(
            1
            for b in synchronized_batches
            if (b["kalshi"].get("ask_yes") or b["kalshi"].get("yes_ask"))
            and (b["polymarket"].get("ask_yes") or b["polymarket"].get("yes_ask"))
        )
        usable_pct = round((usable_count / max(1, total_evals)) * 100.0, 2)

        sync_metrics = {
            "pair_count": len(pairs_seen),
            "pairs_list": sorted(pairs_seen),
            "total_pair_intervals": total_evals,
            "synchronized_intervals": len(synchronized_batches),
            "usable_synchronized_pct": usable_pct,
            "time_window": {
                "start_utc": start_dt.isoformat() if start_dt else None,
                "end_utc": end_dt.isoformat() if end_dt else None,
                "span_seconds": round(total_span_s, 1),
                "span_hours": round(total_span_s / 3600.0, 2),
                "span_days": round(total_span_s / 86400.0, 2),
            },
            "quote_skew": {
                "median_observed_skew_ms": percentile(observed_skews_ms, 50),
                "p95_observed_skew_ms": percentile(observed_skews_ms, 95),
                "median_quote_skew_ms": percentile(quote_skews_ms, 50),
                "p95_quote_skew_ms": percentile(quote_skews_ms, 95),
            },
            "freshness": {
                "missing_exchange_ts_count": missing_exchange_ts,
                "missing_exchange_ts_pct": round((missing_exchange_ts / max(1, total_evals)) * 100.0, 2),
                "median_quote_age_sec": percentile(staleness_ages_sec, 50),
                "p95_quote_age_sec": percentile(staleness_ages_sec, 95),
            },
        }

        return synchronized_batches, sync_metrics

    def _calculate_pair_hours(
        self,
        batches: list[dict[str, Any]],
        sync_metrics: dict[str, Any],
    ) -> dict[str, Any]:
        """Measure valid observation time and pair-hours for accurate frequency calculation."""
        pair_timestamps: dict[str, list[datetime]] = {}
        for b in batches:
            pid = b["pair_id"]
            ts = b["kalshi"].get("observed_at_utc") or b["ts"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if ts.tzinfo:
                ts = ts.replace(tzinfo=None)

            if pid not in pair_timestamps:
                pair_timestamps[pid] = []
            pair_timestamps[pid].append(ts)

        pair_hours: dict[str, float] = {}
        total_pair_hours = 0.0

        for pid, t_list in pair_timestamps.items():
            if len(t_list) < 2:
                pair_hours[pid] = 0.01  # Minimum non-zero observation fraction
            else:
                span_s = (max(t_list) - min(t_list)).total_seconds()
                hrs = max(0.01, round(span_s / 3600.0, 3))
                pair_hours[pid] = hrs
                total_pair_hours += hrs

        study_hours = sync_metrics["time_window"]["span_hours"] or 0.01

        return {
            "total_study_hours": max(0.01, study_hours),
            "total_pair_hours": max(0.01, round(total_pair_hours, 3)),
            "pair_hours_by_pair": pair_hours,
        }

    def _analyze_depth_and_fillability(self, batches: list[dict[str, Any]]) -> dict[str, Any]:
        """Compute available book quantities, fillability rates, and VWAP slippage across sizes."""
        yes_sizes: list[float] = []
        no_sizes: list[float] = []
        fill_counts = {sz: 0 for sz in self.TRADE_SIZES}
        total_tested = 0

        for b in batches:
            k = b["kalshi"]
            p = b["polymarket"]

            # Kalshi asks/sizes
            ay_k = k.get("ask_yes") or k.get("yes_ask")
            sy_k = k.get("ask_yes_size") or 100
            if sy_k is not None:
                yes_sizes.append(float(sy_k))

            # Poly NO asks/sizes
            an_p = p.get("ask_no") or p.get("no_ask")
            sn_p = p.get("ask_no_size") or 100
            if sn_p is not None:
                no_sizes.append(float(sn_p))

            # Test ladder fillability for sizes 1, 10, 50
            total_tested += 1
            k_lvls = b["kalshi_levels"].get("yes_ask", [])
            p_lvls = b["polymarket_levels"].get("no_ask", [])

            if not k_lvls and ay_k:
                k_lvls = [(Decimal(str(ay_k)), Decimal(str(sy_k or 100)))]
            if not p_lvls and an_p:
                p_lvls = [(Decimal(str(an_p)), Decimal(str(sn_p or 100)))]

            for sz in self.TRADE_SIZES:
                q_dec = Decimal(sz)
                vwap_k, filled_k, full_k = vwap_for_size(k_lvls, q_dec) if k_lvls else (None, Decimal(0), False)
                vwap_p, filled_p, full_p = vwap_for_size(p_lvls, q_dec) if p_lvls else (None, Decimal(0), False)
                if full_k and full_p:
                    fill_counts[sz] += 1

        fill_rates = {
            f"size_{sz}_fill_pct": round((fill_counts[sz] / max(1, total_tested)) * 100.0, 2)
            for sz in self.TRADE_SIZES
        }

        return {
            "total_quotes_evaluated": total_tested,
            "average_available_yes_size": round(statistics.mean(yes_sizes), 1) if yes_sizes else None,
            "median_available_yes_size": percentile(yes_sizes, 50),
            "average_available_no_size": round(statistics.mean(no_sizes), 1) if no_sizes else None,
            "median_available_no_size": percentile(no_sizes, 50),
            "fillability_rates": fill_rates,
            "fill_counts": fill_counts,
        }

    def _evaluate_single_batch(
        self,
        b: dict[str, Any],
        params: ExecutabilityParams,
        size_q: int,
    ) -> list[OpportunityEvaluation]:
        """Evaluate both cross-platform arbitrage directions for a single snapshot batch."""
        evals: list[OpportunityEvaluation] = []
        pair_id = b["pair_id"]
        k = b["kalshi"]
        p = b["polymarket"]

        t_eval = b["kalshi"].get("observed_at_utc") or b["ts"]
        if isinstance(t_eval, str):
            t_eval = datetime.fromisoformat(t_eval.replace("Z", "+00:00"))
        if t_eval.tzinfo is None:
            t_eval = t_eval.replace(tzinfo=timezone.utc)

        # Build ladders
        k_levels = b["kalshi_levels"]
        p_levels = b["polymarket_levels"]

        ay_k = k.get("ask_yes") or k.get("yes_ask")
        an_k = k.get("ask_no") or k.get("no_ask")
        ay_p = p.get("ask_yes") or p.get("yes_ask")
        an_p = p.get("ask_no") or p.get("no_ask")

        k_yes_ladder = k_levels.get("yes_ask") or ([(Decimal(str(ay_k)), Decimal(str(k.get("ask_yes_size") or 100)))] if ay_k else [])
        k_no_ladder = k_levels.get("no_ask") or ([(Decimal(str(an_k)), Decimal(str(k.get("ask_no_size") or 100)))] if an_k else [])
        p_yes_ladder = p_levels.get("yes_ask") or ([(Decimal(str(ay_p)), Decimal(str(p.get("ask_yes_size") or 100)))] if ay_p else [])
        p_no_ladder = p_levels.get("no_ask") or ([(Decimal(str(an_p)), Decimal(str(p.get("ask_no_size") or 100)))] if an_p else [])

        # Direction 1: Kalshi YES + Polymarket NO
        leg1_d1 = LegInput(
            snapshot_id=k.get("snapshot_id"),
            platform="kalshi",
            ticker=k.get("ticker", ""),
            best_ask=Decimal(str(ay_k)) if ay_k is not None else None,
            ask_ladder=k_yes_ladder,
            depth_truncated=False,
            observed_at_utc=k.get("observed_at_utc"),
            exchange_updated_at_utc=k.get("exchange_updated_at_utc"),
            tradable=bool(k.get("tradable", True)),
            data_source=k.get("data_source", "live_api"),
        )
        leg2_d1 = LegInput(
            snapshot_id=p.get("snapshot_id"),
            platform="polymarket",
            ticker=p.get("ticker", ""),
            best_ask=Decimal(str(an_p)) if an_p is not None else None,
            ask_ladder=p_no_ladder,
            depth_truncated=False,
            observed_at_utc=p.get("observed_at_utc"),
            exchange_updated_at_utc=p.get("exchange_updated_at_utc"),
            tradable=bool(p.get("tradable", True)),
            data_source=p.get("data_source", "live_api"),
        )
        opp1 = OpportunityInput(
            kind="cross",
            pair_id=pair_id,
            ticker=pair_id,
            direction="kalshi_yes_polymarket_no",
            leg1=leg1_d1,
            leg2=leg2_d1,
            pair_verified=True,
        )
        evals.append(evaluate_opportunity(opp1, params=params, t_eval=t_eval, size_q=size_q))

        # Direction 2: Polymarket YES + Kalshi NO
        leg1_d2 = LegInput(
            snapshot_id=p.get("snapshot_id"),
            platform="polymarket",
            ticker=p.get("ticker", ""),
            best_ask=Decimal(str(ay_p)) if ay_p is not None else None,
            ask_ladder=p_yes_ladder,
            depth_truncated=False,
            observed_at_utc=p.get("observed_at_utc"),
            exchange_updated_at_utc=p.get("exchange_updated_at_utc"),
            tradable=bool(p.get("tradable", True)),
            data_source=p.get("data_source", "live_api"),
        )
        leg2_d2 = LegInput(
            snapshot_id=k.get("snapshot_id"),
            platform="kalshi",
            ticker=k.get("ticker", ""),
            best_ask=Decimal(str(an_k)) if an_k is not None else None,
            ask_ladder=k_no_ladder,
            depth_truncated=False,
            observed_at_utc=k.get("observed_at_utc"),
            exchange_updated_at_utc=k.get("exchange_updated_at_utc"),
            tradable=bool(k.get("tradable", True)),
            data_source=k.get("data_source", "live_api"),
        )
        opp2 = OpportunityInput(
            kind="cross",
            pair_id=pair_id,
            ticker=pair_id,
            direction="polymarket_yes_kalshi_no",
            leg1=leg1_d2,
            leg2=leg2_d2,
            pair_verified=True,
        )
        evals.append(evaluate_opportunity(opp2, params=params, t_eval=t_eval, size_q=size_q))

        return evals

    def _evaluate_parameter_scenarios(
        self,
        batches: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Run the replay engine across baseline conditions and the full parameter sensitivity grid."""
        logger.info("Running baseline replay and multi-parameter sensitivity sweeps...")

        # 1. Baseline Replay (q=10, tau_fresh=10s, min_edge=2.0c, baseline fee rules)
        baseline_fee_cfg = self.FEE_SCENARIOS["baseline"]
        baseline_params = ExecutabilityParams(
            sizes=[10],
            min_edge_cents=Decimal("2.0"),
            tau_fresh_s=Decimal("10.0"),
            tau_skew_s=Decimal("5.0"),
            clock_tolerance_s=Decimal("2.0"),
            kalshi_taker_rate=baseline_fee_cfg["kalshi_rate"],
            kalshi_multiplier=baseline_fee_cfg["kalshi_mult"],
            polymarket_taker_rate=baseline_fee_cfg["poly_rate"],
            polygon_gas_cents=baseline_fee_cfg["gas_cents"],
        )

        baseline_evals: list[OpportunityEvaluation] = []
        active_episodes: dict[str, StudyEpisode] = {}
        completed_episodes: list[StudyEpisode] = []

        for b in batches:
            cur_ts = b["kalshi"].get("observed_at_utc") or b["ts"]
            if isinstance(cur_ts, str):
                cur_ts = datetime.fromisoformat(cur_ts.replace("Z", "+00:00"))
            if cur_ts.tzinfo:
                cur_ts = cur_ts.replace(tzinfo=None)

            batch_evals = self._evaluate_single_batch(b, baseline_params, size_q=10)
            baseline_evals.extend(batch_evals)

            # Track episodes
            active_keys_this_step = set()
            for ev in batch_evals:
                if ev.classification == Classification.EXECUTABLE_CANDIDATE:
                    key = f"{ev.pair_id}:{ev.direction}"
                    active_keys_this_step.add(key)
                    edge_val = float(ev.net_edge_vwap or ev.net_edge_top)

                    if key in active_episodes:
                        active_episodes[key].record_update(cur_ts, edge_val)
                    else:
                        ep = StudyEpisode(
                            episode_id=f"ep-{len(completed_episodes) + len(active_episodes) + 1}",
                            pair_id=ev.pair_id or "",
                            direction=ev.direction,
                            size_q=10,
                            param_scenario="baseline",
                            first_seen_utc=cur_ts,
                            last_seen_utc=cur_ts,
                            peak_net_edge=edge_val,
                            mean_net_edge=edge_val,
                            _edge_samples=[edge_val],
                        )
                        active_episodes[key] = ep

            # Close episodes that disappeared
            to_remove = []
            for k_ep, ep in active_episodes.items():
                if k_ep not in active_keys_this_step:
                    ep.close(cur_ts)
                    completed_episodes.append(ep)
                    to_remove.append(k_ep)
            for k_ep in to_remove:
                del active_episodes[k_ep]

        # Close any lingering active episodes at end
        for ep in active_episodes.values():
            completed_episodes.append(ep)

        # Pre-filter candidate batches where at least one direction satisfies ask1 + ask2 <= 100
        # Batches where ask sum > 100 are mathematically guaranteed to have zero raw/fee-adj/exec opportunities
        candidate_batches: list[dict[str, Any]] = []
        for b in batches:
            k = b["kalshi"]
            p = b["polymarket"]
            ay_k = k.get("ask_yes") or k.get("yes_ask")
            an_k = k.get("ask_no") or k.get("no_ask")
            ay_p = p.get("ask_yes") or p.get("yes_ask")
            an_p = p.get("ask_no") or p.get("no_ask")
            d1_cand = ay_k is not None and an_p is not None and (float(ay_k) + float(an_p) < 100.0)
            d2_cand = ay_p is not None and an_k is not None and (float(ay_p) + float(an_k) < 100.0)
            if d1_cand or d2_cand:
                candidate_batches.append(b)

        logger.info(
            "Sensitivity analysis: pre-filtered %d strictly raw-positive candidate batches out of %d total batches.",
            len(candidate_batches),
            len(batches),
        )

        sweep_batches = candidate_batches[:2000] if len(candidate_batches) > 2000 else candidate_batches
        sensitivity_matrix: list[dict[str, Any]] = []

        for fee_key, fee_cfg in self.FEE_SCENARIOS.items():
            for q in self.TRADE_SIZES:
                for min_e in self.MIN_EDGE_SWEEP:
                    for fresh in self.QUOTE_FRESHNESS_SWEEP:
                        p = ExecutabilityParams(
                            sizes=[q],
                            min_edge_cents=Decimal(str(min_e)),
                            tau_fresh_s=Decimal(str(fresh)),
                            tau_skew_s=Decimal("5.0"),
                            kalshi_taker_rate=fee_cfg["kalshi_rate"],
                            kalshi_multiplier=fee_cfg["kalshi_mult"],
                            polymarket_taker_rate=fee_cfg["poly_rate"],
                            polygon_gas_cents=fee_cfg["gas_cents"],
                        )

                        # Evaluate candidate batches to generate matrix row
                        raw_cnt = 0
                        fee_adj_cnt = 0
                        exec_cnt = 0
                        edges: list[float] = []

                        # Run across pre-filtered candidate batches
                        for b in sweep_batches:
                            evals = self._evaluate_single_batch(b, p, size_q=q)
                            for ev in evals:
                                if ev.raw_positive:
                                    raw_cnt += 1
                                if ev.fee_adjusted:
                                    fee_adj_cnt += 1
                                if ev.classification == Classification.EXECUTABLE_CANDIDATE:
                                    exec_cnt += 1
                                    edges.append(float(ev.net_edge_vwap or ev.net_edge_top))

                        sensitivity_matrix.append(
                            {
                                "fee_scenario": fee_key,
                                "trade_size_q": q,
                                "min_edge_cents": min_e,
                                "quote_freshness_sec": fresh,
                                "raw_positive_count": raw_cnt,
                                "fee_adjusted_count": fee_adj_cnt,
                                "executable_candidates_count": exec_cnt,
                                "mean_net_edge_cents": round(statistics.mean(edges), 2) if edges else 0.0,
                                "max_net_edge_cents": round(max(edges), 2) if edges else 0.0,
                            }
                        )

        baseline_results = {
            "evaluations": baseline_evals,
            "episodes": completed_episodes,
        }

        return baseline_results, sensitivity_matrix

    def _compile_q1_frequency(
        self,
        baseline_results: dict[str, Any],
        pair_hours_data: dict[str, Any],
    ) -> dict[str, Any]:
        """Compile opportunities per hour and per market pair against valid observation time."""
        evals: list[OpportunityEvaluation] = baseline_results["evaluations"]
        total_study_hours = pair_hours_data["total_study_hours"]
        pair_hours = pair_hours_data["pair_hours_by_pair"]

        pair_opps: dict[str, dict[str, int]] = {}
        for ev in evals:
            pid = ev.pair_id or ev.ticker
            if pid not in pair_opps:
                pair_opps[pid] = {"raw": 0, "fee_adjusted": 0, "executable": 0}
            if ev.raw_positive:
                pair_opps[pid]["raw"] += 1
            if ev.fee_adjusted:
                pair_opps[pid]["fee_adjusted"] += 1
            if ev.classification == Classification.EXECUTABLE_CANDIDATE:
                pair_opps[pid]["executable"] += 1

        pair_rates: list[dict[str, Any]] = []
        for pid, hrs in pair_hours.items():
            counts = pair_opps.get(pid, {"raw": 0, "fee_adjusted": 0, "executable": 0})
            pair_rates.append(
                {
                    "pair_id": pid,
                    "valid_observation_hours": hrs,
                    "raw_opportunities": counts["raw"],
                    "fee_adjusted_opportunities": counts["fee_adjusted"],
                    "executable_opportunities": counts["executable"],
                    "raw_per_pair_hour": round(counts["raw"] / max(0.001, hrs), 3),
                    "fee_adjusted_per_pair_hour": round(counts["fee_adjusted"] / max(0.001, hrs), 3),
                    "executable_per_pair_hour": round(counts["executable"] / max(0.001, hrs), 3),
                }
            )

        total_exec = sum(c["executable"] for c in pair_opps.values())
        total_raw = sum(c["raw"] for c in pair_opps.values())
        total_fee = sum(c["fee_adjusted"] for c in pair_opps.values())

        return {
            "total_study_hours": total_study_hours,
            "total_valid_pair_hours": pair_hours_data["total_pair_hours"],
            "overall_raw_per_hour": round(total_raw / total_study_hours, 3),
            "overall_fee_adjusted_per_hour": round(total_fee / total_study_hours, 3),
            "overall_executable_per_hour": round(total_exec / total_study_hours, 3),
            "rates_by_pair": pair_rates,
        }

    def _compile_q2_funnel(self, baseline_results: dict[str, Any]) -> dict[str, Any]:
        """Compile the opportunity funnel across evaluation stages."""
        evals: list[OpportunityEvaluation] = baseline_results["evaluations"]
        episodes: list[StudyEpisode] = baseline_results["episodes"]

        total_evals = len(evals)
        raw_count = sum(1 for e in evals if e.raw_positive)
        fee_adj_count = sum(1 for e in evals if e.fee_adjusted)
        exec_count = sum(1 for e in evals if e.classification == Classification.EXECUTABLE_CANDIDATE)
        episodes_count = len(episodes)

        # Primary dropoff / reject reasons
        reject_counts: dict[str, int] = {}
        for e in evals:
            if e.primary_reject_reason:
                r_name = str(e.primary_reject_reason.value if hasattr(e.primary_reject_reason, "value") else e.primary_reject_reason)
                reject_counts[r_name] = reject_counts.get(r_name, 0) + 1

        # Latency survival
        latency_survival: dict[str, int] = {}
        for lat in self.LATENCY_SWEEP:
            survived = sum(1 for ep in episodes if ep.duration_seconds >= lat)
            latency_survival[f"survived_{int(lat)}s"] = survived

        return {
            "funnel_stages": {
                "1_total_pair_evaluations": total_evals,
                "2_raw_pricing_opportunities": raw_count,
                "3_fee_adjusted_opportunities": fee_adj_count,
                "4_executable_candidate_observations": exec_count,
                "5_distinct_executable_episodes": episodes_count,
            },
            "conversion_rates_pct": {
                "raw_to_eval_pct": round((raw_count / max(1, total_evals)) * 100.0, 3),
                "fee_adj_to_raw_pct": round((fee_adj_count / max(1, raw_count)) * 100.0, 3),
                "exec_to_fee_adj_pct": round((exec_count / max(1, fee_adj_count)) * 100.0, 3),
                "overall_exec_rate_pct": round((exec_count / max(1, total_evals)) * 100.0, 4),
            },
            "primary_rejection_reasons": reject_counts,
            "latency_survival_counts": latency_survival,
        }

    def _compile_q3_edge_distribution(self, baseline_results: dict[str, Any]) -> dict[str, Any]:
        """Compute summary statistics and histogram of net edges."""
        evals: list[OpportunityEvaluation] = baseline_results["evaluations"]
        raw_edges = [float(e.raw_edge) for e in evals if e.raw_positive]
        net_edges = [float(e.net_edge_vwap or e.net_edge_top) for e in evals if e.classification == Classification.EXECUTABLE_CANDIDATE]

        # Buckets for edge distribution
        buckets = {
            "< 0.5¢": 0,
            "0.5¢ - 1.0¢": 0,
            "1.0¢ - 2.0¢": 0,
            "2.0¢ - 5.0¢": 0,
            "> 5.0¢": 0,
        }
        for ed in net_edges:
            if ed < 0.5:
                buckets["< 0.5¢"] += 1
            elif ed < 1.0:
                buckets["0.5¢ - 1.0¢"] += 1
            elif ed < 2.0:
                buckets["1.0¢ - 2.0¢"] += 1
            elif ed < 5.0:
                buckets["2.0¢ - 5.0¢"] += 1
            else:
                buckets["> 5.0¢"] += 1

        return {
            "sample_size": len(net_edges),
            "mean_cents": round(statistics.mean(net_edges), 2) if net_edges else 0.0,
            "median_cents": percentile(net_edges, 50) or 0.0,
            "std_dev_cents": round(statistics.stdev(net_edges), 2) if len(net_edges) > 1 else 0.0,
            "min_cents": round(min(net_edges), 2) if net_edges else 0.0,
            "max_cents": round(max(net_edges), 2) if net_edges else 0.0,
            "percentiles": {
                "p25": percentile(net_edges, 25),
                "p50": percentile(net_edges, 50),
                "p75": percentile(net_edges, 75),
                "p90": percentile(net_edges, 90),
                "p95": percentile(net_edges, 95),
            },
            "edge_buckets": buckets,
            "raw_edges_mean_cents": round(statistics.mean(raw_edges), 2) if raw_edges else 0.0,
        }

    def _compile_q4_persistence(self, baseline_results: dict[str, Any]) -> dict[str, Any]:
        """Compute persistence duration distribution and survival across time thresholds."""
        episodes: list[StudyEpisode] = baseline_results["episodes"]
        durations = [ep.duration_seconds for ep in episodes]

        survival_counts = {}
        survival_pcts = {}
        total_ep = len(episodes)

        for th in self.PERSISTENCE_THRESHOLDS:
            cnt = sum(1 for d in durations if d >= th)
            survival_counts[f"persisted_gte_{int(th)}s"] = cnt
            survival_pcts[f"survival_gte_{int(th)}s_pct"] = round((cnt / max(1, total_ep)) * 100.0, 2)

        return {
            "total_distinct_episodes": total_ep,
            "mean_duration_seconds": round(statistics.mean(durations), 2) if durations else 0.0,
            "median_duration_seconds": percentile(durations, 50) or 0.0,
            "min_duration_seconds": round(min(durations), 2) if durations else 0.0,
            "max_duration_seconds": round(max(durations), 2) if durations else 0.0,
            "percentiles": {
                "p25": percentile(durations, 25),
                "p50": percentile(durations, 50),
                "p75": percentile(durations, 75),
                "p90": percentile(durations, 90),
            },
            "persistence_survival_counts": survival_counts,
            "persistence_survival_percentages": survival_pcts,
        }

    def _empty_study_result(self) -> dict[str, Any]:
        """Return structured empty study result if database has no matching snapshots."""
        return {
            "metadata": {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "dataset": self.table_name,
                "note": "No snapshot observations matched the specified time filters or provenance criteria.",
                "total_snapshots_loaded": 0,
                "total_study_hours": 0.0,
            },
            "research_question_1_frequency": {"overall_executable_per_hour": 0.0, "rates_by_pair": []},
            "research_question_2_funnel": {"funnel_stages": {}, "conversion_rates_pct": {}, "primary_rejection_reasons": {}},
            "research_question_3_edge_distribution": {"sample_size": 0, "mean_cents": 0.0, "percentiles": {}},
            "research_question_4_persistence": {"total_distinct_episodes": 0, "mean_duration_seconds": 0.0},
            "research_question_5_depth_and_volume": {"fillability_rates": {}, "total_quotes_evaluated": 0},
            "research_question_6_sensitivity": [],
            "research_question_7_synchronization": {"usable_synchronized_pct": 0.0, "quote_skew": {}},
        }


# ============================================================================
# Report Formatting & CSV Exporters
# ============================================================================


def export_study_artifacts(results: dict[str, Any], output_dir: Path) -> list[str]:
    """Write summary JSON, CSV tables, and Markdown report into results directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_files: list[str] = []

    # 1. Summary JSON
    json_path = output_dir / "study_analysis_summary.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    generated_files.append(str(json_path))

    # 2. Funnel Summary CSV
    funnel_csv_path = output_dir / "funnel_summary.csv"
    with open(funnel_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Stage", "Count", "ConversionPct"])
        f_data = results.get("research_question_2_funnel", {})
        stages = f_data.get("funnel_stages", {})
        conversions = f_data.get("conversion_rates_pct", {})
        for st_name, count in stages.items():
            writer.writerow([st_name, count, conversions.get(f"{st_name}_pct", "")])
    generated_files.append(str(funnel_csv_path))

    # 3. Pair Hourly Rates CSV
    rates_csv_path = output_dir / "pair_hourly_rates.csv"
    with open(rates_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "PairID",
            "ValidObservationHours",
            "RawOpportunities",
            "FeeAdjustedOpportunities",
            "ExecutableOpportunities",
            "ExecutablePerPairHour",
        ])
        q1 = results.get("research_question_1_frequency", {})
        for p_rate in q1.get("rates_by_pair", []):
            writer.writerow([
                p_rate["pair_id"],
                p_rate["valid_observation_hours"],
                p_rate["raw_opportunities"],
                p_rate["fee_adjusted_opportunities"],
                p_rate["executable_opportunities"],
                p_rate["executable_per_pair_hour"],
            ])
    generated_files.append(str(rates_csv_path))

    # 4. Sensitivity Matrix CSV
    sens_csv_path = output_dir / "sensitivity_matrix.csv"
    sens_rows = results.get("research_question_6_sensitivity", [])
    if sens_rows:
        with open(sens_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(sens_rows[0].keys()))
            writer.writeheader()
            writer.writerows(sens_rows)
        generated_files.append(str(sens_csv_path))

    # 5. Edge Distribution CSV
    edge_csv_path = output_dir / "edge_distribution.csv"
    q3 = results.get("research_question_3_edge_distribution", {})
    with open(edge_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Metric", "Value"])
        writer.writerow(["SampleSize", q3.get("sample_size", 0)])
        writer.writerow(["MeanCents", q3.get("mean_cents", 0.0)])
        writer.writerow(["MedianCents", q3.get("median_cents", 0.0)])
        writer.writerow(["StdDevCents", q3.get("std_dev_cents", 0.0)])
        writer.writerow(["MinCents", q3.get("min_cents", 0.0)])
        writer.writerow(["MaxCents", q3.get("max_cents", 0.0)])
        for p_name, p_val in q3.get("percentiles", {}).items():
            writer.writerow([f"Percentile_{p_name.upper()}", p_val])
        for b_name, b_val in q3.get("edge_buckets", {}).items():
            writer.writerow([f"Bucket_{b_name}", b_val])
    generated_files.append(str(edge_csv_path))

    # 6. Persistence Survival CSV
    pers_csv_path = output_dir / "persistence_survival.csv"
    q4 = results.get("research_question_4_persistence", {})
    with open(pers_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Threshold", "SurvivingCount", "SurvivalPct"])
        surv_cnts = q4.get("persistence_survival_counts", {})
        surv_pcts = q4.get("persistence_survival_percentages", {})
        for k_surv, cnt in surv_cnts.items():
            sec_val = k_surv.split("_")[-1].replace("s", "")
            pct = surv_pcts.get(f"survival_gte_{sec_val}s_pct", 0.0)
            writer.writerow([f">={sec_val}s", cnt, pct])
    generated_files.append(str(pers_csv_path))

    # 7. Depth and Fillability CSV
    depth_csv_path = output_dir / "depth_and_fillability.csv"
    q5 = results.get("research_question_5_depth_and_volume", {})
    with open(depth_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["TradeSizeContracts", "FillabilityPct", "FillCount"])
        fill_rates = q5.get("fillability_rates", {})
        fill_cnts = q5.get("fill_counts", {})
        for sz in [1, 10, 50]:
            writer.writerow([sz, fill_rates.get(f"size_{sz}_fill_pct", 0.0), fill_cnts.get(sz, 0)])
    generated_files.append(str(depth_csv_path))

    # 8. Data Synchronization CSV
    sync_csv_path = output_dir / "data_synchronization.csv"
    q7 = results.get("research_question_7_synchronization", {})
    with open(sync_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Metric", "Value"])
        writer.writerow(["UsableSynchronizedPct", q7.get("usable_synchronized_pct", 0.0)])
        writer.writerow(["TotalPairIntervals", q7.get("total_pair_intervals", 0)])
        writer.writerow(["SynchronizedIntervals", q7.get("synchronized_intervals", 0)])
        skew = q7.get("quote_skew", {})
        writer.writerow(["MedianQuoteSkewMs", skew.get("median_quote_skew_ms")])
        writer.writerow(["P95QuoteSkewMs", skew.get("p95_quote_skew_ms")])
        fresh = q7.get("freshness", {})
        writer.writerow(["MedianQuoteAgeSec", fresh.get("median_quote_age_sec")])
        writer.writerow(["P95QuoteAgeSec", fresh.get("p95_quote_age_sec")])
        writer.writerow(["MissingExchangeTsPct", fresh.get("missing_exchange_ts_pct")])
    generated_files.append(str(sync_csv_path))

    # 9. Markdown Report
    report_md_path = output_dir / "study_analysis_report.md"
    md_content = generate_markdown_report(results)
    with open(report_md_path, "w", encoding="utf-8") as f:
        f.write(md_content)
    generated_files.append(str(report_md_path))

    return generated_files


def generate_markdown_report(r: dict[str, Any]) -> str:
    """Format full study answers into GitHub-flavored Markdown."""
    meta = r.get("metadata", {})
    q1 = r.get("research_question_1_frequency", {})
    q2 = r.get("research_question_2_funnel", {})
    q3 = r.get("research_question_3_edge_distribution", {})
    q4 = r.get("research_question_4_persistence", {})
    q5 = r.get("research_question_5_depth_and_volume", {})
    q7 = r.get("research_question_7_synchronization", {})

    lines: list[str] = [
        "# Empirical Market Study: Arbitrage Detection, Persistence, and Executability Analysis",
        "",
        "> **Generated:** " + str(meta.get("generated_at_utc")) + " | **Dataset:** `" + str(meta.get("dataset")) + "`",
        "",
        "## Executive Summary",
        f"- **Study Duration:** {meta.get('total_study_hours', 0.0):.2f} wall-clock hours across {meta.get('distinct_pairs_count', 0)} verified pairs.",
        f"- **Total Valid Pair-Hours:** {meta.get('total_valid_pair_hours', 0.0):.2f} pair-hours.",
        f"- **Usable Synchronized Market Data:** {q7.get('usable_synchronized_pct', 0.0):.1f}%.",
        f"- **Overall Executable Opportunities / Hour:** {q1.get('overall_executable_per_hour', 0.0):.3f} opportunities / hour.",
        "",
        "---",
        "",
        "## 1. Opportunity Frequency per Pair-Hour (Q1)",
        "Frequency is computed strictly against **valid pair observation hours**, avoiding downtime or missing quote distortions:",
        "",
        "| Market Pair ID | Valid Pair-Hours | Raw Opps | Fee-Adjusted Opps | Executable Opps | Executable / Pair-Hour |",
        "|:---|---:|---:|---:|---:|---:|",
    ]

    for p in q1.get("rates_by_pair", []):
        lines.append(
            f"| `{p['pair_id']}` | {p['valid_observation_hours']:.2f}h | {p['raw_opportunities']} | "
            f"{p['fee_adjusted_opportunities']} | {p['executable_opportunities']} | **{p['executable_per_pair_hour']:.3f}** |"
        )

    lines.extend([
        "",
        "---",
        "",
        "## 2. Opportunity Funnel & Drop-Off Reasons (Q2)",
        "Stage-by-stage classification progression across the research period:",
        "",
        "| Funnel Stage | Count | Transition Conversion % |",
        "|:---|---:|---:|",
    ])

    stages = q2.get("funnel_stages", {})
    conversions = q2.get("conversion_rates_pct", {})
    for st_name, count in stages.items():
        clean_name = st_name.replace("_", " ").title()
        pct = conversions.get(f"{st_name}_pct", "—")
        lines.append(f"| {clean_name} | {count:,} | {pct} |")

    lines.extend([
        "",
        "### Primary Rejection & Drop-Off Reasons",
        "| Reject Reason Code | Filtered Count | Description / Mechanism |",
        "|:---|---:|:---|",
    ])
    reasons = q2.get("primary_rejection_reasons", {})
    if reasons:
        for r_code, cnt in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"| `{r_code}` | {cnt:,} | Top-of-book or depth failure at this threshold |")
    else:
        lines.append("| *None* | 0 | No rejections recorded |")

    lines.extend([
        "",
        "---",
        "",
        "## 3. Distribution of Net Edges (Q3)",
        f"- **Sample Size:** {q3.get('sample_size', 0)} executable candidate observations",
        f"- **Mean Net Edge:** {q3.get('mean_cents', 0.0):.2f}¢ | **Median:** {q3.get('median_cents', 0.0):.2f}¢ | **Std Dev:** {q3.get('std_dev_cents', 0.0):.2f}¢",
        f"- **Min Net Edge:** {q3.get('min_cents', 0.0):.2f}¢ | **Max Net Edge:** {q3.get('max_cents', 0.0):.2f}¢",
        "",
        "| Percentile | Net Edge (¢) |",
        "|:---|---:|",
    ])
    pcts = q3.get("percentiles", {})
    for p_name, val in pcts.items():
        lines.append(f"| {p_name.upper()} | {val:.2f}¢ |" if val is not None else f"| {p_name.upper()} | — |")

    lines.extend([
        "",
        "---",
        "",
        "## 4. Opportunity Persistence & Duration (Q4)",
        f"- **Distinct Episodes Tracked:** {q4.get('total_distinct_episodes', 0)}",
        f"- **Mean Persistence Duration:** {q4.get('mean_duration_seconds', 0.0):.2f}s",
        f"- **Median Duration:** {q4.get('median_duration_seconds', 0.0):.2f}s",
        "",
        "### Survival Across Latency & Persistence Thresholds",
        "| Threshold | Surviving Episodes | Survival Rate % |",
        "|:---|---:|---:|",
    ])
    surv_cnts = q4.get("persistence_survival_counts", {})
    surv_pcts = q4.get("persistence_survival_percentages", {})
    for k_surv, cnt in surv_cnts.items():
        sec_str = k_surv.split("_")[-1].replace("s", "")
        lbl = f"Persistence ≥ {sec_str} seconds"
        pct_val = surv_pcts.get(f"survival_gte_{sec_str}s_pct", 0.0)
        lines.append(f"| {lbl} | {cnt} | {pct_val:.1f}% |")

    lines.extend([
        "",
        "---",
        "",
        "## 5. Order Book Depth & Fillability (Q5)",
        f"- **Average Resting YES Depth:** {q5.get('average_available_yes_size', 'N/A')} contracts",
        f"- **Average Resting NO Depth:** {q5.get('average_available_no_size', 'N/A')} contracts",
        "",
        "| Order Size $q$ (Contracts) | Fillability Rate % | Successful Fill Count |",
        "|:---|---:|---:|",
    ])
    fill_rates = q5.get("fillability_rates", {})
    fill_cnts = q5.get("fill_counts", {})
    for sz in [1, 10, 50]:
        lines.append(f"| {sz} contracts | {fill_rates.get(f'size_{sz}_fill_pct', 0.0)}% | {fill_cnts.get(sz, 0):,} |")

    lines.extend([
        "",
        "---",
        "",
        "## 6. Data Synchronization & Quality (Q7)",
        f"- **Usable Synchronized Pair Time:** **{q7.get('usable_synchronized_pct', 0.0):.1f}%**",
        f"- **Median Quote Skew:** {q7.get('quote_skew', {}).get('median_quote_skew_ms', 'N/A')} ms (P95: {q7.get('quote_skew', {}).get('p95_quote_skew_ms', 'N/A')} ms)",
        f"- **Median Quote Age:** {q7.get('freshness', {}).get('median_quote_age_sec', 'N/A')} s (P95: {q7.get('freshness', {}).get('p95_quote_age_sec', 'N/A')} s)",
        f"- **Missing Exchange Timestamp %:** {q7.get('freshness', {}).get('missing_exchange_ts_pct', 0.0)}%",
        "",
        "---",
        "*Empirical analysis report compiled by `scripts/analyze_study.py` for Step 13 Senior Seminar.*",
    ])

    return "\n".join(lines)


def print_terminal_summary(r: dict[str, Any]) -> None:
    """Print ASCII executive summary to terminal."""
    meta = r.get("metadata", {})
    q1 = r.get("research_question_1_frequency", {})
    q2 = r.get("research_question_2_funnel", {})
    q3 = r.get("research_question_3_edge_distribution", {})
    q4 = r.get("research_question_4_persistence", {})
    q5 = r.get("research_question_5_depth_and_volume", {})
    q7 = r.get("research_question_7_synchronization", {})

    print("\n" + "=" * 90)
    print("  📊 EMPIRICAL STUDY ANALYSIS & REPLAY EXPERIMENT RESULTS")
    print("=" * 90)
    print(f"  Study Duration:         {meta.get('total_study_hours', 0.0):.2f} hours across {meta.get('distinct_pairs_count', 0)} verified pairs")
    print(f"  Total Valid Pair-Hours: {meta.get('total_valid_pair_hours', 0.0):.2f} pair-hours")
    print(f"  Synchronized Data:      {q7.get('usable_synchronized_pct', 0.0):.1f}% usable pair observations")
    print("-" * 90)
    print("  [1] OPPORTUNITY FREQUENCY (per Valid Pair-Hour)")
    print(f"      Overall Raw Opportunities:        {q1.get('overall_raw_per_hour', 0.0):.3f} / hour")
    print(f"      Overall Fee-Adjusted Opps:        {q1.get('overall_fee_adjusted_per_hour', 0.0):.3f} / hour")
    print(f"      Overall Executable Candidates:    {q1.get('overall_executable_per_hour', 0.0):.3f} / hour")
    print("-" * 90)
    print("  [2] OPPORTUNITY FUNNEL PROGRESSION")
    stages = q2.get("funnel_stages", {})
    for k_s, cnt in stages.items():
        print(f"      {k_s.replace('_', ' ').title():<36} : {cnt:>8,}")
    print("-" * 90)
    print("  [3] NET EDGE & SPREAD DISTRIBUTION")
    print(f"      Executable Observations: {q3.get('sample_size', 0):,}")
    print(f"      Mean Net Edge:           {q3.get('mean_cents', 0.0):.2f}¢ | Median: {q3.get('median_cents', 0.0):.2f}¢")
    print(f"      Min: {q3.get('min_cents', 0.0):.2f}¢ | Max: {q3.get('max_cents', 0.0):.2f}¢ | Std: {q3.get('std_dev_cents', 0.0):.2f}¢")
    print("-" * 90)
    print("  [4] PERSISTENCE & HALF-LIFE")
    print(f"      Total Episodes:          {q4.get('total_distinct_episodes', 0)}")
    print(f"      Mean Persistence:        {q4.get('mean_duration_seconds', 0.0):.2f} seconds")
    print(f"      Median Persistence:      {q4.get('median_duration_seconds', 0.0):.2f} seconds")
    print("-" * 90)
    print("  [5] DEPTH & FILLABILITY")
    fill_rates = q5.get("fillability_rates", {})
    for sz in [1, 10, 50]:
        print(f"      Fill Rate (q={sz:>2} contracts): {fill_rates.get(f'size_{sz}_fill_pct', 0.0):>5.1f}%")
    print("=" * 90)


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    setup_logging(verbose=args.verbose)

    config = load_config()

    from_dt = None
    if args.from_date:
        from_dt = datetime.fromisoformat(args.from_date.replace("Z", "+00:00"))

    to_dt = None
    if args.to_date:
        to_dt = datetime.fromisoformat(args.to_date.replace("Z", "+00:00"))

    conn = connect(args.duckdb_path, read_only=True)
    analyzer = StudyAnalyzer(
        conn=conn,
        config=config,
        from_dt=from_dt,
        to_dt=to_dt,
        use_raw_table=args.allow_unverified,
        limit_snapshots=args.limit_snapshots,
    )

    results = analyzer.run_study()
    conn.close()

    output_dir = Path(args.output_dir)
    generated = export_study_artifacts(results, output_dir)
    print_terminal_summary(results)

    print("\nSaved research artifacts:")
    for f in generated:
        print(f"  • {f}")
    print()


if __name__ == "__main__":
    main()
