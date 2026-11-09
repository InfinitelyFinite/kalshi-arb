"""Persistence and DuckDB storage for opportunity evaluations, episodes, and latency analysis.

Manages migration and schema for Step 12 tables:
- param_sets: parameter configuration sets and code version.
- opportunity_evaluations: individual evaluated opportunity records (raw_positive=True).
- evaluation_counts: aggregated counts to preserve denominators for all evaluations.
- opportunity_episodes: runs of consecutive observations across levels.
- episode_latency_results: latency survival results per episode and latency threshold.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
import logging
from pathlib import Path
from typing import Any, Optional, Sequence

import duckdb

from core.executability import (
    Episode,
    EpisodeTracker,
    ExecutabilityParams,
    LatencyStatus,
    OpportunityEvaluation,
)

logger = logging.getLogger(__name__)

OPPORTUNITY_STORE_SCHEMA_VERSION = 1


def ensure_executability_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all tables required for Step 12 opportunity executability analysis. Idempotent."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS param_sets (
            param_set_id VARCHAR PRIMARY KEY,
            params_json VARCHAR,
            code_version VARCHAR,
            created_at_utc TIMESTAMP
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS opportunity_evaluations (
            param_set_id VARCHAR,
            evaluation_id VARCHAR,
            kind VARCHAR,
            pair_id VARCHAR,
            ticker VARCHAR,
            direction VARCHAR,
            size_q INTEGER,
            raw_edge DECIMAL(10, 4),
            raw_positive BOOLEAN,
            net_edge_top DECIMAL(10, 4),
            fee_adjusted BOOLEAN,
            vwap1 DECIMAL(10, 4),
            vwap2 DECIMAL(10, 4),
            fees_cents DECIMAL(10, 4),
            gas_cents_total DECIMAL(10, 4),
            slippage_cents DECIMAL(10, 4),
            net_edge_vwap DECIMAL(10, 4),
            quote_age_k DECIMAL(10, 4),
            quote_age_p DECIMAL(10, 4),
            quote_skew DECIMAL(10, 4),
            snapshot_id_1 BIGINT,
            snapshot_id_2 BIGINT,
            data_source VARCHAR,
            classification VARCHAR,
            primary_reject_reason VARCHAR,
            all_reject_reasons VARCHAR,
            depth_truncated BOOLEAN,
            t_eval_utc TIMESTAMP
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS evaluation_counts (
            param_set_id VARCHAR,
            eval_date DATE,
            pair_or_ticker VARCHAR,
            kind VARCHAR,
            size_q INTEGER,
            n_evaluated BIGINT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS opportunity_episodes (
            episode_id VARCHAR,
            param_set_id VARCHAR,
            kind VARCHAR,
            pair_or_ticker VARCHAR,
            direction VARCHAR,
            size_q INTEGER,
            level VARCHAR,
            first_seen_utc TIMESTAMP,
            last_seen_utc TIMESTAMP,
            n_observations INTEGER,
            peak_edge DECIMAL(10, 4),
            mean_edge DECIMAL(10, 4),
            duration_lower_s DECIMAL(10, 4),
            duration_upper_s DECIMAL(10, 4),
            censored_left BOOLEAN,
            censored_right BOOLEAN
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS episode_latency_results (
            episode_id VARCHAR,
            param_set_id VARCHAR,
            latency_s DOUBLE,
            status VARCHAR,
            edge_after_latency DECIMAL(10, 4)
        )
        """
    )


def save_param_set(
    conn: duckdb.DuckDBPyConnection,
    param_set_id: str,
    params: ExecutabilityParams,
    code_version: str = "step12",
    extra: Optional[dict[str, Any]] = None,
) -> None:
    """Store parameter set configuration idempotently."""
    data = {
        "sizes": params.sizes,
        "min_edge_cents": str(params.min_edge_cents),
        "tau_fresh_s": str(params.tau_fresh_s),
        "tau_skew_s": str(params.tau_skew_s),
        "clock_tolerance_s": str(params.clock_tolerance_s),
        "kalshi_taker_rate": str(params.kalshi_taker_rate),
        "kalshi_multiplier": str(params.kalshi_multiplier),
        "polymarket_taker_rate": str(params.polymarket_taker_rate),
        "polygon_gas_cents": str(params.polygon_gas_cents),
        "slippage_cents_per_bundle": str(params.slippage_cents_per_bundle),
    }
    if extra:
        data.update(extra)

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(
        """
        INSERT INTO param_sets (param_set_id, params_json, code_version, created_at_utc)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (param_set_id) DO UPDATE SET
            params_json = EXCLUDED.params_json,
            code_version = EXCLUDED.code_version,
            created_at_utc = EXCLUDED.created_at_utc
        """,
        [param_set_id, json.dumps(data), code_version, now_utc],
    )


def write_evaluations_and_counts(
    conn: duckdb.DuckDBPyConnection,
    param_set_id: str,
    evaluations: Sequence[OpportunityEvaluation],
    t_eval: datetime,
) -> None:
    """Write evaluations (full rows only where raw_positive is True) and aggregated count denominators.

    Idempotent for a given (param_set_id, snapshot ids).
    """
    if not evaluations:
        return

    t_eval_naive = t_eval.astimezone(timezone.utc).replace(tzinfo=None) if t_eval.tzinfo else t_eval
    eval_d = t_eval_naive.date()

    # Aggregate total evaluations for denominators
    # key: (pair_or_ticker, kind, size_q) -> count
    counts: dict[tuple[str, str, int], int] = {}
    for ev in evaluations:
        key_id = ev.pair_id or ev.ticker
        k = (key_id, ev.kind, ev.size_q)
        counts[k] = counts.get(k, 0) + 1

    # Insert or update evaluation_counts
    for (pair_or_ticker, kind, size_q), n_eval in counts.items():
        conn.execute(
            """
            INSERT INTO evaluation_counts (param_set_id, eval_date, pair_or_ticker, kind, size_q, n_evaluated)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [param_set_id, eval_d, pair_or_ticker, kind, size_q, n_eval],
        )

    # Filter to raw_positive=True for full row persistence
    raw_positive_evals = [ev for ev in evaluations if ev.raw_positive]
    if not raw_positive_evals:
        return

    # Delete any existing matching rows for idempotency
    snapshot_pairs = {(ev.snapshot_id_1, ev.snapshot_id_2) for ev in raw_positive_evals if ev.snapshot_id_1 is not None}
    for sid1, sid2 in snapshot_pairs:
        conn.execute(
            """
            DELETE FROM opportunity_evaluations
            WHERE param_set_id = ? AND snapshot_id_1 = ? AND (snapshot_id_2 = ? OR (snapshot_id_2 IS NULL AND ? IS NULL))
            """,
            [param_set_id, sid1, sid2, sid2],
        )

    rows: list[tuple[Any, ...]] = []
    for ev in raw_positive_evals:
        eval_id = f"{ev.kind}_{ev.pair_id or ev.ticker}_{ev.direction}_q{ev.size_q}_{ev.snapshot_id_1}_{ev.snapshot_id_2}"
        all_reasons_str = json.dumps([r.value for r in ev.all_reject_reasons])
        primary_str = ev.primary_reject_reason.value if ev.primary_reject_reason else None

        rows.append(
            (
                param_set_id,
                eval_id,
                ev.kind,
                ev.pair_id,
                ev.ticker,
                ev.direction,
                ev.size_q,
                ev.raw_edge,
                ev.raw_positive,
                ev.net_edge_top,
                ev.fee_adjusted,
                ev.vwap1,
                ev.vwap2,
                ev.fees_cents,
                ev.gas_cents_total,
                ev.slippage_cents,
                ev.net_edge_vwap,
                ev.quote_age_k,
                ev.quote_age_p,
                ev.quote_skew,
                ev.snapshot_id_1,
                ev.snapshot_id_2,
                ev.data_source,
                ev.classification.value,
                primary_str,
                all_reasons_str,
                ev.depth_truncated,
                t_eval_naive,
            )
        )

    conn.executemany(
        """
        INSERT INTO opportunity_evaluations (
            param_set_id, evaluation_id, kind, pair_id, ticker, direction, size_q,
            raw_edge, raw_positive, net_edge_top, fee_adjusted, vwap1, vwap2,
            fees_cents, gas_cents_total, slippage_cents, net_edge_vwap,
            quote_age_k, quote_age_p, quote_skew, snapshot_id_1, snapshot_id_2,
            data_source, classification, primary_reject_reason, all_reject_reasons,
            depth_truncated, t_eval_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def write_episodes_and_latencies(
    conn: duckdb.DuckDBPyConnection,
    param_set_id: str,
    tracker: EpisodeTracker,
    episodes: Sequence[Episode],
) -> None:
    """Store finalized episodes and latency survival evaluations idempotently."""
    if not episodes:
        return

    # Delete previous episodes for this param_set_id to avoid duplication
    conn.execute("DELETE FROM episode_latency_results WHERE param_set_id = ?", [param_set_id])
    conn.execute("DELETE FROM opportunity_episodes WHERE param_set_id = ?", [param_set_id])

    ep_rows: list[tuple[Any, ...]] = []
    lat_rows: list[tuple[Any, ...]] = []

    for ep in episodes:
        first_naive = ep.first_seen_utc.astimezone(timezone.utc).replace(tzinfo=None)
        last_naive = ep.last_seen_utc.astimezone(timezone.utc).replace(tzinfo=None)

        ep_rows.append(
            (
                ep.episode_id,
                param_set_id,
                ep.kind,
                ep.pair_or_ticker,
                ep.direction,
                ep.size_q,
                ep.level,
                first_naive,
                last_naive,
                ep.n_observations,
                ep.peak_edge,
                ep.mean_edge,
                ep.duration_lower_s,
                ep.duration_upper_s,
                ep.censored_left,
                ep.censored_right,
            )
        )

        for L in tracker.latencies_s:
            status, edge_l = tracker.evaluate_latency_status(ep, L)
            lat_rows.append(
                (
                    ep.episode_id,
                    param_set_id,
                    float(L),
                    status.value,
                    edge_l,
                )
            )

    conn.executemany(
        """
        INSERT INTO opportunity_episodes (
            episode_id, param_set_id, kind, pair_or_ticker, direction, size_q,
            level, first_seen_utc, last_seen_utc, n_observations, peak_edge,
            mean_edge, duration_lower_s, duration_upper_s, censored_left, censored_right
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ep_rows,
    )

    conn.executemany(
        """
        INSERT INTO episode_latency_results (
            episode_id, param_set_id, latency_s, status, edge_after_latency
        ) VALUES (?, ?, ?, ?, ?)
        """,
        lat_rows,
    )
