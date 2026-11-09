#!/usr/bin/env python3
"""Opportunity Funnel & Executability Analysis Report Generator.

Queries stored DuckDB executability evaluations, episodes, and latency survival results
and outputs structured funnel reports in Markdown, JSON, and terminal ASCII formats.

Funnel Stages (per size q and parameter set):
  Total Evaluations -> Raw Observations -> Fee-Adjusted Observations ->
  Executable Observations -> Executable Episodes ->
  Latency Survival (Satisfied / Failed / Unresolved).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
from pathlib import Path
import sys
from typing import Any, Optional

import duckdb

logger = logging.getLogger(__name__)


def generate_funnel_data(
    db_path: Path,
    param_set_id: Optional[str] = None,
) -> dict[str, Any]:
    """Query DuckDB and construct funnel and sensitivity dictionaries."""
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path}")

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        # Check required tables
        tables = {r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        required = {"param_sets", "opportunity_evaluations", "opportunity_episodes", "episode_latency_results"}
        if not required.issubset(tables):
            return {
                "error": "Required executability tables not found. Run historical replay first.",
                "tables_found": list(tables),
            }

        # Query param sets
        if param_set_id:
            param_rows = conn.execute(
                "SELECT param_set_id, params_json, code_version, created_at_utc FROM param_sets WHERE param_set_id = ?",
                [param_set_id],
            ).fetchall()
        else:
            param_rows = conn.execute(
                "SELECT param_set_id, params_json, code_version, created_at_utc FROM param_sets ORDER BY created_at_utc DESC"
            ).fetchall()

        if not param_rows:
            return {"error": "No parameter sets found in database."}

        reports: dict[str, Any] = {}

        for pid, p_json, code_ver, created_at in param_rows:
            p_config = json.loads(p_json) if p_json else {}
            sizes = p_config.get("sizes", [1, 10, 50])

            p_report: dict[str, Any] = {
                "param_set_id": pid,
                "code_version": code_ver,
                "created_at_utc": str(created_at),
                "params": p_config,
                "sizes": {},
                "reject_reasons": {},
                "latency_survival": {},
                "episode_durations": {},
            }

            # Denominators from evaluation_counts
            eval_counts = conn.execute(
                "SELECT size_q, sum(n_evaluated) FROM evaluation_counts WHERE param_set_id = ? GROUP BY size_q",
                [pid],
            ).fetchall()
            denom_by_size = {int(r[0]): int(r[1]) for r in eval_counts}

            for q in sizes:
                q_int = int(q)
                total_evals = denom_by_size.get(q_int, 0)

                # Query observations at each stage
                raw_count = conn.execute(
                    """
                    SELECT count(*) FROM opportunity_evaluations
                    WHERE param_set_id = ? AND size_q = ? AND raw_positive IS TRUE
                    """,
                    [pid, q_int],
                ).fetchone()[0]

                # If denominators table wasn't populated or total_evals < raw_count, ensure total >= raw
                total_evals = max(total_evals, raw_count)

                fee_adj_count = conn.execute(
                    """
                    SELECT count(*) FROM opportunity_evaluations
                    WHERE param_set_id = ? AND size_q = ? AND fee_adjusted IS TRUE
                    """,
                    [pid, q_int],
                ).fetchone()[0]

                exec_count = conn.execute(
                    """
                    SELECT count(*) FROM opportunity_evaluations
                    WHERE param_set_id = ? AND size_q = ? AND classification = 'EXECUTABLE_CANDIDATE'
                    """,
                    [pid, q_int],
                ).fetchone()[0]

                # Query episodes
                exec_episodes = conn.execute(
                    """
                    SELECT count(*), coalesce(avg(n_observations), 0), coalesce(avg(duration_lower_s), 0),
                           coalesce(min(duration_lower_s), 0), coalesce(max(duration_lower_s), 0)
                    FROM opportunity_episodes
                    WHERE param_set_id = ? AND size_q = ? AND level = 'executable'
                    """,
                    [pid, q_int],
                ).fetchone()

                # Edge distribution for executable candidates
                edge_stats_row = conn.execute(
                    """
                    SELECT coalesce(avg(net_edge_vwap), 0), coalesce(min(net_edge_vwap), 0), coalesce(max(net_edge_vwap), 0)
                    FROM opportunity_evaluations
                    WHERE param_set_id = ? AND size_q = ? AND classification = 'EXECUTABLE_CANDIDATE'
                    """,
                    [pid, q_int],
                ).fetchone()

                p_report["sizes"][str(q_int)] = {
                    "total_evaluations": int(total_evals),
                    "raw_observations": int(raw_count),
                    "fee_adjusted_observations": int(fee_adj_count),
                    "executable_observations": int(exec_count),
                    "executable_episodes": int(exec_episodes[0]),
                    "avg_observations_per_episode": round(float(exec_episodes[1]), 2),
                    "avg_duration_lower_s": round(float(exec_episodes[2]), 2),
                    "min_duration_lower_s": round(float(exec_episodes[3]), 2),
                    "max_duration_lower_s": round(float(exec_episodes[4]), 2),
                    "mean_net_edge_vwap": round(float(edge_stats_row[0]), 2),
                    "min_net_edge_vwap": round(float(edge_stats_row[1]), 2),
                    "max_net_edge_vwap": round(float(edge_stats_row[2]), 2),
                    "raw_conversion_pct": round((raw_count / total_evals * 100.0) if total_evals else 0.0, 2),
                    "fee_conversion_pct": round((fee_adj_count / raw_count * 100.0) if raw_count else 0.0, 2),
                    "exec_conversion_pct": round((exec_count / fee_adj_count * 100.0) if fee_adj_count else 0.0, 2),
                }

            # Reject reasons breakdown across all evaluations
            reject_rows = conn.execute(
                """
                SELECT primary_reject_reason, count(*)
                FROM opportunity_evaluations
                WHERE param_set_id = ? AND primary_reject_reason IS NOT NULL
                GROUP BY primary_reject_reason
                ORDER BY count(*) DESC
                """,
                [pid],
            ).fetchall()
            p_report["reject_reasons"] = {r[0]: int(r[1]) for r in reject_rows}

            # Censoring breakdown
            censoring_rows = conn.execute(
                """
                SELECT level,
                       count(*) as total_episodes,
                       sum(CASE WHEN censored_left THEN 1 ELSE 0 END) as censored_left,
                       sum(CASE WHEN censored_right THEN 1 ELSE 0 END) as censored_right
                FROM opportunity_episodes
                WHERE param_set_id = ?
                GROUP BY level
                """,
                [pid],
            ).fetchall()
            p_report["censoring"] = {
                r[0]: {
                    "total_episodes": int(r[1]),
                    "censored_left": int(r[2]),
                    "censored_right": int(r[3]),
                }
                for r in censoring_rows
            }

            # Latency survival breakdown (for executable episodes)
            lat_rows = conn.execute(
                """
                SELECT r.latency_s, r.status, count(*)
                FROM episode_latency_results r
                JOIN opportunity_episodes e ON r.episode_id = e.episode_id AND r.param_set_id = e.param_set_id
                WHERE r.param_set_id = ? AND e.level = 'executable'
                GROUP BY r.latency_s, r.status
                ORDER BY r.latency_s, r.status
                """,
                [pid],
            ).fetchall()
            lat_dict: dict[str, dict[str, int]] = {}
            for l_s, status, cnt in lat_rows:
                k = f"{l_s}s"
                if k not in lat_dict:
                    lat_dict[k] = {"LATENCY_SATISFIED": 0, "LATENCY_FAILED": 0, "LATENCY_UNRESOLVED": 0}
                lat_dict[k][status] = int(cnt)
            p_report["latency_survival"] = lat_dict

            reports[pid] = p_report

        return reports
    finally:
        conn.close()


def format_funnel_markdown(funnel_data: dict[str, Any]) -> str:
    """Format the funnel analysis as a GitHub-flavored Markdown report."""
    md: list[str] = []
    md.append("# 📊 Prediction Market Arbitrage Executability & Funnel Report")
    md.append(f"\n*Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}*")
    md.append("\n---\n")

    if "error" in funnel_data:
        md.append(f"> [!WARNING]\n> {funnel_data['error']}\n")
        return "\n".join(md)

    for pid, data in funnel_data.items():
        params = data.get("params", {})
        md.append(f"## Parameter Set: `{pid}`")
        md.append(
            f"- **Freshness Threshold ($\tau_{{fresh}}$)**: {params.get('tau_fresh_s', 10.0)}s\n"
            f"- **Skew Threshold ($\tau_{{skew}}$)**: {params.get('tau_skew_s', 5.0)}s\n"
            f"- **Min Edge ($E_{{min}}$)**: {params.get('min_edge_cents', 2.0)}¢\n"
            f"- **Kalshi Taker Fee**: {params.get('kalshi_taker_rate', 0.07)} variable rate (quadratic)\n"
            f"- **Polymarket Fee**: {params.get('polymarket_taker_rate', 0.0)} rate\n"
            f"- **Polygon Gas**: {params.get('polygon_gas_cents', 0.0)}¢\n"
            f"- **Slippage Allowance**: {params.get('slippage_cents_per_bundle', 0.0)}¢\n"
        )

        # 1. Funnel Table per Size
        md.append("### 1. Arbitrage Filter Funnel by Order Size")
        md.append("| Order Size | Evaluations | Raw Edge (>0) | Fee-Adjusted (>E_min) | Executable Candidate | Executable Episodes |")
        md.append("| :--- | :--- | :--- | :--- | :--- | :--- |")
        for q, sz_data in data.get("sizes", {}).items():
            tot = sz_data["total_evaluations"]
            raw = f"{sz_data['raw_observations']:,} ({sz_data['raw_conversion_pct']}%)"
            fee = f"{sz_data['fee_adjusted_observations']:,} ({sz_data['fee_conversion_pct']}%)"
            exe = f"{sz_data['executable_observations']:,} ({sz_data['exec_conversion_pct']}%)"
            eps = f"{sz_data['executable_episodes']:,}"
            md.append(f"| **{q} contracts** | {tot:,} | {raw} | {fee} | {exe} | {eps} |")
        md.append("")

        # 2. Executable Edge Distribution & Persistence
        md.append("### 2. Candidate Edge & Duration Distributions")
        md.append("| Order Size | Mean Net Edge (VWAP) | Edge Range (Min - Max) | Mean Persistence (Lower Bound) | Max Duration |")
        md.append("| :--- | :--- | :--- | :--- | :--- |")
        for q, sz_data in data.get("sizes", {}).items():
            m_edge = f"{sz_data['mean_net_edge_vwap']}¢"
            r_edge = f"{sz_data['min_net_edge_vwap']}¢ - {sz_data['max_net_edge_vwap']}¢"
            m_dur = f"{sz_data['avg_duration_lower_s']}s"
            mx_dur = f"{sz_data['max_duration_lower_s']}s"
            md.append(f"| **{q} contracts** | {m_edge} | {r_edge} | {m_dur} | mx_dur |".replace("mx_dur", mx_dur))
        md.append("")

        # 3. Latency Survival Table
        md.append("### 3. Execution Latency Survival (Executable Episodes)")
        md.append("| Latency Threshold $L$ | Satisfied (>= L) | Failed (< L) | Unresolved | Total |")
        md.append("| :--- | :--- | :--- | :--- | :--- |")
        for lat, counts in data.get("latency_survival", {}).items():
            sat = counts.get("LATENCY_SATISFIED", 0)
            fld = counts.get("LATENCY_FAILED", 0)
            unr = counts.get("LATENCY_UNRESOLVED", 0)
            tot_lat = sat + fld + unr
            sat_pct = f"{(sat / tot_lat * 100.0):.1f}%" if tot_lat else "0%"
            md.append(f"| **{lat}** | {sat} ({sat_pct}) | {fld} | {unr} | {tot_lat} |")
        md.append("")

        # 4. Rejection Reasons Breakdown
        md.append("### 4. Rejection Reasons Breakdown")
        md.append("| Primary Rejection Reason | Count |")
        md.append("| :--- | :--- |")
        for r_code, cnt in data.get("reject_reasons", {}).items():
            md.append(f"| `{r_code}` | {cnt:,} |")
        md.append("")

        # 5. Censoring Summary
        md.append("### 5. Episode Censoring Breakdown")
        md.append("| Level | Total Episodes | Censored Left (Open at Start) | Censored Right (Open at End / Data Gap) |")
        md.append("| :--- | :--- | :--- | :--- |")
        for lvl, c_info in data.get("censoring", {}).items():
            md.append(f"| **{lvl}** | {c_info['total_episodes']} | {c_info['censored_left']} | {c_info['censored_right']} |")
        md.append("")

    # Data Limitations Section
    md.append("## ⚠️ Research Data Limitations & Assumptions")
    md.append(
        "1. **Discrete Sampling Resolution**: Observations occur at periodic snapshot intervals "
        "(e.g. 60s). Sub-interval execution latencies ($L < 60$s) cannot be confirmed by single-point "
        "observations and are classified honestly as `LATENCY_UNRESOLVED`.\n"
        "2. **Orderbook Truncation**: Depth is stored up to $N=10$ levels per side. Any order exceeding "
        "the cumulative contracts available in the top 10 levels is rejected conservatively with "
        "`INSUFFICIENT_DEPTH` rather than extrapolated.\n"
        "3. **Data Quality & Timestamp Quality**: Unverified pairs, markets flagged as paused/closed, "
        "and quotes with missing or future exchange timestamps are rejected from research candidate pools.\n"
        "4. **Settlement Capital Lockup**: This filter models transaction costs, exchange taker fees, gas, "
        "slippage, and instantaneous fillability. It does **not** model the opportunity cost or financing rate "
        "of capital locked up in contracts until market resolution.\n"
    )

    return "\n".join(md)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate opportunity funnel and executability report.")
    parser.add_argument("--db", default="data/market_snapshots.duckdb", help="Path to snapshots DuckDB file")
    parser.add_argument("--param-set-id", default=None, help="Filter to specific parameter set ID")
    parser.add_argument("--output-dir", default="reports", help="Directory to save report artifacts")
    parser.add_argument("--save-md", action="store_true", default=True, help="Save markdown report")
    parser.add_argument("--save-json", action="store_true", default=True, help="Save json report")

    args = parser.parse_args()
    db_path = Path(args.db)

    try:
        data = generate_funnel_data(db_path, param_set_id=args.param_set_id)
    except Exception as e:
        print(f"Error reading database: {e}", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    md_content = format_funnel_markdown(data)
    print(md_content)

    if args.save_md:
        md_file = out_dir / "opportunity_funnel_report.md"
        md_file.write_text(md_content, encoding="utf-8")
        print(f"\nSaved Markdown report to: {md_file}")

    if args.save_json:
        json_file = out_dir / "opportunity_funnel_report.json"
        json_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
        print(f"Saved JSON report to: {json_file}")


if __name__ == "__main__":
    main()
