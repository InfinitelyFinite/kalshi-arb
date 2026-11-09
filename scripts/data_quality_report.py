#!/usr/bin/env python3
"""Data quality reporting tool for prediction market snapshot datasets in DuckDB.

Evaluates coverage, freshness, order book depth, fillability via VWAP, quote skew,
and data integrity across historical observations, assigning reliability labels.

Usage:
    python scripts/data_quality_report.py
    python scripts/data_quality_report.py --duckdb-path data/snapshots.duckdb --from 2026-08-01
    python scripts/data_quality_report.py --output-dir reports
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
import math
from pathlib import Path
import sys
from typing import Any, Optional, Sequence

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

from core.config_loader import AppConfig, load_config
from core.depth import Level, vwap_for_size
from core.snapshot_store import connect, ensure_schema, view_exists

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("data_quality_report")

LABEL_USABLE = "usable"
LABEL_USABLE_WITH_CAVEATS = "usable_with_caveats"
LABEL_UNRELIABLE = "unreliable"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate data quality and reliability report from DuckDB market snapshots."
    )
    parser.add_argument(
        "--duckdb-path",
        "--db",
        dest="duckdb_path",
        type=str,
        default="data/snapshots.duckdb",
        help="Path to DuckDB database file (default: data/snapshots.duckdb)",
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
        "--output-dir",
        "--out",
        dest="output_dir",
        type=str,
        default="reports",
        help="Directory to save output reports (default: reports)",
    )
    parser.add_argument(
        "--snapshot-interval",
        type=float,
        default=None,
        help="Expected seconds between snapshots (default from config or 60s)",
    )
    parser.add_argument(
        "--gap-threshold",
        type=float,
        default=None,
        help="Gap threshold in seconds (default from config or 300s)",
    )
    parser.add_argument(
        "--stale-threshold",
        type=float,
        default=None,
        help="Stale quote age threshold in seconds (default from config or 60s)",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        default=False,
        help="Analyze raw orderbook_snapshots table instead of research_snapshots view",
    )
    return parser


def parse_dt(val: Optional[str], is_end: bool = False) -> Optional[datetime]:
    if not val:
        return None
    val_str = str(val).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(val_str, fmt)
            if is_end:
                return dt.replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc)
            return dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(val_str.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def percentile(data: Sequence[float], p: float) -> Optional[float]:
    if not data:
        return None
    s = sorted(data)
    k = (len(s) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(s[int(k)])
    d0 = s[int(f)] * (c - k)
    d1 = s[int(c)] * (k - f)
    return float(d0 + d1)


class DataQualityAnalyzer:
    """Computes quality metrics on market snapshots from DuckDB."""

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        config: AppConfig,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        snapshot_interval: Optional[float] = None,
        gap_threshold: Optional[float] = None,
        stale_threshold: Optional[float] = None,
        use_raw_table: bool = False,
    ) -> None:
        self.conn = conn
        self.config = config
        self.from_dt = from_dt
        self.to_dt = to_dt
        self.snapshot_interval = snapshot_interval or config.data_quality.snapshot_interval_seconds
        self.gap_threshold = gap_threshold or config.data_quality.gap_threshold_seconds
        self.stale_threshold = stale_threshold or config.data_quality.stale_quote_seconds
        self.vwap_sizes = config.depth.vwap_sizes

        # Choose target table or view
        if use_raw_table:
            self.table_name = "orderbook_snapshots"
        elif view_exists(conn, "research_snapshots"):
            self.table_name = "research_snapshots"
        else:
            self.table_name = "orderbook_snapshots"

    def analyze(self) -> dict[str, Any]:
        try:
            ensure_schema(self.conn)
        except Exception as e:
            logger.debug("Could not run ensure_schema (likely read-only database): %s", e)

        # 1. Total records overview (raw vs research)
        raw_total = self.conn.execute("SELECT count(*) FROM orderbook_snapshots").fetchone()[0]
        raw_legacy = 0
        try:
            raw_legacy = self.conn.execute(
                "SELECT count(*) FROM orderbook_snapshots WHERE data_source = 'legacy_unverified'"
            ).fetchone()[0]
        except Exception:
            pass

        # Apply date filters
        where_clauses: list[str] = ["1=1"]
        params: list[Any] = []
        if self.from_dt:
            where_clauses.append("observed_at_utc >= ?")
            params.append(self.from_dt.replace(tzinfo=None))
        if self.to_dt:
            where_clauses.append("observed_at_utc <= ?")
            params.append(self.to_dt.replace(tzinfo=None))
        where_sql = " AND ".join(where_clauses)

        query = f"SELECT * FROM {self.table_name} WHERE {where_sql} ORDER BY observed_at_utc ASC"
        cursor = self.conn.execute(query, params)
        cols = [desc[0] for desc in cursor.description]
        raw_rows = cursor.fetchall()
        rows = [dict(zip(cols, r)) for r in raw_rows]

        total_snapshots = len(rows)
        if total_snapshots == 0:
            return self._empty_analysis(raw_total, raw_legacy)

        # Distinct platforms and pairs/tickers
        platforms = sorted({r["platform"] for r in rows if r.get("platform")})
        tickers = sorted({r["ticker"] for r in rows if r.get("ticker")})
        pair_ids = sorted({r["pair_id"] for r in rows if r.get("pair_id")})

        # 2. Time coverage & Gaps
        timestamps = [r["observed_at_utc"] for r in rows if r.get("observed_at_utc")]
        min_ts, max_ts = min(timestamps), max(timestamps)
        span_seconds = max(0.0, (max_ts - min_ts).total_seconds())
        expected_snapshots = max(1, int(round(span_seconds / self.snapshot_interval))) + 1
        coverage_pct = round(min(100.0, (total_snapshots / expected_snapshots) * 100.0), 2)

        # Find gaps > gap_threshold between consecutive distinct observation timestamps
        unique_ts = sorted(set(timestamps))
        gaps: list[dict[str, Any]] = []
        for i in range(1, len(unique_ts)):
            gap_dur = (unique_ts[i] - unique_ts[i - 1]).total_seconds()
            if gap_dur > self.gap_threshold:
                gaps.append(
                    {
                        "start_utc": unique_ts[i - 1].isoformat(),
                        "end_utc": unique_ts[i].isoformat(),
                        "duration_seconds": round(gap_dur, 1),
                    }
                )

        # 3. Exchange updated at & Quote Staleness
        null_exchange_ts_count = sum(1 for r in rows if r.get("exchange_updated_at_utc") is None)
        null_exchange_ts_pct = round((null_exchange_ts_count / total_snapshots) * 100.0, 2)

        stale_count = 0
        quote_ages: list[float] = []
        for r in rows:
            obs = r.get("observed_at_utc")
            exch = r.get("exchange_updated_at_utc")
            if obs is not None and exch is not None:
                obs_dt = obs if isinstance(obs, datetime) else datetime.fromisoformat(str(obs))
                exch_dt = exch if isinstance(exch, datetime) else datetime.fromisoformat(str(exch))
                age = (obs_dt - exch_dt).total_seconds()
                quote_ages.append(age)
                if age > self.stale_threshold:
                    stale_count += 1

        stale_quote_pct = round((stale_count / total_snapshots) * 100.0, 2)
        quote_age_median = percentile(quote_ages, 50) if quote_ages else None
        quote_age_p95 = percentile(quote_ages, 95) if quote_ages else None

        # 4. Depth & Fillability (VWAP)
        empty_books_count = 0
        one_sided_count = 0
        crossed_books_count = 0

        # Query levels if available
        levels_by_snap: dict[int, dict[str, list[Level]]] = {}
        try:
            snap_ids = [r["snapshot_id"] for r in rows if r.get("snapshot_id") is not None]
            if snap_ids:
                # Query top levels for these snapshots
                lvl_df = self.conn.execute(
                    """
                    SELECT snapshot_id, side, price, size
                    FROM orderbook_levels
                    WHERE snapshot_id IN (SELECT unnest(?))
                    ORDER BY snapshot_id, side, level_idx ASC
                    """,
                    [snap_ids],
                ).fetchall()
                for sid, side, price, size in lvl_df:
                    if sid not in levels_by_snap:
                        levels_by_snap[sid] = {}
                    if side not in levels_by_snap[sid]:
                        levels_by_snap[sid][side] = []
                    levels_by_snap[sid][side].append((Decimal(str(price)), Decimal(str(size))))
        except Exception as e:
            logger.debug("Could not query orderbook_levels: %s", e)

        fillable_counts = {sz: 0 for sz in self.vwap_sizes}

        for row in rows:
            sid = row.get("snapshot_id")
            # Ask prices & bid prices
            ask_yes = row.get("ask_yes") or row.get("yes_ask")
            ask_no = row.get("ask_no") or row.get("no_ask")
            bid_yes = row.get("bid_yes") or row.get("yes_bid")
            bid_no = row.get("bid_no") or row.get("no_bid")

            has_yes_ask = ask_yes is not None
            has_no_ask = ask_no is not None
            has_yes_bid = bid_yes is not None
            has_no_bid = bid_no is not None

            if not has_yes_ask and not has_no_ask:
                empty_books_count += 1
            elif (has_yes_ask != has_no_ask) or (has_yes_bid != has_no_bid):
                one_sided_count += 1

            # Check crossed book
            if has_yes_ask and has_yes_bid and float(bid_yes) > float(ask_yes):
                crossed_books_count += 1
            elif has_no_ask and has_no_bid and float(bid_no) > float(ask_no):
                crossed_books_count += 1

            # Test fillability using ladders or best sizes
            snap_lvls = levels_by_snap.get(sid, {}) if sid else {}
            yes_ask_levels = snap_lvls.get("yes_ask", [])
            no_ask_levels = snap_lvls.get("no_ask", [])

            # Fallback to best size if levels not in child table
            if not yes_ask_levels and has_yes_ask:
                size_yes = row.get("ask_yes_size")
                if size_yes is not None:
                    yes_ask_levels = [(Decimal(str(ask_yes)), Decimal(str(size_yes)))]
            if not no_ask_levels and has_no_ask:
                size_no = row.get("ask_no_size")
                if size_no is not None:
                    no_ask_levels = [(Decimal(str(ask_no)), Decimal(str(size_no)))]

            for sz in self.vwap_sizes:
                qty = Decimal(sz)
                # Market is fillable at sz if at least one ask side can fill qty (or both if present)
                can_fill_yes = False
                can_fill_no = False
                if yes_ask_levels:
                    vwap_val, filled, fully_filled = vwap_for_size(yes_ask_levels, qty)
                    can_fill_yes = fully_filled
                if no_ask_levels:
                    vwap_val, filled, fully_filled = vwap_for_size(no_ask_levels, qty)
                    can_fill_no = fully_filled

                if can_fill_yes or can_fill_no:
                    fillable_counts[sz] += 1

        fillability_pct = {
            f"size_{sz}_pct": round((fillable_counts[sz] / total_snapshots) * 100.0, 2)
            for sz in self.vwap_sizes
        }

        # 5. Cross-platform quote skew
        skew_metrics: dict[str, Any] = {}
        try:
            if view_exists(self.conn, "research_pair_snapshots"):
                skew_rows = self.conn.execute("SELECT observed_skew_ms, quote_skew_ms FROM research_pair_snapshots").fetchall()
                obs_skews = [r[0] for r in skew_rows if r[0] is not None]
                quote_skews = [r[1] for r in skew_rows if r[1] is not None]
                skew_metrics = {
                    "pairs_evaluated": len(skew_rows),
                    "observed_skew_median_ms": percentile(obs_skews, 50) if obs_skews else None,
                    "observed_skew_p95_ms": percentile(obs_skews, 95) if obs_skews else None,
                    "quote_skew_median_ms": percentile(quote_skews, 50) if quote_skews else None,
                    "quote_skew_p95_ms": percentile(quote_skews, 95) if quote_skews else None,
                }
        except Exception as e:
            logger.debug("Quote skew calculation skipped: %s", e)

        # 6. Overall Reliability Assessment
        overall_label = self._classify_reliability(
            coverage_pct=coverage_pct,
            stale_pct=stale_quote_pct,
            fillability_10=fillability_pct.get("size_10_pct", 0.0),
            crossed_count=crossed_books_count,
            gaps_count=len(gaps),
        )

        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "target_dataset": self.table_name,
            "overall_reliability": overall_label,
            "time_window": {
                "start_utc": min_ts.isoformat(),
                "end_utc": max_ts.isoformat(),
                "span_hours": round(span_seconds / 3600.0, 2),
            },
            "snapshot_counts": {
                "total_analyzed": total_snapshots,
                "expected_snapshots": expected_snapshots,
                "coverage_pct": coverage_pct,
                "raw_total_in_db": raw_total,
                "raw_legacy_unverified": raw_legacy,
            },
            "gaps": {
                "gap_threshold_seconds": self.gap_threshold,
                "count": len(gaps),
                "incidents": gaps,
            },
            "quote_freshness": {
                "missing_exchange_ts_count": null_exchange_ts_count,
                "missing_exchange_ts_pct": null_exchange_ts_pct,
                "stale_quote_count": stale_count,
                "stale_quote_pct": stale_quote_pct,
                "quote_age_median_sec": quote_age_median,
                "quote_age_p95_sec": quote_age_p95,
            },
            "liquidity_and_depth": {
                "empty_books_count": empty_books_count,
                "empty_books_pct": round((empty_books_count / total_snapshots) * 100.0, 2),
                "one_sided_count": one_sided_count,
                "one_sided_pct": round((one_sided_count / total_snapshots) * 100.0, 2),
                "crossed_books_count": crossed_books_count,
                "crossed_books_pct": round((crossed_books_count / total_snapshots) * 100.0, 2),
                "fillability": fillability_pct,
            },
            "cross_platform_skew": skew_metrics,
            "entities": {
                "platforms": platforms,
                "tickers_count": len(tickers),
                "pairs_count": len(pair_ids),
                "pairs": pair_ids,
            },
        }

    def _classify_reliability(
        self,
        coverage_pct: float,
        stale_pct: float,
        fillability_10: float,
        crossed_count: int,
        gaps_count: int,
    ) -> str:
        """Classify dataset into usable, usable_with_caveats, or unreliable."""
        if crossed_count > 0 or coverage_pct < 50.0 or fillability_10 < 50.0:
            return LABEL_UNRELIABLE
        if gaps_count == 0 and coverage_pct >= 95.0 and stale_pct <= 5.0 and fillability_10 >= 95.0:
            return LABEL_USABLE
        return LABEL_USABLE_WITH_CAVEATS

    def _empty_analysis(self, raw_total: int, raw_legacy: int) -> dict[str, Any]:
        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "target_dataset": self.table_name,
            "overall_reliability": LABEL_UNRELIABLE,
            "note": (
                "No snapshots found in target dataset matching filters. "
                "In research mode, only observations with data_source='live_api' "
                "for verified pairs are included."
            ),
            "snapshot_counts": {
                "total_analyzed": 0,
                "raw_total_in_db": raw_total,
                "raw_legacy_unverified": raw_legacy,
            },
        }


def format_markdown_report(data: dict[str, Any]) -> str:
    """Format dictionary metrics into a clean Github-flavored Markdown report."""
    md = [
        "# Data Quality & Reliability Report",
        "",
        f"- **Generated At (UTC):** `{data['generated_at_utc']}`",
        f"- **Target Dataset:** `{data['target_dataset']}`",
        f"- **Overall Reliability Label:** **`{data['overall_reliability'].upper()}`**",
        "",
    ]

    if "note" in data:
        md.extend([
            f"> [!IMPORTANT]\n> {data['note']}",
            "",
            "## Database Snapshot Overview",
            f"- Total raw snapshots stored in DuckDB: `{data['snapshot_counts']['raw_total_in_db']}`",
            f"- Legacy unverified snapshots: `{data['snapshot_counts']['raw_legacy_unverified']}`",
            "",
        ])
        return "\n".join(md)

    counts = data["snapshot_counts"]
    tw = data["time_window"]
    fresh = data["quote_freshness"]
    depth = data["liquidity_and_depth"]
    gaps = data["gaps"]

    md.extend([
        "## Executive Summary",
        "",
        "| Metric | Value |",
        "| :--- | :--- |",
        f"| Overall Assessment | **`{data['overall_reliability']}`** |",
        f"| Time Window | `{tw['start_utc']}` to `{tw['end_utc']}` ({tw['span_hours']}h) |",
        f"| Total Snapshots Analyzed | `{counts['total_analyzed']}` |",
        f"| Coverage Completeness | `{counts['coverage_pct']}%` (expected ~{counts['expected_snapshots']}) |",
        f"| Significant Gaps (> {gaps['gap_threshold_seconds']}s) | `{gaps['count']}` |",
        f"| Missing Exchange Timestamps | `{fresh['missing_exchange_ts_pct']}%` ({fresh['missing_exchange_ts_count']}) |",
        f"| Stale Quotes (> 60s) | `{fresh['stale_quote_pct']}%` ({fresh['stale_quote_count']}) |",
        f"| Crossed Books (bid > ask) | `{depth['crossed_books_count']}` |",
        "",
        "## Order Book Depth & VWAP Fillability",
        "",
        "| Contract Order Size | Fillable Snapshots % |",
        "| :--- | :--- |",
    ])

    for k, v in depth["fillability"].items():
        md.append(f"| {k.replace('_', ' ').title()} | `{v}%` |")

    md.extend([
        "",
        f"- **Empty Books:** `{depth['empty_books_pct']}%` ({depth['empty_books_count']})",
        f"- **One-Sided Books:** `{depth['one_sided_pct']}%` ({depth['one_sided_count']})",
        "",
    ])

    skew = data.get("cross_platform_skew", {})
    if skew and skew.get("pairs_evaluated", 0) > 0:
        md.extend([
            "## Cross-Platform Quote Skew",
            "",
            f"- Paired Snapshots Evaluated: `{skew.get('pairs_evaluated')}`",
            f"- Local Observation Skew (Median / P95): `{skew.get('observed_skew_median_ms')} ms` / `{skew.get('observed_skew_p95_ms')} ms`",
            f"- Exchange Quote Skew (Median / P95): `{skew.get('quote_skew_median_ms')} ms` / `{skew.get('quote_skew_p95_ms')} ms`",
            "",
        ])

    if gaps["count"] > 0:
        md.extend([
            "## Data Gaps Detected",
            "",
            "| Gap Start (UTC) | Gap End (UTC) | Duration |",
            "| :--- | :--- | :--- |",
        ])
        for g in gaps["incidents"][:15]:
            md.append(f"| `{g['start_utc']}` | `{g['end_utc']}` | `{g['duration_seconds']}s` |")
        if gaps["count"] > 15:
            md.append(f"| ... and {gaps['count'] - 15} more incidents | | |")
        md.append("")

    return "\n".join(md)


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    config = load_config()
    from_dt = parse_dt(args.from_date)
    to_dt = parse_dt(args.to_date, is_end=True)

    db_path = Path(args.duckdb_path)
    if not db_path.exists():
        logger.error("DuckDB database file not found at: %s", db_path)
        sys.exit(1)

    try:
        conn = connect(str(db_path), read_only=False)
    except Exception:
        conn = connect(str(db_path), read_only=True)
    analyzer = DataQualityAnalyzer(
        conn=conn,
        config=config,
        from_dt=from_dt,
        to_dt=to_dt,
        snapshot_interval=args.snapshot_interval,
        gap_threshold=args.gap_threshold,
        stale_threshold=args.stale_threshold,
        use_raw_table=args.raw,
    )

    logger.info("Computing data quality metrics on %s...", args.duckdb_path)
    report_data = analyzer.analyze()
    conn.close()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    json_path = out_dir / f"data_quality_{today_str}.json"
    md_path = out_dir / f"data_quality_{today_str}.md"
    latest_md_path = out_dir / "data_quality_report.md"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2)

    md_content = format_markdown_report(report_data)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_content)
    with open(latest_md_path, "w", encoding="utf-8") as f:
        f.write(md_content)

    print("\n" + "=" * 80)
    print("  DATA QUALITY & RELIABILITY REPORT")
    print("=" * 80)
    print(f"Target:       {report_data['target_dataset']}")
    print(f"Reliability:  {report_data['overall_reliability'].upper()}")
    if "snapshot_counts" in report_data and report_data["snapshot_counts"].get("total_analyzed", 0) > 0:
        counts = report_data["snapshot_counts"]
        print(f"Snapshots:    {counts['total_analyzed']} (coverage {counts['coverage_pct']}%)")
        print(f"Gaps:         {report_data['gaps']['count']} incidents > {report_data['gaps']['gap_threshold_seconds']}s")
        print(f"Fillability:  Size 10 = {report_data['liquidity_and_depth']['fillability'].get('size_10_pct', 0)}%")
    else:
        print(f"Note:         {report_data.get('note', 'No snapshots found')}")
    print("=" * 80)
    print(f"Saved: {md_path}")
    print(f"Saved: {json_path}\n")


if __name__ == "__main__":
    main()
