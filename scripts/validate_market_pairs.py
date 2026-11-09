#!/usr/bin/env python3
"""Validate market pair overrides against live exchange APIs and schema constraints.

Usage:
    python scripts/validate_market_pairs.py
    python scripts/validate_market_pairs.py --strict
    python scripts/validate_market_pairs.py --overrides-path config/market_overrides.yaml --suggest-exclusions
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sys
from typing import Any, Optional

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
import duckdb
import yaml

from core.config_loader import load_config
from core.pair_registry import (
    DEFAULT_OVERRIDES_PATH,
    MarketPairOverride,
    PairIssue,
    RegistryThresholds,
    parse_pair_registry,
)
from core.pair_validation import (
    EXCLUDED,
    PASS,
    WARN,
    PairCheckResult,
    ReasonCode,
    check_pairs_live,
)
from core.snapshot_store import connect, ensure_schema, log_exclusions, sync_verified_pairs
from kalshi_client.rest import KalshiClient
from polymarket_client.rest import PolymarketClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("validate_market_pairs")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate market pair overrides against schema rules and live platform APIs."
    )
    parser.add_argument(
        "--overrides-path",
        type=str,
        default=DEFAULT_OVERRIDES_PATH,
        help="Path to market overrides YAML file (default: config/market_overrides.yaml)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        default=False,
        help="Exit with non-zero status if any verified pair fails live check or registry bounds are violated",
    )
    parser.add_argument(
        "--duckdb-path",
        type=str,
        default="data/snapshots.duckdb",
        help="Path to DuckDB database to record exclusions and sync verified pairs",
    )
    parser.add_argument(
        "--close-mismatch-hours",
        type=float,
        default=24.0,
        help="Threshold in hours for close time mismatch warning (default: 24.0)",
    )
    parser.add_argument(
        "--suggest-exclusions",
        action="store_true",
        default=False,
        help="Print suggested YAML exclusion blocks for failing pairs",
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        default=False,
        help="Use Kalshi production API instead of sandbox",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="data/pair_validation_report.json",
        help="Path for JSON validation report output",
    )
    parser.add_argument(
        "--output-md",
        type=str,
        default="reports/pair_validation_report.md",
        help="Path for Markdown validation report output",
    )
    return parser


def format_table(rows: list[list[str]], headers: list[str]) -> str:
    """Format an ASCII table for terminal output."""
    if not rows:
        return "No entries to display."
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    fmt = " | ".join(f"{{:<{w}}}" for w in widths)
    separator = "-+-".join("-" * w for w in widths)
    lines = [fmt.format(*headers), separator]
    for row in rows:
        lines.append(fmt.format(*row))
    return "\n".join(lines)


def generate_markdown_report(
    summary: dict[str, Any],
    results: list[PairCheckResult],
    issues: list[PairIssue],
    warnings: list[str],
) -> str:
    """Generate human-readable markdown report of validation results."""
    md = [
        "# Market Pair Validation Report",
        "",
        f"- **Generated At (UTC):** {summary['generated_at_utc']}",
        f"- **Overrides File:** `{summary['overrides_path']}`",
        f"- **Strict Mode:** `{summary['strict_mode']}`",
        "",
        "## Summary Metrics",
        "",
        "| Metric | Count |",
        "| :--- | :--- |",
        f"| Total Overrides Evaluated | {summary['total_evaluated']} |",
        f"| Verified Pairs Configured | {summary['verified_configured']} |",
        f"| Pairs Passed (Tradable) | {summary['passed_count']} |",
        f"| Pairs Warned | {summary['warned_count']} |",
        f"| Pairs Excluded | {summary['excluded_count']} |",
        f"| Schema / Cross-Entry Issues | {summary['schema_issues_count']} |",
        "",
    ]

    if warnings:
        md.extend(["### Registry Warnings", ""])
        for w in warnings:
            md.append(f"- ⚠️ {w}")
        md.append("")

    if issues:
        md.extend(["### Schema Issues", ""])
        for iss in issues:
            md.append(f"- ❌ `[{iss.pair_id}]`: {iss.message}")
        md.append("")

    md.extend([
        "## Pair Check Details",
        "",
        "| Pair ID | Category | Outcome | Reason Code | Details |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ])
    for r in results:
        code = r.reason_label or "-"
        detail = r.detail.replace("|", "/") if r.detail else "-"
        md.append(f"| `{r.pair_id}` | {r.category} | **{r.outcome}** | `{code}` | {detail} |")
    md.append("")
    return "\n".join(md)


async def run_validation(
    overrides_path: str,
    strict: bool,
    duckdb_path: str,
    close_mismatch_hours: float,
    suggest_exclusions: bool,
    is_prod: bool,
    output_json: str,
    output_md: str,
    kalshi_client: Optional[Any] = None,
    polymarket_client: Optional[Any] = None,
) -> int:
    load_dotenv()
    config = load_config()
    thresholds = RegistryThresholds.from_config(config)

    now_utc = datetime.now(timezone.utc)
    logger.info("Parsing pair registry from %s...", overrides_path)
    reg_result = parse_pair_registry(overrides_path, thresholds=thresholds)

    all_results: list[PairCheckResult] = []

    # Map any schema issues to PairCheckResult with SCHEMA_INVALID
    for issue in reg_result.issues:
        all_results.append(
            PairCheckResult(
                pair_id=issue.pair_id,
                category="unknown",
                outcome=EXCLUDED,
                reason_code=ReasonCode.SCHEMA_INVALID,
                detail=issue.message,
                checked_at=now_utc,
            )
        )

    # Initialize API clients if not injected
    k_client = kalshi_client
    p_client = polymarket_client
    own_clients = False

    if k_client is None or p_client is None:
        own_clients = True
        is_sandbox = not is_prod
        k_client = KalshiClient.from_config(config, is_sandbox=is_sandbox)
        p_client = PolymarketClient.from_config(config)

    try:
        if own_clients:
            await k_client.__aenter__()
            await p_client.__aenter__()

        # Run live checks across all pairs (excluded pairs return immediately as EXCLUDED)
        check_results = await check_pairs_live(
            reg_result.pairs,
            k_client,
            p_client,
            now=now_utc,
            close_mismatch_hours=close_mismatch_hours,
        )
        all_results.extend(check_results)
    finally:
        if own_clients:
            await k_client.__aexit__(None, None, None)
            await p_client.__aexit__(None, None, None)

    # Log exclusions and sync verified pairs to DuckDB if accessible
    if duckdb_path:
        try:
            conn = connect(duckdb_path)
            ensure_schema(conn)

            # Log exclusions
            exclusion_rows = [
                (r.pair_id, r.reason_label or "UNKNOWN", r.detail)
                for r in all_results
                if r.outcome != PASS
            ]
            if exclusion_rows:
                log_exclusions(conn, exclusion_rows, checked_at=now_utc)

            # Sync verified pairs that are currently verified in registry
            sync_verified_pairs(conn, reg_result.verified, now=now_utc)
            conn.close()
            logger.info("Updated DuckDB exclusions and verified pairs in %s", duckdb_path)
        except Exception as e:
            logger.warning("Could not record results to DuckDB (%s): %s", duckdb_path, e)

    # Calculate summary counts
    passed_count = sum(1 for r in all_results if r.outcome == PASS)
    warned_count = sum(1 for r in all_results if r.outcome == WARN)
    excluded_count = sum(1 for r in all_results if r.outcome == EXCLUDED)
    schema_issues_count = len(reg_result.issues)

    summary = {
        "generated_at_utc": now_utc.isoformat(),
        "overrides_path": overrides_path,
        "strict_mode": strict,
        "total_evaluated": len(all_results),
        "verified_configured": len(reg_result.verified),
        "passed_count": passed_count,
        "warned_count": warned_count,
        "excluded_count": excluded_count,
        "schema_issues_count": schema_issues_count,
        "warnings": reg_result.warnings,
        "results": [r.to_dict() for r in all_results],
    }

    # Print console summary
    table_rows = [
        [
            r.pair_id,
            r.category,
            r.outcome,
            r.reason_label or "-",
            (r.detail[:60] + "...") if len(r.detail) > 60 else (r.detail or "-"),
        ]
        for r in all_results
    ]
    headers = ["Pair ID", "Category", "Outcome", "Reason", "Details"]
    print("\n" + "=" * 90)
    print("  MARKET PAIR VALIDATION REPORT")
    print("=" * 90)
    print(format_table(table_rows, headers))
    print("-" * 90)
    print(
        f"Evaluated: {len(all_results)} | Verified: {len(reg_result.verified)} | "
        f"Passed: {passed_count} | Warned: {warned_count} | Excluded: {excluded_count} | "
        f"Schema Issues: {schema_issues_count}"
    )
    if reg_result.warnings:
        print("\nWarnings:")
        for w in reg_result.warnings:
            print(f"  [WARN] {w}")
    print("=" * 90 + "\n")

    # Suggest exclusions if requested
    if suggest_exclusions:
        failing = [r for r in all_results if r.outcome != PASS]
        if failing:
            print("# Suggested exclusions for failing / unverified pairs:")
            yaml_entries = []
            for r in failing:
                yaml_entries.append(
                    {
                        "pair_id": r.pair_id,
                        "category": r.category,
                        "status": "excluded",
                        "exclusion_reason": r.detail or r.reason_label or "tradability check failed",
                    }
                )
            print(yaml.dump({"pairs": yaml_entries}, sort_keys=False))
            print("-" * 90)

    # Save reports
    if output_json:
        out_j = Path(output_json)
        out_j.parent.mkdir(parents=True, exist_ok=True)
        with open(out_j, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        logger.info("Saved JSON report to %s", output_json)

    if output_md:
        out_m = Path(output_md)
        out_m.parent.mkdir(parents=True, exist_ok=True)
        md_text = generate_markdown_report(summary, all_results, reg_result.issues, reg_result.warnings)
        with open(out_m, "w", encoding="utf-8") as f:
            f.write(md_text)
        logger.info("Saved Markdown report to %s", output_md)

    # Strict mode exit condition
    if strict:
        # Fails if any schema issues, bound warnings, or if any pair is not PASS
        has_verified_failures = any(r.outcome != PASS for r in check_results if getattr(r, "passed", False) is False)
        if schema_issues_count > 0 or len(reg_result.warnings) > 0 or has_verified_failures:
            logger.error("Strict validation failed (schema issues, bounds violations, or pair failures)")
            return 1

    return 0


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    code = asyncio.run(
        run_validation(
            overrides_path=args.overrides_path,
            strict=args.strict,
            duckdb_path=args.duckdb_path,
            close_mismatch_hours=args.close_mismatch_hours,
            suggest_exclusions=args.suggest_exclusions,
            is_prod=args.prod,
            output_json=args.output_json,
            output_md=args.output_md,
        )
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
