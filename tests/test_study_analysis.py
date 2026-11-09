"""Unit and integration tests for sustained study collection and empirical replay analysis."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import pytest
import duckdb

from core.config_loader import load_config
from core.depth import Level
from core.pair_registry import MarketPairOverride
from core.snapshot_store import (
    SnapshotRecord,
    connect,
    ensure_schema,
    sync_verified_pairs,
    write_snapshots,
)
from scripts.collect_study import (
    SustainedStudyCollector,
    build_arg_parser,
    ensure_sessions_schema,
    find_resumable_session,
    register_session,
    update_session_progress,
)
from scripts.analyze_study import (
    StudyAnalyzer,
    StudyEpisode,
    export_study_artifacts,
    generate_markdown_report,
)


def test_collect_study_cli_args() -> None:
    """Verify CLI parser options and defaults for scripts/collect_study.py."""
    parser = build_arg_parser()

    # Default options
    args = parser.parse_args([])
    assert args.duration_hours == 24.0
    assert args.snapshot_interval == 5.0
    assert args.overrides_only is True
    assert args.duckdb_path == "data/snapshots.duckdb"
    assert args.resume is False

    # Custom options
    args_custom = parser.parse_args([
        "--duration-hours", "168",
        "--snapshot-interval", "5",
        "--overrides-only",
        "--db", "data/custom.duckdb",
        "--resume",
    ])
    assert args_custom.duration_hours == 168.0
    assert args_custom.snapshot_interval == 5.0
    assert args_custom.overrides_only is True
    assert args_custom.duckdb_path == "data/custom.duckdb"
    assert args_custom.resume is True


def test_collect_study_session_metadata(tmp_path: Path) -> None:
    """Test session metadata recording, progress updating, and resumption lookup."""
    db_file = tmp_path / "sessions_test.duckdb"
    conn = duckdb.connect(str(db_file))
    ensure_sessions_schema(conn)

    session_id = "test-session-123"
    register_session(
        conn=conn,
        session_id=session_id,
        target_hours=24.0,
        interval_s=5.0,
        overrides_path="config/market_overrides.yaml",
        pairs_count=20,
    )

    # Check initially recorded session
    resumable = find_resumable_session(conn)
    assert resumable is not None
    assert resumable["session_id"] == session_id
    assert resumable["status"] == "RUNNING"
    assert resumable["total_batches"] == 0

    # Update progress
    update_session_progress(
        conn=conn,
        session_id=session_id,
        batches=100,
        snapshots=2000,
        levels=20000,
        errors=1,
        status="RUNNING",
    )

    updated = find_resumable_session(conn)
    assert updated is not None
    assert updated["total_batches"] == 100
    assert updated["total_snapshots"] == 2000
    assert updated["total_levels"] == 20000
    assert updated["error_count"] == 1

    # Mark completed
    update_session_progress(
        conn=conn,
        session_id=session_id,
        batches=200,
        snapshots=4000,
        levels=40000,
        errors=1,
        status="COMPLETED",
    )

    # Completed session should no longer be resumable
    assert find_resumable_session(conn) is None
    conn.close()


def test_collect_study_snapshot_building() -> None:
    """Verify transformation of exchange books into SnapshotRecords."""
    config = load_config()
    pair = MarketPairOverride(
        pair_id="test-pair",
        category="politics",
        status="excluded",
        exclusion_reason="mock",
        kalshi_ticker="KXTEST-1",
        polymarket_condition_id="0xtestcondition",
        polymarket_yes_token_id="0xtestyes",
        polymarket_no_token_id="0xtestno",
    )
    pair.status = "verified"

    collector = SustainedStudyCollector(
        config=config,
        pairs=[pair],
        duckdb_path=":memory:",
        duration_hours=1.0,
        snapshot_interval=5.0,
        overrides_path="config/market_overrides.yaml",
    )

    class MockBook:
        def __init__(self, yes_bids=None, no_bids=None, asks=None, bids=None):
            self.yes_bid_levels = yes_bids or [(Decimal("45"), Decimal("100"))]
            self.no_bid_levels = no_bids or [(Decimal("45"), Decimal("100"))]
            self.ask_levels = asks or [(Decimal("50"), Decimal("100"))]
            self.bid_levels = bids or [(Decimal("48"), Decimal("100"))]
            self.exchange_updated_at = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)

    k_books = {"KXTEST-1": MockBook()}
    p_books = {
        "0xtestyes": MockBook(),
        "0xtestno": MockBook(),
    }

    batch_ts = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    records = collector._build_snapshot_records(batch_ts, k_books, p_books)

    assert len(records) == 2
    kalshi_rec = next(r for r in records if r.platform == "kalshi")
    poly_rec = next(r for r in records if r.platform == "polymarket")

    assert kalshi_rec.ticker == "KXTEST-1"
    assert kalshi_rec.tradable is True
    assert kalshi_rec.ask_yes == Decimal("55")  # 100 - no_bid (45)
    assert kalshi_rec.ask_no == Decimal("55")

    assert poly_rec.ticker == "0xtestcondition"
    assert poly_rec.tradable is True
    assert poly_rec.ask_yes == Decimal("50")
    assert poly_rec.ask_no == Decimal("50")


def test_study_analyzer_empty_db(tmp_path: Path) -> None:
    """Verify StudyAnalyzer gracefully handles an empty database without crashing."""
    db_file = tmp_path / "empty_study.duckdb"
    conn = duckdb.connect(str(db_file))
    ensure_schema(conn)

    config = load_config()
    analyzer = StudyAnalyzer(conn=conn, config=config, use_raw_table=True)
    results = analyzer.run_study()

    assert results["metadata"]["total_snapshots_loaded"] == 0
    assert results["research_question_1_frequency"]["overall_executable_per_hour"] == 0.0
    assert results["research_question_2_funnel"]["funnel_stages"] == {}
    assert results["research_question_3_edge_distribution"]["sample_size"] == 0
    assert results["research_question_4_persistence"]["total_distinct_episodes"] == 0
    conn.close()


def test_study_analyzer_with_synthetic_opportunities(tmp_path: Path) -> None:
    """Verify StudyAnalyzer calculation of Q1-Q7 with synthetic historical data."""
    db_file = tmp_path / "synthetic_study.duckdb"
    conn = connect(str(db_file))
    ensure_schema(conn)

    pair = MarketPairOverride(
        pair_id="dem-nom-test",
        category="politics",
        status="excluded",
        exclusion_reason="mock",
        kalshi_ticker="KXTEST-DEM",
        polymarket_condition_id="0xpolydem",
        polymarket_yes_token_id="0xpolydem_yes",
        polymarket_no_token_id="0xpolydem_no",
    )
    pair.status = "verified"
    sync_verified_pairs(conn, [pair])

    base_time = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    t0 = base_time
    t1 = base_time + timedelta(seconds=5)
    t2 = base_time + timedelta(seconds=10)
    t3 = base_time + timedelta(seconds=15)

    records: list[SnapshotRecord] = []

    # t0: Fair market, Kalshi Yes ask = 52, Poly No ask = 50 -> sum = 102 (no arb)
    records.append(
        SnapshotRecord(
            ts=t0, platform="kalshi", ticker="KXTEST-DEM",
            observed_at_utc=t0.replace(tzinfo=None), exchange_updated_at_utc=t0.replace(tzinfo=None),
            ask_yes=Decimal("52"), ask_no=Decimal("52"), ask_yes_size=Decimal("100"), ask_no_size=Decimal("100"),
            tradable=True, data_source="live_api",
            levels={"yes_ask": [(Decimal("52"), Decimal("100"))], "no_ask": [(Decimal("52"), Decimal("100"))]},
        )
    )
    records.append(
        SnapshotRecord(
            ts=t0, platform="polymarket", ticker="0xpolydem",
            observed_at_utc=t0.replace(tzinfo=None), exchange_updated_at_utc=t0.replace(tzinfo=None),
            ask_yes=Decimal("50"), ask_no=Decimal("50"), ask_yes_size=Decimal("100"), ask_no_size=Decimal("100"),
            tradable=True, data_source="live_api",
            levels={"yes_ask": [(Decimal("50"), Decimal("100"))], "no_ask": [(Decimal("50"), Decimal("100"))]},
        )
    )

    # t1: Raw & Executable Arb appears! Kalshi Yes ask = 40, Poly No ask = 48 -> sum = 88 < 100 (raw edge = 12c)
    records.append(
        SnapshotRecord(
            ts=t1, platform="kalshi", ticker="KXTEST-DEM",
            observed_at_utc=t1.replace(tzinfo=None), exchange_updated_at_utc=t1.replace(tzinfo=None),
            ask_yes=Decimal("40"), ask_no=Decimal("62"), ask_yes_size=Decimal("100"), ask_no_size=Decimal("100"),
            tradable=True, data_source="live_api",
            levels={"yes_ask": [(Decimal("40"), Decimal("100"))], "no_ask": [(Decimal("62"), Decimal("100"))]},
        )
    )
    records.append(
        SnapshotRecord(
            ts=t1, platform="polymarket", ticker="0xpolydem",
            observed_at_utc=t1.replace(tzinfo=None), exchange_updated_at_utc=t1.replace(tzinfo=None),
            ask_yes=Decimal("55"), ask_no=Decimal("48"), ask_yes_size=Decimal("100"), ask_no_size=Decimal("100"),
            tradable=True, data_source="live_api",
            levels={"yes_ask": [(Decimal("55"), Decimal("100"))], "no_ask": [(Decimal("48"), Decimal("100"))]},
        )
    )

    # t2: Arb persists! Kalshi Yes ask = 42, Poly No ask = 49 -> sum = 91 < 100 (raw edge = 9c)
    records.append(
        SnapshotRecord(
            ts=t2, platform="kalshi", ticker="KXTEST-DEM",
            observed_at_utc=t2.replace(tzinfo=None), exchange_updated_at_utc=t2.replace(tzinfo=None),
            ask_yes=Decimal("42"), ask_no=Decimal("60"), ask_yes_size=Decimal("100"), ask_no_size=Decimal("100"),
            tradable=True, data_source="live_api",
            levels={"yes_ask": [(Decimal("42"), Decimal("100"))], "no_ask": [(Decimal("60"), Decimal("100"))]},
        )
    )
    records.append(
        SnapshotRecord(
            ts=t2, platform="polymarket", ticker="0xpolydem",
            observed_at_utc=t2.replace(tzinfo=None), exchange_updated_at_utc=t2.replace(tzinfo=None),
            ask_yes=Decimal("53"), ask_no=Decimal("49"), ask_yes_size=Decimal("100"), ask_no_size=Decimal("100"),
            tradable=True, data_source="live_api",
            levels={"yes_ask": [(Decimal("53"), Decimal("100"))], "no_ask": [(Decimal("49"), Decimal("100"))]},
        )
    )

    # t3: Arb disappears: Kalshi Yes ask = 51, Poly No ask = 51 -> sum = 102
    records.append(
        SnapshotRecord(
            ts=t3, platform="kalshi", ticker="KXTEST-DEM",
            observed_at_utc=t3.replace(tzinfo=None), exchange_updated_at_utc=t3.replace(tzinfo=None),
            ask_yes=Decimal("51"), ask_no=Decimal("51"), ask_yes_size=Decimal("100"), ask_no_size=Decimal("100"),
            tradable=True, data_source="live_api",
            levels={"yes_ask": [(Decimal("51"), Decimal("100"))], "no_ask": [(Decimal("51"), Decimal("100"))]},
        )
    )
    records.append(
        SnapshotRecord(
            ts=t3, platform="polymarket", ticker="0xpolydem",
            observed_at_utc=t3.replace(tzinfo=None), exchange_updated_at_utc=t3.replace(tzinfo=None),
            ask_yes=Decimal("51"), ask_no=Decimal("51"), ask_yes_size=Decimal("100"), ask_no_size=Decimal("100"),
            tradable=True, data_source="live_api",
            levels={"yes_ask": [(Decimal("51"), Decimal("100"))], "no_ask": [(Decimal("51"), Decimal("100"))]},
        )
    )

    write_snapshots(conn, records)

    config = load_config()
    analyzer = StudyAnalyzer(conn=conn, config=config, use_raw_table=False)
    results = analyzer.run_study()

    # Validations on research answers
    meta = results["metadata"]
    assert meta["synchronized_pair_observations"] == 4
    assert meta["distinct_pairs_count"] == 1

    # Q1: Frequency per pair-hour
    q1 = results["research_question_1_frequency"]
    assert len(q1["rates_by_pair"]) == 1
    pair_stat = q1["rates_by_pair"][0]
    assert pair_stat["pair_id"] == "dem-nom-test"
    assert pair_stat["raw_opportunities"] >= 2
    assert pair_stat["executable_opportunities"] >= 2
    assert pair_stat["executable_per_pair_hour"] > 0

    # Q2: Funnel
    q2 = results["research_question_2_funnel"]
    stages = q2["funnel_stages"]
    assert stages["2_raw_pricing_opportunities"] >= 2
    assert stages["3_fee_adjusted_opportunities"] >= 2
    assert stages["4_executable_candidate_observations"] >= 2
    assert stages["5_distinct_executable_episodes"] >= 1

    # Q3: Edge distribution
    q3 = results["research_question_3_edge_distribution"]
    assert q3["sample_size"] >= 2
    assert q3["mean_cents"] > 2.0
    assert q3["min_cents"] > 0.0

    # Q4: Persistence
    q4 = results["research_question_4_persistence"]
    assert q4["total_distinct_episodes"] >= 1
    assert q4["mean_duration_seconds"] >= 5.0

    # Q5: Depth
    q5 = results["research_question_5_depth_and_volume"]
    assert q5["fillability_rates"]["size_10_fill_pct"] == 100.0

    # Q6: Sensitivity matrix
    q6 = results["research_question_6_sensitivity"]
    assert len(q6) > 0

    # Q7: Synchronization
    q7 = results["research_question_7_synchronization"]
    assert q7["usable_synchronized_pct"] == 100.0
    conn.close()


def test_export_study_artifacts(tmp_path: Path) -> None:
    """Verify that all export artifacts (JSON, Markdown, and CSV tables) are written."""
    results = {
        "metadata": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "dataset": "research_snapshots",
            "total_snapshots_loaded": 100,
            "total_study_hours": 1.0,
            "total_valid_pair_hours": 20.0,
            "distinct_pairs_count": 20,
        },
        "research_question_1_frequency": {
            "overall_executable_per_hour": 1.5,
            "rates_by_pair": [
                {
                    "pair_id": "pair-1",
                    "valid_observation_hours": 1.0,
                    "raw_opportunities": 5,
                    "fee_adjusted_opportunities": 3,
                    "executable_opportunities": 2,
                    "executable_per_pair_hour": 2.0,
                }
            ],
        },
        "research_question_2_funnel": {
            "funnel_stages": {
                "1_total_pair_evaluations": 100,
                "2_raw_pricing_opportunities": 20,
                "3_fee_adjusted_opportunities": 10,
                "4_executable_candidate_observations": 5,
                "5_distinct_executable_episodes": 2,
            },
            "conversion_rates_pct": {
                "raw_to_eval_pct": 20.0,
                "fee_adj_to_raw_pct": 50.0,
                "exec_to_fee_adj_pct": 50.0,
            },
            "primary_rejection_reasons": {"BELOW_MIN_EDGE_AFTER_COSTS": 5},
            "latency_survival_counts": {"survived_2s": 2, "survived_5s": 1},
        },
        "research_question_3_edge_distribution": {
            "sample_size": 5,
            "mean_cents": 3.5,
            "median_cents": 3.0,
            "std_dev_cents": 0.5,
            "min_cents": 2.5,
            "max_cents": 4.5,
            "percentiles": {"p25": 2.8, "p50": 3.0, "p75": 4.0},
            "edge_buckets": {"2.0¢ - 5.0¢": 5},
        },
        "research_question_4_persistence": {
            "total_distinct_episodes": 2,
            "mean_duration_seconds": 15.0,
            "median_duration_seconds": 15.0,
            "min_duration_seconds": 10.0,
            "max_duration_seconds": 20.0,
            "persistence_survival_counts": {"persisted_gte_0s": 2, "persisted_gte_10s": 2, "persisted_gte_30s": 0},
            "persistence_survival_percentages": {"survival_gte_0s_pct": 100.0, "survival_gte_10s_pct": 100.0, "survival_gte_30s_pct": 0.0},
        },
        "research_question_5_depth_and_volume": {
            "total_quotes_evaluated": 100,
            "average_available_yes_size": 150.0,
            "average_available_no_size": 150.0,
            "fillability_rates": {"size_1_fill_pct": 100.0, "size_10_fill_pct": 100.0, "size_50_fill_pct": 98.0},
            "fill_counts": {1: 100, 10: 100, 50: 98},
        },
        "research_question_6_sensitivity": [
            {
                "fee_scenario": "baseline",
                "trade_size_q": 10,
                "min_edge_cents": 2.0,
                "quote_freshness_sec": 10.0,
                "raw_positive_count": 20,
                "fee_adjusted_count": 10,
                "executable_candidates_count": 5,
                "mean_net_edge_cents": 3.5,
                "max_net_edge_cents": 4.5,
            }
        ],
        "research_question_7_synchronization": {
            "usable_synchronized_pct": 99.5,
            "total_pair_intervals": 100,
            "synchronized_intervals": 100,
            "quote_skew": {"median_quote_skew_ms": 12.0, "p95_quote_skew_ms": 45.0},
            "freshness": {"median_quote_age_sec": 1.2, "p95_quote_age_sec": 4.0, "missing_exchange_ts_pct": 0.0},
        },
    }

    out_dir = tmp_path / "results_export"
    generated_files = export_study_artifacts(results, out_dir)

    expected_files = [
        "study_analysis_summary.json",
        "study_analysis_report.md",
        "funnel_summary.csv",
        "pair_hourly_rates.csv",
        "sensitivity_matrix.csv",
        "edge_distribution.csv",
        "persistence_survival.csv",
        "depth_and_fillability.csv",
        "data_synchronization.csv",
    ]

    for expected in expected_files:
        p = out_dir / expected
        assert p.exists(), f"Expected artifact {expected} was not created"
        assert p.stat().st_size > 0, f"Expected artifact {expected} is empty"
