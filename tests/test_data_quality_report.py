"""Unit tests for data quality and snapshot dataset reliability analyzer."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import pytest

from core.config_loader import load_config
from core.models import DataSource, MarketStatus
from core.pair_registry import MarketPairOverride
from core.snapshot_store import (
    SnapshotRecord,
    connect,
    ensure_schema,
    sync_verified_pairs,
    write_snapshots,
)
from scripts.data_quality_report import (
    LABEL_UNRELIABLE,
    LABEL_USABLE,
    LABEL_USABLE_WITH_CAVEATS,
    DataQualityAnalyzer,
    format_markdown_report,
)


@pytest.fixture
def mock_db_with_verified_pair(tmp_path: Path):
    db_file = tmp_path / "quality_test.duckdb"
    conn = connect(str(db_file))
    ensure_schema(conn)

    pair = MarketPairOverride(
        pair_id="test-quality-pair",
        category="politics",
        status="excluded",
        exclusion_reason="mock",
        kalshi_ticker="KXTEST-Q",
        polymarket_condition_id="0x" + "b" * 64,
    )
    pair.status = "verified"
    sync_verified_pairs(conn, [pair])
    yield conn, db_file
    conn.close()


def test_clean_data_classified_as_usable(mock_db_with_verified_pair):
    conn, db_file = mock_db_with_verified_pair
    config = load_config()

    base_time = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    records = []
    for i in range(10):
        t = base_time + timedelta(seconds=i * 60)
        rec = SnapshotRecord(
            ts=t,
            platform="kalshi",
            ticker="KXTEST-Q",
            observed_at_utc=t.replace(tzinfo=None),
            exchange_updated_at_utc=t.replace(tzinfo=None),
            tradable=True,
            data_source=DataSource.LIVE_API.value,
            market_status=MarketStatus.OPEN.value,
            ask_yes=Decimal("50.0"),
            ask_no=Decimal("52.0"),
            ask_yes_size=Decimal("100.0"),
            ask_no_size=Decimal("100.0"),
            bid_yes=Decimal("48.0"),
            bid_no=Decimal("46.0"),
            bid_yes_size=Decimal("100.0"),
            bid_no_size=Decimal("100.0"),
            levels={
                "yes_ask": [(Decimal("50.0"), Decimal("100.0"))],
                "no_ask": [(Decimal("52.0"), Decimal("100.0"))],
            },
        )
        records.append(rec)

    write_snapshots(conn, records)

    analyzer = DataQualityAnalyzer(
        conn=conn,
        config=config,
        snapshot_interval=60.0,
        gap_threshold=300.0,
        stale_threshold=60.0,
    )
    res = analyzer.analyze()
    assert res["overall_reliability"] == LABEL_USABLE
    assert res["snapshot_counts"]["total_analyzed"] == 10
    assert res["gaps"]["count"] == 0
    assert res["liquidity_and_depth"]["crossed_books_count"] == 0
    assert res["liquidity_and_depth"]["fillability"]["size_10_pct"] == 100.0

    md = format_markdown_report(res)
    assert "# Data Quality & Reliability Report" in md
    assert "USABLE" in md


def test_data_with_gaps_classified_with_caveats(mock_db_with_verified_pair):
    conn, db_file = mock_db_with_verified_pair
    config = load_config()

    base_time = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    # Create two snapshots separated by 900 seconds (> 300s gap threshold)
    t0 = base_time
    t1 = base_time + timedelta(seconds=900)

    r0 = SnapshotRecord(
        ts=t0,
        platform="kalshi",
        ticker="KXTEST-Q",
        observed_at_utc=t0.replace(tzinfo=None),
        exchange_updated_at_utc=t0.replace(tzinfo=None),
        tradable=True,
        data_source=DataSource.LIVE_API.value,
        market_status=MarketStatus.OPEN.value,
        ask_yes=Decimal("50.0"),
        ask_yes_size=Decimal("100.0"),
    )
    r1 = SnapshotRecord(
        ts=t1,
        platform="kalshi",
        ticker="KXTEST-Q",
        observed_at_utc=t1.replace(tzinfo=None),
        exchange_updated_at_utc=t1.replace(tzinfo=None),
        tradable=True,
        data_source=DataSource.LIVE_API.value,
        market_status=MarketStatus.OPEN.value,
        ask_yes=Decimal("50.0"),
        ask_yes_size=Decimal("100.0"),
    )
    write_snapshots(conn, [r0, r1])

    analyzer = DataQualityAnalyzer(
        conn=conn,
        config=config,
        snapshot_interval=60.0,
        gap_threshold=300.0,
    )
    res = analyzer.analyze()
    assert res["gaps"]["count"] == 1
    assert res["gaps"]["incidents"][0]["duration_seconds"] == 900.0


def test_data_with_crossed_book_classified_as_unreliable(mock_db_with_verified_pair):
    conn, db_file = mock_db_with_verified_pair
    config = load_config()

    base_time = datetime(2026, 11, 9, 10, 0, 0, tzinfo=timezone.utc)
    # bid_yes (55) > ask_yes (50) -> crossed book!
    rec = SnapshotRecord(
        ts=base_time,
        platform="kalshi",
        ticker="KXTEST-Q",
        observed_at_utc=base_time.replace(tzinfo=None),
        exchange_updated_at_utc=base_time.replace(tzinfo=None),
        tradable=True,
        data_source=DataSource.LIVE_API.value,
        market_status=MarketStatus.OPEN.value,
        bid_yes=Decimal("55.0"),
        ask_yes=Decimal("50.0"),
        bid_yes_size=Decimal("10.0"),
        ask_yes_size=Decimal("10.0"),
    )
    write_snapshots(conn, [rec])

    analyzer = DataQualityAnalyzer(conn=conn, config=config)
    res = analyzer.analyze()
    assert res["liquidity_and_depth"]["crossed_books_count"] == 1
    assert res["overall_reliability"] == LABEL_UNRELIABLE


def test_empty_dataset_analysis(mock_db_with_verified_pair):
    conn, db_file = mock_db_with_verified_pair
    config = load_config()

    analyzer = DataQualityAnalyzer(conn=conn, config=config)
    res = analyzer.analyze()
    assert res["overall_reliability"] == LABEL_UNRELIABLE
    assert res["snapshot_counts"]["total_analyzed"] == 0
    assert "note" in res
