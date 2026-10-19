"""Unit and integration tests for ReplayEngine and historical snapshot backtesting."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest
import duckdb

from core.config_loader import load_config
from core.models import Opportunity, OpportunityLeg
from simulation.replay_engine import (
    OpportunityRecord,
    ReplayEngine,
    ReplaySummary,
)


def create_mock_snapshots_db(db_path: Path) -> list[datetime]:
    """Helper to populate a test DuckDB database with mock orderbook snapshots."""
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE orderbook_snapshots (
            ts TIMESTAMP WITH TIME ZONE,
            platform VARCHAR,
            ticker VARCHAR,
            yes_bid INTEGER,
            yes_ask INTEGER,
            no_bid INTEGER,
            no_ask INTEGER,
            volume BIGINT
        );
    """)

    base_ts = datetime(2026, 8, 1, 12, 0, 0, tzinfo=timezone.utc)
    t0 = base_ts
    t1 = base_ts + timedelta(seconds=60)
    t2 = base_ts + timedelta(seconds=120)
    t3 = base_ts + timedelta(seconds=180)

    rows = [
        # t0: Fair market, no intra arbitrage
        (t0, "kalshi", "KXTEST-1", 45, 52, 45, 52, 1000),
        (t0, "polymarket", "0x0f49db97f71c68b1e42a6d16e3de93d85dbf7d4148e3f018eb79e88554be9f75", 48, 50, 48, 51, 5000),

        # t1: Intra-market arbitrage appears on Kalshi! yes_ask=40, no_ask=50 -> sum=90 < 100 (edge = 10c)
        (t1, "kalshi", "KXTEST-1", 45, 40, 45, 50, 1500),
        (t1, "polymarket", "0x0f49db97f71c68b1e42a6d16e3de93d85dbf7d4148e3f018eb79e88554be9f75", 48, 50, 48, 51, 5200),

        # t2: Intra-market arbitrage persists on Kalshi! yes_ask=42, no_ask=51 -> sum=93 < 100 (edge = 7c)
        (t2, "kalshi", "KXTEST-1", 45, 42, 45, 51, 2000),
        (t2, "polymarket", "0x0f49db97f71c68b1e42a6d16e3de93d85dbf7d4148e3f018eb79e88554be9f75", 48, 50, 48, 51, 5500),

        # t3: Arbitrage disappears (filled/corrected): yes_ask=50, no_ask=52 -> sum=102 >= 100
        (t3, "kalshi", "KXTEST-1", 45, 50, 45, 52, 2500),
        (t3, "polymarket", "0x0f49db97f71c68b1e42a6d16e3de93d85dbf7d4148e3f018eb79e88554be9f75", 48, 50, 48, 51, 5800),
    ]

    conn.executemany(
        """
        INSERT INTO orderbook_snapshots (ts, platform, ticker, yes_bid, yes_ask, no_bid, no_ask, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )
    conn.close()
    return [t0, t1, t2, t3]


@pytest.mark.asyncio
async def test_replay_engine_persistence_and_stats(tmp_path: Path):
    """Test ReplayEngine lifecycle tracking: birth, persistence, disappearance, and stats."""
    db_file = tmp_path / "test_snapshots.duckdb"
    timestamps = create_mock_snapshots_db(db_file)

    engine = ReplayEngine(
        duckdb_path=db_file,
        speed=0.0,  # Instantaneous
        include_intra=True,
        include_cross=False,
    )

    summary = await engine.run()

    # Validations
    assert summary.total_snapshots == 4
    assert summary.total_rows == 8
    assert summary.total_opportunities_found == 1
    assert summary.intra_market_count == 1
    assert summary.cross_platform_count == 0

    opp = summary.opportunities[0]
    assert opp.platform == "kalshi"
    assert opp.ticker == "KXTEST-1"
    assert opp.snapshots_count == 2  # Persisted over t1 and t2
    assert opp.start_ts == timestamps[1]  # t1
    assert opp.last_seen_ts == timestamps[2]  # t2
    assert opp.end_ts == timestamps[3]  # Disappeared at t3
    assert opp.duration_seconds == 120.0  # t3 - t1 = 120s
    assert opp.persistence_span_seconds == 60.0  # t2 - t1 = 60s
    assert opp.is_active is False

    # Edge statistics
    assert summary.edge_stats["count"] == 1
    assert summary.edge_stats["max"] >= 7.0
    assert summary.duration_stats["count"] == 1
    assert summary.duration_stats["mean"] == 120.0

    # Test report formatting
    report = summary.format_report()
    assert "HISTORICAL REPLAY ARBITRAGE REPORT" in report
    assert "KXTEST-1" in report
    assert "Total Unique Opportunities: 1" in report


@pytest.mark.asyncio
async def test_replay_engine_date_filtering(tmp_path: Path):
    """Test start_date and end_date filtering in ReplayEngine."""
    db_file = tmp_path / "filter_snapshots.duckdb"
    create_mock_snapshots_db(db_file)

    # Filter to only t1 and t2
    engine = ReplayEngine(
        duckdb_path=db_file,
        speed=0.0,
        start_date="2026-08-01 12:01:00",
        end_date="2026-08-01 12:02:00",
    )

    summary = await engine.run()
    assert summary.total_snapshots == 2
    assert summary.total_rows == 4


@pytest.mark.asyncio
async def test_replay_engine_speed_multiplier(tmp_path: Path):
    """Test speed multiplier calculations with non-zero speed."""
    db_file = tmp_path / "speed_snapshots.duckdb"
    create_mock_snapshots_db(db_file)

    # Run with fast 1000x multiplier
    engine = ReplayEngine(
        duckdb_path=db_file,
        speed=1000.0,
        limit_snapshots=2,
    )

    summary = await engine.run()
    assert summary.total_snapshots == 2
    assert summary.wall_clock_duration_seconds >= 0.0


def test_calculate_distributions_edge_cases():
    """Test statistics calculators with empty list and single item."""
    empty_edge = ReplayEngine._calculate_edge_distribution([])
    assert empty_edge == {}

    empty_dur = ReplayEngine._calculate_duration_distribution([])
    assert empty_dur == {}

    # Single opportunity
    rec = OpportunityRecord(
        opportunity_id="test",
        strategy="intra_market",
        platform="kalshi",
        ticker="TICK",
        start_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        last_seen_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration_seconds=30.0,
        initial_edge_cents=3.5,
        min_edge_cents=3.5,
        max_edge_cents=3.5,
        mean_edge_cents=3.5,
        edge_samples=[3.5],
    )
    edge_stats = ReplayEngine._calculate_edge_distribution([rec])
    assert edge_stats["mean"] == 3.5
    assert edge_stats["count"] == 1
    assert edge_stats["histogram"]["2.00¢ - 5.00¢"] == 1


@pytest.mark.asyncio
async def test_replay_engine_cross_platform(tmp_path: Path):
    """Test ReplayEngine detecting and tracking cross-platform arbitrage on override pair."""
    db_file = tmp_path / "cross_snapshots.duckdb"
    conn = duckdb.connect(str(db_file))
    conn.execute("""
        CREATE TABLE orderbook_snapshots (
            ts TIMESTAMP WITH TIME ZONE,
            platform VARCHAR,
            ticker VARCHAR,
            yes_bid INTEGER,
            yes_ask INTEGER,
            no_bid INTEGER,
            no_ask INTEGER,
            volume BIGINT
        );
    """)

    t0 = datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 8, 1, 10, 1, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 8, 1, 10, 2, 0, tzinfo=timezone.utc)

    # Use verified override pair: Gavin Newsom 2028 Democratic nomination
    k_ticker = "KXPRESNOMD-28-GN"
    p_cond = "0x0f49db97f71c68b1e42a6d16e3de93d85dbf7d4148e3f018eb79e88554be9f75"

    rows = [
        # t0: Arb exists: Kalshi YES Ask=10, Polymarket NO Ask=80 -> sum=90 < 100
        (t0, "kalshi", k_ticker, 9, 10, 89, 90, 100),
        (t0, "polymarket", p_cond, 18, 19, 79, 80, 200),

        # t1: Arb continues: Kalshi YES Ask=12, Polymarket NO Ask=81 -> sum=93 < 100
        (t1, "kalshi", k_ticker, 11, 12, 87, 88, 150),
        (t1, "polymarket", p_cond, 18, 19, 80, 81, 250),

        # t2: Arb closes: Kalshi YES Ask=15, Polymarket NO Ask=87 -> sum=102 >= 100
        (t2, "kalshi", k_ticker, 14, 15, 84, 85, 200),
        (t2, "polymarket", p_cond, 12, 13, 86, 87, 300),
    ]

    conn.executemany(
        """
        INSERT INTO orderbook_snapshots (ts, platform, ticker, yes_bid, yes_ask, no_bid, no_ask, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )
    conn.close()

    engine = ReplayEngine(
        duckdb_path=db_file,
        speed=0.0,
        include_intra=False,
        include_cross=True,
    )

    summary = await engine.run()
    assert summary.total_snapshots == 3
    assert summary.total_opportunities_found == 1
    assert summary.cross_platform_count == 1

    opp = summary.opportunities[0]
    assert opp.strategy == "cross_platform"
    assert opp.ticker == k_ticker
    assert opp.target_ticker == p_cond
    assert opp.snapshots_count == 2
    assert opp.start_ts == t0
    assert opp.last_seen_ts == t1
    assert opp.end_ts == t2
    assert opp.duration_seconds == 120.0

