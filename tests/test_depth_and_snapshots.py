"""Unit tests for depth calculations, ladders, VWAP, and DuckDB snapshot store."""

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import duckdb
import pytest

from core.depth import (
    clean_levels,
    kalshi_ladders,
    polymarket_ladders,
    truncate_levels,
    vwap_for_size,
)
from core.models import DataSource, MarketStatus
from core.pair_registry import MarketPairOverride
from core.snapshot_store import (
    SnapshotRecord,
    connect,
    ensure_schema,
    log_exclusions,
    sync_verified_pairs,
    view_exists,
    write_snapshots,
)


# ============================================================================
# Depth & VWAP Tests
# ============================================================================


def test_clean_and_truncate_levels():
    # Asks: ascending sort
    raw_asks = [(Decimal("52"), Decimal("10")), (Decimal("50"), Decimal("5")), (Decimal("48"), Decimal("15"))]
    cleaned = clean_levels(raw_asks, ascending=True)
    assert cleaned == [(Decimal("48"), Decimal("15")), (Decimal("50"), Decimal("5")), (Decimal("52"), Decimal("10"))]

    # Bids: descending sort
    raw_bids = [(Decimal("45"), Decimal("10")), (Decimal("48"), Decimal("5")), (Decimal("42"), Decimal("5"))]
    cleaned_bids = clean_levels(raw_bids, ascending=False)
    assert cleaned_bids == [(Decimal("48"), Decimal("5")), (Decimal("45"), Decimal("10")), (Decimal("42"), Decimal("5"))]

    # Truncate top 1
    truncated, was_cut = truncate_levels(cleaned_bids, 1)
    assert len(truncated) == 1
    assert was_cut is True
    assert truncated[0] == (Decimal("48"), Decimal("5"))


def test_vwap_for_size():
    levels = [
        (Decimal("40"), Decimal("5")),
        (Decimal("50"), Decimal("10")),
    ]
    # 1. Exact fill within first level
    price, filled, fully_filled = vwap_for_size(levels, Decimal("4"))
    assert filled == Decimal("4")
    assert fully_filled is True
    assert price == Decimal("40")

    # 2. Multi-level fill: 5 @ 40 + 5 @ 50 = 200 + 250 = 450 / 10 = 45
    price, filled, fully_filled = vwap_for_size(levels, Decimal("10"))
    assert filled == Decimal("10")
    assert fully_filled is True
    assert price == Decimal("45")

    # 3. Partial fill: total available is 15 contracts, asking for 20
    price, filled, fully_filled = vwap_for_size(levels, Decimal("20"))
    assert filled == Decimal("15")
    assert fully_filled is False
    # Total cost: 5*40 + 10*50 = 700. VWAP = 700 / 15 = 46.6667
    assert round(price, 4) == Decimal("46.6667")

    # 4. Empty levels
    price, filled, fully_filled = vwap_for_size([], Decimal("10"))
    assert price is None
    assert filled == Decimal("0")
    assert fully_filled is False

    # 5. Non-positive size raises ValueError
    with pytest.raises(ValueError, match="qty must be a positive number"):
        vwap_for_size(levels, Decimal("0"))


def test_kalshi_and_polymarket_ladders():
    from unittest.mock import MagicMock

    # Kalshi bids derive opposite asks
    k_book = MagicMock()
    k_book.yes_bid_levels = [(Decimal("45"), Decimal("100"))]
    k_book.no_bid_levels = [(Decimal("52"), Decimal("80"))]
    k_book.exchange_updated_at = None

    k_ladders = kalshi_ladders(k_book)
    assert k_ladders.yes_bid[0] == (Decimal("45"), Decimal("100"))
    # Derived yes ask = 100 - no_bid = 100 - 52 = 48
    assert k_ladders.yes_ask[0] == (Decimal("48"), Decimal("80"))

    # Polymarket separate yes and no tokens
    p_yes = MagicMock()
    p_yes.bid_levels = [(Decimal("46"), Decimal("200"))]
    p_yes.ask_levels = [(Decimal("49"), Decimal("150"))]
    p_yes.exchange_updated_at = None

    p_no = MagicMock()
    p_no.bid_levels = [(Decimal("50"), Decimal("100"))]
    p_no.ask_levels = [(Decimal("53"), Decimal("100"))]
    p_no.exchange_updated_at = None

    p_ladders = polymarket_ladders(p_yes, p_no)
    assert p_ladders.yes_bid[0] == (Decimal("46"), Decimal("200"))
    assert p_ladders.yes_ask[0] == (Decimal("49"), Decimal("150"))


# ============================================================================
# DuckDB Schema, Migration, and Views Tests
# ============================================================================


def test_duckdb_schema_and_atomic_write(tmp_path: Path):
    db_file = tmp_path / "test_store.duckdb"
    conn = connect(str(db_file))
    res = ensure_schema(conn)
    assert res["legacy_rows_migrated"] == 0

    now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
    record = SnapshotRecord(
        ts=now_utc,
        platform="kalshi",
        ticker="KXTEST-1",
        observed_at_utc=now_utc.replace(tzinfo=None),
        exchange_updated_at_utc=now_utc.replace(tzinfo=None),
        yes_bid=45,
        yes_ask=50,
        no_bid=48,
        no_ask=53,
        volume=1000,
        ask_yes=Decimal("50.0"),
        ask_no=Decimal("53.0"),
        ask_yes_size=Decimal("100.0"),
        ask_no_size=Decimal("100.0"),
        bid_yes=Decimal("45.0"),
        bid_no=Decimal("48.0"),
        bid_yes_size=Decimal("100.0"),
        bid_no_size=Decimal("100.0"),
        market_status=MarketStatus.OPEN.value,
        tradable=True,
        data_source=DataSource.LIVE_API.value,
        levels={
            "yes_ask": [(Decimal("50.0"), Decimal("100.0"))],
            "no_ask": [(Decimal("53.0"), Decimal("100.0"))],
        },
    )

    written = write_snapshots(conn, [record])
    assert written == 1
    assert record.snapshot_id is not None

    # Check orderbook_snapshots
    snap_row = conn.execute(
        "SELECT snapshot_id, platform, ticker, ask_yes, tradable, data_source FROM orderbook_snapshots WHERE snapshot_id = ?",
        [record.snapshot_id],
    ).fetchone()
    assert snap_row[0] == record.snapshot_id
    assert snap_row[1] == "kalshi"
    assert snap_row[2] == "KXTEST-1"
    assert Decimal(str(snap_row[3])) == Decimal("50.0")
    assert snap_row[4] is True
    assert snap_row[5] == "live_api"

    # Check orderbook_levels
    levels = conn.execute(
        "SELECT side, price, size FROM orderbook_levels WHERE snapshot_id = ?",
        [record.snapshot_id],
    ).fetchall()
    assert len(levels) == 2
    sides = {l[0] for l in levels}
    assert sides == {"yes_ask", "no_ask"}


def test_legacy_schema_migration(tmp_path: Path):
    db_file = tmp_path / "legacy_test.duckdb"
    # Create legacy 8-column table manually
    raw_conn = duckdb.connect(str(db_file))
    raw_conn.execute(
        """
        CREATE TABLE orderbook_snapshots (
            ts TIMESTAMP WITH TIME ZONE,
            platform VARCHAR,
            ticker VARCHAR,
            yes_bid INTEGER,
            yes_ask INTEGER,
            no_bid INTEGER,
            no_ask INTEGER,
            volume BIGINT
        )
        """
    )
    t0 = datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)
    raw_conn.execute(
        "INSERT INTO orderbook_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [t0, "kalshi", "KXTEST-OLD", 45, 50, 48, 53, 500],
    )
    raw_conn.close()

    # Now open with our connect and ensure_schema
    conn = connect(str(db_file))
    res = ensure_schema(conn)
    assert res["legacy_rows_migrated"] == 1

    row = conn.execute(
        "SELECT snapshot_id, data_source, tradable, market_status, schema_version FROM orderbook_snapshots"
    ).fetchone()
    assert row[0] is not None  # snapshot_id assigned
    assert row[1] == "legacy_unverified"
    assert row[2] is False  # not tradable
    assert row[3] == "unknown"
    assert row[4] == 1


def test_research_snapshots_view_filters(tmp_path: Path):
    db_file = tmp_path / "view_test.duckdb"
    conn = connect(str(db_file))
    ensure_schema(conn)

    now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
    # Register verified pair
    pair = MarketPairOverride(
        pair_id="p-test-1",
        category="politics",
        status="excluded",
        exclusion_reason="init",
        kalshi_ticker="KXTEST-1",
        polymarket_condition_id="0x" + "1" * 64,
    )
    # Mark status as verified for syncing
    pair.status = "verified"
    sync_verified_pairs(conn, [pair])

    # 1. Valid live_api row for verified pair -> should appear in research_snapshots
    r_valid = SnapshotRecord(
        ts=now_utc,
        platform="kalshi",
        ticker="KXTEST-1",
        observed_at_utc=now_utc.replace(tzinfo=None),
        exchange_updated_at_utc=now_utc.replace(tzinfo=None),
        tradable=True,
        data_source=DataSource.LIVE_API.value,
        market_status=MarketStatus.OPEN.value,
    )
    # 2. Non-tradable row -> excluded
    r_nontradable = SnapshotRecord(
        ts=now_utc,
        platform="kalshi",
        ticker="KXTEST-1",
        observed_at_utc=now_utc.replace(tzinfo=None),
        exchange_updated_at_utc=now_utc.replace(tzinfo=None),
        tradable=False,
        data_source=DataSource.LIVE_API.value,
    )
    # 3. Unverified market row -> excluded
    r_unverified = SnapshotRecord(
        ts=now_utc,
        platform="kalshi",
        ticker="KXUNKNOWN",
        observed_at_utc=now_utc.replace(tzinfo=None),
        exchange_updated_at_utc=now_utc.replace(tzinfo=None),
        tradable=True,
        data_source=DataSource.LIVE_API.value,
    )
    # 4. Synthetic generator row -> excluded
    r_synthetic = SnapshotRecord(
        ts=now_utc,
        platform="kalshi",
        ticker="KXTEST-1",
        observed_at_utc=now_utc.replace(tzinfo=None),
        exchange_updated_at_utc=now_utc.replace(tzinfo=None),
        tradable=True,
        data_source=DataSource.SYNTHETIC.value,
    )

    write_snapshots(conn, [r_valid, r_nontradable, r_unverified, r_synthetic])

    # Query research_snapshots
    research_rows = conn.execute("SELECT snapshot_id, ticker, pair_id FROM research_snapshots").fetchall()
    assert len(research_rows) == 1
    assert research_rows[0][0] == r_valid.snapshot_id
    assert research_rows[0][1] == "KXTEST-1"
    assert research_rows[0][2] == "p-test-1"


def test_log_exclusions_audit_trail(tmp_path: Path):
    db_file = tmp_path / "audit_test.duckdb"
    conn = connect(str(db_file))
    ensure_schema(conn)

    exclusions = [
        ("pair-1", "NO_ORDERBOOK", "Order book is empty"),
        ("pair-2", "EXPIRED", "Market closed"),
    ]
    logged = log_exclusions(conn, exclusions)
    assert logged == 2

    rows = conn.execute("SELECT pair_id, reason_code, detail FROM pair_exclusions").fetchall()
    assert len(rows) == 2
    assert rows[0] == ("pair-1", "NO_ORDERBOOK", "Order book is empty")
    assert rows[1] == ("pair-2", "EXPIRED", "Market closed")
