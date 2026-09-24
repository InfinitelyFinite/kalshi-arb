"""Unit tests for DataFeed and DuckDB orderbook snapshots."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest

from core.data_feed import DataFeed, MarketState
from kalshi_client.models import Market as KalshiMarket, Orderbook as KalshiOrderbook
from kalshi_client.ws import WSEvent
from polymarket_client.models import Market as PolyMarket, Orderbook as PolyOrderbook, Token


@pytest.fixture
def sample_kalshi_orderbook() -> KalshiOrderbook:
    return KalshiOrderbook.model_validate(
        {
            "ticker": "KXHIGHNY-26AUG27-T80",
            "yes_bids": [(45, 100), (40, 50)],
            "no_bids": [(52, 120), (50, 80)],
        }
    )


@pytest.fixture
def sample_poly_orderbook() -> PolyOrderbook:
    return PolyOrderbook.model_validate(
        {
            "ticker": "0x1234567890abcdef",
            "token_id": "0x1234567890abcdef",
            "condition_id": "0xcond123",
            "yes_bids": [(48, 200), (46, 150)],
            "yes_asks": [(52, 250), (55, 300)],
        }
    )


def test_market_state_properties():
    """Test MarketState spread and mid price properties."""
    state = MarketState(
        platform="kalshi",
        ticker="KXTEST-1",
        title="Test Kalshi Market",
        yes_bid=40,
        yes_ask=45,
        no_bid=55,
        no_ask=60,
        volume=1000,
    )
    assert state.platform == "kalshi"
    assert state.ticker == "KXTEST-1"
    assert state.yes_spread == 5
    assert state.no_spread == 5
    assert state.mid_price == 42.5

    partial_state = MarketState(
        platform="polymarket",
        ticker="0xtest",
        yes_bid=50,
        yes_ask=None,
    )
    assert partial_state.yes_spread is None
    assert partial_state.mid_price is None


@pytest.mark.asyncio
async def test_data_feed_initialization(sample_kalshi_orderbook, sample_poly_orderbook):
    """Test initializing DataFeed with custom mock clients."""
    mock_kalshi = AsyncMock()
    mock_poly = AsyncMock()

    feed = DataFeed(
        kalshi_client=mock_kalshi,
        polymarket_client=mock_poly,
        kalshi_tickers=["KXHIGHNY-26AUG27-T80"],
        polymarket_tokens=["0x1234567890abcdef"],
        snapshot_interval=30.0,
        poll_interval=5.0,
        enable_ws=False,
    )

    assert feed.kalshi_tickers == ["KXHIGHNY-26AUG27-T80"]
    assert feed.polymarket_tokens == ["0x1234567890abcdef"]
    assert feed.snapshot_interval == 30.0
    assert feed.poll_interval == 5.0
    assert feed.market_state == {}


@pytest.mark.asyncio
async def test_poll_kalshi(sample_kalshi_orderbook):
    """Test polling Kalshi populates in-memory market state with derived asks."""
    mock_kalshi = AsyncMock()
    mock_poly = AsyncMock()
    mock_kalshi.get_orderbooks.return_value = {
        sample_kalshi_orderbook.ticker: sample_kalshi_orderbook
    }

    feed = DataFeed(
        kalshi_client=mock_kalshi,
        polymarket_client=mock_poly,
        kalshi_tickers=[sample_kalshi_orderbook.ticker],
        enable_ws=False,
    )

    result = await feed.poll_kalshi()
    assert sample_kalshi_orderbook.ticker in result

    state = feed.get_market_state("kalshi", sample_kalshi_orderbook.ticker)
    assert state is not None
    assert state.platform == "kalshi"
    assert state.ticker == sample_kalshi_orderbook.ticker
    assert state.yes_bid == 45
    # best_yes_ask is derived: 100 - best_no_bid (100 - 52 = 48)
    assert state.yes_ask == 48
    assert state.no_bid == 52
    # best_no_ask is derived: 100 - best_yes_bid (100 - 45 = 55)
    assert state.no_ask == 55


@pytest.mark.asyncio
async def test_poll_polymarket(sample_poly_orderbook):
    """Test polling Polymarket populates in-memory market state."""
    mock_kalshi = AsyncMock()
    mock_poly = AsyncMock()
    mock_poly.get_orderbooks.return_value = {
        sample_poly_orderbook.token_id: sample_poly_orderbook
    }

    feed = DataFeed(
        kalshi_client=mock_kalshi,
        polymarket_client=mock_poly,
        polymarket_tokens=[sample_poly_orderbook.token_id],
        enable_ws=False,
    )

    result = await feed.poll_polymarket()
    assert sample_poly_orderbook.token_id in result

    state = feed.get_market_state("polymarket", sample_poly_orderbook.token_id)
    assert state is not None
    assert state.platform == "polymarket"
    assert state.ticker == sample_poly_orderbook.token_id
    assert state.yes_bid == 48
    assert state.yes_ask == 52
    assert state.no_bid == 48  # 100 - yes_ask (100 - 52 = 48)
    assert state.no_ask == 52  # 100 - yes_bid (100 - 48 = 52)


@pytest.mark.asyncio
async def test_poll_all_and_discover_markets(sample_kalshi_orderbook, sample_poly_orderbook):
    """Test discover_markets and poll_all concurrency."""
    mock_kalshi = AsyncMock()
    mock_poly = AsyncMock()

    # Mock list_markets for discovery
    k_market = KalshiMarket(
        ticker="KXAUTO-1",
        title="Auto Kalshi",
        volume=5000,
    )
    p_market = PolyMarket(
        condition_id="0xpolyauto",
        ticker="0xpolyauto",
        title="Auto Poly",
        volume=12000,
        tokens=[Token(token_id="tok-1", outcome="Yes", price=0.6)],
        clob_token_ids=["tok-1"],
    )

    mock_kalshi.list_markets.return_value = [k_market]
    mock_poly.list_markets.return_value = [p_market]

    k_ob = KalshiOrderbook.model_validate({"ticker": "KXAUTO-1", "yes_bids": [(60, 10)], "no_bids": [(38, 20)]})
    p_ob = PolyOrderbook.model_validate({"ticker": "tok-1", "token_id": "tok-1", "yes_bids": [(61, 50)], "yes_asks": [(63, 50)]})

    mock_kalshi.get_orderbooks.return_value = {"KXAUTO-1": k_ob}
    mock_poly.get_orderbooks.return_value = {"tok-1": p_ob}

    feed = DataFeed(
        kalshi_client=mock_kalshi,
        polymarket_client=mock_poly,
        enable_ws=False,
    )

    await feed.discover_markets()
    assert "KXAUTO-1" in feed.kalshi_tickers
    assert "tok-1" in feed.polymarket_tokens

    await feed.poll_all()

    k_state = feed.get_market_state("kalshi", "KXAUTO-1")
    p_state = feed.get_market_state("polymarket", "tok-1")

    assert k_state is not None
    assert k_state.yes_bid == 60
    assert k_state.volume == 5000

    assert p_state is not None
    assert p_state.yes_bid == 61
    assert p_state.volume == 12000


def test_snapshot_to_duckdb(tmp_path: Path):
    """Test snapshot_to_duckdb creates orderbook_snapshots table and inserts rows."""
    db_path = tmp_path / "test_snapshots.duckdb"
    feed = DataFeed(duckdb_path=db_path, enable_ws=False)

    # Manually populate state
    feed.update_market_state(
        platform="kalshi",
        ticker="KXTEST-NY",
        title="Kalshi Test Market",
        yes_bid=42,
        yes_ask=45,
        no_bid=55,
        no_ask=58,
        volume=1500,
    )
    feed.update_market_state(
        platform="polymarket",
        ticker="0xpoly123",
        title="Poly Test Market",
        yes_bid=43,
        yes_ask=46,
        no_bid=54,
        no_ask=57,
        volume=3200,
    )

    fixed_ts = datetime(2026, 9, 23, 14, 0, 0, tzinfo=timezone.utc)
    rows_written = feed.snapshot_to_duckdb(ts=fixed_ts)
    assert rows_written == 2

    # Query DuckDB directly using duckdb.sql to verify schema and contents
    con = duckdb.connect(str(db_path))
    res = con.sql("SELECT ts, platform, ticker, yes_bid, yes_ask, no_bid, no_ask, volume FROM orderbook_snapshots ORDER BY platform").fetchall()
    con.close()

    assert len(res) == 2

    # Kalshi row
    k_row = res[0]
    assert k_row[1] == "kalshi"
    assert k_row[2] == "KXTEST-NY"
    assert k_row[3] == 42
    assert k_row[4] == 45
    assert k_row[5] == 55
    assert k_row[6] == 58
    assert k_row[7] == 1500

    # Polymarket row
    p_row = res[1]
    assert p_row[1] == "polymarket"
    assert p_row[2] == "0xpoly123"
    assert p_row[3] == 43
    assert p_row[4] == 46
    assert p_row[5] == 54
    assert p_row[6] == 57
    assert p_row[7] == 3200


def test_multiple_snapshots_queryable(tmp_path: Path):
    """Test running multiple snapshot intervals produces timestamped rows queryable via duckdb.sql."""
    db_path = tmp_path / "multi_snapshots.duckdb"
    feed = DataFeed(duckdb_path=db_path, enable_ws=False)

    feed.update_market_state(platform="kalshi", ticker="KX1", yes_bid=50, yes_ask=52, volume=100)
    feed.update_market_state(platform="polymarket", ticker="POLY1", yes_bid=51, yes_ask=53, volume=200)

    # Snapshot 1
    t1 = datetime(2026, 9, 23, 14, 0, 0, tzinfo=timezone.utc)
    feed.snapshot_to_duckdb(ts=t1)

    # Update prices and snapshot 2
    feed.update_market_state(platform="kalshi", ticker="KX1", yes_bid=52, yes_ask=54, volume=150)
    t2 = datetime(2026, 9, 23, 14, 1, 0, tzinfo=timezone.utc)
    feed.snapshot_to_duckdb(ts=t2)

    # Snapshot 3
    t3 = datetime(2026, 9, 23, 14, 2, 0, tzinfo=timezone.utc)
    feed.snapshot_to_duckdb(ts=t3)

    # Snapshot 4
    t4 = datetime(2026, 9, 23, 14, 3, 0, tzinfo=timezone.utc)
    feed.snapshot_to_duckdb(ts=t4)

    # Snapshot 5
    t5 = datetime(2026, 9, 23, 14, 4, 0, tzinfo=timezone.utc)
    feed.snapshot_to_duckdb(ts=t5)

    # Query using duckdb
    conn = duckdb.connect(str(db_path))
    total_count = conn.sql("SELECT count(*) FROM orderbook_snapshots").fetchone()[0]
    counts_by_ticker = conn.sql(
        "SELECT ticker, count(*) as cnt FROM orderbook_snapshots GROUP BY ticker ORDER BY ticker"
    ).fetchall()
    conn.close()

    assert total_count == 10  # 2 markets * 5 snapshots
    assert counts_by_ticker == [("KX1", 5), ("POLY1", 5)]

    # Query helper method
    query_results = feed.query_snapshots("SELECT ticker, yes_bid FROM orderbook_snapshots WHERE ticker='KX1' ORDER BY ts")
    assert len(query_results) == 5
    assert query_results[0]["yes_bid"] == 50
    assert query_results[1]["yes_bid"] == 52


def test_kalshi_ws_event_handler(sample_kalshi_orderbook):
    """Test processing WebSocket events in DataFeed."""
    feed = DataFeed(kalshi_tickers=[sample_kalshi_orderbook.ticker], enable_ws=False)

    # 1. Orderbook snapshot event
    event = WSEvent(
        event_type="snapshot",
        ticker=sample_kalshi_orderbook.ticker,
        data=sample_kalshi_orderbook,
    )
    feed._handle_kalshi_ws_event(event)

    state = feed.get_market_state("kalshi", sample_kalshi_orderbook.ticker)
    assert state is not None
    assert state.yes_bid == 45
    assert state.yes_ask == 48

    # 2. Ticker event updating volume
    ticker_event = WSEvent(
        event_type="ticker",
        ticker=sample_kalshi_orderbook.ticker,
        data={"price_dollars": "0.46", "volume_fp": "8500"},
    )
    feed._handle_kalshi_ws_event(ticker_event)

    updated_state = feed.get_market_state("kalshi", sample_kalshi_orderbook.ticker)
    assert updated_state is not None
    assert updated_state.volume == 8500
    assert updated_state.yes_bid == 45  # Preserved


@pytest.mark.asyncio
async def test_start_stop_lifecycle(tmp_path: Path):
    """Test start and stop background loops cleanly."""
    mock_kalshi = AsyncMock()
    mock_poly = AsyncMock()

    mock_kalshi.get_orderbooks.return_value = {}
    mock_poly.get_orderbooks.return_value = {}

    db_path = tmp_path / "lifecycle.duckdb"
    feed = DataFeed(
        kalshi_client=mock_kalshi,
        polymarket_client=mock_poly,
        kalshi_tickers=["KXTEST"],
        polymarket_tokens=["POLYTEST"],
        duckdb_path=db_path,
        snapshot_interval=0.05,
        poll_interval=0.05,
        enable_ws=False,
    )

    feed.update_market_state("kalshi", "KXTEST", yes_bid=50, yes_ask=52)

    await feed.start()
    assert feed._running is True
    assert feed._poll_task is not None
    assert feed._snapshot_task is not None

    # Let background tasks run for a few cycles
    await asyncio.sleep(0.15)

    await feed.stop()
    assert feed._running is False
    assert feed._poll_task is None
    assert feed._snapshot_task is None

    # Check that snapshots were written to duckdb
    con = duckdb.connect(str(db_path))
    count = con.sql("SELECT count(*) FROM orderbook_snapshots").fetchone()[0]
    con.close()
    assert count >= 1


@pytest.mark.asyncio
async def test_poll_error_resilience():
    """Test that client exceptions during poll do not crash or corrupt previous state."""
    mock_kalshi = AsyncMock()
    mock_poly = AsyncMock()

    mock_kalshi.get_orderbooks.side_effect = RuntimeError("Kalshi 500 error")
    mock_poly.get_orderbooks.side_effect = RuntimeError("Polymarket network timeout")

    feed = DataFeed(
        kalshi_client=mock_kalshi,
        polymarket_client=mock_poly,
        kalshi_tickers=["KX1"],
        polymarket_tokens=["P1"],
        enable_ws=False,
    )

    # Pre-populate valid state
    feed.update_market_state("kalshi", "KX1", yes_bid=40, yes_ask=45)

    # Should not raise exception
    await feed.poll_all()

    # Previous state remains intact
    state = feed.get_market_state("kalshi", "KX1")
    assert state is not None
    assert state.yes_bid == 40
    assert state.yes_ask == 45
