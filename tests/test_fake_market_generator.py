"""Unit and integration tests for FakeMarketGenerator and simulation mode.

Verifies:
1. FakeMarketGenerator interface compatibility with real Kalshi and Polymarket clients.
2. Synthetic Market and Orderbook models structure and derived properties.
3. Realistic random walk dynamics within configured price bounds [5, 95] cents.
4. Deliberate intra-market mispricing injections and detection by Step 7 engine.
5. Deliberate cross-platform divergence injections and detection by Step 8 engine.
6. Efficient market invariant: normal ticks produce zero spurious arbitrage opportunities.
7. Zero network calls made when data_mode == 'simulation'.
8. End-to-end pipeline integration from simulation through market matching and arbitrage detection.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch
import pytest

from core.config_loader import (
    AppConfig,
    DataMode,
    KalshiConfig,
    MatchingConfig,
    ModeConfig,
    PolymarketConfig,
    RiskConfig,
    TradingConfig,
    TradingMode,
)
from core.cross_platform_arb import find_cross_platform_opportunities
from core.data_feed import DataFeed
from core.intra_market_arb import find_intra_market_opportunities
from core.market_matcher import MarketMatcher
from kalshi_client.models import Market as KalshiMarket, Orderbook as KalshiOrderbook
from main import run_pipeline
from polymarket_client.models import Market as PolyMarket, Orderbook as PolyOrderbook
from simulation.fake_market_generator import (
    DEFAULT_SYNTHETIC_PAIRS,
    FakeKalshiClient,
    FakeMarketGenerator,
    FakePolymarketClient,
    SyntheticMarketConfig,
)


@pytest.fixture
def sim_config() -> AppConfig:
    """AppConfig configured in simulation mode with 0 fees for exact arithmetic."""
    return AppConfig(
        mode=ModeConfig(
            trading_mode=TradingMode.DRY_RUN,
            data_mode=DataMode.SIMULATION,
        ),
        kalshi=KalshiConfig(),
        polymarket=PolymarketConfig(),
        trading=TradingConfig(
            min_edge=0.02,  # 2.0 cents min edge
            default_order_size=10,
            maker_fee_bps=0.0,
            taker_fee_bps=0.0,
            polygon_gas_estimate_cents=0.0,
        ),
        risk=RiskConfig(),
        matching=MatchingConfig(min_similarity=0.85),
    )


@pytest.fixture
def fake_generator() -> FakeMarketGenerator:
    """Standard FakeMarketGenerator instance with deterministic seed."""
    return FakeMarketGenerator(mispricing_interval=5, auto_tick=False, seed=42)


# ============================================================================
# 1. Generator Initialization & Interface Tests
# ============================================================================


def test_generator_initialization(fake_generator: FakeMarketGenerator) -> None:
    """Verify generator initializes synthetic pairs and internal states correctly."""
    assert len(fake_generator._states) == len(DEFAULT_SYNTHETIC_PAIRS)
    assert fake_generator.tick_count == 0
    assert fake_generator.last_injected_info is None

    # Check that initial prices stay within bounds
    for s in fake_generator._states:
        assert 5 <= s.kalshi_price <= 95
        assert 5 <= s.poly_price <= 95
        assert s.kalshi_volume > 0
        assert s.poly_volume > 0


@pytest.mark.asyncio
async def test_client_interface_list_markets(fake_generator: FakeMarketGenerator) -> None:
    """Verify list_markets() returns typed Kalshi and Polymarket market models."""
    k_markets = await fake_generator.kalshi_client.list_markets()
    p_markets = await fake_generator.polymarket_client.list_markets()

    assert len(k_markets) == len(DEFAULT_SYNTHETIC_PAIRS)
    assert len(p_markets) == len(DEFAULT_SYNTHETIC_PAIRS)

    for km in k_markets:
        assert isinstance(km, KalshiMarket)
        assert km.ticker != ""
        assert km.title != ""
        assert km.status == "open"
        assert km.yes_bid is not None
        assert km.yes_ask is not None

    for pm in p_markets:
        assert isinstance(pm, PolyMarket)
        assert pm.condition_id != ""
        assert pm.title != ""
        assert pm.status == "open"
        assert pm.yes_token_id is not None
        assert pm.yes_ask is not None


@pytest.mark.asyncio
async def test_client_interface_get_orderbooks(fake_generator: FakeMarketGenerator) -> None:
    """Verify get_orderbooks() returns properly structured Orderbook instances."""
    k_tickers = [cfg.kalshi_ticker for cfg in DEFAULT_SYNTHETIC_PAIRS]
    p_tokens = [cfg.poly_token_id for cfg in DEFAULT_SYNTHETIC_PAIRS]

    k_books = await fake_generator.kalshi_client.get_orderbooks(k_tickers)
    p_books = await fake_generator.polymarket_client.get_orderbooks(p_tokens)

    assert len(k_books) == len(k_tickers)
    assert len(p_books) >= len(p_tokens)

    for ticker, ob in k_books.items():
        assert isinstance(ob, KalshiOrderbook)
        assert ob.ticker == ticker
        assert len(ob.yes_bids) > 0
        assert len(ob.no_bids) > 0
        assert ob.best_yes_bid is not None
        assert ob.best_yes_ask is not None
        assert ob.best_no_bid is not None
        assert ob.best_no_ask is not None

    for token_id, ob in p_books.items():
        assert isinstance(ob, PolyOrderbook)
        assert ob.best_yes_bid is not None
        assert ob.best_yes_ask is not None
        assert ob.best_no_bid is not None
        assert ob.best_no_ask is not None


# ============================================================================
# 2. Random Walk Dynamics & Bounds Tests
# ============================================================================


def test_random_walk_bounds(fake_generator: FakeMarketGenerator) -> None:
    """Verify that random walk price steps fluctuate but remain strictly clamped within bounds."""
    initial_prices = [s.kalshi_price for s in fake_generator._states]

    # Advance 30 ticks with mispricing interval high so only normal random walk runs
    fake_generator.mispricing_interval = 100
    for _ in range(30):
        fake_generator.step()
        for s in fake_generator._states:
            assert 5 <= s.kalshi_price <= 95
            assert 5 <= s.poly_price <= 95

    # Confirm prices have actually fluctuated (not stuck static)
    current_prices = [s.kalshi_price for s in fake_generator._states]
    assert current_prices != initial_prices


def test_normal_ticks_produce_no_false_positive_arbitrage(
    fake_generator: FakeMarketGenerator,
    sim_config: AppConfig,
) -> None:
    """Verify that on normal ticks, efficient pricing invariants hold (no spurious arb)."""
    fake_generator.reset_injections()

    k_markets = fake_generator.get_kalshi_markets()
    p_markets = fake_generator.get_polymarket_markets()
    all_markets = k_markets + p_markets

    # Intra-market scan should find zero opportunities
    intra_opps = find_intra_market_opportunities(all_markets, config=sim_config)
    assert len(intra_opps) == 0

    # Cross-platform scan should also find zero opportunities
    matcher = MarketMatcher.from_config(sim_config)
    matches = matcher.match(k_markets, p_markets)
    k_books = fake_generator.get_kalshi_orderbooks()
    p_books = fake_generator.get_polymarket_orderbooks()

    cross_opps = find_cross_platform_opportunities(
        matches, k_books, p_books, config=sim_config
    )
    assert len(cross_opps) == 0


# ============================================================================
# 3. Deliberate Injected Mispricing Detection Tests (Step 7 & Step 8)
# ============================================================================


def test_injected_intra_market_mispricing_detection(
    fake_generator: FakeMarketGenerator,
    sim_config: AppConfig,
) -> None:
    """Confirm injected intra-market mispricing is detected by the Step 7 engine."""
    # 1. Inject on Kalshi: Gross cost = 92¢ -> 8¢ gross edge
    info = fake_generator.inject_intra_market_mispricing(
        platform="kalshi", pair_index=0, gross_cost_cents=92
    )
    assert info["type"] == "intra_market"
    assert info["gross_edge"] == 8

    k_markets = fake_generator.get_kalshi_markets()
    opps = find_intra_market_opportunities(k_markets, config=sim_config)

    assert len(opps) == 1
    opp = opps[0]
    assert opp.platform == "kalshi"
    assert opp.ticker == info["ticker"]
    assert opp.edge_cents == 8.0
    assert opp.side_actions == ["buy_yes", "buy_no"]
    assert opp.orderbook_snapshot["yes_ask"] == info["yes_ask"]
    assert opp.orderbook_snapshot["no_ask"] == info["no_ask"]

    # 2. Inject on Polymarket: Gross cost = 90¢ -> 10¢ gross edge
    fake_generator.inject_intra_market_mispricing(
        platform="polymarket", pair_index=1, gross_cost_cents=90
    )
    p_markets = fake_generator.get_polymarket_markets()
    opps_poly = find_intra_market_opportunities(p_markets, config=sim_config)

    assert len(opps_poly) == 1
    assert opps_poly[0].platform == "polymarket"
    assert opps_poly[0].edge_cents == 10.0


def test_injected_cross_platform_mispricing_detection(
    fake_generator: FakeMarketGenerator,
    sim_config: AppConfig,
) -> None:
    """Confirm injected cross-platform divergence is detected by the Step 8 engine."""
    # Inject Direction (a): Kalshi YES + Poly NO = 90¢ -> 10¢ edge
    info = fake_generator.inject_cross_platform_mispricing(
        direction="kalshi_yes_poly_no", pair_index=0, gross_cost_cents=90
    )
    assert info["type"] == "cross_platform"
    assert info["gross_edge"] == 10

    matcher = MarketMatcher.from_config(sim_config)
    k_markets = fake_generator.get_kalshi_markets()
    p_markets = fake_generator.get_polymarket_markets()
    matches = matcher.match(k_markets, p_markets)

    k_books = fake_generator.get_kalshi_orderbooks()
    p_books = fake_generator.get_polymarket_orderbooks()

    cross_opps = find_cross_platform_opportunities(
        matched_pairs=matches,
        kalshi_orderbooks=k_books,
        polymarket_orderbooks=p_books,
        config=sim_config,
    )

    assert len(cross_opps) >= 1
    # Find matching opportunity
    target_opp = next(
        (o for o in cross_opps if o.ticker == info["kalshi_ticker"]), None
    )
    assert target_opp is not None
    assert target_opp.strategy == "cross_platform"
    assert target_opp.edge_cents == 10.0
    assert len(target_opp.legs) == 2
    assert target_opp.legs[0].platform == "kalshi"
    assert target_opp.legs[0].side == "buy_yes"
    assert target_opp.legs[1].platform == "polymarket"
    assert target_opp.legs[1].side == "buy_no"

    # Test Direction (b): Poly YES + Kalshi NO = 88¢ -> 12¢ edge
    info_b = fake_generator.inject_cross_platform_mispricing(
        direction="poly_yes_kalshi_no", pair_index=2, gross_cost_cents=88
    )
    k_books_b = fake_generator.get_kalshi_orderbooks()
    p_books_b = fake_generator.get_polymarket_orderbooks()

    cross_opps_b = find_cross_platform_opportunities(
        matched_pairs=matches,
        kalshi_orderbooks=k_books_b,
        polymarket_orderbooks=p_books_b,
        config=sim_config,
    )

    target_opp_b = next(
        (
            o
            for o in cross_opps_b
            if o.ticker == info_b["kalshi_ticker"]
            or o.target_ticker == info_b["kalshi_ticker"]
        ),
        None,
    )
    assert target_opp_b is not None
    assert target_opp_b.edge_cents == 12.0
    assert target_opp_b.legs[0].platform == "polymarket"
    assert target_opp_b.legs[0].side == "buy_yes"
    assert target_opp_b.legs[1].platform == "kalshi"
    assert target_opp_b.legs[1].side == "buy_no"


# ============================================================================
# 4. DataFeed Wiring & Zero Network Call Verification
# ============================================================================


@pytest.mark.asyncio
async def test_datafeed_simulation_wiring_zero_network_calls(
    sim_config: AppConfig, tmp_path: Path
) -> None:
    """Verify DataFeed automatically activates fake generator in simulation mode with 0 network calls."""
    db_file = tmp_path / "sim_snapshots.duckdb"

    # Patch httpx.AsyncClient.request to ensure NO outgoing HTTP requests occur
    with patch(
        "httpx.AsyncClient.request",
        side_effect=RuntimeError("Real network call attempted in simulation mode!"),
    ):
        feed = DataFeed(
            config=sim_config,
            duckdb_path=str(db_file),
            poll_interval=1.0,
            snapshot_interval=5.0,
        )

        assert feed.is_simulation is True
        assert feed.enable_ws is False
        assert feed.fake_generator is not None

        # Verify market auto-discovery and initial polling execute strictly in-memory
        await feed.discover_markets()
        assert len(feed.kalshi_tickers) > 0
        assert len(feed.polymarket_tokens) > 0

        await feed.poll_all()
        assert len(feed.market_state) == len(feed.kalshi_tickers) + len(feed.polymarket_tokens)

        # Confirm DuckDB snapshots record properly
        rows_written = feed.snapshot_to_duckdb()
        assert rows_written > 0

        records = feed.query_snapshots("SELECT count(*) as cnt FROM orderbook_snapshots")
        assert records[0]["cnt"] == rows_written

        await feed.close()


# ============================================================================
# 5. End-to-End Pipeline & Periodic Injections
# ============================================================================


@pytest.mark.asyncio
async def test_end_to_end_simulation_to_detection_integration(
    sim_config: AppConfig, tmp_path: Path
) -> None:
    """Integration test: advances multi-tick simulation and verifies detection pipeline finds mispricings."""
    db_file = tmp_path / "integration_pipeline.duckdb"

    # Generator with mispricing every 2 ticks
    gen = FakeMarketGenerator(mispricing_interval=2, auto_tick=True, seed=123)

    feed = DataFeed(
        config=sim_config,
        fake_generator=gen,
        duckdb_path=str(db_file),
        poll_interval=0.5,
    )

    matcher = MarketMatcher.from_config(sim_config)

    detected_intra: list[Any] = []
    detected_cross: list[Any] = []

    await feed.discover_markets()

    # Simulate 8 polling cycles
    for cycle in range(8):
        await feed.poll_all()
        all_states = list(feed.market_state.values())

        # Step 7 scan
        intra = find_intra_market_opportunities(all_states, config=sim_config)
        detected_intra.extend(intra)

        # Step 8 scan
        k_states = [s for s in all_states if s.platform == "kalshi"]
        p_states = [s for s in all_states if s.platform == "polymarket"]
        matches = matcher.match(k_states, p_states)
        k_books = {s.ticker: s.raw_orderbook for s in k_states if s.raw_orderbook}
        p_books = {s.ticker: s.raw_orderbook for s in p_states if s.raw_orderbook}

        cross = find_cross_platform_opportunities(
            matches, k_books, p_books, config=sim_config
        )
        detected_cross.extend(cross)

    await feed.close()

    # Confirm that over 8 cycles, both intra-market and cross-platform opportunities were detected
    assert len(detected_intra) > 0, "Expected at least 1 intra-market opportunity"
    assert len(detected_cross) > 0, "Expected at least 1 cross-platform opportunity"

    for opp in detected_intra:
        assert opp.strategy == "intra_market"
        assert opp.edge_cents >= 2.0  # Above min_edge

    for opp in detected_cross:
        assert opp.strategy == "cross_platform"
        assert opp.edge_cents >= 2.0  # Above min_edge


@pytest.mark.asyncio
async def test_main_pipeline_execution(tmp_path: Path) -> None:
    """Verify that main.run_pipeline() runs smoothly in simulation mode."""
    db_file = tmp_path / "test_main.duckdb"
    code = await run_pipeline(
        duration=3.0,
        poll_interval=0.8,
        duckdb_path=str(db_file),
    )
    assert code == 0
    assert db_file.exists()
