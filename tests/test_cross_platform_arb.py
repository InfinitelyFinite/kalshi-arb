"""Unit tests for cross-platform arbitrage detection engine.

Verifies:
- Direction (a): Clear cross-platform edge (Buy Kalshi YES + Buy Polymarket NO).
- Direction (b): Clear cross-platform edge (Buy Polymarket YES + Buy Kalshi NO).
- No edge: Efficient markets where both directions sum to > 100 cents.
- DEFINITION OF DONE: Edge exists pre-fee but disappears post-fee, confirming that
  fee accounting (exchange taker fees + Polygon gas estimate) is actively applied,
  not merely decorative.
- Exact assertions on Opportunity fields: platform, ticker, target_platform, target_ticker,
  side_actions, legs, gross_edge_cents, estimated_fees_cents, edge_cents, and snapshot.
- Flexible input formats: dicts, Orderbook models, Market models, tuples, and MarketMatch.
- Gas estimate configurability and maker vs taker fee modes.
"""

import pytest

from core.config_loader import (
    AppConfig,
    KalshiConfig,
    MatchingConfig,
    ModeConfig,
    PolymarketConfig,
    RiskConfig,
    TradingConfig,
)
from core.cross_platform_arb import (
    calculate_cross_platform_fees,
    find_cross_platform_opportunities,
)
from core.market_matcher import MarketMatch
from core.models import Market, Opportunity, OpportunityLeg
from kalshi_client.models import Orderbook as KalshiOrderbook
from polymarket_client.models import Orderbook as PolyOrderbook


# ============================================================================
# Test Fixtures & Configurations
# ============================================================================


@pytest.fixture
def config_zero_fees() -> AppConfig:
    """Config with 0.0 taker fee, 0.0 gas, and 0.0 min_edge."""
    return AppConfig(
        mode=ModeConfig(),
        kalshi=KalshiConfig(),
        polymarket=PolymarketConfig(),
        trading=TradingConfig(
            min_edge=0.0,
            default_order_size=10,
            maker_fee_bps=0.0,
            taker_fee_bps=0.0,
            polygon_gas_estimate_cents=0.0,
        ),
        risk=RiskConfig(),
        matching=MatchingConfig(),
    )


@pytest.fixture
def config_realistic_fees() -> AppConfig:
    """Realistic config with 100 bps (1.0%) taker fee and 1.5c Polygon gas."""
    return AppConfig(
        mode=ModeConfig(),
        kalshi=KalshiConfig(),
        polymarket=PolymarketConfig(),
        trading=TradingConfig(
            min_edge=0.0,
            default_order_size=10,
            maker_fee_bps=20.0,
            taker_fee_bps=100.0,  # 1.0%
            polygon_gas_estimate_cents=1.5,  # 1.5 cents gas
        ),
        risk=RiskConfig(),
        matching=MatchingConfig(),
    )


# ============================================================================
# Direction (a) Test: Buy Kalshi YES + Buy Polymarket NO
# ============================================================================


def test_clear_cross_platform_edge_direction_a(config_realistic_fees):
    """Direction (a): Buy Kalshi YES (42c) + Buy Polymarket NO (46c).

    Cost = 42 + 46 = 88c.
    Gross edge = 100 - 88 = 12.0c.
    Fees:
      - Kalshi leg (taker 100 bps = 1.0%): 42 * 0.01 = 0.42c
      - Polymarket leg (100 bps + 1.5c gas): 46 * 0.01 + 1.5 = 1.96c
      - Total fees = 0.42 + 1.96 = 2.38c
    Net edge = 12.0 - 2.38 = 9.62c.

    Direction (b): Poly YES (56c) + Kalshi NO (60c) = 116c > 100c (no edge).
    """
    pair = MarketMatch(
        kalshi_ticker="KXFED-26",
        polymarket_condition_id="0xfed2026",
        kalshi_title="Fed rate cut in 2026",
        polymarket_title="Fed rate cut in 2026",
        confidence=1.0,
        match_source="override",
    )

    kalshi_books = {
        "KXFED-26": KalshiOrderbook(
            ticker="KXFED-26",
            yes_bids=[(40, 10)],  # Implied NO ask = 100 - 40 = 60c
            no_bids=[(58, 10)],   # Implied YES ask = 100 - 58 = 42c
        )
    }
    poly_books = {
        "0xfed2026": PolyOrderbook(
            ticker="0xfed2026",
            token_id="tok_fed",
            condition_id="0xfed2026",
            yes_asks=[(56, 10)],
            yes_bids=[(54, 10)],  # Implied NO ask = 100 - 54 = 46c
        )
    }

    opps = find_cross_platform_opportunities(
        matched_pairs=[pair],
        kalshi_orderbooks=kalshi_books,
        polymarket_orderbooks=poly_books,
        config=config_realistic_fees,
    )

    assert len(opps) == 1
    opp = opps[0]

    # Platform & Direction
    assert opp.platform == "kalshi"
    assert opp.ticker == "KXFED-26"
    assert opp.target_platform == "polymarket"
    assert opp.target_ticker == "0xfed2026"
    assert opp.strategy == "cross_platform"
    assert opp.side_actions == ["buy_yes", "buy_no"]

    # Precision math
    assert opp.gross_edge_cents == 12.0
    assert opp.estimated_fees_cents == 2.38
    assert opp.edge_cents == 9.62

    # Legs verification
    assert len(opp.legs) == 2
    leg_k, leg_p = opp.legs[0], opp.legs[1]

    assert leg_k.platform == "kalshi"
    assert leg_k.ticker == "KXFED-26"
    assert leg_k.side == "buy_yes"
    assert leg_k.price_cents == 42.0
    assert leg_k.fee_cents == 0.42

    assert leg_p.platform == "polymarket"
    assert leg_p.ticker == "0xfed2026"
    assert leg_p.side == "buy_no"
    assert leg_p.price_cents == 46.0
    assert leg_p.fee_cents == 1.96

    # Snapshot inspection
    snapshot = opp.orderbook_snapshot
    assert snapshot["direction"] == "kalshi_yes_polymarket_no"
    assert snapshot["cost_cents"] == 88.0
    assert snapshot["gross_edge_cents"] == 12.0
    assert snapshot["net_edge_cents"] == 9.62


# ============================================================================
# Direction (b) Test: Buy Polymarket YES + Buy Kalshi NO
# ============================================================================


def test_clear_cross_platform_edge_direction_b(config_realistic_fees):
    """Direction (b): Buy Polymarket YES (43c) + Buy Kalshi NO (41c).

    Cost = 43 + 41 = 84c.
    Gross edge = 100 - 84 = 16.0c.
    Fees:
      - Kalshi leg (100 bps): 41 * 0.01 = 0.41c
      - Polymarket leg (100 bps + 1.5c gas): 43 * 0.01 + 1.5 = 1.93c
      - Total fees = 0.41 + 1.93 = 2.34c
    Net edge = 16.0 - 2.34 = 13.66c.

    Direction (a): Kalshi YES (62c) + Poly NO (61c) = 123c > 100c (no edge).
    """
    pair = MarketMatch(
        kalshi_ticker="KXBTC-100K",
        polymarket_condition_id="0xbtc100k",
        kalshi_title="Bitcoin above $100k",
        polymarket_title="Bitcoin above $100k",
        confidence=1.0,
        match_source="override",
    )

    kalshi_books = {
        "KXBTC-100K": KalshiOrderbook(
            ticker="KXBTC-100K",
            yes_bids=[(59, 10)],  # Implied NO ask = 100 - 59 = 41c
            no_bids=[(38, 10)],   # Implied YES ask = 100 - 38 = 62c
        )
    }
    poly_books = {
        "0xbtc100k": PolyOrderbook(
            ticker="0xbtc100k",
            token_id="tok_btc",
            condition_id="0xbtc100k",
            yes_asks=[(43, 10)],
            yes_bids=[(39, 10)],  # Implied NO ask = 100 - 39 = 61c
        )
    }

    opps = find_cross_platform_opportunities(
        matched_pairs=[pair],
        kalshi_orderbooks=kalshi_books,
        polymarket_orderbooks=poly_books,
        config=config_realistic_fees,
    )

    assert len(opps) == 1
    opp = opps[0]

    # Platform & Direction
    assert opp.platform == "polymarket"
    assert opp.ticker == "0xbtc100k"
    assert opp.target_platform == "kalshi"
    assert opp.target_ticker == "KXBTC-100K"
    assert opp.strategy == "cross_platform"

    # Math
    assert opp.gross_edge_cents == 16.0
    assert opp.estimated_fees_cents == 2.34
    assert opp.edge_cents == 13.66

    # Legs
    assert len(opp.legs) == 2
    leg_p, leg_k = opp.legs[0], opp.legs[1]

    assert leg_p.platform == "polymarket"
    assert leg_p.ticker == "0xbtc100k"
    assert leg_p.side == "buy_yes"
    assert leg_p.price_cents == 43.0
    assert leg_p.fee_cents == 1.93

    assert leg_k.platform == "kalshi"
    assert leg_k.ticker == "KXBTC-100K"
    assert leg_k.side == "buy_no"
    assert leg_k.price_cents == 41.0
    assert leg_k.fee_cents == 0.41


# ============================================================================
# No Edge Test: Efficient Markets
# ============================================================================


def test_no_edge_when_markets_efficient(config_zero_fees):
    """When both platforms have prices summing to > 100 cents, no opportunity is emitted.

    Kalshi: YES ask = 52c, NO ask = 50c
    Polymarket: YES ask = 51c, NO ask = 51c
    Direction (a): Kalshi YES (52c) + Poly NO (51c) = 103c > 100c
    Direction (b): Poly YES (51c) + Kalshi NO (50c) = 101c > 100c
    """
    pair = ("KXEFF-1", "0xeff1")
    kalshi_books = {
        "KXEFF-1": {"yes_ask": 52, "no_ask": 50}
    }
    poly_books = {
        "0xeff1": {"yes_ask": 51, "no_ask": 51}
    }

    opps = find_cross_platform_opportunities(
        matched_pairs=[pair],
        kalshi_orderbooks=kalshi_books,
        polymarket_orderbooks=poly_books,
        config=config_zero_fees,
    )
    assert opps == []


# ============================================================================
# DEFINITION OF DONE: Edge Exists Pre-Fee but Disappears Post-Fee
# ============================================================================


def test_definition_of_done_edge_disappears_post_fee(config_zero_fees, config_realistic_fees):
    """DEFINITION OF DONE TEST:
    Confirms fee accounting is wired into execution logic, not just decorative.

    Market setup:
      Kalshi:     YES ask = 49c, NO ask = 54c
      Polymarket: YES ask = 54c, NO ask = 49c

    Direction (a):
      Cost = Kalshi YES (49c) + Polymarket NO (49c) = 98c.
      Gross edge = 100 - 98 = +2.0 cents (pre-fee edge exists!).

    Under Zero-Fee Model:
      Fees = 0.0c -> Net Edge = +2.0c -> Opportunity IS emitted.

    Under Realistic Fee Model:
      Taker fee = 100 bps (1.0%), Gas = 1.5c
      Kalshi fee: 49 * 0.01 = 0.49c
      Polymarket fee: 49 * 0.01 + 1.5 = 1.99c
      Total fees = 0.49 + 1.99 = 2.48c
      Cost + Fees = 98 + 2.48 = 100.48c > 100c (Net Edge = -0.48c < 0).
      Cost < 100c - Fees is FALSE (98 < 97.52 is False).
      Opportunity DISAPPEARS post-fee!
    """
    pair = MarketMatch(
        kalshi_ticker="KXFEE-TEST",
        polymarket_condition_id="0xfeetest",
        kalshi_title="Fee Test Event",
        polymarket_title="Fee Test Event",
        confidence=1.0,
        match_source="override",
    )

    kalshi_books = {
        "KXFEE-TEST": {"yes_ask": 49, "no_ask": 54}
    }
    poly_books = {
        "0xfeetest": {"yes_ask": 54, "no_ask": 49}
    }

    # Step 1: Pre-fee check (zero fees) -> Confirms theoretical price gap of 2 cents is found
    pre_fee_opps = find_cross_platform_opportunities(
        matched_pairs=[pair],
        kalshi_orderbooks=kalshi_books,
        polymarket_orderbooks=poly_books,
        config=config_zero_fees,
    )
    assert len(pre_fee_opps) == 1, "Pre-fee edge of 2.0c MUST be detected when fees are 0"
    assert pre_fee_opps[0].gross_edge_cents == 2.0
    assert pre_fee_opps[0].estimated_fees_cents == 0.0
    assert pre_fee_opps[0].edge_cents == 2.0

    # Step 2: Post-fee check (realistic fees) -> Confirms edge disappears due to fee accounting
    post_fee_opps = find_cross_platform_opportunities(
        matched_pairs=[pair],
        kalshi_orderbooks=kalshi_books,
        polymarket_orderbooks=poly_books,
        config=config_realistic_fees,
    )
    assert len(post_fee_opps) == 0, (
        "Opportunity MUST disappear post-fee because total fees (2.48c) exceed gross edge (2.0c)"
    )


# ============================================================================
# Additional Comprehensive Tests
# ============================================================================


def test_polygon_gas_estimate_override():
    """Verify that configurable polygon_gas_estimate_cents parameter dynamically shifts edge."""
    pair = ("KXGAS-1", "0xgas1")
    kalshi_books = {"KXGAS-1": {"yes_ask": 45, "no_ask": 58}}
    poly_books = {"0xgas1": {"yes_ask": 58, "no_ask": 48}}

    # Cost = 45 + 48 = 93c. Gross edge = 7.0c. 0 trading fees.
    base_config = AppConfig(
        mode=ModeConfig(),
        kalshi=KalshiConfig(),
        polymarket=PolymarketConfig(),
        trading=TradingConfig(min_edge=0.0, maker_fee_bps=0.0, taker_fee_bps=0.0),
        risk=RiskConfig(),
        matching=MatchingConfig(),
    )

    # Low gas (0.5c) -> Net edge = 7.0 - 0.5 = 6.5c
    opps_low = find_cross_platform_opportunities(
        [pair], kalshi_books, poly_books,
        config=base_config,
        polygon_gas_estimate_cents=0.5,
    )
    assert len(opps_low) == 1
    assert opps_low[0].edge_cents == 6.5
    assert opps_low[0].legs[1].fee_cents == 0.5

    # High gas (8.0c) -> Total cost + fee = 93 + 8 = 101c > 100c -> Disappears
    opps_high = find_cross_platform_opportunities(
        [pair], kalshi_books, poly_books,
        config=base_config,
        polygon_gas_estimate_cents=8.0,
    )
    assert len(opps_high) == 0


def test_maker_vs_taker_fee_modes():
    """Verify that is_maker=True switches to config.trading.maker_fee_bps."""
    pair = ("KXMAKER-1", "0xmaker1")
    kalshi_books = {"KXMAKER-1": {"yes_ask": 45, "no_ask": 58}}
    poly_books = {"0xmaker1": {"yes_ask": 58, "no_ask": 45}}

    # Cost = 90c, gross edge = 10c
    config = AppConfig(
        mode=ModeConfig(),
        kalshi=KalshiConfig(),
        polymarket=PolymarketConfig(),
        trading=TradingConfig(
            min_edge=0.0,
            maker_fee_bps=10.0,   # 0.1%
            taker_fee_bps=200.0,  # 2.0%
            polygon_gas_estimate_cents=0.0,
        ),
        risk=RiskConfig(),
        matching=MatchingConfig(),
    )

    # Taker mode: fee rate = 2.0% -> Kalshi = 0.90c, Poly = 0.90c -> Total fee = 1.80c
    opps_taker = find_cross_platform_opportunities(
        [pair], kalshi_books, poly_books, config=config, is_maker=False
    )
    assert opps_taker[0].estimated_fees_cents == 1.80
    assert opps_taker[0].edge_cents == 8.20

    # Maker mode: fee rate = 0.1% -> Kalshi = 0.045c, Poly = 0.045c -> Total fee = 0.09c
    opps_maker = find_cross_platform_opportunities(
        [pair], kalshi_books, poly_books, config=config, is_maker=True
    )
    assert opps_maker[0].estimated_fees_cents == 0.09
    assert opps_maker[0].edge_cents == 9.91


def test_orderbooks_passed_as_list():
    """Verify that orderbooks can be passed as lists of objects instead of dicts."""
    pair = MarketMatch(
        kalshi_ticker="KXLIST-1",
        polymarket_condition_id="0xlist1",
        kalshi_title="List test",
        polymarket_title="List test",
        confidence=1.0,
        match_source="override",
    )

    k_market = Market(platform="kalshi", ticker="KXLIST-1", yes_ask=40, no_ask=65)
    p_market = Market(platform="polymarket", ticker="0xlist1", yes_ask=65, no_ask=45)

    opps = find_cross_platform_opportunities(
        matched_pairs=[pair],
        kalshi_orderbooks=[k_market],
        polymarket_orderbooks=[p_market],
        min_edge=0.0,
    )
    assert len(opps) == 1
    assert opps[0].platform == "kalshi"
    assert opps[0].gross_edge_cents == 15.0  # 100 - (40 + 45)


def test_missing_or_closed_books_handled_gracefully(config_zero_fees):
    """Verify that missing books, None asks, or closed markets are skipped cleanly."""
    closed_market = Market(platform="kalshi", ticker="KXCLOSED", yes_ask=40, no_ask=40, status="closed")
    active_market = Market(platform="polymarket", ticker="0xactive", yes_ask=40, no_ask=40)

    # 1. Closed market
    opps_closed = find_cross_platform_opportunities(
        [("KXCLOSED", "0xactive")],
        {"KXCLOSED": closed_market},
        {"0xactive": active_market},
        config=config_zero_fees,
    )
    assert opps_closed == []

    # 2. Missing from dict
    opps_missing = find_cross_platform_opportunities(
        [("KXMISSING", "0xactive")],
        {},
        {"0xactive": active_market},
        config=config_zero_fees,
    )
    assert opps_missing == []

    # 3. None asks
    empty_book = {"yes_ask": None, "no_ask": None}
    opps_none = find_cross_platform_opportunities(
        [("KXEMPTY", "0xactive")],
        {"KXEMPTY": empty_book},
        {"0xactive": active_market},
        config=config_zero_fees,
    )
    assert opps_none == []
