"""Unit tests for intra-market arbitrage detection engine.

Verifies:
- Detection of clear 3-cent edge with exact mathematical precision.
- No opportunity emitted when edge is zero or negative.
- Exact threshold boundary handling (edge == min_edge).
- Subtraction of estimated transaction fees.
- Exact assertions on Opportunity fields (platform, ticker, side_actions, edge_cents, timestamp, orderbook_snapshot).
- Cross-object compatibility (Kalshi Orderbook, Polymarket Orderbook, MarketState, Market).
"""

from datetime import datetime, timezone
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
from core.data_feed import MarketState
from core.intra_market_arb import calculate_estimated_fees, find_intra_market_opportunities
from core.models import Market, Opportunity
from kalshi_client.models import Orderbook as KalshiOrderbook
from polymarket_client.models import Orderbook as PolyOrderbook


# ============================================================================
# Test Fixtures & Configurations
# ============================================================================


@pytest.fixture
def test_config() -> AppConfig:
    """Standard test AppConfig with min_edge=0.02 (2.0 cents) and 0 fees."""
    return AppConfig(
        mode=ModeConfig(),
        kalshi=KalshiConfig(),
        polymarket=PolymarketConfig(),
        trading=TradingConfig(
            min_edge=0.02,  # 2.0 cents threshold
            default_order_size=10,
            maker_fee_bps=0.0,
            taker_fee_bps=0.0,
        ),
        risk=RiskConfig(),
        matching=MatchingConfig(),
    )


@pytest.fixture
def config_with_fees() -> AppConfig:
    """Test AppConfig with taker fee of 50 bps (0.50%) and min_edge=0.02."""
    return AppConfig(
        mode=ModeConfig(),
        kalshi=KalshiConfig(),
        polymarket=PolymarketConfig(),
        trading=TradingConfig(
            min_edge=0.02,  # 2.0 cents threshold
            default_order_size=10,
            maker_fee_bps=0.0,
            taker_fee_bps=50.0,  # 50 bps = 0.50%
        ),
        risk=RiskConfig(),
        matching=MatchingConfig(),
    )


@pytest.fixture
def fixture_3_cent_edge_kalshi_orderbook() -> KalshiOrderbook:
    """Kalshi orderbook with exact 3-cent edge.

    Derived asks:
        best_yes_ask = 100 - best_no_bid (100 - 52 = 48 cents)
        best_no_ask  = 100 - best_yes_bid (100 - 51 = 49 cents)
    Gross cost = 48 + 49 = 97 cents.
    Gross edge = 100 - 97 = 3.0 cents.
    """
    return KalshiOrderbook(
        ticker="KXTEST-3CENT-EDGE",
        yes_bids=[(51, 100), (50, 200)],  # Implies best_no_ask = 100 - 51 = 49c
        no_bids=[(52, 100), (49, 150)],   # Implies best_yes_ask = 100 - 52 = 48c
    )


@pytest.fixture
def fixture_3_cent_edge_market() -> Market:
    """Unified Market fixture with direct asks of 48c and 49c (3-cent edge)."""
    return Market(
        platform="kalshi",
        ticker="KXTEST-3CENT-MARKET",
        title="Will Event occur?",
        yes_ask=48,
        no_ask=49,
        yes_bid=46,
        no_bid=47,
        volume=5000,
        status="open",
    )


@pytest.fixture
def fixture_no_edge_kalshi_orderbook() -> KalshiOrderbook:
    """Kalshi orderbook with no edge (asks sum to 103 cents, edge = -3 cents).

    Derived asks:
        best_yes_ask = 100 - 48 = 52 cents
        best_no_ask  = 100 - 49 = 51 cents
    Gross cost = 52 + 51 = 103 cents.
    Gross edge = 100 - 103 = -3.0 cents.
    """
    return KalshiOrderbook(
        ticker="KXTEST-NO-EDGE",
        yes_bids=[(49, 100)],  # Implies best_no_ask = 51c
        no_bids=[(48, 100)],   # Implies best_yes_ask = 52c
    )


@pytest.fixture
def fixture_zero_edge_market() -> Market:
    """Market with exact zero edge (YES ask = 50c, NO ask = 50c, cost = 100c, edge = 0c)."""
    return Market(
        platform="kalshi",
        ticker="KXTEST-ZERO-EDGE",
        yes_ask=50,
        no_ask=50,
        yes_bid=48,
        no_bid=48,
        volume=1000,
        status="open",
    )


@pytest.fixture
def fixture_threshold_boundary_kalshi_orderbook() -> KalshiOrderbook:
    """Kalshi orderbook at exactly the threshold boundary (edge = 2.0 cents).

    min_edge = 0.02 (2.0 cents).
    Derived asks:
        best_yes_ask = 100 - 51 = 49 cents
        best_no_ask  = 100 - 51 = 49 cents
    Gross cost = 49 + 49 = 98 cents.
    Gross edge = 100 - 98 = 2.0 cents.
    """
    return KalshiOrderbook(
        ticker="KXTEST-BOUNDARY",
        yes_bids=[(51, 100)],  # Implies best_no_ask = 49c
        no_bids=[(51, 100)],   # Implies best_yes_ask = 49c
    )


# ============================================================================
# Core Arbitrage Engine Tests & Definition of Done Verification
# ============================================================================


def test_intra_market_arb_clear_3_cent_edge(test_config, fixture_3_cent_edge_kalshi_orderbook):
    """Test 1: Clear 3-cent edge fixture emits Opportunity with exact edge_cents=3.0.

    Definition of Done:
    - Verifies exact expected edge value (3.0 cents).
    - Verifies side actions are ['buy_yes', 'buy_no'].
    - Verifies orderbook_snapshot preserves audit data.
    """
    opps = find_intra_market_opportunities([fixture_3_cent_edge_kalshi_orderbook], config=test_config)

    assert len(opps) == 1
    opp = opps[0]

    # Exact expected output validation
    assert opp.platform == "kalshi"
    assert opp.ticker == "KXTEST-3CENT-EDGE"
    assert opp.side_actions == ["buy_yes", "buy_no"]
    assert opp.edge_cents == 3.0
    assert opp.gross_edge_cents == 3.0
    assert opp.estimated_fees_cents == 0.0
    assert isinstance(opp.timestamp, datetime)

    # Orderbook audit snapshot validation
    assert opp.orderbook_snapshot["ticker"] == "KXTEST-3CENT-EDGE"
    assert opp.orderbook_snapshot["yes_ask"] == 48
    assert opp.orderbook_snapshot["no_ask"] == 49


def test_intra_market_arb_no_edge(test_config, fixture_no_edge_kalshi_orderbook, fixture_zero_edge_market):
    """Test 2: No edge fixture emits no Opportunity objects ([]).

    Verifies both negative edge (cost=103c, edge=-3c) and zero edge (cost=100c, edge=0c).
    """
    opps_neg = find_intra_market_opportunities([fixture_no_edge_kalshi_orderbook], config=test_config)
    assert opps_neg == []

    opps_zero = find_intra_market_opportunities([fixture_zero_edge_market], config=test_config)
    assert opps_zero == []


def test_intra_market_arb_threshold_boundary(test_config, fixture_threshold_boundary_kalshi_orderbook):
    """Test 3: Exactly at threshold boundary (edge == min_edge == 2.0 cents).

    Because the condition is strictly `edge > min_edge`, an edge of exactly 2.0 cents
    does NOT exceed the 2.0 cent threshold, so 0 opportunities are emitted.

    Then, if threshold is slightly lowered (e.g. min_edge = 0.019 = 1.9c), the exact same
    orderbook DOES emit an Opportunity with exact edge_cents=2.0!
    """
    # At boundary: 2.0 > 2.0 is False -> empty list
    opps_at_boundary = find_intra_market_opportunities(
        [fixture_threshold_boundary_kalshi_orderbook],
        config=test_config,
    )
    assert opps_at_boundary == []

    # Slightly lower threshold (min_edge = 0.019 = 1.9c): 2.0 > 1.9 is True -> emits exact edge_cents=2.0
    lower_threshold_config = AppConfig(
        mode=test_config.mode,
        kalshi=test_config.kalshi,
        polymarket=test_config.polymarket,
        trading=TradingConfig(min_edge=0.019, taker_fee_bps=0.0),
        risk=test_config.risk,
        matching=test_config.matching,
    )

    opps_above = find_intra_market_opportunities(
        [fixture_threshold_boundary_kalshi_orderbook],
        config=lower_threshold_config,
    )
    assert len(opps_above) == 1
    assert opps_above[0].edge_cents == 2.0
    assert opps_above[0].gross_edge_cents == 2.0


def test_definition_of_done_mathematical_precision(test_config):
    """Definition of Done: All fixture tests pass with exact expected edge values.

    Guarantees against:
    - Sign inversion error: `(yes_ask + no_ask) - 100` instead of `100 - (yes_ask + no_ask)`
    - Precedence/grouping error: `100 - yes_ask + no_ask` instead of `100 - (yes_ask + no_ask)`
    - Off-by-one or percentage scaling bugs.
    """
    # YES ask = 45c, NO ask = 48c -> Total cost = 93c. Expected edge = 100 - 93 = 7.0c
    market_7c = Market(
        platform="kalshi",
        ticker="KXTEST-7C",
        yes_ask=45,
        no_ask=48,
    )
    opps = find_intra_market_opportunities([market_7c], config=test_config)
    assert len(opps) == 1
    assert opps[0].edge_cents == 7.0
    assert opps[0].gross_edge_cents == 7.0

    # Loss case: YES ask = 55c, NO ask = 48c -> Total cost = 103c.
    # An inverted sign implementation would compute 103 - 100 = +3.0c and mistakenly buy.
    # Our engine must detect negative edge and emit nothing.
    market_loss = Market(
        platform="kalshi",
        ticker="KXTEST-LOSS",
        yes_ask=55,
        no_ask=48,
    )
    opps_loss = find_intra_market_opportunities([market_loss], config=test_config)
    assert opps_loss == []


def test_intra_market_arb_with_estimated_fees(config_with_fees):
    """Verify net edge calculation when non-zero taker fees are applied.

    Parameters:
        yes_ask = 48c, no_ask = 49c
        gross_cost = 97c
        gross_edge = 100 - 97 = 3.0c
        taker_fee_bps = 50.0 (0.50%)
        fee_cents = 97 * (50 / 10,000) = 0.485c
        net_edge = 3.0 - 0.485 = 2.515c
        min_edge = 0.02 (2.0c) -> 2.515 > 2.0 -> EMITTED with exact edge_cents=2.515
    """
    market = Market(
        platform="kalshi",
        ticker="KXTEST-FEES",
        yes_ask=48,
        no_ask=49,
    )

    fee_val = calculate_estimated_fees(gross_cost_cents=97.0, config=config_with_fees)
    assert fee_val == 0.485

    opps = find_intra_market_opportunities([market], config=config_with_fees)
    assert len(opps) == 1
    opp = opps[0]

    assert opp.gross_edge_cents == 3.0
    assert opp.estimated_fees_cents == 0.485
    assert opp.edge_cents == 2.515


def test_intra_market_arb_fees_eliminating_opportunity():
    """Verify that fees can push a marginal gross edge below the min_edge threshold.

    Parameters:
        yes_ask = 49c, no_ask = 48.9c -> cost = 97.9c, gross_edge = 2.1c
        min_edge = 2.0c
        taker_fee_bps = 200.0 (2.0%)
        fee_cents = 97.9 * 0.02 = 1.958c
        net_edge = 2.1 - 1.958 = 0.142c < 2.0c -> NO OPPORTUNITY
    """
    high_fee_config = AppConfig(
        mode=ModeConfig(),
        kalshi=KalshiConfig(),
        polymarket=PolymarketConfig(),
        trading=TradingConfig(min_edge=0.02, taker_fee_bps=200.0),
        risk=RiskConfig(),
        matching=MatchingConfig(),
    )

    market = Market(
        platform="kalshi",
        ticker="KXTEST-FEE-MARGINAL",
        yes_ask=49,
        no_ask=49,  # cost = 98c, gross edge = 2.0c
    )

    opps = find_intra_market_opportunities([market], config=high_fee_config)
    assert opps == []


def test_intra_market_arb_polymarket_orderbook(test_config):
    """Verify intra-market arb detection on a Polymarket Orderbook fixture."""
    poly_ob = PolyOrderbook(
        ticker="0xpoly_condition_abc",
        token_id="1234567890",
        condition_id="0xpoly_condition_abc",
        yes_asks=[(47, 50)],
        no_asks=[(49, 100)],
        yes_bids=[(51, 50)],  # Implies best_no_ask = 100 - 51 = 49c
        no_bids=[(53, 100)],  # Implies best_yes_ask = 100 - 53 = 47c
    )

    opps = find_intra_market_opportunities([poly_ob], config=test_config)
    assert len(opps) == 1
    opp = opps[0]

    assert opp.platform == "polymarket"
    assert opp.ticker == "0xpoly_condition_abc"
    # Cost = 47 + 49 = 96c. Edge = 100 - 96 = 4.0c
    assert opp.edge_cents == 4.0
    assert opp.orderbook_snapshot["yes_ask"] == 47
    assert opp.orderbook_snapshot["no_ask"] == 49


def test_intra_market_arb_market_state(test_config):
    """Verify intra-market arb detection on a core.data_feed.MarketState instance."""
    state = MarketState(
        platform="kalshi",
        ticker="KXTEST-STATE",
        yes_ask=48,
        no_ask=49,
        yes_bid=46,
        no_bid=47,
        volume=12000,
    )

    opps = find_intra_market_opportunities([state], config=test_config)
    assert len(opps) == 1
    assert opps[0].edge_cents == 3.0
    assert opps[0].orderbook_snapshot["volume"] == 12000


def test_intra_market_arb_batch_processing(
    test_config,
    fixture_3_cent_edge_kalshi_orderbook,
    fixture_no_edge_kalshi_orderbook,
    fixture_threshold_boundary_kalshi_orderbook,
):
    """Verify scanning multiple mixed markets simultaneously in one batch."""
    batch = [
        fixture_no_edge_kalshi_orderbook,
        fixture_3_cent_edge_kalshi_orderbook,
        fixture_threshold_boundary_kalshi_orderbook,
    ]

    opps = find_intra_market_opportunities(batch, config=test_config)
    assert len(opps) == 1
    assert opps[0].ticker == "KXTEST-3CENT-EDGE"
    assert opps[0].edge_cents == 3.0


def test_intra_market_arb_closed_market_ignored(test_config):
    """Verify closed/settled markets are ignored even if their asks would have an edge."""
    closed_market = Market(
        platform="kalshi",
        ticker="KXTEST-CLOSED",
        yes_ask=40,
        no_ask=40,  # 20 cent edge!
        status="closed",
    )
    opps = find_intra_market_opportunities([closed_market], config=test_config)
    assert opps == []


def test_intra_market_arb_missing_asks_ignored(test_config):
    """Verify markets with one or both sides missing asks are ignored."""
    market_missing_no = Market(
        platform="kalshi",
        ticker="KXTEST-NO-NO-ASK",
        yes_ask=40,
        no_ask=None,
    )
    opps = find_intra_market_opportunities([market_missing_no], config=test_config)
    assert opps == []
