"""Unit tests for Polymarket Pydantic models (Market, Orderbook, Token) and parity with Kalshi models."""

import pytest
from kalshi_client.models import Market as KalshiMarket, Orderbook as KalshiOrderbook
from polymarket_client.models import Market as PolyMarket, Orderbook as PolyOrderbook, Token


def test_market_parsing_gamma_format() -> None:
    """Test parsing raw Gamma API dictionary with stringified JSON fields."""
    raw_gamma = {
        "id": "559651",
        "question": "Xi Jinping out before 2027?",
        "conditionId": "0xa467b14d51f01b957109d9cbb1d6c124fab2a089d52ed8f471d23c2812e743b7",
        "slug": "xi-jinping-out-before-2027",
        "endDate": "2027-01-01T04:59:00Z",
        "outcomes": '["Yes", "No"]',
        "outcomePrices": '["0.40", "0.60"]',
        "volume": "12831450.215930002",
        "active": True,
        "closed": False,
        "clobTokenIds": '["3233822019", "2565931067"]',
        "bestBid": 0.40,
        "bestAsk": 0.45,
    }

    market = PolyMarket.from_api(raw_gamma)

    assert market.condition_id == "0xa467b14d51f01b957109d9cbb1d6c124fab2a089d52ed8f471d23c2812e743b7"
    assert market.title == "Xi Jinping out before 2027?"
    assert market.ticker == "0xa467b14d51f01b957109d9cbb1d6c124fab2a089d52ed8f471d23c2812e743b7"
    assert market.yes_bid == 40
    assert market.yes_ask == 45
    assert market.no_bid == 55  # 100 - yes_ask (100 - 45)
    assert market.no_ask == 60  # 100 - yes_bid (100 - 40)
    assert market.volume == 12831450
    assert market.status == "open"
    assert market.yes_token_id == "3233822019"
    assert market.no_token_id == "2565931067"
    assert len(market.tokens) == 2
    assert market.tokens[0].token_id == "3233822019"
    assert market.tokens[0].outcome == "Yes"


def test_orderbook_parsing_clob_format() -> None:
    """Test parsing raw CLOB API orderbook response with bids and asks."""
    raw_clob = {
        "market": "0xa467b14d51f01b957109d9cbb1d6c124fab2a089d52ed8f471d23c2812e743b7",
        "asset_id": "3233822019",
        "bids": [
            {"price": "0.40", "size": "100.0"},
            {"price": "0.42", "size": "250.0"},
        ],
        "asks": [
            {"price": "0.45", "size": "300.0"},
            {"price": "0.48", "size": "150.0"},
        ],
    }

    ob = PolyOrderbook.from_api(raw_clob)

    assert ob.token_id == "3233822019"
    assert ob.ticker == "3233822019"
    assert ob.condition_id == "0xa467b14d51f01b957109d9cbb1d6c124fab2a089d52ed8f471d23c2812e743b7"

    # YES Bids sorted descending
    assert ob.yes_bids == [(42, 250), (40, 100)]
    assert ob.best_yes_bid == 42

    # YES Asks sorted ascending
    assert ob.yes_asks == [(45, 300), (48, 150)]
    assert ob.best_yes_ask == 45

    # Derived NO Bids (100 - yes_ask) sorted descending: best NO bid is 100 - 45 = 55
    assert ob.best_no_bid == 55
    assert ob.best_no_ask == 58  # 100 - 42

    # Spreads
    assert ob.yes_spread == 3  # 45 - 42
    assert ob.no_spread == 3   # 58 - 55


def test_orderbook_empty_and_partial() -> None:
    """Test orderbook with empty or single-sided bids/asks."""
    empty_ob = PolyOrderbook.from_api({"asset_id": "T1", "bids": [], "asks": []})
    assert empty_ob.best_yes_bid is None
    assert empty_ob.best_yes_ask is None
    assert empty_ob.best_no_bid is None
    assert empty_ob.best_no_ask is None
    assert empty_ob.yes_spread is None
    assert empty_ob.no_spread is None

    bids_only_ob = PolyOrderbook.from_api({
        "asset_id": "T2",
        "bids": [{"price": "0.30", "size": "10"}],
        "asks": [],
    })
    assert bids_only_ob.best_yes_bid == 30
    assert bids_only_ob.best_yes_ask is None
    assert bids_only_ob.best_no_bid is None
    assert bids_only_ob.best_no_ask == 70  # 100 - 30
    assert bids_only_ob.yes_spread is None


def test_cross_platform_interface_parity() -> None:
    """Verify that Kalshi and Polymarket models expose overlapping fields and properties."""
    kalshi_m = KalshiMarket(
        ticker="MKT-SAME",
        title="Same Prediction Market",
        yes_bid=40,
        yes_ask=45,
        no_bid=55,
        no_ask=60,
        volume=1000,
        status="open",
    )

    poly_m = PolyMarket(
        condition_id="0x123",
        ticker="MKT-SAME",
        title="Same Prediction Market",
        yes_bid=40,
        yes_ask=45,
        no_bid=55,
        no_ask=60,
        volume=1000,
        status="open",
    )

    # Core market fields overlap identically in naming, types, and values
    for field in ["ticker", "title", "yes_bid", "yes_ask", "no_bid", "no_ask", "volume", "status"]:
        assert hasattr(kalshi_m, field)
        assert hasattr(poly_m, field)
        assert getattr(kalshi_m, field) == getattr(poly_m, field)

    kalshi_ob = KalshiOrderbook(
        ticker="MKT-SAME",
        yes_bids=[(40, 10)],
        no_bids=[(55, 20)],
    )

    poly_ob = PolyOrderbook(
        ticker="MKT-SAME",
        token_id="TOK-SAME",
        yes_bids=[(40, 10)],
        yes_asks=[(45, 20)],
    )

    # Core orderbook properties overlap identically in naming, types, and values
    for prop in [
        "ticker",
        "best_yes_bid",
        "best_yes_ask",
        "best_no_bid",
        "best_no_ask",
        "yes_spread",
        "no_spread",
        "yes_bids",
        "no_bids",
        "yes_asks",
        "no_asks",
    ]:
        assert hasattr(kalshi_ob, prop)
        assert hasattr(poly_ob, prop)
        assert getattr(kalshi_ob, prop) == getattr(poly_ob, prop)
