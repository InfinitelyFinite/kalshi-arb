"""Unit tests for Kalshi data models and derived-ask orderbook logic."""

import pytest
from kalshi_client.models import Event, Market, Orderbook


def test_orderbook_derived_asks_hand_computed_example_1() -> None:
    """Hand-computed example 1: Standard two-sided order book.

    Given:
        YES bids: 40c (size 100), 35c (size 50)
        NO bids:  55c (size 200), 50c (size 100)

    Hand calculation:
        best_yes_bid = 40c
        best_no_bid  = 55c
        best_yes_ask = 100 - 55 = 45c
        best_no_ask  = 100 - 40 = 60c
        yes_spread   = 45 - 40 = 5c
        no_spread    = 60 - 55 = 5c
    """
    ob = Orderbook(
        ticker="TEST-MARKET-1",
        yes_bids=[(40, 100), (35, 50)],
        no_bids=[(55, 200), (50, 100)],
    )

    assert ob.best_yes_bid == 40
    assert ob.best_no_bid == 55
    assert ob.best_yes_ask == 45
    assert ob.best_no_ask == 60
    assert ob.yes_spread == 5
    assert ob.no_spread == 5

    # Check derived ask lists
    # Implied YES asks derived from NO bids (100 - 55 = 45 @ 200, 100 - 50 = 50 @ 100)
    assert ob.yes_asks == [(45, 200), (50, 100)]
    # Implied NO asks derived from YES bids (100 - 40 = 60 @ 100, 100 - 35 = 65 @ 50)
    assert ob.no_asks == [(60, 100), (65, 50)]


def test_orderbook_derived_asks_hand_computed_example_2() -> None:
    """Hand-computed example 2: Asymmetric order book with unsorted bids.

    Given:
        YES bids: 12c (size 10), 88c (size 5), 45c (size 50)  -> best YES bid = 88c
        NO bids:  8c (size 100), 10c (size 20)                 -> best NO bid = 10c

    Hand calculation:
        best_yes_bid = 88c
        best_no_bid  = 10c
        best_yes_ask = 100 - 10 = 90c
        best_no_ask  = 100 - 88 = 12c
        yes_spread   = 90 - 88 = 2c
        no_spread    = 12 - 10 = 2c
    """
    ob = Orderbook(
        ticker="TEST-MARKET-2",
        yes_bids=[(12, 10), (88, 5), (45, 50)],
        no_bids=[(8, 100), (10, 20)],
    )

    assert ob.best_yes_bid == 88
    assert ob.best_no_bid == 10
    assert ob.best_yes_ask == 90
    assert ob.best_no_ask == 12
    assert ob.yes_spread == 2
    assert ob.no_spread == 2


def test_orderbook_one_sided_books() -> None:
    """Test behavior when an orderbook has bids on only one side or is completely empty."""
    # 1. Only YES bids
    ob_yes_only = Orderbook(
        ticker="YES-ONLY",
        yes_bids=[(70, 25)],
        no_bids=[],
    )
    assert ob_yes_only.best_yes_bid == 70
    assert ob_yes_only.best_no_bid is None
    assert ob_yes_only.best_yes_ask is None
    assert ob_yes_only.best_no_ask == 30  # 100 - 70
    assert ob_yes_only.yes_spread is None
    assert ob_yes_only.no_spread is None

    # 2. Only NO bids
    ob_no_only = Orderbook(
        ticker="NO-ONLY",
        yes_bids=[],
        no_bids=[(62, 40)],
    )
    assert ob_no_only.best_yes_bid is None
    assert ob_no_only.best_no_bid == 62
    assert ob_no_only.best_yes_ask == 38  # 100 - 62
    assert ob_no_only.best_no_ask is None

    # 3. Completely empty
    ob_empty = Orderbook(ticker="EMPTY", yes_bids=[], no_bids=[])
    assert ob_empty.best_yes_bid is None
    assert ob_empty.best_no_bid is None
    assert ob_empty.best_yes_ask is None
    assert ob_empty.best_no_ask is None
    assert ob_empty.yes_asks == []
    assert ob_empty.no_asks == []


def test_orderbook_from_api_orderbook_fp() -> None:
    """Test parsing raw Kalshi orderbook_fp fixed-point response."""
    raw_api_data = {
        "ticker": "KXFED-24DEC",
        "orderbook_fp": {
            "yes_dollars": [
                ["0.0100", "100.00"],
                ["0.6500", "25.00"],
            ],
            "no_dollars": [
                ["0.3000", "50.00"],
            ],
        },
    }

    ob = Orderbook.from_api(raw_api_data)
    assert ob.ticker == "KXFED-24DEC"
    assert ob.yes_bids == [(1, 100), (65, 25)]
    assert ob.no_bids == [(30, 50)]
    assert ob.best_yes_bid == 65
    assert ob.best_no_bid == 30
    assert ob.best_yes_ask == 70  # 100 - 30
    assert ob.best_no_ask == 35  # 100 - 65


def test_orderbook_from_api_nested_cents() -> None:
    """Test parsing raw Kalshi integer cents orderbook payload."""
    raw_api_data = {
        "ticker": "KXCPI-24OCT",
        "orderbook": {
            "yes": [[45, 10], [50, 20]],
            "no": [[48, 15]],
        },
    }

    ob = Orderbook.from_api(raw_api_data)
    assert ob.ticker == "KXCPI-24OCT"
    assert ob.yes_bids == [(45, 10), (50, 20)]
    assert ob.no_bids == [(48, 15)]
    assert ob.best_yes_bid == 50
    assert ob.best_no_bid == 48
    assert ob.best_yes_ask == 52  # 100 - 48
    assert ob.best_no_ask == 50  # 100 - 50


def test_market_model_parsing() -> None:
    """Test Market model extraction from various Kalshi API field styles."""
    raw_market = {
        "ticker": "KXGOLD-24AUG",
        "title": "Will Gold close above 2500?",
        "yes_bid_dollars": "0.6200",
        "yes_ask_dollars": "0.6500",
        "no_bid_dollars": "0.3500",
        "no_ask_dollars": "0.3800",
        "volume_fp": "1250.00",
        "status": "active",
        "close_time": "2026-08-24T20:00:00Z",
        "event_ticker": "KXGOLD",
    }

    market = Market.from_api(raw_market)
    assert market.ticker == "KXGOLD-24AUG"
    assert market.title == "Will Gold close above 2500?"
    assert market.yes_bid == 62
    assert market.yes_ask == 65
    assert market.no_bid == 35
    assert market.no_ask == 38
    assert market.volume == 1250
    assert market.status == "active"
    assert market.event_ticker == "KXGOLD"


def test_event_model_parsing() -> None:
    """Test Event model parsing from Kalshi API response."""
    raw_event = {
        "event_ticker": "KXFED-24",
        "title": "Federal Reserve Rate Decisions",
        "sub_title": "Interest Rates in 2024",
        "category": "Economics",
        "series_ticker": "KXFED",
        "mutually_exclusive": False,
    }

    event = Event.from_api(raw_event)
    assert event.event_ticker == "KXFED-24"
    assert event.title == "Federal Reserve Rate Decisions"
    assert event.category == "Economics"
    assert event.series_ticker == "KXFED"
    assert event.mutually_exclusive is False
