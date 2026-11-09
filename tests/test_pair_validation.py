"""Unit tests for live pair validation and tradability checking with mocked clients."""

from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import AsyncMock, MagicMock
import pytest

from core.models import MarketStatus
from core.pair_registry import CHECKLIST_KEYS, MarketPairOverride, VerificationRecord
from core.pair_validation import (
    EXCLUDED,
    PASS,
    WARN,
    ReasonCode,
    check_pair_live,
    check_pairs_live,
)


def _make_verified_pair(
    pair_id: str = "test-pair-1",
    kalshi_close: Optional[datetime] = None,
    poly_end: Optional[datetime] = None,
) -> MarketPairOverride:
    k_close = kalshi_close or datetime(2028, 11, 7, 12, 0, tzinfo=timezone.utc)
    p_end = poly_end or datetime(2028, 11, 7, 12, 0, tzinfo=timezone.utc)
    checklist = {k: {"pass": True, "note": "ok"} for k in CHECKLIST_KEYS}
    v_rec = VerificationRecord(
        verified_by="test_user",
        verified_at=datetime.now(timezone.utc).date(),
        kalshi_rules_url="https://kalshi.com/test",
        polymarket_rules_url="https://polymarket.com/test",
        kalshi_expected_close_utc=k_close,
        polymarket_expected_end_utc=p_end,
        checklist=checklist,
    )
    return MarketPairOverride(
        pair_id=pair_id,
        category="politics",
        kalshi_ticker="KXTEST-1",
        polymarket_condition_id="0x" + "a" * 64,
        polymarket_yes_token_id="1111",
        polymarket_no_token_id="2222",
        status="verified",
        verification=v_rec,
    )


class HTTPStatusError(Exception):
    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        self.status_code = status_code


@pytest.mark.asyncio
async def test_excluded_pair_returns_not_verified():
    pair = MarketPairOverride(
        pair_id="excluded-1",
        category="politics",
        status="excluded",
        exclusion_reason="Manual exclusion reason",
    )
    k_mock = AsyncMock()
    p_mock = AsyncMock()

    res = await check_pair_live(pair, k_mock, p_mock)
    assert res.outcome == EXCLUDED
    assert res.reason_code == ReasonCode.NOT_VERIFIED
    assert res.detail == "Manual exclusion reason"
    # Neither client should have been queried
    assert not k_mock.get_market.called
    assert not p_mock.get_market.called


@pytest.mark.asyncio
async def test_kalshi_market_not_found():
    pair = _make_verified_pair()
    k_mock = AsyncMock()
    k_mock.get_market.side_effect = HTTPStatusError("Not found", 404)
    p_mock = AsyncMock()

    res = await check_pair_live(pair, k_mock, p_mock)
    assert res.outcome == EXCLUDED
    assert res.reason_code == ReasonCode.NONEXISTENT
    assert "not found (404)" in res.detail


@pytest.mark.asyncio
async def test_kalshi_market_closed():
    pair = _make_verified_pair()
    k_mock = AsyncMock()
    k_market = MagicMock()
    k_market.status = "closed"
    k_market.close_time = "2028-11-07T12:00:00Z"
    k_mock.get_market.return_value = k_market

    p_mock = AsyncMock()

    res = await check_pair_live(pair, k_mock, p_mock)
    assert res.outcome == EXCLUDED
    assert res.reason_code == ReasonCode.EXPIRED
    assert "closed" in res.detail


@pytest.mark.asyncio
async def test_kalshi_close_time_in_past():
    pair = _make_verified_pair()
    k_mock = AsyncMock()
    k_market = MagicMock()
    k_market.status = "open"
    k_market.close_time = "2020-01-01T00:00:00Z"  # in past
    k_mock.get_market.return_value = k_market

    p_mock = AsyncMock()

    now = datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc)
    res = await check_pair_live(pair, k_mock, p_mock, now=now)
    assert res.outcome == EXCLUDED
    assert res.reason_code == ReasonCode.EXPIRED
    assert "not in the future" in res.detail


@pytest.mark.asyncio
async def test_polymarket_market_inactive():
    pair = _make_verified_pair()
    k_mock = AsyncMock()
    k_market = MagicMock()
    k_market.status = "open"
    k_market.close_time = "2028-11-07T12:00:00Z"
    k_mock.get_market.return_value = k_market

    p_mock = AsyncMock()
    p_market = MagicMock()
    p_market.status = "open"
    p_market.accepting_orders = False  # orders disabled!
    p_market.archived = False
    p_market.enable_order_book = True
    p_market.close_time = "2028-11-07T12:00:00Z"
    p_mock.get_market.return_value = p_market

    res = await check_pair_live(pair, k_mock, p_mock)
    assert res.outcome == EXCLUDED
    assert res.reason_code == ReasonCode.INACTIVE
    assert "not accepting orders" in res.detail


@pytest.mark.asyncio
async def test_empty_orderbook_returns_no_orderbook():
    pair = _make_verified_pair()
    k_mock = AsyncMock()
    k_market = MagicMock(status="open", close_time="2028-11-07T12:00:00Z")
    k_mock.get_market.return_value = k_market
    k_book = MagicMock(yes_bids=[], no_bids=[])  # empty book
    k_mock.get_orderbook.return_value = k_book

    p_mock = AsyncMock()
    p_market = MagicMock(status="open", accepting_orders=True, archived=False, enable_order_book=True, close_time="2028-11-07T12:00:00Z")
    p_mock.get_market.return_value = p_market

    res = await check_pair_live(pair, k_mock, p_mock)
    assert res.outcome == EXCLUDED
    assert res.reason_code == ReasonCode.NO_ORDERBOOK
    assert "empty" in res.detail


@pytest.mark.asyncio
async def test_close_time_mismatch_warning():
    # 48 hour gap between close times
    k_close = datetime(2028, 11, 7, 0, 0, tzinfo=timezone.utc)
    p_end = datetime(2028, 11, 9, 12, 0, tzinfo=timezone.utc)
    pair = _make_verified_pair(kalshi_close=k_close, poly_end=p_end)

    k_mock = AsyncMock()
    k_mock.get_market.return_value = MagicMock(status="open", close_time=k_close.isoformat())
    k_mock.get_orderbook.return_value = MagicMock(yes_bids=[(50, 10)])

    p_mock = AsyncMock()
    p_mock.get_market.return_value = MagicMock(status="open", accepting_orders=True, archived=False, enable_order_book=True, close_time=p_end.isoformat())
    p_mock.get_orderbook.return_value = MagicMock(yes_bids=[(50, 10)], yes_asks=[(52, 10)])

    res = await check_pair_live(pair, k_mock, p_mock, close_mismatch_hours=24.0)
    assert res.outcome == WARN
    assert res.reason_code == ReasonCode.CLOSE_TIME_MISMATCH
    assert "differ by" in res.detail


@pytest.mark.asyncio
async def test_api_error_reported_without_invalidating_market():
    pair = _make_verified_pair()
    k_mock = AsyncMock()
    k_mock.get_market.side_effect = TimeoutError("Connection to Kalshi timed out")
    p_mock = AsyncMock()

    res = await check_pair_live(pair, k_mock, p_mock)
    assert res.outcome == EXCLUDED
    assert res.reason_code == ReasonCode.API_ERROR
    assert "TimeoutError" in res.detail


@pytest.mark.asyncio
async def test_active_tradable_pair_passes():
    close_time = datetime(2028, 11, 7, 12, 0, tzinfo=timezone.utc)
    pair = _make_verified_pair(kalshi_close=close_time, poly_end=close_time)

    k_mock = AsyncMock()
    k_mock.get_market.return_value = MagicMock(status="open", close_time=close_time.isoformat())
    k_mock.get_orderbook.return_value = MagicMock(yes_bids=[(48, 100)], no_bids=[(48, 100)])

    p_mock = AsyncMock()
    p_mock.get_market.return_value = MagicMock(status="open", accepting_orders=True, archived=False, enable_order_book=True, close_time=close_time.isoformat())
    p_mock.get_orderbook.return_value = MagicMock(yes_bids=[(49, 100)], yes_asks=[(51, 100)])

    res = await check_pair_live(pair, k_mock, p_mock)
    assert res.outcome == PASS
    assert res.reason_code is None
    assert res.passed is True


@pytest.mark.asyncio
async def test_check_pairs_live_concurrency_and_order():
    p1 = _make_verified_pair(pair_id="p1")
    p2 = MarketPairOverride(pair_id="p2", category="pol", status="excluded", exclusion_reason="skip")
    p3 = _make_verified_pair(pair_id="p3")

    k_mock = AsyncMock()
    k_mock.get_market.return_value = MagicMock(status="open", close_time="2028-11-07T12:00:00Z")
    k_mock.get_orderbook.return_value = MagicMock(yes_bids=[(50, 10)])

    p_mock = AsyncMock()
    p_mock.get_market.return_value = MagicMock(status="open", accepting_orders=True, archived=False, enable_order_book=True, close_time="2028-11-07T12:00:00Z")
    p_mock.get_orderbook.return_value = MagicMock(yes_bids=[(50, 10)], yes_asks=[(50, 10)])

    results = await check_pairs_live([p1, p2, p3], k_mock, p_mock, concurrency=2)
    assert len(results) == 3
    assert [r.pair_id for r in results] == ["p1", "p2", "p3"]
    assert results[0].passed is True
    assert results[1].outcome == EXCLUDED
    assert results[2].passed is True
