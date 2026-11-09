"""Live tradability checks for verified market pairs (shared by the validation script and the collection gate).

``check_pair_live`` queries the real platform APIs through the existing clients and returns ONE result per pair
with a machine-readable reason code. API failures (timeouts, 5xx, auth) are reported as ``API_ERROR`` and never
as the market being invalid.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional

from core.models import MarketStatus, normalize_market_status
from core.pair_registry import MarketPairOverride

logger = logging.getLogger(__name__)


class ReasonCode(str, Enum):
    SCHEMA_INVALID = "SCHEMA_INVALID"
    NOT_VERIFIED = "NOT_VERIFIED"
    NONEXISTENT = "NONEXISTENT"
    EXPIRED = "EXPIRED"
    INACTIVE = "INACTIVE"
    NO_ORDERBOOK = "NO_ORDERBOOK"
    CLOSE_TIME_MISMATCH = "CLOSE_TIME_MISMATCH"
    API_ERROR = "API_ERROR"


PASS = "PASS"
WARN = "WARN"
EXCLUDED = "EXCLUDED"


@dataclass
class PairCheckResult:
    pair_id: str
    category: str
    outcome: str  # PASS | WARN | EXCLUDED
    reason_code: Optional[ReasonCode] = None
    detail: str = ""
    kalshi_close_utc: Optional[datetime] = None
    polymarket_end_utc: Optional[datetime] = None
    kalshi_status: MarketStatus = MarketStatus.UNKNOWN
    polymarket_status: MarketStatus = MarketStatus.UNKNOWN
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def passed(self) -> bool:
        return self.outcome == PASS

    @property
    def reason_label(self) -> str:
        return self.reason_code.value if self.reason_code else ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_id": self.pair_id,
            "category": self.category,
            "outcome": self.outcome,
            "reason_code": self.reason_label or None,
            "detail": self.detail,
            "kalshi_close_utc": self.kalshi_close_utc.isoformat() if self.kalshi_close_utc else None,
            "polymarket_end_utc": self.polymarket_end_utc.isoformat() if self.polymarket_end_utc else None,
            "kalshi_status": self.kalshi_status.value,
            "polymarket_status": self.polymarket_status.value,
            "checked_at_utc": self.checked_at.isoformat(),
        }


def parse_dt(value: Any) -> Optional[datetime]:
    """Parse datetime/ISO string to aware UTC; None if missing or unparseable."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _status_code(exc: BaseException) -> Optional[int]:
    code = getattr(exc, "status_code", None)
    return code if isinstance(code, int) else None


def is_not_found(exc: BaseException) -> bool:
    """True only for an explicit HTTP 404 from the platform."""
    return _status_code(exc) == 404


class _Abort(Exception):
    """Internal control flow: stop checking this pair with the given result."""

    def __init__(self, code: ReasonCode, detail: str, outcome: str = EXCLUDED) -> None:
        self.code, self.detail, self.outcome = code, detail, outcome


def _api_error(platform: str, what: str, exc: BaseException) -> _Abort:
    # Only the exception type and message from the platform client; never request headers or credentials.
    return _Abort(ReasonCode.API_ERROR, f"{platform} {what}: {type(exc).__name__}: {str(exc)[:200]}")


def _kalshi_book_nonempty(book: Any) -> bool:
    return bool(getattr(book, "yes_bids", None) or getattr(book, "no_bids", None))


def _poly_book_nonempty(book: Any) -> bool:
    return bool(getattr(book, "yes_bids", None) or getattr(book, "yes_asks", None))


async def check_pair_live(
    pair: MarketPairOverride,
    kalshi_client: Any,
    polymarket_client: Any,
    *,
    now: Optional[datetime] = None,
    close_mismatch_hours: Optional[float] = 24.0,
) -> PairCheckResult:
    """Check one verified pair against the live APIs.

    Order of checks: verified status -> Kalshi market -> Polymarket market -> order books -> close-time mismatch.
    ``close_mismatch_hours=None`` skips the (comparability, not tradability) mismatch warning, as the gate does.
    """
    now = now or datetime.now(timezone.utc)
    result = PairCheckResult(pair_id=pair.pair_id, category=pair.category, outcome=PASS, checked_at=now)

    if not pair.is_verified:
        result.outcome = EXCLUDED
        result.reason_code = ReasonCode.NOT_VERIFIED
        result.detail = pair.exclusion_reason or "status is not verified"
        return result

    try:
        await _check_kalshi(pair, kalshi_client, now, result)
        await _check_polymarket(pair, polymarket_client, now, result)
        await _check_books(pair, kalshi_client, polymarket_client, result)
        _check_close_mismatch(pair, result, close_mismatch_hours)
    except _Abort as stop:
        result.outcome = stop.outcome
        result.reason_code = stop.code
        result.detail = stop.detail
    return result


async def _check_kalshi(pair: MarketPairOverride, client: Any, now: datetime, result: PairCheckResult) -> None:
    try:
        market = await client.get_market(pair.kalshi_ticker)
    except Exception as e:  # noqa: BLE001 - classify every failure mode
        if is_not_found(e):
            raise _Abort(ReasonCode.NONEXISTENT, f"Kalshi market {pair.kalshi_ticker} not found (404)") from e
        raise _api_error("Kalshi", "get_market", e) from e
    if market is None:
        raise _Abort(ReasonCode.NONEXISTENT, f"Kalshi market {pair.kalshi_ticker} not found")

    status = normalize_market_status(getattr(market, "status", None))
    result.kalshi_status = status
    close = parse_dt(getattr(market, "close_time", None))
    result.kalshi_close_utc = close
    if status in (MarketStatus.CLOSED, MarketStatus.SETTLED):
        raise _Abort(ReasonCode.EXPIRED, f"Kalshi status is {market.status!r}")
    if status is not MarketStatus.OPEN:
        raise _Abort(ReasonCode.INACTIVE, f"Kalshi status is {getattr(market, 'status', None)!r}, not open/active")
    if close is not None and close <= now:
        raise _Abort(ReasonCode.EXPIRED, f"Kalshi close time {close.isoformat()} is not in the future")


async def _check_polymarket(pair: MarketPairOverride, client: Any, now: datetime, result: PairCheckResult) -> None:
    try:
        market = await client.get_market(pair.polymarket_condition_id)
    except Exception as e:  # noqa: BLE001
        if is_not_found(e):
            raise _Abort(ReasonCode.NONEXISTENT, f"Polymarket market {pair.polymarket_condition_id} not found (404)") from e
        raise _api_error("Polymarket", "get_market", e) from e
    if market is None:
        raise _Abort(ReasonCode.NONEXISTENT, f"Polymarket market {pair.polymarket_condition_id} not found")

    accepting = getattr(market, "accepting_orders", None)
    status = normalize_market_status(
        getattr(market, "status", None),
        accepting_orders=accepting,
        archived=getattr(market, "archived", None),
    )
    result.polymarket_status = status
    end = parse_dt(getattr(market, "close_time", None))
    result.polymarket_end_utc = end

    raw_status = str(getattr(market, "status", "")).lower()
    if status in (MarketStatus.CLOSED, MarketStatus.SETTLED) or raw_status in ("closed", "resolved"):
        raise _Abort(ReasonCode.EXPIRED, f"Polymarket market is closed/resolved (status={market.status!r})")
    if raw_status != "open":
        raise _Abort(ReasonCode.INACTIVE, f"Polymarket market is not active (status={market.status!r})")
    if accepting is None:
        raise _Abort(ReasonCode.INACTIVE, "Polymarket acceptingOrders flag missing from response; cannot confirm tradability")
    if accepting is False:
        raise _Abort(ReasonCode.INACTIVE, "Polymarket market is not accepting orders")
    if getattr(market, "enable_order_book", None) is False:
        raise _Abort(ReasonCode.INACTIVE, "Polymarket order book is disabled for this market")
    if end is not None and end <= now:
        raise _Abort(ReasonCode.EXPIRED, f"Polymarket end date {end.isoformat()} is not in the future")


async def _check_books(pair: MarketPairOverride, kalshi: Any, poly: Any, result: PairCheckResult) -> None:
    try:
        k_book = await kalshi.get_orderbook(pair.kalshi_ticker)
    except Exception as e:  # noqa: BLE001
        if is_not_found(e):
            raise _Abort(ReasonCode.NO_ORDERBOOK, f"Kalshi order book for {pair.kalshi_ticker} not found (404)") from e
        raise _api_error("Kalshi", "get_orderbook", e) from e
    if not _kalshi_book_nonempty(k_book):
        raise _Abort(ReasonCode.NO_ORDERBOOK, f"Kalshi order book for {pair.kalshi_ticker} is empty")

    token_books = []
    for label, token in (("YES", pair.polymarket_yes_token_id), ("NO", pair.polymarket_no_token_id)):
        try:
            token_books.append(await poly.get_orderbook(token))
        except Exception as e:  # noqa: BLE001
            if is_not_found(e):
                token_books.append(None)  # CLOB answers 404 for tokens without an order book
                continue
            raise _api_error("Polymarket", f"get_orderbook({label} token)", e) from e
    if not any(b is not None and _poly_book_nonempty(b) for b in token_books):
        raise _Abort(ReasonCode.NO_ORDERBOOK, "Polymarket YES/NO token order books are both missing or empty")


def _check_close_mismatch(pair: MarketPairOverride, result: PairCheckResult, threshold_hours: Optional[float]) -> None:
    if threshold_hours is None:
        return
    k_close = result.kalshi_close_utc
    p_end = result.polymarket_end_utc
    source = "live"
    if (k_close is None or p_end is None) and pair.verification is not None:
        # Fall back to the human-recorded dates (A2) when a platform omits them live.
        k_close = k_close or pair.verification.kalshi_expected_close_utc
        p_end = p_end or pair.verification.polymarket_expected_end_utc
        source = "recorded"
    k_close, p_end = parse_dt(k_close), parse_dt(p_end)
    if k_close is None or p_end is None:
        return
    gap = abs(k_close - p_end)
    if gap > timedelta(hours=threshold_hours):
        raise _Abort(
            ReasonCode.CLOSE_TIME_MISMATCH,
            f"expected close times differ by {gap.total_seconds() / 3600:.1f}h (> {threshold_hours:g}h; {source} dates): "
            f"Kalshi {k_close.isoformat()} vs Polymarket {p_end.isoformat()}",
            outcome=WARN,
        )


async def check_pairs_live(
    pairs: list[MarketPairOverride],
    kalshi_client: Any,
    polymarket_client: Any,
    *,
    now: Optional[datetime] = None,
    close_mismatch_hours: Optional[float] = 24.0,
    concurrency: int = 4,
) -> list[PairCheckResult]:
    """Check many pairs with bounded concurrency; result order matches input order."""
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(p: MarketPairOverride) -> PairCheckResult:
        async with sem:
            return await check_pair_live(
                p, kalshi_client, polymarket_client, now=now, close_mismatch_hours=close_mismatch_hours
            )

    return list(await asyncio.gather(*(one(p) for p in pairs)))
