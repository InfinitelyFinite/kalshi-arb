"""Pydantic data models for Kalshi API entities: Market, Orderbook, and Event."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Union
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _to_decimal(val: Any) -> Optional[Decimal]:
    """Exact Decimal from str/int/float; None if missing or not finite."""
    if val is None or val == "":
        return None
    try:
        d = Decimal(str(val).strip())
    except (InvalidOperation, ValueError):
        return None
    return d if d.is_finite() else None


def _dollar_levels(raw: Any) -> list[tuple[Decimal, Decimal]]:
    """Parse [["0.4500", "10.00"], ...] (price in DOLLARS, size in contracts) -> (cents, contracts) exactly."""
    out: list[tuple[Decimal, Decimal]] = []
    for item in raw or []:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        p, s = _to_decimal(item[0]), _to_decimal(item[1])
        if p is not None and s is not None:
            out.append((p * 100, s))
    return out


def _cent_levels(raw: Any) -> list[tuple[Decimal, Decimal]]:
    """Parse [[45, 10], ...] (price in CENTS, size in contracts) exactly."""
    out: list[tuple[Decimal, Decimal]] = []
    for item in raw or []:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        p, s = _to_decimal(item[0]), _to_decimal(item[1])
        if p is not None and s is not None:
            out.append((p, s))
    return out


def parse_exchange_timestamp(val: Any) -> Optional[datetime]:
    """Parse a platform-supplied timestamp (epoch seconds/ms or ISO-8601) to aware UTC. None if absent/invalid.

    The caller must never substitute the local receive time for a missing exchange timestamp.
    """
    if val is None or val == "" or isinstance(val, bool):
        return None
    try:
        if isinstance(val, (int, float)) or (isinstance(val, str) and val.strip().replace(".", "", 1).isdigit()):
            num = float(val)
            if num <= 0:
                return None
            if num > 1e14:  # microseconds
                num /= 1_000_000
            elif num > 1e11:  # milliseconds
                num /= 1000
            return datetime.fromtimestamp(num, tz=timezone.utc)
        if isinstance(val, datetime):
            return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        dt = datetime.fromisoformat(str(val).strip().replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def _parse_cents(val: Any) -> Optional[int]:
    """Helper to convert string/float/int dollar or cent representations to integer cents (0-100)."""
    if val is None or val == "":
        return None
    if isinstance(val, (int, float)):
        # If <= 1.0 and > 0, assume dollar decimal (e.g. 0.45 -> 45 cents)
        if 0.0 < float(val) <= 1.0:
            return int(round(float(val) * 100))
        return int(round(float(val)))
    if isinstance(val, str):
        try:
            f = float(val)
            if 0.0 < f <= 1.0:
                return int(round(f * 100))
            return int(round(f))
        except ValueError:
            return None
    return None


def _parse_volume(val: Any) -> Optional[int]:
    """Helper to convert volume strings/floats to integer contracts."""
    if val is None or val == "":
        return None
    try:
        return int(round(float(val)))
    except (ValueError, TypeError):
        return None


class Market(BaseModel):
    """Kalshi Market data model."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    ticker: str = Field(description="Unique market ticker symbol (e.g. KXHIGHNY-24JAN01-T60)")
    title: str = Field(description="Human-readable title describing the prediction market")
    yes_bid: Optional[int] = Field(default=None, description="Best YES bid in cents (1-99)")
    yes_ask: Optional[int] = Field(default=None, description="Best YES ask in cents (1-99)")
    no_bid: Optional[int] = Field(default=None, description="Best NO bid in cents (1-99)")
    no_ask: Optional[int] = Field(default=None, description="Best NO ask in cents (1-99)")
    volume: Optional[int] = Field(default=0, description="Total volume or 24h volume in contracts")
    status: Optional[str] = Field(default="open", description="Market status (e.g. open, active, closed, settled)")
    close_time: Optional[Union[datetime, str]] = Field(default=None, description="Market expiration or close timestamp")
    result: Optional[str] = Field(default=None, description="Settlement result if determined ('yes'/'no'/'')")
    event_ticker: Optional[str] = Field(default=None, description="Parent event ticker")

    @model_validator(mode="before")
    @classmethod
    def extract_api_fields(cls, data: Any) -> Any:
        """Extract fields from raw Kalshi API responses with differing field conventions."""
        if not isinstance(data, dict):
            return data

        # Map ticker and title
        ticker = data.get("ticker") or data.get("market_ticker")
        title = data.get("title") or data.get("market_title") or data.get("subtitle") or ""

        # Parse prices. Units are explicit: `*_dollars` fields are DOLLARS; bare `yes_bid` etc. are integer CENTS.
        # (The generic _parse_cents heuristic would misread a bare 1 (= 1 cent) as $1.00 = 100 cents.)
        def price_cents(key: str) -> Optional[int]:
            dollars = _to_decimal(data.get(f"{key}_dollars"))
            if dollars is not None:
                return int(round(dollars * 100))
            raw = data.get(key)
            if raw is None or raw == "":
                return None
            if isinstance(raw, str) and "." in raw:
                return _parse_cents(raw)  # legacy dollar-formatted string
            cents = _to_decimal(raw)
            return int(round(cents)) if cents is not None else None

        yes_bid = price_cents("yes_bid")
        yes_ask = price_cents("yes_ask")
        no_bid = price_cents("no_bid")
        no_ask = price_cents("no_ask")

        # Parse volume
        volume = _parse_volume(
            data.get("volume")
            or data.get("volume_fp")
            or data.get("volume_24h_fp")
            or data.get("volume_24h")
        )

        status = data.get("status") or "open"
        close_time = data.get("close_time") or data.get("expiration_time") or data.get("expected_expiration_time")
        event_ticker = data.get("event_ticker")

        return {
            "ticker": ticker,
            "title": title,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "volume": volume or 0,
            "status": status,
            "close_time": close_time,
            "event_ticker": event_ticker,
            "result": data.get("result") or None,
        }

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> Market:
        """Construct Market instance from raw Kalshi API dictionary."""
        return cls.model_validate(data)


class Orderbook(BaseModel):
    """Kalshi Orderbook model.

    Kalshi's orderbook API is bids-only on each side (YES bids and NO bids).
    Asks are derived via the binary market identity:
        Ask_YES = 100 - Bid_NO
        Ask_NO  = 100 - Bid_YES
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    ticker: str = Field(description="Market ticker")
    yes_bids: list[tuple[int, int]] = Field(
        default_factory=list,
        description="List of (price_in_cents, size_in_contracts) bids for YES side (legacy rounded ints)",
    )
    no_bids: list[tuple[int, int]] = Field(
        default_factory=list,
        description="List of (price_in_cents, size_in_contracts) bids for NO side (legacy rounded ints)",
    )
    yes_bid_levels: list[tuple[Decimal, Decimal]] = Field(
        default_factory=list,
        description="Exact (price_cents, size_contracts) YES bids; fractional sizes/sub-cent prices preserved",
    )
    no_bid_levels: list[tuple[Decimal, Decimal]] = Field(
        default_factory=list,
        description="Exact (price_cents, size_contracts) NO bids; fractional sizes/sub-cent prices preserved",
    )
    exchange_updated_at: Optional[datetime] = Field(
        default=None,
        description="Platform-supplied book timestamp (WS message). REST orderbook responses carry none -> None",
    )

    @model_validator(mode="before")
    @classmethod
    def extract_orderbook_data(cls, data: Any) -> Any:
        """Parse raw Kalshi orderbook response containing either orderbook_fp or orderbook or direct tuples."""
        if not isinstance(data, dict):
            return data

        ticker = data.get("ticker", "")

        exchange_ts = parse_exchange_timestamp(
            data.get("ts") or data.get("exchange_updated_at") or data.get("updated_time")
        )

        def build(yes_levels: list[tuple[Decimal, Decimal]], no_levels: list[tuple[Decimal, Decimal]]) -> dict[str, Any]:
            return {
                "ticker": ticker,
                # legacy rounded ints for existing consumers
                "yes_bids": [(int(round(p)), int(round(s))) for p, s in yes_levels],
                "no_bids": [(int(round(p)), int(round(s))) for p, s in no_levels],
                "yes_bid_levels": yes_levels,
                "no_bid_levels": no_levels,
                "exchange_updated_at": exchange_ts,
            }

        # If data has nested 'orderbook_fp' (e.g. {'orderbook_fp': {'yes_dollars': [...], 'no_dollars': [...]}})
        # Units: prices in DOLLARS (strings), sizes in contracts (fixed-point strings, may be fractional).
        nested_fp = data.get("orderbook_fp")
        if isinstance(nested_fp, dict):
            return build(
                _dollar_levels(nested_fp.get("yes_dollars")),
                _dollar_levels(nested_fp.get("no_dollars")),
            )

        # If data has nested 'orderbook' (e.g. {'orderbook': {'yes': [[40, 10]], 'no': [[55, 20]]}})
        # Units: prices in integer CENTS.
        nested_cents = data.get("orderbook")
        if isinstance(nested_cents, dict):
            return build(_cent_levels(nested_cents.get("yes")), _cent_levels(nested_cents.get("no")))

        # If raw dict contains yes_dollars / no_dollars directly (DOLLARS)
        if "yes_dollars" in data or "no_dollars" in data:
            return build(_dollar_levels(data.get("yes_dollars")), _dollar_levels(data.get("no_dollars")))

        # Direct yes_bids / no_bids (CENTS, ints)
        return data

    @model_validator(mode="after")
    def _fill_levels(self) -> Orderbook:
        """Directly constructed books (WS state, fixtures, simulator) only supply int tuples: mirror to Decimal."""
        if not self.yes_bid_levels and self.yes_bids:
            self.yes_bid_levels = [(Decimal(p), Decimal(s)) for p, s in self.yes_bids]
        if not self.no_bid_levels and self.no_bids:
            self.no_bid_levels = [(Decimal(p), Decimal(s)) for p, s in self.no_bids]
        return self

    @classmethod
    def from_api(cls, data: dict[str, Any], ticker: Optional[str] = None) -> Orderbook:
        """Construct Orderbook from raw API payload."""
        if ticker and "ticker" not in data:
            data = {**data, "ticker": ticker}
        return cls.model_validate(data)

    @property
    def best_yes_bid(self) -> Optional[int]:
        """Highest price among YES bids (in cents). None if no YES bids."""
        if not self.yes_bids:
            return None
        return max(p for p, _ in self.yes_bids)

    @property
    def best_no_bid(self) -> Optional[int]:
        """Highest price among NO bids (in cents). None if no NO bids."""
        if not self.no_bids:
            return None
        return max(p for p, _ in self.no_bids)

    @property
    def best_yes_ask(self) -> Optional[int]:
        """Lowest implied YES ask derived from best NO bid (100 - best_no_bid).

        None if no NO bids exist.
        """
        if self.best_no_bid is None:
            return None
        return 100 - self.best_no_bid

    @property
    def best_no_ask(self) -> Optional[int]:
        """Lowest implied NO ask derived from best YES bid (100 - best_yes_bid).

        None if no YES bids exist.
        """
        if self.best_yes_bid is None:
            return None
        return 100 - self.best_yes_bid

    @property
    def yes_asks(self) -> list[tuple[int, int]]:
        """All implied YES asks derived from NO bids: (100 - no_bid_price, size).

        Sorted ascending by ask price (best/lowest ask first).
        """
        asks = [(100 - p, s) for p, s in self.no_bids]
        return sorted(asks, key=lambda x: x[0])

    @property
    def no_asks(self) -> list[tuple[int, int]]:
        """All implied NO asks derived from YES bids: (100 - yes_bid_price, size).

        Sorted ascending by ask price (best/lowest ask first).
        """
        asks = [(100 - p, s) for p, s in self.yes_bids]
        return sorted(asks, key=lambda x: x[0])

    @property
    def yes_spread(self) -> Optional[int]:
        """YES side bid-ask spread in cents. None if either side is missing."""
        if self.best_yes_bid is not None and self.best_yes_ask is not None:
            return self.best_yes_ask - self.best_yes_bid
        return None

    @property
    def no_spread(self) -> Optional[int]:
        """NO side bid-ask spread in cents. None if either side is missing."""
        if self.best_no_bid is not None and self.best_no_ask is not None:
            return self.best_no_ask - self.best_no_bid
        return None


class Event(BaseModel):
    """Kalshi Event metadata model."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    event_ticker: str = Field(description="Unique event ticker (e.g. KXFEDRATE-24DEC)")
    title: str = Field(description="Event title")
    sub_title: Optional[str] = Field(default=None, description="Event subtitle")
    category: Optional[str] = Field(default=None, description="Market category (Economics, Politics, World, etc.)")
    series_ticker: Optional[str] = Field(default=None, description="Series ticker")
    mutually_exclusive: Optional[bool] = Field(default=False, description="Whether child markets are mutually exclusive")
    strike_period: Optional[str] = Field(default=None, description="Target strike period")
    markets: list[Market] = Field(default_factory=list, description="Nested child markets if present")

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> Event:
        """Construct Event instance from API payload."""
        return cls.model_validate(data)
