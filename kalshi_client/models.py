"""Pydantic data models for Kalshi API entities: Market, Orderbook, and Event."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Union
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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

        # Parse prices (handling yes_bid_dollars, yes_bid, etc.)
        yes_bid = _parse_cents(data.get("yes_bid") or data.get("yes_bid_dollars"))
        yes_ask = _parse_cents(data.get("yes_ask") or data.get("yes_ask_dollars"))
        no_bid = _parse_cents(data.get("no_bid") or data.get("no_bid_dollars"))
        no_ask = _parse_cents(data.get("no_ask") or data.get("no_ask_dollars"))

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
        description="List of (price_in_cents, size_in_contracts) bids for YES side",
    )
    no_bids: list[tuple[int, int]] = Field(
        default_factory=list,
        description="List of (price_in_cents, size_in_contracts) bids for NO side",
    )

    @model_validator(mode="before")
    @classmethod
    def extract_orderbook_data(cls, data: Any) -> Any:
        """Parse raw Kalshi orderbook response containing either orderbook_fp or orderbook or direct tuples."""
        if not isinstance(data, dict):
            return data

        ticker = data.get("ticker", "")

        # If data has nested 'orderbook_fp' (e.g. {'orderbook_fp': {'yes_dollars': [...], 'no_dollars': [...]}})
        nested_fp = data.get("orderbook_fp")
        if isinstance(nested_fp, dict):
            yes_raw = nested_fp.get("yes_dollars") or []
            no_raw = nested_fp.get("no_dollars") or []
            yes_bids = [
                (int(round(float(p) * 100)), int(round(float(s))))
                for p, s in yes_raw
                if p is not None and s is not None
            ]
            no_bids = [
                (int(round(float(p) * 100)), int(round(float(s))))
                for p, s in no_raw
                if p is not None and s is not None
            ]
            return {"ticker": ticker, "yes_bids": yes_bids, "no_bids": no_bids}

        # If data has nested 'orderbook' (e.g. {'orderbook': {'yes': [[40, 10]], 'no': [[55, 20]]}})
        nested_cents = data.get("orderbook")
        if isinstance(nested_cents, dict):
            yes_raw = nested_cents.get("yes") or []
            no_raw = nested_cents.get("no") or []
            yes_bids = [(int(p), int(s)) for p, s in yes_raw if p is not None and s is not None]
            no_bids = [(int(p), int(s)) for p, s in no_raw if p is not None and s is not None]
            return {"ticker": ticker, "yes_bids": yes_bids, "no_bids": no_bids}

        # If raw dict contains yes_dollars / no_dollars directly
        if "yes_dollars" in data or "no_dollars" in data:
            yes_raw = data.get("yes_dollars") or []
            no_raw = data.get("no_dollars") or []
            yes_bids = [
                (int(round(float(p) * 100)), int(round(float(s))))
                for p, s in yes_raw
                if p is not None and s is not None
            ]
            no_bids = [
                (int(round(float(p) * 100)), int(round(float(s))))
                for p, s in no_raw
                if p is not None and s is not None
            ]
            return {"ticker": ticker, "yes_bids": yes_bids, "no_bids": no_bids}

        # Direct yes_bids / no_bids
        return data

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
