"""Pydantic data models for Polymarket API entities: Market, Orderbook, and Token.

Mirrors the Market and Orderbook models from kalshi_client/models.py to ensure
consistent downstream interface across prediction market platforms.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional, Union
from pydantic import BaseModel, ConfigDict, Field, model_validator


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
    """Helper to convert volume strings/floats to integer contracts / dollar amount."""
    if val is None or val == "":
        return None
    try:
        return int(round(float(val)))
    except (ValueError, TypeError):
        return None


def _parse_json_list(val: Any) -> list[Any]:
    """Helper to parse JSON string list or return list directly."""
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, list):
                return parsed
        except (ValueError, json.JSONDecodeError):
            pass
    return []


class Token(BaseModel):
    """Polymarket Outcome Token."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    token_id: str = Field(description="Unique token asset ID in Polymarket CLOB")
    outcome: str = Field(description="Outcome label (e.g. 'Yes', 'No', team name)")
    price: Optional[float] = Field(default=None, description="Current price in decimal dollars (0.0 to 1.0)")
    winner: Optional[bool] = Field(default=None, description="Whether token resolved as winner")


class Market(BaseModel):
    """Polymarket Market data model.

    Mirrors Kalshi Market field names (ticker, title, yes_bid, yes_ask, no_bid,
    no_ask, volume, status, close_time) to avoid per-platform branching in trading code.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    condition_id: str = Field(description="Unique Polymarket condition ID (0x...)")
    ticker: str = Field(description="Market identifier alias for Kalshi parity (condition_id or slug)")
    title: str = Field(description="Human-readable title/question describing the prediction market")
    tokens: list[Token] = Field(default_factory=list, description="Associated outcome tokens")
    clob_token_ids: list[str] = Field(default_factory=list, description="List of CLOB token asset IDs")
    outcomes: list[str] = Field(default_factory=list, description="List of outcome names (e.g. ['Yes', 'No'])")
    outcome_prices: list[float] = Field(default_factory=list, description="List of outcome decimal prices")
    yes_bid: Optional[int] = Field(default=None, description="Best YES bid in cents (1-99)")
    yes_ask: Optional[int] = Field(default=None, description="Best YES ask in cents (1-99)")
    no_bid: Optional[int] = Field(default=None, description="Best NO bid in cents (1-99)")
    no_ask: Optional[int] = Field(default=None, description="Best NO ask in cents (1-99)")
    volume: Optional[int] = Field(default=0, description="Total volume or 24h volume in USD/contracts")
    status: Optional[str] = Field(default="open", description="Market status (open, closed, active, resolved)")
    close_time: Optional[Union[datetime, str]] = Field(default=None, description="Market expiration or close timestamp")
    slug: Optional[str] = Field(default=None, description="URL-friendly market slug")
    description: Optional[str] = Field(default=None, description="Detailed resolution rules/description")
    event_ticker: Optional[str] = Field(default=None, description="Parent event ID or slug")

    @property
    def yes_token_id(self) -> Optional[str]:
        """Convenience property for the YES outcome token ID."""
        for tok in self.tokens:
            if tok.outcome.strip().lower() == "yes":
                return tok.token_id
        if self.clob_token_ids:
            return self.clob_token_ids[0]
        return None

    @property
    def no_token_id(self) -> Optional[str]:
        """Convenience property for the NO outcome token ID."""
        for tok in self.tokens:
            if tok.outcome.strip().lower() == "no":
                return tok.token_id
        if len(self.clob_token_ids) > 1:
            return self.clob_token_ids[1]
        return None

    @model_validator(mode="before")
    @classmethod
    def extract_gamma_fields(cls, data: Any) -> Any:
        """Extract and normalize fields from raw Gamma API market responses."""
        if not isinstance(data, dict):
            return data

        condition_id = (
            data.get("conditionId")
            or data.get("condition_id")
            or data.get("id")
            or ""
        )
        slug = data.get("slug")
        ticker = data.get("ticker") or condition_id or slug or ""
        title = data.get("question") or data.get("title") or data.get("groupItemTitle") or ""

        # Parse outcomes, outcomePrices, clobTokenIds (often json-encoded strings in Gamma API)
        raw_outcomes = _parse_json_list(data.get("outcomes"))
        raw_prices = _parse_json_list(data.get("outcomePrices"))
        raw_tokens = data.get("tokens")
        raw_clob_tokens = _parse_json_list(data.get("clobTokenIds"))

        parsed_outcomes = [str(o) for o in raw_outcomes] if raw_outcomes else []
        parsed_prices: list[float] = []
        for p in raw_prices:
            try:
                parsed_prices.append(float(p))
            except (ValueError, TypeError):
                pass

        parsed_clob_tokens = [str(t) for t in raw_clob_tokens] if raw_clob_tokens else []

        # Parse tokens list
        tokens_list: list[Token] = []
        if isinstance(raw_tokens, list) and raw_tokens and isinstance(raw_tokens[0], dict):
            for t_item in raw_tokens:
                t_id = str(t_item.get("token_id") or t_item.get("tokenId") or t_item.get("id") or "")
                t_outcome = str(t_item.get("outcome") or "")
                t_price = None
                if t_item.get("price") is not None:
                    try:
                        t_price = float(t_item["price"])
                    except (ValueError, TypeError):
                        pass
                if t_id:
                    tokens_list.append(
                        Token(
                            token_id=t_id,
                            outcome=t_outcome,
                            price=t_price,
                            winner=t_item.get("winner"),
                        )
                    )
        elif parsed_clob_tokens:
            for idx, t_id in enumerate(parsed_clob_tokens):
                t_outcome = parsed_outcomes[idx] if idx < len(parsed_outcomes) else f"Outcome {idx+1}"
                t_price = parsed_prices[idx] if idx < len(parsed_prices) else None
                tokens_list.append(
                    Token(
                        token_id=t_id,
                        outcome=t_outcome,
                        price=t_price,
                    )
                )

        # Parse prices in cents
        best_bid_raw = data.get("bestBid") or data.get("yes_bid")
        best_ask_raw = data.get("bestAsk") or data.get("yes_ask")

        yes_bid = _parse_cents(best_bid_raw)
        yes_ask = _parse_cents(best_ask_raw)

        # Fallback to outcomePrices if bestBid/bestAsk missing
        if yes_bid is None and parsed_prices:
            yes_bid = _parse_cents(parsed_prices[0])
        if yes_ask is None and yes_bid is not None:
            # Implied from outcomePrices[0]
            yes_ask = yes_bid

        # Derive NO prices
        no_bid = (100 - yes_ask) if yes_ask is not None else None
        no_ask = (100 - yes_bid) if yes_bid is not None else None
        if len(parsed_prices) > 1 and no_bid is None:
            no_bid = _parse_cents(parsed_prices[1])
            no_ask = no_bid

        # Parse volume
        volume = _parse_volume(
            data.get("volumeNum")
            or data.get("volume")
            or data.get("volume24hr")
            or data.get("volumeClob")
        )

        # Parse status
        is_closed = data.get("closed", False)
        is_active = data.get("active", True)
        if is_closed:
            status = "closed"
        elif is_active:
            status = "open"
        else:
            status = "inactive"

        close_time = data.get("endDate") or data.get("endDateIso") or data.get("close_time")
        description = data.get("description")

        # Parent event
        event_ticker = None
        if isinstance(data.get("events"), list) and data["events"]:
            event_obj = data["events"][0]
            event_ticker = event_obj.get("ticker") or event_obj.get("slug") or str(event_obj.get("id", ""))

        return {
            "condition_id": condition_id,
            "ticker": ticker,
            "title": title,
            "tokens": tokens_list,
            "clob_token_ids": parsed_clob_tokens,
            "outcomes": parsed_outcomes,
            "outcome_prices": parsed_prices,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "volume": volume or 0,
            "status": status,
            "close_time": close_time,
            "slug": slug,
            "description": description,
            "event_ticker": event_ticker,
        }

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> Market:
        """Construct Market instance from raw Polymarket Gamma API dictionary."""
        return cls.model_validate(data)


class Orderbook(BaseModel):
    """Polymarket Orderbook model.

    Mirrors the Kalshi Orderbook interface and properties (best_yes_bid, best_yes_ask,
    best_no_bid, best_no_ask, yes_spread, no_spread, yes_bids, no_bids, yes_asks, no_asks)
    so downstream arbitrage and order-routing engines do not require per-platform branching.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    ticker: str = Field(description="Token asset ID or market identifier")
    token_id: str = Field(description="Polymarket CLOB token asset ID")
    condition_id: Optional[str] = Field(default=None, description="Parent condition ID / market address")
    yes_bids: list[tuple[int, int]] = Field(
        default_factory=list,
        description="List of (price_in_cents, size_in_contracts) bids for YES side",
    )
    yes_asks: list[tuple[int, int]] = Field(
        default_factory=list,
        description="List of (price_in_cents, size_in_contracts) asks for YES side",
    )
    no_bids: list[tuple[int, int]] = Field(
        default_factory=list,
        description="List of (price_in_cents, size_in_contracts) bids for NO side",
    )
    no_asks: list[tuple[int, int]] = Field(
        default_factory=list,
        description="List of (price_in_cents, size_in_contracts) asks for NO side",
    )

    @model_validator(mode="before")
    @classmethod
    def extract_clob_orderbook(cls, data: Any) -> Any:
        """Parse raw Polymarket CLOB orderbook response:

        {
            "market": "0x...",
            "asset_id": "...",
            "bids": [{"price": "0.45", "size": "100"}, ...],
            "asks": [{"price": "0.50", "size": "200"}, ...]
        }
        """
        if not isinstance(data, dict):
            return data

        token_id = str(data.get("asset_id") or data.get("token_id") or data.get("ticker") or "")
        condition_id = data.get("market") or data.get("condition_id")
        ticker = str(data.get("ticker") or token_id or condition_id or "")

        raw_bids = data.get("bids") if "bids" in data else data.get("yes_bids", [])
        raw_asks = data.get("asks") if "asks" in data else data.get("yes_asks", [])

        # Parse bids into (price_in_cents, size_in_contracts)
        parsed_yes_bids: list[tuple[int, int]] = []
        for b in raw_bids:
            if isinstance(b, dict):
                p = _parse_cents(b.get("price"))
                s = _parse_volume(b.get("size"))
            elif isinstance(b, (list, tuple)) and len(b) >= 2:
                p = _parse_cents(b[0])
                s = _parse_volume(b[1])
            else:
                continue
            if p is not None and s is not None and s > 0:
                parsed_yes_bids.append((p, s))

        # Parse asks into (price_in_cents, size_in_contracts)
        parsed_yes_asks: list[tuple[int, int]] = []
        for a in raw_asks:
            if isinstance(a, dict):
                p = _parse_cents(a.get("price"))
                s = _parse_volume(a.get("size"))
            elif isinstance(a, (list, tuple)) and len(a) >= 2:
                p = _parse_cents(a[0])
                s = _parse_volume(a[1])
            else:
                continue
            if p is not None and s is not None and s > 0:
                parsed_yes_asks.append((p, s))

        # Sort YES bids descending by price (highest bid first)
        parsed_yes_bids.sort(key=lambda x: x[0], reverse=True)
        # Sort YES asks ascending by price (lowest ask first)
        parsed_yes_asks.sort(key=lambda x: x[0])

        # If direct no_bids / no_asks provided (e.g. if combined)
        raw_no_bids = data.get("no_bids", [])
        raw_no_asks = data.get("no_asks", [])

        if raw_no_bids:
            parsed_no_bids = []
            for b in raw_no_bids:
                if isinstance(b, (list, tuple)) and len(b) >= 2:
                    p = _parse_cents(b[0])
                    s = _parse_volume(b[1])
                    if p is not None and s is not None:
                        parsed_no_bids.append((p, s))
            parsed_no_bids.sort(key=lambda x: x[0], reverse=True)
        else:
            # Derive NO bids from YES asks: NO bid = 100 - YES ask
            parsed_no_bids = [(100 - p, s) for p, s in parsed_yes_asks]
            parsed_no_bids.sort(key=lambda x: x[0], reverse=True)

        if raw_no_asks:
            parsed_no_asks = []
            for a in raw_no_asks:
                if isinstance(a, (list, tuple)) and len(a) >= 2:
                    p = _parse_cents(a[0])
                    s = _parse_volume(a[1])
                    if p is not None and s is not None:
                        parsed_no_asks.append((p, s))
            parsed_no_asks.sort(key=lambda x: x[0])
        else:
            # Derive NO asks from YES bids: NO ask = 100 - YES bid
            parsed_no_asks = [(100 - p, s) for p, s in parsed_yes_bids]
            parsed_no_asks.sort(key=lambda x: x[0])

        return {
            "ticker": ticker,
            "token_id": token_id,
            "condition_id": condition_id,
            "yes_bids": parsed_yes_bids,
            "yes_asks": parsed_yes_asks,
            "no_bids": parsed_no_bids,
            "no_asks": parsed_no_asks,
        }

    @classmethod
    def from_api(cls, data: dict[str, Any], token_id: Optional[str] = None) -> Orderbook:
        """Construct Orderbook from raw CLOB API payload."""
        if token_id and "asset_id" not in data and "token_id" not in data:
            data = {**data, "token_id": token_id, "ticker": token_id}
        return cls.model_validate(data)

    @property
    def bids(self) -> list[tuple[int, int]]:
        """Alias for yes_bids."""
        return self.yes_bids

    @property
    def asks(self) -> list[tuple[int, int]]:
        """Alias for yes_asks."""
        return self.yes_asks

    @property
    def best_yes_bid(self) -> Optional[int]:
        """Highest price among YES bids (in cents). None if no YES bids."""
        if not self.yes_bids:
            return None
        return max(p for p, _ in self.yes_bids)

    @property
    def best_yes_ask(self) -> Optional[int]:
        """Lowest price among YES asks (in cents). None if no YES asks."""
        if not self.yes_asks:
            return None
        return min(p for p, _ in self.yes_asks)

    @property
    def best_no_bid(self) -> Optional[int]:
        """Highest price among NO bids (in cents). Derived as (100 - best_yes_ask) if yes_asks present."""
        if self.best_yes_ask is not None:
            return 100 - self.best_yes_ask
        if self.no_bids:
            return max(p for p, _ in self.no_bids)
        return None

    @property
    def best_no_ask(self) -> Optional[int]:
        """Lowest price among NO asks (in cents). Derived as (100 - best_yes_bid) if yes_bids present."""
        if self.best_yes_bid is not None:
            return 100 - self.best_yes_bid
        if self.no_asks:
            return min(p for p, _ in self.no_asks)
        return None

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
