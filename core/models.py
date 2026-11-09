"""Domain models for prediction market trading and arbitrage detection.

Unit conventions (internal, enforced at the client boundary):
- **Prices are in cents, 0 to 100** (``Decimal`` where exactness matters, e.g. DuckDB depth tables; legacy
  integer fields such as ``yes_ask`` are rounded cents). Never dollars, never 0..1 fractions.
- **Sizes are in contracts** (``Decimal``; Polymarket sizes can be fractional).
- Source formats normalised at the boundary:
  * Kalshi REST ``orderbook_fp``: ``yes_dollars``/``no_dollars`` = [["0.4500", "10.00"]] -> dollars -> *100 -> cents;
    legacy ``orderbook``: [[45, 10]] already cents. Kalshi books are BIDS ONLY; asks are ``100 - opposite bid``.
  * Kalshi WebSocket: ``*_dollars*`` fields are dollars, ``yes``/``no``/``price`` legacy fields are cents.
  * Polymarket CLOB ``/book``: {"price": "0.485", "size": "30.5"} -> dollars -> cents (sub-cent ticks allowed).
    Each outcome token has its OWN book (bids and asks); NO levels come from the NO token, not from the YES book.
- Timestamps are UTC. ``observed_at`` = when this app received the data; ``exchange_updated_at`` = the platform's
  own timestamp when supplied, otherwise ``None`` (never backfilled with ``observed_at``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, Field


SCHEMA_VERSION = 2


class MarketStatus(str, Enum):
    """Normalised market status stored with every snapshot."""

    OPEN = "open"
    CLOSED = "closed"
    SETTLED = "settled"
    PAUSED = "paused"
    UNKNOWN = "unknown"


class DataSource(str, Enum):
    """Provenance of a snapshot row. Only ``live_api`` rows feed research analysis."""

    LIVE_API = "live_api"
    REPLAY = "replay"
    SYNTHETIC = "synthetic"
    LEGACY_UNVERIFIED = "legacy_unverified"


_KALSHI_STATUS_MAP = {
    "open": MarketStatus.OPEN,
    "active": MarketStatus.OPEN,
    "closed": MarketStatus.CLOSED,
    "determined": MarketStatus.SETTLED,
    "finalized": MarketStatus.SETTLED,
    "settled": MarketStatus.SETTLED,
    "resolved": MarketStatus.SETTLED,
    "inactive": MarketStatus.PAUSED,
    "paused": MarketStatus.PAUSED,
    "initialized": MarketStatus.PAUSED,
}


def normalize_market_status(
    raw_status: Optional[str],
    *,
    accepting_orders: Optional[bool] = None,
    archived: Optional[bool] = None,
) -> MarketStatus:
    """Map platform status strings (and Polymarket order-acceptance flags) to ``MarketStatus``.

    Missing/unrecognised information yields ``UNKNOWN`` (which is never tradable), not ``OPEN``.
    """
    if archived:
        return MarketStatus.CLOSED
    if raw_status is None:
        return MarketStatus.UNKNOWN
    status = _KALSHI_STATUS_MAP.get(str(raw_status).strip().lower(), MarketStatus.UNKNOWN)
    if status is MarketStatus.OPEN and accepting_orders is False:
        return MarketStatus.PAUSED
    return status


def is_tradable(status: MarketStatus) -> bool:
    """A market is tradable only when its normalised status is OPEN."""
    return status is MarketStatus.OPEN


class OpportunityLeg(BaseModel):
    """Execution leg of an arbitrage opportunity.

    Attributes:
        platform: Platform for this leg ('kalshi' or 'polymarket').
        ticker: Market ticker or token/condition identifier.
        side: Trading side action ('buy_yes' or 'buy_no').
        price_cents: Ask price in cents.
        fee_cents: Estimated transaction/gas fee in cents for this leg.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    platform: str = Field(description="Execution platform ('kalshi' or 'polymarket')")
    ticker: str = Field(description="Market identifier, ticker, condition ID, or token ID")
    side: str = Field(description="Execution action ('buy_yes' or 'buy_no')")
    price_cents: float = Field(description="Purchase ask price in cents")
    fee_cents: float = Field(default=0.0, description="Estimated fee in cents for this leg")


class Opportunity(BaseModel):
    """Arbitrage opportunity model shared across intra-market and cross-platform engines.

    Attributes:
        platform: Primary platform where the trade executes ('kalshi', 'polymarket').
        ticker: Market ticker or asset identifier.
        side_actions: Side actions to execute arbitrage, e.g. ['buy_yes', 'buy_no'].
        edge_cents: Net arbitrage profit edge in cents after subtracting estimated fees.
        timestamp: Time the opportunity was detected.
        orderbook_snapshot: Preserved market prices and book state for audit and replay.
        target_platform: Secondary platform identifier (for cross-platform arbitrage).
        target_ticker: Secondary ticker identifier (for cross-platform arbitrage).
        gross_edge_cents: Gross profit edge in cents before fees.
        estimated_fees_cents: Total estimated transaction fees in cents.
        strategy: Strategy name ('intra_market' or 'cross_platform').
        legs: List of execution legs specifying platform, ticker, side, price, and leg fee.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    platform: str = Field(description="Primary execution platform ('kalshi', 'polymarket')")
    ticker: str = Field(description="Market identifier or ticker")
    side_actions: list[str] = Field(
        default_factory=lambda: ["buy_yes", "buy_no"],
        description="List of side actions to execute arbitrage, e.g. ['buy_yes', 'buy_no']",
    )
    edge_cents: float = Field(description="Net arbitrage profit edge in cents after fees")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when the opportunity was detected",
    )
    orderbook_snapshot: dict[str, Any] = Field(
        default_factory=dict,
        description="Orderbook / market price snapshot for audit and execution verification",
    )
    target_platform: Optional[str] = Field(
        default=None,
        description="Secondary platform for cross-platform arbitrage",
    )
    target_ticker: Optional[str] = Field(
        default=None,
        description="Secondary market ticker for cross-platform arbitrage",
    )
    gross_edge_cents: Optional[float] = Field(
        default=None,
        description="Gross edge in cents before subtracting fees",
    )
    estimated_fees_cents: Optional[float] = Field(
        default=0.0,
        description="Estimated transaction fees in cents",
    )
    strategy: str = Field(
        default="intra_market",
        description="Arbitrage strategy type ('intra_market' or 'cross_platform')",
    )
    legs: list[OpportunityLeg] = Field(
        default_factory=list,
        description="Explicit trade legs for multi-leg or cross-platform execution",
    )


class Market(BaseModel):
    """Unified prediction market model."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    platform: str = Field(default="kalshi", description="Platform identifier ('kalshi', 'polymarket')")
    ticker: str = Field(description="Market identifier / ticker")
    title: Optional[str] = Field(default=None, description="Market title / question")
    yes_bid: Optional[int] = Field(default=None, description="Best YES bid in cents (0-100)")
    yes_ask: Optional[int] = Field(default=None, description="Best YES ask in cents (0-100)")
    no_bid: Optional[int] = Field(default=None, description="Best NO bid in cents (0-100)")
    no_ask: Optional[int] = Field(default=None, description="Best NO ask in cents (0-100)")
    volume: Optional[int] = Field(default=0, description="Volume")
    status: Optional[str] = Field(default="open", description="Market status")
    raw_orderbook: Optional[Any] = Field(default=None, description="Raw orderbook instance if available")
    ts: Optional[datetime] = Field(default=None, description="Market timestamp")

    @property
    def best_yes_ask(self) -> Optional[int]:
        return self.yes_ask

    @property
    def best_no_ask(self) -> Optional[int]:
        return self.no_ask

    @property
    def best_yes_bid(self) -> Optional[int]:
        return self.yes_bid

    @property
    def best_no_bid(self) -> Optional[int]:
        return self.no_bid
