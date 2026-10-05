"""Domain models for prediction market trading and arbitrage detection."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, Field


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
