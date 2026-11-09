"""Configuration loader and validator for kalshi-arb."""

from __future__ import annotations

import os
import re
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


class TradingMode(str, Enum):
    DRY_RUN = "dry_run"
    SANDBOX = "sandbox"
    LIVE = "live"


class DataMode(str, Enum):
    SIMULATION = "simulation"
    REAL = "real"


class ModeConfig(BaseModel):
    trading_mode: TradingMode = Field(
        default=TradingMode.DRY_RUN,
        description="Execution mode: dry_run, sandbox, or live",
    )
    data_mode: DataMode = Field(
        default=DataMode.SIMULATION,
        description="Market data source mode: simulation or real",
    )


class KalshiConfig(BaseModel):
    base_url_prod: str = Field(
        default="https://api.elections.kalshi.com/trade-api/v2",
        description="Kalshi production API base URL",
    )
    base_url_sandbox: str = Field(
        default="https://demo-api.kalshi.co/trade-api/v2",
        description="Kalshi sandbox API base URL",
    )
    key_id: Optional[str] = Field(
        default=None,
        description="Kalshi API Key ID (read from environment)",
    )
    private_key_path: Optional[str] = Field(
        default=None,
        description="Path to Kalshi RSA private key PEM file (read from environment)",
    )

    @field_validator("base_url_prod", "base_url_sandbox")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError(f"URL must start with http:// or https://: {v}")
        return v.rstrip("/")


class PolymarketConfig(BaseModel):
    gamma_url: str = Field(
        default="https://gamma-api.polymarket.com",
        description="Polymarket Gamma API endpoint",
    )
    clob_url: str = Field(
        default="https://clob.polymarket.com",
        description="Polymarket CLOB API endpoint",
    )

    @field_validator("gamma_url", "clob_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError(f"URL must start with http:// or https://: {v}")
        return v.rstrip("/")


class TradingConfig(BaseModel):
    min_edge: float = Field(
        default=0.02,
        ge=0.0,
        description="Minimum arbitrage profit edge threshold (e.g. 0.02 = 2%)",
    )
    default_order_size: int = Field(
        default=10,
        gt=0,
        description="Default contract size for generated orders",
    )
    maker_fee_bps: float = Field(
        default=0.0,
        ge=0.0,
        description="Maker fee in basis points",
    )
    taker_fee_bps: float = Field(
        default=0.0,
        ge=0.0,
        description="Taker fee in basis points",
    )
    polygon_gas_estimate_cents: float = Field(
        default=0.0,
        ge=0.0,
        description="Estimated Polygon network gas cost per Polymarket trade in cents",
    )


class RiskConfig(BaseModel):
    max_position_per_market: int = Field(
        default=100,
        gt=0,
        description="Maximum contract exposure per individual market",
    )
    max_global_exposure: float = Field(
        default=1000.0,
        gt=0.0,
        description="Maximum total portfolio exposure in USD",
    )
    max_daily_loss: float = Field(
        default=100.0,
        gt=0.0,
        description="Maximum allowable daily loss threshold in USD",
    )


class MatchingConfig(BaseModel):
    min_similarity: float = Field(
        default=0.85,
        ge=0.0,
        le=1.0,
        description="Minimum string similarity ratio (0.0 to 1.0) for cross-platform event matching",
    )
    overrides_path: str = Field(
        default="config/market_overrides.yaml",
        description="Path to manual market overrides YAML file",
    )
    overrides_only: bool = Field(
        default=False,
        description="If True, only scan markets in market_overrides.yaml and skip fuzzy similarity matching",
    )


class PairRegistryConfig(BaseModel):
    min_verified_pairs: int = Field(default=10, ge=0, description="Minimum verified pairs (error in strict mode)")
    max_verified_pairs: int = Field(default=20, ge=1, description="Maximum verified pairs (error in strict mode)")
    min_categories: int = Field(default=3, ge=0, description="Minimum distinct categories among verified pairs")
    gate_interval_minutes: float = Field(
        default=30.0, gt=0, description="How often the collection gate re-checks each verified pair's tradability"
    )
    close_time_mismatch_hours: float = Field(
        default=24.0, ge=0, description="Warn when Kalshi/Polymarket expected close times differ by more than this"
    )

    @model_validator(mode="after")
    def _check_bounds(self) -> PairRegistryConfig:
        if self.min_verified_pairs > self.max_verified_pairs:
            raise ValueError("min_verified_pairs must be <= max_verified_pairs")
        return self


class DepthConfig(BaseModel):
    depth_levels: int = Field(default=10, ge=1, description="Top N book levels stored per side")
    vwap_sizes: list[int] = Field(default_factory=lambda: [1, 10, 50], description="Contract sizes for VWAP/fillability")
    ws_reconcile_seconds: float = Field(
        default=300.0, gt=0, description="Period of full REST snapshot reconciliation of WebSocket-maintained books"
    )

    @field_validator("vwap_sizes")
    @classmethod
    def _positive_sizes(cls, v: list[int]) -> list[int]:
        if not v or any(s <= 0 for s in v):
            raise ValueError("vwap_sizes must be a non-empty list of positive integers")
        return v


class DataQualityConfig(BaseModel):
    snapshot_interval_seconds: float = Field(default=60.0, gt=0, description="Expected seconds between snapshots")
    gap_threshold_seconds: float = Field(default=300.0, gt=0, description="Gaps longer than this are reported")
    stale_quote_seconds: float = Field(
        default=60.0, ge=0, description="exchange_updated_at older than this vs observed_at counts as stale"
    )


class AppConfig(BaseModel):
    mode: ModeConfig
    kalshi: KalshiConfig
    polymarket: PolymarketConfig
    trading: TradingConfig
    risk: RiskConfig
    matching: MatchingConfig
    pair_registry: PairRegistryConfig = Field(default_factory=PairRegistryConfig)
    depth: DepthConfig = Field(default_factory=DepthConfig)
    data_quality: DataQualityConfig = Field(default_factory=DataQualityConfig)

    @model_validator(mode="after")
    def validate_environment_credentials(self) -> AppConfig:
        """Validate that live and sandbox modes have required credentials configured."""
        trading_mode = self.mode.trading_mode
        if trading_mode == TradingMode.LIVE:
            if not self.kalshi.key_id:
                raise ValueError(
                    "KALSHI_KEY_ID environment variable is required when trading_mode is 'live'"
                )
            if not self.kalshi.private_key_path:
                raise ValueError(
                    "KALSHI_PRIVATE_KEY_PATH environment variable is required when trading_mode is 'live'"
                )
        elif trading_mode == TradingMode.SANDBOX:
            # Fallback to sandbox specific env vars if not set
            if not self.kalshi.key_id:
                sandbox_key = os.getenv("KALSHI_SANDBOX_KEY_ID")
                if sandbox_key:
                    self.kalshi.key_id = sandbox_key
            if not self.kalshi.private_key_path:
                sandbox_key_path = os.getenv("KALSHI_SANDBOX_PRIVATE_KEY_PATH")
                if sandbox_key_path:
                    self.kalshi.private_key_path = sandbox_key_path

            if not self.kalshi.key_id:
                raise ValueError(
                    "KALSHI_KEY_ID or KALSHI_SANDBOX_KEY_ID is required when trading_mode is 'sandbox'"
                )
            if not self.kalshi.private_key_path:
                raise ValueError(
                    "KALSHI_PRIVATE_KEY_PATH or KALSHI_SANDBOX_PRIVATE_KEY_PATH is required when trading_mode is 'sandbox'"
                )
        return self


def _substitute_env_vars(obj: Any) -> Any:
    """Recursively expand ${VAR} and $VAR patterns in configuration strings using environment variables."""
    if isinstance(obj, str):
        pattern = re.compile(r"\$\{(\w+)\}|\$(\w+)")

        def replace_match(match: re.Match) -> str:
            var_name = match.group(1) or match.group(2)
            return os.getenv(var_name, "")

        substituted = pattern.sub(replace_match, obj)
        return substituted if substituted != "" else None
    elif isinstance(obj, dict):
        return {k: _substitute_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_substitute_env_vars(elem) for elem in obj]
    return obj


_dotenv_loaded = False


def load_config(config_path: Optional[str | Path] = None) -> AppConfig:
    """Load and validate config.yaml with environment variable substitutions.

    Fails loudly with detailed ValidationError if required sections/fields are missing or invalid.
    """
    global _dotenv_loaded
    if not _dotenv_loaded:
        load_dotenv()
        _dotenv_loaded = True

    if config_path is None:
        # Default to config.yaml in the project root
        base_dir = Path(__file__).resolve().parent.parent
        config_path = base_dir / "config.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw_data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse YAML from {config_path}: {e}") from e

    if not isinstance(raw_data, dict):
        raise ValueError(f"Invalid YAML structure in {config_path}: expected dictionary mapping")

    resolved_data = _substitute_env_vars(raw_data)

    try:
        config = AppConfig.model_validate(resolved_data)
        return config
    except ValidationError as e:
        raise ValueError(f"Configuration validation failed for {config_path}:\n{e}") from e
