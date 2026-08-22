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
        gt=0.0,
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


class AppConfig(BaseModel):
    mode: ModeConfig
    kalshi: KalshiConfig
    polymarket: PolymarketConfig
    trading: TradingConfig
    risk: RiskConfig
    matching: MatchingConfig

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


def load_config(config_path: Optional[str | Path] = None) -> AppConfig:
    """Load and validate config.yaml with environment variable substitutions.

    Fails loudly with detailed ValidationError if required sections/fields are missing or invalid.
    """
    load_dotenv()

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
