"""Unit tests for config_loader."""

import os
import tempfile
from pathlib import Path
import pytest
import yaml

from core.config_loader import AppConfig, TradingMode, DataMode, load_config


def test_load_default_config():
    """Verify that default config.yaml loads properly."""
    config = load_config()
    assert isinstance(config, AppConfig)
    assert config.mode.trading_mode == TradingMode.DRY_RUN
    assert config.mode.data_mode == DataMode.SIMULATION
    assert config.trading.min_edge == 0.02
    assert config.trading.default_order_size == 10
    assert config.risk.max_position_per_market == 100
    assert config.risk.max_global_exposure == 1000.0
    assert config.risk.max_daily_loss == 100.0
    assert config.matching.min_similarity == 0.85
    assert "kalshi.com" in config.kalshi.base_url_prod
    assert "polymarket.com" in config.polymarket.gamma_url


def test_missing_required_section_fails_loudly():
    """Verify that omitting a required section raises ValueError."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump({"mode": {"trading_mode": "dry_run"}}, f)
        temp_path = f.name

    try:
        with pytest.raises(ValueError) as exc_info:
            load_config(temp_path)
        assert "Configuration validation failed" in str(exc_info.value)
    finally:
        os.remove(temp_path)


def test_live_mode_requires_credentials(monkeypatch):
    """Verify that live mode raises an error if Kalshi credentials are missing."""
    monkeypatch.delenv("KALSHI_KEY_ID", raising=False)
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PATH", raising=False)

    sample_config = {
        "mode": {"trading_mode": "live", "data_mode": "real"},
        "kalshi": {
            "base_url_prod": "https://api.elections.kalshi.com/trade-api/v2",
            "base_url_sandbox": "https://demo-api.kalshi.co/trade-api/v2",
            "key_id": "${KALSHI_KEY_ID}",
            "private_key_path": "${KALSHI_PRIVATE_KEY_PATH}",
        },
        "polymarket": {
            "gamma_url": "https://gamma-api.polymarket.com",
            "clob_url": "https://clob.polymarket.com",
        },
        "trading": {
            "min_edge": 0.02,
            "default_order_size": 10,
            "maker_fee_bps": 0.0,
            "taker_fee_bps": 0.0,
        },
        "risk": {
            "max_position_per_market": 100,
            "max_global_exposure": 1000.0,
            "max_daily_loss": 100.0,
        },
        "matching": {"min_similarity": 0.85},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(sample_config, f)
        temp_path = f.name

    try:
        with pytest.raises(ValueError) as exc_info:
            load_config(temp_path)
        assert "KALSHI_KEY_ID environment variable is required" in str(exc_info.value)
    finally:
        os.remove(temp_path)


def test_env_var_substitution(monkeypatch):
    """Verify that environment variables are interpolated correctly."""
    monkeypatch.setenv("KALSHI_KEY_ID", "test_key_123")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", "/path/to/key.pem")

    sample_config = {
        "mode": {"trading_mode": "live", "data_mode": "real"},
        "kalshi": {
            "base_url_prod": "https://api.elections.kalshi.com/trade-api/v2",
            "base_url_sandbox": "https://demo-api.kalshi.co/trade-api/v2",
            "key_id": "${KALSHI_KEY_ID}",
            "private_key_path": "${KALSHI_PRIVATE_KEY_PATH}",
        },
        "polymarket": {
            "gamma_url": "https://gamma-api.polymarket.com",
            "clob_url": "https://clob.polymarket.com",
        },
        "trading": {
            "min_edge": 0.02,
            "default_order_size": 10,
            "maker_fee_bps": 0.0,
            "taker_fee_bps": 0.0,
        },
        "risk": {
            "max_position_per_market": 100,
            "max_global_exposure": 1000.0,
            "max_daily_loss": 100.0,
        },
        "matching": {"min_similarity": 0.85},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(sample_config, f)
        temp_path = f.name

    try:
        config = load_config(temp_path)
        assert config.kalshi.key_id == "test_key_123"
        assert config.kalshi.private_key_path == "/path/to/key.pem"
    finally:
        os.remove(temp_path)
