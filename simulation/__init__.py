"""Market simulation and backtesting modules."""

from simulation.fake_market_generator import (
    DEFAULT_SYNTHETIC_PAIRS,
    FakeKalshiClient,
    FakeMarketGenerator,
    FakePolymarketClient,
    SyntheticMarketConfig,
)

__all__ = [
    "DEFAULT_SYNTHETIC_PAIRS",
    "FakeKalshiClient",
    "FakeMarketGenerator",
    "FakePolymarketClient",
    "SyntheticMarketConfig",
]
