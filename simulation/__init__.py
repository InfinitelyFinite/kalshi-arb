"""Market simulation and backtesting modules."""

from simulation.fake_market_generator import (
    DEFAULT_SYNTHETIC_PAIRS,
    FakeKalshiClient,
    FakeMarketGenerator,
    FakePolymarketClient,
    SyntheticMarketConfig,
)
from simulation.replay_engine import (
    OpportunityRecord,
    ReplayEngine,
    ReplaySummary,
)

__all__ = [
    "DEFAULT_SYNTHETIC_PAIRS",
    "FakeKalshiClient",
    "FakeMarketGenerator",
    "FakePolymarketClient",
    "OpportunityRecord",
    "ReplayEngine",
    "ReplaySummary",
    "SyntheticMarketConfig",
]

