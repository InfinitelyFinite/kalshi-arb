"""Synthetic market and orderbook generator for prediction market simulation.

Provides `FakeMarketGenerator` and platform client adapters (`FakeKalshiClient`,
`FakePolymarketClient`) that mirror real Kalshi and Polymarket REST clients.
Supports realistic random walks and deterministic injection of intra-market and
cross-platform arbitrage mispricings on configurable intervals.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import random
from typing import Any, Optional, Sequence

from kalshi_client.models import Market as KalshiMarket, Orderbook as KalshiOrderbook
from polymarket_client.models import Market as PolyMarket, Orderbook as PolyOrderbook

logger = logging.getLogger(__name__)


@dataclass
class SyntheticMarketConfig:
    """Configuration for a synthetic market pair across Kalshi and Polymarket."""

    pair_id: str
    title: str
    kalshi_ticker: str
    kalshi_event_ticker: str
    poly_condition_id: str
    poly_token_id: str
    poly_slug: str
    initial_price: int  # Mid-price in cents (5-95)
    spread: int = 2  # Bid-ask spread in cents


# Pre-configured synthetic market pairs aligned with manual overrides and realistic topics
DEFAULT_SYNTHETIC_PAIRS: list[SyntheticMarketConfig] = [
    SyntheticMarketConfig(
        pair_id="fed_cut",
        title="Federal Reserve interest rate target decision for May 2026 meeting",
        kalshi_ticker="FED-26MAY-T500",
        kalshi_event_ticker="FED-26MAY",
        poly_condition_id="0x8f34a123bc901e4a77d562145b23e89fbc321456a7d8e9f0123456789abcdef0",
        poly_token_id="0x8f34a123bc901e4a77d562145b23e89fbc321456a7d8e9f0123456789abcdef0_yes",
        poly_slug="fed-interest-rate-may-2026",
        initial_price=52,
    ),
    SyntheticMarketConfig(
        pair_id="pres_2028_dem",
        title="Democratic Party nominee for 2028 US Presidential Election",
        kalshi_ticker="PRES-2028-DEM",
        kalshi_event_ticker="PRES-2028",
        poly_condition_id="0x1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b",
        poly_token_id="0x1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b_yes",
        poly_slug="democratic-nominee-2028",
        initial_price=40,
    ),
    SyntheticMarketConfig(
        pair_id="btc_100k",
        title="Bitcoin price reaches or exceeds $100,000 before end of 2026",
        kalshi_ticker="BTC-100K-2026",
        kalshi_event_ticker="BTC-2026",
        poly_condition_id="0x3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d",
        poly_token_id="0x3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d_yes",
        poly_slug="bitcoin-100k-end-of-2026",
        initial_price=68,
    ),
    SyntheticMarketConfig(
        pair_id="cpi_apr",
        title="US Annual CPI Inflation rate >= 3.0% for April 2026",
        kalshi_ticker="CPI-26APR-T30",
        kalshi_event_ticker="CPI-26APR",
        poly_condition_id="0x5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f",
        poly_token_id="0x5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f_yes",
        poly_slug="cpi-inflation-april-2026",
        initial_price=35,
    ),
    SyntheticMarketConfig(
        pair_id="eth_5k",
        title="Ethereum price reaches $5,000 in 2026",
        kalshi_ticker="ETH-5K-2026",
        kalshi_event_ticker="ETH-2026",
        poly_condition_id="0x7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b",
        poly_token_id="0x7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b_yes",
        poly_slug="ethereum-5k-in-2026",
        initial_price=45,
    ),
    SyntheticMarketConfig(
        pair_id="recession_2026",
        title="US economic recession official declaration in 2026",
        kalshi_ticker="KX-RECESSION-2026",
        kalshi_event_ticker="KX-RECESSION",
        poly_condition_id="0xd3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4",
        poly_token_id="0xd3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4_yes",
        poly_slug="us-recession-2026",
        initial_price=28,
    ),
]


@dataclass
class MarketStateInternal:
    """Internal simulation state for a single prediction market pair."""

    config: SyntheticMarketConfig
    kalshi_price: float
    poly_price: float
    kalshi_volume: int = 1200
    poly_volume: int = 2400

    # Overrides when a deliberate mispricing is injected
    injected_kalshi_yes_ask: Optional[int] = None
    injected_kalshi_no_ask: Optional[int] = None
    injected_kalshi_yes_bid: Optional[int] = None
    injected_kalshi_no_bid: Optional[int] = None

    injected_poly_yes_ask: Optional[int] = None
    injected_poly_no_ask: Optional[int] = None
    injected_poly_yes_bid: Optional[int] = None
    injected_poly_no_bid: Optional[int] = None


class FakeMarketGenerator:
    """Synthetic prediction market generator producing Kalshi & Polymarket models.

    Maintains synchrony between platforms, performs random walks within realistic
    bounds ([5, 95] cents), and regularly injects intra-market gaps and cross-platform
    divergences so detection pipelines have predictable opportunities to identify.
    """

    def __init__(
        self,
        pairs: Optional[Sequence[SyntheticMarketConfig]] = None,
        mispricing_interval: int = 5,
        min_price: int = 5,
        max_price: int = 95,
        auto_tick: bool = True,
        seed: Optional[int] = None,
        platform: Optional[str] = None,
    ) -> None:
        """Initialize FakeMarketGenerator.

        Args:
            pairs: List of SyntheticMarketConfig definitions (defaults to DEFAULT_SYNTHETIC_PAIRS).
            mispricing_interval: Number of ticks between deliberate mispricing injections (default 5).
            min_price: Minimum mid-price clamp in cents (default 5).
            max_price: Maximum mid-price clamp in cents (default 95).
            auto_tick: If True, automatically advance tick counter when both platforms poll orderbooks.
            seed: Optional random seed for reproducible simulation.
            platform: Optional platform filter ('kalshi' or 'polymarket') if instance acts as a direct client.
        """
        self.mispricing_interval = max(1, mispricing_interval)
        self.min_price = min_price
        self.max_price = max_price
        self.auto_tick = auto_tick
        self.platform = platform.lower() if platform else None

        if seed is not None:
            self._rng = random.Random(seed)
        else:
            self._rng = random.Random()

        # Initialize pair states
        initial_configs = list(pairs or DEFAULT_SYNTHETIC_PAIRS)
        self._states: list[MarketStateInternal] = [
            MarketStateInternal(
                config=cfg,
                kalshi_price=float(cfg.initial_price),
                poly_price=float(cfg.initial_price),
            )
            for cfg in initial_configs
        ]

        self.tick_count: int = 0
        self._polled_platforms: set[str] = set()
        self._last_injected_info: Optional[dict[str, Any]] = None

        # Platform client adapters
        self.kalshi_client = FakeKalshiClient(self)
        self.polymarket_client = FakePolymarketClient(self)

    @property
    def last_injected_info(self) -> Optional[dict[str, Any]]:
        """Metadata about the most recently injected mispricing opportunity."""
        return self._last_injected_info

    def reset_injections(self) -> None:
        """Clear all active injected mispricings from internal market states."""
        for state in self._states:
            state.injected_kalshi_yes_ask = None
            state.injected_kalshi_no_ask = None
            state.injected_kalshi_yes_bid = None
            state.injected_kalshi_no_bid = None

            state.injected_poly_yes_ask = None
            state.injected_poly_no_ask = None
            state.injected_poly_yes_bid = None
            state.injected_poly_no_bid = None

        self._last_injected_info = None

    def inject_intra_market_mispricing(
        self,
        platform: str = "kalshi",
        pair_index: int = 0,
        gross_cost_cents: int = 92,
    ) -> dict[str, Any]:
        """Manually inject an intra-market arbitrage gap: BestAsk(YES) + BestAsk(NO) < 100¢.

        Args:
            platform: 'kalshi' or 'polymarket'.
            pair_index: Index of market pair to modify.
            gross_cost_cents: Sum of YES ask and NO ask in cents (e.g. 92¢ -> 8¢ gross edge).

        Returns:
            dict[str, Any]: Injection summary details.
        """
        self.reset_injections()
        idx = pair_index % len(self._states)
        state = self._states[idx]

        # Split gross cost into realistic YES ask and NO ask (e.g. 46¢ and 46¢ for 92¢)
        yes_ask = gross_cost_cents // 2
        no_ask = gross_cost_cents - yes_ask

        # In binary prediction markets:
        # best_yes_ask = 100 - best_no_bid => best_no_bid = 100 - yes_ask
        # best_no_ask  = 100 - best_yes_bid => best_yes_bid = 100 - no_ask
        target_platform = platform.lower()
        if target_platform == "kalshi":
            state.injected_kalshi_yes_ask = yes_ask
            state.injected_kalshi_no_ask = no_ask
            state.injected_kalshi_no_bid = 100 - yes_ask
            state.injected_kalshi_yes_bid = 100 - no_ask
            ticker = state.config.kalshi_ticker
        else:
            state.injected_poly_yes_ask = yes_ask
            state.injected_poly_no_ask = no_ask
            state.injected_poly_no_bid = 100 - yes_ask
            state.injected_poly_yes_bid = 100 - no_ask
            ticker = state.config.poly_token_id

        info = {
            "type": "intra_market",
            "platform": target_platform,
            "pair_id": state.config.pair_id,
            "ticker": ticker,
            "yes_ask": yes_ask,
            "no_ask": no_ask,
            "gross_cost": gross_cost_cents,
            "gross_edge": 100 - gross_cost_cents,
            "tick": self.tick_count,
        }
        self._last_injected_info = info
        logger.info(
            "Injected INTRA-MARKET mispricing on %s (%s): YES ask=%d¢, NO ask=%d¢ (Gross Edge=%d¢)",
            target_platform.upper(),
            ticker,
            yes_ask,
            no_ask,
            100 - gross_cost_cents,
        )
        return info

    def inject_cross_platform_mispricing(
        self,
        direction: str = "kalshi_yes_poly_no",
        pair_index: int = 0,
        gross_cost_cents: int = 90,
    ) -> dict[str, Any]:
        """Manually inject a cross-platform arbitrage divergence: Leg1_Ask + Leg2_Ask < 100¢.

        Args:
            direction: 'kalshi_yes_poly_no' (Kalshi YES + Poly NO) or 'poly_yes_kalshi_no'.
            pair_index: Index of market pair to modify.
            gross_cost_cents: Combined gross cost across both platforms (e.g. 90¢ -> 10¢ edge).

        Returns:
            dict[str, Any]: Injection summary details.
        """
        self.reset_injections()
        idx = pair_index % len(self._states)
        state = self._states[idx]

        leg1_ask = gross_cost_cents // 2
        leg2_ask = gross_cost_cents - leg1_ask

        if direction == "kalshi_yes_poly_no":
            # Direction (a): Kalshi YES ask + Poly NO ask < 100¢
            # 1. Kalshi YES ask = leg1_ask.
            # Derived on Kalshi: best_yes_ask = 100 - best_no_bid => no_bid = 100 - leg1_ask
            kalshi_yes_ask = leg1_ask
            kalshi_no_bid = 100 - kalshi_yes_ask
            kalshi_yes_bid = max(1, kalshi_yes_ask - 4)
            kalshi_no_ask = 100 - kalshi_yes_bid  # >= 100 - kalshi_yes_ask + 4 (no intra-market arb)

            state.injected_kalshi_yes_ask = kalshi_yes_ask
            state.injected_kalshi_no_ask = kalshi_no_ask
            state.injected_kalshi_no_bid = kalshi_no_bid
            state.injected_kalshi_yes_bid = kalshi_yes_bid

            # 2. Polymarket NO ask = leg2_ask.
            # In PolyOrderbook: best_no_ask = 100 - best_yes_bid => poly_yes_bid = 100 - leg2_ask!
            poly_no_ask = leg2_ask
            poly_yes_bid = 100 - poly_no_ask
            poly_no_bid = max(1, poly_no_ask - 4)
            poly_yes_ask = 100 - poly_no_bid  # >= 100 - poly_no_ask + 4 (no intra-market arb)

            state.injected_poly_no_ask = poly_no_ask
            state.injected_poly_no_bid = poly_no_bid
            state.injected_poly_yes_ask = poly_yes_ask
            state.injected_poly_yes_bid = poly_yes_bid

            info = {
                "type": "cross_platform",
                "direction": "kalshi_yes_poly_no",
                "pair_id": state.config.pair_id,
                "kalshi_ticker": state.config.kalshi_ticker,
                "poly_id": state.config.poly_condition_id,
                "kalshi_yes_ask": kalshi_yes_ask,
                "poly_no_ask": poly_no_ask,
                "gross_cost": gross_cost_cents,
                "gross_edge": 100 - gross_cost_cents,
                "tick": self.tick_count,
            }
        else:
            # Direction (b): Poly YES ask + Kalshi NO ask < 100¢
            # 1. Polymarket YES ask = leg1_ask
            poly_yes_ask = leg1_ask
            poly_no_bid = 100 - poly_yes_ask
            poly_yes_bid = max(1, poly_yes_ask - 4)
            poly_no_ask = 100 - poly_yes_bid

            state.injected_poly_yes_ask = poly_yes_ask
            state.injected_poly_yes_bid = poly_yes_bid
            state.injected_poly_no_ask = poly_no_ask
            state.injected_poly_no_bid = poly_no_bid

            # 2. Kalshi NO ask = leg2_ask
            # On Kalshi: best_no_ask = 100 - best_yes_bid => kalshi_yes_bid = 100 - leg2_ask
            kalshi_no_ask = leg2_ask
            kalshi_yes_bid = 100 - kalshi_no_ask
            kalshi_no_bid = max(1, kalshi_no_ask - 4)
            kalshi_yes_ask = 100 - kalshi_no_bid

            state.injected_kalshi_yes_ask = kalshi_yes_ask
            state.injected_kalshi_no_ask = kalshi_no_ask
            state.injected_kalshi_yes_bid = kalshi_yes_bid
            state.injected_kalshi_no_bid = kalshi_no_bid

            info = {
                "type": "cross_platform",
                "direction": "poly_yes_kalshi_no",
                "pair_id": state.config.pair_id,
                "kalshi_ticker": state.config.kalshi_ticker,
                "poly_id": state.config.poly_condition_id,
                "poly_yes_ask": poly_yes_ask,
                "kalshi_no_ask": kalshi_no_ask,
                "gross_cost": gross_cost_cents,
                "gross_edge": 100 - gross_cost_cents,
                "tick": self.tick_count,
            }

        self._last_injected_info = info
        logger.info(
            "Injected CROSS-PLATFORM mispricing (%s): Kalshi=%s, Poly=%s (Gross Edge=%d¢)",
            direction,
            state.config.kalshi_ticker,
            state.config.poly_condition_id,
            100 - gross_cost_cents,
        )
        return info

    def step(self) -> None:
        """Advance the simulation by one tick: random-walks prices and injects periodic mispricings."""
        self.tick_count += 1
        self.reset_injections()

        # Check if this tick is scheduled for a deliberate mispricing injection
        should_inject = (self.tick_count > 0) and (self.tick_count % self.mispricing_interval == 0)

        for i, state in enumerate(self._states):
            # Normal random walk: step delta in [-2, 2] cents
            delta_macro = self._rng.choice([-2, -1, 0, 1, 2])
            delta_noise = self._rng.choice([-1, 0, 1])

            new_k = state.kalshi_price + delta_macro
            new_p = state.poly_price + delta_macro + delta_noise

            # Clamp mid prices within realistic bounds
            state.kalshi_price = max(float(self.min_price), min(float(self.max_price), new_k))
            state.poly_price = max(float(self.min_price), min(float(self.max_price), new_p))

            # Slowly increment volume
            state.kalshi_volume += self._rng.randint(5, 30)
            state.poly_volume += self._rng.randint(10, 50)

        if should_inject:
            # Alternate between intra-market and cross-platform mispricings
            injection_cycle = self.tick_count // self.mispricing_interval
            target_pair_idx = (injection_cycle - 1) % len(self._states)

            if injection_cycle % 2 == 1:
                # Intra-market injection: alternate between Kalshi and Polymarket
                target_plat = "kalshi" if (injection_cycle // 2) % 2 == 0 else "polymarket"
                self.inject_intra_market_mispricing(
                    platform=target_plat,
                    pair_index=target_pair_idx,
                    gross_cost_cents=92,  # 8¢ edge
                )
            else:
                # Cross-platform injection: alternate between directions
                dir_type = "kalshi_yes_poly_no" if (injection_cycle // 2) % 2 == 0 else "poly_yes_kalshi_no"
                self.inject_cross_platform_mispricing(
                    direction=dir_type,
                    pair_index=target_pair_idx,
                    gross_cost_cents=90,  # 10¢ edge
                )

    def _notify_platform_poll(self, platform: str) -> None:
        """Track platform polling to auto-advance tick counter once both platforms query books."""
        if not self.auto_tick:
            return

        plat = platform.lower()
        if plat in self._polled_platforms:
            # Platform polled again -> start a new tick cycle
            self.step()
            self._polled_platforms = {plat}
        else:
            self._polled_platforms.add(plat)
            if self._polled_platforms == {"kalshi", "polymarket"}:
                # Both platforms have fetched this tick's books.
                pass

    def get_kalshi_markets(self, status: Optional[str] = None, limit: int = 100) -> list[KalshiMarket]:
        """Generate list of synthetic Kalshi Market instances."""
        markets: list[KalshiMarket] = []
        for state in self._states[:limit]:
            spread = state.config.spread
            mid = int(round(state.kalshi_price))

            yes_bid = state.injected_kalshi_yes_bid if state.injected_kalshi_yes_bid is not None else max(1, mid - spread // 2)
            yes_ask = state.injected_kalshi_yes_ask if state.injected_kalshi_yes_ask is not None else min(99, mid + (spread + 1) // 2)
            no_bid = state.injected_kalshi_no_bid if state.injected_kalshi_no_bid is not None else max(1, (100 - mid) - spread // 2)
            no_ask = state.injected_kalshi_no_ask if state.injected_kalshi_no_ask is not None else min(99, (100 - mid) + (spread + 1) // 2)

            markets.append(
                KalshiMarket(
                    ticker=state.config.kalshi_ticker,
                    title=state.config.title,
                    yes_bid=yes_bid,
                    yes_ask=yes_ask,
                    no_bid=no_bid,
                    no_ask=no_ask,
                    volume=state.kalshi_volume,
                    status="open",
                    event_ticker=state.config.kalshi_event_ticker,
                )
            )
        return markets

    def get_polymarket_markets(self, status: Optional[str] = None, limit: int = 100) -> list[PolyMarket]:
        """Generate list of synthetic Polymarket Market instances."""
        markets: list[PolyMarket] = []
        for state in self._states[:limit]:
            spread = state.config.spread
            mid = int(round(state.poly_price))

            yes_bid = state.injected_poly_yes_bid if state.injected_poly_yes_bid is not None else max(1, mid - spread // 2)
            yes_ask = state.injected_poly_yes_ask if state.injected_poly_yes_ask is not None else min(99, mid + (spread + 1) // 2)
            no_bid = state.injected_poly_no_bid if state.injected_poly_no_bid is not None else max(1, (100 - mid) - spread // 2)
            no_ask = state.injected_poly_no_ask if state.injected_poly_no_ask is not None else min(99, (100 - mid) + (spread + 1) // 2)

            markets.append(
                PolyMarket(
                    ticker=state.config.poly_token_id,
                    condition_id=state.config.poly_condition_id,
                    title=state.config.title,
                    yes_token_id=state.config.poly_token_id,
                    no_token_id=f"{state.config.poly_condition_id}_no",
                    clob_token_ids=[state.config.poly_token_id, f"{state.config.poly_condition_id}_no"],
                    yes_bid=yes_bid,
                    yes_ask=yes_ask,
                    no_bid=no_bid,
                    no_ask=no_ask,
                    volume=state.poly_volume,
                    status="open",
                    slug=state.config.poly_slug,
                )
            )
        return markets

    def get_kalshi_orderbooks(self, tickers: Optional[Sequence[str]] = None) -> dict[str, KalshiOrderbook]:
        """Generate synthetic Kalshi Orderbook instances mapped by ticker."""
        self._notify_platform_poll("kalshi")
        books: dict[str, KalshiOrderbook] = {}
        target_tickers = set(tickers) if tickers is not None else None

        for state in self._states:
            ticker = state.config.kalshi_ticker
            if target_tickers is not None and ticker not in target_tickers:
                continue

            spread = state.config.spread
            mid = int(round(state.kalshi_price))

            # Kalshi asks are derived via:
            # best_yes_ask = 100 - best_no_bid
            # best_no_ask  = 100 - best_yes_bid
            if state.injected_kalshi_yes_bid is not None and state.injected_kalshi_no_bid is not None:
                yes_bid = state.injected_kalshi_yes_bid
                no_bid = state.injected_kalshi_no_bid
            else:
                # Normal efficient pricing (sum of bids <= 100 - spread, sum of asks >= 100)
                yes_bid = max(1, mid - spread // 2)
                no_bid = max(1, (100 - mid) - (spread + 1) // 2)

            # Build depth levels
            yes_bids = [(yes_bid, 50), (max(1, yes_bid - 1), 100), (max(1, yes_bid - 2), 200)]
            no_bids = [(no_bid, 50), (max(1, no_bid - 1), 100), (max(1, no_bid - 2), 200)]

            books[ticker] = KalshiOrderbook(
                ticker=ticker,
                yes_bids=yes_bids,
                no_bids=no_bids,
            )

        return books

    def get_polymarket_orderbooks(self, token_ids: Optional[Sequence[str]] = None) -> dict[str, PolyOrderbook]:
        """Generate synthetic Polymarket Orderbook instances mapped by token_id/condition_id."""
        self._notify_platform_poll("polymarket")
        books: dict[str, PolyOrderbook] = {}
        target_ids = set(token_ids) if token_ids is not None else None

        for state in self._states:
            token_id = state.config.poly_token_id
            cond_id = state.config.poly_condition_id

            if target_ids is not None:
                if token_id not in target_ids and cond_id not in target_ids:
                    continue

            spread = state.config.spread
            mid = int(round(state.poly_price))

            if state.injected_poly_yes_ask is not None and state.injected_poly_yes_bid is not None:
                yes_ask = state.injected_poly_yes_ask
                yes_bid = state.injected_poly_yes_bid
                no_bid = state.injected_poly_no_bid or (100 - yes_ask)
                no_ask = state.injected_poly_no_ask or (100 - yes_bid)
            else:
                yes_bid = max(1, mid - spread // 2)
                yes_ask = min(99, mid + (spread + 1) // 2)
                no_bid = max(1, (100 - mid) - (spread + 1) // 2)
                no_ask = min(99, (100 - mid) + spread // 2)

            # Multi-level orderbook depth
            yes_bids = [(yes_bid, 50), (max(1, yes_bid - 1), 100)]
            yes_asks = [(yes_ask, 50), (min(99, yes_ask + 1), 100)]
            no_bids = [(no_bid, 50), (max(1, no_bid - 1), 100)]
            no_asks = [(no_ask, 50), (min(99, no_ask + 1), 100)]

            ob = PolyOrderbook(
                ticker=token_id,
                token_id=token_id,
                condition_id=cond_id,
                yes_bids=yes_bids,
                yes_asks=yes_asks,
                no_bids=no_bids,
                no_asks=no_asks,
            )

            if target_ids is not None:
                if token_id in target_ids:
                    books[token_id] = ob
                if cond_id in target_ids and cond_id != token_id:
                    books[cond_id] = ob
            else:
                books[token_id] = ob

        return books

    # Generic client interface compatibility methods
    async def list_markets(
        self,
        status: Optional[str] = "open",
        limit: int = 100,
        max_pages: Optional[int] = 1,
        **kwargs: Any,
    ) -> list[Any]:
        """Client interface: list markets for configured platform or Kalshi by default."""
        if self.platform == "polymarket":
            return self.get_polymarket_markets(status=status, limit=limit)
        return self.get_kalshi_markets(status=status, limit=limit)

    async def get_orderbooks(self, tickers: list[str]) -> dict[str, Any]:
        """Client interface: get orderbooks for tickers or tokens."""
        if self.platform == "polymarket":
            return self.get_polymarket_orderbooks(tickers)
        return self.get_kalshi_orderbooks(tickers)

    async def get_orderbook(self, identifier: str) -> Any:
        """Client interface: get single orderbook."""
        obs = await self.get_orderbooks([identifier])
        if identifier in obs:
            return obs[identifier]
        # Return first available book or empty book
        if obs:
            return next(iter(obs.values()))
        raise KeyError(f"Orderbook not found for {identifier}")

    async def close(self) -> None:
        """Client interface: close HTTP connection (no-op for simulation)."""
        pass


class FakeKalshiClient:
    """Drop-in simulation replacement for KalshiClient."""

    def __init__(self, generator: FakeMarketGenerator) -> None:
        self.generator = generator
        self.auth = None

    async def list_markets(
        self,
        status: Optional[str] = "open",
        event_ticker: Optional[str] = None,
        series_ticker: Optional[str] = None,
        limit: int = 100,
        cursor: Optional[str] = None,
        max_pages: Optional[int] = 1,
        **kwargs: Any,
    ) -> list[KalshiMarket]:
        """Retrieve synthetic Kalshi markets matching KalshiClient signature."""
        return self.generator.get_kalshi_markets(status=status, limit=limit)

    async def get_orderbooks(self, tickers: list[str]) -> dict[str, KalshiOrderbook]:
        """Retrieve synthetic Kalshi orderbooks matching KalshiClient signature."""
        return self.generator.get_kalshi_orderbooks(tickers)

    async def get_orderbook(self, ticker: str) -> KalshiOrderbook:
        """Retrieve single Kalshi orderbook."""
        books = self.generator.get_kalshi_orderbooks([ticker])
        if ticker in books:
            return books[ticker]
        raise KeyError(f"Kalshi market {ticker} not found in simulation")

    async def list_events(self, **kwargs: Any) -> list[Any]:
        return []

    async def close(self) -> None:
        """No-op for simulation."""
        pass

    async def __aenter__(self) -> FakeKalshiClient:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()


class FakePolymarketClient:
    """Drop-in simulation replacement for PolymarketClient."""

    def __init__(self, generator: FakeMarketGenerator) -> None:
        self.generator = generator

    async def list_markets(
        self,
        status: Optional[str] = "open",
        condition_id: Optional[str] = None,
        slug: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order: Optional[str] = None,
        ascending: Optional[bool] = None,
        max_pages: Optional[int] = 1,
        **kwargs: Any,
    ) -> list[PolyMarket]:
        """Retrieve synthetic Polymarket markets matching PolymarketClient signature."""
        return self.generator.get_polymarket_markets(status=status, limit=limit)

    async def get_orderbooks(self, token_ids: list[str]) -> dict[str, PolyOrderbook]:
        """Retrieve synthetic Polymarket orderbooks matching PolymarketClient signature."""
        return self.generator.get_polymarket_orderbooks(token_ids)

    async def get_orderbook(self, token_id: str) -> PolyOrderbook:
        """Retrieve single Polymarket orderbook."""
        books = self.generator.get_polymarket_orderbooks([token_id])
        if token_id in books:
            return books[token_id]
        raise KeyError(f"Polymarket token {token_id} not found in simulation")

    async def get_market(self, condition_id: str) -> Optional[PolyMarket]:
        """Retrieve single Polymarket market by condition_id."""
        for m in self.generator.get_polymarket_markets(limit=100):
            if m.condition_id == condition_id:
                return m
        return None

    async def close(self) -> None:
        """No-op for simulation."""
        pass

    async def __aenter__(self) -> FakePolymarketClient:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
