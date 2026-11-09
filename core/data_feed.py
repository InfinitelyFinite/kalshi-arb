"""Unified real-time and polling DataFeed for Kalshi and Polymarket prediction markets.

Features:
- Owns Kalshi and Polymarket REST and WebSocket clients.
- Maintains in-memory dictionary of normalized market state keyed by `(platform, ticker)`.
- Supports REST polling, WebSocket streaming, and periodic snapshots.
- `snapshot_to_duckdb()` writes current market states into DuckDB (schema v2, see `core/snapshot_store.py`):
  one `orderbook_snapshots` row per market observation (best asks/bids with sizes, observed and exchange
  timestamps, market status, data_source, depth flag) plus top-N levels per side in `orderbook_levels`.
- Collection gate (registry mode): only pairs that are `status: verified` in `config/market_overrides.yaml` AND
  pass a live tradability check are collected. Failing pairs are dropped for that period (reason recorded in
  `pair_exclusions`) and re-admitted automatically when the check passes again.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Sequence, Union

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from core import snapshot_store
from core.depth import Ladders, kalshi_ladders, polymarket_ladders
from core.models import DataSource, MarketStatus, normalize_market_status
from core.pair_registry import (
    MarketPairOverride,
    PairRegistryError,
    RegistryThresholds,
    parse_pair_registry,
)
from kalshi_client.models import Market as KalshiMarket, Orderbook as KalshiOrderbook
from kalshi_client.rest import KalshiClient
from kalshi_client.ws import KalshiWSClient, WSEvent
from polymarket_client.models import Market as PolyMarket, Orderbook as PolyOrderbook
from polymarket_client.rest import PolymarketClient

if TYPE_CHECKING:
    from core.config_loader import AppConfig
    from core.pair_validation import PairCheckResult

logger = logging.getLogger(__name__)


class MarketState(BaseModel):
    """Normalized snapshot of a prediction market's current state."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True, arbitrary_types_allowed=True)

    platform: str = Field(description="Platform identifier ('kalshi' or 'polymarket')")
    ticker: str = Field(description="Market identifier (Kalshi ticker or Polymarket token_id/condition_id)")
    title: Optional[str] = Field(default=None, description="Human-readable title or question")
    yes_bid: Optional[int] = Field(default=None, description="Best YES bid in cents (0-100)")
    yes_ask: Optional[int] = Field(default=None, description="Best YES ask in cents (0-100)")
    no_bid: Optional[int] = Field(default=None, description="Best NO bid in cents (0-100)")
    no_ask: Optional[int] = Field(default=None, description="Best NO ask in cents (0-100)")
    volume: Optional[int] = Field(default=0, description="Total or 24h volume in contracts/USD")
    ts: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp of the most recent price update",
    )
    raw_market: Optional[Any] = Field(default=None, description="Underlying Market model instance")
    raw_orderbook: Optional[Any] = Field(default=None, description="Underlying Orderbook model instance")
    condition_id: Optional[str] = Field(default=None, description="Polymarket condition ID if applicable")
    observed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When this app received the data for this market (never the exchange's time)",
    )
    exchange_updated_at: Optional[datetime] = Field(
        default=None,
        description="Platform-supplied quote timestamp if the payload had one, else None (never backfilled)",
    )
    market_status: MarketStatus = Field(default=MarketStatus.UNKNOWN, description="Normalized market status")
    ladders: Optional[Ladders] = Field(
        default=None, description="Exact full ladders (cents/contracts); None when only best prices are known"
    )

    @property
    def yes_spread(self) -> Optional[int]:
        """Bid-ask spread for YES side in cents."""
        if self.yes_bid is not None and self.yes_ask is not None:
            return self.yes_ask - self.yes_bid
        return None

    @property
    def no_spread(self) -> Optional[int]:
        """Bid-ask spread for NO side in cents."""
        if self.no_bid is not None and self.no_ask is not None:
            return self.no_ask - self.no_bid
        return None

    @property
    def mid_price(self) -> Optional[float]:
        """Mid-point price of YES side in cents."""
        if self.yes_bid is not None and self.yes_ask is not None:
            return (self.yes_bid + self.yes_ask) / 2.0
        return None


def _round_cents(price: Any) -> Optional[int]:
    """Legacy integer-cent view of an exact ladder price."""
    return int(round(price)) if price is not None else None


def _best_price(levels: Sequence[Any]) -> Optional[int]:
    return _round_cents(levels[0][0]) if levels else None


class DataFeed:
    """Unified data feed manager for Kalshi and Polymarket prediction markets."""

    DEFAULT_DUCKDB_PATH = "data/snapshots.duckdb"

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        kalshi_client: Optional[Any] = None,
        kalshi_ws_client: Optional[KalshiWSClient] = None,
        polymarket_client: Optional[Any] = None,
        fake_generator: Optional[Any] = None,
        kalshi_tickers: Optional[list[str]] = None,
        polymarket_tokens: Optional[list[str]] = None,
        duckdb_path: Union[str, Path] = DEFAULT_DUCKDB_PATH,
        snapshot_interval: float = 60.0,
        poll_interval: float = 10.0,
        is_sandbox: Optional[bool] = None,
        enable_ws: bool = True,
        pairs: Optional[Sequence[MarketPairOverride]] = None,
        data_source: Optional[Union[DataSource, str]] = None,
        depth_levels: Optional[int] = None,
        gate_interval: Optional[float] = None,
        reconcile_interval: Optional[float] = None,
    ) -> None:
        """Initialize DataFeed.

        Args:
            config: Optional AppConfig instance.
            kalshi_client: Injected Kalshi REST client.
            kalshi_ws_client: Injected Kalshi WebSocket client.
            polymarket_client: Injected Polymarket REST client.
            fake_generator: Optional FakeMarketGenerator for simulation mode.
            kalshi_tickers: List of Kalshi market tickers to track.
            polymarket_tokens: List of Polymarket token IDs or condition IDs to track.
            duckdb_path: Filepath or :memory: for DuckDB storage.
            snapshot_interval: Interval in seconds between DuckDB snapshot writes (default 60s).
            poll_interval: Interval in seconds between REST poll cycles (default 10s).
            is_sandbox: Sandbox mode flag for Kalshi.
            enable_ws: Whether to enable Kalshi WebSocket streaming.
            pairs: Verified pairs to collect (enables registry mode + the live collection gate). If omitted and no
                explicit tickers/tokens are given, the verified pairs are loaded from the overrides YAML
                (real mode) - simulation mode keeps tracking the synthetic generator's markets.
            data_source: Provenance tag for written rows (default: synthetic in simulation mode, else live_api).
            depth_levels: Top N levels stored per side (default: config.depth.depth_levels or 10).
            gate_interval: Seconds between gate re-checks (default: config.pair_registry.gate_interval_minutes * 60).
            reconcile_interval: Seconds between REST reconciliation of WebSocket books (default 300).
        """
        self.config = config
        self.duckdb_path = str(duckdb_path)
        self.snapshot_interval = snapshot_interval
        self.poll_interval = poll_interval
        self.enable_ws = enable_ws

        depth_cfg = getattr(config, "depth", None)
        registry_cfg = getattr(config, "pair_registry", None)
        self.depth_levels = int(depth_levels if depth_levels is not None else getattr(depth_cfg, "depth_levels", 10))
        self.reconcile_interval = float(
            reconcile_interval if reconcile_interval is not None else getattr(depth_cfg, "ws_reconcile_seconds", 300.0)
        )
        self.gate_interval = float(
            gate_interval
            if gate_interval is not None
            else getattr(registry_cfg, "gate_interval_minutes", 30.0) * 60.0
        )

        # Determine sandbox mode
        if is_sandbox is not None:
            self.is_sandbox = is_sandbox
        elif config and hasattr(config, "mode"):
            mode_val = config.mode.trading_mode
            mode_str = mode_val.value if hasattr(mode_val, "value") else str(mode_val)
            self.is_sandbox = mode_str.lower() in ("sandbox", "dry_run")
        else:
            self.is_sandbox = False

        # Determine data_mode (simulation vs real)
        if config and hasattr(config, "mode") and hasattr(config.mode, "data_mode"):
            dm_val = config.mode.data_mode
            dm_str = dm_val.value if hasattr(dm_val, "value") else str(dm_val)
            self.is_simulation = dm_str.lower() == "simulation"
        else:
            self.is_simulation = False

        self.data_source = DataSource(
            data_source if data_source is not None else (DataSource.SYNTHETIC if self.is_simulation else DataSource.LIVE_API)
        )

        self.fake_generator = fake_generator

        # Clients
        if self.is_simulation:
            # In simulation mode, disable real WebSocket and use synthetic generator
            self.enable_ws = False
            if not self.fake_generator:
                from simulation.fake_market_generator import FakeMarketGenerator
                self.fake_generator = FakeMarketGenerator()

            if kalshi_client:
                self.kalshi_client = kalshi_client
            else:
                self.kalshi_client = self.fake_generator.kalshi_client

            if polymarket_client:
                self.polymarket_client = polymarket_client
            else:
                self.polymarket_client = self.fake_generator.polymarket_client
        else:
            if kalshi_client:
                self.kalshi_client = kalshi_client
            elif config:
                self.kalshi_client = KalshiClient.from_config(config, is_sandbox=self.is_sandbox)
            else:
                self.kalshi_client = KalshiClient(is_sandbox=self.is_sandbox)

            if polymarket_client:
                self.polymarket_client = polymarket_client
            elif config:
                self.polymarket_client = PolymarketClient.from_config(config)
            else:
                self.polymarket_client = PolymarketClient()

        # Tracked markets
        self.kalshi_tickers: list[str] = list(kalshi_tickers or [])
        self.polymarket_tokens: list[str] = list(polymarket_tokens or [])

        # Internal market mappings
        self._kalshi_markets: dict[str, KalshiMarket] = {}
        self._polymarket_markets: dict[str, PolyMarket] = {}
        self._poly_token_to_market: dict[str, PolyMarket] = {}

        # Registry mode (verified pairs + live gate)
        self.pairs: list[MarketPairOverride] = []
        self._registry_mode = False
        self._active_pairs: set[str] = set()
        self._leg_status: dict[tuple[str, str], MarketStatus] = {}
        self._gate_last_run: Optional[datetime] = None
        self._schema_ready: set[str] = set()
        self._last_reconcile: Optional[datetime] = None
        self.reconcile_mismatches = 0

        explicit = bool(self.kalshi_tickers or self.polymarket_tokens)
        if pairs is not None:
            self._enter_registry_mode([p for p in pairs if p.is_verified])
        elif not explicit and config is not None:
            overrides_path = None
            if hasattr(config, "matching") and getattr(config.matching, "overrides_path", None):
                overrides_path = config.matching.overrides_path
            elif Path("config/market_overrides.yaml").exists():
                overrides_path = "config/market_overrides.yaml"

            if overrides_path and Path(overrides_path).exists():
                thresholds = RegistryThresholds.from_config(config)
                parsed = parse_pair_registry(overrides_path, thresholds)
                if self.is_simulation:
                    self._load_simulation_overrides(parsed.pairs, overrides_path)
                else:
                    if parsed.issues:
                        raise PairRegistryError(parsed.issues)
                    for warning in parsed.warnings:
                        logger.warning("Pair registry: %s", warning)
                    self._enter_registry_mode(parsed.verified)

        # WebSocket client
        self.kalshi_ws_client = kalshi_ws_client
        if self.enable_ws and not self.kalshi_ws_client and self.kalshi_tickers:
            auth = getattr(self.kalshi_client, "auth", None)
            self.kalshi_ws_client = KalshiWSClient(
                tickers=self.kalshi_tickers,
                auth=auth,
                is_sandbox=self.is_sandbox,
            )

        # In-memory current market states: keyed by (platform, ticker)
        self._market_state: dict[tuple[str, str], MarketState] = {}
        self._lock = asyncio.Lock()

        # Background tasks
        self._running = False
        self._poll_task: Optional[asyncio.Task[None]] = None
        self._snapshot_task: Optional[asyncio.Task[None]] = None
        self._gate_task: Optional[asyncio.Task[None]] = None
        self._ws_task: Optional[asyncio.Task[None]] = None

    # ------------------------------------------------------------------
    # Registry mode / collection gate
    # ------------------------------------------------------------------

    def _enter_registry_mode(self, verified: Sequence[MarketPairOverride]) -> None:
        """Track exactly the given verified pairs; nothing is collected until the gate admits a pair."""
        self._registry_mode = True
        self.pairs = list(verified)
        self.kalshi_tickers = [p.kalshi_ticker for p in self.pairs if p.kalshi_ticker]
        self.polymarket_tokens = []
        for p in self.pairs:
            for tok in (p.polymarket_yes_token_id, p.polymarket_no_token_id):
                if tok:
                    self.polymarket_tokens.append(tok)
        logger.info(
            "DataFeed registry mode: %d verified pair(s) across %d categorie(s)",
            len(self.pairs),
            len({p.category for p in self.pairs}),
        )
        if not self.pairs:
            logger.warning(
                "No verified pairs in the registry: nothing will be collected. Verify pairs in "
                "config/market_overrides.yaml (human task) and run scripts/validate_market_pairs.py."
            )

    def _load_simulation_overrides(self, pairs: Sequence[MarketPairOverride], overrides_path: Any) -> None:
        """Simulation mode: track every registry entry (any status) so synthetic data keeps flowing.

        Rows are tagged data_source='synthetic' and never reach the research view.
        """
        for ov in pairs:
            if ov.kalshi_ticker and ov.kalshi_ticker not in self.kalshi_tickers:
                self.kalshi_tickers.append(ov.kalshi_ticker)
                title = ov.notes or ov.kalshi_ticker
                self._kalshi_markets[ov.kalshi_ticker] = KalshiMarket(
                    ticker=ov.kalshi_ticker,
                    title=title,
                    status="open",
                )
            if ov.polymarket_condition_id:
                p_id = ov.polymarket_condition_id
                if p_id not in self.polymarket_tokens:
                    self.polymarket_tokens.append(p_id)
                p_title = ov.notes or p_id
                pm = PolyMarket(
                    ticker=p_id,
                    condition_id=p_id,
                    title=p_title,
                    status="open",
                    yes_token_id=f"{p_id}_yes",
                    no_token_id=f"{p_id}_no",
                )
                self._polymarket_markets[p_id] = pm
                self._poly_token_to_market[p_id] = pm
                self._poly_token_to_market[f"{p_id}_yes"] = pm
        if self.kalshi_tickers or self.polymarket_tokens:
            logger.info(
                "DataFeed (simulation) configured with %d Kalshi ticker(s) and %d Polymarket token(s) from %s",
                len(self.kalshi_tickers),
                len(self.polymarket_tokens),
                overrides_path,
            )

    @property
    def registry_mode(self) -> bool:
        return self._registry_mode

    @property
    def active_pair_ids(self) -> set[str]:
        """Pairs that currently pass the live gate (registry mode)."""
        return set(self._active_pairs)

    def _active_pair_objs(self) -> list[MarketPairOverride]:
        return [p for p in self.pairs if p.pair_id in self._active_pairs]

    def _active_kalshi_tickers(self) -> list[str]:
        if self._registry_mode:
            return [p.kalshi_ticker for p in self._active_pair_objs() if p.kalshi_ticker]
        return self.kalshi_tickers

    def _is_collectable(self, platform: str, ticker: str) -> bool:
        """Registry mode: only states belonging to currently admitted pairs are written."""
        if not self._registry_mode:
            return True
        platform = platform.lower()
        for p in self._active_pair_objs():
            if platform == "kalshi" and p.kalshi_ticker == ticker:
                return True
            if platform == "polymarket" and (p.polymarket_condition_id or "").lower() == ticker.lower():
                return True
        return False

    def _drop_state(self, platform: str, ticker: str) -> None:
        self._market_state.pop((platform.lower(), ticker), None)

    async def run_gate(self, now: Optional[datetime] = None) -> list["PairCheckResult"]:
        """Re-check tradability of every verified pair and update the admitted set.

        Pairs that fail are dropped for this period (their in-memory state is removed so no rows are written) and a
        ``pair_exclusions`` row with the reason is recorded. They are re-admitted automatically when the check passes.
        """
        from core.pair_validation import check_pairs_live

        if not self._registry_mode:
            return []
        now = now or datetime.now(timezone.utc)
        results = await check_pairs_live(
            self.pairs, self.kalshi_client, self.polymarket_client, now=now, close_mismatch_hours=None
        )
        exclusions: list[tuple[str, str, str]] = []
        for pair, res in zip(self.pairs, results):
            if res.passed:
                if pair.pair_id not in self._active_pairs:
                    logger.info("Gate: pair %s admitted", pair.pair_id)
                self._active_pairs.add(pair.pair_id)
                self._leg_status[("kalshi", pair.kalshi_ticker or "")] = res.kalshi_status
                self._leg_status[("polymarket", pair.polymarket_condition_id or "")] = res.polymarket_status
            else:
                if pair.pair_id in self._active_pairs:
                    logger.warning("Gate: pair %s dropped (%s) %s", pair.pair_id, res.reason_label, res.detail)
                self._active_pairs.discard(pair.pair_id)
                if pair.kalshi_ticker:
                    self._drop_state("kalshi", pair.kalshi_ticker)
                if pair.polymarket_condition_id:
                    self._drop_state("polymarket", pair.polymarket_condition_id)
                exclusions.append((pair.pair_id, res.reason_label, res.detail))
        self._gate_last_run = now
        if exclusions:
            self._record_exclusions(exclusions, now)
        return results

    def _open_store(self, path: str) -> duckdb.DuckDBPyConnection:
        conn = snapshot_store.connect(path)
        try:
            if path == ":memory:" or path not in self._schema_ready:
                snapshot_store.ensure_schema(conn)
                self._schema_ready.add(path)
            if self._registry_mode:
                snapshot_store.sync_verified_pairs(conn, self.pairs)
        except Exception:
            conn.close()
            raise
        return conn

    def _record_exclusions(self, rows: list[tuple[str, str, str]], now: datetime) -> None:
        try:
            conn = self._open_store(self.duckdb_path)
            try:
                snapshot_store.log_exclusions(conn, rows, checked_at=now)
            finally:
                conn.close()
        except Exception as e:  # noqa: BLE001 - never let audit logging break collection
            logger.error("Failed to record pair_exclusions rows: %s", e)

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    @property
    def market_state(self) -> dict[tuple[str, str], MarketState]:
        """Current in-memory market states mapping (platform, ticker) -> MarketState."""
        return self._market_state

    def get_market_state(self, platform: str, ticker: str) -> Optional[MarketState]:
        """Retrieve current state for a specific market platform and ticker."""
        return self._market_state.get((platform.lower(), ticker))

    @staticmethod
    def _derive_ladders(raw_orderbook: Any) -> Optional[Ladders]:
        """Build ladders from a Kalshi/Polymarket Orderbook model if it carries exact levels."""
        if raw_orderbook is None:
            return None
        try:
            if hasattr(raw_orderbook, "yes_bid_levels"):
                return kalshi_ladders(raw_orderbook)
            if hasattr(raw_orderbook, "ask_levels"):
                return polymarket_ladders(raw_orderbook, None)
        except Exception as e:  # noqa: BLE001
            logger.debug("Could not derive ladders from %r: %s", type(raw_orderbook).__name__, e)
        return None

    def update_market_state(
        self,
        platform: str,
        ticker: str,
        yes_bid: Optional[int] = None,
        yes_ask: Optional[int] = None,
        no_bid: Optional[int] = None,
        no_ask: Optional[int] = None,
        volume: Optional[int] = None,
        title: Optional[str] = None,
        ts: Optional[datetime] = None,
        raw_market: Optional[Any] = None,
        raw_orderbook: Optional[Any] = None,
        condition_id: Optional[str] = None,
        observed_at: Optional[datetime] = None,
        exchange_updated_at: Optional[datetime] = None,
        market_status: Optional[MarketStatus] = None,
        ladders: Optional[Ladders] = None,
    ) -> MarketState:
        """Update or insert a market's state in memory.

        ``observed_at`` defaults to the update time; ``exchange_updated_at`` is stored exactly as supplied (None
        when the platform gave no timestamp) and is never replaced by ``observed_at``.
        """
        platform_key = platform.lower()
        key = (platform_key, ticker)
        now_ts = ts or datetime.now(timezone.utc)

        existing = self._market_state.get(key)
        cid = (
            condition_id
            or getattr(raw_market, "condition_id", None)
            or getattr(raw_orderbook, "condition_id", None)
            or (existing.condition_id if existing else None)
        )

        if ladders is None:
            ladders = self._derive_ladders(raw_orderbook)
        if market_status is None:
            market_status = existing.market_status if existing else self._leg_status.get(key, MarketStatus.UNKNOWN)

        if existing:
            # Update fields preserving previous metadata if not supplied
            new_title = title if title is not None else existing.title
            new_volume = volume if volume is not None else existing.volume
            new_raw_market = raw_market if raw_market is not None else existing.raw_market
            new_raw_ob = raw_orderbook if raw_orderbook is not None else existing.raw_orderbook
        else:
            new_title = title
            new_volume = volume
            new_raw_market = raw_market
            new_raw_ob = raw_orderbook

        state = MarketState(
            platform=platform_key,
            ticker=ticker,
            title=new_title,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            volume=new_volume or 0,
            ts=now_ts,
            raw_market=new_raw_market,
            raw_orderbook=new_raw_ob,
            condition_id=cid,
            observed_at=observed_at or now_ts,
            exchange_updated_at=exchange_updated_at,
            market_status=market_status,
            ladders=ladders,
        )

        self._market_state[key] = state
        return state

    async def discover_markets(self, limit: int = 30) -> None:
        """Auto-discover open markets from Kalshi and Polymarket if none are configured."""
        if self._registry_mode:
            logger.debug("Skipping market auto-discovery in registry mode (only verified pairs are collected).")
            return
        if getattr(getattr(self.config, "matching", None), "overrides_only", False):
            logger.debug("Skipping market auto-discovery because matching.overrides_only is True.")
            return

        if not self.kalshi_tickers:
            try:
                k_markets = await self.kalshi_client.list_markets(status="open", limit=limit, max_pages=1)
                if not k_markets:
                    k_markets = await self.kalshi_client.list_markets(status="", limit=limit, max_pages=1)
                for m in k_markets:
                    if m.ticker:
                        self.kalshi_tickers.append(m.ticker)
                        self._kalshi_markets[m.ticker] = m
                logger.info("Discovered %d Kalshi markets.", len(self.kalshi_tickers))
            except Exception as e:
                logger.warning("Failed to auto-discover Kalshi markets: %s", e)

        if not self.polymarket_tokens:
            try:
                p_markets = await self.polymarket_client.list_markets(status="open", limit=limit, max_pages=1)
                if not p_markets:
                    p_markets = await self.polymarket_client.list_markets(status=None, limit=limit, max_pages=1)
                for m in p_markets:
                    if m.condition_id:
                        self._polymarket_markets[m.condition_id] = m
                    token_id = m.yes_token_id or (m.clob_token_ids[0] if m.clob_token_ids else None) or m.condition_id or m.ticker
                    if token_id:
                        self.polymarket_tokens.append(token_id)
                        self._poly_token_to_market[token_id] = m
                logger.info("Discovered %d Polymarket tokens.", len(self.polymarket_tokens))
            except Exception as e:
                logger.warning("Failed to auto-discover Polymarket markets: %s", e)

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------

    def _kalshi_status(self, ticker: str, market: Optional[KalshiMarket]) -> MarketStatus:
        """Registry mode: status from the last gate check. Otherwise from the Market object, else unknown."""
        if self._registry_mode:
            return self._leg_status.get(("kalshi", ticker), MarketStatus.UNKNOWN)
        if market is not None:
            return normalize_market_status(getattr(market, "status", None))
        return MarketStatus.UNKNOWN

    def _ws_is_live(self) -> bool:
        return bool(
            self.enable_ws
            and self.kalshi_ws_client is not None
            and getattr(self.kalshi_ws_client, "is_connected", False) is True
        )

    @staticmethod
    def _top_levels(levels: Sequence[Any], depth: int) -> list[tuple[Any, Any]]:
        return [(p, s) for p, s in list(levels)[:depth]]

    def _reconcile_kalshi_book(self, ticker: str, ws_book: KalshiOrderbook, rest_book: KalshiOrderbook) -> bool:
        """Compare the WebSocket-maintained book with a fresh REST snapshot. Returns True if they agree."""
        depth = self.depth_levels
        ws_l = kalshi_ladders(ws_book)
        rest_l = kalshi_ladders(rest_book)
        agree = (
            self._top_levels(ws_l.yes_bid, depth) == self._top_levels(rest_l.yes_bid, depth)
            and self._top_levels(ws_l.no_bid, depth) == self._top_levels(rest_l.no_bid, depth)
        )
        if not agree:
            self.reconcile_mismatches += 1
            logger.warning(
                "Kalshi book reconcile mismatch for %s: WS best yes/no bid=%s/%s vs REST=%s/%s "
                "(REST snapshot adopted, WS book re-seeded)",
                ticker,
                ws_book.best_yes_bid,
                ws_book.best_no_bid,
                rest_book.best_yes_bid,
                rest_book.best_no_bid,
            )
        else:
            logger.debug("Kalshi book reconcile OK for %s", ticker)
        return agree

    async def poll_kalshi(self) -> dict[str, KalshiOrderbook]:
        """Fetch orderbooks for tracked Kalshi tickers and update in-memory state.

        When the WebSocket maintains the book, state keeps coming from that maintained state (depth levels and
        message timestamp included); this REST poll only acts as the periodic full-snapshot reconciliation.
        """
        tickers = self._active_kalshi_tickers()
        if not tickers:
            return {}

        try:
            orderbooks = await self.kalshi_client.get_orderbooks(tickers)
        except Exception as e:
            logger.warning("Error fetching Kalshi orderbooks: %s", e)
            if self._registry_mode:
                for t in tickers:  # unknown quote: do not keep re-snapshotting stale data
                    self._drop_state("kalshi", t)
            return {}

        now = datetime.now(timezone.utc)
        ws_live = self._ws_is_live()
        reconcile_due = (
            self._last_reconcile is None or (now - self._last_reconcile).total_seconds() >= self.reconcile_interval
        )

        for ticker, ob in orderbooks.items():
            market = self._kalshi_markets.get(ticker)
            title = market.title if market else ticker
            vol = market.volume if market else 0

            ws_book = self.kalshi_ws_client.get_orderbook(ticker) if ws_live else None
            if ws_book is not None:
                if not reconcile_due:
                    continue  # WS-maintained state stays authoritative between reconciliations
                if not self._reconcile_kalshi_book(ticker, ws_book, ob):
                    reseed = getattr(self.kalshi_ws_client, "reseed_book", None)
                    if callable(reseed):
                        reseed(ticker, ob)
                else:
                    continue  # books agree: keep the WS state (it carries the exchange timestamp)

            self.update_market_state(
                platform="kalshi",
                ticker=ticker,
                title=title,
                yes_bid=ob.best_yes_bid,
                yes_ask=ob.best_yes_ask,
                no_bid=ob.best_no_bid,
                no_ask=ob.best_no_ask,
                volume=vol,
                raw_market=market,
                raw_orderbook=ob,
                observed_at=now,
                exchange_updated_at=getattr(ob, "exchange_updated_at", None),
                market_status=self._kalshi_status(ticker, market),
            )

        if reconcile_due and ws_live:
            self._last_reconcile = now

        if self._registry_mode:
            for t in tickers:
                if t not in orderbooks:
                    self._drop_state("kalshi", t)

        return orderbooks

    def _poll_polymarket_pairs(self, orderbooks: dict[str, PolyOrderbook], now: datetime) -> None:
        """Registry mode: combine each pair's YES and NO token books into one market state keyed by condition id."""
        for pair in self._active_pair_objs():
            cid = pair.polymarket_condition_id or ""
            yes_ob = orderbooks.get(pair.polymarket_yes_token_id or "")
            no_ob = orderbooks.get(pair.polymarket_no_token_id or "")
            if yes_ob is None or no_ob is None:
                logger.warning(
                    "Polymarket pair %s: missing %s token book; no row for this cycle",
                    pair.pair_id,
                    "YES" if yes_ob is None else "NO",
                )
                self._drop_state("polymarket", cid)
                continue

            lad = polymarket_ladders(yes_ob, no_ob)
            self.update_market_state(
                platform="polymarket",
                ticker=cid,
                title=pair.notes or pair.pair_id,
                yes_bid=_best_price(lad.yes_bid),
                yes_ask=_best_price(lad.yes_ask),
                no_bid=_best_price(lad.no_bid),
                no_ask=_best_price(lad.no_ask),
                raw_orderbook=yes_ob,
                condition_id=cid,
                observed_at=now,
                exchange_updated_at=lad.exchange_updated_at,
                market_status=self._leg_status.get(("polymarket", cid), MarketStatus.UNKNOWN),
                ladders=lad,
            )

    async def poll_polymarket(self) -> dict[str, PolyOrderbook]:
        """Fetch orderbooks for tracked Polymarket tokens and update in-memory state."""
        if self._registry_mode:
            token_ids: list[str] = []
            for p in self._active_pair_objs():
                token_ids.extend(t for t in (p.polymarket_yes_token_id, p.polymarket_no_token_id) if t)
        else:
            token_ids = self.polymarket_tokens
        if not token_ids:
            return {}

        try:
            orderbooks = await self.polymarket_client.get_orderbooks(token_ids)
        except Exception as e:
            logger.warning("Error fetching Polymarket orderbooks: %s", e)
            if self._registry_mode:
                for p in self._active_pair_objs():
                    self._drop_state("polymarket", p.polymarket_condition_id or "")
            return {}

        now = datetime.now(timezone.utc)
        if self._registry_mode:
            self._poll_polymarket_pairs(orderbooks, now)
            return orderbooks

        for token_id, ob in orderbooks.items():
            market = self._poly_token_to_market.get(token_id) or self._polymarket_markets.get(token_id)
            title = market.title if market else token_id
            vol = market.volume if market else 0
            cid = getattr(market, "condition_id", None) or getattr(ob, "condition_id", None) or (token_id if token_id.startswith("0x") else None)
            status = (
                normalize_market_status(
                    getattr(market, "status", None),
                    accepting_orders=getattr(market, "accepting_orders", None),
                    archived=getattr(market, "archived", None),
                )
                if market is not None
                else MarketStatus.UNKNOWN
            )

            self.update_market_state(
                platform="polymarket",
                ticker=token_id,
                title=title,
                yes_bid=ob.best_yes_bid,
                yes_ask=ob.best_yes_ask,
                no_bid=ob.best_no_bid,
                no_ask=ob.best_no_ask,
                volume=vol,
                raw_market=market,
                raw_orderbook=ob,
                condition_id=cid,
                observed_at=now,
                exchange_updated_at=getattr(ob, "exchange_updated_at", None),
                market_status=status,
            )

        return orderbooks

    async def poll_all(self) -> None:
        """Poll both Kalshi and Polymarket concurrently."""
        await asyncio.gather(
            self.poll_kalshi(),
            self.poll_polymarket(),
            return_exceptions=True,
        )

    def _handle_kalshi_ws_event(self, event: WSEvent) -> None:
        """Callback handler for Kalshi WebSocket streaming events.

        Book events use the WS-maintained book (depth levels from that state) and the message's own timestamp as
        ``exchange_updated_at`` (None if the message carried none). ``event.timestamp`` is receive time only.
        """
        if not event.ticker:
            return
        if self._registry_mode and not self._is_collectable("kalshi", event.ticker):
            return

        received = event.timestamp or datetime.now(timezone.utc)
        if event.event_type in ("snapshot", "delta") and isinstance(event.data, KalshiOrderbook):
            ob = event.data
            market = self._kalshi_markets.get(event.ticker)
            self.update_market_state(
                platform="kalshi",
                ticker=event.ticker,
                title=market.title if market else event.ticker,
                yes_bid=ob.best_yes_bid,
                yes_ask=ob.best_yes_ask,
                no_bid=ob.best_no_bid,
                no_ask=ob.best_no_ask,
                volume=market.volume if market else 0,
                ts=received,
                raw_market=market,
                raw_orderbook=ob,
                observed_at=received,
                exchange_updated_at=getattr(event, "exchange_timestamp", None) or getattr(ob, "exchange_updated_at", None),
                market_status=self._kalshi_status(event.ticker, market),
            )
        elif event.event_type == "ticker" and isinstance(event.data, dict):
            # Update volume / price from ticker feed
            t_data = event.data
            vol = t_data.get("volume_fp") or t_data.get("volume")
            try:
                vol_int = int(float(vol)) if vol is not None else None
            except (ValueError, TypeError):
                vol_int = None

            current = self.get_market_state("kalshi", event.ticker)
            if current:
                # Volume-only update: the quote (and its observation/exchange times, depth) is unchanged.
                self.update_market_state(
                    platform="kalshi",
                    ticker=event.ticker,
                    title=current.title,
                    yes_bid=current.yes_bid,
                    yes_ask=current.yes_ask,
                    no_bid=current.no_bid,
                    no_ask=current.no_ask,
                    volume=vol_int if vol_int is not None else current.volume,
                    ts=received,
                    raw_market=current.raw_market,
                    raw_orderbook=current.raw_orderbook,
                    observed_at=current.observed_at,
                    exchange_updated_at=current.exchange_updated_at,
                    market_status=current.market_status,
                    ladders=current.ladders,
                )

    # ------------------------------------------------------------------
    # DuckDB
    # ------------------------------------------------------------------

    def snapshot_to_duckdb(
        self,
        duckdb_path: Optional[Union[str, Path]] = None,
        ts: Optional[datetime] = None,
    ) -> int:
        """Write the current state of all collectable markets into DuckDB (schema v2).

        ``ts`` is the snapshot batch time (legacy column; the replay engine groups rows by it). Each row also carries
        its own ``observed_at_utc`` and nullable ``exchange_updated_at_utc``. Top ``depth_levels`` levels per side are
        written to ``orderbook_levels``. Rows are tagged with this feed's ``data_source``.

        Returns:
            int: Number of snapshot rows written.
        """
        target_path = str(duckdb_path or self.duckdb_path)
        snapshot_ts = ts or datetime.now(timezone.utc)

        items = [s for s in self._market_state.values() if self._is_collectable(s.platform, s.ticker)]
        if not items:
            return 0

        records = [
            snapshot_store.build_snapshot_record(s, snapshot_ts, self.depth_levels, self.data_source) for s in items
        ]

        conn = self._open_store(target_path)
        try:
            written = snapshot_store.write_snapshots(conn, records)
        finally:
            conn.close()

        logger.debug("Wrote %d orderbook snapshots to %s at %s", written, target_path, snapshot_ts)
        return written

    def query_snapshots(
        self,
        query: str = "SELECT * FROM orderbook_snapshots",
        duckdb_path: Optional[Union[str, Path]] = None,
    ) -> list[dict[str, Any]]:
        """Query DuckDB snapshot table and return results as a list of dicts.

        Args:
            query: SQL query string.
            duckdb_path: DuckDB file path.

        Returns:
            list[dict[str, Any]]: Query result rows.
        """
        target_path = str(duckdb_path or self.duckdb_path)
        conn = duckdb.connect(target_path)
        try:
            rel = conn.sql(query)
            if rel is None:
                return []
            cols = rel.columns
            rows = rel.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------

    async def _periodic_poll_loop(self) -> None:
        """Internal background loop to poll REST APIs on poll_interval."""
        while self._running:
            try:
                await self.poll_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in DataFeed polling loop: %s", e)

            try:
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break

    async def _periodic_snapshot_loop(self) -> None:
        """Internal background loop to write DuckDB snapshots on snapshot_interval."""
        while self._running:
            try:
                await asyncio.sleep(self.snapshot_interval)
            except asyncio.CancelledError:
                break

            try:
                self.snapshot_to_duckdb()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error writing periodic DuckDB snapshot: %s", e)

    async def _periodic_gate_loop(self) -> None:
        """Re-check pair tradability every gate_interval seconds (registry mode)."""
        while self._running:
            try:
                await asyncio.sleep(self.gate_interval)
            except asyncio.CancelledError:
                break
            try:
                await self.run_gate()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in collection gate loop: %s", e)

    async def start(self) -> None:
        """Start data feed polling, streaming, and snapshotting background loops."""
        if self._running:
            return

        self._running = True

        if self._registry_mode:
            # Admit only pairs that pass the live check right now.
            await self.run_gate()
        elif not self.kalshi_tickers or not self.polymarket_tokens:
            # Auto-discover if market lists are empty
            await self.discover_markets()

        # Initial poll to populate state immediately
        await self.poll_all()
        # Take initial snapshot
        self.snapshot_to_duckdb()

        # Start WebSocket if enabled
        if self.enable_ws and self.kalshi_tickers:
            if not self.kalshi_ws_client:
                auth = getattr(self.kalshi_client, "auth", None)
                self.kalshi_ws_client = KalshiWSClient(
                    tickers=self.kalshi_tickers,
                    auth=auth,
                    is_sandbox=self.is_sandbox,
                )
            self.kalshi_ws_client.on_update(self._handle_kalshi_ws_event)
            self._ws_task = self.kalshi_ws_client.start()

        # Start periodic tasks
        self._poll_task = asyncio.create_task(self._periodic_poll_loop())
        self._snapshot_task = asyncio.create_task(self._periodic_snapshot_loop())
        if self._registry_mode:
            self._gate_task = asyncio.create_task(self._periodic_gate_loop())

        logger.info(
            "DataFeed started (Kalshi tickers: %d, Polymarket tokens: %d, Snapshot interval: %.1fs, data_source=%s)",
            len(self.kalshi_tickers),
            len(self.polymarket_tokens),
            self.snapshot_interval,
            self.data_source.value,
        )

    async def stop(self) -> None:
        """Stop all background tasks and disconnect WebSocket."""
        self._running = False

        for attr in ("_poll_task", "_snapshot_task", "_gate_task"):
            task = getattr(self, attr)
            if task:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
                setattr(self, attr, None)

        if self.kalshi_ws_client:
            await self.kalshi_ws_client.stop()
            self._ws_task = None

        logger.info("DataFeed stopped.")

    async def close(self) -> None:
        """Stop background tasks and close underlying HTTP clients."""
        await self.stop()
        if self.kalshi_client:
            await self.kalshi_client.close()
        if self.polymarket_client:
            await self.polymarket_client.close()

    async def __aenter__(self) -> DataFeed:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
