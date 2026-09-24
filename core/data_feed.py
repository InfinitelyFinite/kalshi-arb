"""Unified real-time and polling DataFeed for Kalshi and Polymarket prediction markets.

Features:
- Owns Kalshi and Polymarket REST and WebSocket clients.
- Maintains in-memory dictionary of normalized market state keyed by `(platform, ticker)`.
- Supports REST polling, WebSocket streaming, and periodic snapshots.
- `snapshot_to_duckdb()` writes current market states into DuckDB with schema:
  `orderbook_snapshots(ts, platform, ticker, yes_bid, yes_ask, no_bid, no_ask, volume)`.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from kalshi_client.models import Market as KalshiMarket, Orderbook as KalshiOrderbook
from kalshi_client.rest import KalshiClient
from kalshi_client.ws import KalshiWSClient, WSEvent
from polymarket_client.models import Market as PolyMarket, Orderbook as PolyOrderbook
from polymarket_client.rest import PolymarketClient

if TYPE_CHECKING:
    from core.config_loader import AppConfig

logger = logging.getLogger(__name__)


class MarketState(BaseModel):
    """Normalized snapshot of a prediction market's current state."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

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


class DataFeed:
    """Unified data feed manager for Kalshi and Polymarket prediction markets."""

    DEFAULT_DUCKDB_PATH = "data/snapshots.duckdb"

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        kalshi_client: Optional[KalshiClient] = None,
        kalshi_ws_client: Optional[KalshiWSClient] = None,
        polymarket_client: Optional[PolymarketClient] = None,
        kalshi_tickers: Optional[list[str]] = None,
        polymarket_tokens: Optional[list[str]] = None,
        duckdb_path: Union[str, Path] = DEFAULT_DUCKDB_PATH,
        snapshot_interval: float = 60.0,
        poll_interval: float = 10.0,
        is_sandbox: Optional[bool] = None,
        enable_ws: bool = True,
    ) -> None:
        """Initialize DataFeed.

        Args:
            config: Optional AppConfig instance.
            kalshi_client: Injected Kalshi REST client.
            kalshi_ws_client: Injected Kalshi WebSocket client.
            polymarket_client: Injected Polymarket REST client.
            kalshi_tickers: List of Kalshi market tickers to track.
            polymarket_tokens: List of Polymarket token IDs or condition IDs to track.
            duckdb_path: Filepath or :memory: for DuckDB storage.
            snapshot_interval: Interval in seconds between DuckDB snapshot writes (default 60s).
            poll_interval: Interval in seconds between REST poll cycles (default 10s).
            is_sandbox: Sandbox mode flag for Kalshi.
            enable_ws: Whether to enable Kalshi WebSocket streaming.
        """
        self.config = config
        self.duckdb_path = str(duckdb_path)
        self.snapshot_interval = snapshot_interval
        self.poll_interval = poll_interval
        self.enable_ws = enable_ws

        # Determine sandbox mode
        if is_sandbox is not None:
            self.is_sandbox = is_sandbox
        elif config and hasattr(config, "mode"):
            mode_val = config.mode.trading_mode
            mode_str = mode_val.value if hasattr(mode_val, "value") else str(mode_val)
            self.is_sandbox = mode_str.lower() in ("sandbox", "dry_run")
        else:
            self.is_sandbox = False

        # Clients
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
        self._ws_task: Optional[asyncio.Task[None]] = None

    @property
    def market_state(self) -> dict[tuple[str, str], MarketState]:
        """Current in-memory market states mapping (platform, ticker) -> MarketState."""
        return self._market_state

    def get_market_state(self, platform: str, ticker: str) -> Optional[MarketState]:
        """Retrieve current state for a specific market platform and ticker."""
        return self._market_state.get((platform.lower(), ticker))

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
    ) -> MarketState:
        """Update or insert a market's state in memory."""
        platform_key = platform.lower()
        key = (platform_key, ticker)
        now_ts = ts or datetime.now(timezone.utc)

        existing = self._market_state.get(key)
        if existing:
            # Update fields preserving previous metadata if not supplied
            new_title = title if title is not None else existing.title
            new_volume = volume if volume is not None else existing.volume
            new_raw_market = raw_market if raw_market is not None else existing.raw_market
            new_raw_ob = raw_orderbook if raw_orderbook is not None else existing.raw_orderbook

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
            )
        else:
            state = MarketState(
                platform=platform_key,
                ticker=ticker,
                title=title,
                yes_bid=yes_bid,
                yes_ask=yes_ask,
                no_bid=no_bid,
                no_ask=no_ask,
                volume=volume or 0,
                ts=now_ts,
                raw_market=raw_market,
                raw_orderbook=raw_orderbook,
            )

        self._market_state[key] = state
        return state

    async def discover_markets(self, limit: int = 30) -> None:
        """Auto-discover open markets from Kalshi and Polymarket if none are configured."""
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

    async def poll_kalshi(self) -> dict[str, KalshiOrderbook]:
        """Fetch orderbooks for tracked Kalshi tickers and update in-memory state."""
        if not self.kalshi_tickers:
            return {}

        try:
            orderbooks = await self.kalshi_client.get_orderbooks(self.kalshi_tickers)
        except Exception as e:
            logger.warning("Error fetching Kalshi orderbooks: %s", e)
            return {}

        for ticker, ob in orderbooks.items():
            market = self._kalshi_markets.get(ticker)
            title = market.title if market else ticker
            vol = market.volume if market else 0

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
            )

        return orderbooks

    async def poll_polymarket(self) -> dict[str, PolyOrderbook]:
        """Fetch orderbooks for tracked Polymarket tokens and update in-memory state."""
        if not self.polymarket_tokens:
            return {}

        try:
            orderbooks = await self.polymarket_client.get_orderbooks(self.polymarket_tokens)
        except Exception as e:
            logger.warning("Error fetching Polymarket orderbooks: %s", e)
            return {}

        for token_id, ob in orderbooks.items():
            market = self._poly_token_to_market.get(token_id) or self._polymarket_markets.get(token_id)
            title = market.title if market else token_id
            vol = market.volume if market else 0

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
        """Callback handler for Kalshi WebSocket streaming events."""
        if not event.ticker:
            return

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
                ts=event.timestamp or datetime.now(timezone.utc),
                raw_market=market,
                raw_orderbook=ob,
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
                self.update_market_state(
                    platform="kalshi",
                    ticker=event.ticker,
                    title=current.title,
                    yes_bid=current.yes_bid,
                    yes_ask=current.yes_ask,
                    no_bid=current.no_bid,
                    no_ask=current.no_ask,
                    volume=vol_int if vol_int is not None else current.volume,
                    ts=event.timestamp or datetime.now(timezone.utc),
                    raw_market=current.raw_market,
                    raw_orderbook=current.raw_orderbook,
                )

    def snapshot_to_duckdb(
        self,
        duckdb_path: Optional[Union[str, Path]] = None,
        ts: Optional[datetime] = None,
    ) -> int:
        """Write the current state of all tracked markets into DuckDB.

        Schema:
            table orderbook_snapshots(
                ts TIMESTAMP WITH TIME ZONE,
                platform VARCHAR,
                ticker VARCHAR,
                yes_bid INTEGER,
                yes_ask INTEGER,
                no_bid INTEGER,
                no_ask INTEGER,
                volume BIGINT
            )

        Args:
            duckdb_path: Target DuckDB database path or :memory:.
            ts: Snapshot timestamp (defaults to current UTC time).

        Returns:
            int: Number of snapshot rows written.
        """
        target_path = str(duckdb_path or self.duckdb_path)
        snapshot_ts = ts or datetime.now(timezone.utc)

        if not self._market_state:
            return 0

        # Snapshot current dictionary state
        items = list(self._market_state.values())
        rows = [
            (
                snapshot_ts,
                s.platform,
                s.ticker,
                s.yes_bid,
                s.yes_ask,
                s.no_bid,
                s.no_ask,
                int(s.volume) if s.volume is not None else 0,
            )
            for s in items
        ]

        if target_path != ":memory:":
            Path(target_path).parent.mkdir(parents=True, exist_ok=True)

        conn = duckdb.connect(target_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                    ts TIMESTAMP WITH TIME ZONE,
                    platform VARCHAR,
                    ticker VARCHAR,
                    yes_bid INTEGER,
                    yes_ask INTEGER,
                    no_bid INTEGER,
                    no_ask INTEGER,
                    volume BIGINT
                );
            """)

            conn.executemany(
                """
                INSERT INTO orderbook_snapshots (
                    ts, platform, ticker, yes_bid, yes_ask, no_bid, no_ask, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                rows,
            )
        finally:
            conn.close()

        logger.debug("Wrote %d orderbook snapshots to %s at %s", len(rows), target_path, snapshot_ts)
        return len(rows)

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
            df = rel.df()
            return df.to_dict(orient="records")
        finally:
            conn.close()

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

    async def start(self) -> None:
        """Start data feed polling, streaming, and snapshotting background loops."""
        if self._running:
            return

        self._running = True

        # Auto-discover if market lists are empty
        if not self.kalshi_tickers or not self.polymarket_tokens:
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

        logger.info(
            "DataFeed started (Kalshi tickers: %d, Polymarket tokens: %d, Snapshot interval: %.1fs)",
            len(self.kalshi_tickers),
            len(self.polymarket_tokens),
            self.snapshot_interval,
        )

    async def stop(self) -> None:
        """Stop all background tasks and disconnect WebSocket."""
        self._running = False

        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass
            self._poll_task = None

        if self._snapshot_task:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except (asyncio.CancelledError, Exception):
                pass
            self._snapshot_task = None

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
