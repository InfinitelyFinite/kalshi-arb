"""Kalshi asynchronous WebSocket client with real-time orderbook and ticker feeds.

Features:
- RSA-PSS SHA-256 authentication during WebSocket handshake (optional for read-only market data).
- Subscribes to `orderbook_delta` and `ticker` channels for a configurable list of tickers.
- Maintains in-memory current-state orderbooks per ticker (applying deltas on top of initial snapshots).
- Exposes an `on_update` callback interface (supporting both sync and async callbacks).
- Resilient auto-reconnection with exponential backoff and jitter on disconnect.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import ssl
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, Union
from urllib.parse import urlparse

import websockets
from websockets.exceptions import ConnectionClosed

try:
    import certifi

    DEFAULT_SSL_CONTEXT: Optional[ssl.SSLContext] = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    DEFAULT_SSL_CONTEXT = ssl.create_default_context()

from kalshi_client.auth import KalshiAuth
from kalshi_client.models import Orderbook, _parse_cents, _parse_volume

if TYPE_CHECKING:
    from core.config_loader import AppConfig, KalshiConfig

logger = logging.getLogger(__name__)

# Type aliases for callbacks
CallbackFunc = Union[
    Callable[[Any], Any],
    Callable[[Any], Coroutine[Any, Any, Any]],
]


@dataclass
class WSEvent:
    """Structured event emitted by the Kalshi WebSocket client."""

    event_type: str  # "snapshot", "delta", "ticker", "subscribed", "error"
    ticker: str
    data: Any  # Orderbook or dict (for ticker / raw payload)
    sid: Optional[int] = None
    seq: Optional[int] = None
    timestamp: Optional[datetime] = field(default_factory=lambda: datetime.now(timezone.utc))


class _InMemoryBook:
    """Internal mutable representation of an orderbook for applying snapshots and deltas."""

    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        # price_in_cents -> size_in_contracts
        self.yes_bids: dict[int, int] = {}
        self.no_bids: dict[int, int] = {}
        self.last_seq: Optional[int] = None

    def apply_snapshot(
        self,
        yes_raw: list[Union[list[Any], tuple[Any, Any]]],
        no_raw: list[Union[list[Any], tuple[Any, Any]]],
        seq: Optional[int] = None,
    ) -> Orderbook:
        """Replace all book levels with snapshot data."""
        self.yes_bids.clear()
        self.no_bids.clear()
        self.last_seq = seq

        for item in yes_raw:
            if len(item) >= 2:
                price = _parse_cents(item[0])
                size = _parse_volume(item[1])
                if price is not None and size is not None and size > 0:
                    self.yes_bids[price] = size

        for item in no_raw:
            if len(item) >= 2:
                price = _parse_cents(item[0])
                size = _parse_volume(item[1])
                if price is not None and size is not None and size > 0:
                    self.no_bids[price] = size

        return self.to_orderbook()

    def apply_delta(
        self,
        side: str,
        price_val: Any,
        delta_val: Any,
        seq: Optional[int] = None,
    ) -> Orderbook:
        """Apply incremental signed volume delta to a specific price level."""
        self.last_seq = seq
        price = _parse_cents(price_val)
        delta = _parse_volume(delta_val)

        if price is None or delta is None:
            return self.to_orderbook()

        side_lower = side.lower().strip()
        target_dict = self.yes_bids if side_lower == "yes" else self.no_bids

        current_size = target_dict.get(price, 0)
        new_size = current_size + delta

        if new_size > 0:
            target_dict[price] = new_size
        else:
            target_dict.pop(price, None)

        return self.to_orderbook()

    def to_orderbook(self) -> Orderbook:
        """Export current state as a frozen/immutable Orderbook Pydantic model."""
        sorted_yes = sorted(self.yes_bids.items(), key=lambda x: x[0], reverse=True)
        sorted_no = sorted(self.no_bids.items(), key=lambda x: x[0], reverse=True)
        return Orderbook(
            ticker=self.ticker,
            yes_bids=sorted_yes,
            no_bids=sorted_no,
        )


class KalshiWSClient:
    """Asynchronous WebSocket client for Kalshi Trade API v2."""

    DEFAULT_PROD_WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    DEFAULT_SANDBOX_WS_URL = "wss://demo-api.kalshi.co/trade-api/ws/v2"
    DEFAULT_CHANNELS = ["orderbook_delta", "ticker"]

    def __init__(
        self,
        tickers: Optional[list[str]] = None,
        channels: Optional[list[str]] = None,
        auth: Optional[KalshiAuth] = None,
        ws_url: Optional[str] = None,
        is_sandbox: bool = False,
        auto_reconnect: bool = True,
        initial_reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 30.0,
        ping_interval: float = 10.0,
        ping_timeout: float = 10.0,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        self.auth = auth
        self.is_sandbox = is_sandbox
        self.auto_reconnect = auto_reconnect
        self.initial_reconnect_delay = initial_reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.ssl_context = ssl_context or DEFAULT_SSL_CONTEXT

        if ws_url:
            self.ws_url = ws_url
        else:
            self.ws_url = self.DEFAULT_SANDBOX_WS_URL if is_sandbox else self.DEFAULT_PROD_WS_URL

        self._ws_path = urlparse(self.ws_url).path or "/trade-api/ws/v2"

        # Active subscriptions
        self._subscribed_tickers: set[str] = set(tickers or [])
        self._subscribed_channels: set[str] = set(channels or self.DEFAULT_CHANNELS)

        # In-memory books
        self._books: dict[str, _InMemoryBook] = {}
        for ticker in self._subscribed_tickers:
            self._books[ticker] = _InMemoryBook(ticker)

        # Latest ticker cache
        self._tickers_data: dict[str, dict[str, Any]] = {}

        # Callback registries
        self._update_callbacks: list[CallbackFunc] = []
        self._orderbook_callbacks: list[CallbackFunc] = []
        self._ticker_callbacks: list[CallbackFunc] = []
        self._error_callbacks: list[CallbackFunc] = []

        # Connection and lifecycle state
        self._ws: Optional[websockets.ClientConnection] = None
        self._running: bool = False
        self._task: Optional[asyncio.Task[None]] = None
        self._msg_id_counter: int = 1
        self._sid_to_channel: dict[int, str] = {}
        self._sid_seq_tracker: dict[int, int] = {}
        self._connected_event = asyncio.Event()

    @classmethod
    def from_config(
        cls,
        config: Union[AppConfig, KalshiConfig],
        tickers: Optional[list[str]] = None,
        channels: Optional[list[str]] = None,
        is_sandbox: Optional[bool] = None,
        auto_reconnect: bool = True,
    ) -> KalshiWSClient:
        """Create KalshiWSClient instance from configuration.

        Args:
            config: AppConfig or KalshiConfig.
            tickers: Initial tickers to subscribe to.
            channels: Channels to subscribe to (defaults to orderbook_delta, ticker).
            is_sandbox: Force sandbox mode if True. If None, inferred from config.
            auto_reconnect: Whether to automatically reconnect on disconnects.
        """
        if is_sandbox is None:
            if hasattr(config, "mode"):
                mode_val = config.mode.trading_mode
                mode_str = mode_val.value if hasattr(mode_val, "value") else str(mode_val)
                is_sandbox = mode_str.lower() in ("sandbox", "dry_run")
            else:
                is_sandbox = False

        auth: Optional[KalshiAuth] = None
        try:
            auth = KalshiAuth.from_config(config, is_sandbox=is_sandbox)
        except Exception as e:
            logger.warning(
                "KalshiAuth initialization from config omitted: %s. Proceeding with unauthenticated WS client.",
                e,
            )

        return cls(
            tickers=tickers,
            channels=channels,
            auth=auth,
            is_sandbox=is_sandbox,
            auto_reconnect=auto_reconnect,
        )

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        """Check if websocket is currently active and open."""
        return self._ws is not None and getattr(self._ws, "state", None) == websockets.protocol.State.OPEN

    @property
    def orderbooks(self) -> dict[str, Orderbook]:
        """Return snapshot of current in-memory orderbooks for all tracked tickers."""
        return {ticker: book.to_orderbook() for ticker, book in self._books.items()}

    def get_orderbook(self, ticker: str) -> Optional[Orderbook]:
        """Retrieve current orderbook for a specific ticker, or None if not tracked."""
        book = self._books.get(ticker)
        return book.to_orderbook() if book else None

    def get_ticker_data(self, ticker: str) -> Optional[dict[str, Any]]:
        """Retrieve latest ticker feed payload for a specific ticker."""
        return self._tickers_data.get(ticker)

    # -------------------------------------------------------------------------
    # Callback Registration
    # -------------------------------------------------------------------------

    def on_update(self, callback: CallbackFunc) -> CallbackFunc:
        """Register a general update callback receiving WSEvent objects."""
        if callback not in self._update_callbacks:
            self._update_callbacks.append(callback)
        return callback

    def on_orderbook(self, callback: CallbackFunc) -> CallbackFunc:
        """Register a callback specifically receiving updated Orderbook objects."""
        if callback not in self._orderbook_callbacks:
            self._orderbook_callbacks.append(callback)
        return callback

    def on_ticker(self, callback: CallbackFunc) -> CallbackFunc:
        """Register a callback specifically receiving ticker dictionary updates."""
        if callback not in self._ticker_callbacks:
            self._ticker_callbacks.append(callback)
        return callback

    def on_error(self, callback: CallbackFunc) -> CallbackFunc:
        """Register a callback receiving error payloads or exceptions."""
        if callback not in self._error_callbacks:
            self._error_callbacks.append(callback)
        return callback

    async def _dispatch_callback(self, callback: CallbackFunc, arg: Any) -> None:
        """Execute a callback safely, handling both sync and async functions."""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(arg)
            else:
                callback(arg)
        except Exception as e:
            logger.exception("Error in WebSocket callback %s: %s", callback, e)

    async def _emit_event(self, event: WSEvent) -> None:
        """Dispatch event to registered callbacks."""
        # 1. General on_update listeners
        for cb in self._update_callbacks:
            await self._dispatch_callback(cb, event)

        # 2. Specific on_orderbook listeners
        if event.event_type in ("snapshot", "delta") and isinstance(event.data, Orderbook):
            for cb in self._orderbook_callbacks:
                await self._dispatch_callback(cb, event.data)

        # 3. Specific on_ticker listeners
        elif event.event_type == "ticker" and isinstance(event.data, dict):
            for cb in self._ticker_callbacks:
                await self._dispatch_callback(cb, event.data)

        # 4. Error listeners
        elif event.event_type == "error":
            for cb in self._error_callbacks:
                await self._dispatch_callback(cb, event.data)

    # -------------------------------------------------------------------------
    # Authentication & Subscription Commands
    # -------------------------------------------------------------------------

    def _get_auth_headers(self) -> dict[str, str]:
        """Generate RSA-PSS headers for WS connection handshake if auth is configured."""
        if not self.auth:
            return {}
        return self.auth.get_auth_headers(method="GET", path=self._ws_path)

    async def _send_json(self, data: dict[str, Any]) -> None:
        """Send a JSON payload over the active WebSocket connection."""
        if not self._ws:
            raise ConnectionError("WebSocket is not connected")
        payload = json.dumps(data)
        await self._ws.send(payload)

    async def _send_subscriptions(self) -> None:
        """Send subscription commands for all tracked tickers and channels."""
        if not self._subscribed_channels or not self._subscribed_tickers:
            return

        tickers_list = sorted(list(self._subscribed_tickers))
        channels_list = sorted(list(self._subscribed_channels))

        for channel in channels_list:
            self._msg_id_counter += 1
            sub_msg = {
                "id": self._msg_id_counter,
                "cmd": "subscribe",
                "params": {
                    "channels": [channel],
                    "market_tickers": tickers_list,
                },
            }
            logger.info("Sending subscription: channel=%s, tickers=%s", channel, tickers_list)
            await self._send_json(sub_msg)

    async def subscribe(
        self,
        tickers: Union[str, list[str]],
        channels: Optional[list[str]] = None,
    ) -> None:
        """Dynamically subscribe to one or more tickers and channels.

        Args:
            tickers: Market ticker string or list of market tickers.
            channels: Optional list of channels (defaults to client's subscribed channels).
        """
        ticker_list = [tickers] if isinstance(tickers, str) else list(tickers)
        target_channels = channels or list(self._subscribed_channels) or self.DEFAULT_CHANNELS

        new_tickers: list[str] = []
        for t in ticker_list:
            if t not in self._books:
                self._books[t] = _InMemoryBook(t)
            if t not in self._subscribed_tickers:
                self._subscribed_tickers.add(t)
                new_tickers.append(t)

        for c in target_channels:
            self._subscribed_channels.add(c)

        if self.is_connected and new_tickers:
            for channel in target_channels:
                self._msg_id_counter += 1
                msg = {
                    "id": self._msg_id_counter,
                    "cmd": "subscribe",
                    "params": {
                        "channels": [channel],
                        "market_tickers": new_tickers,
                    },
                }
                await self._send_json(msg)


    async def unsubscribe(
        self,
        tickers: Union[str, list[str]],
        channels: Optional[list[str]] = None,
    ) -> None:
        """Dynamically unsubscribe from tickers."""
        ticker_list = [tickers] if isinstance(tickers, str) else list(tickers)
        target_channels = channels or list(self._subscribed_channels)

        for t in ticker_list:
            self._subscribed_tickers.discard(t)

        if self.is_connected:
            self._msg_id_counter += 1
            msg = {
                "id": self._msg_id_counter,
                "cmd": "unsubscribe",
                "params": {
                    "channels": target_channels,
                    "market_tickers": ticker_list,
                },
            }
            try:
                await self._send_json(msg)
            except Exception as e:
                logger.warning("Failed to send unsubscribe command: %s", e)

    # -------------------------------------------------------------------------
    # Message Handling & Orderbook Maintenance
    # -------------------------------------------------------------------------

    def _extract_ticker(self, msg_body: dict[str, Any]) -> str:
        """Extract market ticker symbol from message payload."""
        return msg_body.get("market_ticker") or msg_body.get("ticker") or ""

    async def _handle_message(self, raw_message: str) -> None:
        """Process incoming raw WebSocket JSON message."""
        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError as e:
            logger.warning("Received invalid JSON from Kalshi WebSocket: %s", e)
            return

        msg_type = data.get("type", "")
        sid = data.get("sid")
        seq = data.get("seq")
        msg_body = data.get("msg") or {}

        # Track sequence gap detection
        if sid is not None and seq is not None:
            last_seq = self._sid_seq_tracker.get(sid)
            if last_seq is not None and seq > last_seq + 1:
                logger.warning(
                    "Sequence gap detected on sid %s! Expected %d, got %d. Some updates may have been missed.",
                    sid,
                    last_seq + 1,
                    seq,
                )
            self._sid_seq_tracker[sid] = seq

        if msg_type == "subscribed":
            channel = msg_body.get("channel")
            if sid is not None and channel:
                self._sid_to_channel[sid] = channel
            logger.info("Subscribed successfully: channel=%s, sid=%s", channel, sid)
            await self._emit_event(
                WSEvent(
                    event_type="subscribed",
                    ticker="",
                    data=msg_body,
                    sid=sid,
                    seq=seq,
                )
            )

        elif msg_type == "orderbook_snapshot":
            ticker = self._extract_ticker(msg_body)
            if not ticker:
                return

            if ticker not in self._books:
                self._books[ticker] = _InMemoryBook(ticker)

            book_obj = self._books[ticker]
            yes_raw = msg_body.get("yes_dollars_fp") or msg_body.get("yes_dollars") or msg_body.get("yes") or []
            no_raw = msg_body.get("no_dollars_fp") or msg_body.get("no_dollars") or msg_body.get("no") or []

            updated_book = book_obj.apply_snapshot(yes_raw, no_raw, seq=seq)
            await self._emit_event(
                WSEvent(
                    event_type="snapshot",
                    ticker=ticker,
                    data=updated_book,
                    sid=sid,
                    seq=seq,
                )
            )

        elif msg_type == "orderbook_delta":
            ticker = self._extract_ticker(msg_body)
            if not ticker:
                return

            if ticker not in self._books:
                self._books[ticker] = _InMemoryBook(ticker)

            book_obj = self._books[ticker]
            side = msg_body.get("side", "")
            price = msg_body.get("price_dollars") if "price_dollars" in msg_body else msg_body.get("price")
            delta = msg_body.get("delta_fp") if "delta_fp" in msg_body else msg_body.get("delta")

            updated_book = book_obj.apply_delta(side=side, price_val=price, delta_val=delta, seq=seq)
            await self._emit_event(
                WSEvent(
                    event_type="delta",
                    ticker=ticker,
                    data=updated_book,
                    sid=sid,
                    seq=seq,
                )
            )

        elif msg_type == "ticker":
            ticker = self._extract_ticker(msg_body)
            if ticker:
                self._tickers_data[ticker] = msg_body

            await self._emit_event(
                WSEvent(
                    event_type="ticker",
                    ticker=ticker,
                    data=msg_body,
                    sid=sid,
                    seq=seq,
                )
            )

        elif msg_type == "error":
            logger.error("Kalshi WebSocket error message: %s", msg_body)
            await self._emit_event(
                WSEvent(
                    event_type="error",
                    ticker="",
                    data=msg_body,
                    sid=sid,
                    seq=seq,
                )
            )

        else:
            logger.debug("Received unhandled message type '%s': %s", msg_type, data)

    # -------------------------------------------------------------------------
    # Connection Lifecycle & Reconnect Loop
    # -------------------------------------------------------------------------

    async def _connect_and_listen(self) -> None:
        """Establish single WebSocket connection, subscribe, and listen for messages."""
        headers = self._get_auth_headers()
        connect_kwargs: dict[str, Any] = {
            "ssl": self.ssl_context,
            "ping_interval": self.ping_interval,
            "ping_timeout": self.ping_timeout,
        }

        if headers:
            connect_kwargs["additional_headers"] = headers

        logger.info("Connecting to Kalshi WebSocket at %s...", self.ws_url)
        async with websockets.connect(self.ws_url, **connect_kwargs) as ws:
            self._ws = ws
            self._connected_event.set()
            logger.info("Connected to Kalshi WebSocket.")

            # Send subscriptions
            await self._send_subscriptions()

            # Message processing loop
            async for message in ws:
                if not self._running:
                    break
                await self._handle_message(message)

    async def run(self) -> None:
        """Main client loop managing connection and exponential backoff auto-reconnect."""
        self._running = True
        reconnect_delay = self.initial_reconnect_delay

        while self._running:
            try:
                self._connected_event.clear()
                await self._connect_and_listen()
                # If connected and processed without immediate error, reset reconnect delay
                reconnect_delay = self.initial_reconnect_delay
            except asyncio.CancelledError:
                logger.info("KalshiWSClient run loop cancelled.")
                break
            except Exception as e:
                self._ws = None
                self._connected_event.clear()
                if not self._running:
                    break

                logger.warning(
                    "Kalshi WebSocket connection closed or failed (%s: %s). Reconnecting in %.2fs...",
                    type(e).__name__,
                    e,
                    reconnect_delay,
                )

                # Emit error event
                await self._emit_event(
                    WSEvent(
                        event_type="error",
                        ticker="",
                        data={"exception": str(e), "type": type(e).__name__},
                    )
                )

                if not self.auto_reconnect:
                    logger.info("auto_reconnect is False. Terminating WS client.")
                    break

                # Exponential backoff with jitter
                jitter = random.uniform(0.8, 1.2)
                sleep_duration = min(reconnect_delay * jitter, self.max_reconnect_delay)
                try:
                    await asyncio.sleep(sleep_duration)
                except asyncio.CancelledError:
                    break

                reconnect_delay = min(reconnect_delay * 2.0, self.max_reconnect_delay)
            finally:
                self._ws = None

        self._running = False
        logger.info("KalshiWSClient run loop exited.")

    def start(self) -> asyncio.Task[None]:
        """Start WebSocket client in a background asyncio Task."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run())
        return self._task

    async def stop(self) -> None:
        """Gracefully stop WebSocket client and disconnect."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def wait_until_connected(self, timeout: float = 10.0) -> bool:
        """Wait until websocket connection is established."""
        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def __aenter__(self) -> KalshiWSClient:
        """Async context manager entrance."""
        self.start()
        await self.wait_until_connected()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.stop()
