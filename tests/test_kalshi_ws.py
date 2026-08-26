"""Unit and integration tests for KalshiWSClient and in-memory orderbook management."""

from __future__ import annotations

import asyncio
import json
import ssl
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from kalshi_client.auth import KalshiAuth
from kalshi_client.models import Orderbook
from kalshi_client.ws import KalshiWSClient, WSEvent, _InMemoryBook


@pytest.fixture
def test_rsa_key() -> rsa.RSAPrivateKey:
    """Generate in-memory 2048-bit RSA private key for testing."""
    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )


@pytest.fixture
def kalshi_auth(test_rsa_key: rsa.RSAPrivateKey) -> KalshiAuth:
    """Create test KalshiAuth instance."""
    return KalshiAuth(
        key_id="test-key-id-1234",
        private_key=test_rsa_key,
        is_sandbox=True,
    )


class TestInMemoryBook:
    """Test _InMemoryBook snapshot and incremental delta tracking."""

    def test_empty_book(self) -> None:
        book = _InMemoryBook("TEST-TICKER")
        ob = book.to_orderbook()
        assert ob.ticker == "TEST-TICKER"
        assert ob.yes_bids == []
        assert ob.no_bids == []
        assert ob.best_yes_bid is None
        assert ob.best_yes_ask is None

    def test_apply_snapshot(self) -> None:
        book = _InMemoryBook("TEST-TICKER")
        yes_raw = [["0.4500", "100.00"], ["0.4000", "50.00"], ["0.5000", "20.00"]]
        no_raw = [["0.4800", "150.00"], ["0.4200", "80.00"]]

        ob = book.apply_snapshot(yes_raw, no_raw, seq=1)

        # Levels should be sorted descending by price in cents
        assert ob.yes_bids == [(50, 20), (45, 100), (40, 50)]
        assert ob.no_bids == [(48, 150), (42, 80)]
        assert ob.best_yes_bid == 50
        assert ob.best_no_bid == 48
        # Derived asks
        assert ob.best_yes_ask == 52  # 100 - 48
        assert ob.best_no_ask == 50   # 100 - 50
        assert ob.yes_spread == 2     # 52 - 50

    def test_apply_delta_add_and_update(self) -> None:
        book = _InMemoryBook("TEST-TICKER")
        book.apply_snapshot([["0.45", "100"]], [["0.48", "100"]], seq=1)

        # 1. Increase existing YES level
        ob = book.apply_delta("yes", "0.45", "50", seq=2)
        assert ob.yes_bids == [(45, 150)]

        # 2. Add new YES level at 47c
        ob = book.apply_delta("yes", "0.47", "30", seq=3)
        assert ob.yes_bids == [(47, 30), (45, 150)]
        assert ob.best_yes_bid == 47

        # 3. Add NO level at 51c
        ob = book.apply_delta("no", "0.51", "60", seq=4)
        assert ob.no_bids == [(51, 60), (48, 100)]
        assert ob.best_no_bid == 51
        assert ob.best_yes_ask == 49  # 100 - 51

    def test_apply_delta_reduce_and_remove(self) -> None:
        book = _InMemoryBook("TEST-TICKER")
        book.apply_snapshot([["0.45", "100"], ["0.40", "50"]], [], seq=1)

        # Reduce level 45c by 40 -> 60 left
        ob = book.apply_delta("yes", "0.45", "-40", seq=2)
        assert ob.yes_bids == [(45, 60), (40, 50)]

        # Reduce level 45c by 60 -> 0 -> should be removed
        ob = book.apply_delta("yes", "0.45", "-60", seq=3)
        assert ob.yes_bids == [(40, 50)]
        assert ob.best_yes_bid == 40

        # Further negative delta on non-existent or zero level should remain empty
        ob = book.apply_delta("yes", "0.45", "-10", seq=4)
        assert ob.yes_bids == [(40, 50)]


class TestKalshiWSClient:
    """Test KalshiWSClient subscriptions, callbacks, and lifecycle."""

    def test_init_and_properties(self, kalshi_auth: KalshiAuth) -> None:
        client = KalshiWSClient(
            tickers=["MKT-1", "MKT-2"],
            channels=["orderbook_delta", "ticker"],
            auth=kalshi_auth,
            is_sandbox=True,
        )
        assert client.is_sandbox is True
        assert client.ws_url == KalshiWSClient.DEFAULT_SANDBOX_WS_URL
        assert "MKT-1" in client.orderbooks
        assert "MKT-2" in client.orderbooks
        assert client.is_connected is False

    def test_auth_headers_generation(self, kalshi_auth: KalshiAuth) -> None:
        client = KalshiWSClient(auth=kalshi_auth, is_sandbox=True)
        headers = client._get_auth_headers()
        assert "KALSHI-ACCESS-KEY" in headers
        assert headers["KALSHI-ACCESS-KEY"] == "test-key-id-1234"
        assert "KALSHI-ACCESS-TIMESTAMP" in headers
        assert "KALSHI-ACCESS-SIGNATURE" in headers

    def test_unauthenticated_headers(self) -> None:
        client = KalshiWSClient(auth=None)
        headers = client._get_auth_headers()
        assert headers == {}

    @pytest.mark.asyncio
    async def test_handle_subscribed_message(self) -> None:
        client = KalshiWSClient(tickers=["MKT-1"])
        events_received: list[WSEvent] = []

        @client.on_update
        def on_upd(evt: WSEvent) -> None:
            events_received.append(evt)

        raw = json.dumps({"type": "subscribed", "sid": 10, "msg": {"channel": "orderbook_delta", "sid": 10}})
        await client._handle_message(raw)

        assert len(events_received) == 1
        assert events_received[0].event_type == "subscribed"
        assert events_received[0].sid == 10
        assert client._sid_to_channel.get(10) == "orderbook_delta"

    @pytest.mark.asyncio
    async def test_handle_orderbook_snapshot_and_delta(self) -> None:
        client = KalshiWSClient(tickers=["MKT-1"])
        received_books: list[Orderbook] = []
        received_events: list[WSEvent] = []

        @client.on_orderbook
        async def on_ob(ob: Orderbook) -> None:
            received_books.append(ob)

        @client.on_update
        def on_upd(evt: WSEvent) -> None:
            received_events.append(evt)

        # 1. Snapshot
        snapshot_msg = json.dumps({
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "MKT-1",
                "yes_dollars_fp": [["0.4800", "200.00"]],
                "no_dollars_fp": [["0.4800", "200.00"]],
            },
        })
        await client._handle_message(snapshot_msg)

        assert len(received_books) == 1
        assert received_books[0].ticker == "MKT-1"
        assert received_books[0].best_yes_bid == 48
        assert received_books[0].best_yes_ask == 52

        # Verify state in client
        stored_ob = client.get_orderbook("MKT-1")
        assert stored_ob is not None
        assert stored_ob.best_yes_bid == 48

        # 2. Delta
        delta_msg = json.dumps({
            "type": "orderbook_delta",
            "sid": 1,
            "seq": 2,
            "msg": {
                "market_ticker": "MKT-1",
                "price_dollars": "0.4800",
                "delta_fp": "100.00",
                "side": "yes",
            },
        })
        await client._handle_message(delta_msg)

        assert len(received_books) == 2
        assert received_books[1].yes_bids == [(48, 300)]
        assert len(received_events) == 2
        assert received_events[1].event_type == "delta"

    @pytest.mark.asyncio
    async def test_handle_ticker_message(self) -> None:
        client = KalshiWSClient(tickers=["MKT-1"])
        ticker_updates: list[dict[str, Any]] = []

        @client.on_ticker
        def on_tk(data: dict[str, Any]) -> None:
            ticker_updates.append(data)

        ticker_msg = json.dumps({
            "type": "ticker",
            "sid": 2,
            "msg": {
                "market_ticker": "MKT-1",
                "price_dollars": "0.5200",
                "yes_bid_dollars": "0.4800",
                "yes_ask_dollars": "0.5200",
                "volume_fp": "1082.00",
            },
        })
        await client._handle_message(ticker_msg)

        assert len(ticker_updates) == 1
        assert ticker_updates[0]["market_ticker"] == "MKT-1"
        assert client.get_ticker_data("MKT-1") == ticker_updates[0]

    @pytest.mark.asyncio
    async def test_dynamic_subscribe_and_unsubscribe(self) -> None:
        client = KalshiWSClient()
        mock_ws = AsyncMock()
        client._ws = mock_ws

        with patch.object(type(client), "is_connected", True):
            await client.subscribe(["MKT-NEW"], channels=["orderbook_delta"])
            assert "MKT-NEW" in client._subscribed_tickers
            assert client.get_orderbook("MKT-NEW") is not None
            mock_ws.send.assert_called_once()

            sent_payload = json.loads(mock_ws.send.call_args[0][0])
            assert sent_payload["cmd"] == "subscribe"
            assert sent_payload["params"]["market_tickers"] == ["MKT-NEW"]

            mock_ws.reset_mock()
            await client.unsubscribe(["MKT-NEW"])
            assert "MKT-NEW" not in client._subscribed_tickers
            mock_ws.send.assert_called_once()
            sent_unsub = json.loads(mock_ws.send.call_args[0][0])
            assert sent_unsub["cmd"] == "unsubscribe"

    @pytest.mark.asyncio
    async def test_auto_reconnect_retry_and_cancellation(self) -> None:
        client = KalshiWSClient(
            tickers=["MKT-1"],
            auto_reconnect=True,
            initial_reconnect_delay=0.01,
            max_reconnect_delay=0.05,
        )

        attempts = 0

        async def mock_connect_and_listen() -> None:
            nonlocal attempts
            attempts += 1
            if attempts <= 2:
                raise ConnectionError(f"Simulated network drop attempt {attempts}")
            # On 3rd attempt, simulate a running connection until stopped
            while client._running:
                await asyncio.sleep(0.01)

        client._connect_and_listen = mock_connect_and_listen  # type: ignore

        task = client.start()
        await asyncio.sleep(0.1)

        assert attempts >= 2
        await client.stop()
        assert client._running is False
        assert task.done()
