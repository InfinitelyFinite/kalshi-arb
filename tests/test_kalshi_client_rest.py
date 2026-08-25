"""Unit and integration tests for KalshiClient REST methods (pagination, batching, models)."""

import httpx
import pytest

from kalshi_client.auth import KalshiAuth
from kalshi_client.models import Market, Orderbook
from kalshi_client.rest import KalshiClient


@pytest.fixture
def mock_auth() -> KalshiAuth:
    # Use a dummy key for testing REST request routing
    from cryptography.hazmat.primitives.asymmetric import rsa
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return KalshiAuth(key_id="test-key-id", private_key=key, is_sandbox=True)


@pytest.mark.asyncio
async def test_list_markets_pagination(mock_auth: KalshiAuth) -> None:
    """Test cursor-based pagination in list_markets."""
    client = KalshiClient(auth=mock_auth, base_url="https://demo-api.kalshi.co/trade-api/v2")

    # Mock page 1 and page 2 responses
    page1_data = {
        "cursor": "cursor_page_2",
        "markets": [
            {
                "ticker": "MKT-1",
                "title": "Market 1",
                "yes_bid_dollars": "0.4000",
                "yes_ask_dollars": "0.4500",
                "no_bid_dollars": "0.5500",
                "no_ask_dollars": "0.6000",
            },
        ],
    }
    page2_data = {
        "cursor": "",
        "markets": [
            {
                "ticker": "MKT-2",
                "title": "Market 2",
                "yes_bid_dollars": "0.7000",
                "yes_ask_dollars": "0.7500",
                "no_bid_dollars": "0.2500",
                "no_ask_dollars": "0.3000",
            },
        ],
    }

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        if "cursor=cursor_page_2" in url_str:
            return httpx.Response(200, json=page2_data)
        return httpx.Response(200, json=page1_data)

    client._client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    async with client:
        # Test full pagination (fetches both pages)
        all_markets = await client.list_markets(status="open")
        assert len(all_markets) == 2
        assert all_markets[0].ticker == "MKT-1"
        assert all_markets[0].yes_bid == 40
        assert all_markets[1].ticker == "MKT-2"
        assert all_markets[1].yes_bid == 70

        # Test max_pages=1 cap
        capped_markets = await client.list_markets(status="open", max_pages=1)
        assert len(capped_markets) == 1
        assert capped_markets[0].ticker == "MKT-1"


@pytest.mark.asyncio
async def test_get_orderbooks_chunking(mock_auth: KalshiAuth) -> None:
    """Test get_orderbooks chunking for > 100 tickers."""
    client = KalshiClient(auth=mock_auth, base_url="https://demo-api.kalshi.co/trade-api/v2")

    # Generate 150 dummy tickers
    tickers = [f"TICKER-{i:03d}" for i in range(150)]

    requested_chunks = []

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        tickers_in_query = request.url.params.get_list("tickers")
        requested_chunks.append(tickers_in_query)

        orderbooks = []
        for t in tickers_in_query:
            orderbooks.append({
                "ticker": t,
                "orderbook_fp": {
                    "yes_dollars": [["0.4000", "10.00"]],
                    "no_dollars": [["0.5500", "20.00"]],
                }
            })
        return httpx.Response(200, json={"orderbooks": orderbooks})

    client._client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    async with client:
        result = await client.get_orderbooks(tickers)

    # 150 tickers should be split into 2 chunks: chunk 1 (100) and chunk 2 (50)
    assert len(requested_chunks) == 2
    assert len(requested_chunks[0]) == 100
    assert len(requested_chunks[1]) == 50
    assert len(result) == 150

    # Verify parsed derived asks
    sample_ob = result["TICKER-001"]
    assert isinstance(sample_ob, Orderbook)
    assert sample_ob.best_yes_bid == 40
    assert sample_ob.best_no_bid == 55
    assert sample_ob.best_yes_ask == 45  # 100 - 55
    assert sample_ob.best_no_ask == 60   # 100 - 40
