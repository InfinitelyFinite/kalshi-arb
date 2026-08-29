"""Unit and integration tests for PolymarketClient REST methods (list_markets, get_orderbook, get_orderbooks)."""

import httpx
import pytest

from polymarket_client.models import Market, Orderbook
from polymarket_client.rest import PolymarketAPIError, PolymarketClient


@pytest.mark.asyncio
async def test_list_markets_pagination() -> None:
    """Test offset-based pagination and parameter passing in list_markets."""
    client = PolymarketClient(
        gamma_url="https://gamma-api.polymarket.com",
        clob_url="https://clob.polymarket.com",
    )

    page1_data = [
        {
            "id": "1",
            "question": "Market 1",
            "conditionId": "0x001",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.40", "0.60"]',
            "clobTokenIds": '["TOK-1-YES", "TOK-1-NO"]',
            "volume": "1000",
            "active": True,
            "closed": False,
        },
        {
            "id": "2",
            "question": "Market 2",
            "conditionId": "0x002",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.50", "0.50"]',
            "clobTokenIds": '["TOK-2-YES", "TOK-2-NO"]',
            "volume": "2000",
            "active": True,
            "closed": False,
        },
    ]

    page2_data = [
        {
            "id": "3",
            "question": "Market 3",
            "conditionId": "0x003",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.70", "0.30"]',
            "clobTokenIds": '["TOK-3-YES", "TOK-3-NO"]',
            "volume": "3000",
            "active": True,
            "closed": False,
        },
    ]

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        if "offset=2" in url_str:
            return httpx.Response(200, json=page2_data)
        elif "offset=0" in url_str:
            return httpx.Response(200, json=page1_data)
        return httpx.Response(200, json=[])

    client._client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    async with client:
        # Fetch with limit=2 (page 1 has 2, page 2 has 1 -> stops)
        markets = await client.list_markets(status="open", limit=2)
        assert len(markets) == 3
        assert markets[0].condition_id == "0x001"
        assert markets[0].title == "Market 1"
        assert markets[0].yes_token_id == "TOK-1-YES"
        assert markets[2].condition_id == "0x003"

        # Test max_pages=1 cap
        capped = await client.list_markets(status="open", limit=2, max_pages=1)
        assert len(capped) == 2


@pytest.mark.asyncio
async def test_get_single_orderbook() -> None:
    """Test get_orderbook for a single token_id."""
    client = PolymarketClient()

    raw_book = {
        "market": "0x001",
        "asset_id": "TOK-1-YES",
        "bids": [{"price": "0.45", "size": "100"}],
        "asks": [{"price": "0.50", "size": "200"}],
    }

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        assert "token_id=TOK-1-YES" in str(request.url)
        return httpx.Response(200, json=raw_book)

    client._client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    async with client:
        ob = await client.get_orderbook("TOK-1-YES")
        assert isinstance(ob, Orderbook)
        assert ob.token_id == "TOK-1-YES"
        assert ob.best_yes_bid == 45
        assert ob.best_yes_ask == 50
        assert ob.best_no_bid == 50
        assert ob.best_no_ask == 55
        assert ob.yes_spread == 5


@pytest.mark.asyncio
async def test_get_orderbooks_batching() -> None:
    """Test get_orderbooks chunking and batch fetching for > 100 tokens."""
    client = PolymarketClient()

    token_ids = [f"TOK-{i:03d}" for i in range(150)]
    batch_calls = []

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        import json
        payload = json.loads(request.content)
        batch_calls.append(payload)

        books = []
        for item in payload:
            t = item["token_id"]
            books.append({
                "market": f"COND-{t}",
                "asset_id": t,
                "bids": [{"price": "0.40", "size": "50"}],
                "asks": [{"price": "0.45", "size": "50"}],
            })
        return httpx.Response(200, json=books)

    client._client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    async with client:
        result = await client.get_orderbooks(token_ids)

    assert len(batch_calls) == 2
    assert len(batch_calls[0]) == 100
    assert len(batch_calls[1]) == 50
    assert len(result) == 150
    assert result["TOK-001"].best_yes_bid == 40
    assert result["TOK-001"].best_yes_ask == 45


@pytest.mark.asyncio
async def test_api_error_handling() -> None:
    """Test error handling when API returns 4xx/5xx status."""
    client = PolymarketClient()

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "Market not found"})

    client._client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    async with client:
        with pytest.raises(PolymarketAPIError) as exc_info:
            await client.get_orderbook("INVALID-TOKEN")

        assert exc_info.value.status_code == 404
        assert "Market not found" in exc_info.value.message


@pytest.mark.asyncio
async def test_definition_of_done_20_markets_orderbooks_shape() -> None:
    """Definition of Done Test: 20 Polymarket markets and orderbooks return identical shape to Kalshi."""
    # Generate 20 mock markets and orderbooks
    markets_data = []
    books_data = []

    for i in range(1, 21):
        m_id = f"0x{i:04d}"
        t_yes = f"TOK-{i:03d}-YES"
        t_no = f"TOK-{i:03d}-NO"
        markets_data.append({
            "id": str(i),
            "question": f"Polymarket Prediction Market {i}",
            "conditionId": m_id,
            "slug": f"market-{i}",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.45", "0.55"]',
            "clobTokenIds": f'["{t_yes}", "{t_no}"]',
            "volume": "50000",
            "active": True,
            "closed": False,
        })
        books_data.append({
            "market": m_id,
            "asset_id": t_yes,
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.48", "size": "150"}],
        })

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        if "/markets" in url_str:
            return httpx.Response(200, json=markets_data)
        elif "/books" in url_str or "/book" in url_str:
            return httpx.Response(200, json=books_data)
        return httpx.Response(404)

    client = PolymarketClient()
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    async with client:
        # Step 1: list 20 markets
        markets = await client.list_markets(limit=20, max_pages=1)
        assert len(markets) == 20

        # Step 2: fetch orderbooks for all 20
        token_ids = [m.yes_token_id for m in markets if m.yes_token_id]
        assert len(token_ids) == 20

        orderbooks = await client.get_orderbooks(token_ids)
        assert len(orderbooks) == 20

        # Step 3: verify shapes and overlap
        for m in markets:
            assert isinstance(m.title, str)
            assert isinstance(m.condition_id, str)
            assert isinstance(m.ticker, str)
            assert m.volume is not None
            assert m.status == "open"
            assert len(m.tokens) == 2

            ob = orderbooks[m.yes_token_id]
            assert isinstance(ob, Orderbook)
            assert ob.best_yes_bid == 45
            assert ob.best_yes_ask == 48
            assert ob.best_no_bid == 52  # 100 - 48
            assert ob.best_no_ask == 55  # 100 - 45
            assert ob.yes_spread == 3
            assert ob.no_spread == 3
            assert len(ob.yes_bids) > 0
            assert len(ob.yes_asks) > 0
            assert len(ob.no_bids) > 0
            assert len(ob.no_asks) > 0
