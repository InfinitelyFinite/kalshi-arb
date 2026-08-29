"""Polymarket asynchronous REST client using httpx.

Note: Polymarket has no official Python SDK either; use httpx directly,
and public market/orderbook data needs no auth.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional, Union

import httpx

from polymarket_client.models import Market, Orderbook

if TYPE_CHECKING:
    from core.config_loader import AppConfig, PolymarketConfig

logger = logging.getLogger(__name__)


class PolymarketAPIError(Exception):
    """Custom exception raised when a Polymarket API request fails."""

    def __init__(self, status_code: int, message: str, response_body: Optional[str] = None) -> None:
        super().__init__(f"Polymarket API Error {status_code}: {message}")
        self.status_code = status_code
        self.message = message
        self.response_body = response_body


class PolymarketClient:
    """Asynchronous client for interacting with Polymarket Gamma (discovery) and CLOB (orderbook) APIs.

    Public market and orderbook endpoints require no authentication.
    """

    DEFAULT_GAMMA_URL = "https://gamma-api.polymarket.com"
    DEFAULT_CLOB_URL = "https://clob.polymarket.com"

    def __init__(
        self,
        gamma_url: Optional[str] = None,
        clob_url: Optional[str] = None,
        timeout: float = 10.0,
    ) -> None:
        self.gamma_url = (gamma_url or self.DEFAULT_GAMMA_URL).rstrip("/")
        self.clob_url = (clob_url or self.DEFAULT_CLOB_URL).rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout)

    @classmethod
    def from_config(
        cls,
        config: Union[AppConfig, PolymarketConfig],
        timeout: float = 10.0,
    ) -> PolymarketClient:
        """Initialize PolymarketClient from application configuration.

        Args:
            config: AppConfig or PolymarketConfig instance.
            timeout: Request timeout in seconds.
        """
        poly_cfg = config.polymarket if hasattr(config, "polymarket") else config
        gamma_url = getattr(poly_cfg, "gamma_url", cls.DEFAULT_GAMMA_URL)
        clob_url = getattr(poly_cfg, "clob_url", cls.DEFAULT_CLOB_URL)
        return cls(gamma_url=gamma_url, clob_url=clob_url, timeout=timeout)

    async def _request(
        self,
        method: str,
        url: str,
        params: Optional[Union[dict[str, Any], list[tuple[str, Any]]]] = None,
        json: Optional[Any] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Execute an HTTP request with centralized error handling."""
        response = await self._client.request(
            method=method.upper(),
            url=url,
            params=params,
            json=json,
            headers=headers,
        )

        if response.is_error:
            try:
                err_data = response.json()
                err_msg = (
                    err_data.get("message")
                    or err_data.get("error")
                    or err_data.get("error_msg")
                    or response.text
                )
            except Exception:
                err_msg = response.text

            logger.error(
                "Polymarket API request %s %s failed (%s): %s",
                method.upper(),
                url,
                response.status_code,
                err_msg,
            )
            raise PolymarketAPIError(
                status_code=response.status_code,
                message=err_msg,
                response_body=response.text,
            )

        return response

    async def list_markets(
        self,
        status: Optional[str] = "open",
        limit: int = 100,
        offset: int = 0,
        max_pages: Optional[int] = None,
        condition_id: Optional[str] = None,
        slug: Optional[str] = None,
        order: Optional[str] = None,
        ascending: Optional[bool] = None,
    ) -> list[Market]:
        """Retrieve markets via the Polymarket Gamma API with automatic pagination.

        Market discovery returns title/question, condition_id, tokens (YES/NO token asset IDs),
        volume, prices, and status.

        Args:
            status: Filter markets by status ('open', 'active', 'closed', or None for all).
            limit: Number of markets per page (max 100).
            offset: Starting offset for pagination.
            max_pages: Optional maximum number of pages to fetch. If None, fetches all pages.
            condition_id: Filter by specific condition ID.
            slug: Filter by market slug.
            order: Order by field (e.g. 'volume24hr', 'volume', 'createdAt').
            ascending: Sort direction ascending/descending.

        Returns:
            list[Market]: List of parsed Market model instances.
        """
        markets: list[Market] = []
        current_offset = offset
        page = 0

        while True:
            params: dict[str, Any] = {
                "limit": limit,
                "offset": current_offset,
            }

            if status:
                status_lower = status.lower()
                if status_lower in ("open", "active"):
                    params["closed"] = "false"
                    params["active"] = "true"
                elif status_lower == "closed":
                    params["closed"] = "true"

            if condition_id:
                params["condition_id"] = condition_id
            if slug:
                params["slug"] = slug
            if order:
                params["order"] = order
            if ascending is not None:
                params["ascending"] = "true" if ascending else "false"

            url = f"{self.gamma_url}/markets"
            response = await self._request(method="GET", url=url, params=params)
            data = response.json()

            if not isinstance(data, list):
                break

            for item in data:
                markets.append(Market.from_api(item))

            page += 1
            current_offset += limit

            # Stop if last page returned fewer items than limit or max_pages reached
            if len(data) < limit or (max_pages is not None and page >= max_pages):
                break

        return markets

    async def get_market(self, condition_id: str) -> Optional[Market]:
        """Retrieve a single market by condition ID via the Gamma API.

        Args:
            condition_id: Unique condition ID / address.

        Returns:
            Optional[Market]: Parsed Market instance, or None if not found.
        """
        url = f"{self.gamma_url}/markets"
        response = await self._request(method="GET", url=url, params={"condition_id": condition_id})
        data = response.json()
        if isinstance(data, list) and data:
            return Market.from_api(data[0])
        elif isinstance(data, dict) and "conditionId" in data:
            return Market.from_api(data)
        return None

    async def get_orderbook(self, token_id: str) -> Orderbook:
        """Retrieve the orderbook for a single Polymarket token asset ID via the CLOB API.

        Calls GET {clob_url}/book?token_id={token_id} (public endpoint, no auth required).

        Args:
            token_id: Polymarket CLOB token asset ID.

        Returns:
            Orderbook: Parsed Orderbook instance with bids, asks, and derived NO prices.
        """
        url = f"{self.clob_url}/book"
        response = await self._request(method="GET", url=url, params={"token_id": token_id})
        data = response.json()
        return Orderbook.from_api(data, token_id=token_id)

    async def get_orderbooks(self, token_ids: list[str]) -> dict[str, Orderbook]:
        """Retrieve orderbooks for multiple token IDs using Polymarket CLOB's batch endpoint.

        Batches requests using POST /books up to 100 token IDs per call.

        Args:
            token_ids: List of CLOB token asset IDs.

        Returns:
            dict[str, Orderbook]: Mapping of token_id to parsed Orderbook object.
        """
        if not token_ids:
            return {}

        result: dict[str, Orderbook] = {}
        chunk_size = 100

        for i in range(0, len(token_ids), chunk_size):
            chunk = token_ids[i : i + chunk_size]
            payload = [{"token_id": t} for t in chunk]
            url = f"{self.clob_url}/books"

            try:
                response = await self._request(method="POST", url=url, json=payload)
                data = response.json()

                if isinstance(data, list):
                    for item in data:
                        t_id = str(item.get("asset_id") or item.get("token_id") or "")
                        if t_id:
                            result[t_id] = Orderbook.from_api(item, token_id=t_id)
            except Exception as e:
                logger.warning(
                    "Batch orderbook fetch failed (%s). Falling back to single-book requests.",
                    e,
                )
                # Fallback to sequential/individual requests if batch endpoint errors
                for t in chunk:
                    try:
                        result[t] = await self.get_orderbook(t)
                    except Exception as err:
                        logger.warning("Failed to fetch orderbook for token %s: %s", t, err)

        return result

    async def close(self) -> None:
        """Close underlying HTTP client session."""
        await self._client.aclose()

    async def __aenter__(self) -> PolymarketClient:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
