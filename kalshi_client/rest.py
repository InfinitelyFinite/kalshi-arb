"""Kalshi asynchronous REST client using httpx."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional, Union
from urllib.parse import urlparse

import httpx

from kalshi_client.auth import KalshiAuth

if TYPE_CHECKING:
    from core.config_loader import AppConfig, KalshiConfig

logger = logging.getLogger(__name__)


class KalshiAPIError(Exception):
    """Custom exception raised when a Kalshi API request fails."""

    def __init__(self, status_code: int, message: str, response_body: Optional[str] = None) -> None:
        super().__init__(f"Kalshi API Error {status_code}: {message}")
        self.status_code = status_code
        self.message = message
        self.response_body = response_body


class KalshiClient:
    """Thin asynchronous client for interacting with Kalshi Trade API v2."""

    DEFAULT_PROD_URL = "https://api.elections.kalshi.com/trade-api/v2"
    DEFAULT_SANDBOX_URL = "https://demo-api.kalshi.co/trade-api/v2"

    def __init__(
        self,
        auth: Optional[KalshiAuth] = None,
        base_url: Optional[str] = None,
        is_sandbox: bool = False,
        timeout: float = 10.0,
    ) -> None:
        self.auth = auth
        self.is_sandbox = is_sandbox

        if base_url:
            self.base_url = base_url.rstrip("/")
        else:
            self.base_url = self.DEFAULT_SANDBOX_URL if is_sandbox else self.DEFAULT_PROD_URL

        # Extract base path prefix (e.g., '/trade-api/v2')
        self._base_path = urlparse(self.base_url).path.rstrip("/")

        self._client = httpx.AsyncClient(timeout=timeout)

    @classmethod
    def from_config(
        cls,
        config: Union[AppConfig, KalshiConfig],
        is_sandbox: Optional[bool] = None,
        timeout: float = 10.0,
    ) -> KalshiClient:
        """Initialize KalshiClient from configuration.

        Args:
            config: AppConfig or KalshiConfig instance.
            is_sandbox: Explicitly specify sandbox mode, or infer from config.
            timeout: Request timeout in seconds.
        """
        if is_sandbox is None:
            if hasattr(config, "mode"):
                mode_val = config.mode.trading_mode
                mode_str = mode_val.value if hasattr(mode_val, "value") else str(mode_val)
                is_sandbox = mode_str.lower() in ("sandbox", "dry_run")
            else:
                is_sandbox = False

        kalshi_cfg = config.kalshi if hasattr(config, "kalshi") else config
        base_url = kalshi_cfg.base_url_sandbox if is_sandbox else kalshi_cfg.base_url_prod

        auth = KalshiAuth.from_config(config, is_sandbox=is_sandbox)
        return cls(auth=auth, base_url=base_url, is_sandbox=is_sandbox, timeout=timeout)

    def _resolve_paths(self, endpoint_path: str) -> tuple[str, str]:
        """Resolve full target URL and the API root path used for signing.

        Returns:
            tuple[str, str]: (full_url, full_api_path_for_signing)
        """
        clean_endpoint = endpoint_path.strip()
        if not clean_endpoint.startswith("/"):
            clean_endpoint = "/" + clean_endpoint

        # If endpoint already has the base path prefix (e.g. /trade-api/v2/portfolio/balance)
        if self._base_path and clean_endpoint.startswith(self._base_path):
            full_api_path = clean_endpoint
            # Strip base path to avoid duplicate when appending to base_url
            relative_endpoint = clean_endpoint[len(self._base_path) :]
            full_url = f"{self.base_url}{relative_endpoint}"
        else:
            full_api_path = f"{self._base_path}{clean_endpoint}" if self._base_path else clean_endpoint
            full_url = f"{self.base_url}{clean_endpoint}"

        return full_url, full_api_path

    async def _signed_request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> httpx.Response:
        """Perform an authenticated HTTP request signed with Kalshi RSA-PSS headers.

        Args:
            method: HTTP method ('GET', 'POST', etc.).
            path: Target endpoint path (e.g. '/portfolio/balance').
            params: Optional query parameters.
            json: Optional JSON body payload.
            headers: Optional additional headers.

        Returns:
            httpx.Response: Response object.

        Raises:
            KalshiAPIError: If the server returns 4xx or 5xx status code.
        """
        full_url, signing_path = self._resolve_paths(path)

        request_headers: dict[str, str] = {}
        if self.auth:
            request_headers.update(self.auth.get_auth_headers(method=method, path=signing_path))

        if headers:
            request_headers.update(headers)

        response = await self._client.request(
            method=method.upper(),
            url=full_url,
            params=params,
            json=json,
            headers=request_headers,
        )

        if response.is_error:
            try:
                err_data = response.json()
                err_msg = err_data.get("message") or err_data.get("error") or response.text
            except Exception:
                err_msg = response.text

            logger.error(
                "Kalshi API request %s %s failed (%s): %s",
                method.upper(),
                full_url,
                response.status_code,
                err_msg,
            )
            raise KalshiAPIError(
                status_code=response.status_code,
                message=err_msg,
                response_body=response.text,
            )

        return response

    async def get_balance(self) -> dict[str, Any]:
        """Fetch the portfolio balance of the authenticated account.

        Calls GET /trade-api/v2/portfolio/balance.

        Returns:
            dict[str, Any]: Kalshi balance response, e.g. {"balance": 10000} (in cents).
        """
        response = await self._signed_request(method="GET", path="/portfolio/balance")
        return response.json()

    async def close(self) -> None:
        """Close underlying HTTP client session."""
        await self._client.aclose()

    async def __aenter__(self) -> KalshiClient:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
