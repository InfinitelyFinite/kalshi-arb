"""Kalshi RSA-PSS request signing and authentication module.

Implements RSA-PSS request signing according to the Kalshi Trade API v2 specification:
- Signature message: timestamp_ms + method (UPPERCASE) + path (without query parameters)
- Algorithm: RSA-PSS with SHA-256 and MGF1(SHA-256), salt_length = DIGEST_LENGTH
- Headers produced:
    - KALSHI-ACCESS-KEY: API Key ID
    - KALSHI-ACCESS-TIMESTAMP: Current Unix epoch timestamp in milliseconds (string)
    - KALSHI-ACCESS-SIGNATURE: Base64-encoded RSA-PSS SHA-256 signature
"""

from __future__ import annotations

import base64
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

if TYPE_CHECKING:
    from core.config_loader import AppConfig, KalshiConfig


def load_private_key(
    key_source: Union[str, Path, bytes, rsa.RSAPrivateKey],
    password: Optional[bytes] = None,
) -> rsa.RSAPrivateKey:
    """Load an RSA private key from a file path, PEM string, PEM bytes, or return if already RSAPrivateKey.

    Args:
        key_source: File path (str/Path), PEM content (str/bytes), or an existing RSAPrivateKey.
        password: Optional passphrase if the private key is encrypted.

    Returns:
        rsa.RSAPrivateKey: The loaded RSA private key object.

    Raises:
        FileNotFoundError: If a file path was provided but the file does not exist.
        ValueError: If the key format is invalid or not an RSA private key.
    """
    if isinstance(key_source, rsa.RSAPrivateKey):
        return key_source

    key_bytes: bytes

    if isinstance(key_source, bytes):
        key_bytes = key_source
    elif isinstance(key_source, str) and "-----BEGIN" in key_source:
        key_bytes = key_source.encode("utf-8")
    elif isinstance(key_source, (str, Path)):
        try:
            path = Path(key_source).expanduser().resolve()
            if path.is_file():
                with open(path, "rb") as f:
                    key_bytes = f.read()
            else:
                raise FileNotFoundError(f"Private key file not found: {path}")
        except (OSError, ValueError) as e:
            if isinstance(e, FileNotFoundError):
                raise
            # If path parsing/stat failed (e.g. filename too long), try reading as raw PEM string
            if isinstance(key_source, str) and "-----BEGIN" in key_source:
                key_bytes = key_source.encode("utf-8")
            else:
                raise ValueError(f"Invalid private key path or format: {e}") from e
    else:
        raise TypeError(f"Unsupported key_source type: {type(key_source)}")

    try:
        private_key = serialization.load_pem_private_key(key_bytes, password=password)
    except Exception as e:
        raise ValueError(f"Failed to load PEM RSA private key: {e}") from e

    if not isinstance(private_key, rsa.RSAPrivateKey):
        raise ValueError(f"Loaded key is of type {type(private_key).__name__}, expected RSAPrivateKey")

    return private_key


def sign_kalshi_request(
    private_key: rsa.RSAPrivateKey,
    key_id: str,
    method: str,
    path: str,
    timestamp_ms: Optional[int] = None,
) -> dict[str, str]:
    """Generate Kalshi Trade API v2 authentication headers using RSA-PSS SHA-256 signature.

    The signed payload is the concatenation of:
        timestamp (milliseconds as string) + HTTP method (uppercase) + request path (without query params)

    Args:
        private_key: The RSA private key for signing.
        key_id: The Kalshi API key ID.
        method: HTTP method (e.g. 'GET', 'POST', 'DELETE').
        path: The full API request path starting from API root (e.g. '/trade-api/v2/portfolio/balance').
        timestamp_ms: Optional explicit timestamp in ms. If omitted, current system time is used.

    Returns:
        dict[str, str]: Dictionary containing:
            - KALSHI-ACCESS-KEY
            - KALSHI-ACCESS-TIMESTAMP
            - KALSHI-ACCESS-SIGNATURE
            - Content-Type
    """
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)

    timestamp_str = str(timestamp_ms)
    method_str = method.upper()

    # Strip query string and ensure leading slash
    path_clean = path.split("?")[0]
    if not path_clean.startswith("/"):
        path_clean = "/" + path_clean

    # Message string to sign
    message_str = f"{timestamp_str}{method_str}{path_clean}"
    message_bytes = message_str.encode("utf-8")

    # RSA-PSS signature with SHA256 and MGF1(SHA256), salt length = DIGEST_LENGTH (32 bytes for SHA256)
    signature_bytes = private_key.sign(
        message_bytes,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )

    signature_b64 = base64.b64encode(signature_bytes).decode("utf-8")

    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_str,
        "KALSHI-ACCESS-SIGNATURE": signature_b64,
        "Content-Type": "application/json",
    }


class KalshiAuth:
    """Kalshi authentication helper supporting sandbox and production key pairs."""

    def __init__(
        self,
        key_id: str,
        private_key: Union[str, Path, bytes, rsa.RSAPrivateKey],
        is_sandbox: bool = False,
    ) -> None:
        if not key_id:
            raise ValueError("key_id must not be empty")
        self.key_id = key_id
        self.is_sandbox = is_sandbox
        self.private_key = load_private_key(private_key)

    @classmethod
    def from_config(
        cls,
        config: Union[AppConfig, KalshiConfig],
        is_sandbox: Optional[bool] = None,
    ) -> KalshiAuth:
        """Create KalshiAuth instance from configuration.

        Args:
            config: AppConfig or KalshiConfig instance.
            is_sandbox: If specified, forces sandbox mode (True) or production mode (False).
                        If None, inferred from config.mode.trading_mode if AppConfig.

        Returns:
            KalshiAuth instance.
        """
        # Determine sandbox mode
        if is_sandbox is None:
            if hasattr(config, "mode"):
                mode_val = config.mode.trading_mode
                mode_str = mode_val.value if hasattr(mode_val, "value") else str(mode_val)
                is_sandbox = mode_str.lower() in ("sandbox", "dry_run")
            else:
                is_sandbox = False

        kalshi_cfg = config.kalshi if hasattr(config, "kalshi") else config

        key_id: Optional[str] = None
        key_path: Optional[str] = None

        if is_sandbox:
            key_id = os.getenv("KALSHI_SANDBOX_KEY_ID") or kalshi_cfg.key_id
            key_path = os.getenv("KALSHI_SANDBOX_PRIVATE_KEY_PATH") or kalshi_cfg.private_key_path
        else:
            key_id = kalshi_cfg.key_id or os.getenv("KALSHI_KEY_ID")
            key_path = kalshi_cfg.private_key_path or os.getenv("KALSHI_PRIVATE_KEY_PATH")

        if not key_id:
            env_name = "KALSHI_SANDBOX_KEY_ID" if is_sandbox else "KALSHI_KEY_ID"
            raise ValueError(f"Missing Kalshi Key ID for {'sandbox' if is_sandbox else 'production'} mode (check {env_name})")

        if not key_path:
            env_name = "KALSHI_SANDBOX_PRIVATE_KEY_PATH" if is_sandbox else "KALSHI_PRIVATE_KEY_PATH"
            raise ValueError(f"Missing Kalshi Private Key Path for {'sandbox' if is_sandbox else 'production'} mode (check {env_name})")

        return cls(key_id=key_id, private_key=key_path, is_sandbox=is_sandbox)

    def get_auth_headers(
        self,
        method: str,
        path: str,
        timestamp_ms: Optional[int] = None,
    ) -> dict[str, str]:
        """Generate authentication headers for the given method and path."""
        return sign_kalshi_request(
            private_key=self.private_key,
            key_id=self.key_id,
            method=method,
            path=path,
            timestamp_ms=timestamp_ms,
        )
