"""Unit tests for Kalshi RSA-PSS authentication and request signing.

================================================================================
HOW TO MANUALLY VERIFY AGAINST KALSHI SANDBOX:
================================================================================
1. Create a Kalshi Demo account at https://demo.kalshi.co/
2. Go to Account Settings -> API Keys and generate an RSA Key Pair:
   - Save your API Key ID (e.g., 'your-kalshi-sandbox-key-id-uuid').
   - Download/save the private key PEM file (e.g., '/path/to/kalshi_sandbox_private_key.pem').
3. Populate your `.env` file with your sandbox credentials:
   ```bash
   KALSHI_SANDBOX_KEY_ID=your-kalshi-sandbox-key-id-uuid
   KALSHI_SANDBOX_PRIVATE_KEY_PATH=/path/to/kalshi_sandbox_private_key.pem
   ```
4. Run the sandbox connection smoke test script:
   ```bash
   python3 scripts/test_sandbox_connection.py
   ```
5. Expected result:
   The script performs an authenticated GET request to `/portfolio/balance` on the Kalshi
   Sandbox API (`https://demo-api.kalshi.co/trade-api/v2/portfolio/balance`) and outputs
   the account balance JSON payload (HTTP 200).
================================================================================
"""

import base64
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from core.config_loader import AppConfig, KalshiConfig, ModeConfig, PolymarketConfig, RiskConfig, TradingConfig, MatchingConfig, TradingMode
from kalshi_client.auth import KalshiAuth, load_private_key, sign_kalshi_request
from kalshi_client.rest import KalshiClient


@pytest.fixture(scope="module")
def rsa_key_pair() -> tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]:
    """Generate an ephemeral RSA key pair for testing."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    public_key = private_key.public_key()
    return private_key, public_key


@pytest.fixture(scope="module")
def rsa_private_pem(rsa_key_pair: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]) -> bytes:
    """Export the private key to PEM format bytes."""
    private_key, _ = rsa_key_pair
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def test_sign_kalshi_request_header_structure_and_cryptographic_validity(
    rsa_key_pair: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey],
) -> None:
    """Test that sign_kalshi_request creates valid headers and a cryptographically verifiable RSA-PSS signature."""
    private_key, public_key = rsa_key_pair
    key_id = "test-key-id-12345"
    method = "GET"
    path = "/trade-api/v2/portfolio/balance"
    fixed_timestamp_ms = 1719998887776

    headers = sign_kalshi_request(
        private_key=private_key,
        key_id=key_id,
        method=method,
        path=path,
        timestamp_ms=fixed_timestamp_ms,
    )

    # 1. Verify header keys & values
    assert headers["KALSHI-ACCESS-KEY"] == key_id
    assert headers["KALSHI-ACCESS-TIMESTAMP"] == str(fixed_timestamp_ms)
    assert headers["Content-Type"] == "application/json"
    assert "KALSHI-ACCESS-SIGNATURE" in headers

    # 2. Cryptographically verify signature using public key
    signature_raw = base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"])
    expected_message = f"{fixed_timestamp_ms}{method.upper()}{path}".encode("utf-8")

    # This will raise an InvalidSignature exception if signature does not match
    public_key.verify(
        signature_raw,
        expected_message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )


def test_sign_kalshi_request_strips_query_parameters(
    rsa_key_pair: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey],
) -> None:
    """Verify that query parameters are excluded from the signed message."""
    private_key, public_key = rsa_key_pair
    key_id = "test-key-id"
    method = "get"
    path_with_query = "/trade-api/v2/portfolio/orders?limit=100&status=open"
    fixed_timestamp_ms = 1700000000000

    headers = sign_kalshi_request(
        private_key=private_key,
        key_id=key_id,
        method=method,
        path=path_with_query,
        timestamp_ms=fixed_timestamp_ms,
    )

    # Expected signed message should NOT have ?limit=100...
    expected_signed_message = f"{fixed_timestamp_ms}GET/trade-api/v2/portfolio/orders".encode("utf-8")
    signature_raw = base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"])

    public_key.verify(
        signature_raw,
        expected_signed_message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )


def test_sign_kalshi_request_deterministic_input_message() -> None:
    """Test that sign_kalshi_request constructs the exact deterministic message input to private_key.sign()."""
    mock_key = MagicMock(spec=rsa.RSAPrivateKey)
    mock_key.sign.return_value = b"mock-signature-bytes"

    key_id = "key-abc"
    method = "POST"
    path = "/trade-api/v2/portfolio/orders"
    timestamp_ms = 1700001234567

    headers = sign_kalshi_request(
        private_key=mock_key,
        key_id=key_id,
        method=method,
        path=path,
        timestamp_ms=timestamp_ms,
    )

    expected_message = b"1700001234567POST/trade-api/v2/portfolio/orders"
    mock_key.sign.assert_called_once()
    called_message = mock_key.sign.call_args[0][0]
    assert called_message == expected_message

    expected_sig_b64 = base64.b64encode(b"mock-signature-bytes").decode("utf-8")
    assert headers["KALSHI-ACCESS-SIGNATURE"] == expected_sig_b64


def test_load_private_key_formats(rsa_key_pair: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey], tmp_path: Path) -> None:
    """Test loading private key from string, bytes, file path, or direct instance."""
    private_key, _ = rsa_key_pair
    pem_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pem_str = pem_bytes.decode("utf-8")

    # 1. From RSAPrivateKey directly
    assert load_private_key(private_key) is private_key

    # 2. From bytes
    key_from_bytes = load_private_key(pem_bytes)
    assert isinstance(key_from_bytes, rsa.RSAPrivateKey)

    # 3. From PEM string
    key_from_str = load_private_key(pem_str)
    assert isinstance(key_from_str, rsa.RSAPrivateKey)

    # 4. From file path (both .pem and .txt extension)
    pem_file = tmp_path / "test_key.pem"
    pem_file.write_bytes(pem_bytes)
    key_from_pem_file = load_private_key(pem_file)
    assert isinstance(key_from_pem_file, rsa.RSAPrivateKey)

    txt_file = tmp_path / "test_key.txt"
    txt_file.write_bytes(pem_bytes)
    key_from_txt_file = load_private_key(str(txt_file))
    assert isinstance(key_from_txt_file, rsa.RSAPrivateKey)


def test_load_private_key_errors(tmp_path: Path) -> None:
    """Test error handling when loading invalid or non-existent keys."""
    # Non-existent file
    with pytest.raises(FileNotFoundError):
        load_private_key(tmp_path / "does_not_exist.pem")

    # Invalid PEM content
    with pytest.raises(ValueError, match="Failed to load PEM"):
        load_private_key("-----BEGIN RSA PRIVATE KEY-----\ninvalid_base64_data\n-----END RSA PRIVATE KEY-----")


def test_kalshi_auth_from_config(monkeypatch: pytest.MonkeyPatch, rsa_private_pem: bytes, tmp_path: Path) -> None:
    """Test KalshiAuth.from_config switching between sandbox and production."""
    prod_key_file = tmp_path / "prod_key.pem"
    prod_key_file.write_bytes(rsa_private_pem)

    sandbox_key_file = tmp_path / "sandbox_key.pem"
    sandbox_key_file.write_bytes(rsa_private_pem)

    monkeypatch.setenv("KALSHI_KEY_ID", "prod-key-123")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", str(prod_key_file))
    monkeypatch.setenv("KALSHI_SANDBOX_KEY_ID", "sandbox-key-456")
    monkeypatch.setenv("KALSHI_SANDBOX_PRIVATE_KEY_PATH", str(sandbox_key_file))

    app_cfg_sandbox = AppConfig(
        mode=ModeConfig(trading_mode=TradingMode.SANDBOX),
        kalshi=KalshiConfig(
            key_id="sandbox-key-456",
            private_key_path=str(sandbox_key_file),
        ),
        polymarket=PolymarketConfig(),
        trading=TradingConfig(),
        risk=RiskConfig(),
        matching=MatchingConfig(),
    )

    auth_sandbox = KalshiAuth.from_config(app_cfg_sandbox)
    assert auth_sandbox.is_sandbox is True
    assert auth_sandbox.key_id == "sandbox-key-456"

    app_cfg_prod = AppConfig(
        mode=ModeConfig(trading_mode=TradingMode.LIVE),
        kalshi=KalshiConfig(
            key_id="prod-key-123",
            private_key_path=str(prod_key_file),
        ),
        polymarket=PolymarketConfig(),
        trading=TradingConfig(),
        risk=RiskConfig(),
        matching=MatchingConfig(),
    )

    auth_prod = KalshiAuth.from_config(app_cfg_prod)
    assert auth_prod.is_sandbox is False
    assert auth_prod.key_id == "prod-key-123"


def test_kalshi_client_resolve_paths() -> None:
    """Test path resolution in KalshiClient for both relative and full paths."""
    client = KalshiClient(
        base_url="https://demo-api.kalshi.co/trade-api/v2",
        is_sandbox=True,
    )

    # Relative endpoint
    url, sign_path = client._resolve_paths("/portfolio/balance")
    assert url == "https://demo-api.kalshi.co/trade-api/v2/portfolio/balance"
    assert sign_path == "/trade-api/v2/portfolio/balance"

    # Endpoint that already contains /trade-api/v2
    url2, sign_path2 = client._resolve_paths("/trade-api/v2/portfolio/balance")
    assert url2 == "https://demo-api.kalshi.co/trade-api/v2/portfolio/balance"
    assert sign_path2 == "/trade-api/v2/portfolio/balance"
