#!/usr/bin/env python3
"""Manual smoke test script to verify Kalshi Sandbox API connectivity and RSA-PSS authentication.

Usage:
    python3 scripts/test_sandbox_connection.py
"""

import asyncio
import json
import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from core.config_loader import load_config
from kalshi_client.auth import KalshiAuth
from kalshi_client.rest import KalshiAPIError, KalshiClient


async def main() -> int:
    load_dotenv()
    print("=" * 60)
    print("  Kalshi Sandbox API Authentication Smoke Test")
    print("=" * 60)

    # 1. Check environment variables
    key_id = os.getenv("KALSHI_SANDBOX_KEY_ID") or os.getenv("KALSHI_KEY_ID")
    key_path = os.getenv("KALSHI_SANDBOX_PRIVATE_KEY_PATH") or os.getenv("KALSHI_PRIVATE_KEY_PATH")

    print(f"Key ID:           {key_id if key_id else '<NOT SET>'}")
    print(f"Private Key Path: {key_path if key_path else '<NOT SET>'}")

    if not key_id or not key_path:
        print("\n[ERROR] Missing sandbox credentials!")
        print("Please ensure KALSHI_SANDBOX_KEY_ID and KALSHI_SANDBOX_PRIVATE_KEY_PATH are set in your .env file.")
        return 1

    expanded_path = Path(key_path).expanduser().resolve()
    if not expanded_path.exists():
        print(f"\n[ERROR] Private key file does not exist at: {expanded_path}")
        return 1

    print(f"Private Key Exists: {expanded_path} (size: {expanded_path.stat().st_size} bytes)")

    # 2. Initialize KalshiClient for sandbox
    try:
        config = load_config()
        # Ensure we connect to sandbox
        client = KalshiClient.from_config(config, is_sandbox=True)
    except Exception as e:
        print(f"\n[INFO] Loading auth directly (config fallback: {e})")
        auth = KalshiAuth(key_id=key_id, private_key=expanded_path, is_sandbox=True)
        client = KalshiClient(auth=auth, is_sandbox=True)

    print(f"Target Base URL:  {client.base_url}")
    print("\nSending GET /portfolio/balance request with RSA-PSS signature...")

    try:
        async with client:
            balance_data = await client.get_balance()

        print("\n[SUCCESS] HTTP 200 OK — Authentication Verified!")
        print("-" * 60)
        print("Portfolio Balance Response:")
        print(json.dumps(balance_data, indent=2))
        print("-" * 60)

        # Kalshi balance is in cents
        if "balance" in balance_data:
            balance_cents = balance_data["balance"]
            balance_dollars = balance_cents / 100.0
            print(f"Available Balance: ${balance_dollars:,.2f} USD ({balance_cents} cents)")

        return 0

    except KalshiAPIError as e:
        print(f"\n[FAILED] Kalshi API Error {e.status_code}: {e.message}")
        if e.response_body:
            print(f"Response Body: {e.response_body}")
        return 1
    except Exception as e:
        print(f"\n[ERROR] Request failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
