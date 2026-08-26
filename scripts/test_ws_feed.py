#!/usr/bin/env python3
"""Manual verification script for Kalshi WebSocket client.

Subscribes to 5 active markets on Kalshi, maintains in-memory orderbooks,
and streams live ticker and orderbook updates for 30 seconds.

Usage:
    python scripts/test_ws_feed.py
    python scripts/test_ws_feed.py --duration 30 --tickers "KXHIGHNY-26AUG27-T80,KXRAINSHARD2-26AUG27-BOS"
    python scripts/test_ws_feed.py --sandbox
    python scripts/test_ws_feed.py --prod
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from core.config_loader import load_config
from kalshi_client.auth import KalshiAuth
from kalshi_client.models import Orderbook
from kalshi_client.rest import KalshiClient
from kalshi_client.ws import KalshiWSClient, WSEvent


async def discover_active_tickers(is_sandbox: bool, count: int = 5) -> list[str]:
    """Fetch active/open market tickers using Kalshi REST client."""
    print(f"Discovering up to {count} open markets via REST client...")
    config = load_config()
    rest_client = KalshiClient.from_config(config, is_sandbox=is_sandbox)

    async with rest_client:
        try:
            markets = await rest_client.list_markets(status="open", limit=30, max_pages=2)
        except Exception as e:
            print(f"Warning: Failed to fetch markets via status='open': {e}")
            markets = []

        if not markets:
            try:
                markets = await rest_client.list_markets(status="", limit=30, max_pages=2)
            except Exception as e:
                print(f"Warning: Failed to fetch markets with status='': {e}")
                markets = []

    # Sort by volume descending to get active markets
    markets_sorted = sorted(markets, key=lambda m: m.volume or 0, reverse=True)
    tickers = [m.ticker for m in markets_sorted if m.ticker][:count]

    # Fallback to known default tickers if discovery returns fewer than desired
    if len(tickers) < count:
        fallback_tickers = [
            "KXRAINSHARD2-26AUG27-BOS",
            "KXRAINSHARD2-26AUG27-OKC",
            "KXRAINSHARD2-26AUG27-SEA",
            "KXRAINSHARD2-26AUG27-NOLA",
            "KXMAXSHARDINGTEST-26AUG2818-T69599.99",
        ]
        for t in fallback_tickers:
            if t not in tickers and len(tickers) < count:
                tickers.append(t)

    return tickers


def format_price(cents: int | None) -> str:
    """Format price in cents nicely."""
    return f"{cents}¢" if cents is not None else "--"


async def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Stream Kalshi WebSocket real-time orderbooks and tickers.")
    parser.add_argument("--duration", type=float, default=30.0, help="Duration to stream updates in seconds (default: 30)")
    parser.add_argument("--tickers", type=str, default="", help="Comma-separated list of market tickers to subscribe to")
    parser.add_argument("--sandbox", action="store_true", help="Force sandbox environment")
    parser.add_argument("--prod", action="store_true", help="Force production environment")
    args = parser.parse_args()

    config = load_config()
    if args.prod:
        is_sandbox = False
    elif args.sandbox:
        is_sandbox = True
    else:
        is_sandbox = config.mode.trading_mode.value.lower() in ("sandbox", "dry_run")

    print("=" * 95)
    print("  Kalshi Real-Time WebSocket Feed Verification")
    print("=" * 95)
    print(f"Environment: {'SANDBOX' if is_sandbox else 'PRODUCTION'}")
    print(f"Duration:    {args.duration} seconds\n")

    # 1. Determine target tickers
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = await discover_active_tickers(is_sandbox=is_sandbox, count=5)

    print(f"Target Tickers ({len(tickers)}):")
    for idx, t in enumerate(tickers, 1):
        print(f"  {idx}. {t}")
    print()

    # 2. Auth initialization (if available)
    auth: KalshiAuth | None = None
    try:
        auth = KalshiAuth.from_config(config, is_sandbox=is_sandbox)
        print(f"Authentication: Configured for key '{auth.key_id[:8]}...'")
    except Exception as e:
        print(f"Authentication: None configured ({e}). Proceeding unauthenticated.")

    # 3. Create WebSocket client
    ws_client = KalshiWSClient(
        tickers=tickers,
        channels=["orderbook_delta", "ticker"],
        auth=auth,
        is_sandbox=is_sandbox,
        auto_reconnect=True,
    )

    update_counts: dict[str, int] = defaultdict(int)
    event_counts: dict[str, int] = defaultdict(int)
    start_time = time.time()

    # 4. Attach callbacks
    @ws_client.on_update
    def handle_event(event: WSEvent) -> None:
        event_counts[event.event_type] += 1
        if event.ticker:
            update_counts[event.ticker] += 1

        now_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]

        if event.event_type in ("snapshot", "delta") and isinstance(event.data, Orderbook):
            ob = event.data
            y_bid = format_price(ob.best_yes_bid)
            y_ask = format_price(ob.best_yes_ask)
            n_bid = format_price(ob.best_no_bid)
            n_ask = format_price(ob.best_no_ask)
            spread = format_price(ob.yes_spread)
            print(
                f"[{now_str}] [ORDERBOOK {event.event_type.upper():<8}] "
                f"{ob.ticker:<34} "
                f"YES: {y_bid:>4} / {y_ask:<4} (spread {spread:>3}) | "
                f"NO: {n_bid:>4} / {n_ask:<4}"
            )

        elif event.event_type == "ticker" and isinstance(event.data, dict):
            t_data = event.data
            p_dol = t_data.get("price_dollars") or "--"
            y_bid_dol = t_data.get("yes_bid_dollars") or "--"
            y_ask_dol = t_data.get("yes_ask_dollars") or "--"
            vol = t_data.get("volume_fp") or t_data.get("volume") or "0"
            print(
                f"[{now_str}] [TICKER FEED      ] "
                f"{event.ticker:<34} "
                f"Price: ${p_dol:<6} | YES Bid/Ask: ${y_bid_dol} / ${y_ask_dol} | Vol: {vol}"
            )

        elif event.event_type == "error":
            print(f"[{now_str}] [ERROR            ] {event.data}")

    # 5. Start streaming
    print(f"Connecting to {ws_client.ws_url}...")
    task = ws_client.start()

    try:
        connected = await ws_client.wait_until_connected(timeout=10.0)
        if not connected:
            print("[WARN] Connection taking longer than expected...")
        else:
            print("Connected and subscribed! Streaming live updates...\n" + "-" * 95)

        # Stream for requested duration
        elapsed = 0.0
        while elapsed < args.duration:
            await asyncio.sleep(min(1.0, args.duration - elapsed))
            elapsed = time.time() - start_time

    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nStreaming interrupted by user.")
    finally:
        print("\nStopping WebSocket client...")
        await ws_client.stop()

    # 6. Final Summary Report
    total_events = sum(event_counts.values())
    total_ticker_updates = sum(update_counts.values())

    print("\n" + "=" * 95)
    print("  Streaming Summary Report")
    print("=" * 95)
    print(f"Total Run Time:   {time.time() - start_time:.2f} seconds")
    print(f"Total WS Events:  {total_events}")
    print(f"Event Breakdown:  {dict(event_counts)}")
    print("\nUpdates Per Ticker:")
    for t in tickers:
        count = update_counts.get(t, 0)
        ob = ws_client.get_orderbook(t)
        best_yes = f"{ob.best_yes_bid}¢" if ob and ob.best_yes_bid is not None else "--"
        best_no = f"{ob.best_no_bid}¢" if ob and ob.best_no_bid is not None else "--"
        print(f"  - {t:<35}: {count:>4} updates (Final Best YES: {best_yes:>4}, Best NO: {best_no:>4})")
    print("=" * 95)

    if total_events > 0:
        print("\n[SUCCESS] Live WebSocket streaming verification passed.")
        return 0
    else:
        print("\n[WARN] No WebSocket events received during the test period.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
