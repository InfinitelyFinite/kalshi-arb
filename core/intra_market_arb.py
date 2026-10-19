"""Intra-market arbitrage engine.

Scans prediction markets on a single exchange to detect structural mispricings where:
    BestAsk(YES) + BestAsk(NO) < 100 - Fees

Because binary outcome contracts settle at exactly 100 cents ($1.00), buying both sides
locks in an immediate risk-free settlement spread.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, Optional, Sequence, Union

from core.config_loader import AppConfig, load_config
from core.models import Market, Opportunity

logger = logging.getLogger(__name__)


def calculate_estimated_fees(
    gross_cost_cents: float,
    config: AppConfig,
    platform: str = "kalshi",
) -> float:
    """Calculate estimated taker fees in cents for purchasing YES and NO contracts.

    Fee is calculated from `config.trading.taker_fee_bps` (basis points):
        fee_rate = taker_fee_bps / 10,000
        fees_cents = gross_cost_cents * fee_rate

    Args:
        gross_cost_cents: Total cost in cents to purchase 1 YES and 1 NO contract.
        config: Application configuration containing trading fee parameters.
        platform: Exchange platform ('kalshi' or 'polymarket').

    Returns:
        Estimated fee in cents.
    """
    taker_fee_bps = getattr(config.trading, "taker_fee_bps", 0.0) or 0.0
    if taker_fee_bps <= 0.0:
        return 0.0
    fee_rate = taker_fee_bps / 10000.0
    return gross_cost_cents * fee_rate


def _extract_market_info(market: Any) -> tuple[str, str, Optional[int], Optional[int], dict[str, Any], datetime]:
    """Extract platform, ticker, best_yes_ask, best_no_ask, snapshot dict, and timestamp.

    Supports core.models.Market, kalshi_client.models.Market, polymarket_client.models.Market,
    core.data_feed.MarketState, Orderbook models, and raw dictionaries.
    """
    # 1. Ticker
    ticker = (
        getattr(market, "ticker", None)
        or getattr(market, "condition_id", None)
        or getattr(market, "token_id", None)
        or (market.get("ticker") if isinstance(market, dict) else None)
        or (market.get("condition_id") if isinstance(market, dict) else None)
        or ""
    )

    # 2. Platform
    platform = getattr(market, "platform", None) or (market.get("platform") if isinstance(market, dict) else None)
    if not platform:
        if hasattr(market, "condition_id") or hasattr(market, "clob_token_ids") or (isinstance(market, dict) and "condition_id" in market):
            platform = "polymarket"
        else:
            platform = "kalshi"

    # 3. Asks and Bids
    # Check best_yes_ask first (common on Orderbook), then yes_ask (common on Market/MarketState)
    yes_ask = getattr(market, "best_yes_ask", None)
    if yes_ask is None:
        yes_ask = getattr(market, "yes_ask", None)
    if yes_ask is None and isinstance(market, dict):
        yes_ask = market.get("best_yes_ask") if market.get("best_yes_ask") is not None else market.get("yes_ask")

    no_ask = getattr(market, "best_no_ask", None)
    if no_ask is None:
        no_ask = getattr(market, "no_ask", None)
    if no_ask is None and isinstance(market, dict):
        no_ask = market.get("best_no_ask") if market.get("best_no_ask") is not None else market.get("no_ask")

    # If asks are not on top level, inspect nested orderbook
    raw_ob = (
        getattr(market, "raw_orderbook", None)
        or getattr(market, "orderbook", None)
        or (market.get("raw_orderbook") if isinstance(market, dict) else None)
        or (market.get("orderbook") if isinstance(market, dict) else None)
    )
    if raw_ob is not None:
        if yes_ask is None:
            yes_ask = getattr(raw_ob, "best_yes_ask", None)
        if no_ask is None:
            no_ask = getattr(raw_ob, "best_no_ask", None)

    yes_bid = getattr(market, "best_yes_bid", None)
    if yes_bid is None:
        yes_bid = getattr(market, "yes_bid", None)
    if yes_bid is None and isinstance(market, dict):
        yes_bid = market.get("best_yes_bid") if market.get("best_yes_bid") is not None else market.get("yes_bid")

    no_bid = getattr(market, "best_no_bid", None)
    if no_bid is None:
        no_bid = getattr(market, "no_bid", None)
    if no_bid is None and isinstance(market, dict):
        no_bid = market.get("best_no_bid") if market.get("best_no_bid") is not None else market.get("no_bid")

    # 4. Timestamp
    ts = getattr(market, "ts", None) or (market.get("ts") if isinstance(market, dict) else None)
    if not isinstance(ts, datetime):
        ts = datetime.now(timezone.utc)
    elif ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    # 5. Orderbook Snapshot for Audit
    orderbook_snapshot: dict[str, Any] = {
        "platform": platform,
        "ticker": ticker,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "yes_bid": yes_bid,
        "no_bid": no_bid,
    }

    if hasattr(market, "volume") and market.volume is not None:
        orderbook_snapshot["volume"] = market.volume
    elif isinstance(market, dict) and "volume" in market:
        orderbook_snapshot["volume"] = market["volume"]

    # Include book depth tuples if available
    for side in ("yes_bids", "no_bids", "yes_asks", "no_asks"):
        val = getattr(market, side, None) or (getattr(raw_ob, side, None) if raw_ob else None)
        if val is not None:
            orderbook_snapshot[side] = val

    return platform, ticker, yes_ask, no_ask, orderbook_snapshot, ts


def find_intra_market_opportunities(
    markets: Sequence[Any],
    config: Optional[AppConfig] = None,
    min_edge: Optional[float] = None,
) -> list[Opportunity]:
    """Scan a list of prediction markets for intra-market arbitrage opportunities.

    For each market:
        gross_cost = best_yes_ask + best_no_ask
        edge = 100 - gross_cost (in cents)
        net_edge = edge - estimated_fees

    If `net_edge > min_edge` (converted to cents), an Opportunity is emitted.

    Args:
        markets: Sequence of Market or Orderbook instances to evaluate.
        config: Application configuration. If None, loads defaults via `load_config()`.
        min_edge: Optional threshold override in cents or decimal ratio (<1.0).

    Returns:
        List of Opportunity models representing viable intra-market arbitrage trades.
    """
    if config is None:
        config = load_config()

    # Convert min_edge to cents. If min_edge < 1.0 (e.g. 0.02 = 2%), convert to 2.0 cents.
    if min_edge is not None:
        min_edge_cents = min_edge * 100.0 if 0 < min_edge < 1.0 else float(min_edge)
    else:
        min_edge_val = config.trading.min_edge
        min_edge_cents = min_edge_val * 100.0 if min_edge_val < 1.0 else float(min_edge_val)

    opportunities: list[Opportunity] = []


    for market in markets:
        # Skip markets that are closed or settled
        status = getattr(market, "status", None) or (market.get("status") if isinstance(market, dict) else None)
        if status is not None and str(status).strip().lower() in ("closed", "settled", "resolved", "finalized"):
            continue

        platform, ticker, yes_ask, no_ask, orderbook_snapshot, ts = _extract_market_info(market)

        # Both sides must have an available ask to execute the arbitrage bundle
        if yes_ask is None or no_ask is None:
            continue

        # Asks must be within valid valid trading ranges (1-99 cents)
        if yes_ask <= 0 or no_ask <= 0:
            continue

        # Compute cost and edge in cents: payout is guaranteed 100 cents ($1.00)
        gross_cost_cents = float(yes_ask + no_ask)
        gross_edge_cents = 100.0 - gross_cost_cents

        # Subtract estimated transaction fees
        estimated_fees_cents = calculate_estimated_fees(
            gross_cost_cents=gross_cost_cents,
            config=config,
            platform=platform,
        )
        net_edge_cents = gross_edge_cents - estimated_fees_cents

        # Evaluate condition: net edge must strictly exceed min_edge threshold
        if round(net_edge_cents, 8) > round(min_edge_cents, 8):
            edge_cents_rounded = round(net_edge_cents, 4)
            opportunity = Opportunity(
                platform=platform,
                ticker=ticker,
                side_actions=["buy_yes", "buy_no"],
                edge_cents=edge_cents_rounded,
                timestamp=ts,
                orderbook_snapshot=orderbook_snapshot,
                gross_edge_cents=round(gross_edge_cents, 4),
                estimated_fees_cents=round(estimated_fees_cents, 4),
                strategy="intra_market",
            )
            opportunities.append(opportunity)
            logger.info(
                "Intra-market arb found for %s (%s): edge=%.2fc (gross=%.2fc, fee=%.2fc, asks: YES=%dc, NO=%dc)",
                ticker,
                platform,
                edge_cents_rounded,
                gross_edge_cents,
                estimated_fees_cents,
                yes_ask,
                no_ask,
            )

    return opportunities
