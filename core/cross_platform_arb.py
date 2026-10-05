"""Cross-platform arbitrage engine between Kalshi and Polymarket.

Evaluates matched event pairs across Kalshi and Polymarket orderbooks to detect
two-sided arbitrage opportunities:
    Direction (a): Buy Kalshi YES + Buy Polymarket NO
                   Gross Cost = Kalshi_YES_Ask + Polymarket_NO_Ask
    Direction (b): Buy Polymarket YES + Buy Kalshi NO
                   Gross Cost = Polymarket_YES_Ask + Kalshi_NO_Ask

Net profit edge is computed after subtracting all exchange fees (maker or taker bps)
and configurable Polygon network gas estimates for Polymarket transactions:
    Net Edge = 100¢ - Gross Cost - Estimated Fees

If Gross Cost < 100¢ - Estimated Fees (i.e. Net Edge > 0, exceeding min_edge threshold),
an Opportunity is emitted with both execution legs explicitly detailed.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, Optional, Sequence, Union

from core.config_loader import AppConfig, load_config
from core.models import Market, Opportunity, OpportunityLeg

logger = logging.getLogger(__name__)


def calculate_cross_platform_fees(
    kalshi_price_cents: float,
    polymarket_price_cents: float,
    config: Optional[AppConfig] = None,
    polygon_gas_estimate_cents: Optional[float] = None,
    is_maker: bool = False,
) -> tuple[float, float, float]:
    """Calculate fee components in cents for a cross-platform arbitrage trade.

    Fees consist of:
      1. Kalshi leg fee: percentage fee calculated from basis points (taker_fee_bps or maker_fee_bps).
      2. Polymarket leg fee: percentage fee (bps) plus configurable Polygon network gas estimate.

    Args:
        kalshi_price_cents: Ask price of the Kalshi contract in cents.
        polymarket_price_cents: Ask price of the Polymarket contract in cents.
        config: Application configuration containing trading fee parameters.
        polygon_gas_estimate_cents: Optional override for Polygon network gas in cents.
        is_maker: If True, uses `maker_fee_bps`; if False (default), uses `taker_fee_bps`.

    Returns:
        tuple[float, float, float]: (kalshi_fee_cents, polymarket_fee_cents, total_fees_cents)
    """
    fee_bps = 0.0
    cfg_gas = 0.0

    if config is not None and hasattr(config, "trading"):
        trading_cfg = config.trading
        fee_bps = getattr(trading_cfg, "maker_fee_bps" if is_maker else "taker_fee_bps", 0.0) or 0.0
        cfg_gas = (
            getattr(trading_cfg, "polygon_gas_estimate_cents", None)
            or getattr(trading_cfg, "polygon_gas_estimate", None)
            or 0.0
        )

    rate = (fee_bps / 10000.0) if fee_bps > 0.0 else 0.0
    kalshi_fee = kalshi_price_cents * rate
    poly_trade_fee = polymarket_price_cents * rate

    gas_cents = polygon_gas_estimate_cents if polygon_gas_estimate_cents is not None else float(cfg_gas)
    if gas_cents < 0.0:
        gas_cents = 0.0

    polymarket_fee = poly_trade_fee + gas_cents
    total_fees = kalshi_fee + polymarket_fee

    return kalshi_fee, polymarket_fee, total_fees


def _parse_cents_val(val: Any) -> Optional[int]:
    """Convert int, float, or string price representation into integer cents (0-100)."""
    if val is None or val == "":
        return None
    if isinstance(val, (int, float)):
        if 0.0 < float(val) <= 1.0:
            return int(round(float(val) * 100.0))
        return int(round(float(val)))
    if isinstance(val, str):
        try:
            f = float(val.strip())
            if 0.0 < f <= 1.0:
                return int(round(f * 100.0))
            return int(round(f))
        except ValueError:
            return None
    return None


def _extract_book_prices(book: Any) -> tuple[Optional[int], Optional[int], Optional[int], Optional[int], dict[str, Any]]:
    """Extract (yes_ask, no_ask, yes_bid, no_bid, snapshot) from any orderbook/market object or dict."""
    if book is None:
        return None, None, None, None, {}

    # Check best_yes_ask / yes_ask
    yes_ask = _parse_cents_val(getattr(book, "best_yes_ask", None))
    if yes_ask is None:
        yes_ask = _parse_cents_val(getattr(book, "yes_ask", None))
    if yes_ask is None and isinstance(book, dict):
        yes_ask = _parse_cents_val(book.get("best_yes_ask") if book.get("best_yes_ask") is not None else book.get("yes_ask"))

    no_ask = _parse_cents_val(getattr(book, "best_no_ask", None))
    if no_ask is None:
        no_ask = _parse_cents_val(getattr(book, "no_ask", None))
    if no_ask is None and isinstance(book, dict):
        no_ask = _parse_cents_val(book.get("best_no_ask") if book.get("best_no_ask") is not None else book.get("no_ask"))

    # Nested orderbook inspection
    raw_ob = (
        getattr(book, "raw_orderbook", None)
        or getattr(book, "orderbook", None)
        or (book.get("raw_orderbook") if isinstance(book, dict) else None)
        or (book.get("orderbook") if isinstance(book, dict) else None)
    )
    if raw_ob is not None:
        if yes_ask is None:
            yes_ask = _parse_cents_val(getattr(raw_ob, "best_yes_ask", None))
        if no_ask is None:
            no_ask = _parse_cents_val(getattr(raw_ob, "best_no_ask", None))

    yes_bid = _parse_cents_val(getattr(book, "best_yes_bid", None))
    if yes_bid is None:
        yes_bid = _parse_cents_val(getattr(book, "yes_bid", None))
    if yes_bid is None and isinstance(book, dict):
        yes_bid = _parse_cents_val(book.get("best_yes_bid") if book.get("best_yes_bid") is not None else book.get("yes_bid"))

    no_bid = _parse_cents_val(getattr(book, "best_no_bid", None))
    if no_bid is None:
        no_bid = _parse_cents_val(getattr(book, "no_bid", None))
    if no_bid is None and isinstance(book, dict):
        no_bid = _parse_cents_val(book.get("best_no_bid") if book.get("best_no_bid") is not None else book.get("no_bid"))

    # Derive from opposite bids if asks still missing (e.g. Kalshi orderbook identity)
    if yes_ask is None and no_bid is not None:
        yes_ask = 100 - no_bid
    if no_ask is None and yes_bid is not None:
        no_ask = 100 - yes_bid

    snapshot: dict[str, Any] = {
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "yes_bid": yes_bid,
        "no_bid": no_bid,
    }
    return yes_ask, no_ask, yes_bid, no_bid, snapshot


def _build_orderbook_map(orderbooks: Any) -> dict[str, Any]:
    """Convert orderbooks into an identifier-indexed dictionary."""
    mapping: dict[str, Any] = {}
    if isinstance(orderbooks, dict):
        for k, v in orderbooks.items():
            if k is not None:
                mapping[str(k).strip()] = v
                mapping[str(k).strip().lower()] = v
            for attr in ("ticker", "condition_id", "token_id", "market_ticker", "asset_id"):
                val = getattr(v, attr, None) or (v.get(attr) if isinstance(v, dict) else None)
                if val:
                    mapping[str(val).strip()] = v
                    mapping[str(val).strip().lower()] = v
        return mapping

    if isinstance(orderbooks, (list, tuple, set)):
        for item in orderbooks:
            for attr in ("ticker", "condition_id", "token_id", "market_ticker", "asset_id"):
                val = getattr(item, attr, None) or (item.get(attr) if isinstance(item, dict) else None)
                if val:
                    mapping[str(val).strip()] = item
                    mapping[str(val).strip().lower()] = item
    return mapping


def _resolve_book(mapping: dict[str, Any], identifier: str, fallback: Optional[Any] = None) -> Optional[Any]:
    """Look up orderbook by identifier with fallback support."""
    if not identifier and fallback is not None:
        return fallback
    clean_id = str(identifier).strip()
    if clean_id in mapping:
        return mapping[clean_id]
    if clean_id.lower() in mapping:
        return mapping[clean_id.lower()]
    return fallback


def _extract_pair_info(pair: Any) -> tuple[str, str, Optional[Any], Optional[Any]]:
    """Extract kalshi_ticker, polymarket_id, and any cached market objects from a pair."""
    if isinstance(pair, (list, tuple)) and len(pair) >= 2:
        return str(pair[0]), str(pair[1]), None, None

    if isinstance(pair, dict):
        k_id = pair.get("kalshi_ticker") or pair.get("kalshi_id") or pair.get("kalshi") or ""
        p_id = (
            pair.get("polymarket_condition_id")
            or pair.get("polymarket_token_id")
            or pair.get("polymarket_id")
            or pair.get("polymarket")
            or pair.get("condition_id")
            or ""
        )
        k_m = pair.get("kalshi_market")
        p_m = pair.get("polymarket_market")
        return str(k_id), str(p_id), k_m, p_m

    k_id = (
        getattr(pair, "kalshi_ticker", None)
        or getattr(pair, "kalshi_id", None)
        or getattr(pair, "kalshi", None)
        or ""
    )
    p_id = (
        getattr(pair, "polymarket_condition_id", None)
        or getattr(pair, "polymarket_token_id", None)
        or getattr(pair, "polymarket_id", None)
        or getattr(pair, "polymarket", None)
        or getattr(pair, "condition_id", None)
        or ""
    )
    k_m = getattr(pair, "kalshi_market", None)
    p_m = getattr(pair, "polymarket_market", None)
    return str(k_id), str(p_id), k_m, p_m


def _is_market_closed(market_or_book: Any) -> bool:
    """Check if market or orderbook represents a closed/settled market."""
    if market_or_book is None:
        return False
    status = getattr(market_or_book, "status", None) or (
        market_or_book.get("status") if isinstance(market_or_book, dict) else None
    )
    if status is not None and str(status).strip().lower() in ("closed", "settled", "resolved", "finalized", "inactive"):
        return True
    return False


def find_cross_platform_opportunities(
    matched_pairs: Sequence[Any],
    kalshi_orderbooks: Union[dict[str, Any], Sequence[Any]],
    polymarket_orderbooks: Union[dict[str, Any], Sequence[Any]],
    config: Optional[AppConfig] = None,
    min_edge: Optional[float] = None,
    polygon_gas_estimate_cents: Optional[float] = None,
    is_maker: bool = False,
) -> list[Opportunity]:
    """Scan matched prediction market pairs across Kalshi and Polymarket for arbitrage.

    For each matched pair, checks both directions:
      (a) buy Kalshi YES + buy Polymarket NO, cost = kalshi_yes_ask + polymarket_no_ask
      (b) buy Polymarket YES + buy Kalshi NO, cost = polymarket_yes_ask + kalshi_no_ask

    If either cost < 100¢ minus fees (using maker_fee_bps/taker_fee_bps plus Polygon gas estimate),
    an Opportunity is emitted with both execution legs specified.

    Args:
        matched_pairs: Sequence of matched pairs (MarketMatch instances, dicts, or tuples).
        kalshi_orderbooks: Kalshi orderbooks as a mapping (ticker -> book) or sequence.
        polymarket_orderbooks: Polymarket orderbooks as a mapping (condition/token_id -> book) or sequence.
        config: Application configuration. If None, attempts `load_config()`.
        min_edge: Minimum net edge in cents (or decimal ratio < 1.0). Overrides config.trading.min_edge if given.
        polygon_gas_estimate_cents: Optional Polygon gas estimate override in cents.
        is_maker: If True, uses maker_fee_bps; default False (taker).

    Returns:
        list[Opportunity]: List of detected cross-platform arbitrage opportunities.
    """
    if config is None:
        try:
            config = load_config()
        except Exception:
            config = None

    # Compute threshold in cents
    if min_edge is not None:
        threshold_cents = min_edge * 100.0 if 0 < min_edge < 1.0 else float(min_edge)
    elif config is not None and hasattr(config, "trading") and hasattr(config.trading, "min_edge"):
        me = config.trading.min_edge
        threshold_cents = me * 100.0 if 0 < me < 1.0 else float(me)
    else:
        threshold_cents = 0.0

    k_map = _build_orderbook_map(kalshi_orderbooks)
    p_map = _build_orderbook_map(polymarket_orderbooks)

    opportunities: list[Opportunity] = []
    now_ts = datetime.now(timezone.utc)

    for pair in matched_pairs:
        kalshi_id, poly_id, k_fallback, p_fallback = _extract_pair_info(pair)

        k_book = _resolve_book(k_map, kalshi_id, k_fallback)
        p_book = _resolve_book(p_map, poly_id, p_fallback)

        if k_book is None or p_book is None:
            continue

        if _is_market_closed(k_book) or _is_market_closed(p_book):
            continue

        k_yes_ask, k_no_ask, k_yes_bid, k_no_bid, k_snap = _extract_book_prices(k_book)
        p_yes_ask, p_no_ask, p_yes_bid, p_no_bid, p_snap = _extract_book_prices(p_book)

        # --------------------------------------------------------------------
        # Direction (a): Buy Kalshi YES + Buy Polymarket NO
        # --------------------------------------------------------------------
        if (
            k_yes_ask is not None
            and p_no_ask is not None
            and 0 < k_yes_ask < 100
            and 0 < p_no_ask < 100
        ):
            cost_a = float(k_yes_ask + p_no_ask)
            gross_edge_a = 100.0 - cost_a
            k_fee_a, p_fee_a, total_fees_a = calculate_cross_platform_fees(
                kalshi_price_cents=float(k_yes_ask),
                polymarket_price_cents=float(p_no_ask),
                config=config,
                polygon_gas_estimate_cents=polygon_gas_estimate_cents,
                is_maker=is_maker,
            )
            net_edge_a = gross_edge_a - total_fees_a

            # Criteria: cost < 100 - fees (net_edge > 0) and exceeds threshold
            if round(cost_a, 8) < round(100.0 - total_fees_a, 8) and round(net_edge_a, 8) > round(threshold_cents, 8):
                leg_kalshi_a = OpportunityLeg(
                    platform="kalshi",
                    ticker=kalshi_id,
                    side="buy_yes",
                    price_cents=float(k_yes_ask),
                    fee_cents=round(k_fee_a, 4),
                )
                leg_poly_a = OpportunityLeg(
                    platform="polymarket",
                    ticker=poly_id,
                    side="buy_no",
                    price_cents=float(p_no_ask),
                    fee_cents=round(p_fee_a, 4),
                )
                snapshot_a = {
                    "direction": "kalshi_yes_polymarket_no",
                    "kalshi_ticker": kalshi_id,
                    "polymarket_ticker": poly_id,
                    "kalshi_orderbook": k_snap,
                    "polymarket_orderbook": p_snap,
                    "cost_cents": cost_a,
                    "gross_edge_cents": round(gross_edge_a, 4),
                    "kalshi_fee_cents": round(k_fee_a, 4),
                    "polymarket_fee_cents": round(p_fee_a, 4),
                    "total_fees_cents": round(total_fees_a, 4),
                    "net_edge_cents": round(net_edge_a, 4),
                }

                opp_a = Opportunity(
                    platform="kalshi",
                    ticker=kalshi_id,
                    target_platform="polymarket",
                    target_ticker=poly_id,
                    side_actions=["buy_yes", "buy_no"],
                    legs=[leg_kalshi_a, leg_poly_a],
                    edge_cents=round(net_edge_a, 4),
                    gross_edge_cents=round(gross_edge_a, 4),
                    estimated_fees_cents=round(total_fees_a, 4),
                    strategy="cross_platform",
                    timestamp=now_ts,
                    orderbook_snapshot=snapshot_a,
                )
                opportunities.append(opp_a)
                logger.info(
                    "Cross-platform arb found (Kalshi YES + Polymarket NO) for %s / %s: "
                    "net_edge=%.2fc (gross=%.2fc, cost=%.1fc, fees=%.2fc)",
                    kalshi_id,
                    poly_id,
                    net_edge_a,
                    gross_edge_a,
                    cost_a,
                    total_fees_a,
                )

        # --------------------------------------------------------------------
        # Direction (b): Buy Polymarket YES + Buy Kalshi NO
        # --------------------------------------------------------------------
        if (
            p_yes_ask is not None
            and k_no_ask is not None
            and 0 < p_yes_ask < 100
            and 0 < k_no_ask < 100
        ):
            cost_b = float(p_yes_ask + k_no_ask)
            gross_edge_b = 100.0 - cost_b
            k_fee_b, p_fee_b, total_fees_b = calculate_cross_platform_fees(
                kalshi_price_cents=float(k_no_ask),
                polymarket_price_cents=float(p_yes_ask),
                config=config,
                polygon_gas_estimate_cents=polygon_gas_estimate_cents,
                is_maker=is_maker,
            )
            net_edge_b = gross_edge_b - total_fees_b

            # Criteria: cost < 100 - fees (net_edge > 0) and exceeds threshold
            if round(cost_b, 8) < round(100.0 - total_fees_b, 8) and round(net_edge_b, 8) > round(threshold_cents, 8):
                leg_poly_b = OpportunityLeg(
                    platform="polymarket",
                    ticker=poly_id,
                    side="buy_yes",
                    price_cents=float(p_yes_ask),
                    fee_cents=round(p_fee_b, 4),
                )
                leg_kalshi_b = OpportunityLeg(
                    platform="kalshi",
                    ticker=kalshi_id,
                    side="buy_no",
                    price_cents=float(k_no_ask),
                    fee_cents=round(k_fee_b, 4),
                )
                snapshot_b = {
                    "direction": "polymarket_yes_kalshi_no",
                    "kalshi_ticker": kalshi_id,
                    "polymarket_ticker": poly_id,
                    "kalshi_orderbook": k_snap,
                    "polymarket_orderbook": p_snap,
                    "cost_cents": cost_b,
                    "gross_edge_cents": round(gross_edge_b, 4),
                    "kalshi_fee_cents": round(k_fee_b, 4),
                    "polymarket_fee_cents": round(p_fee_b, 4),
                    "total_fees_cents": round(total_fees_b, 4),
                    "net_edge_cents": round(net_edge_b, 4),
                }

                opp_b = Opportunity(
                    platform="polymarket",
                    ticker=poly_id,
                    target_platform="kalshi",
                    target_ticker=kalshi_id,
                    side_actions=["buy_yes", "buy_no"],
                    legs=[leg_poly_b, leg_kalshi_b],
                    edge_cents=round(net_edge_b, 4),
                    gross_edge_cents=round(gross_edge_b, 4),
                    estimated_fees_cents=round(total_fees_b, 4),
                    strategy="cross_platform",
                    timestamp=now_ts,
                    orderbook_snapshot=snapshot_b,
                )
                opportunities.append(opp_b)
                logger.info(
                    "Cross-platform arb found (Polymarket YES + Kalshi NO) for %s / %s: "
                    "net_edge=%.2fc (gross=%.2fc, cost=%.1fc, fees=%.2fc)",
                    poly_id,
                    kalshi_id,
                    net_edge_b,
                    gross_edge_b,
                    cost_b,
                    total_fees_b,
                )

    return opportunities
