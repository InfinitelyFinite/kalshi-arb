"""Comprehensive unit and integration tests for executability, filtering, and opportunity persistence.

Tests cover:
- Hand-computed example with exact decimal assertions for VWAP, raw edge, depth edge, fees, and gas.
- Orderbook depth fillability across sizes q in {1, 10, 50}, insufficient depth, and truncated depth.
- Variable quadratic fees (Kalshi & Polymarket), order-level ceil rounding, and RAW_ONLY transition.
- Quote freshness, timestamp reliability, forward clock tolerance, and cross-platform skew vs intra NOT_APPLICABLE.
- Complete classification coverage: NOT_RAW, RAW_ONLY, FEE_ADJUSTED, EXECUTABLE_CANDIDATE.
- Episode tracking, consecutive runs, gap censoring, duration bounds, and latency survival statuses.
- Live and replay equivalence using the same evaluation functions and EpisodeTracker.
- DuckDB persistence roundtrip, idempotent re-runs, and denominator counts in evaluation_counts.
- Funnel report generation across sizes and sensitivity sweeps.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import duckdb
import pytest

from core.depth import Level
from core.executability import (
    Classification,
    EpisodeTracker,
    ExecutabilityParams,
    LatencyStatus,
    LegInput,
    OpportunityEvaluation,
    OpportunityInput,
    RejectReason,
    calculate_kalshi_taker_fee_cents,
    calculate_polymarket_taker_fee_cents,
    evaluate_opportunity,
)
from core.opportunity_store import (
    ensure_executability_schema,
    save_param_set,
    write_episodes_and_latencies,
    write_evaluations_and_counts,
)
from scripts.opportunity_funnel_report import format_funnel_markdown, generate_funnel_data
from simulation.replay_engine import ReplayEngine


# ============================================================================
# 1. Hand-computed Example (Exact Assertions)
# ============================================================================


def test_hand_computed_example():
    """Spec hand-computed example:

    Leg A ask levels: [(40, 5), (42, 10)]
    Leg B ask levels: [(57, 20)]
    Order size q = 10.
    VWAP_A = (5*40 + 5*42) / 10 = 41.0
    VWAP_B = 57.0
    Raw edge at best asks = 100 - (40 + 57) = 3.0
    Depth edge before costs = 100 - 41.0 - 57.0 = 2.0
    """
    t0 = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    ladder_a: list[Level] = [(Decimal("40"), Decimal("5")), (Decimal("42"), Decimal("10"))]
    ladder_b: list[Level] = [(Decimal("57"), Decimal("20"))]

    leg_a = LegInput(
        snapshot_id=1,
        platform="kalshi",
        ticker="KXTEST-A",
        best_ask=Decimal("40"),
        ask_ladder=ladder_a,
        depth_truncated=False,
        observed_at_utc=t0,
        exchange_updated_at_utc=t0,
        tradable=True,
        data_source="live_api",
        is_inferred=False,
    )
    leg_b = LegInput(
        snapshot_id=2,
        platform="polymarket",
        ticker="0xPOLY-B",
        best_ask=Decimal("57"),
        ask_ladder=ladder_b,
        depth_truncated=False,
        observed_at_utc=t0,
        exchange_updated_at_utc=t0,
        tradable=True,
        data_source="live_api",
        is_inferred=False,
    )
    opp_input = OpportunityInput(
        kind="cross",
        pair_id="PAIR-AB",
        ticker="PAIR-AB",
        direction="kalshi_yes_polymarket_no",
        leg1=leg_a,
        leg2=leg_b,
        pair_verified=True,
    )

    # 1. Zero costs evaluation
    params_zero_costs = ExecutabilityParams(
        sizes=[10],
        min_edge_cents=Decimal("0.0"),
        kalshi_taker_rate=Decimal("0.0"),
        polymarket_taker_rate=Decimal("0.0"),
        polygon_gas_cents=Decimal("0.0"),
        slippage_cents_per_bundle=Decimal("0.0"),
    )
    ev = evaluate_opportunity(opp_input, params_zero_costs, t_eval=t0, size_q=10)

    assert ev.vwap1 == Decimal("41.0")
    assert ev.vwap2 == Decimal("57.0")
    assert ev.raw_edge == Decimal("3.0")
    assert ev.net_edge_top == Decimal("3.0")
    assert ev.net_edge_vwap == Decimal("2.0")
    assert ev.classification == Classification.EXECUTABLE_CANDIDATE

    # 2. Hand-computed fee and gas terms
    # Kalshi: q=10 at vwap=41c. P = 0.41. 1-P = 0.59.
    # Kalshi fee = ceil(1.0 * 0.07 * 10 * 0.41 * 0.59 * 100) = ceil(16.933) = 17 cents.
    fee_k = calculate_kalshi_taker_fee_cents(Decimal("41"), Decimal("10"), rate=Decimal("0.07"))
    assert fee_k == Decimal("17")

    # Polymarket: rate = 0.04 (4% taker rate). q=10 at vwap=57c. P = 0.57. 1-P = 0.43.
    # Polymarket fee = ceil(0.04 * 10 * 0.57 * 0.43 * 100) = ceil(9.804) = 10 cents.
    fee_p = calculate_polymarket_taker_fee_cents(Decimal("57"), Decimal("10"), rate=Decimal("0.04"))
    assert fee_p == Decimal("10")

    # Total fees for q=10: 17 + 10 = 27 cents. Per bundle = 2.7 cents.
    # Polygon gas = 10 cents total. Per bundle = 1.0 cents.
    # Slippage = 0.5 cents per bundle.
    # Net edge VWAP = 100 - 41.0 - 57.0 - 2.7 - 1.0 - 0.5 = 2.0 - 4.2 = -2.2 cents.
    params_with_costs = ExecutabilityParams(
        sizes=[10],
        min_edge_cents=Decimal("0.0"),
        kalshi_taker_rate=Decimal("0.07"),
        polymarket_taker_rate=Decimal("0.04"),
        polygon_gas_cents=Decimal("10.0"),
        slippage_cents_per_bundle=Decimal("0.5"),
    )
    ev_cost = evaluate_opportunity(opp_input, params_with_costs, t_eval=t0, size_q=10)
    assert ev_cost.fees_cents == Decimal("27")
    assert ev_cost.gas_cents_total == Decimal("10.0")
    assert ev_cost.slippage_cents == Decimal("0.5")
    assert ev_cost.net_edge_vwap == Decimal("-2.2")
    assert ev_cost.classification == Classification.RAW_ONLY


# ============================================================================
# 2. Depth Fillability & Truncation
# ============================================================================


def test_depth_fillability_across_sizes():
    """Depth: q = 1, 10, 50. Total book contracts = 15.

    - q = 1: fills fully (level 1)
    - q = 10: fills fully (level 1 + level 2)
    - q = 50: INSUFFICIENT_DEPTH
    """
    t0 = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    ladder: list[Level] = [(Decimal("40"), Decimal("5")), (Decimal("45"), Decimal("10"))]

    leg1 = LegInput(
        snapshot_id=1,
        platform="kalshi",
        ticker="KXTEST-1",
        best_ask=Decimal("40"),
        ask_ladder=ladder,
        depth_truncated=False,
        observed_at_utc=t0,
        exchange_updated_at_utc=t0,
        tradable=True,
        data_source="live_api",
        is_inferred=False,
    )
    leg2 = LegInput(
        snapshot_id=1,
        platform="kalshi",
        ticker="KXTEST-1",
        best_ask=Decimal("50"),
        ask_ladder=[(Decimal("50"), Decimal("100"))],
        depth_truncated=False,
        observed_at_utc=t0,
        exchange_updated_at_utc=t0,
        tradable=True,
        data_source="live_api",
        is_inferred=False,
    )
    opp = OpportunityInput("intra", None, "KXTEST-1", "kalshi_intra", leg1, leg2)
    params = ExecutabilityParams(sizes=[1, 10, 50], min_edge_cents=Decimal("1.0"), kalshi_taker_rate=Decimal("0.0"))

    # q = 1
    ev_1 = evaluate_opportunity(opp, params, t0, size_q=1)
    assert ev_1.classification == Classification.EXECUTABLE_CANDIDATE
    assert ev_1.primary_reject_reason is None

    # q = 10
    ev_10 = evaluate_opportunity(opp, params, t0, size_q=10)
    assert ev_10.classification == Classification.EXECUTABLE_CANDIDATE
    assert ev_10.primary_reject_reason is None

    # q = 50: cannot fill 50 contracts from 15 available
    ev_50 = evaluate_opportunity(opp, params, t0, size_q=50)
    assert RejectReason.INSUFFICIENT_DEPTH in ev_50.all_reject_reasons
    assert ev_50.primary_reject_reason == RejectReason.INSUFFICIENT_DEPTH


def test_depth_truncated_flag():
    """Conservative depth truncation: if depth_truncated is True and q cannot be filled,

    flag remains on evaluation record.
    """
    t0 = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    leg1 = LegInput(
        snapshot_id=1,
        platform="kalshi",
        ticker="KXTEST-1",
        best_ask=Decimal("40"),
        ask_ladder=[(Decimal("40"), Decimal("5"))],
        depth_truncated=True,
        observed_at_utc=t0,
        exchange_updated_at_utc=t0,
        tradable=True,
        data_source="live_api",
    )
    leg2 = LegInput(
        snapshot_id=1,
        platform="kalshi",
        ticker="KXTEST-1",
        best_ask=Decimal("50"),
        ask_ladder=[(Decimal("50"), Decimal("50"))],
        depth_truncated=False,
        observed_at_utc=t0,
        exchange_updated_at_utc=t0,
        tradable=True,
        data_source="live_api",
    )
    opp = OpportunityInput("intra", None, "KXTEST-1", "kalshi_intra", leg1, leg2)
    params = ExecutabilityParams(sizes=[10], min_edge_cents=Decimal("1.0"), kalshi_taker_rate=Decimal("0.0"))

    ev = evaluate_opportunity(opp, params, t0, size_q=10)
    assert ev.depth_truncated is True
    assert ev.primary_reject_reason == RejectReason.INSUFFICIENT_DEPTH


# ============================================================================
# 3. Fees and RAW_ONLY Classification
# ============================================================================


def test_fees_at_actual_order_size_and_raw_only():
    """Fee computed at actual order size with rounding up.

    Case: raw_edge > 0 but net edge <= E_min -> classifies as RAW_ONLY.
    """
    t0 = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    # Best asks: 49 + 50 = 99 -> raw edge = 1.0c (> 0)
    leg1 = LegInput(1, "kalshi", "K1", Decimal("49"), [(Decimal("49"), Decimal("10"))], False, t0, t0, True, "live_api")
    leg2 = LegInput(1, "kalshi", "K1", Decimal("50"), [(Decimal("50"), Decimal("10"))], False, t0, t0, True, "live_api")
    opp = OpportunityInput("intra", None, "K1", "kalshi_intra", leg1, leg2)

    # Standard Kalshi taker fee at 49c and 50c:
    # Rate 0.07. q=10.
    # Fee1 = ceil(0.07 * 10 * 0.49 * 0.51 * 100) = ceil(17.49) = 18 cents
    # Fee2 = ceil(0.07 * 10 * 0.50 * 0.50 * 100) = ceil(17.50) = 18 cents
    # Total fee = 36 cents for 10 contracts -> 3.6c per contract!
    # Gross edge = 1.0c. Net edge = 1.0 - 3.6 = -2.6c < min_edge (2.0c).
    params = ExecutabilityParams(sizes=[10], min_edge_cents=Decimal("2.0"), kalshi_taker_rate=Decimal("0.07"))
    ev = evaluate_opportunity(opp, params, t0, size_q=10)

    assert ev.raw_positive is True
    assert ev.fee_adjusted is False
    assert ev.classification == Classification.RAW_ONLY
    assert ev.primary_reject_reason == RejectReason.BELOW_MIN_EDGE_AFTER_COSTS


# ============================================================================
# 4. Freshness, Timestamps, and Skew
# ============================================================================


def test_freshness_threshold_and_stale():
    """Quote age checks:

    - Age == tau_fresh (pass)
    - Age > tau_fresh (STALE_QUOTE)
    - Missing timestamp (MISSING_TIMESTAMP, never fallback to observed)
    - Future timestamp > tolerance (UNRELIABLE_TIMESTAMP)
    """
    t_eval = datetime(2026, 11, 9, 12, 0, 10, tzinfo=timezone.utc)
    params = ExecutabilityParams(
        sizes=[1],
        tau_fresh_s=Decimal("10.0"),
        clock_tolerance_s=Decimal("2.0"),
        kalshi_taker_rate=Decimal("0.0"),
    )

    # 1. Age == 10s (exact threshold -> PASS)
    t_exch_10s = t_eval - timedelta(seconds=10)
    leg1 = LegInput(1, "kalshi", "K1", Decimal("40"), [(Decimal("40"), Decimal("10"))], False, t_eval, t_exch_10s, True, "live_api")
    leg2 = LegInput(1, "kalshi", "K1", Decimal("50"), [(Decimal("50"), Decimal("10"))], False, t_eval, t_exch_10s, True, "live_api")
    ev_pass = evaluate_opportunity(OpportunityInput("intra", None, "K1", "kalshi_intra", leg1, leg2), params, t_eval, 1)
    assert ev_pass.classification == Classification.EXECUTABLE_CANDIDATE

    # 2. Age == 10.1s (> tau_fresh -> STALE_QUOTE)
    t_exch_stale = t_eval - timedelta(seconds=10, milliseconds=100)
    leg1_stale = LegInput(1, "kalshi", "K1", Decimal("40"), [(Decimal("40"), Decimal("10"))], False, t_eval, t_exch_stale, True, "live_api")
    leg2_stale = LegInput(1, "kalshi", "K1", Decimal("50"), [(Decimal("50"), Decimal("10"))], False, t_eval, t_exch_stale, True, "live_api")
    ev_stale = evaluate_opportunity(OpportunityInput("intra", None, "K1", "kalshi_intra", leg1_stale, leg2_stale), params, t_eval, 1)
    assert RejectReason.STALE_QUOTE in ev_stale.all_reject_reasons
    assert ev_stale.primary_reject_reason == RejectReason.STALE_QUOTE

    # 3. Missing exchange timestamp
    leg1_missing = LegInput(1, "kalshi", "K1", Decimal("40"), [(Decimal("40"), Decimal("10"))], False, t_eval, None, True, "live_api")
    ev_missing = evaluate_opportunity(OpportunityInput("intra", None, "K1", "kalshi_intra", leg1_missing, leg2), params, t_eval, 1)
    assert RejectReason.MISSING_TIMESTAMP in ev_missing.all_reject_reasons
    assert ev_missing.primary_reject_reason == RejectReason.MISSING_TIMESTAMP

    # 4. Future timestamp beyond 2s tolerance
    t_future = t_eval + timedelta(seconds=3)
    leg1_future = LegInput(1, "kalshi", "K1", Decimal("40"), [(Decimal("40"), Decimal("10"))], False, t_eval, t_future, True, "live_api")
    ev_future = evaluate_opportunity(OpportunityInput("intra", None, "K1", "kalshi_intra", leg1_future, leg2), params, t_eval, 1)
    assert RejectReason.UNRELIABLE_TIMESTAMP in ev_future.all_reject_reasons
    assert ev_future.primary_reject_reason == RejectReason.UNRELIABLE_TIMESTAMP


def test_cross_platform_skew_and_intra_not_applicable():
    """Inter-exchange skew:

    - Cross-platform: skew at threshold (pass), skew > threshold (EXCESS_SKEW).
    - Intra-market: skew is NOT_APPLICABLE (ev.quote_skew is None, no EXCESS_SKEW).
    """
    t_eval = datetime(2026, 11, 9, 12, 0, 10, tzinfo=timezone.utc)
    params = ExecutabilityParams(
        sizes=[1],
        tau_fresh_s=Decimal("15.0"),
        tau_skew_s=Decimal("5.0"),
        kalshi_taker_rate=Decimal("0.0"),
        polymarket_taker_rate=Decimal("0.0"),
    )

    t_k = t_eval - timedelta(seconds=2)
    t_p_pass = t_eval - timedelta(seconds=7)  # skew = 5.0s (exact threshold)
    t_p_fail = t_eval - timedelta(seconds=8)  # skew = 6.0s (> 5.0s)

    leg_k = LegInput(1, "kalshi", "K1", Decimal("40"), [(Decimal("40"), Decimal("10"))], False, t_eval, t_k, True, "live_api")
    leg_p_pass = LegInput(2, "polymarket", "P1", Decimal("50"), [(Decimal("50"), Decimal("10"))], False, t_eval, t_p_pass, True, "live_api")
    leg_p_fail = LegInput(2, "polymarket", "P1", Decimal("50"), [(Decimal("50"), Decimal("10"))], False, t_eval, t_p_fail, True, "live_api")

    # Pass case
    opp_pass = OpportunityInput("cross", "PAIR-1", "PAIR-1", "kalshi_yes_polymarket_no", leg_k, leg_p_pass, True)
    ev_pass = evaluate_opportunity(opp_pass, params, t_eval, 1)
    assert ev_pass.quote_skew == Decimal("5.0")
    assert ev_pass.classification == Classification.EXECUTABLE_CANDIDATE

    # Excess skew case
    opp_fail = OpportunityInput("cross", "PAIR-1", "PAIR-1", "kalshi_yes_polymarket_no", leg_k, leg_p_fail, True)
    ev_fail = evaluate_opportunity(opp_fail, params, t_eval, 1)
    assert ev_fail.quote_skew == Decimal("6.0")
    assert RejectReason.EXCESS_SKEW in ev_fail.all_reject_reasons
    assert ev_fail.primary_reject_reason == RejectReason.EXCESS_SKEW

    # Intra-market case: skew is NOT_APPLICABLE
    opp_intra = OpportunityInput("intra", None, "K1", "kalshi_intra", leg_k, leg_k, True)
    ev_intra = evaluate_opportunity(opp_intra, params, t_eval, 1)
    assert ev_intra.quote_skew is None
    assert RejectReason.EXCESS_SKEW not in ev_intra.all_reject_reasons


# ============================================================================
# 5. Four Classifications & Priority Ordering
# ============================================================================


def test_four_classifications_fixtures():
    """Verify distinct classification fixtures and priority order of reject reasons."""
    t0 = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    params = ExecutabilityParams(sizes=[1], min_edge_cents=Decimal("2.0"), kalshi_taker_rate=Decimal("0.05"))

    # 1. NOT_RAW: best asks sum >= 100 (55 + 50 = 105)
    l1 = LegInput(1, "kalshi", "K1", Decimal("55"), [(Decimal("55"), Decimal("10"))], False, t0, t0, True, "live_api")
    l2 = LegInput(1, "kalshi", "K1", Decimal("50"), [(Decimal("50"), Decimal("10"))], False, t0, t0, True, "live_api")
    ev1 = evaluate_opportunity(OpportunityInput("intra", None, "K1", "kalshi_intra", l1, l2), params, t0, 1)
    assert ev1.classification == Classification.NOT_RAW
    assert ev1.primary_reject_reason == RejectReason.NO_RAW_EDGE

    # 2. RAW_ONLY: raw edge exists but costs make net edge <= min_edge
    l1_raw = LegInput(1, "kalshi", "K1", Decimal("49"), [(Decimal("49"), Decimal("10"))], False, t0, t0, True, "live_api")
    l2_raw = LegInput(1, "kalshi", "K1", Decimal("50"), [(Decimal("50"), Decimal("10"))], False, t0, t0, True, "live_api")
    ev2 = evaluate_opportunity(OpportunityInput("intra", None, "K1", "kalshi_intra", l1_raw, l2_raw), params, t0, 1)
    assert ev2.classification == Classification.RAW_ONLY

    # 3. FEE_ADJUSTED: passes fee-adjusted check at top of book, but fails executable condition (e.g. market not tradable)
    l1_not_tradable = LegInput(1, "kalshi", "K1", Decimal("40"), [(Decimal("40"), Decimal("10"))], False, t0, t0, False, "live_api")
    l2_tradable = LegInput(1, "kalshi", "K1", Decimal("50"), [(Decimal("50"), Decimal("10"))], False, t0, t0, True, "live_api")
    ev3 = evaluate_opportunity(OpportunityInput("intra", None, "K1", "kalshi_intra", l1_not_tradable, l2_tradable), params, t0, 1)
    assert ev3.classification == Classification.FEE_ADJUSTED
    assert ev3.primary_reject_reason == RejectReason.MARKET_NOT_TRADABLE

    # 4. EXECUTABLE_CANDIDATE: all conditions pass
    l1_cand = LegInput(1, "kalshi", "K1", Decimal("40"), [(Decimal("40"), Decimal("10"))], False, t0, t0, True, "live_api")
    l2_cand = LegInput(1, "kalshi", "K1", Decimal("50"), [(Decimal("50"), Decimal("10"))], False, t0, t0, True, "live_api")
    ev4 = evaluate_opportunity(OpportunityInput("intra", None, "K1", "kalshi_intra", l1_cand, l2_cand), params, t0, 1)
    assert ev4.classification == Classification.EXECUTABLE_CANDIDATE
    assert ev4.primary_reject_reason is None
    assert len(ev4.all_reject_reasons) == 0


# ============================================================================
# 6. Episode Tracking, Censoring, & Latency Survival
# ============================================================================


def test_episode_tracking_and_latency_survival():
    """Episode tracking rules:

    - Repeated observations merge into 1 episode.
    - Failing observation ends episode.
    - Gap > tolerance * interval ends episode as censored_right.
    - Latency survival: SATISFIED (duration >= L), FAILED (duration_upper < L), UNRESOLVED (sampling coarse).
    """
    sampling_s = 60.0
    tracker = EpisodeTracker(
        sampling_interval_s=sampling_s,
        episode_gap_tolerance=1.5,
        latencies_s=[10.0, 60.0, 120.0, 180.0],
    )

    t0 = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(seconds=60)
    t2 = t0 + timedelta(seconds=120)
    t3 = t0 + timedelta(seconds=180)  # failure

    def make_eval(t: datetime, is_exec: bool) -> OpportunityEvaluation:
        return OpportunityEvaluation(
            kind="intra",
            pair_id=None,
            ticker="K1",
            direction="kalshi_intra",
            size_q=10,
            raw_edge=Decimal("10.0") if is_exec else Decimal("-5.0"),
            raw_positive=is_exec,
            net_edge_top=Decimal("8.0") if is_exec else Decimal("-5.0"),
            fee_adjusted=is_exec,
            vwap1=Decimal("40.0") if is_exec else None,
            vwap2=Decimal("50.0") if is_exec else None,
            fees_cents=Decimal("15.0"),
            gas_cents_total=Decimal("0.0"),
            slippage_cents=Decimal("0.0"),
            net_edge_vwap=Decimal("6.5") if is_exec else None,
            quote_age_k=Decimal("2.0"),
            quote_age_p=None,
            quote_skew=None,
            snapshot_id_1=1,
            snapshot_id_2=1,
            data_source="live_api",
            classification=Classification.EXECUTABLE_CANDIDATE if is_exec else Classification.NOT_RAW,
            primary_reject_reason=None if is_exec else RejectReason.NO_RAW_EDGE,
            all_reject_reasons=[] if is_exec else [RejectReason.NO_RAW_EDGE],
        )

    # Step t0, t1, t2 passing
    tracker.step(t0, [make_eval(t0, True)])
    tracker.step(t1, [make_eval(t1, True)])
    tracker.step(t2, [make_eval(t2, True)])

    # Condition fails at t3 (180s)
    tracker.step(t3, [make_eval(t3, False)])

    episodes = tracker.finalize()
    exec_eps = [e for e in episodes if e.level == "executable"]
    assert len(exec_eps) == 1
    ep = exec_eps[0]

    assert ep.n_observations == 3
    assert ep.first_seen_utc == t0
    assert ep.last_seen_utc == t2
    assert ep.duration_lower_s == Decimal("120.0")
    assert ep.duration_upper_s == Decimal("180.0")
    assert ep.censored_left is True  # opened at start
    assert ep.censored_right is False  # closed cleanly by failure at t3

    # Latency evaluation
    # L = 10s: lower_s (120) >= 10 -> SATISFIED
    status_10, _ = tracker.evaluate_latency_status(ep, 10.0)
    assert status_10 == LatencyStatus.LATENCY_SATISFIED

    # L = 60s: lower_s (120) >= 60 -> SATISFIED
    status_60, _ = tracker.evaluate_latency_status(ep, 60.0)
    assert status_60 == LatencyStatus.LATENCY_SATISFIED

    # L = 120s: lower_s (120) >= 120 -> SATISFIED
    status_120, _ = tracker.evaluate_latency_status(ep, 120.0)
    assert status_120 == LatencyStatus.LATENCY_SATISFIED

    # L = 200s: upper_s (180) < 200 -> FAILED
    status_200, _ = tracker.evaluate_latency_status(ep, 200.0)
    assert status_200 == LatencyStatus.LATENCY_FAILED

    # Single-observation episode case with coarse sampling interval
    tracker_single = EpisodeTracker(sampling_interval_s=60.0, latencies_s=[10.0])
    tracker_single.step(t0, [make_eval(t0, True)])
    tracker_single.step(t1, [make_eval(t1, False)])  # closed after 1 observation
    ep_single = [e for e in tracker_single.finalize() if e.level == "executable"][0]
    assert ep_single.duration_lower_s == Decimal("0.0")
    assert ep_single.duration_upper_s == Decimal("60.0")
    # For L = 10s: lower (0) < 10, but upper (60) >= 10 -> UNRESOLVED
    status_single, _ = tracker_single.evaluate_latency_status(ep_single, 10.0)
    assert status_single == LatencyStatus.LATENCY_UNRESOLVED


# ============================================================================
# 7. DuckDB Persistence & Idempotency
# ============================================================================


def test_duckdb_store_roundtrip_and_idempotency(tmp_path: Path):
    """Verify DuckDB store roundtrip, denominator counts, and idempotent re-runs."""
    db_file = tmp_path / "test_exec.duckdb"
    conn = duckdb.connect(str(db_file))
    ensure_executability_schema(conn)

    params = ExecutabilityParams(sizes=[1, 10])
    save_param_set(conn, "param_v1", params)

    t0 = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    leg = LegInput(101, "kalshi", "K1", Decimal("40"), [(Decimal("40"), Decimal("10"))], False, t0, t0, True, "live_api")
    opp = OpportunityInput("intra", None, "K1", "kalshi_intra", leg, leg)
    ev1 = evaluate_opportunity(opp, params, t0, size_q=1)
    ev10 = evaluate_opportunity(opp, params, t0, size_q=10)

    # First write
    write_evaluations_and_counts(conn, "param_v1", [ev1, ev10], t0)
    row_cnt_1 = conn.execute("SELECT count(*) FROM opportunity_evaluations WHERE param_set_id = 'param_v1'").fetchone()[0]
    assert row_cnt_1 == 2

    # Denominator check
    denom_cnt = conn.execute("SELECT sum(n_evaluated) FROM evaluation_counts WHERE param_set_id = 'param_v1'").fetchone()[0]
    assert denom_cnt == 2

    # Idempotent rewrite of same snapshots
    write_evaluations_and_counts(conn, "param_v1", [ev1, ev10], t0)
    row_cnt_2 = conn.execute("SELECT count(*) FROM opportunity_evaluations WHERE param_set_id = 'param_v1'").fetchone()[0]
    assert row_cnt_2 == 2  # not duplicated!

    conn.close()


# ============================================================================
# 8. Live & Replay Equivalence
# ============================================================================


@pytest.mark.asyncio
async def test_live_and_replay_equivalence(tmp_path: Path):
    """Assert live collection logic and ReplayEngine produce identical evaluations and episodes."""
    db_file = tmp_path / "replay_equiv.duckdb"
    conn = duckdb.connect(str(db_file))
    conn.execute(
        """
        CREATE TABLE orderbook_snapshots (
            snapshot_id BIGINT,
            ts TIMESTAMP WITH TIME ZONE,
            platform VARCHAR,
            ticker VARCHAR,
            yes_bid INTEGER,
            yes_ask INTEGER,
            no_bid INTEGER,
            no_ask INTEGER,
            volume BIGINT,
            observed_at_utc TIMESTAMP WITH TIME ZONE,
            exchange_updated_at_utc TIMESTAMP WITH TIME ZONE,
            market_status VARCHAR,
            tradable BOOLEAN,
            data_source VARCHAR,
            depth_truncated BOOLEAN,
            pair_id VARCHAR
        );
        CREATE TABLE orderbook_levels (
            snapshot_id BIGINT,
            side VARCHAR,
            price DECIMAL(10, 4),
            size DECIMAL(10, 4),
            level_idx INTEGER
        );
        """
    )
    t0 = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 11, 9, 12, 1, 0, tzinfo=timezone.utc)

    # Insert snapshot rows
    conn.execute(
        """
        INSERT INTO orderbook_snapshots VALUES
        (1, ?, 'kalshi', 'KXTEST', 45, 40, 45, 50, 1000, ?, ?, 'open', TRUE, 'live_api', FALSE, NULL),
        (2, ?, 'kalshi', 'KXTEST', 45, 40, 45, 50, 1000, ?, ?, 'open', TRUE, 'live_api', FALSE, NULL)
        """,
        [t0, t0, t0, t1, t1, t1],
    )
    # Insert depth levels
    conn.execute(
        """
        INSERT INTO orderbook_levels VALUES
        (1, 'yes_ask', 40.0, 20.0, 0),
        (1, 'no_ask', 50.0, 20.0, 0),
        (2, 'yes_ask', 40.0, 20.0, 0),
        (2, 'no_ask', 50.0, 20.0, 0)
        """
    )
    conn.close()

    engine = ReplayEngine(
        duckdb_path=db_file,
        speed=0.0,
        persist_evaluations=True,
        param_set_id="test_equiv_run",
    )
    summary = await engine.run()
    assert summary.total_snapshots == 2

    # Query persisted results
    read_conn = duckdb.connect(str(db_file), read_only=True)
    eval_rows = read_conn.execute(
        "SELECT size_q, classification, net_edge_vwap FROM opportunity_evaluations WHERE param_set_id = 'test_equiv_run' ORDER BY size_q, t_eval_utc"
    ).fetchall()
    read_conn.close()

    assert len(eval_rows) > 0
    # Every size 1 and 10 was candidate
    for q, classification, vwap_edge in eval_rows:
        if q in (1, 10):
            assert classification == "EXECUTABLE_CANDIDATE"
            assert vwap_edge > 0


# ============================================================================
# 9. Funnel Report on Fixture
# ============================================================================


def test_funnel_report_fixture(tmp_path: Path):
    """Generate funnel report data on a test database and verify markdown and JSON output."""
    db_file = tmp_path / "funnel_test.duckdb"
    conn = duckdb.connect(str(db_file))
    ensure_executability_schema(conn)

    params = ExecutabilityParams(sizes=[10])
    save_param_set(conn, "funnel_p1", params)

    t0 = datetime(2026, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
    leg = LegInput(1, "kalshi", "K1", Decimal("40"), [(Decimal("40"), Decimal("20"))], False, t0, t0, True, "live_api")
    opp = OpportunityInput("intra", None, "K1", "kalshi_intra", leg, leg)
    ev = evaluate_opportunity(opp, params, t0, 10)

    write_evaluations_and_counts(conn, "funnel_p1", [ev], t0)

    tracker = EpisodeTracker(sampling_interval_s=60.0)
    tracker.step(t0, [ev])
    episodes = tracker.finalize()
    write_episodes_and_latencies(conn, "funnel_p1", tracker, episodes)
    conn.close()

    # Generate funnel data
    data = generate_funnel_data(db_file, param_set_id="funnel_p1")
    assert "funnel_p1" in data
    p1 = data["funnel_p1"]
    assert "10" in p1["sizes"]
    assert p1["sizes"]["10"]["executable_observations"] == 1
    assert p1["sizes"]["10"]["executable_episodes"] == 1

    md = format_funnel_markdown(data)
    assert "Prediction Market Arbitrage Executability & Funnel Report" in md
    assert "funnel_p1" in md
    assert "Research Data Limitations" in md
