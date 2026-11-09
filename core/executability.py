"""Executable arbitrage filtering, liquidity analysis, and opportunity evaluation.

Implements pure, stateless opportunity evaluation across three classification levels:
- Level 1: Raw pricing opportunity (best ask sum < 100)
- Level 2: Fee-adjusted opportunity (top of book net edge > E_min after exchange fees, gas, slippage)
- Level 3: Executable candidate (depth-aware fillability & VWAP edge, quote freshness & skew checks,
           tradable market status, verified pair provenance).

Fee Documentation References:
- Kalshi Taker Fees: Variable quadratic formula scaling with contract probability:
  Formula: Fee = ceil(M * 0.07 * C * P * (1 - P) * 100) cents, where P = price in dollars,
  C = contract count, M = series fee multiplier (default 1.0).
  URL: https://kalshi.com/docs/trading/fees (Official Kalshi Trading Rules & Fee Schedule).
- Polymarket Taker Fees: Dynamic quadratic fee by category:
  Formula: Fee = ceil(fee_rate * C * P * (1 - P) * 100) cents, where fee_rate is 0.0 for
  fee-free / geopolitical markets or configured category rates (e.g. 0.04 - 0.07).
  URL: https://docs.polymarket.com (Polymarket CLOB & Trading Fee Documentation).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
from enum import Enum
import math
from typing import Any, Optional, Sequence

from core.depth import Level, clean_levels, vwap_for_size

CENTS_100 = Decimal("100")
CENTS_0 = Decimal("0")
ONE_DOLLAR = Decimal("1")


class Classification(str, Enum):
    """Classification levels for an evaluated arbitrage opportunity."""

    NOT_RAW = "NOT_RAW"
    RAW_ONLY = "RAW_ONLY"
    FEE_ADJUSTED = "FEE_ADJUSTED"
    EXECUTABLE_CANDIDATE = "EXECUTABLE_CANDIDATE"


class RejectReason(str, Enum):
    """Reasons why an opportunity failed to qualify as an executable candidate.

    Priority order for selecting `primary_reject_reason`:
    1. DATA_SOURCE_NOT_RESEARCH
    2. PAIR_NOT_VERIFIED
    3. MARKET_NOT_TRADABLE
    4. MISSING_TIMESTAMP
    5. UNRELIABLE_TIMESTAMP
    6. STALE_QUOTE
    7. EXCESS_SKEW
    8. NO_RAW_EDGE
    9. INSUFFICIENT_DEPTH
    10. BELOW_MIN_EDGE_AFTER_COSTS
    """

    DATA_SOURCE_NOT_RESEARCH = "DATA_SOURCE_NOT_RESEARCH"
    PAIR_NOT_VERIFIED = "PAIR_NOT_VERIFIED"
    MARKET_NOT_TRADABLE = "MARKET_NOT_TRADABLE"
    MISSING_TIMESTAMP = "MISSING_TIMESTAMP"
    UNRELIABLE_TIMESTAMP = "UNRELIABLE_TIMESTAMP"
    STALE_QUOTE = "STALE_QUOTE"
    EXCESS_SKEW = "EXCESS_SKEW"
    NO_RAW_EDGE = "NO_RAW_EDGE"
    INSUFFICIENT_DEPTH = "INSUFFICIENT_DEPTH"
    BELOW_MIN_EDGE_AFTER_COSTS = "BELOW_MIN_EDGE_AFTER_COSTS"


REJECT_REASON_PRIORITY: list[RejectReason] = [
    RejectReason.DATA_SOURCE_NOT_RESEARCH,
    RejectReason.PAIR_NOT_VERIFIED,
    RejectReason.MARKET_NOT_TRADABLE,
    RejectReason.MISSING_TIMESTAMP,
    RejectReason.UNRELIABLE_TIMESTAMP,
    RejectReason.STALE_QUOTE,
    RejectReason.EXCESS_SKEW,
    RejectReason.NO_RAW_EDGE,
    RejectReason.INSUFFICIENT_DEPTH,
    RejectReason.BELOW_MIN_EDGE_AFTER_COSTS,
]


def calculate_kalshi_taker_fee_cents(
    price_cents: Decimal,
    contracts: Decimal,
    rate: Decimal = Decimal("0.07"),
    multiplier: Decimal = Decimal("1.0"),
) -> Decimal:
    """Calculate Kalshi taker fee in cents for an order of `contracts` at `price_cents`.

    Kalshi fee formula:
        price_dollars P = price_cents / 100
        fee_dollars = multiplier * rate * contracts * P * (1 - P)
        fee_cents = ceil(fee_dollars * 100)
    Rounds up to the next whole cent at the order level.
    """
    if contracts <= 0 or price_cents <= 0 or price_cents >= 100:
        return Decimal("0")
    p_dlr = price_cents / CENTS_100
    fee_dlr = multiplier * rate * contracts * p_dlr * (ONE_DOLLAR - p_dlr)
    fee_cents = (fee_dlr * CENTS_100).quantize(Decimal("1"), rounding=ROUND_CEILING)
    return fee_cents


def calculate_polymarket_taker_fee_cents(
    price_cents: Decimal,
    contracts: Decimal,
    rate: Decimal = Decimal("0.0"),
) -> Decimal:
    """Calculate Polymarket taker fee in cents for an order of `contracts` at `price_cents`.

    Polymarket quadratic fee formula:
        price_dollars P = price_cents / 100
        fee_dollars = rate * contracts * P * (1 - P)
        fee_cents = ceil(fee_dollars * 100)
    Rounds up to the next whole cent at the order level.
    """
    if rate <= 0 or contracts <= 0 or price_cents <= 0 or price_cents >= 100:
        return Decimal("0")
    p_dlr = price_cents / CENTS_100
    fee_dlr = rate * contracts * p_dlr * (ONE_DOLLAR - p_dlr)
    fee_cents = (fee_dlr * CENTS_100).quantize(Decimal("1"), rounding=ROUND_CEILING)
    return fee_cents


@dataclass(frozen=True)
class LegInput:
    """Input snapshot data for a single leg of an arbitrage opportunity."""

    snapshot_id: Optional[int]
    platform: str  # "kalshi" or "polymarket"
    ticker: str
    best_ask: Optional[Decimal]
    ask_ladder: list[Level]  # cleaned list of (price_cents, size_contracts)
    depth_truncated: bool
    observed_at_utc: datetime
    exchange_updated_at_utc: Optional[datetime]
    tradable: bool
    data_source: str
    is_inferred: bool = False  # e.g. no_side_derived or synthetic quote


@dataclass(frozen=True)
class OpportunityInput:
    """Input payload for a two-leg arbitrage candidate evaluation."""

    kind: str  # "intra" or "cross"
    pair_id: Optional[str]  # None for intra-market
    ticker: str  # primary ticker (or pair_id)
    direction: str  # "kalshi_intra", "kalshi_yes_polymarket_no", "polymarket_yes_kalshi_no"
    leg1: LegInput
    leg2: LegInput
    pair_verified: bool = True


@dataclass(frozen=True)
class ExecutabilityParams:
    """Configurable evaluation thresholds and cost parameters."""

    sizes: list[int] = field(default_factory=lambda: [1, 10, 50])
    min_edge_cents: Decimal = Decimal("2.0")
    tau_fresh_s: Decimal = Decimal("10.0")
    tau_skew_s: Decimal = Decimal("5.0")
    clock_tolerance_s: Decimal = Decimal("2.0")

    # Fee parameters
    kalshi_taker_rate: Decimal = Decimal("0.07")
    kalshi_multiplier: Decimal = Decimal("1.0")
    polymarket_taker_rate: Decimal = Decimal("0.0")
    polygon_gas_cents: Decimal = Decimal("0.0")  # total trade gas in cents
    slippage_cents_per_bundle: Decimal = Decimal("0.0")  # S(q) per bundle


@dataclass
class OpportunityEvaluation:
    """Complete evaluation result for an opportunity at a specific order size q."""

    kind: str
    pair_id: Optional[str]
    ticker: str
    direction: str
    size_q: int

    # Level 1
    raw_edge: Decimal
    raw_positive: bool

    # Level 2
    net_edge_top: Decimal
    fee_adjusted: bool

    # Level 3
    vwap1: Optional[Decimal]
    vwap2: Optional[Decimal]
    fees_cents: Decimal
    gas_cents_total: Decimal
    slippage_cents: Decimal
    net_edge_vwap: Optional[Decimal]

    # Timestamp & Quality Metrics
    quote_age_k: Optional[Decimal]
    quote_age_p: Optional[Decimal]
    quote_skew: Optional[Decimal]  # None if NOT_APPLICABLE (intra)
    snapshot_id_1: Optional[int]
    snapshot_id_2: Optional[int]
    data_source: str

    # Classification & Rejection
    classification: Classification
    primary_reject_reason: Optional[RejectReason]
    all_reject_reasons: list[RejectReason]
    depth_truncated: bool = False


def _to_aware_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_leg_fee(
    platform: str,
    price_cents: Decimal,
    size_q: Decimal,
    params: ExecutabilityParams,
) -> Decimal:
    """Calculate the fee for a single execution leg at the intended order size."""
    p_norm = platform.lower()
    if p_norm == "kalshi":
        return calculate_kalshi_taker_fee_cents(
            price_cents=price_cents,
            contracts=size_q,
            rate=params.kalshi_taker_rate,
            multiplier=params.kalshi_multiplier,
        )
    elif p_norm == "polymarket":
        return calculate_polymarket_taker_fee_cents(
            price_cents=price_cents,
            contracts=size_q,
            rate=params.polymarket_taker_rate,
        )
    return Decimal("0")


def evaluate_opportunity(
    opp_input: OpportunityInput,
    params: ExecutabilityParams,
    t_eval: datetime,
    size_q: int,
) -> OpportunityEvaluation:
    """Pure, stateless evaluation of an opportunity at size q at clock time t_eval.

    Evaluates Level 1 (raw), Level 2 (fee-adjusted), and Level 3 (executable candidate)
    independently and determines primary and secondary rejection reasons.
    """
    t_eval_utc = _to_aware_utc(t_eval)
    q_dec = Decimal(size_q)
    leg1 = opp_input.leg1
    leg2 = opp_input.leg2

    # Reject reasons list
    reject_reasons: list[RejectReason] = []

    # 1. Provenance and Tradability Checks
    # Only live_api (or replay of live_api data) count as research
    data_source_ok = (leg1.data_source in ("live_api", "replay")) and (leg2.data_source in ("live_api", "replay"))
    if not data_source_ok:
        reject_reasons.append(RejectReason.DATA_SOURCE_NOT_RESEARCH)

    if opp_input.kind == "cross" and not opp_input.pair_verified:
        reject_reasons.append(RejectReason.PAIR_NOT_VERIFIED)

    if not (leg1.tradable and leg2.tradable):
        reject_reasons.append(RejectReason.MARKET_NOT_TRADABLE)

    # 2. Timestamp Quality and Freshness Checks
    t1_exch = _to_aware_utc(leg1.exchange_updated_at_utc)
    t2_exch = _to_aware_utc(leg2.exchange_updated_at_utc)

    # Inferred quotes are unreliable
    if leg1.is_inferred or leg2.is_inferred:
        reject_reasons.append(RejectReason.UNRELIABLE_TIMESTAMP)

    age_k: Optional[Decimal] = None
    age_p: Optional[Decimal] = None
    skew: Optional[Decimal] = None

    if opp_input.kind == "intra":
        # Intra-market (Kalshi): both asks from same book, single exchange timestamp
        # Skew is NOT_APPLICABLE
        if t1_exch is None:
            reject_reasons.append(RejectReason.MISSING_TIMESTAMP)
        else:
            diff_s = Decimal(str((t_eval_utc - t1_exch).total_seconds()))
            age_k = diff_s
            if diff_s < -params.clock_tolerance_s:
                reject_reasons.append(RejectReason.UNRELIABLE_TIMESTAMP)
            elif diff_s > params.tau_fresh_s:
                reject_reasons.append(RejectReason.STALE_QUOTE)
    else:
        # Cross-platform: both timestamps required
        if t1_exch is None or t2_exch is None:
            reject_reasons.append(RejectReason.MISSING_TIMESTAMP)
        else:
            # Determine platform ages
            age_1 = Decimal(str((t_eval_utc - t1_exch).total_seconds()))
            age_2 = Decimal(str((t_eval_utc - t2_exch).total_seconds()))
            if leg1.platform.lower() == "kalshi":
                age_k, age_p = age_1, age_2
            else:
                age_p, age_k = age_1, age_2

            # Future timestamps beyond clock tolerance
            if age_1 < -params.clock_tolerance_s or age_2 < -params.clock_tolerance_s:
                reject_reasons.append(RejectReason.UNRELIABLE_TIMESTAMP)

            # Freshness: max(age_k, age_p) <= tau_fresh
            if max(age_1, age_2) > params.tau_fresh_s:
                reject_reasons.append(RejectReason.STALE_QUOTE)

            # Skew: |t1 - t2| <= tau_skew
            skew_val = abs(Decimal(str((t1_exch - t2_exch).total_seconds())))
            skew = skew_val
            if skew_val > params.tau_skew_s:
                reject_reasons.append(RejectReason.EXCESS_SKEW)

    # 3. Level 1: Raw Pricing Opportunity
    # raw_edge = 100 - (A1 + A2) at best asks
    a1 = leg1.best_ask
    a2 = leg2.best_ask
    if a1 is None or a2 is None or a1 <= 0 or a2 <= 0:
        raw_edge = Decimal("-100")
        raw_positive = False
    else:
        raw_edge = CENTS_100 - (a1 + a2)
        raw_positive = raw_edge > Decimal("0")

    if not raw_positive:
        reject_reasons.append(RejectReason.NO_RAW_EDGE)

    # 4. Level 2: Fee-Adjusted Opportunity (Top of book)
    # E_net_top(q) = 100 - A1 - A2 - Fees(q)/q - G(q)/q - S(q)
    gas_cents_total = params.polygon_gas_cents if (opp_input.kind == "cross" or leg1.platform.lower() == "polymarket" or leg2.platform.lower() == "polymarket") else Decimal("0")
    gas_per_bundle = gas_cents_total / q_dec
    slippage_per_bundle = params.slippage_cents_per_bundle

    if a1 is not None and a2 is not None and a1 > 0 and a2 > 0:
        fee_leg1_top = compute_leg_fee(leg1.platform, a1, q_dec, params)
        fee_leg2_top = compute_leg_fee(leg2.platform, a2, q_dec, params)
        total_fees_top = fee_leg1_top + fee_leg2_top
        net_edge_top = CENTS_100 - a1 - a2 - (total_fees_top / q_dec) - gas_per_bundle - slippage_per_bundle
    else:
        net_edge_top = Decimal("-100")
        total_fees_top = Decimal("0")

    fee_adjusted = net_edge_top > params.min_edge_cents

    # 5. Level 3: Depth & Executable Candidate
    # VWAP and fillability from stored depth levels
    vwap1, filled1, fully_filled1 = vwap_for_size(leg1.ask_ladder, q_dec)
    vwap2, filled2, fully_filled2 = vwap_for_size(leg2.ask_ladder, q_dec)

    any_truncated = leg1.depth_truncated or leg2.depth_truncated
    # If depth was truncated and book could not fully fill q, treat as INSUFFICIENT_DEPTH
    if not (fully_filled1 and fully_filled2):
        reject_reasons.append(RejectReason.INSUFFICIENT_DEPTH)

    net_edge_vwap: Optional[Decimal] = None
    fees_cents: Decimal = Decimal("0")

    if fully_filled1 and fully_filled2 and vwap1 is not None and vwap2 is not None:
        fee_leg1_vwap = compute_leg_fee(leg1.platform, vwap1, q_dec, params)
        fee_leg2_vwap = compute_leg_fee(leg2.platform, vwap2, q_dec, params)
        fees_cents = fee_leg1_vwap + fee_leg2_vwap
        net_edge_vwap = CENTS_100 - vwap1 - vwap2 - (fees_cents / q_dec) - gas_per_bundle - slippage_per_bundle
        if net_edge_vwap <= params.min_edge_cents:
            reject_reasons.append(RejectReason.BELOW_MIN_EDGE_AFTER_COSTS)
    else:
        # If not fully filled, we cannot compute complete depth edge
        if RejectReason.BELOW_MIN_EDGE_AFTER_COSTS not in reject_reasons and not fee_adjusted:
            reject_reasons.append(RejectReason.BELOW_MIN_EDGE_AFTER_COSTS)

    # 6. Primary Reject Reason Determination
    # Deduplicate while preserving order of occurrence
    ordered_rejects: list[RejectReason] = []
    for r in REJECT_REASON_PRIORITY:
        if r in reject_reasons and r not in ordered_rejects:
            ordered_rejects.append(r)

    primary_reject = ordered_rejects[0] if ordered_rejects else None

    # 7. Classification Assignment
    if not raw_positive:
        classification = Classification.NOT_RAW
    elif not fee_adjusted:
        classification = Classification.RAW_ONLY
    elif len(ordered_rejects) > 0:
        classification = Classification.FEE_ADJUSTED
    else:
        classification = Classification.EXECUTABLE_CANDIDATE

    return OpportunityEvaluation(
        kind=opp_input.kind,
        pair_id=opp_input.pair_id,
        ticker=opp_input.ticker,
        direction=opp_input.direction,
        size_q=size_q,
        raw_edge=raw_edge,
        raw_positive=raw_positive,
        net_edge_top=net_edge_top,
        fee_adjusted=fee_adjusted,
        vwap1=vwap1,
        vwap2=vwap2,
        fees_cents=fees_cents,
        gas_cents_total=gas_cents_total,
        slippage_cents=slippage_per_bundle,
        net_edge_vwap=net_edge_vwap,
        quote_age_k=age_k,
        quote_age_p=age_p,
        quote_skew=skew,
        snapshot_id_1=leg1.snapshot_id,
        snapshot_id_2=leg2.snapshot_id,
        data_source=leg1.data_source,
        classification=classification,
        primary_reject_reason=primary_reject,
        all_reject_reasons=ordered_rejects,
        depth_truncated=any_truncated,
    )


# ============================================================================
# Episode Tracking and Latency Survival
# ============================================================================


class LatencyStatus(str, Enum):
    """Execution latency survival classification."""

    LATENCY_SATISFIED = "LATENCY_SATISFIED"
    LATENCY_FAILED = "LATENCY_FAILED"
    LATENCY_UNRESOLVED = "LATENCY_UNRESOLVED"


@dataclass
class Episode:
    """A run of consecutive observations where a given classification level holds."""

    episode_id: str
    kind: str
    pair_or_ticker: str
    direction: str
    size_q: int
    level: str  # "raw", "fee_adjusted", "executable"
    first_seen_utc: datetime
    last_seen_utc: datetime
    n_observations: int = 1
    peak_edge: Decimal = Decimal("0")
    mean_edge: Decimal = Decimal("0")
    _edge_sum: Decimal = Decimal("0")

    duration_lower_s: Decimal = Decimal("0")
    duration_upper_s: Optional[Decimal] = None
    censored_left: bool = False
    censored_right: bool = False
    is_active: bool = True

    # Secondary measure tracking: first snapshot at or after t0 + L
    # Map L -> edge_after_latency
    edge_after_latency: dict[float, Optional[Decimal]] = field(default_factory=dict)
    _t0: datetime = field(init=False)

    def __post_init__(self) -> None:
        self._t0 = self.first_seen_utc
        self._edge_sum = self.peak_edge
        self.mean_edge = self.peak_edge

    def record_update(self, t_obs: datetime, edge: Decimal) -> None:
        """Update active episode with consecutive passing observation."""
        self.last_seen_utc = t_obs
        self.n_observations += 1
        self._edge_sum += edge
        self.mean_edge = self._edge_sum / Decimal(self.n_observations)
        if edge > self.peak_edge:
            self.peak_edge = edge
        self.duration_lower_s = Decimal(str((self.last_seen_utc - self.first_seen_utc).total_seconds()))

    def close(self, t_fail: Optional[datetime], censored_by_gap: bool = False) -> None:
        """Close episode upon condition failure or data gap."""
        self.is_active = False
        self.duration_lower_s = Decimal(str((self.last_seen_utc - self.first_seen_utc).total_seconds()))
        if censored_by_gap or t_fail is None:
            self.censored_right = True
            self.duration_upper_s = None
        else:
            self.duration_upper_s = Decimal(str((t_fail - self.first_seen_utc).total_seconds()))


class EpisodeTracker:
    """Stateful tracker for opportunity episodes across raw, fee-adjusted, and executable levels.

    Maintains active episodes, detects closures via failing observations or data gaps,
    and evaluates latency survival for configured latencies L.
    """

    def __init__(
        self,
        sampling_interval_s: float = 60.0,
        episode_gap_tolerance: float = 1.5,
        latencies_s: Optional[Sequence[float]] = None,
    ) -> None:
        self.sampling_interval_s = sampling_interval_s
        self.episode_gap_tolerance = episode_gap_tolerance
        self.max_gap_s = sampling_interval_s * episode_gap_tolerance
        self.latencies_s = list(latencies_s) if latencies_s is not None else [1.0, 2.0, 5.0, 10.0, 30.0]

        # Key -> active Episode: key = (kind, pair_or_ticker, direction, size_q, level)
        self.active_episodes: dict[tuple[str, str, str, int, str], Episode] = {}
        self.completed_episodes: list[Episode] = []
        self._episode_seq = 0
        self._first_observation = True

    def _next_episode_id(self) -> str:
        self._episode_seq += 1
        return f"ep_{self._episode_seq}"

    def step(
        self,
        t_eval: datetime,
        evaluations: Sequence[OpportunityEvaluation],
    ) -> None:
        """Process evaluations for a single snapshot timestamp.

        Tracks condition holds for each evaluation across three levels:
        - "raw": raw_positive is True
        - "fee_adjusted": fee_adjusted is True
        - "executable": classification == EXECUTABLE_CANDIDATE
        """
        t_utc = _to_aware_utc(t_eval)

        # Build condition map for current observations:
        # key = (kind, pair_or_ticker, direction, size_q, level) -> (holds: bool, edge: Decimal)
        current_states: dict[tuple[str, str, str, int, str], tuple[bool, Decimal]] = {}

        for ev in evaluations:
            key_id = ev.pair_id or ev.ticker
            base = (ev.kind, key_id, ev.direction, ev.size_q)

            # Raw level
            current_states[(*base, "raw")] = (ev.raw_positive, ev.raw_edge)

            # Fee-adjusted level
            current_states[(*base, "fee_adjusted")] = (ev.fee_adjusted, ev.net_edge_top)

            # Executable level
            is_exec = ev.classification == Classification.EXECUTABLE_CANDIDATE
            exec_edge = ev.net_edge_vwap if ev.net_edge_vwap is not None else Decimal("0")
            current_states[(*base, "executable")] = (is_exec, exec_edge)

        # 1. Update / close existing active episodes
        active_keys = list(self.active_episodes.keys())
        for k in active_keys:
            ep = self.active_episodes[k]
            gap = (t_utc - ep.last_seen_utc).total_seconds()

            if k in current_states:
                holds, edge = current_states[k]
                if gap > self.max_gap_s:
                    # Data gap ended the episode
                    ep.close(t_fail=None, censored_by_gap=True)
                    self.completed_episodes.append(ep)
                    del self.active_episodes[k]

                    # If it currently holds, start a new episode
                    if holds:
                        new_ep = Episode(
                            episode_id=self._next_episode_id(),
                            kind=k[0],
                            pair_or_ticker=k[1],
                            direction=k[2],
                            size_q=k[3],
                            level=k[4],
                            first_seen_utc=t_utc,
                            last_seen_utc=t_utc,
                            peak_edge=edge,
                            censored_left=False,
                        )
                        self.active_episodes[k] = new_ep
                elif holds:
                    # Continues episode
                    ep.record_update(t_utc, edge)
                    self._check_edge_after_latency(ep, t_utc, edge)
                else:
                    # Condition failed
                    ep.close(t_fail=t_utc, censored_by_gap=False)
                    self.completed_episodes.append(ep)
                    del self.active_episodes[k]
            else:
                # Key not present in evaluations
                if gap > self.max_gap_s:
                    ep.close(t_fail=None, censored_by_gap=True)
                else:
                    ep.close(t_fail=t_utc, censored_by_gap=False)
                self.completed_episodes.append(ep)
                del self.active_episodes[k]

        # 2. Open new episodes for newly passing conditions
        for k, (holds, edge) in current_states.items():
            if holds and k not in self.active_episodes:
                new_ep = Episode(
                    episode_id=self._next_episode_id(),
                    kind=k[0],
                    pair_or_ticker=k[1],
                    direction=k[2],
                    size_q=k[3],
                    level=k[4],
                    first_seen_utc=t_utc,
                    last_seen_utc=t_utc,
                    peak_edge=edge,
                    censored_left=self._first_observation,
                )
                self.active_episodes[k] = new_ep

        self._first_observation = False

    def _check_edge_after_latency(self, ep: Episode, t_current: datetime, current_edge: Decimal) -> None:
        """Record edge_after_latency at first snapshot at or after t0 + L within 2x sampling interval."""
        dt_s = (t_current - ep._t0).total_seconds()
        for L in self.latencies_s:
            if L not in ep.edge_after_latency:
                if dt_s >= L:
                    # Check if within 2x sampling interval of t0 + L
                    if dt_s <= L + 2.0 * self.sampling_interval_s:
                        ep.edge_after_latency[L] = current_edge
                    else:
                        ep.edge_after_latency[L] = None

    def finalize(self) -> list[Episode]:
        """Close any remaining active episodes as right-censored and return all episodes."""
        for ep in self.active_episodes.values():
            ep.close(t_fail=None, censored_by_gap=True)
            self.completed_episodes.append(ep)
        self.active_episodes.clear()
        return list(self.completed_episodes)

    def evaluate_latency_status(self, episode: Episode, latency_s: float) -> tuple[LatencyStatus, Optional[Decimal]]:
        """Evaluate latency survival status for a specific latency L (seconds).

        Rules:
        - LATENCY_SATISFIED if duration_lower_s >= L
        - LATENCY_FAILED if duration_upper_s is known and < L
        - LATENCY_UNRESOLVED otherwise
        """
        l_dec = Decimal(str(latency_s))
        if episode.duration_lower_s >= l_dec:
            status = LatencyStatus.LATENCY_SATISFIED
        elif episode.duration_upper_s is not None and episode.duration_upper_s < l_dec:
            status = LatencyStatus.LATENCY_FAILED
        else:
            status = LatencyStatus.LATENCY_UNRESOLVED

        edge_l = episode.edge_after_latency.get(latency_s)
        return status, edge_l
