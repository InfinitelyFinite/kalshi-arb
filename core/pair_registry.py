"""Verified market pair registry: pydantic models and loader for ``config/market_overrides.yaml``.

Each YAML entry is one Kalshi <-> Polymarket pair. A pair only enters the research dataset when
``status: verified`` AND its human verification record is complete (see ``MarketPairOverride``).
This module never sets ``status`` itself: verification is a human task.

Checklist keys (spec order C1..C8): event definition, resolution source, thresholds, measurement, timing,
outcomes, edge cases, tradability. (The progress report was not available in the repo; the names follow the spec.)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, Optional, Union
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

logger = logging.getLogger(__name__)

CHECKLIST_KEYS: tuple[str, ...] = (
    "C1_event_definition",
    "C2_resolution_source",
    "C3_thresholds",
    "C4_measurement",
    "C5_timing",
    "C6_outcomes",
    "C7_edge_cases",
    "C8_tradability",
)

DEFAULT_OVERRIDES_PATH = "config/market_overrides.yaml"


class PairRegistryError(ValueError):
    """Raised when the pair registry is invalid. ``issues`` lists every problem with its pair_id."""

    def __init__(self, issues: list["PairIssue"]) -> None:
        self.issues = issues
        super().__init__("Invalid market pair registry:\n" + "\n".join(f"  - [{i.pair_id}] {i.message}" for i in issues))


@dataclass(frozen=True)
class PairIssue:
    pair_id: str
    message: str
    reason_code: str = "SCHEMA_INVALID"


class ChecklistItem(BaseModel):
    """One comparability check (C1..C8) with a human-written justification."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    passed: bool = Field(alias="pass", description="True only if the two contracts are equivalent on this dimension")
    note: str = Field(default="", description="Evidence/justification for the verdict")


class VerificationRecord(BaseModel):
    """Evidence that two contracts are comparable. Filled in by a human."""

    model_config = ConfigDict(extra="forbid")

    verified_by: Optional[str] = None
    verified_at: Optional[Union[datetime, date]] = None
    kalshi_rules_url: Optional[str] = None
    polymarket_rules_url: Optional[str] = None
    kalshi_expected_close_utc: Optional[datetime] = None
    polymarket_expected_end_utc: Optional[datetime] = None
    checklist: dict[str, ChecklistItem] = Field(default_factory=dict)


def _is_https_url(value: Optional[str]) -> bool:
    if not value or not isinstance(value, str):
        return False
    parsed = urlparse(value.strip())
    return parsed.scheme == "https" and bool(parsed.netloc) and " " not in value.strip()


class MarketPairOverride(BaseModel):
    """One verified (or excluded) Kalshi/Polymarket pair."""

    model_config = ConfigDict(extra="forbid")

    pair_id: str
    category: str
    kalshi_ticker: Optional[str] = None
    polymarket_condition_id: Optional[str] = None
    polymarket_yes_token_id: Optional[str] = None
    polymarket_no_token_id: Optional[str] = None
    status: Literal["verified", "excluded"]
    exclusion_reason: Optional[str] = None
    notes: Optional[str] = None
    verification: Optional[VerificationRecord] = None

    @field_validator("pair_id", "category")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @model_validator(mode="after")
    def _validate_status_rules(self) -> "MarketPairOverride":
        pid = self.pair_id
        if self.status == "excluded":
            if not (self.exclusion_reason and self.exclusion_reason.strip()):
                raise ValueError(f"pair '{pid}': status 'excluded' requires a non-empty exclusion_reason")
            return self

        # status == verified
        problems: list[str] = []
        for name in (
            "kalshi_ticker",
            "polymarket_condition_id",
            "polymarket_yes_token_id",
            "polymarket_no_token_id",
        ):
            if not getattr(self, name):
                problems.append(f"missing {name}")
        if self.exclusion_reason:
            problems.append("exclusion_reason must be empty for a verified pair")

        v = self.verification
        if v is None:
            problems.append("missing verification block")
        else:
            if not (v.verified_by and v.verified_by.strip()):
                problems.append("verification.verified_by is missing")
            if v.verified_at is None:
                problems.append("verification.verified_at is missing")
            if not _is_https_url(v.kalshi_rules_url):
                problems.append(f"verification.kalshi_rules_url must be a valid https URL (got {v.kalshi_rules_url!r})")
            if not _is_https_url(v.polymarket_rules_url):
                problems.append(
                    f"verification.polymarket_rules_url must be a valid https URL (got {v.polymarket_rules_url!r})"
                )
            if v.kalshi_expected_close_utc is None:
                problems.append("verification.kalshi_expected_close_utc is missing")
            if v.polymarket_expected_end_utc is None:
                problems.append("verification.polymarket_expected_end_utc is missing")
            for key in CHECKLIST_KEYS:
                item = v.checklist.get(key)
                if item is None:
                    problems.append(f"checklist item {key} is missing")
                elif not item.passed:
                    problems.append(f"checklist item {key} has pass != true")
                elif not item.note.strip():
                    problems.append(f"checklist item {key} has an empty note")
            unknown = sorted(set(v.checklist) - set(CHECKLIST_KEYS))
            if unknown:
                problems.append(f"unknown checklist keys: {unknown}")

        if problems:
            raise ValueError(f"pair '{pid}': cannot be verified: " + "; ".join(problems))
        return self

    @property
    def is_verified(self) -> bool:
        return self.status == "verified"


@dataclass
class RegistryThresholds:
    """Bounds on the verified set; errors in strict mode, warnings otherwise."""

    min_verified_pairs: int = 10
    max_verified_pairs: int = 20
    min_categories: int = 3

    @classmethod
    def from_config(cls, config: Any) -> "RegistryThresholds":
        section = getattr(config, "pair_registry", None)
        if section is None:
            return cls()
        return cls(
            min_verified_pairs=section.min_verified_pairs,
            max_verified_pairs=section.max_verified_pairs,
            min_categories=section.min_categories,
        )


@dataclass
class RegistryLoadResult:
    """Lenient parse result: valid pairs plus per-pair problems (used by the validation script)."""

    pairs: list[MarketPairOverride] = field(default_factory=list)
    issues: list[PairIssue] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def verified(self) -> list[MarketPairOverride]:
        return [p for p in self.pairs if p.is_verified]

    @property
    def categories(self) -> set[str]:
        return {p.category for p in self.verified}


def _entry_label(entry: Any, index: int) -> str:
    if isinstance(entry, dict) and entry.get("pair_id"):
        return str(entry["pair_id"])
    return f"entry#{index}"


def _read_entries(path: Path) -> list[Any]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        entries = raw.get("pairs", raw.get("overrides", []))
        if entries is None:
            return []
        if not isinstance(entries, list):
            raise PairRegistryError([PairIssue("<file>", f"'pairs' must be a list, got {type(entries).__name__}")])
        return entries
    raise PairRegistryError([PairIssue("<file>", f"expected a list or mapping at top level, got {type(raw).__name__}")])


def parse_pair_registry(
    path: Union[str, Path] = DEFAULT_OVERRIDES_PATH,
    thresholds: Optional[RegistryThresholds] = None,
) -> RegistryLoadResult:
    """Parse the registry leniently: invalid entries become ``issues`` instead of aborting.

    Cross-entry rules (unique pair_id; no ticker/condition shared by two verified pairs) are also reported as issues.
    Bound violations (pair/category counts) are reported in ``warnings``.
    """
    result = RegistryLoadResult()
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Market overrides file not found: {p}")

    try:
        entries = _read_entries(p)
    except yaml.YAMLError as e:
        raise PairRegistryError([PairIssue("<file>", f"YAML parse error: {e}")]) from e

    seen_ids: set[str] = set()
    for idx, entry in enumerate(entries):
        label = _entry_label(entry, idx)
        if not isinstance(entry, dict):
            result.issues.append(PairIssue(label, "entry is not a mapping"))
            continue
        try:
            pair = MarketPairOverride.model_validate(entry)
        except ValidationError as e:
            for err in e.errors():
                loc = ".".join(str(x) for x in err["loc"])
                msg = err["msg"].removeprefix("Value error, ")
                result.issues.append(PairIssue(label, f"{loc + ': ' if loc else ''}{msg}"))
            continue

        if pair.pair_id in seen_ids:
            result.issues.append(PairIssue(pair.pair_id, "duplicate pair_id"))
            continue
        seen_ids.add(pair.pair_id)
        result.pairs.append(pair)

    # Verified pairs must not share a Kalshi ticker or a Polymarket condition id.
    kalshi_owner: dict[str, str] = {}
    poly_owner: dict[str, str] = {}
    for pair in result.verified:
        if pair.kalshi_ticker:
            other = kalshi_owner.get(pair.kalshi_ticker)
            if other:
                result.issues.append(
                    PairIssue(pair.pair_id, f"kalshi_ticker {pair.kalshi_ticker} already used by verified pair '{other}'")
                )
            else:
                kalshi_owner[pair.kalshi_ticker] = pair.pair_id
        if pair.polymarket_condition_id:
            key = pair.polymarket_condition_id.lower()
            other = poly_owner.get(key)
            if other:
                result.issues.append(
                    PairIssue(
                        pair.pair_id,
                        f"polymarket_condition_id {pair.polymarket_condition_id} already used by verified pair '{other}'",
                    )
                )
            else:
                poly_owner[key] = pair.pair_id

    th = thresholds or RegistryThresholds()
    n_verified, n_categories = len(result.verified), len(result.categories)
    if n_verified < th.min_verified_pairs:
        result.warnings.append(f"{n_verified} verified pairs < min_verified_pairs={th.min_verified_pairs}")
    if n_verified > th.max_verified_pairs:
        result.warnings.append(f"{n_verified} verified pairs > max_verified_pairs={th.max_verified_pairs}")
    if n_categories < th.min_categories:
        result.warnings.append(f"{n_categories} categories < min_categories={th.min_categories}")
    return result


def load_pair_overrides(
    path: Union[str, Path] = DEFAULT_OVERRIDES_PATH,
    *,
    include_excluded: bool = False,
    strict: bool = False,
    thresholds: Optional[RegistryThresholds] = None,
) -> list[MarketPairOverride]:
    """Load the registry, failing loudly (``PairRegistryError``) on any schema or cross-entry problem.

    Args:
        path: YAML file path.
        include_excluded: Also return ``status: excluded`` pairs (for reporting). Default returns verified only.
        strict: Enforce verified-pair count and category bounds as errors (otherwise logged as warnings).
        thresholds: Bounds (defaults to 10..20 pairs, 3 categories).
    """
    result = parse_pair_registry(path, thresholds)
    if result.issues:
        raise PairRegistryError(result.issues)
    if result.warnings:
        if strict:
            raise PairRegistryError([PairIssue("<registry>", w, "BOUNDS") for w in result.warnings])
        for w in result.warnings:
            logger.warning("Pair registry: %s", w)
    return list(result.pairs) if include_excluded else result.verified
