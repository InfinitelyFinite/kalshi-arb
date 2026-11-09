"""Unit tests for verified market pair registry and validation models."""

from datetime import date, datetime, timezone
from pathlib import Path
import pytest
import yaml

from core.pair_registry import (
    CHECKLIST_KEYS,
    ChecklistItem,
    MarketPairOverride,
    PairIssue,
    PairRegistryError,
    RegistryThresholds,
    VerificationRecord,
    load_pair_overrides,
    parse_pair_registry,
)


def _valid_verification_data() -> dict:
    checklist = {
        key: {"pass": True, "note": f"Verification note for {key}"}
        for key in CHECKLIST_KEYS
    }
    return {
        "verified_by": "researcher_1",
        "verified_at": "2026-11-01",
        "kalshi_rules_url": "https://kalshi.com/markets/pres-2028",
        "polymarket_rules_url": "https://polymarket.com/event/presidential-election-2028",
        "kalshi_expected_close_utc": "2028-11-07T05:00:00Z",
        "polymarket_expected_end_utc": "2028-11-07T05:00:00Z",
        "checklist": checklist,
    }


def test_excluded_pair_valid():
    pair = MarketPairOverride(
        pair_id="pres-2028-candidate-1",
        category="politics",
        status="excluded",
        exclusion_reason="Settlement criteria diverge between platforms",
    )
    assert pair.pair_id == "pres-2028-candidate-1"
    assert pair.status == "excluded"
    assert pair.is_verified is False
    assert pair.exclusion_reason == "Settlement criteria diverge between platforms"


def test_excluded_pair_missing_reason_rejected():
    with pytest.raises(ValueError, match="status 'excluded' requires a non-empty exclusion_reason"):
        MarketPairOverride(
            pair_id="pres-2028-candidate-2",
            category="politics",
            status="excluded",
            exclusion_reason="",
        )


def test_verified_pair_valid():
    v_data = _valid_verification_data()
    pair = MarketPairOverride(
        pair_id="pres-2028-harris",
        category="politics",
        kalshi_ticker="KXPRES-2028-HAR",
        polymarket_condition_id="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        polymarket_yes_token_id="11111111",
        polymarket_no_token_id="22222222",
        status="verified",
        verification=VerificationRecord.model_validate(v_data),
    )
    assert pair.is_verified is True
    assert pair.verification is not None
    assert pair.verification.verified_by == "researcher_1"
    assert len(pair.verification.checklist) == 8


def test_verified_pair_missing_tokens_rejected():
    v_data = _valid_verification_data()
    with pytest.raises(ValueError, match="missing polymarket_yes_token_id"):
        MarketPairOverride(
            pair_id="pres-2028-harris",
            category="politics",
            kalshi_ticker="KXPRES-2028-HAR",
            polymarket_condition_id="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            status="verified",
            verification=VerificationRecord.model_validate(v_data),
        )


def test_verified_pair_non_https_url_rejected():
    v_data = _valid_verification_data()
    v_data["kalshi_rules_url"] = "http://insecure-kalshi.com"
    with pytest.raises(ValueError, match="kalshi_rules_url must be a valid https URL"):
        MarketPairOverride(
            pair_id="pres-2028-harris",
            category="politics",
            kalshi_ticker="KXPRES-2028-HAR",
            polymarket_condition_id="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            polymarket_yes_token_id="111",
            polymarket_no_token_id="222",
            status="verified",
            verification=VerificationRecord.model_validate(v_data),
        )


def test_verified_pair_missing_checklist_item_rejected():
    v_data = _valid_verification_data()
    del v_data["checklist"]["C1_event_definition"]
    with pytest.raises(ValueError, match="checklist item C1_event_definition is missing"):
        MarketPairOverride(
            pair_id="pres-2028-harris",
            category="politics",
            kalshi_ticker="KXPRES-2028-HAR",
            polymarket_condition_id="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            polymarket_yes_token_id="111",
            polymarket_no_token_id="222",
            status="verified",
            verification=VerificationRecord.model_validate(v_data),
        )


def test_verified_pair_checklist_item_not_passed_rejected():
    v_data = _valid_verification_data()
    v_data["checklist"]["C5_timing"]["pass"] = False
    with pytest.raises(ValueError, match="checklist item C5_timing has pass != true"):
        MarketPairOverride(
            pair_id="pres-2028-harris",
            category="politics",
            kalshi_ticker="KXPRES-2028-HAR",
            polymarket_condition_id="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            polymarket_yes_token_id="111",
            polymarket_no_token_id="222",
            status="verified",
            verification=VerificationRecord.model_validate(v_data),
        )


def test_verified_pair_checklist_item_empty_note_rejected():
    v_data = _valid_verification_data()
    v_data["checklist"]["C2_resolution_source"]["note"] = "   "
    with pytest.raises(ValueError, match="checklist item C2_resolution_source has an empty note"):
        MarketPairOverride(
            pair_id="pres-2028-harris",
            category="politics",
            kalshi_ticker="KXPRES-2028-HAR",
            polymarket_condition_id="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            polymarket_yes_token_id="111",
            polymarket_no_token_id="222",
            status="verified",
            verification=VerificationRecord.model_validate(v_data),
        )


def test_parse_pair_registry_lenient(tmp_path: Path):
    yaml_content = {
        "pairs": [
            {
                "pair_id": "pair-1",
                "category": "politics",
                "status": "excluded",
                "exclusion_reason": "Not comparable",
            },
            {
                "pair_id": "pair-bad",
                "category": "politics",
                "status": "excluded",
                # missing exclusion_reason -> error
            },
            {
                "pair_id": "pair-1",  # duplicate pair_id
                "category": "sports",
                "status": "excluded",
                "exclusion_reason": "Duplicate",
            },
        ]
    }
    file_path = tmp_path / "test_overrides.yaml"
    file_path.write_text(yaml.dump(yaml_content))

    result = parse_pair_registry(file_path)
    assert len(result.pairs) == 1
    assert result.pairs[0].pair_id == "pair-1"
    assert len(result.issues) == 2
    issue_pids = [iss.pair_id for iss in result.issues]
    assert "pair-bad" in issue_pids
    assert "pair-1" in issue_pids


def test_parse_pair_registry_duplicate_verified_market(tmp_path: Path):
    v_data = _valid_verification_data()
    yaml_content = {
        "pairs": [
            {
                "pair_id": "pair-v1",
                "category": "politics",
                "kalshi_ticker": "KXTEST-SAME",
                "polymarket_condition_id": "0xaaa",
                "polymarket_yes_token_id": "111",
                "polymarket_no_token_id": "222",
                "status": "verified",
                "verification": v_data,
            },
            {
                "pair_id": "pair-v2",
                "category": "politics",
                "kalshi_ticker": "KXTEST-SAME",  # shared Kalshi ticker
                "polymarket_condition_id": "0xbbb",
                "polymarket_yes_token_id": "333",
                "polymarket_no_token_id": "444",
                "status": "verified",
                "verification": v_data,
            },
        ]
    }
    file_path = tmp_path / "test_duplicate.yaml"
    file_path.write_text(yaml.dump(yaml_content))

    result = parse_pair_registry(file_path)
    assert any("already used by verified pair" in iss.message for iss in result.issues)


def test_load_pair_overrides_strict_mode(tmp_path: Path):
    yaml_content = {
        "pairs": [
            {
                "pair_id": "pair-1",
                "category": "politics",
                "status": "excluded",
                "exclusion_reason": "Not verified yet",
            }
        ]
    }
    file_path = tmp_path / "test_strict.yaml"
    file_path.write_text(yaml.dump(yaml_content))

    # Strict mode enforces bounds: 0 verified pairs < min_verified_pairs (10)
    with pytest.raises(PairRegistryError, match="verified pairs < min_verified_pairs"):
        load_pair_overrides(file_path, strict=True)

    # Non-strict mode logs warnings instead of raising
    all_pairs = load_pair_overrides(file_path, strict=False, include_excluded=True)
    assert len(all_pairs) == 1
    assert all_pairs[0].pair_id == "pair-1"
