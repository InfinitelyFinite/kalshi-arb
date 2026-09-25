"""Unit tests for core.market_matcher."""

from pathlib import Path
import tempfile
import pytest
import yaml

from core.config_loader import AppConfig, MatchingConfig, ModeConfig, KalshiConfig, PolymarketConfig, TradingConfig, RiskConfig
from core.market_matcher import (
    BaseSimilarityScorer,
    CallableSimilarityScorer,
    MarketMatch,
    MarketMatcher,
    MarketOverride,
    RapidFuzzTokenSortScorer,
    load_market_overrides,
)
from kalshi_client.models import Market as KalshiMarket
from polymarket_client.models import Market as PolyMarket


# ============================================================================
# Test Fixtures & Sample Datasets
# ============================================================================


@pytest.fixture
def fake_kalshi_markets() -> list[dict[str, str]]:
    return [
        {
            "ticker": "FED-26MAY-CUT",
            "title": "Federal Reserve interest rate cut in May 2026",
        },
        {
            "ticker": "BTC-100K-2026",
            "title": "Bitcoin reaches $100k in 2026",
        },
        {
            "ticker": "OSCARS-BESTPIC-2026",
            "title": "Oscars 2026 Best Picture Winner",
        },
        {
            "ticker": "PRES-2028-DEM",
            "title": "Democratic Nominee 2028 US Presidential Election",
        },
        {
            "ticker": "CPI-26APR-T30",
            "title": "US CPI annual inflation rate 3.0% or higher in April 2026",
        },
        {
            "ticker": "UNMATCHED-KALSHI",
            "title": "Will SpaceX land Starship on Mars by 2027?",
        },
    ]


@pytest.fixture
def fake_polymarket_markets() -> list[dict[str, str]]:
    return [
        # Phrased similarly to FED-26MAY-CUT
        {
            "condition_id": "0xfed_may_2026_cut",
            "title": "Will the Federal Reserve cut interest rates in May 2026?",
        },
        # Phrased similarly to BTC-100K-2026 (token reordering)
        {
            "condition_id": "0xbtc_100k_2026",
            "title": "In 2026 Bitcoin reaches $100k",
        },
        # Phrased similarly to PRES-2028-DEM
        {
            "condition_id": "0xpres_2028_dem_nom",
            "title": "2028 US Presidential Election Democratic Nominee",
        },
        # Different event entirely
        {
            "condition_id": "0xunrelated_weather",
            "title": "Will it snow in Central Park on Christmas Day 2026?",
        },
    ]


# ============================================================================
# RapidFuzz Similarity Scorer Tests
# ============================================================================


def test_rapidfuzz_scorer_same_event_different_phrasing():
    """Verify that same-event titles score high similarity with token_sort_ratio."""
    scorer = RapidFuzzTokenSortScorer()

    # Reordered words and minor punctuation variations
    s1 = scorer.score(
        "Will the Federal Reserve cut interest rates in May 2026?",
        "Federal Reserve interest rate cut in May 2026?",
    )
    assert s1 > 0.80

    # Token sort reordering
    s2 = scorer.score(
        "Bitcoin reaches $100k in 2026",
        "In 2026 Bitcoin reaches $100k",
    )
    assert s2 == 1.0


def test_rapidfuzz_scorer_obviously_different_events():
    """Verify that clearly distinct event titles receive low similarity scores."""
    scorer = RapidFuzzTokenSortScorer()

    score = scorer.score(
        "Will the Federal Reserve cut interest rates in May 2026?",
        "Oscars 2026 Best Picture Winner",
    )
    assert score < 0.50

    score_empty = scorer.score("", "Some Market Title")
    assert score_empty == 0.0


# ============================================================================
# MarketMatcher Core Matching Tests
# ============================================================================


def test_market_matcher_fuzzy_match_and_threshold(fake_kalshi_markets, fake_polymarket_markets):
    """Verify that matches above min_similarity are found and non-matches are excluded."""
    matcher = MarketMatcher(min_similarity=0.75, overrides=[])
    matches = matcher.match(fake_kalshi_markets, fake_polymarket_markets)

    assert len(matches) == 3

    matched_k_tickers = {m.kalshi_ticker for m in matches}
    assert "FED-26MAY-CUT" in matched_k_tickers
    assert "BTC-100K-2026" in matched_k_tickers
    assert "PRES-2028-DEM" in matched_k_tickers

    # Unmatched markets should not be paired
    assert "UNMATCHED-KALSHI" not in matched_k_tickers
    for m in matches:
        assert m.polymarket_condition_id != "0xunrelated_weather"
        assert m.confidence >= 0.75
        assert m.match_source == "fuzzy"


def test_market_matcher_high_threshold_filters_out_weak_matches(fake_kalshi_markets, fake_polymarket_markets):
    """Verify that a strict min_similarity threshold (e.g. 0.95) excludes loose phrasing."""
    strict_matcher = MarketMatcher(min_similarity=0.95, overrides=[])
    matches = strict_matcher.match(fake_kalshi_markets, fake_polymarket_markets)

    for m in matches:
        assert m.confidence >= 0.95


def test_market_matcher_sorting_descending(fake_kalshi_markets, fake_polymarket_markets):
    """Verify that matches are strictly returned sorted descending by confidence."""
    matcher = MarketMatcher(min_similarity=0.50, overrides=[])
    matches = matcher.match(fake_kalshi_markets, fake_polymarket_markets)

    confidences = [m.confidence for m in matches]
    assert confidences == sorted(confidences, reverse=True)


# ============================================================================
# Manual Override Tests
# ============================================================================


def test_manual_overrides_bypass_fuzzy_matching_with_confidence_1():
    """Verify that manual overrides take precedence with confidence=1.0 even if titles differ."""
    k_markets = [
        {"ticker": "KX-ODD-1", "title": "Totally Disparate Kalshi Title ABC"},
        {"ticker": "KX-REGULAR", "title": "Federal Reserve Rate Cut May 2026"},
    ]
    p_markets = [
        {"condition_id": "0xpoly_odd_1", "title": "Completely Unrelated Poly Question XYZ"},
        {"condition_id": "0xpoly_regular", "title": "Federal Reserve Rate Cut May 2026"},
    ]

    overrides = [
        MarketOverride(
            kalshi_ticker="KX-ODD-1",
            polymarket_condition_id="0xpoly_odd_1",
            notes="Manually confirmed arbitrage event despite different titles",
        )
    ]

    matcher = MarketMatcher(min_similarity=0.85, overrides=overrides)
    matches = matcher.match(k_markets, p_markets)

    assert len(matches) == 2

    # First match should be the override with confidence 1.0
    override_match = matches[0]
    assert override_match.kalshi_ticker == "KX-ODD-1"
    assert override_match.polymarket_condition_id == "0xpoly_odd_1"
    assert override_match.confidence == 1.0
    assert override_match.match_source == "override"
    assert override_match.is_override is True
    assert override_match.notes == "Manually confirmed arbitrage event despite different titles"

    # Second match should be the fuzzy match
    fuzzy_match = matches[1]
    assert fuzzy_match.kalshi_ticker == "KX-REGULAR"
    assert fuzzy_match.polymarket_condition_id == "0xpoly_regular"
    assert fuzzy_match.confidence == 1.0
    assert fuzzy_match.match_source == "fuzzy"


def test_load_market_overrides_yaml_formats(tmp_path: Path):
    """Verify loading market overrides from YAML with root list or overrides dict."""
    # Test format 1: root dict with 'overrides'
    yaml_content_1 = """
overrides:
  - kalshi_ticker: "TICKER-1"
    polymarket_condition_id: "0xcond1"
    notes: "Note 1"
  - ticker: "TICKER-2"
    condition_id: "0xcond2"
"""
    f1 = tmp_path / "overrides1.yaml"
    f1.write_text(yaml_content_1, encoding="utf-8")

    loaded_1 = load_market_overrides(f1)
    assert len(loaded_1) == 2
    assert loaded_1[0].kalshi_ticker == "TICKER-1"
    assert loaded_1[0].polymarket_condition_id == "0xcond1"
    assert loaded_1[1].kalshi_ticker == "TICKER-2"
    assert loaded_1[1].polymarket_condition_id == "0xcond2"

    # Test format 2: root list
    yaml_content_2 = """
- kalshi_ticker: "TICKER-3"
  polymarket_condition_id: "0xcond3"
"""
    f2 = tmp_path / "overrides2.yaml"
    f2.write_text(yaml_content_2, encoding="utf-8")

    loaded_2 = load_market_overrides(f2)
    assert len(loaded_2) == 1
    assert loaded_2[0].kalshi_ticker == "TICKER-3"

    # Missing file returns empty list
    assert load_market_overrides(tmp_path / "nonexistent.yaml") == []


# ============================================================================
# Model Compatibility Tests (KalshiMarket, PolyMarket Models)
# ============================================================================


def test_matcher_with_pydantic_market_models():
    """Verify MarketMatcher works directly with parsed KalshiMarket and PolyMarket models."""
    k_market = KalshiMarket(
        ticker="FED-26MAY-T500",
        title="Will the Fed cut rates in May 2026?",
        yes_bid=40,
        yes_ask=45,
    )
    p_market = PolyMarket(
        condition_id="0x777fed",
        ticker="0x777fed",
        title="Fed rate cut by May 2026?",
        yes_bid=42,
        yes_ask=44,
    )

    matcher = MarketMatcher(min_similarity=0.70, overrides=[])
    matches = matcher.match([k_market], [p_market])

    assert len(matches) == 1
    m = matches[0]
    assert m.kalshi_ticker == "FED-26MAY-T500"
    assert m.polymarket_condition_id == "0x777fed"
    assert m.kalshi_market is k_market
    assert m.polymarket_market is p_market


# ============================================================================
# Extension Point (Custom Scorer) Tests
# ============================================================================


class MockEmbeddingScorer(BaseSimilarityScorer):
    """Mock implementation of embedding-based similarity scorer."""

    def __init__(self, semantic_map: dict[tuple[str, str], float]) -> None:
        self.semantic_map = semantic_map

    def score(self, text_a: str, text_b: str) -> float:
        return self.semantic_map.get((text_a, text_b), 0.0)


def test_custom_similarity_scorer_extension_point():
    """Verify that a custom BaseSimilarityScorer can be plugged in."""
    k_title = "Will the US central bank lower borrowing costs?"
    p_title = "Federal Reserve interest rate reduction in 2026?"

    # RapidFuzz token sort would score low because of vocabulary differences
    fuzz_score = RapidFuzzTokenSortScorer().score(k_title, p_title)
    assert fuzz_score < 0.50

    # Mock embedding scorer recognizes semantic equivalence
    custom_scorer = MockEmbeddingScorer({(k_title, p_title): 0.94})
    matcher = MarketMatcher(min_similarity=0.85, scorer=custom_scorer, overrides=[])

    matches = matcher.match(
        [{"ticker": "KX-FED-SEMANTIC", "title": k_title}],
        [{"condition_id": "0xpoly_fed_semantic", "title": p_title}],
    )

    assert len(matches) == 1
    assert matches[0].confidence == 0.94


def test_callable_similarity_scorer():
    """Verify CallableSimilarityScorer adapter."""
    custom_fn = lambda a, b: 0.99 if "crypto" in a.lower() and "crypto" in b.lower() else 0.10
    scorer = CallableSimilarityScorer(custom_fn)
    matcher = MarketMatcher(min_similarity=0.80, scorer=scorer, overrides=[])

    matches = matcher.match(
        [{"ticker": "K-CRYPTO", "title": "Top Crypto 2026"}],
        [{"condition_id": "0xpoly_crypto", "title": "Crypto Market Cap in 2026"}],
    )
    assert len(matches) == 1
    assert matches[0].confidence == 0.99


# ============================================================================
# Confirmed Overrides In config/market_overrides.yaml Validation
# ============================================================================


def test_project_market_overrides_file_contains_confirmed_pairs():
    """Verify that config/market_overrides.yaml exists, loads, and contains at least 3-5 confirmed pairs."""
    overrides_file = Path(__file__).resolve().parent.parent / "config" / "market_overrides.yaml"
    assert overrides_file.exists(), f"Overrides file missing at {overrides_file}"

    overrides = load_market_overrides(overrides_file)
    assert len(overrides) >= 3, f"Expected at least 3 confirmed pairs, got {len(overrides)}"

    for ov in overrides:
        assert ov.kalshi_ticker, "Kalshi ticker must not be empty"
        assert ov.polymarket_condition_id, "Polymarket condition ID must not be empty"
        assert ov.kalshi_ticker.strip() == ov.kalshi_ticker
        assert ov.polymarket_condition_id.strip() == ov.polymarket_condition_id
