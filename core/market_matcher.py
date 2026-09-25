"""Market event matcher for identifying identical cross-platform prediction markets.

Features:
- RapidFuzz baseline string similarity (`token_sort_ratio` on market titles) as the default.
- Documented extension point (`BaseSimilarityScorer`) for swapping in embedding-based models
  (e.g., SentenceTransformers, OpenAI embeddings).
- Manual override support via `config/market_overrides.yaml` to bypass fuzzy matching with confidence=1.0.
- Merges manual overrides first, followed by fuzzy matches above `matching.min_similarity`.
- Returns candidate pairs sorted in descending order of confidence score.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence, Union

from pydantic import BaseModel, ConfigDict, Field
from rapidfuzz import fuzz, utils
import yaml

if TYPE_CHECKING:
    from core.config_loader import AppConfig

logger = logging.getLogger(__name__)


# ============================================================================
# Similarity Scorer Interface & Extension Point
# ============================================================================


class BaseSimilarityScorer(ABC):
    """Abstract base class for title/event similarity scoring algorithms.

    ============================================================================
    EXTENSION POINT: Embedding-Based Similarity
    ============================================================================
    To swap in an embedding-based model (e.g. SentenceTransformers or OpenAI embeddings):

    1. Subclass `BaseSimilarityScorer`:
       ```python
       from sentence_transformers import SentenceTransformer, util

       class EmbeddingSimilarityScorer(BaseSimilarityScorer):
           def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
               self.model = SentenceTransformer(model_name)

           def score(self, text_a: str, text_b: str) -> float:
               if not text_a or not text_b:
                   return 0.0
               emb1 = self.model.encode(text_a, convert_to_tensor=True)
               emb2 = self.model.encode(text_b, convert_to_tensor=True)
               cos_sim = util.cos_sim(emb1, emb2).item()
               # Normalize cosine similarity [-1, 1] to [0, 1]
               return max(0.0, min(1.0, (cos_sim + 1.0) / 2.0))
       ```

    2. Pass your custom scorer to `MarketMatcher`:
       ```python
       scorer = EmbeddingSimilarityScorer()
       matcher = MarketMatcher(scorer=scorer)
       ```
    ============================================================================
    """

    @abstractmethod
    def score(self, text_a: str, text_b: str) -> float:
        """Compute similarity score between two market titles/descriptions.

        Args:
            text_a: Title/description of the first market.
            text_b: Title/description of the second market.

        Returns:
            float: Similarity score between 0.0 and 1.0.
        """
        pass


class RapidFuzzTokenSortScorer(BaseSimilarityScorer):
    """Default similarity scorer using RapidFuzz `token_sort_ratio`.

    Token sort ratio tokenizes strings, sorts tokens alphabetically, and computes
    the Levenshtein distance ratio, making it robust against token reorderings
    (e.g., "Fed Rate Cut May 2026" vs "Will the Fed cut interest rates in May 2026?").
    """

    def __init__(self, processor: Optional[Callable[[str], str]] = utils.default_process) -> None:
        self.processor = processor

    def score(self, text_a: str, text_b: str) -> float:
        """Compute token sort ratio normalized to [0.0, 1.0]."""
        if not text_a or not text_b:
            return 0.0
        ratio = fuzz.token_sort_ratio(str(text_a).strip(), str(text_b).strip(), processor=self.processor)
        return round(float(ratio) / 100.0, 4)


class CallableSimilarityScorer(BaseSimilarityScorer):
    """Adapter wrapping any callable `(str, str) -> float` into a `BaseSimilarityScorer`."""

    def __init__(self, fn: Callable[[str, str], float]) -> None:
        self.fn = fn

    def score(self, text_a: str, text_b: str) -> float:
        return self.fn(text_a, text_b)


# ============================================================================
# Data Models
# ============================================================================


class MarketOverride(BaseModel):
    """Manual market pair override definition."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    kalshi_ticker: str = Field(description="Kalshi market ticker symbol")
    polymarket_condition_id: str = Field(description="Polymarket condition ID (0x...) or identifier")
    notes: Optional[str] = Field(default=None, description="Optional description/rationale for this verified pair")
    kalshi_title: Optional[str] = Field(default=None, description="Cached or expected Kalshi title")
    polymarket_title: Optional[str] = Field(default=None, description="Cached or expected Polymarket title")


class MarketMatch(BaseModel):
    """Matched market pair across Kalshi and Polymarket platforms."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    kalshi_ticker: str = Field(description="Kalshi market ticker symbol")
    polymarket_condition_id: str = Field(description="Polymarket condition ID")
    kalshi_title: str = Field(description="Title/question of the Kalshi market")
    polymarket_title: str = Field(description="Title/question of the Polymarket market")
    confidence: float = Field(ge=0.0, le=1.0, description="Match confidence score (1.0 for overrides)")
    match_source: str = Field(description="Match provenance: 'override', 'fuzzy', or custom scorer name")
    notes: Optional[str] = Field(default=None, description="Optional notes or context")
    kalshi_market: Optional[Any] = Field(default=None, description="Original Kalshi market model/dict")
    polymarket_market: Optional[Any] = Field(default=None, description="Original Polymarket market model/dict")

    @property
    def is_override(self) -> bool:
        """Return True if match originated from manual overrides."""
        return self.match_source == "override"


# ============================================================================
# Overrides Loading Utility
# ============================================================================


def load_market_overrides(overrides_path: Union[str, Path]) -> list[MarketOverride]:
    """Load manual market override pairs from a YAML file.

    Supports both top-level list format and dictionary format with an `overrides` key.

    Args:
        overrides_path: Path to `market_overrides.yaml`.

    Returns:
        list[MarketOverride]: List of parsed MarketOverride objects.
    """
    path = Path(overrides_path)
    if not path.exists():
        logger.debug("Market overrides file not found at %s. Returning empty overrides.", path)
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw_data = yaml.safe_load(f)
    except Exception as e:
        logger.warning("Failed to parse market overrides YAML file at %s: %s", path, e)
        return []

    if raw_data is None:
        return []

    entries: list[Any] = []
    if isinstance(raw_data, list):
        entries = raw_data
    elif isinstance(raw_data, dict):
        entries = raw_data.get("overrides", [])
        if not isinstance(entries, list):
            logger.warning("Expected 'overrides' key in %s to be a list, got %s", path, type(entries))
            return []
    else:
        logger.warning("Unrecognized structure in %s: expected dict or list", path)
        return []

    overrides: list[MarketOverride] = []
    for item in entries:
        if not isinstance(item, dict):
            continue

        # Normalize key aliases
        kalshi_ticker = (
            item.get("kalshi_ticker")
            or item.get("kalshiTicker")
            or item.get("ticker_kalshi")
            or item.get("ticker")
        )
        poly_condition = (
            item.get("polymarket_condition_id")
            or item.get("polymarket_condition")
            or item.get("polymarketConditionId")
            or item.get("condition_id")
            or item.get("conditionId")
            or item.get("polymarket_ticker")
        )

        if kalshi_ticker and poly_condition:
            overrides.append(
                MarketOverride(
                    kalshi_ticker=str(kalshi_ticker).strip(),
                    polymarket_condition_id=str(poly_condition).strip(),
                    notes=item.get("notes"),
                    kalshi_title=item.get("kalshi_title"),
                    polymarket_title=item.get("polymarket_title"),
                )
            )

    logger.debug("Loaded %d manual market overrides from %s", len(overrides), path)
    return overrides


# ============================================================================
# Helper Market Normalization
# ============================================================================


def _extract_market_info(market: Any) -> tuple[str, str, Any]:
    """Extract (identifier, title, raw_object) from any market object, model, or dict.

    Supports KalshiMarket, PolyMarket, MarketState, or dictionaries.
    """
    if isinstance(market, dict):
        ticker = str(
            market.get("ticker")
            or market.get("condition_id")
            or market.get("conditionId")
            or market.get("id")
            or ""
        ).strip()
        title = str(
            market.get("title")
            or market.get("question")
            or market.get("name")
            or ""
        ).strip()
        return ticker, title, market

    # Object / Pydantic model
    ticker = (
        getattr(market, "ticker", None)
        or getattr(market, "condition_id", None)
        or getattr(market, "id", None)
        or ""
    )
    title = (
        getattr(market, "title", None)
        or getattr(market, "question", None)
        or getattr(market, "name", None)
        or ""
    )
    return str(ticker).strip(), str(title).strip(), market


# ============================================================================
# Market Matcher
# ============================================================================


class MarketMatcher:
    """Matches prediction markets across Kalshi and Polymarket using overrides & fuzzy similarity."""

    def __init__(
        self,
        min_similarity: float = 0.85,
        overrides_path: Optional[Union[str, Path]] = "config/market_overrides.yaml",
        overrides: Optional[Sequence[Union[MarketOverride, dict[str, Any]]]] = None,
        scorer: Optional[BaseSimilarityScorer] = None,
    ) -> None:
        """Initialize MarketMatcher.

        Args:
            min_similarity: Minimum similarity score threshold (0.0 to 1.0) for fuzzy matches.
            overrides_path: Path to `market_overrides.yaml` manual override file.
            overrides: Optional explicitly passed override objects or dicts (overrides file).
            scorer: Custom BaseSimilarityScorer implementation (defaults to RapidFuzzTokenSortScorer).
        """
        self.min_similarity = float(min_similarity)
        self.scorer = scorer or RapidFuzzTokenSortScorer()
        self.overrides_path = Path(overrides_path) if overrides_path else None

        if overrides is not None:
            self.overrides: list[MarketOverride] = []
            for item in overrides:
                if isinstance(item, MarketOverride):
                    self.overrides.append(item)
                elif isinstance(item, dict):
                    self.overrides.append(MarketOverride.model_validate(item))
        elif self.overrides_path and self.overrides_path.exists():
            self.overrides = load_market_overrides(self.overrides_path)
        else:
            self.overrides = []

    @classmethod
    def from_config(
        cls,
        config: AppConfig,
        overrides_path: Optional[Union[str, Path]] = None,
        scorer: Optional[BaseSimilarityScorer] = None,
    ) -> MarketMatcher:
        """Factory method to construct MarketMatcher from application configuration.

        Args:
            config: AppConfig instance containing matching configuration.
            overrides_path: Optional override for manual overrides YAML path.
            scorer: Optional custom BaseSimilarityScorer instance.
        """
        min_similarity = config.matching.min_similarity
        target_path = overrides_path or getattr(config.matching, "overrides_path", "config/market_overrides.yaml")
        return cls(min_similarity=min_similarity, overrides_path=target_path, scorer=scorer)

    def compute_similarity(self, title_a: str, title_b: str) -> float:
        """Compute similarity score between two title strings using the active scorer."""
        return self.scorer.score(title_a, title_b)

    def match(
        self,
        kalshi_markets: Sequence[Any],
        polymarket_markets: Sequence[Any],
        deduplicate: bool = True,
    ) -> list[MarketMatch]:
        """Propose candidate market pairs representing the same real-world event.

        Process:
        1. Manual Overrides: Checks `self.overrides`. Verified pairs present in the input
           market lists receive confidence = 1.0 and match_source = 'override'.
        2. Fuzzy Similarity: Computes similarity scores for remaining candidate pairs using
           the active similarity scorer. Pairs meeting or exceeding `min_similarity` are included.
        3. Sorting & Deduplication: Results are returned sorted by confidence descending.
           If `deduplicate=True` (default), each market is paired with its single highest-confidence
           match without conflicts.

        Args:
            kalshi_markets: Sequence of Kalshi market objects, models, or dicts.
            polymarket_markets: Sequence of Polymarket market objects, models, or dicts.
            deduplicate: Whether to enforce 1-to-1 matching (highest score takes priority).

        Returns:
            list[MarketMatch]: List of matched market pairs sorted descending by confidence score.
        """
        # Build lookup tables for Kalshi markets
        kalshi_by_ticker: dict[str, tuple[str, str, Any]] = {}
        for km in kalshi_markets:
            ticker, title, raw = _extract_market_info(km)
            if ticker:
                kalshi_by_ticker[ticker] = (ticker, title, raw)

        # Build lookup tables for Polymarket markets (by condition_id and slug/ticker)
        poly_by_id: dict[str, tuple[str, str, Any]] = {}
        for pm in polymarket_markets:
            condition_id, title, raw = _extract_market_info(pm)
            if condition_id:
                poly_by_id[condition_id] = (condition_id, title, raw)
                # Also index by slug/ticker if present on object
                slug = getattr(pm, "slug", None) or (pm.get("slug") if isinstance(pm, dict) else None)
                if slug:
                    poly_by_id[slug] = (condition_id, title, raw)
                ticker = getattr(pm, "ticker", None) or (pm.get("ticker") if isinstance(pm, dict) else None)
                if ticker and ticker not in poly_by_id:
                    poly_by_id[ticker] = (condition_id, title, raw)

        matched_kalshi_tickers: set[str] = set()
        matched_poly_conditions: set[str] = set()
        results: list[MarketMatch] = []

        # ----------------------------------------------------------------------
        # Phase 1: Manual Overrides (confidence = 1.0)
        # ----------------------------------------------------------------------
        for override in self.overrides:
            k_ticker = override.kalshi_ticker
            p_cond = override.polymarket_condition_id

            k_info = kalshi_by_ticker.get(k_ticker)
            p_info = poly_by_id.get(p_cond)

            # If both markets are present in the provided market lists
            if k_info and p_info:
                actual_k_ticker, k_title, k_raw = k_info
                actual_p_cond, p_title, p_raw = p_info

                results.append(
                    MarketMatch(
                        kalshi_ticker=actual_k_ticker,
                        polymarket_condition_id=actual_p_cond,
                        kalshi_title=k_title or override.kalshi_title or actual_k_ticker,
                        polymarket_title=p_title or override.polymarket_title or actual_p_cond,
                        confidence=1.0,
                        match_source="override",
                        notes=override.notes,
                        kalshi_market=k_raw,
                        polymarket_market=p_raw,
                    )
                )
                matched_kalshi_tickers.add(actual_k_ticker)
                matched_poly_conditions.add(actual_p_cond)
            elif override.kalshi_title and override.polymarket_title:
                # If market objects weren't passed but override defines titles
                results.append(
                    MarketMatch(
                        kalshi_ticker=k_ticker,
                        polymarket_condition_id=p_cond,
                        kalshi_title=override.kalshi_title,
                        polymarket_title=override.polymarket_title,
                        confidence=1.0,
                        match_source="override",
                        notes=override.notes,
                    )
                )
                matched_kalshi_tickers.add(k_ticker)
                matched_poly_conditions.add(p_cond)

        # ----------------------------------------------------------------------
        # Phase 2: Fuzzy Similarity Matching
        # ----------------------------------------------------------------------
        candidate_fuzzy_matches: list[MarketMatch] = []

        # Deduplicate Polymarket list so we only compare unique condition IDs
        unique_poly_markets = {info[0]: info for info in poly_by_id.values()}

        for k_ticker, (actual_k_ticker, k_title, k_raw) in kalshi_by_ticker.items():
            if deduplicate and actual_k_ticker in matched_kalshi_tickers:
                continue

            for actual_p_cond, p_info in unique_poly_markets.items():
                _, p_title, p_raw = p_info

                if deduplicate and actual_p_cond in matched_poly_conditions:
                    continue

                if not k_title or not p_title:
                    continue

                score = self.scorer.score(k_title, p_title)

                if score >= self.min_similarity:
                    candidate_fuzzy_matches.append(
                        MarketMatch(
                            kalshi_ticker=actual_k_ticker,
                            polymarket_condition_id=actual_p_cond,
                            kalshi_title=k_title,
                            polymarket_title=p_title,
                            confidence=score,
                            match_source="fuzzy",
                            kalshi_market=k_raw,
                            polymarket_market=p_raw,
                        )
                    )

        # Sort candidate fuzzy matches by confidence descending
        candidate_fuzzy_matches.sort(key=lambda m: m.confidence, reverse=True)

        # ----------------------------------------------------------------------
        # Phase 3: Deduplication & Final Merge
        # ----------------------------------------------------------------------
        if deduplicate:
            for match in candidate_fuzzy_matches:
                if (
                    match.kalshi_ticker not in matched_kalshi_tickers
                    and match.polymarket_condition_id not in matched_poly_conditions
                ):
                    results.append(match)
                    matched_kalshi_tickers.add(match.kalshi_ticker)
                    matched_poly_conditions.add(match.polymarket_condition_id)
        else:
            results.extend(candidate_fuzzy_matches)

        # Sort all final matches: overrides (1.0) and highest confidence scores first
        results.sort(key=lambda m: (m.confidence, 1 if m.match_source == "override" else 0), reverse=True)
        return results
