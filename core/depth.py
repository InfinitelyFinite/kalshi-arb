"""Order book depth helpers: exact-decimal ladders, truncation, VWAP and fillability.

Conventions (see also ``core/models.py``):
- prices are in **cents** (0 to 100), sizes in **contracts**; both are ``Decimal`` so values written to
  and read back from DuckDB are exact (Polymarket sizes and sub-cent ticks can be fractional).
- an *ask ladder* is the list of levels you would BUY against, sorted best (lowest) first.
- a *bid ladder* is sorted best (highest) first.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Optional, Sequence

Level = tuple[Decimal, Decimal]  # (price_cents, size_contracts)

ONE_DOLLAR_CENTS = Decimal(100)


def to_decimal(value: Any) -> Optional[Decimal]:
    """Convert numbers/strings to ``Decimal`` without float artefacts. Returns None if not numeric/finite."""
    if value is None or value == "":
        return None
    try:
        # str() first so floats such as 0.1 become Decimal("0.1") rather than the binary expansion.
        d = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    return d if d.is_finite() else None


def clean_levels(levels: Iterable[Level], ascending: bool) -> list[Level]:
    """Drop non-positive sizes and sort the ladder (ascending=asks, descending=bids)."""
    kept = [(Decimal(p), Decimal(s)) for p, s in levels if s is not None and p is not None and Decimal(s) > 0]
    return sorted(kept, key=lambda lv: lv[0], reverse=not ascending)


def truncate_levels(levels: Sequence[Level], depth: int) -> tuple[list[Level], bool]:
    """Return the top ``depth`` levels and whether anything was cut off."""
    if depth < 0:
        raise ValueError("depth must be >= 0")
    return list(levels[:depth]), len(levels) > depth


def vwap_for_size(levels: Sequence[Level], qty: Any) -> tuple[Optional[Decimal], Decimal, bool]:
    """Volume-weighted average price (cents) for buying ``qty`` contracts against an ask ladder.

    Walks levels from best (lowest ask) to worst until ``qty`` is filled. If the supplied depth cannot fill
    ``qty`` this returns ``(vwap_of_what_could_be_filled, filled_qty, False)``; it never extrapolates and never
    assumes the best ask applies beyond its own size. Empty book -> ``(None, 0, False)``.

    Returns:
        (vwap_cents, filled_qty, fully_filled)
    """
    want = to_decimal(qty)
    if want is None or want <= 0:
        raise ValueError(f"qty must be a positive number, got {qty!r}")

    remaining = want
    cost = Decimal(0)
    filled = Decimal(0)
    for price, size in sorted(levels, key=lambda lv: lv[0]):
        if size <= 0:
            continue
        take = size if size < remaining else remaining
        cost += take * price
        filled += take
        remaining -= take
        if remaining <= 0:
            break

    if filled == 0:
        return None, Decimal(0), False
    return cost / filled, filled, remaining <= 0


@dataclass
class Ladders:
    """Per-market ladders in cents/contracts. Empty list means "no resting orders on that side"."""

    yes_ask: list[Level] = field(default_factory=list)
    no_ask: list[Level] = field(default_factory=list)
    yes_bid: list[Level] = field(default_factory=list)
    no_bid: list[Level] = field(default_factory=list)
    exchange_updated_at: Optional[datetime] = None
    # True when NO ladders were derived from the YES book instead of observed on a NO token book.
    no_side_derived: bool = False

    def side(self, name: str) -> list[Level]:
        return getattr(self, name)


def kalshi_ladders(orderbook: Any) -> Ladders:
    """Build ladders from a Kalshi ``Orderbook`` (bids only; asks are ``100 - opposite bid``).

    YES ask ladder = NO bids mirrored (price 100 - no_bid, size = that NO bid's size); NO ask ladder likewise.
    """
    yes_bids = clean_levels(orderbook.yes_bid_levels, ascending=False)
    no_bids = clean_levels(orderbook.no_bid_levels, ascending=False)
    yes_ask = clean_levels([(ONE_DOLLAR_CENTS - p, s) for p, s in no_bids], ascending=True)
    no_ask = clean_levels([(ONE_DOLLAR_CENTS - p, s) for p, s in yes_bids], ascending=True)
    return Ladders(
        yes_ask=yes_ask,
        no_ask=no_ask,
        yes_bid=yes_bids,
        no_bid=no_bids,
        exchange_updated_at=getattr(orderbook, "exchange_updated_at", None),
    )


def polymarket_ladders(yes_book: Any, no_book: Optional[Any] = None) -> Ladders:
    """Build ladders from Polymarket per-token books.

    YES ask/bid levels come from the YES token's own asks/bids and NO levels from the NO token's own asks/bids.
    If no NO-token book is available the NO side is derived from the YES book and flagged ``no_side_derived``.
    """
    yes_ask = clean_levels(yes_book.ask_levels, ascending=True)
    yes_bid = clean_levels(yes_book.bid_levels, ascending=False)
    if no_book is not None:
        no_ask = clean_levels(no_book.ask_levels, ascending=True)
        no_bid = clean_levels(no_book.bid_levels, ascending=False)
        derived = False
        stamps = [yes_book.exchange_updated_at, no_book.exchange_updated_at]
        # Two token books -> two exchange times. The older one bounds the freshness of the combined quote;
        # if either is missing the combined time is unknown (never backfilled from the other leg).
        exchange_ts = min(stamps) if all(t is not None for t in stamps) else None
    else:
        no_ask = clean_levels([(ONE_DOLLAR_CENTS - p, s) for p, s in yes_bid], ascending=True)
        no_bid = clean_levels([(ONE_DOLLAR_CENTS - p, s) for p, s in yes_ask], ascending=False)
        derived = True
        exchange_ts = yes_book.exchange_updated_at
    return Ladders(
        yes_ask=yes_ask,
        no_ask=no_ask,
        yes_bid=yes_bid,
        no_bid=no_bid,
        exchange_updated_at=exchange_ts,
        no_side_derived=derived,
    )
