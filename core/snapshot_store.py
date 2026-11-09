"""DuckDB snapshot store: schema, idempotent migration, writers and research views.

Tables
- ``orderbook_snapshots``: one row per market observation. Legacy columns (``ts`` = snapshot batch time,
  ``yes_bid``/``yes_ask``/``no_bid``/``no_ask`` rounded integer cents, ``volume``) are kept so existing readers
  keep working; v2 adds exact-decimal best asks/bids with sizes, both timestamps, status, provenance, etc.
- ``orderbook_levels``: top-N depth per side for each snapshot (``snapshot_id`` joins to the snapshot row).
- ``pair_exclusions``: audit log of pairs dropped by validation / the collection gate.
- ``verified_pairs``: copy of the pairs that are currently ``status: verified`` in the YAML registry.

Views
- ``research_snapshots``: ONLY ``data_source = 'live_api'`` AND ``tradable`` AND pair currently verified.
  Every analysis/replay/quality report must read this view, never the raw table.
- ``research_pair_snapshots``: per-pair join of the Kalshi and Polymarket legs with ``quote_skew_ms``.

Timestamps in v2 columns are naive-UTC ``TIMESTAMP`` (so reading them never needs pytz); ``ts`` stays TIMESTAMPTZ.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Union

import duckdb

from core.depth import Ladders, Level, truncate_levels
from core.models import SCHEMA_VERSION, DataSource, MarketStatus, is_tradable

logger = logging.getLogger(__name__)

PRICE_TYPE = "DECIMAL(9,4)"
SIZE_TYPE = "DECIMAL(18,4)"
SIDES = ("yes_ask", "no_ask", "yes_bid", "no_bid")

_ENUMS = {
    "market_status_t": [s.value for s in MarketStatus],
    "data_source_t": [s.value for s in DataSource],
    "book_side_t": list(SIDES),
}

# (column, DDL type) added on top of the legacy 8-column table. Order matters only for readability.
_V2_COLUMNS: list[tuple[str, str]] = [
    ("snapshot_id", "BIGINT"),
    ("observed_at_utc", "TIMESTAMP"),
    ("exchange_updated_at_utc", "TIMESTAMP"),
    ("ask_yes", PRICE_TYPE),
    ("ask_no", PRICE_TYPE),
    ("ask_yes_size", SIZE_TYPE),
    ("ask_no_size", SIZE_TYPE),
    ("bid_yes", PRICE_TYPE),
    ("bid_no", PRICE_TYPE),
    ("bid_yes_size", SIZE_TYPE),
    ("bid_no_size", SIZE_TYPE),
    ("market_status", "market_status_t"),
    ("tradable", "BOOLEAN"),
    ("data_source", "data_source_t"),
    ("depth_truncated", "BOOLEAN"),
    ("schema_version", "INTEGER"),
]


def to_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Aware -> naive UTC; naive is assumed to already be UTC. None stays None."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def to_aware_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


@dataclass
class SnapshotRecord:
    """One market observation ready to be written (v2 schema)."""

    ts: datetime
    platform: str
    ticker: str
    observed_at_utc: datetime
    exchange_updated_at_utc: Optional[datetime]
    yes_bid: Optional[int] = None
    yes_ask: Optional[int] = None
    no_bid: Optional[int] = None
    no_ask: Optional[int] = None
    volume: int = 0
    ask_yes: Optional[Decimal] = None
    ask_no: Optional[Decimal] = None
    ask_yes_size: Optional[Decimal] = None
    ask_no_size: Optional[Decimal] = None
    bid_yes: Optional[Decimal] = None
    bid_no: Optional[Decimal] = None
    bid_yes_size: Optional[Decimal] = None
    bid_no_size: Optional[Decimal] = None
    market_status: str = MarketStatus.UNKNOWN.value
    tradable: bool = False
    data_source: str = DataSource.LIVE_API.value
    depth_truncated: bool = False
    levels: dict[str, list[Level]] = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION
    snapshot_id: Optional[int] = None  # assigned on write


def _best(levels: Sequence[Level]) -> tuple[Optional[Decimal], Optional[Decimal]]:
    return (levels[0][0], levels[0][1]) if levels else (None, None)


def build_snapshot_record(
    state: Any,
    ts: datetime,
    depth_levels: int,
    data_source: Union[DataSource, str],
) -> SnapshotRecord:
    """Convert a ``MarketState`` (duck-typed) into a ``SnapshotRecord``.

    If the state carries ladders, best prices/sizes and top-N levels come from them (sizes known). Without
    ladders (e.g. synthetic generator) best prices come from the integer fields and sizes stay NULL (= unknown).
    """
    ladders: Optional[Ladders] = getattr(state, "ladders", None)
    status = getattr(state, "market_status", MarketStatus.UNKNOWN)
    status_val = status.value if isinstance(status, MarketStatus) else str(status)
    try:
        status_enum = MarketStatus(status_val)
    except ValueError:
        status_enum = MarketStatus.UNKNOWN

    levels: dict[str, list[Level]] = {}
    truncated = False
    if ladders is not None:
        for side in SIDES:
            kept, cut = truncate_levels(ladders.side(side), depth_levels)
            levels[side] = kept
            truncated = truncated or cut
        ask_yes, ask_yes_size = _best(levels["yes_ask"])
        ask_no, ask_no_size = _best(levels["no_ask"])
        bid_yes, bid_yes_size = _best(levels["yes_bid"])
        bid_no, bid_no_size = _best(levels["no_bid"])
    else:
        def dec(v: Optional[int]) -> Optional[Decimal]:
            return Decimal(v) if v is not None else None

        ask_yes, ask_no = dec(state.yes_ask), dec(state.no_ask)
        bid_yes, bid_no = dec(state.yes_bid), dec(state.no_bid)
        ask_yes_size = ask_no_size = bid_yes_size = bid_no_size = None

    observed = getattr(state, "observed_at", None) or state.ts
    return SnapshotRecord(
        ts=ts,
        platform=state.platform,
        ticker=state.ticker,
        observed_at_utc=to_naive_utc(observed),  # type: ignore[arg-type]
        exchange_updated_at_utc=to_naive_utc(getattr(state, "exchange_updated_at", None)),
        yes_bid=state.yes_bid,
        yes_ask=state.yes_ask,
        no_bid=state.no_bid,
        no_ask=state.no_ask,
        volume=int(state.volume) if state.volume is not None else 0,
        ask_yes=ask_yes,
        ask_no=ask_no,
        ask_yes_size=ask_yes_size,
        ask_no_size=ask_no_size,
        bid_yes=bid_yes,
        bid_no=bid_no,
        bid_yes_size=bid_yes_size,
        bid_no_size=bid_no_size,
        market_status=status_enum.value,
        tradable=is_tradable(status_enum),
        data_source=DataSource(data_source).value,
        depth_truncated=truncated,
        levels=levels,
    )


# ---------------------------------------------------------------------------
# Schema / migration
# ---------------------------------------------------------------------------


def connect(path: Union[str, Path], read_only: bool = False) -> duckdb.DuckDBPyConnection:
    target = str(path)
    if target != ":memory:" and not read_only:
        Path(target).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(target, read_only=read_only)


def _existing_columns(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?", [table]
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def ensure_schema(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Create/upgrade all tables and views. Idempotent; never deletes data.

    Returns:
        {"legacy_rows_migrated": n} number of pre-v2 rows tagged ``legacy_unverified`` by this call.
    """
    existing_types = {r[0] for r in conn.execute("SELECT type_name FROM duckdb_types()").fetchall()}
    for name, values in _ENUMS.items():
        if name not in existing_types:
            literal = ", ".join(f"'{v}'" for v in values)
            conn.execute(f"CREATE TYPE {name} AS ENUM ({literal})")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            ts TIMESTAMP WITH TIME ZONE,
            platform VARCHAR,
            ticker VARCHAR,
            yes_bid INTEGER,
            yes_ask INTEGER,
            no_bid INTEGER,
            no_ask INTEGER,
            volume BIGINT
        )
        """
    )
    for col, ddl in _V2_COLUMNS:
        conn.execute(f"ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS {col} {ddl}")

    conn.execute("CREATE SEQUENCE IF NOT EXISTS snapshot_id_seq START 1")

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS orderbook_levels (
            snapshot_id BIGINT,
            platform VARCHAR,
            ticker VARCHAR,
            side book_side_t,
            level_idx INTEGER,
            price {PRICE_TYPE},
            size {SIZE_TYPE}
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pair_exclusions (
            checked_at_utc TIMESTAMP,
            pair_id VARCHAR,
            reason_code VARCHAR,
            detail VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS verified_pairs (
            pair_id VARCHAR,
            category VARCHAR,
            kalshi_ticker VARCHAR,
            polymarket_condition_id VARCHAR,
            synced_at_utc TIMESTAMP
        )
        """
    )

    migrated = _migrate_legacy_rows(conn)
    _create_views(conn)
    return {"legacy_rows_migrated": migrated}


def _migrate_legacy_rows(conn: duckdb.DuckDBPyConnection) -> int:
    """Tag rows without a data_source (pre-v2 rows) as legacy_unverified. Idempotent."""
    pending = conn.execute("SELECT count(*) FROM orderbook_snapshots WHERE data_source IS NULL").fetchone()[0]
    if not pending:
        return 0

    ts_type = _existing_columns(conn, "orderbook_snapshots").get("ts", "")
    observed_expr = "(ts AT TIME ZONE 'UTC')" if "WITH TIME ZONE" in ts_type.upper() else "ts"
    conn.execute("BEGIN")
    try:
        conn.execute(
            f"""
            UPDATE orderbook_snapshots
            SET data_source = 'legacy_unverified',
                observed_at_utc = COALESCE(observed_at_utc, {observed_expr}),
                snapshot_id = COALESCE(snapshot_id, nextval('snapshot_id_seq')),
                market_status = COALESCE(market_status, 'unknown'),
                tradable = COALESCE(tradable, false),
                depth_truncated = COALESCE(depth_truncated, false),
                schema_version = COALESCE(schema_version, 1)
            WHERE data_source IS NULL
            """
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    logger.info("Migrated %d legacy snapshot rows to data_source='legacy_unverified'", pending)
    return int(pending)


def _create_views(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE VIEW research_snapshots AS
        SELECT s.*, p.pair_id, p.category
        FROM orderbook_snapshots s
        JOIN verified_pairs p
          ON (s.platform = 'kalshi' AND s.ticker = p.kalshi_ticker)
          OR (s.platform = 'polymarket' AND lower(s.ticker) = lower(p.polymarket_condition_id))
        WHERE s.data_source = 'live_api'
          AND s.tradable IS TRUE
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE VIEW research_pair_snapshots AS
        SELECT
            k.pair_id,
            k.category,
            k.ts,
            k.snapshot_id AS kalshi_snapshot_id,
            p.snapshot_id AS polymarket_snapshot_id,
            k.observed_at_utc AS kalshi_observed_at_utc,
            p.observed_at_utc AS polymarket_observed_at_utc,
            k.exchange_updated_at_utc AS kalshi_exchange_updated_at_utc,
            p.exchange_updated_at_utc AS polymarket_exchange_updated_at_utc,
            abs(epoch_ms(k.observed_at_utc) - epoch_ms(p.observed_at_utc)) AS observed_skew_ms,
            CASE
                WHEN k.exchange_updated_at_utc IS NULL OR p.exchange_updated_at_utc IS NULL THEN NULL
                ELSE abs(epoch_ms(k.exchange_updated_at_utc) - epoch_ms(p.exchange_updated_at_utc))
            END AS quote_skew_ms
        FROM research_snapshots k
        JOIN research_snapshots p
          ON k.pair_id = p.pair_id AND k.ts = p.ts
        WHERE k.platform = 'kalshi' AND p.platform = 'polymarket'
        """
    )


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def write_snapshots(conn: duckdb.DuckDBPyConnection, records: Sequence[SnapshotRecord]) -> int:
    """Insert snapshot rows and their depth levels atomically. Assigns ``snapshot_id`` from the sequence."""
    if not records:
        return 0

    ids = [r[0] for r in conn.execute("SELECT nextval('snapshot_id_seq') FROM range(?)", [len(records)]).fetchall()]
    snap_rows: list[tuple[Any, ...]] = []
    level_rows: list[tuple[Any, ...]] = []
    for sid, r in zip(ids, records):
        r.snapshot_id = int(sid)
        snap_rows.append(
            (
                r.ts, r.platform, r.ticker, r.yes_bid, r.yes_ask, r.no_bid, r.no_ask, r.volume,
                r.snapshot_id, r.observed_at_utc, r.exchange_updated_at_utc,
                r.ask_yes, r.ask_no, r.ask_yes_size, r.ask_no_size,
                r.bid_yes, r.bid_no, r.bid_yes_size, r.bid_no_size,
                r.market_status, r.tradable, r.data_source, r.depth_truncated, r.schema_version,
            )
        )
        for side, lvls in r.levels.items():
            for idx, (price, size) in enumerate(lvls):
                level_rows.append((r.snapshot_id, r.platform, r.ticker, side, idx, price, size))

    conn.execute("BEGIN")
    try:
        conn.executemany(
            """
            INSERT INTO orderbook_snapshots (
                ts, platform, ticker, yes_bid, yes_ask, no_bid, no_ask, volume,
                snapshot_id, observed_at_utc, exchange_updated_at_utc,
                ask_yes, ask_no, ask_yes_size, ask_no_size,
                bid_yes, bid_no, bid_yes_size, bid_no_size,
                market_status, tradable, data_source, depth_truncated, schema_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            snap_rows,
        )
        if level_rows:
            conn.executemany(
                "INSERT INTO orderbook_levels (snapshot_id, platform, ticker, side, level_idx, price, size) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                level_rows,
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return len(records)


def sync_verified_pairs(conn: duckdb.DuckDBPyConnection, pairs: Iterable[Any], now: Optional[datetime] = None) -> int:
    """Replace ``verified_pairs`` with the currently verified registry entries (research view depends on it)."""
    stamp = to_naive_utc(now or datetime.now(timezone.utc))
    rows = [
        (p.pair_id, p.category, p.kalshi_ticker, p.polymarket_condition_id, stamp)
        for p in pairs
        if getattr(p, "status", "verified") == "verified"
    ]
    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM verified_pairs")
        if rows:
            conn.executemany("INSERT INTO verified_pairs VALUES (?, ?, ?, ?, ?)", rows)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return len(rows)


def log_exclusions(
    conn: duckdb.DuckDBPyConnection,
    rows: Iterable[tuple[str, str, str]],
    checked_at: Optional[datetime] = None,
) -> int:
    """Append (pair_id, reason_code, detail) rows to ``pair_exclusions``."""
    stamp = to_naive_utc(checked_at or datetime.now(timezone.utc))
    data = [(stamp, pid, code, detail) for pid, code, detail in rows]
    if data:
        conn.executemany("INSERT INTO pair_exclusions VALUES (?, ?, ?, ?)", data)
    return len(data)


def view_exists(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    return bool(
        conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [name]).fetchone()[0]
    )
