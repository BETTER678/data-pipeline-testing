"""Transactional target publishing.

Properties
----------
* All entities for a run are published in one SQLite transaction.  A failure
  rolls the whole run back, so a retry cannot leave a half-published batch.
* Rows are matched by business primary key.
* An identical payload is a no-op (``unchanged``).
* A newer version updates the target and archives the replaced row in
  ``record_history``.
* An older version is archived as late-arriving and does not overwrite newer
  data.
* A same-version/different-payload row is quarantined as a conflict.
* Quarantine and history rows are idempotent via deterministic IDs.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import Engine, create_engine, event, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import StaticPool

from src import reconciliation
from src.config import (
    ALLOWED_CURRENCIES,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PAYMENT_STATUSES,
    PRIMARY_KEYS,
)
from src.monitoring import get_logger
from src.schemas import target_columns
from src.validation import make_rejected, rejection_rules

logger = get_logger(__name__)

ENTITY_ORDER = ("customers", "products", "orders", "order_items", "payments")


class PublishError(Exception):
    """Base class for publish-layer failures."""


class TransientPublishError(PublishError):
    """A failure that is safe to retry (e.g. a busy/locked database)."""


class TargetIntegrityError(PublishError):
    """Target checks failed after publishing; the transaction was rolled back."""

    def __init__(self, checks: dict[str, Any]):
        self.checks = checks
        super().__init__(f"Target integrity checks failed: {checks.get('failures')}")


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------
_CURRENCIES_SQL = ", ".join(f"'{value}'" for value in sorted(ALLOWED_CURRENCIES))
_ORDER_STATUS_SQL = ", ".join(f"'{value}'" for value in sorted(ORDER_STATUSES))
_PAYMENT_STATUS_SQL = ", ".join(f"'{value}'" for value in sorted(PAYMENT_STATUSES))
_PAYMENT_METHOD_SQL = ", ".join(f"'{value}'" for value in sorted(PAYMENT_METHODS))

DDL: dict[str, str] = {
    "customers": """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            country TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            _source_file TEXT NOT NULL,
            _source_row_number INTEGER NOT NULL,
            _run_id TEXT NOT NULL,
            _ingested_at TEXT NOT NULL,
            _pipeline_version TEXT NOT NULL,
            _record_hash TEXT NOT NULL,
            CHECK (length(customer_id) > 0)
        )
    """,
    "products": f"""
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            unit_price REAL NOT NULL CHECK (unit_price >= 0),
            currency TEXT NOT NULL CHECK (currency IN ({_CURRENCIES_SQL})),
            updated_at TEXT NOT NULL,
            _source_file TEXT NOT NULL,
            _source_row_number INTEGER NOT NULL,
            _run_id TEXT NOT NULL,
            _ingested_at TEXT NOT NULL,
            _pipeline_version TEXT NOT NULL,
            _record_hash TEXT NOT NULL
        )
    """,
    "orders": f"""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ({_ORDER_STATUS_SQL})),
            total_amount REAL NOT NULL CHECK (total_amount >= 0),
            currency TEXT NOT NULL CHECK (currency IN ({_CURRENCIES_SQL})),
            updated_at TEXT NOT NULL,
            order_year_month TEXT NOT NULL,
            _source_file TEXT NOT NULL,
            _source_row_number INTEGER NOT NULL,
            _run_id TEXT NOT NULL,
            _ingested_at TEXT NOT NULL,
            _pipeline_version TEXT NOT NULL,
            _record_hash TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """,
    "order_items": """
        CREATE TABLE IF NOT EXISTS order_items (
            item_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity >= 1),
            unit_price REAL NOT NULL CHECK (unit_price >= 0),
            updated_at TEXT NOT NULL,
            line_total REAL NOT NULL CHECK (line_total >= 0),
            _source_file TEXT NOT NULL,
            _source_row_number INTEGER NOT NULL,
            _run_id TEXT NOT NULL,
            _ingested_at TEXT NOT NULL,
            _pipeline_version TEXT NOT NULL,
            _record_hash TEXT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """,
    "payments": f"""
        CREATE TABLE IF NOT EXISTS payments (
            payment_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            amount REAL NOT NULL CHECK (amount > 0),
            payment_method TEXT NOT NULL CHECK (payment_method IN ({_PAYMENT_METHOD_SQL})),
            payment_date TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ({_PAYMENT_STATUS_SQL})),
            updated_at TEXT NOT NULL,
            _source_file TEXT NOT NULL,
            _source_row_number INTEGER NOT NULL,
            _run_id TEXT NOT NULL,
            _ingested_at TEXT NOT NULL,
            _pipeline_version TEXT NOT NULL,
            _record_hash TEXT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        )
    """,
    "record_history": """
        CREATE TABLE IF NOT EXISTS record_history (
            history_id TEXT PRIMARY KEY,
            entity TEXT NOT NULL,
            record_id TEXT NOT NULL,
            version_ts TEXT NOT NULL,
            record_hash TEXT,
            reason TEXT NOT NULL,
            first_run_id TEXT NOT NULL,
            last_seen_run_id TEXT NOT NULL,
            source_file TEXT,
            source_row_number INTEGER,
            payload TEXT NOT NULL,
            first_archived_at TEXT NOT NULL,
            last_archived_at TEXT NOT NULL,
            occurrence_count INTEGER NOT NULL DEFAULT 1
        )
    """,
    "quarantine": """
        CREATE TABLE IF NOT EXISTS quarantine (
            quarantine_id TEXT PRIMARY KEY,
            entity TEXT NOT NULL,
            source_record_id TEXT,
            source_file TEXT NOT NULL,
            source_row_number INTEGER,
            first_run_id TEXT NOT NULL,
            last_run_id TEXT NOT NULL,
            rejection_rules TEXT NOT NULL,
            rejection_reasons TEXT NOT NULL,
            raw_record TEXT NOT NULL,
            first_rejected_at TEXT NOT NULL,
            last_rejected_at TEXT NOT NULL,
            occurrence_count INTEGER NOT NULL DEFAULT 1
        )
    """,
    "pipeline_runs": """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            pipeline_version TEXT NOT NULL,
            source_file TEXT,
            start_timestamp TEXT NOT NULL,
            end_timestamp TEXT NOT NULL,
            duration_seconds REAL NOT NULL,
            input_rows INTEGER NOT NULL,
            valid_rows INTEGER NOT NULL,
            rejected_rows INTEGER NOT NULL,
            duplicate_rows INTEGER NOT NULL,
            published_rows INTEGER NOT NULL,
            unchanged_rows INTEGER NOT NULL,
            late_arriving_rows INTEGER NOT NULL,
            error_count INTEGER NOT NULL,
            retry_attempts INTEGER NOT NULL,
            status TEXT NOT NULL,
            failure_reason TEXT,
            metrics_json TEXT NOT NULL
        )
    """,
}

INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_items_order ON order_items(order_id)",
    "CREATE INDEX IF NOT EXISTS idx_items_product ON order_items(product_id)",
    "CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id)",
    "CREATE INDEX IF NOT EXISTS idx_quarantine_entity ON quarantine(entity, source_record_id)",
    "CREATE INDEX IF NOT EXISTS idx_history_record ON record_history(entity, record_id)",
    "CREATE INDEX IF NOT EXISTS idx_runs_status ON pipeline_runs(status)",
)


def get_engine(db_url: str) -> Engine:
    """Create an engine and enforce SQLite foreign keys on every connection."""

    kwargs: dict[str, Any] = {"echo": False, "future": True}
    if db_url in {"sqlite://", "sqlite:///:memory:"}:
        kwargs["connect_args"] = {"check_same_thread": False}
        kwargs["poolclass"] = StaticPool
    engine = create_engine(db_url, **kwargs)

    if engine.dialect.name == "sqlite":

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragmas(dbapi_connection, _connection_record):  # type: ignore[no-untyped-def]
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.close()

    return engine


def init_db(engine: Engine) -> Engine:
    with engine.begin() as connection:
        for entity in ENTITY_ORDER:
            connection.execute(text(DDL[entity]))
        connection.execute(text(DDL["record_history"]))
        connection.execute(text(DDL["quarantine"]))
        connection.execute(text(DDL["pipeline_runs"]))
        for statement in INDEXES:
            connection.execute(text(statement))
    logger.info("Database initialised url=%s", engine.url)
    return engine


# ---------------------------------------------------------------------------
# Result containers
# ---------------------------------------------------------------------------
@dataclass
class EntityPublishStats:
    entity: str
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    late_arriving: int = 0
    conflicts: int = 0

    @property
    def published(self) -> int:
        return self.inserted + self.updated

    @property
    def valid(self) -> int:
        return self.published + self.unchanged + self.late_arriving

    def as_dict(self) -> dict[str, int]:
        return {
            "inserted_rows": self.inserted,
            "updated_rows": self.updated,
            "published_rows": self.published,
            "unchanged_rows": self.unchanged,
            "late_arriving_rows": self.late_arriving,
            "publish_conflict_rows": self.conflicts,
        }


@dataclass
class PublishOutcome:
    entity_stats: dict[str, EntityPublishStats]
    publish_conflicts: dict[str, pd.DataFrame]
    target_checks: dict[str, Any]
    history_written: int = 0
    quarantine_written: int = 0

    @property
    def totals(self) -> dict[str, int]:
        fields = ("inserted", "updated", "unchanged", "late_arriving", "conflicts")
        return {
            field: sum(getattr(stats, field) for stats in self.entity_stats.values())
            for field in fields
        }


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------
def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a frame to DB-ready dictionaries, mapping NaN to NULL."""

    if frame.empty:
        return []
    clean = frame.astype(object).where(pd.notna(frame), None)
    return clean.to_dict("records")


def _chunks(items: list[Any], size: int = 400) -> Iterable[list[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def fetch_existing_keys(
    engine: Engine,
    entity: str,
    keys: Iterable[str],
    chunk_size: int = 400,
) -> set[str]:
    """Return only the requested primary keys that already exist in a target.

    Cross-batch referential integrity should not load entire parent tables.
    Referenced keys are sorted for deterministic SQL and queried in bounded
    chunks to stay below SQLite's bind-variable limit.
    """

    if entity not in PRIMARY_KEYS:
        raise ValueError(f"Unknown entity for key lookup: {entity!r}")
    key_list = sorted({str(key) for key in keys if str(key)})
    if not key_list:
        return set()
    pk = PRIMARY_KEYS[entity]
    existing: set[str] = set()
    with engine.connect() as connection:
        for chunk in _chunks(key_list, size=chunk_size):
            params = {f"k{index}": value for index, value in enumerate(chunk)}
            placeholders = ", ".join(f":k{index}" for index in range(len(chunk)))
            rows = connection.execute(
                text(f"SELECT {pk} FROM {entity} WHERE {pk} IN ({placeholders})"),
                params,
            )
            existing.update(str(value) for (value,) in rows if value is not None)
    return existing


def fetch_published_order_totals(
    engine: Engine, order_ids: Iterable[str], chunk_size: int = 400
) -> dict[str, float]:
    """Return totals for committed orders, querying only the requested keys."""

    key_list = sorted({str(order_id) for order_id in order_ids if str(order_id)})
    if not key_list:
        return {}
    totals: dict[str, float] = {}
    with engine.connect() as connection:
        for chunk in _chunks(key_list, size=chunk_size):
            params = {f"k{index}": value for index, value in enumerate(chunk)}
            placeholders = ", ".join(f":k{index}" for index in range(len(chunk)))
            rows = connection.execute(
                text(
                    f"SELECT order_id, total_amount FROM orders "
                    f"WHERE order_id IN ({placeholders})"
                ),
                params,
            )
            totals.update({str(order_id): float(total) for order_id, total in rows})
    return totals


def _fetch_existing(connection, entity: str, keys: Iterable[str]) -> dict[str, dict[str, Any]]:
    pk = PRIMARY_KEYS[entity]
    key_list = [str(key) for key in keys]
    existing: dict[str, dict[str, Any]] = {}
    for chunk in _chunks(key_list):
        params = {f"k{index}": value for index, value in enumerate(chunk)}
        placeholders = ", ".join(f":k{index}" for index in range(len(chunk)))
        result = connection.execute(
            text(f"SELECT * FROM {entity} WHERE {pk} IN ({placeholders})"), params
        )
        for row in result.mappings():
            existing[str(row[pk])] = dict(row)
    return existing


def _insert_rows(connection, entity: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    columns = [column for column in target_columns(entity) if column in frame.columns]
    if not columns:
        raise PublishError(f"No target columns available for {entity}")
    sql = (
        f"INSERT INTO {entity} ({', '.join(columns)}) VALUES "
        f"({', '.join(':' + column for column in columns)})"
    )
    for chunk in _chunks(_records(frame[columns]), size=500):
        connection.execute(text(sql), chunk)


def _update_rows(connection, entity: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    pk = PRIMARY_KEYS[entity]
    columns = [column for column in target_columns(entity) if column in frame.columns]
    update_columns = [column for column in columns if column != pk]
    sql = (
        f"UPDATE {entity} SET "
        + ", ".join(f"{column} = :{column}" for column in update_columns)
        + f" WHERE {pk} = :{pk}"
    )
    for chunk in _chunks(_records(frame[columns]), size=500):
        connection.execute(text(sql), chunk)


def _history_id(entity: str, record_id: str, version_ts: str, record_hash: str, reason: str) -> str:
    raw = f"{entity}|{record_id}|{version_ts}|{record_hash}|{reason}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _insert_history(
    connection,
    *,
    entity: str,
    record_id: str,
    version_ts: str,
    record_hash: str,
    reason: str,
    run_id: str,
    source_file: str | None,
    source_row_number: int | None,
    payload: str,
) -> None:
    now = datetime.now(UTC).isoformat()
    history_id = _history_id(entity, record_id, version_ts, record_hash, reason)
    connection.execute(
        text(
            """
            INSERT INTO record_history (
                history_id, entity, record_id, version_ts, record_hash, reason,
                first_run_id, last_seen_run_id, source_file, source_row_number,
                payload, first_archived_at, last_archived_at, occurrence_count
            ) VALUES (
                :history_id, :entity, :record_id, :version_ts, :record_hash, :reason,
                :run_id, :run_id, :source_file, :source_row_number,
                :payload, :now, :now, 1
            )
            ON CONFLICT(history_id) DO UPDATE SET
                last_seen_run_id = excluded.last_seen_run_id,
                last_archived_at = excluded.last_archived_at,
                occurrence_count = record_history.occurrence_count + 1
            """
        ),
        {
            "history_id": history_id,
            "entity": entity,
            "record_id": record_id,
            "version_ts": version_ts,
            "record_hash": record_hash,
            "reason": reason,
            "run_id": run_id,
            "source_file": source_file,
            "source_row_number": source_row_number,
            "payload": payload,
            "now": now,
        },
    )


def _payload_for_record(record: Mapping[str, Any]) -> str:
    business = {
        key: value
        for key, value in record.items()
        if key not in {"_source_file", "_source_row_number", "_run_id", "_ingested_at"}
    }
    return json.dumps(business, default=str, sort_keys=True)


def _make_publish_conflicts(frame: pd.DataFrame, positions: np.ndarray, pk: str) -> pd.DataFrame:
    if positions.size == 0:
        return pd.DataFrame()
    errors: list[list[tuple[str, str]]] = [[] for _ in range(len(frame))]
    for position in positions:
        row = frame.iloc[int(position)]
        errors[int(position)].append(
            (
                "PUBLISH_VERSION_CONFLICT",
                f"target already contains {pk}={row[pk]!r} with version "
                f"{row['updated_at']!r} but a different payload; "
                "incoming row quarantined instead of overwriting",
            )
        )
    return make_rejected(frame, positions, errors)


# ---------------------------------------------------------------------------
# Entity publish
# ---------------------------------------------------------------------------
@dataclass
class _PublishDecisions:
    insert_positions: list[int] = field(default_factory=list)
    update_positions: list[int] = field(default_factory=list)
    late_positions: list[int] = field(default_factory=list)
    conflict_positions: list[int] = field(default_factory=list)
    history: list[dict[str, Any]] = field(default_factory=list)


def _classify_publish_rows(
    frame: pd.DataFrame,
    existing: Mapping[str, Mapping[str, Any]],
    pk: str,
    stats: EntityPublishStats,
) -> _PublishDecisions:
    decisions = _PublishDecisions()
    for position, row in enumerate(frame.to_dict("records")):
        key = str(row[pk])
        current = existing.get(key)
        if current is None:
            decisions.insert_positions.append(position)
            continue
        if str(row["_record_hash"]) == str(current["_record_hash"]):
            stats.unchanged += 1
            continue
        if str(row["updated_at"]) < str(current["updated_at"]):
            decisions.late_positions.append(position)
            continue
        if str(row["updated_at"]) == str(current["updated_at"]):
            decisions.conflict_positions.append(position)
            continue
        decisions.update_positions.append(position)
        decisions.history.append(
            {
                "record_id": key,
                "version_ts": str(current["updated_at"]),
                "record_hash": str(current["_record_hash"]),
                "reason": "REPLACED_BY_NEWER_VERSION",
                "payload": _payload_for_record(current),
                "source_file": current.get("_source_file"),
                "source_row_number": current.get("_source_row_number"),
            }
        )
    return decisions


def _archive_versions(connection, entity: str, history: list[dict[str, Any]], run_id: str) -> None:
    for item in history:
        _insert_history(
            connection,
            entity=entity,
            record_id=item["record_id"],
            version_ts=item["version_ts"],
            record_hash=item["record_hash"],
            reason=item["reason"],
            run_id=run_id,
            source_file=item["source_file"],
            source_row_number=item["source_row_number"],
            payload=item["payload"],
        )


def _archive_late_versions(
    connection,
    entity: str,
    frame: pd.DataFrame,
    positions: list[int],
    pk: str,
    run_id: str,
) -> None:
    for position in positions:
        row = frame.iloc[position].to_dict()
        _insert_history(
            connection,
            entity=entity,
            record_id=str(row[pk]),
            version_ts=str(row["updated_at"]),
            record_hash=str(row["_record_hash"]),
            reason="LATE_ARRIVING_OLDER_VERSION",
            run_id=run_id,
            source_file=row.get("_source_file"),
            source_row_number=int(row.get("_source_row_number") or 0),
            payload=_payload_for_record(row),
        )


def _publish_entity(
    connection,
    entity: str,
    frame: pd.DataFrame,
    run_id: str,
) -> tuple[EntityPublishStats, pd.DataFrame]:
    stats = EntityPublishStats(entity=entity)
    if frame.empty:
        return stats, pd.DataFrame()

    pk = PRIMARY_KEYS[entity]
    frame = frame.reset_index(drop=True)
    existing = _fetch_existing(connection, entity, frame[pk].astype(str))
    decisions = _classify_publish_rows(frame, existing, pk, stats)

    if decisions.insert_positions:
        _insert_rows(connection, entity, frame.iloc[decisions.insert_positions])
        stats.inserted = len(decisions.insert_positions)
    if decisions.update_positions:
        _update_rows(connection, entity, frame.iloc[decisions.update_positions])
        stats.updated = len(decisions.update_positions)

    _archive_versions(connection, entity, decisions.history, run_id)
    _archive_late_versions(connection, entity, frame, decisions.late_positions, pk, run_id)
    stats.late_arriving = len(decisions.late_positions)
    stats.conflicts = len(decisions.conflict_positions)

    conflicts = _make_publish_conflicts(
        frame, np.array(decisions.conflict_positions, dtype=int), pk
    )
    logger.info(
        "Published entity=%s inserted=%d updated=%d unchanged=%d late=%d conflicts=%d",
        entity,
        stats.inserted,
        stats.updated,
        stats.unchanged,
        stats.late_arriving,
        stats.conflicts,
    )
    return stats, conflicts


# ---------------------------------------------------------------------------
# Quarantine and history from validation/dedup
# ---------------------------------------------------------------------------
def _quarantine_id(
    entity: str, source_file: str, source_row: int, rules: list[str], raw: str
) -> str:
    seed = "|".join([entity, source_file, str(source_row), ",".join(sorted(rules)), raw])
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def save_quarantine(
    connection,
    rejected_by_entity: Mapping[str, pd.DataFrame],
    run_id: str,
) -> int:
    """Upsert rejected records; repeated runs increment occurrence_count."""

    now = datetime.now(UTC).isoformat()
    written = 0
    for entity, frame in rejected_by_entity.items():
        if frame is None or frame.empty:
            continue
        pk = PRIMARY_KEYS.get(entity)
        for row in frame.to_dict("records"):
            rules = json.loads(row.get("_reject_rules") or "[]")
            reasons = json.loads(row.get("_reject_reasons") or "[]")
            raw = row.get("_raw_record") or "{}"
            source_file = str(row.get("_source_file") or "")
            source_row = int(row.get("_source_row_number") or 0)
            record_id = (
                str(row.get(pk)) if pk and row.get(pk) not in (None, "") else f"row:{source_row}"
            )
            quarantine_id = _quarantine_id(entity, source_file, source_row, rules, raw)
            connection.execute(
                text(
                    """
                    INSERT INTO quarantine (
                        quarantine_id, entity, source_record_id, source_file,
                        source_row_number, first_run_id, last_run_id,
                        rejection_rules, rejection_reasons, raw_record,
                        first_rejected_at, last_rejected_at, occurrence_count
                    ) VALUES (
                        :quarantine_id, :entity, :record_id, :source_file,
                        :source_row, :run_id, :run_id,
                        :rules, :reasons, :raw,
                        :now, :now, 1
                    )
                    ON CONFLICT(quarantine_id) DO UPDATE SET
                        last_run_id = excluded.last_run_id,
                        rejection_reasons = excluded.rejection_reasons,
                        last_rejected_at = excluded.last_rejected_at,
                        occurrence_count = quarantine.occurrence_count + 1
                    """
                ),
                {
                    "quarantine_id": quarantine_id,
                    "entity": entity,
                    "record_id": record_id,
                    "source_file": source_file,
                    "source_row": source_row,
                    "run_id": run_id,
                    "rules": json.dumps(rules),
                    "reasons": json.dumps(reasons),
                    "raw": raw,
                    "now": now,
                },
            )
            written += 1
    logger.info("Quarantine upsert run_id=%s rows=%d", run_id, written)
    return written


def archive_batch_duplicates(
    connection,
    duplicates_by_entity: Mapping[str, pd.DataFrame],
    run_id: str,
) -> int:
    """Archive in-batch duplicate/superseded source versions in history."""

    written = 0
    for entity, frame in duplicates_by_entity.items():
        if frame is None or frame.empty:
            continue
        pk = PRIMARY_KEYS[entity]
        for row in frame.to_dict("records"):
            rules = rejection_rules(pd.DataFrame([row]))
            reason = "SUPERSEDED_VERSION" if "DEDUP_SUPERSEDED" in rules else "EXACT_DUPLICATE"
            record_hash = str(
                row.get("_dedup_hash")
                or hashlib.sha256(str(row.get("_raw_record")).encode()).hexdigest()
            )
            _insert_history(
                connection,
                entity=entity,
                record_id=str(row.get(pk)),
                version_ts=str(row.get("updated_at")),
                record_hash=record_hash,
                reason=reason,
                run_id=run_id,
                source_file=str(row.get("_source_file") or ""),
                source_row_number=int(row.get("_source_row_number") or 0),
                payload=str(row.get("_raw_record") or "{}"),
            )
            written += 1
    if written:
        logger.info("Archived in-batch duplicate versions rows=%d", written)
    return written


# ---------------------------------------------------------------------------
# Public transaction entry point
# ---------------------------------------------------------------------------
def _publish_all_entities(
    connection,
    canonical_by_entity: Mapping[str, pd.DataFrame],
    run_id: str,
) -> tuple[dict[str, EntityPublishStats], dict[str, pd.DataFrame]]:
    entity_stats: dict[str, EntityPublishStats] = {}
    publish_conflicts: dict[str, pd.DataFrame] = {}
    for entity in ENTITY_ORDER:
        canonical = canonical_by_entity.get(entity, pd.DataFrame())
        stats, conflicts = _publish_entity(connection, entity, canonical, run_id)
        entity_stats[entity] = stats
        publish_conflicts[entity] = conflicts
        if stats.valid + stats.conflicts != len(canonical):
            raise reconciliation.ReconciliationError(
                f"Publish accounting failed for {entity}: canonical={len(canonical)} "
                f"published={stats.valid} conflicts={stats.conflicts}"
            )
    return entity_stats, publish_conflicts


def _combine_rejected(
    rejected_by_entity: Mapping[str, pd.DataFrame],
    publish_conflicts: Mapping[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    combined = dict(rejected_by_entity)
    for entity, conflicts in publish_conflicts.items():
        if conflicts is None or conflicts.empty:
            continue
        if entity in combined and not combined[entity].empty:
            combined[entity] = pd.concat([combined[entity], conflicts], ignore_index=True)
        else:
            combined[entity] = conflicts
    return combined


def publish_run(
    engine: Engine,
    canonical_by_entity: Mapping[str, pd.DataFrame],
    rejected_by_entity: Mapping[str, pd.DataFrame],
    duplicates_by_entity: Mapping[str, pd.DataFrame],
    run_id: str,
    verify: Callable[[PublishOutcome], None] | None = None,
) -> PublishOutcome:
    """Publish all entities, archive history/quarantine, and verify the target.

    The whole run is one transaction.  If target checks fail, the optional
    ``verify`` callback raises, or any SQL statement raises, everything is
    rolled back and the caller can safely retry.  The callback runs after target
    checks and before commit so reconciliation failures cannot leave published
    data behind.
    """

    with engine.begin() as connection:
        entity_stats, publish_conflicts = _publish_all_entities(
            connection, canonical_by_entity, run_id
        )
        history_written = archive_batch_duplicates(connection, duplicates_by_entity, run_id)
        quarantine_written = save_quarantine(
            connection, _combine_rejected(rejected_by_entity, publish_conflicts), run_id
        )
        target_checks = reconciliation.run_target_checks(connection)
        if not target_checks.get("ok", False):
            raise TargetIntegrityError(target_checks)
        outcome = PublishOutcome(
            entity_stats=entity_stats,
            publish_conflicts=publish_conflicts,
            target_checks=target_checks,
            history_written=history_written,
            quarantine_written=quarantine_written,
        )
        if verify is not None:
            verify(outcome)
    return outcome


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------
def is_transient(error: Exception) -> bool:
    if isinstance(error, TransientPublishError):
        return True
    if isinstance(error, OperationalError):
        message = str(error).lower()
        return any(token in message for token in ("locked", "busy", "timeout"))
    return False


def retry_call(
    func: Callable[[], Any],
    *,
    max_attempts: int = 3,
    base_delay: float = 0.2,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> tuple[Any, int]:
    """Run ``func`` with exponential backoff on transient errors.

    Returns ``(result, attempts_used)``.  Permanent errors are re-raised
    immediately.
    """

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return func(), attempt
        except Exception as exc:  # classified below
            last_error = exc
            if attempt >= max_attempts or not is_transient(exc):
                raise
            if on_retry is not None:
                on_retry(attempt, exc)
            time.sleep(base_delay * (2 ** (attempt - 1)))
    assert last_error is not None
    raise last_error


# ---------------------------------------------------------------------------
# Run metadata
# ---------------------------------------------------------------------------
def save_run_metrics(metrics: dict[str, Any], engine: Engine) -> None:
    payload = {
        "run_id": metrics["run_id"],
        "pipeline_version": metrics["pipeline_version"],
        "source_file": metrics.get("source_file", ""),
        "start_timestamp": metrics["start_timestamp"],
        "end_timestamp": metrics["end_timestamp"],
        "duration_seconds": float(metrics["duration_seconds"]),
        "input_rows": int(metrics["input_rows"]),
        "valid_rows": int(metrics["valid_rows"]),
        "rejected_rows": int(metrics["rejected_rows"]),
        "duplicate_rows": int(metrics["duplicate_rows"]),
        "published_rows": int(metrics["published_rows"]),
        "unchanged_rows": int(metrics["unchanged_rows"]),
        "late_arriving_rows": int(metrics["late_arriving_rows"]),
        "error_count": int(metrics["error_count"]),
        "retry_attempts": int(metrics.get("retry_attempts", 0)),
        "status": metrics["status"],
        "failure_reason": metrics.get("failure_reason", ""),
        "metrics_json": json.dumps(metrics, default=str),
    }
    with engine.begin() as connection:
        columns = list(payload)
        connection.execute(
            text(
                f"INSERT OR REPLACE INTO pipeline_runs ({', '.join(columns)}) "
                f"VALUES ({', '.join(':' + column for column in columns)})"
            ),
            payload,
        )
    logger.info(
        "Run metrics saved run_id=%s status=%s",
        metrics["run_id"],
        metrics["status"],
    )


def table_counts(engine: Engine) -> dict[str, int]:
    counts: dict[str, int] = {}
    with engine.connect() as connection:
        for entity in ENTITY_ORDER:
            counts[entity] = int(
                connection.execute(text(f"SELECT COUNT(*) FROM {entity}")).scalar() or 0
            )
        for table in ("quarantine", "record_history", "pipeline_runs"):
            counts[table] = int(
                connection.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0
            )
    return counts
