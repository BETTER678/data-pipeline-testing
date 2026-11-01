"""Reconciliation and target integrity checks.

Two layers are reported:

1. Source accounting — every input row must be explained as rejected, duplicate,
   published/updated, unchanged, or late-arriving.  The identities are:
   ``input = rejected + duplicates + valid`` and
   ``valid = published + unchanged + late_arriving``.
2. Target integrity — primary keys, foreign keys, nulls, timestamp formats,
   ranges and business aggregates are checked with SQL on the same transaction
   that published the data.  A failure rolls the transaction back.
"""

from __future__ import annotations

import csv
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, text

from src.config import (
    ENTITIES,
    FOREIGN_KEYS,
    MAX_AMOUNT,
    MAX_QUANTITY,
    MAX_UNIT_PRICE,
    MONEY_TOLERANCE_ABS,
    PRIMARY_KEYS,
)
from src.monitoring import get_logger
from src.schemas import ENTITY_SCHEMAS, TIMESTAMP_PATTERN

logger = get_logger(__name__)
_TS_RE = re.compile(TIMESTAMP_PATTERN)


class ReconciliationError(Exception):
    """Source accounting does not balance or the target is not consistent."""


def _scalar(connection, sql: str, params: Mapping[str, Any] | None = None) -> Any:
    return connection.execute(text(sql), params or {}).scalar()


def _count(connection, sql: str) -> int:
    return int(_scalar(connection, sql) or 0)


def _check(
    checks: dict[str, dict[str, Any]],
    name: str,
    value: int | float,
    *,
    threshold: float = 0,
    hard: bool = True,
    detail: str = "",
) -> None:
    checks[name] = {
        "value": value,
        "threshold": threshold,
        "passed": bool(value <= threshold),
        "hard": hard,
        "detail": detail,
    }


def _timestamp_issues(connection) -> int:
    issues = 0
    for entity in ENTITIES:
        schema = ENTITY_SCHEMAS[entity]
        timestamp_columns = [
            column.name for column in schema.columns if column.dtype == "timestamp"
        ]
        if not timestamp_columns:
            continue
        select = ", ".join(timestamp_columns)
        for row in connection.execute(text(f"SELECT {select} FROM {entity}")):
            for value in row:
                if value is None or not _TS_RE.fullmatch(str(value)):
                    issues += 1
    return issues


def _entity_integrity_checks(
    connection,
    checks: dict[str, dict[str, Any]],
    row_counts: dict[str, int],
) -> None:
    """Check row counts, primary keys, required fields and numeric ranges."""

    for entity in ENTITIES:
        row_counts[entity] = _count(connection, f"SELECT COUNT(*) FROM {entity}")
        pk = PRIMARY_KEYS[entity]
        _check(
            checks,
            f"{entity}.pk_unique",
            _count(
                connection,
                f"SELECT COUNT(*) - COUNT(DISTINCT {pk}) FROM {entity}",
            ),
            detail=f"{entity}.{pk} must be unique",
        )

        schema = ENTITY_SCHEMAS[entity]
        null_sql = " + ".join(
            f"SUM(CASE WHEN {column} IS NULL OR "
            f"TRIM(CAST({column} AS TEXT)) = '' THEN 1 ELSE 0 END)"
            for column in schema.required_columns
        )
        _check(
            checks,
            f"{entity}.required_fields",
            _count(connection, f"SELECT COALESCE({null_sql}, 0) FROM {entity}"),
            detail="required columns must not be null/blank",
        )

        range_parts: list[str] = []
        for column in schema.columns:
            if column.dtype not in {"integer", "float"}:
                continue
            if column.min_value is not None:
                operator = "<=" if column.min_exclusive else "<"
                range_parts.append(
                    f"SUM(CASE WHEN {column.name} {operator} {column.min_value} "
                    "THEN 1 ELSE 0 END)"
                )
            if column.max_value is not None:
                range_parts.append(
                    f"SUM(CASE WHEN {column.name} > {column.max_value} " "THEN 1 ELSE 0 END)"
                )
        _check(
            checks,
            f"{entity}.ranges",
            _count(
                connection,
                f"SELECT COALESCE({' + '.join(range_parts) if range_parts else '0'}, 0) "
                f"FROM {entity}",
            ),
            detail="numeric business ranges",
        )


def _explicit_numeric_checks(connection, checks: dict[str, dict[str, Any]]) -> None:
    """Check upper bounds that the DDL itself does not encode."""

    _check(
        checks,
        "order_items.quantity_bounds",
        _count(
            connection,
            f"SELECT COUNT(*) FROM order_items WHERE quantity < 1 OR quantity > {MAX_QUANTITY}",
        ),
    )
    _check(
        checks,
        "order_items.unit_price_bounds",
        _count(
            connection,
            f"SELECT COUNT(*) FROM order_items WHERE unit_price < 0 OR unit_price > {MAX_UNIT_PRICE}",
        ),
    )
    _check(
        checks,
        "payments.amount_bounds",
        _count(
            connection,
            f"SELECT COUNT(*) FROM payments WHERE amount <= 0 OR amount > {MAX_AMOUNT}",
        ),
    )


def _foreign_key_checks(connection, checks: dict[str, dict[str, Any]]) -> None:
    """Verify every declared child-to-parent relationship in the target."""

    for child, mapping in FOREIGN_KEYS.items():
        for column, (parent, parent_pk) in mapping.items():
            value = _count(
                connection,
                f"SELECT COUNT(*) FROM {child} c LEFT JOIN {parent} p "
                f"ON c.{column} = p.{parent_pk} WHERE p.{parent_pk} IS NULL",
            )
            _check(
                checks,
                f"{child}.{column}_fk",
                value,
                detail=f"{child}.{column} -> {parent}.{parent_pk}",
            )


def _timestamp_checks(connection, checks: dict[str, dict[str, Any]]) -> None:
    _check(
        checks,
        "timestamps.format",
        _timestamp_issues(connection),
        detail="target timestamps must be ISO-8601 UTC",
    )


def _order_item_checks(connection, checks: dict[str, dict[str, Any]]) -> None:
    item_summary = """
        SELECT order_id, COUNT(*) AS item_count, SUM(line_total) AS item_total
        FROM order_items GROUP BY order_id
    """
    total_mismatch = _count(
        connection,
        f"""
        WITH item_sums AS ({item_summary})
        SELECT COUNT(*) FROM orders o
        LEFT JOIN item_sums i ON o.order_id = i.order_id
        WHERE i.order_id IS NULL
           OR ABS(o.total_amount - i.item_total) > {MONEY_TOLERANCE_ABS}
        """,
    )
    _check(
        checks,
        "orders.total_matches_items",
        total_mismatch,
        detail="order total must equal sum of accepted line totals",
    )
    _check(
        checks,
        "orders.have_items",
        _count(
            connection,
            "SELECT COUNT(*) FROM orders o LEFT JOIN order_items i "
            "ON o.order_id = i.order_id WHERE i.order_id IS NULL",
        ),
    )


def _collect_aggregates(connection, aggregates: dict[str, float]) -> None:
    order_total_sum = float(
        _scalar(connection, "SELECT COALESCE(SUM(total_amount), 0) FROM orders") or 0
    )
    line_total_sum = float(
        _scalar(connection, "SELECT COALESCE(SUM(line_total), 0) FROM order_items") or 0
    )
    captured_sum = float(
        _scalar(
            connection,
            "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE status = 'CAPTURED'",
        )
        or 0
    )
    aggregates["order_total_sum"] = round(order_total_sum, 2)
    aggregates["line_total_sum"] = round(line_total_sum, 2)
    aggregates["captured_payment_sum"] = round(captured_sum, 2)


def _payment_checks(connection, checks: dict[str, dict[str, Any]]) -> None:
    paid_summary = """
        WITH paid AS (
            SELECT order_id, SUM(amount) AS paid
            FROM payments WHERE status = 'CAPTURED' GROUP BY order_id
        )
    """
    _check(
        checks,
        "payments.no_overpayment",
        _count(
            connection,
            f"""
            {paid_summary}
            SELECT COUNT(*) FROM orders o
            LEFT JOIN paid p ON o.order_id = p.order_id
            WHERE COALESCE(p.paid, 0) > o.total_amount + {MONEY_TOLERANCE_ABS}
            """,
        ),
        detail="captured payments per order must not exceed order total",
    )
    underpaid = _count(
        connection,
        f"""
        {paid_summary}
        SELECT COUNT(*) FROM orders o
        LEFT JOIN paid p ON o.order_id = p.order_id
        WHERE COALESCE(p.paid, 0) < o.total_amount - {MONEY_TOLERANCE_ABS}
        """,
    )
    _check(
        checks,
        "payments.underpaid_orders",
        underpaid,
        hard=False,
        detail="informational: zero-total/unpaid orders are legitimate",
    )


def _business_integrity_checks(
    connection,
    checks: dict[str, dict[str, Any]],
    aggregates: dict[str, float],
) -> None:
    """Check order/item/payment consistency and collect aggregate totals."""

    _order_item_checks(connection, checks)
    _collect_aggregates(connection, aggregates)
    _payment_checks(connection, checks)


def run_target_checks(connection) -> dict[str, Any]:
    """Run SQL/Python integrity checks against the current transaction."""

    checks: dict[str, dict[str, Any]] = {}
    row_counts: dict[str, int] = {}
    aggregates: dict[str, float] = {}

    _entity_integrity_checks(connection, checks, row_counts)
    _explicit_numeric_checks(connection, checks)
    _foreign_key_checks(connection, checks)
    _timestamp_checks(connection, checks)
    _business_integrity_checks(connection, checks, aggregates)

    failures = [
        {"check": name, **payload}
        for name, payload in checks.items()
        if payload["hard"] and not payload["passed"]
    ]
    result = {
        "checked_at": datetime.now(UTC).isoformat(),
        "ok": not failures,
        "failures": failures,
        "row_counts": row_counts,
        "checks": checks,
        "aggregates": aggregates,
    }
    if failures:
        logger.error("Target integrity failures: %s", failures)
    else:
        logger.info("Target integrity PASS counts=%s aggregates=%s", row_counts, aggregates)
    return result


def _entity_reconciliation(
    entity_metrics: Mapping[str, Mapping[str, Any]],
    target_checks: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], bool]:
    entities: dict[str, dict[str, Any]] = {}
    all_balanced = True

    for entity in ENTITIES:
        metrics = entity_metrics.get(entity, {})
        input_rows = int(metrics.get("source_rows", 0))
        rejected = int(metrics.get("rejected_rows", 0))
        duplicates = int(metrics.get("duplicate_rows", 0))
        valid = int(metrics.get("valid_rows", 0))
        published = int(metrics.get("published_rows", 0))
        unchanged = int(metrics.get("unchanged_rows", 0))
        late = int(metrics.get("late_arriving_rows", 0))

        input_accounted = rejected + duplicates + valid
        valid_accounted = published + unchanged + late
        balanced = input_accounted == input_rows and valid_accounted == valid
        all_balanced = all_balanced and balanced
        entities[entity] = {
            "source_rows": input_rows,
            "rejected_rows": rejected,
            "duplicate_rows": duplicates,
            "valid_rows": valid,
            "published_rows": published,
            "unchanged_rows": unchanged,
            "late_arriving_rows": late,
            "input_accounted": input_accounted,
            "valid_accounted": valid_accounted,
            "unaccounted_rows": input_rows - input_accounted,
            "valid_unaccounted_rows": valid - valid_accounted,
            "balanced": balanced,
            "target_rows": int(target_checks.get("row_counts", {}).get(entity, 0)),
        }
    return entities, all_balanced


def _reconciliation_totals(entities: Mapping[str, Mapping[str, Any]]) -> dict[str, int]:
    totals = {"input_rows": sum(int(entity["source_rows"]) for entity in entities.values())}
    for field in (
        "rejected_rows",
        "duplicate_rows",
        "valid_rows",
        "published_rows",
        "unchanged_rows",
        "late_arriving_rows",
        "target_rows",
    ):
        totals[field] = sum(int(entity[field]) for entity in entities.values())
    return totals


def build_reconciliation(
    run_id: str,
    entity_metrics: Mapping[str, Mapping[str, Any]],
    target_checks: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the source-accounting + target-integrity reconciliation report."""

    entities, all_balanced = _entity_reconciliation(entity_metrics, target_checks)
    report = {
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "entities": entities,
        "totals": _reconciliation_totals(entities),
        "source_accounting_balanced": all_balanced,
        "target_checks": target_checks,
        "target_checks_ok": bool(target_checks.get("ok", False)),
        "ok": all_balanced and bool(target_checks.get("ok", False)),
    }
    if not all_balanced:
        logger.error("Source reconciliation failed run_id=%s report=%s", run_id, entities)
    return report


def write_reconciliation_report(
    report: Mapping[str, Any], reports_dir: Path, run_id: str
) -> dict[str, Path]:
    """Write reconciliation JSON and CSV artifacts for a run."""
    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / f"reconciliation_{run_id}.json"
    csv_path = reports_dir / f"reconciliation_{run_id}.csv"
    json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    fieldnames = [
        "entity",
        "source_rows",
        "rejected_rows",
        "duplicate_rows",
        "valid_rows",
        "published_rows",
        "unchanged_rows",
        "late_arriving_rows",
        "unaccounted_rows",
        "balanced",
        "target_rows",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for entity, row in report["entities"].items():
            writer.writerow({"entity": entity, **{key: row[key] for key in fieldnames[1:]}})
    logger.info("Reconciliation report written run_id=%s", run_id)
    return {"json": json_path, "csv": csv_path}


# ---------------------------------------------------------------------------
# Small helpers used by tests and evidence scripts
# ---------------------------------------------------------------------------
def query_published_counts(engine: Engine) -> dict[str, int]:
    """Return committed row counts for each target entity."""
    with engine.connect() as connection:
        return {entity: _count(connection, f"SELECT COUNT(*) FROM {entity}") for entity in ENTITIES}


def verify_pk_uniqueness(engine: Engine, entity: str) -> bool:
    """Return True when an entity has no duplicate primary keys."""
    pk = PRIMARY_KEYS[entity]
    with engine.connect() as connection:
        duplicates = _count(
            connection,
            f"SELECT COUNT(*) - COUNT(DISTINCT {pk}) FROM {entity}",
        )
    return duplicates == 0


def verify_fk_integrity(
    engine: Engine, child: str, column: str, parent: str, parent_pk: str
) -> int:
    """Return the number of orphaned child rows for one foreign key."""
    with engine.connect() as connection:
        return _count(
            connection,
            f"SELECT COUNT(*) FROM {child} c LEFT JOIN {parent} p "
            f"ON c.{column} = p.{parent_pk} WHERE p.{parent_pk} IS NULL",
        )
