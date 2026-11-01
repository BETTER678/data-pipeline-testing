"""Shared small helpers for the test-suite."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

import pandas as pd

from src.generator import _small_frames
from src.schemas import ENTITY_SCHEMAS


def source_frame(
    entity: str,
    rows: Iterable[dict[str, Any]],
    *,
    source_file: str | None = None,
    row_numbers: Iterable[int] | None = None,
) -> pd.DataFrame:
    """Build a raw CSV-shaped frame with ingestion lineage columns."""

    columns = list(ENTITY_SCHEMAS[entity].source_columns)
    row_list = [dict(row) for row in rows]
    frame = pd.DataFrame(row_list, columns=columns).astype(object)
    frame["_source_file"] = source_file or f"{entity}.csv"
    if row_numbers is None:
        frame["_source_row_number"] = pd.Series(range(1, len(frame) + 1), dtype="int64")
    else:
        frame["_source_row_number"] = pd.Series(list(row_numbers), dtype="int64")
    frame["_run_id"] = "test-run"
    frame["_ingested_at"] = "2026-01-01T00:00:00+00:00"
    return frame


def small_rows(entity: str) -> list[dict[str, Any]]:
    """Return the curated small-dataset rows for one entity."""

    _columns, rows = _small_frames()[entity]
    return [dict(row) for row in rows]


def rejection_rules_for(rejected: pd.DataFrame, pk: str, value: str) -> list[str]:
    """Return the flat rule list for one rejected primary key."""

    if rejected.empty:
        return []
    match = rejected[rejected[pk].astype(str) == str(value)]
    if match.empty:
        return []
    rules: list[str] = []
    for raw in match["_reject_rules"]:
        rules.extend(json.loads(raw))
    return rules


def hash_frame(frame: pd.DataFrame) -> str:
    """Stable content hash for comparing snapshots in tests."""

    import hashlib

    payload = frame.sort_index(axis=1).to_csv(index=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def valid_minimal_frames() -> dict[str, pd.DataFrame]:
    """A tiny, cross-entity-consistent transformed dataset."""

    from src.transformations import transform
    from src.validation import validate_rows

    rows: dict[str, list[dict[str, Any]]] = {
        "customers": [
            {
                "customer_id": "CUST-001",
                "name": "Alice",
                "email": "alice@example.com",
                "country": "US",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-02T00:00:00Z",
            }
        ],
        "products": [
            {
                "product_id": "PROD-001",
                "name": "Mouse",
                "category": "Electronics",
                "unit_price": "10.00",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        ],
        "orders": [
            {
                "order_id": "ORD-001",
                "customer_id": "CUST-001",
                "order_date": "2026-01-03T00:00:00Z",
                "status": "PAID",
                "total_amount": "10.00",
                "currency": "USD",
                "updated_at": "2026-01-04T00:00:00Z",
            }
        ],
        "order_items": [
            {
                "item_id": "ITEM-001",
                "order_id": "ORD-001",
                "product_id": "PROD-001",
                "quantity": "1",
                "unit_price": "10.00",
                "updated_at": "2026-01-03T01:00:00Z",
            }
        ],
        "payments": [
            {
                "payment_id": "PAY-001",
                "order_id": "ORD-001",
                "amount": "10.00",
                "payment_method": "CARD",
                "payment_date": "2026-01-03T02:00:00Z",
                "status": "CAPTURED",
                "updated_at": "2026-01-04T02:00:00Z",
            }
        ],
    }
    frames: dict[str, pd.DataFrame] = {}
    for entity, entity_rows in rows.items():
        outcome = validate_rows(source_frame(entity, entity_rows), entity)
        assert outcome.rejected.empty, outcome.rejected
        frames[entity] = transform(outcome.valid, entity)
    return frames
