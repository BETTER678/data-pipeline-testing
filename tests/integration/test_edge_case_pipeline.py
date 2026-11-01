"""End-to-end tests for the challenge's mandatory happy/empty/correction cases."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import text

from src.config import PipelineSettings
from src.pipeline import run_pipeline
from src.publishing import get_engine, table_counts
from src.schemas import ENTITY_SCHEMAS

pytestmark = pytest.mark.integration

CUSTOMER = {
    "customer_id": "CUST-001",
    "name": "Alice",
    "email": "alice@example.com",
    "country": "US",
    "created_at": "2026-01-01T00:00:00Z",
    "updated_at": "2026-01-02T00:00:00Z",
}
PRODUCT = {
    "product_id": "PROD-001",
    "name": "Mouse",
    "category": "Electronics",
    "unit_price": "10.00",
    "currency": "USD",
    "updated_at": "2026-01-01T00:00:00Z",
}
ORDER = {
    "order_id": "ORD-001",
    "customer_id": "CUST-001",
    "order_date": "2026-01-03T00:00:00Z",
    "status": "PAID",
    "total_amount": "10.00",
    "currency": "USD",
    "updated_at": "2026-01-04T00:00:00Z",
}
ITEM = {
    "item_id": "ITEM-001",
    "order_id": "ORD-001",
    "product_id": "PROD-001",
    "quantity": "1",
    "unit_price": "10.00",
    "updated_at": "2026-01-03T01:00:00Z",
}
PAYMENT = {
    "payment_id": "PAY-001",
    "order_id": "ORD-001",
    "amount": "10.00",
    "payment_method": "CARD",
    "payment_date": "2026-01-03T02:00:00Z",
    "status": "CAPTURED",
    "updated_at": "2026-01-04T02:00:00Z",
}


def _rows(customer: dict[str, Any] | None = None) -> dict[str, list[dict[str, Any]]]:
    return {
        "customers": [dict(customer or CUSTOMER)],
        "products": [PRODUCT],
        "orders": [ORDER],
        "order_items": [ITEM],
        "payments": [PAYMENT],
    }


LATE_PAYMENT_ORDER = {
    "order_id": "ORD-1001",
    "customer_id": "CUST-1001",
    "order_date": "2026-02-01T00:00:00Z",
    "status": "PAID",
    "total_amount": "140.99",
    "currency": "USD",
    "updated_at": "2026-02-02T00:00:00Z",
}


def _late_payment_rows(amount: str | None = None) -> dict[str, list[dict[str, Any]]]:
    rows: dict[str, list[dict[str, Any]]] = {
        "customers": [
            {
                "customer_id": "CUST-1001",
                "name": "Late Payment Customer",
                "email": "late@example.com",
                "country": "US",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-02T00:00:00Z",
            }
        ],
        "products": [
            {
                "product_id": "PROD-1001",
                "name": "Late Payment Product",
                "category": "Test",
                "unit_price": "140.99",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        ],
        "orders": [dict(LATE_PAYMENT_ORDER)],
        "order_items": [
            {
                "item_id": "ITEM-1001",
                "order_id": "ORD-1001",
                "product_id": "PROD-1001",
                "quantity": "1",
                "unit_price": "140.99",
                "updated_at": "2026-02-01T01:00:00Z",
            }
        ],
        "payments": [],
    }
    if amount is not None:
        rows["payments"] = [
            {
                "payment_id": "PAY-1001",
                "order_id": "ORD-1001",
                "amount": amount,
                "payment_method": "CARD",
                "payment_date": "2026-02-02T02:00:00Z",
                "status": "CAPTURED",
                "updated_at": "2026-02-02T03:00:00Z",
            }
        ]
    return rows


def _settings(tmp_path: Path) -> PipelineSettings:
    data_dir = tmp_path / "data"
    return PipelineSettings(
        raw_dir=data_dir / "raw",
        processed_dir=data_dir / "processed",
        rejected_dir=data_dir / "rejected",
        expected_dir=data_dir / "expected",
        reports_dir=tmp_path / "reports",
        db_url=f"sqlite:///{(data_dir / 'warehouse.db').as_posix()}",
        retry_base_delay=0.001,
        log_level="ERROR",
    )


def _write_dataset(
    settings: PipelineSettings,
    rows_by_entity: dict[str, list[dict[str, Any]]],
) -> None:
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    for entity, rows in rows_by_entity.items():
        columns = list(ENTITY_SCHEMAS[entity].source_columns)
        pd.DataFrame(rows, columns=columns).to_csv(settings.raw_dir / f"{entity}.csv", index=False)


def test_happy_path_publishes_every_entity(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write_dataset(settings, _rows())
    result = run_pipeline(settings=settings, run_id="edge-happy")

    assert result.status == "SUCCESS"
    assert result.error == ""
    assert result.metrics["input_rows"] == 5
    assert result.metrics["valid_rows"] == 5
    assert result.metrics["rejected_rows"] == 0
    assert result.metrics["duplicate_rows"] == 0
    assert result.metrics["published_rows"] == 5
    assert result.reconciliation["ok"] is True

    counts = table_counts(get_engine(settings.db_url))
    for entity in ("customers", "products", "orders", "order_items", "payments"):
        assert counts[entity] == 1


def test_empty_batch_is_success_and_publishes_nothing(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    for entity in ENTITY_SCHEMAS:
        columns = list(ENTITY_SCHEMAS[entity].source_columns)
        pd.DataFrame(columns=columns).to_csv(settings.raw_dir / f"{entity}.csv", index=False)

    result = run_pipeline(settings=settings, run_id="edge-empty")

    assert result.status == "SUCCESS"
    assert result.metrics["input_rows"] == 0
    assert result.metrics["published_rows"] == 0
    assert result.reconciliation["ok"] is True
    counts = table_counts(get_engine(settings.db_url))
    for entity in ("customers", "products", "orders", "order_items", "payments"):
        assert counts[entity] == 0


def test_second_batch_can_reference_parents_already_in_target(
    tmp_path: Path,
) -> None:
    """A later batch may add orders for customers/products from earlier batches."""

    settings = _settings(tmp_path)
    _write_dataset(settings, _rows())
    first = run_pipeline(settings=settings, run_id="batch-1")
    assert first.status == "SUCCESS"

    second_rows = {
        "customers": [
            {
                "customer_id": "CUST-002",
                "name": "Bob",
                "email": "bob@example.com",
                "country": "US",
                "created_at": "2026-01-05T00:00:00Z",
                "updated_at": "2026-01-06T00:00:00Z",
            }
        ],
        "products": [
            {
                "product_id": "PROD-002",
                "name": "Keyboard",
                "category": "Electronics",
                "unit_price": "10.00",
                "currency": "USD",
                "updated_at": "2026-01-05T00:00:00Z",
            }
        ],
        "orders": [
            {
                "order_id": "ORD-002",
                # CUST-001 exists only in the target, not in this batch.
                "customer_id": "CUST-001",
                "order_date": "2026-01-05T00:00:00Z",
                "status": "PAID",
                "total_amount": "10.00",
                "currency": "USD",
                "updated_at": "2026-01-06T00:00:00Z",
            }
        ],
        "order_items": [
            {
                "item_id": "ITEM-002",
                "order_id": "ORD-002",
                # PROD-001 exists only in the target, not in this batch.
                "product_id": "PROD-001",
                "quantity": "1",
                "unit_price": "10.00",
                "updated_at": "2026-01-05T01:00:00Z",
            }
        ],
        "payments": [
            {
                "payment_id": "PAY-002",
                "order_id": "ORD-002",
                "amount": "10.00",
                "payment_method": "CARD",
                "payment_date": "2026-01-05T02:00:00Z",
                "status": "CAPTURED",
                "updated_at": "2026-01-06T02:00:00Z",
            }
        ],
    }
    _write_dataset(settings, second_rows)
    second = run_pipeline(settings=settings, run_id="batch-2")

    assert second.status == "SUCCESS"
    assert second.metrics["input_rows"] == 5
    assert second.metrics["rejected_rows"] == 0
    assert second.metrics["published_rows"] == 5
    assert second.reconciliation["ok"] is True

    counts = table_counts(get_engine(settings.db_url))
    for entity in ("customers", "products", "orders", "order_items", "payments"):
        assert counts[entity] == 2


@pytest.mark.parametrize(
    ("amount", "expected_status", "expected_payments", "expected_quarantine"),
    [
        ("1.00", "PARTIAL_SUCCESS", 0, 1),
        ("140.99", "SUCCESS", 1, 0),
        ("139.59", "SUCCESS", 1, 0),
        ("139.58", "PARTIAL_SUCCESS", 0, 1),
    ],
    ids=["wrong-amount", "exact-amount", "inside-tolerance", "outside-tolerance"],
)
def test_late_payment_is_validated_against_published_order_totals(
    tmp_path: Path,
    amount: str,
    expected_status: str,
    expected_payments: int,
    expected_quarantine: int,
) -> None:
    settings = _settings(tmp_path)
    _write_dataset(settings, _late_payment_rows())
    first = run_pipeline(settings=settings, run_id="late-payment-parent")
    assert first.status == "SUCCESS"
    assert table_counts(get_engine(settings.db_url))["payments"] == 0

    second_rows = {entity: [] for entity in ENTITY_SCHEMAS}
    second_rows["payments"] = _late_payment_rows(amount)["payments"]
    _write_dataset(settings, second_rows)
    second = run_pipeline(settings=settings, run_id="late-payment-child")

    assert second.status == expected_status
    assert second.reconciliation["ok"] is True
    counts = table_counts(get_engine(settings.db_url))
    assert counts["payments"] == expected_payments
    assert counts["quarantine"] == expected_quarantine

    if expected_payments == 0:
        assert second.metrics["rejected_rows"] == 1
        engine = get_engine(settings.db_url)
        with engine.connect() as connection:
            rules = connection.execute(
                text("SELECT rejection_rules FROM quarantine WHERE source_record_id = 'PAY-1001'")
            ).scalar()
        assert "BUSINESS_PAYMENT_MISMATCH" in json.loads(rules)
    else:
        assert second.metrics["published_rows"] == 1


def test_same_key_same_version_different_payload_is_quarantined_not_failed(
    tmp_path: Path,
) -> None:
    """The dedup conflict path must survive entity-metric accounting."""

    settings = _settings(tmp_path)
    rows = {entity: [] for entity in ENTITY_SCHEMAS}
    rows["customers"] = [
        {
            **CUSTOMER,
            "customer_id": "CUST-900",
            "name": "Alice",
            "email": "alice@example.com",
        },
        {
            **CUSTOMER,
            "customer_id": "CUST-900",
            "name": "Bob",
            "email": "bob@example.com",
        },
    ]
    _write_dataset(settings, rows)

    result = run_pipeline(settings=settings, run_id="edge-dedup-conflict")

    assert result.status == "PARTIAL_SUCCESS"
    assert result.error == ""
    assert result.metrics["rejected_rows"] == 2
    assert result.metrics["entities"]["customers"]["rule_counts"] == {"DEDUP_VERSION_CONFLICT": 2}
    assert result.reconciliation["ok"] is True

    engine = get_engine(settings.db_url)
    with engine.connect() as connection:
        quarantined = (
            connection.execute(text("SELECT source_record_id, rejection_rules FROM quarantine"))
            .mappings()
            .all()
        )
    assert len(quarantined) == 2
    assert {str(row["source_record_id"]) for row in quarantined} == {"CUST-900"}
    assert all(
        "DEDUP_VERSION_CONFLICT" in json.loads(row["rejection_rules"]) for row in quarantined
    )


def test_corrected_previously_rejected_record_is_published_on_rerun(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    invalid_customer = dict(CUSTOMER, name="")
    _write_dataset(settings, _rows(invalid_customer))

    first = run_pipeline(settings=settings, run_id="edge-correction-1")
    assert first.status == "PARTIAL_SUCCESS"
    assert first.metrics["rejected_rows"] == 4
    assert first.metrics["published_rows"] == 1
    counts = table_counts(get_engine(settings.db_url))
    assert counts["customers"] == 0
    assert counts["orders"] == 0
    assert counts["order_items"] == 0
    assert counts["payments"] == 0
    assert counts["products"] == 1
    assert counts["quarantine"] == 4

    _write_dataset(settings, _rows())
    second = run_pipeline(settings=settings, run_id="edge-correction-2")

    assert second.status == "SUCCESS"
    assert second.metrics["rejected_rows"] == 0
    assert second.metrics["published_rows"] == 4
    assert second.metrics["unchanged_rows"] == 1
    counts = table_counts(get_engine(settings.db_url))
    for entity in ("customers", "products", "orders", "order_items", "payments"):
        assert counts[entity] == 1
    # The historical rejection evidence is retained, not silently deleted.
    assert counts["quarantine"] == 4
    assert second.reconciliation["ok"] is True
