"""Unit tests for transactional publishing, idempotency, retry and history."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from src.deduplication import deduplicate
from src.publishing import (
    TargetIntegrityError,
    TransientPublishError,
    fetch_existing_keys,
    fetch_published_order_totals,
    is_transient,
    publish_run,
    retry_call,
    table_counts,
)
from src.reconciliation import ReconciliationError
from src.transformations import transform
from src.validation import validate_rows
from tests.helpers import source_frame, valid_minimal_frames

pytestmark = pytest.mark.unit


def _transformed(entity: str, rows: list[dict[str, Any]]) -> pd.DataFrame:
    outcome = validate_rows(source_frame(entity, rows), entity)
    assert outcome.rejected.empty, outcome.rejected
    return transform(outcome.valid, entity)


def _base_frames() -> dict[str, pd.DataFrame]:
    customer = {
        "customer_id": "CUST-001",
        "name": "Alice",
        "email": "alice@example.com",
        "country": "US",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
    }
    product = {
        "product_id": "PROD-001",
        "name": "Mouse",
        "category": "Electronics",
        "unit_price": "10.00",
        "currency": "USD",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    order = {
        "order_id": "ORD-001",
        "customer_id": "CUST-001",
        "order_date": "2026-01-03T00:00:00Z",
        "status": "PAID",
        "total_amount": "10.00",
        "currency": "USD",
        "updated_at": "2026-01-04T00:00:00Z",
    }
    item = {
        "item_id": "ITEM-001",
        "order_id": "ORD-001",
        "product_id": "PROD-001",
        "quantity": "1",
        "unit_price": "10.00",
        "updated_at": "2026-01-03T01:00:00Z",
    }
    payment = {
        "payment_id": "PAY-001",
        "order_id": "ORD-001",
        "amount": "10.00",
        "payment_method": "CARD",
        "payment_date": "2026-01-03T02:00:00Z",
        "status": "CAPTURED",
        "updated_at": "2026-01-04T02:00:00Z",
    }
    return {
        "customers": _transformed("customers", [customer]),
        "products": _transformed("products", [product]),
        "orders": _transformed("orders", [order]),
        "order_items": _transformed("order_items", [item]),
        "payments": _transformed("payments", [payment]),
    }


def _customer_version(*, email: str, updated_at: str, name: str = "Alice") -> dict[str, Any]:
    return {
        "customer_id": "CUST-001",
        "name": name,
        "email": email,
        "country": "US",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": updated_at,
    }


def test_publish_and_rerun_are_idempotent(empty_engine) -> None:
    frames = _base_frames()
    first = publish_run(empty_engine, frames, {}, {}, run_id="run-1")
    assert first.entity_stats["customers"].inserted == 1
    assert table_counts(empty_engine)["customers"] == 1

    second = publish_run(empty_engine, frames, {}, {}, run_id="run-2")
    assert second.entity_stats["customers"].unchanged == 1
    assert second.entity_stats["customers"].published == 0
    counts = table_counts(empty_engine)
    assert counts["customers"] == 1
    assert counts["record_history"] == 0
    assert counts["quarantine"] == 0


def test_newer_version_updates_and_archives_previous(empty_engine) -> None:
    publish_run(empty_engine, _base_frames(), {}, {}, run_id="run-1")
    newer = _transformed(
        "customers",
        [_customer_version(email="new@example.com", updated_at="2026-01-05T00:00:00Z")],
    )
    outcome = publish_run(empty_engine, {"customers": newer}, {}, {}, run_id="run-2")
    assert outcome.entity_stats["customers"].updated == 1
    with empty_engine.connect() as connection:
        email = connection.execute(
            text("SELECT email FROM customers WHERE customer_id = 'CUST-001'")
        ).scalar()
        history = (
            connection.execute(
                text(
                    "SELECT reason, payload, occurrence_count FROM record_history "
                    "WHERE entity = 'customers'"
                )
            )
            .mappings()
            .all()
        )
    assert email == "new@example.com"
    assert len(history) == 1
    assert history[0]["reason"] == "REPLACED_BY_NEWER_VERSION"
    assert "alice@example.com" in history[0]["payload"]


def test_older_version_is_late_arriving_and_does_not_overwrite(empty_engine) -> None:
    publish_run(empty_engine, _base_frames(), {}, {}, run_id="run-1")
    older = _transformed(
        "customers",
        [_customer_version(email="old@example.com", updated_at="2026-01-01T12:00:00Z")],
    )
    outcome = publish_run(empty_engine, {"customers": older}, {}, {}, run_id="run-2")
    assert outcome.entity_stats["customers"].late_arriving == 1
    with empty_engine.connect() as connection:
        email = connection.execute(
            text("SELECT email FROM customers WHERE customer_id = 'CUST-001'")
        ).scalar()
        history = connection.execute(
            text("SELECT reason FROM record_history WHERE entity = 'customers'")
        ).scalar()
    assert email == "alice@example.com"
    assert history == "LATE_ARRIVING_OLDER_VERSION"


def test_same_version_different_payload_is_quarantined(empty_engine) -> None:
    publish_run(empty_engine, _base_frames(), {}, {}, run_id="run-1")
    conflicting = _transformed(
        "customers",
        [_customer_version(email="other@example.com", updated_at="2026-01-02T00:00:00Z")],
    )
    outcome = publish_run(empty_engine, {"customers": conflicting}, {}, {}, run_id="run-2")
    assert outcome.entity_stats["customers"].conflicts == 1
    assert len(outcome.publish_conflicts["customers"]) == 1
    with empty_engine.connect() as connection:
        email = connection.execute(
            text("SELECT email FROM customers WHERE customer_id = 'CUST-001'")
        ).scalar()
        rules = connection.execute(
            text("SELECT rejection_rules FROM quarantine " "WHERE entity = 'customers'")
        ).scalar()
    assert email == "alice@example.com"
    assert "PUBLISH_VERSION_CONFLICT" in rules


def test_failed_target_checks_roll_back_the_whole_run(empty_engine, monkeypatch) -> None:
    import src.reconciliation as reconciliation

    monkeypatch.setattr(
        reconciliation,
        "run_target_checks",
        lambda connection: {
            "ok": False,
            "failures": [{"check": "forced", "value": 1}],
            "row_counts": {},
        },
    )
    rejected = validate_rows(
        source_frame(
            "customers",
            [
                _customer_version(
                    email="alice@example.com", updated_at="2026-01-02T00:00:00Z", name=""
                )
            ],
        ),
        "customers",
    ).rejected
    with pytest.raises(TargetIntegrityError):
        publish_run(
            empty_engine,
            _base_frames(),
            {"customers": rejected},
            {},
            run_id="run-failed",
        )
    counts = table_counts(empty_engine)
    assert counts["customers"] == 0
    assert counts["quarantine"] == 0
    assert counts["record_history"] == 0

    # A retry after restoring the real checks must publish exactly once.
    monkeypatch.undo()
    outcome = publish_run(
        empty_engine, _base_frames(), {"customers": rejected}, {}, run_id="run-retry"
    )
    assert outcome.entity_stats["customers"].inserted == 1
    assert table_counts(empty_engine)["quarantine"] == 1


def test_verify_failure_rolls_back_everything_and_retry_succeeds(empty_engine) -> None:
    called = {"value": False}

    def fail_verification(outcome) -> None:
        called["value"] = True
        raise ReconciliationError("forced reconciliation failure")

    with pytest.raises(ReconciliationError, match="forced reconciliation failure"):
        publish_run(
            empty_engine,
            _base_frames(),
            {},
            {},
            run_id="run-verify-failed",
            verify=fail_verification,
        )
    assert called["value"] is True
    counts = table_counts(empty_engine)
    for table in ("customers", "products", "orders", "order_items", "payments"):
        assert counts[table] == 0
    assert counts["quarantine"] == 0
    assert counts["record_history"] == 0

    retry = publish_run(empty_engine, _base_frames(), {}, {}, run_id="run-verify-retry")
    assert retry.entity_stats["customers"].inserted == 1
    assert table_counts(empty_engine)["customers"] == 1


def test_fetch_existing_keys_chunks_large_reference_sets(empty_engine) -> None:
    customer_ids = ["CUST-000", "CUST-200", "CUST-400"]
    customers = _transformed(
        "customers",
        [
            _customer_version(
                email=f"{customer_id.lower()}@example.com",
                updated_at="2026-01-02T00:00:00Z",
            )
            | {"customer_id": customer_id}
            for customer_id in customer_ids
        ],
    )
    publish_run(empty_engine, {"customers": customers}, {}, {}, run_id="key-lookup")

    requested = [f"CUST-{index:03d}" for index in range(405)]
    assert fetch_existing_keys(empty_engine, "customers", requested) == set(customer_ids)
    assert fetch_existing_keys(empty_engine, "customers", []) == set()


def test_fetch_published_order_totals_chunks_large_reference_sets(empty_engine) -> None:
    publish_run(empty_engine, valid_minimal_frames(), {}, {}, run_id="total-lookup")

    requested = [f"ORD-{index:03d}" for index in range(405)] + ["ORD-001"]
    assert fetch_published_order_totals(empty_engine, requested) == {"ORD-001": 10.0}
    assert fetch_published_order_totals(empty_engine, []) == {}


def test_retry_call_retries_only_transient_errors() -> None:
    calls = {"count": 0}

    def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise TransientPublishError("database is locked")
        return "ok"

    result, attempts = retry_call(flaky, max_attempts=3, base_delay=0.001)
    assert result == "ok"
    assert attempts == 3
    calls["count"] = 0
    retries: list[int] = []
    result, attempts = retry_call(
        flaky,
        max_attempts=3,
        base_delay=0.001,
        on_retry=lambda attempt, exc: retries.append(attempt),
    )
    assert result == "ok"
    assert attempts == 3
    assert retries == [1, 2]


def test_retry_call_does_not_retry_permanent_errors() -> None:
    calls = {"count": 0}

    def broken():
        calls["count"] += 1
        raise ValueError("bad data")

    with pytest.raises(ValueError):
        retry_call(broken, max_attempts=5, base_delay=0.001)
    assert calls["count"] == 1


def test_operational_error_classification() -> None:
    transient = OperationalError("SELECT 1", {}, Exception("database is locked"))
    permanent = OperationalError("SELECT 1", {}, Exception("syntax error"))
    assert is_transient(transient)
    assert not is_transient(permanent)
    assert not is_transient(ValueError("locked"))


def test_quarantine_upsert_is_idempotent(empty_engine) -> None:
    rejected = validate_rows(
        source_frame(
            "customers",
            [
                _customer_version(
                    email="alice@example.com", updated_at="2026-01-02T00:00:00Z", name=""
                )
            ],
        ),
        "customers",
    ).rejected
    publish_run(empty_engine, _base_frames(), {"customers": rejected}, {}, run_id="run-1")
    publish_run(empty_engine, _base_frames(), {"customers": rejected}, {}, run_id="run-2")
    with empty_engine.connect() as connection:
        rows = (
            connection.execute(text("SELECT occurrence_count, rejection_rules FROM quarantine"))
            .mappings()
            .all()
        )
    assert len(rows) == 1
    assert rows[0]["occurrence_count"] == 2
    assert "REQUIRED_NAME" in rows[0]["rejection_rules"]


def test_duplicate_history_upsert_is_idempotent(empty_engine) -> None:
    rows = [_customer_version(email="alice@example.com", updated_at="2026-01-02T00:00:00Z")] * 2
    duplicates = deduplicate(source_frame("customers", rows), "customers").duplicates
    assert len(duplicates) == 1
    publish_run(
        empty_engine,
        _base_frames(),
        {},
        {"customers": duplicates},
        run_id="run-1",
    )
    publish_run(
        empty_engine,
        _base_frames(),
        {},
        {"customers": duplicates},
        run_id="run-2",
    )
    with empty_engine.connect() as connection:
        history = (
            connection.execute(
                text(
                    "SELECT reason, occurrence_count FROM record_history "
                    "WHERE entity = 'customers'"
                )
            )
            .mappings()
            .all()
        )
    assert len(history) == 1
    assert history[0]["reason"] == "EXACT_DUPLICATE"
    assert history[0]["occurrence_count"] == 2
