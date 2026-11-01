"""Unit tests for reconciliation, target integrity and helper queries."""

from __future__ import annotations

import json

import pytest
from sqlalchemy import text

from src.publishing import publish_run, table_counts
from src.reconciliation import (
    build_reconciliation,
    run_target_checks,
    verify_fk_integrity,
    verify_pk_uniqueness,
)
from tests.helpers import valid_minimal_frames

pytestmark = pytest.mark.unit


def test_empty_target_passes_all_checks(empty_engine) -> None:
    with empty_engine.connect() as connection:
        checks = run_target_checks(connection)
    assert checks["ok"] is True
    assert checks["failures"] == []
    assert checks["row_counts"] == {
        "customers": 0,
        "products": 0,
        "orders": 0,
        "order_items": 0,
        "payments": 0,
    }


def test_published_target_passes_and_reports_aggregates(empty_engine) -> None:
    publish_run(empty_engine, valid_minimal_frames(), {}, {}, run_id="run-1")
    with empty_engine.connect() as connection:
        checks = run_target_checks(connection)
    assert checks["ok"] is True
    assert checks["aggregates"] == {
        "order_total_sum": 10.0,
        "line_total_sum": 10.0,
        "captured_payment_sum": 10.0,
    }
    assert verify_pk_uniqueness(empty_engine, "customers") is True
    assert (
        verify_fk_integrity(empty_engine, "orders", "customer_id", "customers", "customer_id") == 0
    )


def test_overpayment_is_detected(empty_engine) -> None:
    publish_run(empty_engine, valid_minimal_frames(), {}, {}, run_id="run-1")
    with empty_engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO payments (
                    payment_id, order_id, amount, payment_method, payment_date,
                    status, updated_at, _source_file, _source_row_number,
                    _run_id, _ingested_at, _pipeline_version, _record_hash
                ) VALUES (
                    'PAY-002', 'ORD-001', 10.00, 'CARD',
                    '2026-01-03T03:00:00.000000Z', 'CAPTURED',
                    '2026-01-04T03:00:00.000000Z', 'payments.csv', 2,
                    'manual', '2026-01-01T00:00:00+00:00', '1.0.0',
                    '0000000000000000000000000000000000000000000000000000000000000000'
                )
                """
            )
        )
    with empty_engine.connect() as connection:
        checks = run_target_checks(connection)
    assert checks["ok"] is False
    assert checks["checks"]["payments.no_overpayment"]["value"] == 1
    assert any(failure["check"] == "payments.no_overpayment" for failure in checks["failures"])


def test_source_accounting_balance_and_imbalance() -> None:
    balanced_metrics = {
        "customers": {
            "source_rows": 10,
            "rejected_rows": 2,
            "duplicate_rows": 1,
            "valid_rows": 7,
            "published_rows": 5,
            "unchanged_rows": 1,
            "late_arriving_rows": 1,
        }
    }
    target = {"ok": True, "row_counts": {"customers": 6}, "checks": {}}
    report = build_reconciliation("run-1", balanced_metrics, target)
    assert report["ok"] is True
    assert report["totals"]["input_rows"] == 10
    assert report["totals"]["target_rows"] == 6

    unbalanced = json.loads(json.dumps(balanced_metrics))
    unbalanced["customers"]["rejected_rows"] = 1
    report = build_reconciliation("run-2", unbalanced, target)
    assert report["ok"] is False
    assert report["entities"]["customers"]["unaccounted_rows"] == 1


def test_target_failure_marks_reconciliation_not_ok() -> None:
    metrics = {
        "customers": {
            "source_rows": 1,
            "rejected_rows": 0,
            "duplicate_rows": 0,
            "valid_rows": 1,
            "published_rows": 1,
            "unchanged_rows": 0,
            "late_arriving_rows": 0,
        }
    }
    report = build_reconciliation(
        "run-1",
        metrics,
        {"ok": False, "row_counts": {"customers": 1}, "checks": {}},
    )
    assert report["source_accounting_balanced"] is True
    assert report["target_checks_ok"] is False
    assert report["ok"] is False


def test_table_counts_include_audit_tables(empty_engine) -> None:
    publish_run(empty_engine, valid_minimal_frames(), {}, {}, run_id="run-1")
    counts = table_counts(empty_engine)
    assert counts["customers"] == 1
    assert counts["payments"] == 1
    assert counts["quarantine"] == 0
    assert counts["record_history"] == 0
