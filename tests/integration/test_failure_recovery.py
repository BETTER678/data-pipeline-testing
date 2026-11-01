"""Failure handling, retry and recovery integration tests."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from src.pipeline import run_pipeline
from src.publishing import get_engine, table_counts

pytestmark = pytest.mark.integration


def _counts(settings) -> dict[str, int]:
    return table_counts(get_engine(settings.db_url))


def test_missing_source_file_fails_without_publishing(small_environment: SimpleNamespace) -> None:
    (small_environment.settings.raw_dir / "orders.csv").unlink()
    result = run_pipeline(settings=small_environment.settings, run_id="test-missing-source")
    assert result.status == "FAILED"
    assert "IngestionError" in result.error
    assert result.artifacts["failure_report"].exists()
    counts = _counts(small_environment.settings)
    assert all(
        counts[entity] == 0
        for entity in ("customers", "products", "orders", "order_items", "payments")
    )
    assert counts["quarantine"] == 0
    assert counts["record_history"] == 0
    assert counts["pipeline_runs"] == 1


def test_database_initialization_failure_writes_failure_report(
    small_environment: SimpleNamespace, monkeypatch
) -> None:
    import src.pipeline as pipeline

    def fail_initialization(engine) -> None:
        raise OperationalError("PRAGMA foreign_keys=ON", {}, Exception("database is locked"))

    monkeypatch.setattr(pipeline, "init_db", fail_initialization)
    result = run_pipeline(settings=small_environment.settings, run_id="test-init-failure")

    assert result.status == "FAILED"
    assert "database is locked" in result.error
    failure_report = result.artifacts["failure_report"]
    assert failure_report.exists()
    report = json.loads(failure_report.read_text(encoding="utf-8"))
    assert report["run_id"] == "test-init-failure"
    assert "database is locked" in report["error"]


def test_schema_drift_fails_without_publishing(small_environment: SimpleNamespace) -> None:
    path = small_environment.settings.raw_dir / "customers.csv"
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    frame["loyalty_tier"] = "gold"
    frame.to_csv(path, index=False)
    result = run_pipeline(settings=small_environment.settings, run_id="test-schema-drift")
    assert result.status == "FAILED"
    assert "unexpected columns" in result.error
    counts = _counts(small_environment.settings)
    assert counts["customers"] == 0
    assert counts["pipeline_runs"] == 1


def test_transient_publish_failure_is_retried_once(
    small_environment: SimpleNamespace, monkeypatch
) -> None:
    import src.publishing as publishing

    original = publishing._insert_rows
    state = {"failed": False}

    def flaky_insert(connection, entity, frame):
        if not state["failed"]:
            state["failed"] = True
            raise OperationalError("INSERT INTO customers", {}, Exception("database is locked"))
        return original(connection, entity, frame)

    monkeypatch.setattr(publishing, "_insert_rows", flaky_insert)
    result = run_pipeline(settings=small_environment.settings, run_id="test-retry")
    assert result.status == "PARTIAL_SUCCESS"
    assert result.metrics["retry_attempts"] == 1
    assert result.artifacts["publish_attempts"] == 2
    # The first attempt was rolled back, so rows are published exactly once.
    counts = _counts(small_environment.settings)
    assert counts["customers"] == 7
    assert counts["orders"] == 6
    assert counts["order_items"] == 8
    assert counts["payments"] == 7


def test_target_failure_rolls_back_and_a_later_run_recovers(
    small_environment: SimpleNamespace, monkeypatch
) -> None:
    import src.reconciliation as reconciliation

    monkeypatch.setattr(
        reconciliation,
        "run_target_checks",
        lambda connection: {
            "ok": False,
            "failures": [{"check": "forced.failure"}],
            "row_counts": {},
        },
    )
    failed = run_pipeline(settings=small_environment.settings, run_id="test-forced-failure")
    assert failed.status == "FAILED"
    assert "TargetIntegrityError" in failed.error
    counts = _counts(small_environment.settings)
    assert all(
        counts[entity] == 0
        for entity in ("customers", "products", "orders", "order_items", "payments")
    )
    assert counts["quarantine"] == 0
    assert counts["record_history"] == 0
    assert counts["pipeline_runs"] == 1

    monkeypatch.undo()
    recovered = run_pipeline(settings=small_environment.settings, run_id="test-recovered")
    assert recovered.status == "PARTIAL_SUCCESS"
    assert recovered.reconciliation["ok"] is True
    counts = _counts(small_environment.settings)
    assert counts["customers"] == 7
    assert counts["orders"] == 6
    assert counts["pipeline_runs"] == 2


@pytest.mark.parametrize("cancellation", [KeyboardInterrupt, SystemExit])
def test_cancellation_marks_run_failed_and_is_re_raised(
    small_environment: SimpleNamespace, monkeypatch, cancellation
) -> None:
    import src.pipeline as pipeline

    def abort(ctx, state) -> None:
        raise cancellation()

    monkeypatch.setattr(pipeline, "_transform_stage", abort)
    with pytest.raises(cancellation):
        run_pipeline(settings=small_environment.settings, run_id="test-cancelled")

    counts = _counts(small_environment.settings)
    for entity in ("customers", "products", "orders", "order_items", "payments"):
        assert counts[entity] == 0
    assert counts["pipeline_runs"] == 1
    with get_engine(small_environment.settings.db_url).connect() as connection:
        status = connection.execute(
            text("SELECT status FROM pipeline_runs WHERE run_id = 'test-cancelled'")
        ).scalar()
    assert status == "FAILED"


def test_reconciliation_failure_rolls_back_and_a_later_run_recovers(
    small_environment: SimpleNamespace, monkeypatch
) -> None:
    import src.pipeline as pipeline

    def failing_reconciliation(**kwargs):
        return {
            "ok": False,
            "source_accounting_balanced": False,
            "target_checks": kwargs["target_checks"],
        }

    monkeypatch.setattr(pipeline, "build_reconciliation", failing_reconciliation)
    failed = run_pipeline(settings=small_environment.settings, run_id="test-recon-failure")
    assert failed.status == "FAILED"
    assert "ReconciliationError" in failed.error

    counts = _counts(small_environment.settings)
    for entity in ("customers", "products", "orders", "order_items", "payments"):
        assert counts[entity] == 0
    assert counts["quarantine"] == 0
    assert counts["record_history"] == 0
    assert counts["pipeline_runs"] == 1
    with get_engine(small_environment.settings.db_url).connect() as connection:
        status = connection.execute(
            text("SELECT status FROM pipeline_runs WHERE run_id = 'test-recon-failure'")
        ).scalar()
    assert status == "FAILED"

    monkeypatch.undo()
    recovered = run_pipeline(settings=small_environment.settings, run_id="test-recon-retry")
    assert recovered.status == "PARTIAL_SUCCESS"
    assert recovered.reconciliation["ok"] is True
    counts = _counts(small_environment.settings)
    for entity, expected in small_environment.expected["target_counts"].items():
        assert counts[entity] == expected
    assert counts["pipeline_runs"] == 2
