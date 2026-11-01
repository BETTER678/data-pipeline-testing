"""Unit tests for run metrics and written artifacts."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.monitoring import PipelineMonitor
from src.reporting import (
    write_dq_report,
    write_duplicates_csv,
    write_failure_report,
    write_processed_snapshots,
    write_quarantine_csv,
)
from src.validation import validate_rows
from tests.helpers import source_frame

pytestmark = pytest.mark.unit


def _rejected_customer() -> pd.DataFrame:
    return validate_rows(
        source_frame(
            "customers",
            [
                {
                    "customer_id": "CUST-001",
                    "name": "",
                    "email": "alice@example.com",
                    "country": "US",
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-02T00:00:00Z",
                }
            ],
        ),
        "customers",
    ).rejected


def test_monitor_status_and_reports(tmp_path: Path) -> None:
    monitor = PipelineMonitor(run_id="run-1", source_file="raw", reports_dir=tmp_path)
    monitor.record_entity_metrics(
        "customers",
        {
            "source_rows": 2,
            "valid_rows": 1,
            "rejected_rows": 1,
            "duplicate_rows": 0,
            "published_rows": 1,
            "unchanged_rows": 0,
            "late_arriving_rows": 0,
            "rule_counts": {"REQUIRED_NAME": 1},
        },
    )
    metrics = monitor.finish()
    assert metrics["status"] == "PARTIAL_SUCCESS"
    assert metrics["input_rows"] == 2
    assert (tmp_path / "run_report_run-1.json").exists()
    assert (tmp_path / "runs.jsonl").read_text(encoding="utf-8").count("\n") == 1

    reconciliation = {"ok": True, "source_accounting_balanced": True}
    finalized = monitor.finalize_with_reconciliation(reconciliation)
    assert finalized["reconciliation"] == reconciliation
    stored = json.loads((tmp_path / "run_report_run-1.json").read_text(encoding="utf-8"))
    assert stored["reconciliation"]["ok"] is True


def test_monitor_failure_status() -> None:
    monitor = PipelineMonitor(run_id="run-2")
    monitor.record_error("boom")
    monitor.mark_failed("boom")
    assert monitor.finish()["status"] == "FAILED"
    assert monitor.error_count == 1


def test_quarantine_and_duplicate_csv(tmp_path: Path) -> None:
    rejected = _rejected_customer()
    path = write_quarantine_csv({"customers": rejected}, tmp_path, "run-1")
    frame = pd.read_csv(path)
    assert len(frame) == 1
    assert frame.loc[0, "entity"] == "customers"
    assert frame.loc[0, "source_record_id"] == "CUST-001"

    duplicates = rejected.copy()
    duplicate_path = write_duplicates_csv({"customers": duplicates}, tmp_path, "run-1")
    duplicate_frame = pd.read_csv(duplicate_path)
    assert len(duplicate_frame) == 1


def test_processed_snapshots_and_dq_report(tmp_path: Path, monkeypatch) -> None:
    valid = validate_rows(
        source_frame(
            "customers",
            [
                {
                    "customer_id": "CUST-002",
                    "name": "Alice",
                    "email": "alice@example.com",
                    "country": "US",
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-02T00:00:00Z",
                }
            ],
        ),
        "customers",
    ).valid
    snapshots = write_processed_snapshots({"customers": valid}, tmp_path / "processed", "run-1")
    assert Path(snapshots["customers"]).exists()
    assert (tmp_path / "processed" / "latest_customers.csv").exists()

    monkeypatch.setattr("src.reporting.write_dq_chart", lambda *args: None)
    metrics = {
        "customers": {
            "source_rows": 2,
            "valid_rows": 1,
            "rejected_rows": 1,
            "duplicate_rows": 0,
            "rule_counts": {"REQUIRED_NAME": 1},
            "null_counts": {"name": 1},
        }
    }
    artifacts = write_dq_report("run-1", metrics, tmp_path / "reports")
    report = json.loads(Path(artifacts["json"]).read_text(encoding="utf-8"))
    assert report["status"] == "WARN"
    assert report["totals"]["source_rows"] == 2
    assert report["entities"]["customers"]["pass_rate_pct"] == 50.0
    assert Path(artifacts["csv"]).exists()


def test_failure_report(tmp_path: Path) -> None:
    path = write_failure_report("run-bad", tmp_path, ValueError("bad"), context={"stage": "test"})
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["error_type"] == "ValueError"
    assert payload["context"]["stage"] == "test"
