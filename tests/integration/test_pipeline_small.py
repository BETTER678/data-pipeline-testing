"""Integration tests for the curated 79-row pipeline."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from src.pipeline import run_pipeline
from src.publishing import get_engine
from src.reconciliation import run_target_checks

pytestmark = pytest.mark.integration


def _quarantine_rules(settings) -> dict[str, set[str]]:
    engine = get_engine(settings.db_url)
    with engine.connect() as connection:
        rows = (
            connection.execute(text("SELECT source_record_id, rejection_rules FROM quarantine"))
            .mappings()
            .all()
        )
    rules: dict[str, set[str]] = {}
    for row in rows:
        rules.setdefault(str(row["source_record_id"]), set()).update(
            json.loads(row["rejection_rules"])
        )
    return rules


def test_small_pipeline_matches_expected_contract(
    small_environment: SimpleNamespace, small_run
) -> None:
    expected = small_environment.expected
    metrics = small_run.metrics
    assert small_run.status == expected["expected_status"]
    assert small_run.error == ""
    assert metrics["input_rows"] == expected["source_rows_total"]
    assert metrics["valid_rows"] == sum(expected["target_counts"].values())
    assert metrics["rejected_rows"] == expected["expected_quarantine_rows"]
    assert metrics["duplicate_rows"] == expected["expected_history_rows"]
    assert metrics["published_rows"] == sum(expected["target_counts"].values())
    assert metrics["unchanged_rows"] == 0

    for entity, entity_expected in expected["entities"].items():
        actual = metrics["entities"][entity]
        assert actual["source_rows"] == entity_expected["source_rows"]
        assert actual["valid_rows"] == entity_expected["valid_rows"]
        assert actual["rejected_rows"] == entity_expected["rejected_rows"]
        assert actual["duplicate_rows"] == entity_expected["duplicate_rows"]
        assert actual["duplicate_rule_counts"] == entity_expected["duplicate_rule_counts"]

    engine = get_engine(small_environment.settings.db_url)
    with engine.connect() as connection:
        for entity, expected_entity in expected["entities"].items():
            pk = {
                "customers": "customer_id",
                "products": "product_id",
                "orders": "order_id",
                "order_items": "item_id",
                "payments": "payment_id",
            }[entity]
            ids = [
                str(value)
                for value in connection.execute(
                    text(f"SELECT {pk} FROM {entity} ORDER BY {pk}")
                ).scalars()
            ]
            assert ids == sorted(expected_entity["canonical_ids"])
            for record_id, overrides in expected_entity.get("canonical_overrides", {}).items():
                columns = ", ".join(overrides)
                row = (
                    connection.execute(
                        text(f"SELECT {columns} FROM {entity} WHERE {pk} = :key"),
                        {"key": record_id},
                    )
                    .mappings()
                    .one()
                )
                assert dict(row) == overrides

        counts = {
            entity: int(connection.execute(text(f"SELECT COUNT(*) FROM {entity}")).scalar())
            for entity in expected["target_counts"]
        }
        quarantine = int(connection.execute(text("SELECT COUNT(*) FROM quarantine")).scalar())
        history = int(connection.execute(text("SELECT COUNT(*) FROM record_history")).scalar())
        checks = run_target_checks(connection)

    assert counts == expected["target_counts"]
    assert quarantine == expected["expected_quarantine_rows"]
    assert history == expected["expected_history_rows"]
    assert checks["ok"] is True
    assert checks["aggregates"] == expected["target_aggregates"]

    actual_rules = _quarantine_rules(small_environment.settings)
    for entity_expected in expected["entities"].values():
        for record_id, rules in entity_expected.get("rejected_rules", {}).items():
            assert actual_rules.get(record_id) == set(rules)

    reconciliation = small_run.reconciliation
    assert reconciliation["ok"] is True
    assert reconciliation["source_accounting_balanced"] is True
    assert reconciliation["target_checks_ok"] is True
    assert reconciliation["totals"]["input_rows"] == expected["source_rows_total"]
    assert reconciliation["totals"]["target_rows"] == sum(expected["target_counts"].values())


def test_small_pipeline_writes_all_artifacts(small_environment: SimpleNamespace, small_run) -> None:
    artifacts = small_run.artifacts
    assert all(artifact.exists() for artifact in artifacts["processed"].values())
    assert artifacts["quarantine_csv"].exists()
    assert artifacts["duplicates_csv"].exists()
    assert artifacts["reconciliation"]["json"].exists()
    assert artifacts["reconciliation"]["csv"].exists()
    assert artifacts["dq_report"]["json"].exists()
    assert artifacts["run_report"].exists()
    assert artifacts["publish_attempts"] == 1
    assert artifacts["history_rows"] == 10
    assert artifacts["quarantine_rows"] == 37


def test_small_pipeline_rerun_is_idempotent(small_environment: SimpleNamespace, small_run) -> None:
    first_counts = small_run.reconciliation["target_checks"]["row_counts"]
    rerun = run_pipeline(settings=small_environment.settings, run_id="test-small-rerun")
    assert rerun.status == "PARTIAL_SUCCESS"
    assert rerun.metrics["published_rows"] == 0
    assert rerun.metrics["unchanged_rows"] == 32
    assert rerun.metrics["rejected_rows"] == 37
    assert rerun.metrics["duplicate_rows"] == 10
    assert rerun.metrics["input_rows"] == 79
    assert rerun.reconciliation["target_checks"]["row_counts"] == first_counts
    assert rerun.reconciliation["ok"] is True

    # Audit rows are upserted, not appended.
    engine = get_engine(small_environment.settings.db_url)
    with engine.connect() as connection:
        assert int(connection.execute(text("SELECT COUNT(*) FROM quarantine")).scalar()) == 37
        assert int(connection.execute(text("SELECT COUNT(*) FROM record_history")).scalar()) == 10
        occurrences = connection.execute(
            text("SELECT MIN(occurrence_count), MAX(occurrence_count) " "FROM quarantine")
        ).one()
    assert occurrences == (2, 2)
