"""Integration test for seeded, scalable generated data."""

from __future__ import annotations

import pytest

from src.config import PipelineSettings
from src.generator import ScalableConfig, generate_scalable
from src.pipeline import run_pipeline
from src.publishing import get_engine, table_counts
from src.reconciliation import run_target_checks

pytestmark = pytest.mark.integration


def test_scalable_pipeline_accounts_for_every_source_row(tmp_path) -> None:
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    expected_dir = data_dir / "expected"
    config = ScalableConfig(
        customers=30,
        products=10,
        orders=120,
        seed=7,
        invalid_pct=0.02,
        duplicate_pct=0.01,
        null_pct=0.01,
        late_pct=0.01,
    )
    expected = generate_scalable(raw_dir, config, expected_dir)
    db_path = data_dir / "warehouse.db"
    settings = PipelineSettings(
        raw_dir=raw_dir,
        processed_dir=data_dir / "processed",
        rejected_dir=data_dir / "rejected",
        expected_dir=expected_dir,
        reports_dir=tmp_path / "reports",
        db_url=f"sqlite:///{db_path.as_posix()}",
        retry_base_delay=0.001,
        log_level="WARNING",
    )
    result = run_pipeline(settings=settings, run_id="test-scalable-1")
    assert result.status == "PARTIAL_SUCCESS"
    assert result.error == ""
    assert result.metrics["input_rows"] == expected["source_rows_total"]

    for entity, base_count in expected["base_counts"].items():
        metrics = result.metrics["entities"][entity]
        assert metrics["source_rows"] == expected["source_rows"][entity]
        assert metrics["rejected_rows"] == expected["expected"]["rejected_rows"][entity]
        assert metrics["duplicate_rows"] == expected["expected"]["duplicate_rows"][entity]
        assert metrics["valid_rows"] == base_count

    engine = get_engine(settings.db_url)
    counts = table_counts(engine)
    assert {entity: counts[entity] for entity in expected["base_counts"]} == expected["base_counts"]
    with engine.connect() as connection:
        checks = run_target_checks(connection)
    assert checks["ok"] is True
    assert result.reconciliation["source_accounting_balanced"] is True
    assert result.reconciliation["ok"] is True
