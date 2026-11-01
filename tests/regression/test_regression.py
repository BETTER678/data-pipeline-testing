"""Regression tests: deterministic generation, expected contract and snapshots."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from src.config import PRIMARY_KEYS
from src.generator import (
    ScalableConfig,
    generate_scalable,
    generate_small,
    small_expected,
)
from src.pipeline import run_pipeline
from src.publishing import get_engine

pytestmark = pytest.mark.regression


def test_committed_expected_file_matches_generator(tmp_path: Path) -> None:
    generate_small(tmp_path / "raw", tmp_path / "expected")
    generated_path = tmp_path / "expected" / "small_expected.json"
    committed_path = (
        Path(__file__).resolve().parents[2] / "data" / "expected" / "small_expected.json"
    )
    generated = json.loads(generated_path.read_text(encoding="utf-8"))
    committed = json.loads(committed_path.read_text(encoding="utf-8"))
    assert generated == committed
    assert generated == small_expected()
    assert generated_path.read_bytes() == committed_path.read_bytes()
    assert b"\r\n" not in committed_path.read_bytes()


def test_small_generation_is_byte_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    generate_small(first / "raw", first / "expected")
    generate_small(second / "raw", second / "expected")
    for path in sorted((first / "raw").glob("*.csv")):
        twin = second / "raw" / path.name
        assert path.read_bytes() == twin.read_bytes()
    assert (first / "expected" / "small_expected.json").read_bytes() == (
        second / "expected" / "small_expected.json"
    ).read_bytes()


def test_scalable_generation_is_byte_deterministic(tmp_path: Path) -> None:
    config = ScalableConfig(
        customers=20,
        products=8,
        orders=50,
        seed=123,
        invalid_pct=0.02,
        duplicate_pct=0.01,
        null_pct=0.01,
        late_pct=0.01,
    )
    first = tmp_path / "first"
    second = tmp_path / "second"
    generate_scalable(first / "raw", config, first / "expected")
    generate_scalable(second / "raw", config, second / "expected")
    for path in sorted((first / "raw").glob("*.csv")):
        twin = second / "raw" / path.name
        assert path.read_bytes() == twin.read_bytes()
    assert (first / "expected" / "large_expected.json").read_bytes() == (
        second / "expected" / "large_expected.json"
    ).read_bytes()


def _snapshot(settings) -> dict[str, str]:
    engine = get_engine(settings.db_url)
    result: dict[str, str] = {}
    with engine.connect() as connection:
        for entity, pk in PRIMARY_KEYS.items():
            rows = [
                dict(row)
                for row in connection.execute(
                    text(f"SELECT * FROM {entity} ORDER BY {pk}")
                ).mappings()
            ]
            result[entity] = hashlib.sha256(
                json.dumps(rows, default=str, sort_keys=True).encode("utf-8")
            ).hexdigest()
    return result


def test_target_snapshot_is_unchanged_by_rerun(small_environment: SimpleNamespace) -> None:
    first = run_pipeline(settings=small_environment.settings, run_id="regression-first")
    assert first.status == "PARTIAL_SUCCESS"
    before = _snapshot(small_environment.settings)
    rerun = run_pipeline(settings=small_environment.settings, run_id="regression-rerun")
    assert rerun.metrics["unchanged_rows"] == 32
    assert rerun.metrics["published_rows"] == 0
    assert _snapshot(small_environment.settings) == before
