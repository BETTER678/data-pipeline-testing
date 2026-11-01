"""Test fixtures: isolated data directories, databases and pipeline runs."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import src.publishing as publishing  # noqa: E402

_ORIGINAL_GET_ENGINE = publishing.get_engine
_TEST_ENGINES = []


def _tracking_get_engine(*args, **kwargs):
    """Track engines created during a test so they can be disposed."""

    engine = _ORIGINAL_GET_ENGINE(*args, **kwargs)
    _TEST_ENGINES.append(engine)
    return engine


publishing.get_engine = _tracking_get_engine

from src.config import PipelineSettings  # noqa: E402
from src.generator import generate_small  # noqa: E402
from src.pipeline import run_pipeline  # noqa: E402
from src.publishing import get_engine, init_db  # noqa: E402


@pytest.fixture(autouse=True)
def _dispose_test_engines():
    """Dispose SQLAlchemy engines created by each test."""

    yield
    while _TEST_ENGINES:
        _TEST_ENGINES.pop().dispose()


@pytest.fixture()
def small_environment(tmp_path: Path) -> SimpleNamespace:
    """Curated 79-row dataset plus an isolated SQLite database."""

    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    expected_dir = data_dir / "expected"
    generate_small(raw_dir, expected_dir)
    db_path = data_dir / "processed" / "warehouse.db"
    settings = PipelineSettings(
        raw_dir=raw_dir,
        processed_dir=data_dir / "processed",
        rejected_dir=data_dir / "rejected",
        expected_dir=expected_dir,
        reports_dir=tmp_path / "reports",
        db_url=f"sqlite:///{db_path.as_posix()}",
        max_attempts=3,
        retry_base_delay=0.01,
        log_level="WARNING",
    )
    expected = json.loads((expected_dir / "small_expected.json").read_text(encoding="utf-8"))
    return SimpleNamespace(
        root=tmp_path,
        data_dir=data_dir,
        settings=settings,
        expected=expected,
        db_path=db_path,
    )


@pytest.fixture()
def small_run(small_environment: SimpleNamespace):
    """Run the small pipeline once and return the result."""

    result = run_pipeline(settings=small_environment.settings, run_id="test-small-1")
    return result


@pytest.fixture()
def empty_engine():
    """An initialised, empty in-memory warehouse."""

    engine = get_engine("sqlite://")
    init_db(engine)
    yield engine
    engine.dispose()
