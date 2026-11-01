"""Unit tests for source discovery, CSV parsing and lineage."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.ingestion import (
    IngestionError,
    ingest_batch,
    inspect_header,
    read_source_csv,
)

pytestmark = pytest.mark.unit


CUSTOMER_HEADER = "customer_id,name,email,country,created_at,updated_at\n"


def _write(path: Path, text: str, encoding: str = "utf-8") -> Path:
    path.write_text(text, encoding=encoding)
    return path


def test_read_source_csv_attaches_lineage(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "customers.csv",
        CUSTOMER_HEADER + "CUST-001,Alice,alice@example.com,US,"
        "2025-12-01T09:00:00Z,2025-12-01T09:00:00Z\n",
    )
    result = read_source_csv(path, "customers", run_id="run-1")
    assert len(result.frame) == 1
    assert result.metadata.row_count == 1
    assert result.metadata.run_id == "run-1"
    assert result.frame.loc[0, "_source_file"] == "customers.csv"
    assert result.frame.loc[0, "_source_row_number"] == 1
    assert result.frame.loc[0, "_run_id"] == "run-1"
    assert result.frame.loc[0, "_ingested_at"]


def test_ingest_batch_requires_all_entities(tmp_path: Path) -> None:
    _write(tmp_path / "customers.csv", CUSTOMER_HEADER)
    with pytest.raises(IngestionError, match="missing"):
        ingest_batch(tmp_path, run_id="run-1")


def test_missing_file_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(IngestionError, match="does not exist"):
        read_source_csv(tmp_path / "nope.csv", "customers")


def test_zero_byte_file_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "customers.csv"
    path.write_bytes(b"")
    with pytest.raises(IngestionError, match="empty"):
        read_source_csv(path, "customers")


def test_duplicate_header_is_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "customers.csv",
        "customer_id,customer_id,created_at\nCUST-001,CUST-002,2025-01-01T00:00:00Z\n",
    )
    with pytest.raises(IngestionError, match="duplicate header"):
        read_source_csv(path, "customers")


def test_blank_header_column_is_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "customers.csv",
        "customer_id,,created_at\nCUST-001,x,2025-01-01T00:00:00Z\n",
    )
    with pytest.raises(IngestionError, match="blank header"):
        read_source_csv(path, "customers")


def test_header_only_file_is_an_empty_batch(tmp_path: Path) -> None:
    path = _write(tmp_path / "customers.csv", CUSTOMER_HEADER)
    result = read_source_csv(path, "customers")
    assert result.frame.empty
    assert list(result.frame.columns)[:6] == [
        "customer_id",
        "name",
        "email",
        "country",
        "created_at",
        "updated_at",
    ]
    assert result.metadata.row_count == 0


def test_malformed_csv_is_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "customers.csv",
        CUSTOMER_HEADER + "CUST-001,Alice,alice@example.com,US,2025-12-01T09:00:00Z,"
        '"2025-12-01T09:00:00Z\n',
    )
    with pytest.raises(IngestionError, match="malformed CSV"):
        read_source_csv(path, "customers")


def test_non_utf8_file_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "customers.csv"
    path.write_bytes(
        b"customer_id,name,email,country,created_at,updated_at\n"
        b"CUST-001,Alice,\xff\xfe,US,2025-12-01T09:00:00Z,"
        b"2025-12-01T09:00:00Z\n"
    )
    with pytest.raises(IngestionError, match="not valid UTF-8"):
        read_source_csv(path, "customers")


def test_utf8_bom_header_is_stripped(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "customers.csv",
        CUSTOMER_HEADER + "CUST-001,Alice,alice@example.com,US,"
        "2025-12-01T09:00:00Z,2025-12-01T09:00:00Z\n",
        encoding="utf-8-sig",
    )
    header = inspect_header(path)
    assert header[0] == "customer_id"
    result = read_source_csv(path, "customers")
    assert "customer_id" in result.frame.columns
