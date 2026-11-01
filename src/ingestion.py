"""Source file discovery, CSV parsing and lineage tagging."""

from __future__ import annotations

import csv
import hashlib
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.config import ENTITIES, INGEST_LINEAGE_COLUMNS
from src.monitoring import get_logger

logger = get_logger(__name__)


class IngestionError(Exception):
    """A source file is missing, unreadable, empty or structurally malformed."""

    def __init__(self, entity: str, path: Path, reason: str):
        self.entity = entity
        self.path = Path(path)
        self.reason = reason
        super().__init__(f"Cannot ingest {entity!r} from {path}: {reason}")


@dataclass(frozen=True)
class FileMetadata:
    entity: str
    path: str
    sha256: str
    size_bytes: int
    row_count: int
    ingested_at: str
    run_id: str


@dataclass
class IngestionResult:
    entity: str
    frame: pd.DataFrame
    metadata: FileMetadata


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_header(path: Path) -> list[str]:
    """Return raw header names and reject duplicate/blank headers.

    Pandas silently renames duplicate headers (``id``, ``id.1``), which would
    hide a serious schema problem.  Reading the header with the csv module keeps
    the original names and lets us fail loudly.
    """

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
    except UnicodeDecodeError as exc:
        raise ValueError("file is not valid UTF-8 text") from exc
    except csv.Error as exc:
        raise ValueError(f"CSV header cannot be parsed: {exc}") from exc

    if header is None:
        raise ValueError("file has no header row")
    if not header or all(not name.strip() for name in header):
        raise ValueError("file has a blank header row")
    duplicates = sorted({name for name in header if header.count(name) > 1})
    if duplicates:
        raise ValueError(f"duplicate header columns: {duplicates}")
    blank = [index + 1 for index, name in enumerate(header) if not name.strip()]
    if blank:
        raise ValueError(f"blank header column positions: {blank}")
    return header


def _validate_source_path(path: Path, entity: str) -> None:
    if not path.exists():
        raise IngestionError(entity, path, "source file does not exist")
    if not path.is_file():
        raise IngestionError(entity, path, "source path is not a regular file")
    if path.stat().st_size == 0:
        raise IngestionError(entity, path, "source file is empty (0 bytes)")
    try:
        inspect_header(path)
    except ValueError as exc:
        raise IngestionError(entity, path, str(exc)) from exc


def _read_csv_frame(path: Path, entity: str) -> pd.DataFrame:
    try:
        return pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            encoding="utf-8-sig",
            low_memory=False,
        )
    except pd.errors.EmptyDataError as exc:
        raise IngestionError(entity, path, "file has no parseable header/data") from exc
    except pd.errors.ParserError as exc:
        raise IngestionError(entity, path, f"malformed CSV: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise IngestionError(entity, path, "file is not valid UTF-8") from exc
    except OSError as exc:
        raise IngestionError(entity, path, f"file could not be read: {exc}") from exc


def _attach_provenance(
    frame: pd.DataFrame, path: Path, run_id: str, ingested_at: str
) -> pd.DataFrame:
    frame.columns = [str(col).strip() for col in frame.columns]
    frame["_source_file"] = path.name
    frame["_source_row_number"] = pd.Series(
        range(1, len(frame) + 1), index=frame.index, dtype="int64"
    )
    frame["_run_id"] = run_id
    frame["_ingested_at"] = ingested_at
    return frame


def read_source_csv(
    path: str | Path,
    entity: str,
    run_id: str | None = None,
) -> IngestionResult:
    """Read one source CSV and attach provenance columns.

    Values are read as text (``dtype=str``) so numeric/date formatting problems
    are visible to validation instead of being silently coerced by pandas.
    Blank cells remain empty strings; validation treats them as nulls.
    """

    path = Path(path)
    run_id = run_id or str(uuid.uuid4())
    ingested_at = datetime.now(UTC).isoformat()
    logger.info("Reading source entity=%s path=%s run_id=%s", entity, path, run_id)
    _validate_source_path(path, entity)
    frame = _attach_provenance(_read_csv_frame(path, entity), path, run_id, ingested_at)
    metadata = FileMetadata(
        entity=entity,
        path=str(path),
        sha256=_sha256(path),
        size_bytes=path.stat().st_size,
        row_count=int(len(frame)),
        ingested_at=ingested_at,
        run_id=run_id,
    )
    logger.info("Ingested entity=%s rows=%d sha256=%s", entity, len(frame), metadata.sha256[:12])
    return IngestionResult(entity=entity, frame=frame, metadata=metadata)


def ingest_batch(
    raw_dir: str | Path,
    run_id: str,
    entities: Iterable[str] = ENTITIES,
    required: bool = True,
) -> dict[str, IngestionResult]:
    """Ingest every expected entity CSV from ``raw_dir``.

    Missing entities raise :class:`IngestionError` when ``required`` is true.
    Tests use this to prove the pipeline fails loudly instead of publishing a
    silently incomplete warehouse.
    """

    raw_dir = Path(raw_dir)
    results: dict[str, IngestionResult] = {}

    if not raw_dir.exists():
        raise IngestionError("batch", raw_dir, "source directory does not exist")

    for entity in entities:
        path = raw_dir / f"{entity}.csv"
        if not path.exists():
            if required:
                raise IngestionError(entity, path, "required source file is missing")
            logger.warning("Optional source missing entity=%s path=%s", entity, path)
            continue
        results[entity] = read_source_csv(path, entity=entity, run_id=run_id)

    return results


def lineage_columns() -> tuple[str, ...]:
    return INGEST_LINEAGE_COLUMNS
