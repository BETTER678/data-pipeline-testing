"""Structured logging and per-run operational metrics."""

from __future__ import annotations

import json
import logging
import time
import uuid
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.config import PIPELINE_VERSION

_run_id_var: ContextVar[str | None] = ContextVar("pipeline_run_id", default=None)


def set_run_id(run_id: str) -> None:
    _run_id_var.set(run_id)


def current_run_id() -> str | None:
    return _run_id_var.get()


class JsonFormatter(logging.Formatter):
    """One JSON object per log line, suitable for log ingestion."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": getattr(record, "run_id", None) or current_run_id(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(log_path: Path | None = None, level: str = "INFO") -> logging.Logger:
    """Configure console + optional JSON-lines file logging once.

    Called by the CLI and pipeline entry point.  Tests can pass ``None`` to keep
    logging stream-only.  Handlers added by this function are tagged so repeated
    calls do not duplicate output.
    """

    root = logging.getLogger()
    root.setLevel(getattr(logging, str(level).upper(), logging.INFO))

    for handler in list(root.handlers):
        if getattr(handler, "_pipeline_handler", False):
            root.removeHandler(handler)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    console._pipeline_handler = True  # type: ignore[attr-defined]
    root.addHandler(console)

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(JsonFormatter())
        file_handler._pipeline_handler = True  # type: ignore[attr-defined]
        root.addHandler(file_handler)

    return root


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


class PipelineMonitor:
    """Accumulates metrics for one pipeline run and writes a JSON run report."""

    STATUS_SUCCESS = "SUCCESS"
    STATUS_PARTIAL = "PARTIAL_SUCCESS"
    STATUS_FAILED = "FAILED"

    def __init__(
        self,
        run_id: str | None = None,
        source_file: str = "",
        reports_dir: Path | None = None,
        pipeline_version: str = PIPELINE_VERSION,
    ) -> None:
        self.run_id = run_id or str(uuid.uuid4())
        self.source_file = source_file
        self.reports_dir = Path(reports_dir) if reports_dir else None
        self.pipeline_version = pipeline_version
        self.start_ts = datetime.now(UTC)
        self.end_ts: datetime | None = None
        self.status: str | None = None
        self.failure_reason = ""
        self.error_count = 0
        self.retry_attempts = 0
        self.errors: list[str] = []

        self.entity_metrics: dict[str, dict[str, Any]] = {}
        self.source_files: dict[str, str] = {}
        self.rejection_rules: dict[str, int] = {}
        self.input_rows = 0
        self.valid_rows = 0
        self.rejected_rows = 0
        self.duplicate_rows = 0
        self.published_rows = 0
        self.unchanged_rows = 0
        self.late_arriving_rows = 0

        self._finished = False
        self._metrics: dict[str, Any] | None = None
        self._logger = get_logger(__name__)
        set_run_id(self.run_id)

    @property
    def logger(self) -> logging.Logger:
        """Public logger for orchestration code; internal state stays private."""

        return self._logger

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------
    def record_ingestion(self, entity: str, path: Path, rows: int) -> None:
        self.source_files[entity] = str(path)
        self._logger.info(
            "Ingested entity=%s rows=%d file=%s run_id=%s",
            entity,
            rows,
            path,
            self.run_id,
        )

    def record_entity_metrics(self, entity: str, metrics: dict[str, Any]) -> None:
        self.entity_metrics[entity] = metrics
        self.input_rows += int(metrics.get("source_rows", 0))
        self.valid_rows += int(metrics.get("valid_rows", 0))
        self.rejected_rows += int(metrics.get("rejected_rows", 0))
        self.duplicate_rows += int(metrics.get("duplicate_rows", 0))
        self.published_rows += int(metrics.get("published_rows", 0))
        self.unchanged_rows += int(metrics.get("unchanged_rows", 0))
        self.late_arriving_rows += int(metrics.get("late_arriving_rows", 0))
        for rule, count in metrics.get("rule_counts", {}).items():
            self.rejection_rules[rule] = self.rejection_rules.get(rule, 0) + int(count)

    def record_rule_counts(self, counts: dict[str, int]) -> None:
        for rule, count in counts.items():
            self.rejection_rules[rule] = self.rejection_rules.get(rule, 0) + int(count)

    def record_error(self, message: str) -> None:
        self.error_count += 1
        self.errors.append(str(message))
        self._logger.error("Pipeline error run_id=%s: %s", self.run_id, message)

    def record_retry(self, attempt: int) -> None:
        self.retry_attempts += 1
        self._logger.warning("Retry attempt %d for run_id=%s", attempt, self.run_id)

    def mark_failed(self, reason: str) -> None:
        self.status = self.STATUS_FAILED
        self.failure_reason = str(reason)
        self._logger.error("Run FAILED run_id=%s reason=%s", self.run_id, self.failure_reason)

    def mark_partial(self, reason: str = "") -> None:
        self.status = self.STATUS_PARTIAL
        self.failure_reason = reason
        self._logger.warning("Run PARTIAL_SUCCESS run_id=%s reason=%s", self.run_id, reason)

    # ------------------------------------------------------------------
    # Completion
    # ------------------------------------------------------------------
    def _resolve_status(self) -> str:
        if self.status:
            return self.status
        if self.error_count > 0:
            return self.STATUS_FAILED
        if self.rejected_rows > 0:
            return self.STATUS_PARTIAL
        return self.STATUS_SUCCESS

    def finish(self, reconciliation: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._finished and self._metrics is not None:
            return self._metrics

        self.end_ts = datetime.now(UTC)
        duration = (self.end_ts - self.start_ts).total_seconds()
        status = self._resolve_status()
        self.status = status

        metrics: dict[str, Any] = {
            "run_id": self.run_id,
            "pipeline_version": self.pipeline_version,
            "source_file": self.source_file,
            "source_files": self.source_files,
            "start_timestamp": self.start_ts.isoformat(),
            "end_timestamp": self.end_ts.isoformat(),
            "duration_seconds": round(duration, 4),
            "input_rows": self.input_rows,
            "valid_rows": self.valid_rows,
            "rejected_rows": self.rejected_rows,
            "duplicate_rows": self.duplicate_rows,
            "published_rows": self.published_rows,
            "unchanged_rows": self.unchanged_rows,
            "late_arriving_rows": self.late_arriving_rows,
            "error_count": self.error_count,
            "retry_attempts": self.retry_attempts,
            "status": status,
            "data_quality_status": "PASS" if self.rejected_rows == 0 else "WARN",
            "failure_reason": self.failure_reason,
            "errors": self.errors,
            "rejection_rules": dict(sorted(self.rejection_rules.items())),
            "entities": self.entity_metrics,
            "reconciliation": reconciliation or {},
        }

        self._logger.info(
            "Run finished run_id=%s status=%s duration=%.3fs input=%d valid=%d "
            "rejected=%d duplicates=%d published=%d unchanged=%d late=%d",
            self.run_id,
            status,
            duration,
            self.input_rows,
            self.valid_rows,
            self.rejected_rows,
            self.duplicate_rows,
            self.published_rows,
            self.unchanged_rows,
            self.late_arriving_rows,
        )

        if self.reports_dir is not None:
            self.reports_dir.mkdir(parents=True, exist_ok=True)
            report_path = self.reports_dir / f"run_report_{self.run_id}.json"
            _atomic_json(report_path, metrics)
            _append_jsonl(self.reports_dir / "runs.jsonl", metrics)

        self._metrics = metrics
        self._finished = True
        return metrics

    def finalize_with_reconciliation(self, reconciliation: dict[str, Any]) -> dict[str, Any]:
        """Attach reconciliation details, then finish the run exactly once."""

        if self._finished and self._metrics is not None:
            metrics = self._metrics
            metrics["reconciliation"] = reconciliation
            if self.reports_dir is not None:
                _atomic_json(self.reports_dir / f"run_report_{self.run_id}.json", metrics)
                _replace_last_jsonl(self.reports_dir / "runs.jsonl", metrics)
            return metrics
        return self.finish(reconciliation=reconciliation)


def _atomic_json(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str) + "\n")


def _replace_last_jsonl(path: Path, payload: dict[str, Any]) -> None:
    """Replace the last JSONL record (used to add late reconciliation data)."""

    if not path.exists():
        _append_jsonl(path, payload)
        return
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        _append_jsonl(path, payload)
        return
    lines[-1] = json.dumps(payload, default=str)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def timed(func):
    """Small decorator used by benchmarks for per-stage timing."""

    def wrapper(*args, **kwargs):
        started = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - started
        return result, elapsed

    return wrapper
