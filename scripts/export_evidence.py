"""Export a curated, sanitized evidence set into docs/evidence/.

The repository intentionally ignores generated reports and warehouse files.  A
submission still needs a small, reviewable sample of the execution evidence, so
this script copies selected artifacts, converts text to UTF-8, and replaces
machine-specific absolute paths with ``<project>``.

Usage:
    python scripts/export_evidence.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_DIR = ROOT / "docs" / "evidence"

# (source relative to project root, destination filename, file kind)
FILES: tuple[tuple[str, str, str], ...] = (
    ("reports/demo_small_001_stdout.txt", "sample_run_output.json", "json"),
    ("reports/demo_small_002_stdout.txt", "idempotent_run_output.json", "json"),
    ("reports/run_report_demo-small-001.json", "run_report_demo-small-001.json", "json"),
    ("reports/run_report_demo-small-002.json", "run_report_demo-small-002.json", "json"),
    (
        "reports/reconciliation_demo-small-001.json",
        "reconciliation_demo-small-001.json",
        "json",
    ),
    (
        "reports/reconciliation_demo-small-002.json",
        "reconciliation_demo-small-002.json",
        "json",
    ),
    ("reports/dq_report_demo-small-001.json", "dq_report_demo-small-001.json", "json"),
    (
        "reports/verification_demo-small-001.json",
        "verification_demo-small-001.json",
        "json",
    ),
    (
        "reports/verification_demo-small-002.json",
        "verification_demo-small-002.json",
        "json",
    ),
    ("reports/final_database_checks.json", "final_database_checks.json", "json"),
    ("reports/pytest_results.txt", "pytest_results.txt", "text"),
    ("reports/pytest_coverage_results.txt", "pytest_coverage_results.txt", "text"),
    ("reports/lint_results.txt", "lint_results.txt", "text"),
    ("reports/benchmark.json", "benchmark.json", "json"),
)

_ABSOLUTE_PATH = re.compile(r"(?i)\b[A-Z]:[\\/][^:\r\n\"']*")


def _sanitize_text(value: str) -> str:
    """Remove machine-specific paths and normalize line endings to UTF-8 LF."""

    value = value.replace("\r\n", "\n").replace("\r", "\n")
    value = value.replace(str(ROOT), "<project>")
    value = value.replace(ROOT.as_posix(), "<project>")
    value = value.replace(str(ROOT).replace("\\", "\\\\"), "<project>")
    value = _ABSOLUTE_PATH.sub("<local>", value)
    return value


def _sanitize_json(value: Any) -> Any:
    if isinstance(value, str):
        return _sanitize_text(value)
    if isinstance(value, list):
        return [_sanitize_json(item) for item in value]
    if isinstance(value, dict):
        return {key: _sanitize_json(item) for key, item in value.items()}
    return value


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return raw.decode("utf-16")
    if raw[:3] == b"\xef\xbb\xbf":
        return raw.decode("utf-8-sig")
    return raw.decode("utf-8")


def export() -> None:
    """Copy sanitized evidence artifacts into docs/evidence."""
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    exported: list[str] = []
    for source_name, destination_name, kind in FILES:
        source = ROOT / source_name
        if not source.exists():
            raise FileNotFoundError(f"Evidence source not found: {source}")
        destination = EVIDENCE_DIR / destination_name
        raw = _read_text(source)
        if kind == "json":
            payload = _sanitize_json(json.loads(raw))
            rendered = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
        else:
            rendered = _sanitize_text(raw)
        destination.write_bytes(rendered.encode("utf-8"))
        exported.append(destination.relative_to(ROOT).as_posix())

    print(f"Exported {len(exported)} evidence files to {EVIDENCE_DIR}:")
    for name in exported:
        print(f"  {name}")


if __name__ == "__main__":
    export()
