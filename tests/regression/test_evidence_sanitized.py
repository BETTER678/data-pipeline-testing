"""Regression checks for the tracked, sanitized evidence package."""

from __future__ import annotations

import re
from pathlib import Path

EVIDENCE = Path(__file__).resolve().parents[2] / "docs" / "evidence"
TEXT_SUFFIXES = {".json", ".md", ".txt"}
WINDOWS_ABSOLUTE_PATH = re.compile(r"(?i)\b[A-Z]:[\\/]")


def test_evidence_package_is_sanitized_and_uses_lf() -> None:
    checked = 0
    for path in sorted(EVIDENCE.rglob("*")):
        if path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        raw = path.read_bytes()
        assert b"\r\n" not in raw, f"CRLF in evidence file: {path}"
        text = raw.decode("utf-8")
        match = WINDOWS_ABSOLUTE_PATH.search(text)
        assert match is None, f"absolute path in evidence file: {path}: {match.group(0)}"
        checked += 1
    assert checked >= 14
