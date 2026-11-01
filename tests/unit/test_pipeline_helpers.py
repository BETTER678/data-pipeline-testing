"""Unit tests for pipeline orchestration helpers."""

from __future__ import annotations

import pytest

from src.deduplication import deduplicate
from src.pipeline import _rule_counts
from tests.helpers import source_frame

pytestmark = pytest.mark.unit


def test_rule_counts_accepts_conflict_frames_as_a_single_iterable_item() -> None:
    frame = source_frame(
        "customers",
        [
            {
                "customer_id": "CUST-900",
                "name": "Alice",
                "email": "alice@example.com",
                "country": "US",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-02T00:00:00Z",
            },
            {
                "customer_id": "CUST-900",
                "name": "Bob",
                "email": "bob@example.com",
                "country": "US",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-02T00:00:00Z",
            },
        ],
    )
    conflicts = deduplicate(frame, "customers").conflicts

    assert _rule_counts([conflicts]) == {"DEDUP_VERSION_CONFLICT": 2}
