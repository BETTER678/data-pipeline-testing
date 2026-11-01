"""Unit tests for deterministic, version-aware deduplication."""

from __future__ import annotations

import pytest

from src.deduplication import (
    attach_record_hash,
    compute_record_hash,
    deduplicate,
    deduplicate_naive,
)
from tests.helpers import small_rows, source_frame

pytestmark = pytest.mark.unit


def _customer(
    customer_id: str = "CUST-001",
    email: str = "alice@example.com",
    updated_at: str = "2026-01-02T00:00:00Z",
) -> dict:
    return {
        "customer_id": customer_id,
        "name": "Alice",
        "email": email,
        "country": "US",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": updated_at,
    }


def test_exact_duplicate_keeps_one_copy() -> None:
    frame = source_frame("customers", [_customer(), _customer()])
    outcome = deduplicate(frame, "customers")
    assert len(outcome.canonical) == 1
    assert len(outcome.duplicates) == 1
    assert outcome.duplicate_counts == {"DEDUP_EXACT": 1}
    assert outcome.duplicates.iloc[0]["_reject_rules"] == ('["DEDUP_EXACT"]')
    assert int(outcome.canonical.iloc[0]["_source_row_number"]) == 1


def test_newer_version_wins_and_older_is_superseded() -> None:
    frame = source_frame(
        "customers",
        [
            _customer(email="old@example.com", updated_at="2026-01-02T00:00:00Z"),
            _customer(email="new@example.com", updated_at="2026-01-03T00:00:00Z"),
        ],
    )
    outcome = deduplicate(frame, "customers")
    assert len(outcome.canonical) == 1
    assert outcome.canonical.iloc[0]["email"] == "new@example.com"
    assert outcome.duplicate_counts == {"DEDUP_SUPERSEDED": 1}


def test_older_version_arriving_after_newer_is_superseded() -> None:
    frame = source_frame(
        "customers",
        [
            _customer(email="new@example.com", updated_at="2026-01-03T00:00:00Z"),
            _customer(email="old@example.com", updated_at="2026-01-02T00:00:00Z"),
        ],
    )
    outcome = deduplicate(frame, "customers")
    assert outcome.canonical.iloc[0]["email"] == "new@example.com"
    assert outcome.duplicate_counts == {"DEDUP_SUPERSEDED": 1}


def test_same_key_and_version_with_different_payload_conflicts() -> None:
    frame = source_frame(
        "customers",
        [
            _customer(email="one@example.com"),
            _customer(email="two@example.com"),
        ],
    )
    outcome = deduplicate(frame, "customers")
    assert outcome.canonical.empty
    assert len(outcome.conflicts) == 2
    assert outcome.duplicate_counts == {"DEDUP_VERSION_CONFLICT": 2}
    assert set(
        outcome.conflicts["_reject_rules"].map(lambda value: "DEDUP_VERSION_CONFLICT" in value)
    ) == {True}


def test_rows_sharing_non_key_attributes_are_not_deduplicated() -> None:
    frame = source_frame(
        "customers",
        [
            _customer(customer_id="CUST-001"),
            _customer(customer_id="CUST-002"),
        ],
    )
    outcome = deduplicate(frame, "customers")
    assert len(outcome.canonical) == 2
    assert outcome.duplicates.empty


def test_empty_frame_is_a_noop() -> None:
    frame = source_frame("customers", [])
    outcome = deduplicate(frame, "customers")
    assert outcome.canonical.empty
    assert outcome.duplicates.empty
    assert outcome.conflicts.empty
    assert outcome.duplicate_counts == {}


def test_hash_is_stable_and_excludes_lineage() -> None:
    frame = source_frame("customers", [_customer()])
    same_payload = source_frame("customers", [_customer()], source_file="other.csv")
    first = compute_record_hash(frame).iloc[0]
    second = compute_record_hash(same_payload).iloc[0]
    assert first == second
    assert len(first) == 64

    output = attach_record_hash(frame, "customers")
    assert "_record_hash" in output.columns
    assert output.iloc[0]["_record_hash"] == first

    changed = frame.copy()
    changed.loc[0, "email"] = "changed@example.com"
    assert compute_record_hash(changed).iloc[0] != first


def test_hash_handles_ambiguous_concatenations() -> None:
    left = source_frame(
        "customers",
        [_customer(customer_id="AB", email="c@example.com")],
    )
    right = source_frame(
        "customers",
        [_customer(customer_id="A", email="Bc@example.com")],
    )
    assert compute_record_hash(left).iloc[0] != compute_record_hash(right).iloc[0]


@pytest.mark.parametrize(
    "entity",
    ["customers", "products", "orders", "order_items", "payments"],
)
def test_vectorised_and_naive_dedup_are_equivalent(entity: str) -> None:
    frame = source_frame(entity, small_rows(entity))
    vector = deduplicate(frame, entity)
    naive = deduplicate_naive(frame, entity)
    assert len(vector.canonical) == len(naive.canonical)
    assert set(vector.canonical.iloc[:, 0]) == set(naive.canonical.iloc[:, 0])
    assert vector.duplicate_counts == naive.duplicate_counts
    assert len(vector.duplicates) == len(naive.duplicates)
    assert len(vector.conflicts) == len(naive.conflicts)
