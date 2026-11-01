"""Deterministic duplicate detection and version-aware conflict handling.

Policy
------
* Business key = the entity primary key.
* Every record has an ``updated_at`` version timestamp (canonical UTC).
* Exact duplicates: identical business payload (including version) are removed;
  one copy is kept.
* Superseded versions: when the same key arrives multiple times, the row with
  the greatest ``updated_at`` wins.  Older versions are retained in
  ``record_history`` (or written to the duplicate audit CSV) so history is not
  silently lost.
* Same key *and* same version with different payload: ambiguous.  The whole key
  is quarantined as ``DEDUP_VERSION_CONFLICT``; no arbitrary winner is chosen.
* Rows that merely share non-key attributes are never deduplicated.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from src.config import INGEST_LINEAGE_COLUMNS, PRIMARY_KEYS, REJECT_COLUMNS
from src.monitoring import get_logger
from src.schemas import ENTITY_SCHEMAS
from src.validation import empty_rejected_frame, make_rejected

logger = get_logger(__name__)


@dataclass
class DedupOutcome:
    canonical: pd.DataFrame
    duplicates: pd.DataFrame
    conflicts: pd.DataFrame
    duplicate_counts: dict[str, int] = field(default_factory=dict)

    @property
    def duplicate_rows(self) -> int:
        return int(len(self.duplicates))

    @property
    def conflict_rows(self) -> int:
        return int(len(self.conflicts))


def _normalise_hash_value(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return format(value, ".12g")
    return str(value)


def compute_record_hash(
    frame: pd.DataFrame,
    columns: Iterable[str] | None = None,
) -> pd.Series:
    """Stable SHA-256 over business columns (lineage excluded).

    ``updated_at`` is part of the payload, so a new version intentionally
    produces a different hash.  Values are length-prefixed before hashing to
    avoid ambiguous concatenations such as ``("ab", "c")`` vs ``("a", "bc")``.
    """

    if frame.empty:
        return pd.Series(dtype="object")
    selected = (
        list(columns)
        if columns is not None
        else [
            column
            for column in frame.columns
            if column not in INGEST_LINEAGE_COLUMNS and column not in REJECT_COLUMNS
        ]
    )
    records = frame[selected].to_dict("records")
    hashes: list[str] = []
    for record in records:
        parts = []
        for column in selected:
            value = _normalise_hash_value(record.get(column))
            parts.append(f"{len(value)}:{value}")
        digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
        hashes.append(digest)
    return pd.Series(hashes, index=frame.index, dtype="object")


def attach_record_hash(frame: pd.DataFrame, entity: str) -> pd.DataFrame:
    """Attach ``_record_hash`` representing the published business payload."""

    if frame.empty:
        result = frame.copy()
        result["_record_hash"] = pd.Series(dtype="object")
        return result
    schema = ENTITY_SCHEMAS[entity]
    columns = list(schema.source_columns) + list(schema.derived_fields)
    result = frame.copy()
    result["_record_hash"] = compute_record_hash(result, columns)
    return result


def _version_and_duplicate_errors(
    work: pd.DataFrame,
    remaining: pd.DataFrame,
    canonical_indices: set[int],
    hash_column: str,
    pk: str,
) -> tuple[np.ndarray, list[list[tuple[str, str]]]]:
    """Classify every non-canonical row.

    The first occurrence of a (key, payload) pair represents that version; any
    later copy is an exact duplicate.  A first occurrence that is not the
    canonical version is superseded by the newest version.  This keeps the
    distinction between "the same row arrived twice" and "an old version
    arrived before/after a newer version", which the audit report exposes.
    """

    duplicate_positions = np.flatnonzero(~remaining.index.isin(canonical_indices)).astype(int)
    previous_same_payload = remaining.duplicated([pk, hash_column], keep="first").to_numpy()
    errors: list[list[tuple[str, str]]] = [[] for _ in range(len(remaining))]
    for position in duplicate_positions:
        row = remaining.iloc[position]
        key = str(row[pk])
        if previous_same_payload[position] > 0:
            errors[position].append(
                (
                    "DEDUP_EXACT",
                    f"exact duplicate of {pk}={key!r}; one copy retained",
                )
            )
        else:
            errors[position].append(
                (
                    "DEDUP_SUPERSEDED",
                    f"older/out-of-order version of {pk}={key!r}; "
                    f"latest updated_at retained and this version archived",
                )
            )
    return duplicate_positions, errors


def _find_version_conflicts(
    work: pd.DataFrame, pk: str
) -> tuple[np.ndarray, list[list[tuple[str, str]]]]:
    """Return a mask and reasons for keys with ambiguous same-version payloads."""

    # Sorted ascending by version: the last row per key is the newest version.
    # Mapping this back avoids a pure-Python groupby transform over object
    # timestamps, which was the dominant cost at 100k rows.
    latest_by_key = work.drop_duplicates(pk, keep="last").set_index(pk)["updated_at"]
    at_max = work["updated_at"].eq(work[pk].map(latest_by_key))
    max_payloads = work.loc[at_max, [pk, "_dedup_hash"]].drop_duplicates()
    conflicted = max_payloads.loc[max_payloads.duplicated(pk, keep=False), pk]
    conflict_keys = set(conflicted.astype(str))

    conflict_mask = work[pk].astype(str).isin(conflict_keys).to_numpy()
    errors: list[list[tuple[str, str]]] = [[] for _ in range(len(work))]
    for position in np.flatnonzero(conflict_mask):
        key = str(work.iloc[position][pk])
        errors[int(position)].append(
            (
                "DEDUP_VERSION_CONFLICT",
                f"multiple different payloads for {pk}={key!r} at the same "
                "updated_at; all versions quarantined for investigation",
            )
        )
    return conflict_mask, errors


def _canonical_without_conflicts(
    work: pd.DataFrame, pk: str, conflict_mask: np.ndarray
) -> tuple[pd.DataFrame, pd.DataFrame]:
    remaining = work[~conflict_mask].reset_index(drop=True)
    latest_by_key = remaining.drop_duplicates(pk, keep="last").set_index(pk)["updated_at"]
    latest_version = remaining[pk].map(latest_by_key)
    canonical_frame = (
        remaining[remaining["updated_at"].eq(latest_version)]
        .drop_duplicates(pk, keep="first")
        .copy()
    )
    return remaining, canonical_frame


def _dedup_counts(duplicates: pd.DataFrame, conflicts: pd.DataFrame) -> dict[str, int]:
    counts = {
        "DEDUP_EXACT": (
            int((duplicates["_reject_rules"].str.contains("DEDUP_EXACT")).sum())
            if not duplicates.empty
            else 0
        ),
        "DEDUP_SUPERSEDED": (
            int((duplicates["_reject_rules"].str.contains("DEDUP_SUPERSEDED")).sum())
            if not duplicates.empty
            else 0
        ),
        "DEDUP_VERSION_CONFLICT": int(len(conflicts)),
    }
    return {rule: count for rule, count in counts.items() if count}


def deduplicate(df: pd.DataFrame, entity: str) -> DedupOutcome:
    """Vectorised, deterministic duplicate handling for one entity."""

    if entity not in PRIMARY_KEYS:
        raise ValueError(f"Unknown entity: {entity!r}")
    if df.empty:
        return DedupOutcome(
            canonical=df.copy(),
            duplicates=empty_rejected_frame(df),
            conflicts=empty_rejected_frame(df),
            duplicate_counts={},
        )

    pk = PRIMARY_KEYS[entity]
    schema = ENTITY_SCHEMAS[entity]
    work = df.copy().reset_index(drop=True)
    work["_dedup_hash"] = compute_record_hash(work, schema.source_columns)
    work = work.sort_values([pk, "updated_at", "_source_row_number"], kind="stable").reset_index(
        drop=True
    )

    conflict_mask, conflict_errors = _find_version_conflicts(work, pk)
    remaining, canonical_frame = _canonical_without_conflicts(work, pk, conflict_mask)
    canonical_indices = set(canonical_frame.index)
    duplicate_positions, duplicate_errors = _version_and_duplicate_errors(
        work, remaining, canonical_indices, "_dedup_hash", pk
    )

    duplicates = make_rejected(remaining, duplicate_positions, duplicate_errors)
    conflicts = make_rejected(work, np.flatnonzero(conflict_mask), conflict_errors)
    canonical = canonical_frame.drop(columns=["_dedup_hash"]).reset_index(drop=True)
    counts = _dedup_counts(duplicates, conflicts)
    if not duplicates.empty or not conflicts.empty:
        logger.info(
            "Deduplication entity=%s removed=%d conflicts=%d counts=%s",
            entity,
            len(duplicates),
            len(conflicts),
            counts,
        )
    return DedupOutcome(
        canonical=canonical,
        duplicates=duplicates,
        conflicts=conflicts,
        duplicate_counts=counts,
    )


def _naive_groups(work: pd.DataFrame, pk: str) -> dict[str, list[int]]:
    groups: dict[str, list[int]] = {}
    for position, key in enumerate(work[pk].astype(str)):
        groups.setdefault(key, []).append(position)
    return groups


def _naive_ordered(work: pd.DataFrame, positions: list[int]) -> list[int]:
    return sorted(
        positions,
        key=lambda p: (
            work.iloc[p]["updated_at"],
            int(work.iloc[p]["_source_row_number"]),
        ),
    )


def _naive_max_positions(work: pd.DataFrame, ordered: list[int]) -> list[int]:
    max_version = max(work.iloc[p]["updated_at"] for p in ordered)
    return [p for p in ordered if work.iloc[p]["updated_at"] == max_version]


def _classify_naive_groups(work: pd.DataFrame, pk: str, groups: dict[str, list[int]]) -> tuple[
    list[int],
    list[int],
    dict[int, tuple[str, str]],
    list[int],
    dict[int, tuple[str, str]],
]:
    conflict_positions: list[int] = []
    duplicate_positions: list[int] = []
    duplicate_errors: dict[int, tuple[str, str]] = {}
    conflict_errors: dict[int, tuple[str, str]] = {}
    canonical_positions: list[int] = []

    for key, positions in groups.items():
        ordered = _naive_ordered(work, positions)
        max_positions = _naive_max_positions(work, ordered)
        if len({work.iloc[p]["_dedup_hash"] for p in max_positions}) > 1:
            for p in positions:
                conflict_positions.append(p)
                conflict_errors[p] = (
                    "DEDUP_VERSION_CONFLICT",
                    f"multiple different payloads for {pk}={key!r} at the same "
                    "updated_at; all versions quarantined for investigation",
                )
            continue
        canonical = max_positions[0]
        canonical_positions.append(canonical)
        seen_payloads: set[str] = set()
        for p in ordered:
            payload = work.iloc[p]["_dedup_hash"]
            if p == canonical:
                seen_payloads.add(payload)
                continue
            duplicate_positions.append(p)
            if payload in seen_payloads:
                duplicate_errors[p] = (
                    "DEDUP_EXACT",
                    f"exact duplicate of {pk}={key!r}; one copy retained",
                )
            else:
                duplicate_errors[p] = (
                    "DEDUP_SUPERSEDED",
                    f"older/out-of-order version of {pk}={key!r}; latest "
                    "updated_at retained and this version archived",
                )
                seen_payloads.add(payload)
    return (
        canonical_positions,
        duplicate_positions,
        duplicate_errors,
        conflict_positions,
        conflict_errors,
    )


def _naive_reject(
    work: pd.DataFrame, positions: list[int], errors: dict[int, tuple[str, str]]
) -> pd.DataFrame:
    if not positions:
        return empty_rejected_frame(work)
    error_lists: list[list[tuple[str, str]]] = [[] for _ in range(len(work))]
    for position, error in errors.items():
        error_lists[position].append(error)
    return make_rejected(work, np.array(positions, dtype=int), error_lists)


def deduplicate_naive(df: pd.DataFrame, entity: str) -> DedupOutcome:
    """Row-wise reference implementation used only to benchmark the vectorised path.

    It intentionally mirrors the production semantics.  Keeping it in the repo
    makes the performance claim measurable instead of speculative.
    """

    if df.empty:
        return DedupOutcome(
            canonical=df.copy(),
            duplicates=empty_rejected_frame(df),
            conflicts=empty_rejected_frame(df),
            duplicate_counts={},
        )
    pk = PRIMARY_KEYS[entity]
    schema = ENTITY_SCHEMAS[entity]
    work = df.copy().reset_index(drop=True)
    work["_dedup_hash"] = compute_record_hash(work, schema.source_columns)
    groups = _naive_groups(work, pk)
    (
        canonical_positions,
        duplicate_positions,
        duplicate_errors,
        conflict_positions,
        conflict_errors,
    ) = _classify_naive_groups(work, pk, groups)

    canonical = work.iloc[sorted(canonical_positions)].copy()
    canonical = canonical.drop(columns=["_dedup_hash"]).reset_index(drop=True)
    duplicates = _naive_reject(work, duplicate_positions, duplicate_errors)
    conflicts = _naive_reject(work, conflict_positions, conflict_errors)
    counts = {
        "DEDUP_EXACT": sum(1 for errors in duplicate_errors.values() if errors[0] == "DEDUP_EXACT"),
        "DEDUP_SUPERSEDED": sum(
            1 for errors in duplicate_errors.values() if errors[0] == "DEDUP_SUPERSEDED"
        ),
        "DEDUP_VERSION_CONFLICT": len(conflict_positions),
    }
    counts = {rule: count for rule, count in counts.items() if count}
    return DedupOutcome(
        canonical=canonical,
        duplicates=duplicates,
        conflicts=conflicts,
        duplicate_counts=counts,
    )
