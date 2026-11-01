"""Schema, data-quality, referential-integrity and business-rule validation.

Every source row is either accepted with explicit typed values or rejected with
one or more rule codes and human-readable reasons.  Rejected rows keep their raw
values and lineage so they can be quarantined and investigated.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

import numpy as np
import pandas as pd

from src.config import (
    INGEST_LINEAGE_COLUMNS,
    MONEY_TOLERANCE_ABS,
    PAYMENT_TOLERANCE_PCT,
    REJECT_COLUMNS,
)
from src.monitoring import get_logger
from src.schemas import ENTITY_SCHEMAS, TIMESTAMP_PATTERN, ColumnSpec, EntitySchema

logger = get_logger(__name__)

_INTEGER_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_TIMESTAMP_RE = re.compile(TIMESTAMP_PATTERN)


# ---------------------------------------------------------------------------
# Exceptions and result containers
# ---------------------------------------------------------------------------
class SchemaValidationError(Exception):
    """A batch cannot be processed because its structure is not acceptable."""

    def __init__(self, entity: str, issues: list[str]):
        self.entity = entity
        self.issues = issues
        super().__init__(f"Schema validation failed for {entity}: {'; '.join(issues)}")


@dataclass
class RowValidationOutcome:
    valid: pd.DataFrame
    rejected: pd.DataFrame
    rule_counts: dict[str, int] = field(default_factory=dict)
    null_counts: dict[str, int] = field(default_factory=dict)
    rows_checked: int = 0

    @property
    def rejected_rows(self) -> int:
        return int(len(self.rejected))


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _as_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _text_series(series: pd.Series) -> pd.Series:
    return series.map(_as_text).astype(object)


def empty_rejected_frame(df: pd.DataFrame) -> pd.DataFrame:
    """An empty rejected frame with the same columns as a populated one."""

    columns = list(df.columns) + list(REJECT_COLUMNS) + ["_raw_record"]
    return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})


def concat_rejections(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate non-empty rejection frames into one table."""
    usable = [frame for frame in frames if frame is not None and not frame.empty]
    if not usable:
        return pd.DataFrame()
    return pd.concat(usable, ignore_index=True)


def rejection_rules(frame: pd.DataFrame) -> list[str]:
    """Flatten rule codes from a rejected frame (JSON-encoded per row)."""

    if frame.empty or "_reject_rules" not in frame.columns:
        return []
    rules: list[str] = []
    for value in frame["_reject_rules"]:
        try:
            parsed = json.loads(value) if isinstance(value, str) else value
            if isinstance(parsed, list):
                rules.extend(str(item) for item in parsed)
        except (TypeError, json.JSONDecodeError):
            continue
    return rules


def make_rejected(
    raw: pd.DataFrame,
    positions: np.ndarray,
    errors: list[list[tuple[str, str]]],
) -> pd.DataFrame:
    """Build a rejected frame from raw rows and accumulated per-row errors."""

    if positions.size == 0:
        return empty_rejected_frame(raw)

    rejected = raw.iloc[positions].copy()
    rules_json: list[str] = []
    reasons_json: list[str] = []
    raw_records: list[str] = []
    rejected_at = _now_iso()

    business_columns = [column for column in raw.columns if column not in INGEST_LINEAGE_COLUMNS]
    clean_rows = rejected[business_columns].astype(object).to_dict("records")
    for ordinal, position in enumerate(positions):
        row_errors = errors[int(position)]
        rules_json.append(json.dumps([rule for rule, _ in row_errors]))
        reasons_json.append(json.dumps([reason for _, reason in row_errors]))
        record = {column: _as_text(value) for column, value in clean_rows[ordinal].items()}
        raw_records.append(json.dumps(record, default=str))

    rejected["_reject_rules"] = rules_json
    rejected["_reject_reasons"] = reasons_json
    rejected["_rejected_at"] = rejected_at
    rejected["_raw_record"] = raw_records
    return rejected.reset_index(drop=True)


def _count_rules(errors: list[list[tuple[str, str]]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row_errors in errors:
        for rule in {rule for rule, _ in row_errors}:
            counts[rule] += 1
    return dict(sorted(counts.items()))


# ---------------------------------------------------------------------------
# Structural schema validation
# ---------------------------------------------------------------------------
def validate_source_structure(df: pd.DataFrame, entity: str) -> None:
    """Validate column presence and detect schema drift.

    Missing required columns or unexpected columns stop the run: processing a
    batch against the wrong contract could publish corrupted data.  Empty
    batches with the correct header are valid.
    """

    schema = ENTITY_SCHEMAS.get(entity)
    if schema is None:
        raise SchemaValidationError(entity, [f"unknown entity {entity!r}"])

    columns = list(df.columns)
    issues: list[str] = []

    duplicates = sorted({column for column in columns if columns.count(column) > 1})
    if duplicates:
        issues.append(f"duplicate columns: {duplicates}")

    expected = set(schema.source_columns) | set(INGEST_LINEAGE_COLUMNS)
    missing = [column for column in schema.source_columns if column not in columns]
    unexpected = [column for column in columns if column not in expected]
    if missing:
        issues.append(f"missing required columns: {missing}")
    if unexpected:
        issues.append(f"unexpected columns (schema drift): {unexpected}")

    if issues:
        logger.error("Schema validation failed entity=%s issues=%s", entity, issues)
        raise SchemaValidationError(entity, issues)


# ---------------------------------------------------------------------------
# Row-level typed validation
# ---------------------------------------------------------------------------
def _typed_empty(df: pd.DataFrame, schema: EntitySchema) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)
    for spec in schema.columns:
        if spec.dtype == "integer":
            result[spec.name] = pd.Series(dtype="int64")
        elif spec.dtype == "float":
            result[spec.name] = pd.Series(dtype="float64")
        else:
            result[spec.name] = pd.Series(dtype="object")
    for column in INGEST_LINEAGE_COLUMNS:
        if column in df.columns:
            result[column] = df[column]
    return result


@dataclass
class _ValidationContext:
    """Mutable error and parsed-value state for one validation pass."""

    errors: list[list[tuple[str, str]]]
    parsed: dict[str, pd.Series] = field(default_factory=dict)
    null_counts: dict[str, int] = field(default_factory=dict)

    def add(
        self,
        mask: np.ndarray,
        rule: str,
        message: str | Sequence[str],
    ) -> None:
        indices = np.flatnonzero(mask)
        if indices.size == 0:
            return
        if isinstance(message, str):
            for idx in indices:
                self.errors[int(idx)].append((rule, message))
        else:
            for idx, msg in zip(indices, message, strict=False):
                self.errors[int(idx)].append((rule, str(msg)))


def _check_required(
    spec: ColumnSpec,
    is_blank: np.ndarray,
    context: _ValidationContext,
) -> None:
    if spec.required:
        context.add(
            is_blank,
            f"REQUIRED_{spec.name.upper()}",
            f"{spec.name} is required but was blank",
        )


def _range_violations(
    spec: ColumnSpec,
    numeric: pd.Series,
    checkable: np.ndarray,
) -> np.ndarray:
    below = np.zeros(len(numeric), dtype=bool)
    above = np.zeros(len(numeric), dtype=bool)
    if spec.min_value is not None:
        if spec.min_exclusive:
            below = checkable & (numeric <= spec.min_value).to_numpy()
        else:
            below = checkable & (numeric < spec.min_value).to_numpy()
    if spec.max_value is not None:
        above = checkable & (numeric > spec.max_value).to_numpy()
    return below | above


def _add_range_errors(
    context: _ValidationContext,
    spec: ColumnSpec,
    series: pd.Series,
    numeric: pd.Series,
    checkable: np.ndarray,
) -> None:
    invalid = _range_violations(spec, numeric, checkable)
    context.add(
        invalid,
        f"RANGE_{spec.name.upper()}",
        [
            f"{spec.name}={series.iloc[i]!r} outside allowed range " f"{_range_label(spec)}"
            for i in np.flatnonzero(invalid)
        ],
    )


def _validate_string_column(
    spec: ColumnSpec,
    series: pd.Series,
    non_blank: np.ndarray,
    context: _ValidationContext,
) -> pd.Series:
    rule_format = f"FORMAT_{spec.name.upper()}"
    rule_domain = f"DOMAIN_{spec.name.upper()}"
    working = series.copy()

    if spec.allowed_values:
        upper = series.str.upper()
        invalid = non_blank & ~upper.isin(spec.allowed_values).to_numpy()
        context.add(
            invalid,
            rule_domain,
            [
                f"{spec.name}={series.iloc[i]!r} is not one of " f"{sorted(spec.allowed_values)}"
                for i in np.flatnonzero(invalid)
            ],
        )
        working = upper.where(~series.eq(""), "")

    if spec.max_length is not None:
        too_long = non_blank & (series.str.len() > spec.max_length).to_numpy()
        context.add(
            too_long,
            rule_format,
            f"{spec.name} exceeds maximum length {spec.max_length}",
        )

    if spec.pattern:
        matches = series.str.fullmatch(spec.pattern, na=False).to_numpy()
        invalid = non_blank & ~matches
        detail = spec.pattern_message or f"does not match {spec.pattern}"
        context.add(
            invalid,
            rule_format,
            [f"{spec.name}={series.iloc[i]!r} {detail}" for i in np.flatnonzero(invalid)],
        )

    return working.where(non_blank, None)


def _validate_integer_column(
    spec: ColumnSpec,
    series: pd.Series,
    non_blank: np.ndarray,
    context: _ValidationContext,
) -> pd.Series:
    format_ok = series.str.fullmatch(_INTEGER_RE, na=False).to_numpy()
    invalid_type = non_blank & ~format_ok
    context.add(
        invalid_type,
        f"TYPE_{spec.name.upper()}",
        [f"{spec.name}={series.iloc[i]!r} is not an integer" for i in np.flatnonzero(invalid_type)],
    )

    numeric = pd.to_numeric(series, errors="coerce")
    checkable = non_blank & format_ok
    _add_range_errors(context, spec, series, numeric, checkable)
    return numeric.where(checkable, np.nan)


def _validate_float_column(
    spec: ColumnSpec,
    series: pd.Series,
    non_blank: np.ndarray,
    context: _ValidationContext,
) -> pd.Series:
    format_ok = series.str.fullmatch(_FLOAT_RE, na=False).to_numpy()
    numeric = pd.to_numeric(series, errors="coerce")
    non_finite = (
        non_blank & format_ok & ~np.isfinite(numeric.to_numpy(dtype=float, na_value=np.nan))
    )
    invalid_type = (non_blank & ~format_ok) | non_finite
    context.add(
        invalid_type,
        f"TYPE_{spec.name.upper()}",
        [
            f"{spec.name}={series.iloc[i]!r} is not a finite decimal number"
            for i in np.flatnonzero(invalid_type)
        ],
    )

    checkable = non_blank & format_ok & ~non_finite
    _add_range_errors(context, spec, series, numeric, checkable)
    return numeric.where(checkable, np.nan)


def _validate_timestamp_column(
    spec: ColumnSpec,
    series: pd.Series,
    non_blank: np.ndarray,
    context: _ValidationContext,
) -> pd.Series:
    regex_ok = series.str.fullmatch(TIMESTAMP_PATTERN, na=False).to_numpy()
    parsed_dt = pd.to_datetime(
        series.where(non_blank, None),
        format="ISO8601",
        utc=True,
        errors="coerce",
    )
    parse_ok = parsed_dt.notna().to_numpy()
    invalid = non_blank & ~(regex_ok & parse_ok)
    context.add(
        invalid,
        f"FORMAT_{spec.name.upper()}",
        [
            f"{spec.name}={series.iloc[i]!r} is not a valid ISO-8601 " "timestamp with timezone"
            for i in np.flatnonzero(invalid)
        ],
    )
    context.parsed[f"__dt_{spec.name}"] = parsed_dt
    return parsed_dt.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ").where(parsed_dt.notna(), None)


_VALUE_VALIDATORS = {
    "string": _validate_string_column,
    "integer": _validate_integer_column,
    "float": _validate_float_column,
    "timestamp": _validate_timestamp_column,
}


def _validate_columns(
    raw: pd.DataFrame,
    schema: EntitySchema,
    context: _ValidationContext,
) -> None:
    for spec in schema.columns:
        series = _text_series(raw[spec.name])
        is_blank = series.eq("").to_numpy()
        non_blank = ~is_blank
        context.null_counts[spec.name] = int(is_blank.sum())
        _check_required(spec, is_blank, context)
        validator = _VALUE_VALIDATORS.get(spec.dtype)
        if validator is None:  # pragma: no cover - guarded by schema definitions
            raise ValueError(f"Unsupported dtype {spec.dtype!r} for {spec.name}")
        context.parsed[spec.name] = validator(spec, series, non_blank, context)


def _validate_temporal(schema: EntitySchema, context: _ValidationContext) -> None:
    for earlier, later in schema.timestamp_pairs:
        earlier_dt = context.parsed.get(f"__dt_{earlier}")
        later_dt = context.parsed.get(f"__dt_{later}")
        if earlier_dt is None or later_dt is None:
            continue
        comparable = earlier_dt.notna().to_numpy() & later_dt.notna().to_numpy()
        out_of_order = comparable & (earlier_dt > later_dt).to_numpy()
        context.add(
            out_of_order,
            f"TEMPORAL_{later.upper()}",
            [
                f"{later}={context.parsed[later].iloc[i]!r} is earlier than "
                f"{earlier}={context.parsed[earlier].iloc[i]!r}"
                for i in np.flatnonzero(out_of_order)
            ],
        )


def _build_valid_frame(
    raw: pd.DataFrame,
    schema: EntitySchema,
    context: _ValidationContext,
    valid_positions: np.ndarray,
) -> pd.DataFrame:
    valid = raw.iloc[valid_positions].copy()
    for spec in schema.columns:
        values = context.parsed[spec.name].iloc[valid_positions]
        if spec.dtype == "integer":
            valid[spec.name] = values.astype("int64")
        elif spec.dtype == "float":
            valid[spec.name] = values.astype("float64")
        else:
            valid[spec.name] = values.where(values.notna(), None)
            if spec.required:
                valid[spec.name] = valid[spec.name].astype(object)
    return valid.reset_index(drop=True)


def _validate_nonempty(df: pd.DataFrame, entity: str, schema: EntitySchema) -> RowValidationOutcome:
    raw = df.copy()
    context = _ValidationContext(errors=[[] for _ in range(len(raw))])
    _validate_columns(raw, schema, context)
    _validate_temporal(schema, context)

    has_error = np.array([bool(row) for row in context.errors], dtype=bool)
    rejected_positions = np.flatnonzero(has_error)
    valid_positions = np.flatnonzero(~has_error)
    valid = _build_valid_frame(raw, schema, context, valid_positions)
    rule_counts = _count_rules(context.errors)
    if rejected_positions.size:
        logger.warning(
            "Row validation entity=%s rejected=%d/%d rules=%s",
            entity,
            len(rejected_positions),
            len(raw),
            rule_counts,
        )
    return RowValidationOutcome(
        valid=valid,
        rejected=make_rejected(raw, rejected_positions, context.errors),
        rule_counts=rule_counts,
        null_counts=context.null_counts,
        rows_checked=len(raw),
    )


def validate_rows(df: pd.DataFrame, entity: str) -> RowValidationOutcome:
    """Validate and cast every row of one entity.

    Checks performed:
    * required / null fields;
    * integer, float and ISO-8601 timestamp formats;
    * string patterns, lengths and enum domains;
    * inclusive/exclusive value ranges;
    * temporal ordering (event date <= updated_at).

    All failures on a row are retained, so quarantine explains every problem.
    The per-type parsing lives in small helpers so each rule family can be
    reviewed and tested independently.
    """

    schema = ENTITY_SCHEMAS.get(entity)
    if schema is None:
        raise ValueError(f"Unknown entity: {entity!r}")
    if df.empty:
        return RowValidationOutcome(
            valid=_typed_empty(df, schema),
            rejected=empty_rejected_frame(df),
            rule_counts={},
            null_counts={spec.name: 0 for spec in schema.columns},
            rows_checked=0,
        )
    return _validate_nonempty(df, entity, schema)


def _range_label(spec: ColumnSpec) -> str:
    low = "-inf" if spec.min_value is None else str(spec.min_value)
    high = "inf" if spec.max_value is None else str(spec.max_value)
    left = "(" if spec.min_exclusive else "["
    return f"{left}{low}, {high}]"


# ---------------------------------------------------------------------------
# Cross-entity checks
# ---------------------------------------------------------------------------
def _rule_rejected(
    df: pd.DataFrame,
    mask: np.ndarray,
    rule: str,
    messages: Sequence[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty or not mask.any():
        return df, empty_rejected_frame(df)
    errors: list[list[tuple[str, str]]] = [[] for _ in range(len(df))]
    for idx, message in zip(np.flatnonzero(mask), messages, strict=False):
        errors[int(idx)].append((rule, str(message)))
    rejected = make_rejected(df, np.flatnonzero(mask), errors)
    valid = df[~mask].reset_index(drop=True)
    return valid, rejected


def check_foreign_keys(
    df: pd.DataFrame,
    column: str,
    valid_keys: Iterable[str],
    parent_entity: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reject rows whose foreign key is not present in the accepted parent set."""

    if df.empty:
        return df, empty_rejected_frame(df)
    keys = {str(key) for key in valid_keys}
    values = df[column].astype(str)
    bad = ~values.isin(keys).to_numpy()
    reasons = [
        f"{column}={values.iloc[i]!r} does not reference an accepted " f"{parent_entity} record"
        for i in np.flatnonzero(bad)
    ]
    valid, rejected = _rule_rejected(df, bad, f"FK_{column.upper()}", reasons)
    if not rejected.empty:
        logger.warning(
            "Referential integrity failure column=%s parent=%s rejected=%d",
            column,
            parent_entity,
            len(rejected),
        )
    return valid, rejected


def check_order_totals(
    orders: pd.DataFrame,
    items: pd.DataFrame,
    tolerance: float = MONEY_TOLERANCE_ABS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Domain rule: an order must have items and total == sum(line totals)."""

    if orders.empty:
        return orders, empty_rejected_frame(orders)

    if items.empty:
        aggregate = pd.DataFrame(columns=["order_id", "item_count", "item_total"])
    else:
        work = items[["order_id", "quantity", "unit_price"]].copy()
        work["_line_total"] = (
            work["quantity"].astype(float) * work["unit_price"].astype(float)
        ).round(2)
        aggregate = work.groupby("order_id", as_index=False).agg(
            item_count=("_line_total", "size"), item_total=("_line_total", "sum")
        )

    merged = orders.merge(aggregate, on="order_id", how="left")
    merged["item_count"] = (
        pd.to_numeric(merged["item_count"], errors="coerce").fillna(0).astype(int)
    )
    merged["item_total"] = (
        pd.to_numeric(merged["item_total"], errors="coerce").fillna(0.0).astype(float)
    )
    no_items = (merged["item_count"] == 0).to_numpy()
    mismatch = (
        ~no_items
        & (
            (merged["total_amount"].astype(float) - merged["item_total"]).abs() > tolerance
        ).to_numpy()
    )
    bad = no_items | mismatch
    if not bad.any():
        return orders, empty_rejected_frame(orders)

    errors: list[list[tuple[str, str]]] = [[] for _ in range(len(orders))]
    for idx in np.flatnonzero(no_items):
        errors[int(idx)].append(
            (
                "BUSINESS_ORDER_WITHOUT_ITEMS",
                f"order {merged['order_id'].iloc[idx]!r} has no accepted order items",
            )
        )
    for idx in np.flatnonzero(mismatch):
        errors[int(idx)].append(
            (
                "BUSINESS_ORDER_TOTAL_MISMATCH",
                f"order total {float(merged['total_amount'].iloc[idx]):.2f} "
                f"!= accepted item total {float(merged['item_total'].iloc[idx]):.2f}",
            )
        )

    rejected = make_rejected(orders, np.flatnonzero(bad), errors)
    valid = orders[~bad].reset_index(drop=True)
    logger.warning("Order total check rejected=%d/%d", len(rejected), len(orders))
    return valid, rejected


def _payment_order_totals(
    orders: pd.DataFrame,
    published_order_totals: Mapping[str, float] | None,
) -> pd.DataFrame:
    """Build order totals, preferring the current batch over committed state."""

    totals: dict[str, float] = {
        str(order_id): float(total) for order_id, total in (published_order_totals or {}).items()
    }
    if not orders.empty:
        batch_totals = (
            orders[["order_id", "total_amount"]]
            .drop_duplicates("order_id")
            .set_index("order_id")["total_amount"]
            .astype(float)
        )
        totals.update({str(key): float(value) for key, value in batch_totals.items()})
    return pd.DataFrame(
        [(key, value) for key, value in totals.items()],
        columns=["order_id", "_order_total"],
    )


def check_payment_amounts(
    payments: pd.DataFrame,
    orders: pd.DataFrame,
    tolerance_pct: float = PAYMENT_TOLERANCE_PCT,
    published_order_totals: Mapping[str, float] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Domain rule: captured payment must match its order total within 1%.

    ``orders`` takes precedence for keys in the current batch.  The optional
    ``published_order_totals`` mapping supplies totals for late-arriving
    payments whose parent order was committed by an earlier run.
    """

    if payments.empty:
        return payments, empty_rejected_frame(payments)

    totals = _payment_order_totals(orders, published_order_totals)
    if totals.empty:
        return payments, empty_rejected_frame(payments)

    merged = payments.merge(totals, on="order_id", how="left")
    captured = merged["status"].eq("CAPTURED").to_numpy()
    order_total = pd.to_numeric(merged["_order_total"], errors="coerce")
    known_total = order_total.notna().to_numpy()
    allowed = np.maximum(MONEY_TOLERANCE_ABS, order_total.abs().to_numpy() * tolerance_pct)
    mismatch = (
        captured
        & known_total
        & ((merged["amount"].astype(float) - order_total).abs() > allowed).to_numpy()
    )
    if not mismatch.any():
        return payments, empty_rejected_frame(payments)

    errors: list[list[tuple[str, str]]] = [[] for _ in range(len(payments))]
    for idx in np.flatnonzero(mismatch):
        errors[int(idx)].append(
            (
                "BUSINESS_PAYMENT_MISMATCH",
                f"captured payment {float(merged['amount'].iloc[idx]):.2f} "
                f"does not match order total "
                f"{float(merged['_order_total'].iloc[idx]):.2f} "
                f"(tolerance {tolerance_pct:.0%})",
            )
        )
    rejected = make_rejected(payments, np.flatnonzero(mismatch), errors)
    valid = payments[~mismatch].reset_index(drop=True)
    logger.warning("Payment amount check rejected=%d/%d", len(rejected), len(payments))
    return valid, rejected


def merge_rule_counts(*mappings: Mapping[str, int]) -> dict[str, int]:
    """Merge rule-count mappings into sorted totals."""
    merged: Counter[str] = Counter()
    for mapping in mappings:
        for rule, count in mapping.items():
            merged[rule] += int(count)
    return dict(sorted(merged.items()))
