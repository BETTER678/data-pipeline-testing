"""Declarative source/target schemas and Pandera target contracts.

Source CSVs are read as text so that malformed values can be rejected at row
level with useful reasons.  The declarative specs below therefore describe the
*expected business type* rather than the physical CSV type.

Pandera is used as a defence-in-depth contract on transformed frames immediately
before publishing.  This keeps the source path explicit (per-row quarantine
reasons) while guaranteeing the target types/keys cannot silently drift.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

import pandas as pd
import pandera.pandas as pa
from pandera import Check

from src.config import (
    ALLOWED_CURRENCIES,
    MAX_AMOUNT,
    MAX_ORDER_TOTAL,
    MAX_QUANTITY,
    MAX_UNIT_PRICE,
    MIN_QUANTITY,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PAYMENT_STATUSES,
    PAYMENT_TOLERANCE_PCT,
    TARGET_LINEAGE_COLUMNS,
)

ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
COUNTRY_PATTERN = r"^[A-Za-z]{2}$"
TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$"


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: str  # string | integer | float | timestamp
    required: bool = True
    nullable: bool = False
    pattern: str | None = None
    pattern_message: str | None = None
    allowed_values: frozenset[str] | None = None
    min_value: float | None = None
    max_value: float | None = None
    min_exclusive: bool = False
    max_length: int | None = None


@dataclass(frozen=True)
class EntitySchema:
    name: str
    columns: tuple[ColumnSpec, ...]
    primary_key: str
    grain: str
    foreign_keys: Mapping[str, tuple[str, str]]
    timestamp_pairs: tuple[tuple[str, str], ...] = ()
    derived_fields: tuple[str, ...] = ()

    @property
    def source_columns(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    @property
    def column_map(self) -> dict[str, ColumnSpec]:
        return {c.name: c for c in self.columns}

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns if c.required)


CUSTOMERS = EntitySchema(
    name="customers",
    primary_key="customer_id",
    grain="one row per customer master record / customer version",
    foreign_keys={},
    timestamp_pairs=(("created_at", "updated_at"),),
    columns=(
        ColumnSpec("customer_id", "string", pattern=ID_PATTERN, max_length=64),
        ColumnSpec("name", "string", max_length=120),
        ColumnSpec(
            "email",
            "string",
            required=False,
            nullable=True,
            pattern=EMAIL_PATTERN,
            pattern_message="must look like name@example.com",
            max_length=254,
        ),
        ColumnSpec(
            "country",
            "string",
            required=False,
            nullable=True,
            pattern=COUNTRY_PATTERN,
            pattern_message="must be a 2-letter country code",
        ),
        ColumnSpec("created_at", "timestamp"),
        ColumnSpec("updated_at", "timestamp"),
    ),
)

PRODUCTS = EntitySchema(
    name="products",
    primary_key="product_id",
    grain="one row per product master record / product version",
    foreign_keys={},
    columns=(
        ColumnSpec("product_id", "string", pattern=ID_PATTERN, max_length=64),
        ColumnSpec("name", "string", max_length=120),
        ColumnSpec("category", "string", max_length=80),
        ColumnSpec(
            "unit_price",
            "float",
            min_value=0.0,
            max_value=MAX_UNIT_PRICE,
        ),
        ColumnSpec("currency", "string", allowed_values=ALLOWED_CURRENCIES),
        ColumnSpec("updated_at", "timestamp"),
    ),
)

ORDERS = EntitySchema(
    name="orders",
    primary_key="order_id",
    grain="one row per order event / order version",
    foreign_keys={"customer_id": ("customers", "customer_id")},
    timestamp_pairs=(("order_date", "updated_at"),),
    derived_fields=("order_year_month",),
    columns=(
        ColumnSpec("order_id", "string", pattern=ID_PATTERN, max_length=64),
        ColumnSpec("customer_id", "string", pattern=ID_PATTERN, max_length=64),
        ColumnSpec("order_date", "timestamp"),
        ColumnSpec("status", "string", allowed_values=ORDER_STATUSES),
        ColumnSpec("total_amount", "float", min_value=0.0, max_value=MAX_ORDER_TOTAL),
        ColumnSpec("currency", "string", allowed_values=ALLOWED_CURRENCIES),
        ColumnSpec("updated_at", "timestamp"),
    ),
)

ORDER_ITEMS = EntitySchema(
    name="order_items",
    primary_key="item_id",
    grain="one row per order line / order line version",
    foreign_keys={
        "order_id": ("orders", "order_id"),
        "product_id": ("products", "product_id"),
    },
    derived_fields=("line_total",),
    columns=(
        ColumnSpec("item_id", "string", pattern=ID_PATTERN, max_length=64),
        ColumnSpec("order_id", "string", pattern=ID_PATTERN, max_length=64),
        ColumnSpec("product_id", "string", pattern=ID_PATTERN, max_length=64),
        ColumnSpec(
            "quantity",
            "integer",
            min_value=float(MIN_QUANTITY),
            max_value=float(MAX_QUANTITY),
        ),
        ColumnSpec("unit_price", "float", min_value=0.0, max_value=MAX_UNIT_PRICE),
        ColumnSpec("updated_at", "timestamp"),
    ),
)

PAYMENTS = EntitySchema(
    name="payments",
    primary_key="payment_id",
    grain="one row per payment event / payment version",
    foreign_keys={"order_id": ("orders", "order_id")},
    timestamp_pairs=(("payment_date", "updated_at"),),
    columns=(
        ColumnSpec("payment_id", "string", pattern=ID_PATTERN, max_length=64),
        ColumnSpec("order_id", "string", pattern=ID_PATTERN, max_length=64),
        ColumnSpec("amount", "float", min_value=0.0, max_value=MAX_AMOUNT, min_exclusive=True),
        ColumnSpec(
            "payment_method",
            "string",
            allowed_values=PAYMENT_METHODS,
        ),
        ColumnSpec("payment_date", "timestamp"),
        ColumnSpec("status", "string", allowed_values=PAYMENT_STATUSES),
        ColumnSpec("updated_at", "timestamp"),
    ),
)

ENTITY_SCHEMAS: dict[str, EntitySchema] = {
    schema.name: schema for schema in (CUSTOMERS, PRODUCTS, ORDERS, ORDER_ITEMS, PAYMENTS)
}

# Business metadata used by documentation and validation helpers.
PAYMENT_TOLERANCE = PAYMENT_TOLERANCE_PCT

# ---------------------------------------------------------------------------
# Pandera target contracts
# ---------------------------------------------------------------------------
_TARGET_EXTRA_COLUMNS: dict[str, dict[str, pa.Column]] = {
    "order_items": {
        "line_total": pa.Column(float, nullable=False, checks=Check.ge(0)),
    },
    "orders": {
        "order_year_month": pa.Column(
            str,
            nullable=False,
            checks=Check.str_matches(r"^\d{4}-\d{2}$"),
        ),
    },
}


def _pandera_column(spec: ColumnSpec) -> pa.Column:
    """Translate a business column spec to a Pandera column."""

    nullable = spec.nullable or not spec.required
    checks: list[Check] = []
    if spec.dtype == "integer":
        checks.append(Check.ge(spec.min_value))
        if spec.max_value is not None:
            checks.append(Check.le(spec.max_value))
        return pa.Column(int, nullable=nullable, checks=checks)
    if spec.dtype == "float":
        if spec.min_exclusive:
            checks.append(Check.gt(spec.min_value))
        elif spec.min_value is not None:
            checks.append(Check.ge(spec.min_value))
        if spec.max_value is not None:
            checks.append(Check.le(spec.max_value))
        return pa.Column(float, nullable=nullable, checks=checks)
    if spec.dtype == "timestamp":
        checks.append(Check.str_matches(TIMESTAMP_PATTERN))
        return pa.Column(str, nullable=nullable, checks=checks)
    if spec.pattern:
        checks.append(Check.str_matches(spec.pattern))
    return pa.Column(str, nullable=nullable, checks=checks)


def _build_target_schema(entity: str) -> pa.DataFrameSchema:
    schema = ENTITY_SCHEMAS[entity]
    columns: dict[str, pa.Column] = {spec.name: _pandera_column(spec) for spec in schema.columns}
    columns.update(_TARGET_EXTRA_COLUMNS.get(entity, {}))
    columns.update(
        {
            "_source_file": pa.Column(str, nullable=False),
            "_source_row_number": pa.Column(int, nullable=False, checks=Check.ge(1)),
            "_run_id": pa.Column(str, nullable=False),
            "_ingested_at": pa.Column(str, nullable=False),
            "_pipeline_version": pa.Column(str, nullable=False),
            "_record_hash": pa.Column(
                str, nullable=False, checks=Check.str_matches(r"^[a-f0-9]{64}$")
            ),
        }
    )
    return pa.DataFrameSchema(columns, strict=True, coerce=False)


PANDERA_TARGET_SCHEMAS: dict[str, pa.DataFrameSchema] = {
    entity: _build_target_schema(entity) for entity in ENTITY_SCHEMAS
}


class TargetSchemaError(Exception):
    """Raised when a transformed frame violates the target contract."""

    def __init__(self, entity: str, details: list[dict]):
        super().__init__(f"Target schema validation failed for {entity}: {details[:3]}")
        self.entity = entity
        self.details = details


def validate_target_frame(df: pd.DataFrame, entity: str) -> pd.DataFrame:
    """Validate a transformed frame with Pandera; return it unchanged."""

    schema = PANDERA_TARGET_SCHEMAS[entity]
    try:
        return schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        details = exc.failure_cases.head(50).to_dict("records")
        raise TargetSchemaError(entity, details) from exc


def target_columns(entity: str) -> tuple[str, ...]:
    schema = ENTITY_SCHEMAS[entity]
    return schema.source_columns + tuple(schema.derived_fields) + tuple(TARGET_LINEAGE_COLUMNS)


def compiled_pattern(name: str) -> re.Pattern[str]:
    patterns = {
        "id": ID_PATTERN,
        "email": EMAIL_PATTERN,
        "country": COUNTRY_PATTERN,
        "timestamp": TIMESTAMP_PATTERN,
    }
    return re.compile(patterns[name])
