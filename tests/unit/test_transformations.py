"""Unit tests for normalisation, derived fields and target contracts."""

from __future__ import annotations

import pandas as pd
import pytest

from src.schemas import TargetSchemaError, validate_target_frame
from src.transformations import (
    transform,
    transform_customers,
    transform_order_items,
    transform_orders,
)
from src.validation import validate_rows
from tests.helpers import small_rows, source_frame

pytestmark = pytest.mark.unit


def _validated(entity: str, rows: list[dict]) -> pd.DataFrame:
    outcome = validate_rows(source_frame(entity, rows), entity)
    assert outcome.rejected.empty
    return outcome.valid


def test_customer_text_normalisation() -> None:
    row = {
        "customer_id": " CUST-001 ",
        "name": " Alice Smith ",
        "email": " ALICE@EXAMPLE.COM ",
        "country": " us ",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-02T00:00:00Z",
    }
    validated = _validated("customers", [row])
    output = transform_customers(validated)
    assert output.iloc[0]["customer_id"] == "CUST-001"
    assert output.iloc[0]["name"] == "Alice Smith"
    assert output.iloc[0]["email"] == "alice@example.com"
    assert output.iloc[0]["country"] == "US"
    assert output.iloc[0]["created_at"] == "2026-01-01T00:00:00.000000Z"


def test_order_derived_year_month() -> None:
    validated = _validated("orders", [small_rows("orders")[0]])
    output = transform_orders(validated)
    assert output.iloc[0]["order_year_month"] == "2026-01"


def test_line_total_is_rounded_money() -> None:
    validated = _validated("order_items", [small_rows("order_items")[0]])
    output = transform_order_items(validated)
    assert output.iloc[0]["line_total"] == 51.00
    assert output.iloc[0]["quantity"] == 2
    assert output.iloc[0]["updated_at"].endswith("Z")


def test_transform_attaches_pipeline_metadata_and_hash() -> None:
    validated = _validated("customers", [small_rows("customers")[0]])
    output = transform(validated, "customers")
    assert output.iloc[0]["_pipeline_version"] == "1.0.0"
    assert len(output.iloc[0]["_record_hash"]) == 64
    assert "_source_file" in output.columns
    # The input frame is not mutated.
    assert "_pipeline_version" not in validated.columns


def test_target_contract_accepts_transformed_small_data() -> None:
    for entity in ("customers", "products", "orders", "order_items", "payments"):
        validated = _validated(entity, [small_rows(entity)[0]])
        output = transform(validated, entity)
        validate_target_frame(output, entity)


def test_target_contract_rejects_schema_drift() -> None:
    validated = _validated("customers", [small_rows("customers")[0]])
    output = transform(validated, "customers")
    broken = output.drop(columns=["email"])
    with pytest.raises(TargetSchemaError):
        validate_target_frame(broken, "customers")


def test_target_contract_rejects_invalid_derived_value() -> None:
    validated = _validated("order_items", [small_rows("order_items")[0]])
    output = transform(validated, "order_items")
    output.loc[output.index[0], "line_total"] = -1.0
    with pytest.raises(TargetSchemaError):
        validate_target_frame(output, "order_items")
