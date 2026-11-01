"""Parametrised boundary matrices for numeric, timestamp and null handling."""

from __future__ import annotations

import pytest

from src.validation import validate_rows
from tests.helpers import rejection_rules_for, small_rows, source_frame

pytestmark = [pytest.mark.data_quality, pytest.mark.edge_case]


@pytest.mark.parametrize(
    ("amount", "valid"),
    [
        ("0.00", False),
        ("0.01", True),
        ("1000000000.00", True),
        ("1000000000.01", False),
        ("-1.00", False),
    ],
    ids=["zero-exclusive", "minimum-positive", "at-maximum", "above-maximum", "negative"],
)
def test_payment_amount_boundaries(amount: str, valid: bool) -> None:
    row = small_rows("payments")[0]
    row["amount"] = amount
    outcome = validate_rows(source_frame("payments", [row]), "payments")
    if valid:
        assert outcome.rejected.empty
        assert float(outcome.valid.iloc[0]["amount"]) == float(amount)
    else:
        assert rejection_rules_for(outcome.rejected, "payment_id", row["payment_id"]) == [
            "RANGE_AMOUNT"
        ]


@pytest.mark.parametrize(
    ("timestamp", "valid"),
    [
        ("2026-01-01T00:00:00Z", True),
        ("2026-01-01T00:00:00+00:00", True),
        ("2026-01-01T00:00:00.123456Z", True),
        ("2026-01-01T00:00:00+02:00", True),
        ("2026-01-01T00:00:00", False),
        ("2026-02-30T00:00:00Z", False),
    ],
    ids=[
        "utc-z",
        "utc-offset",
        "microseconds",
        "non-utc-offset",
        "missing-offset",
        "impossible-day",
    ],
)
def test_timestamp_format_matrix(timestamp: str, valid: bool) -> None:
    row = small_rows("customers")[0]
    row["created_at"] = timestamp
    row["updated_at"] = "2026-12-01T00:00:00Z"
    outcome = validate_rows(source_frame("customers", [row]), "customers")
    if valid:
        assert outcome.rejected.empty
    else:
        assert rejection_rules_for(outcome.rejected, "customer_id", row["customer_id"]) == [
            "FORMAT_CREATED_AT"
        ]


@pytest.mark.parametrize(
    ("entity", "field", "rule", "pk"),
    [
        ("customers", "name", "REQUIRED_NAME", "customer_id"),
        ("products", "name", "REQUIRED_NAME", "product_id"),
        ("orders", "customer_id", "REQUIRED_CUSTOMER_ID", "order_id"),
        ("order_items", "quantity", "REQUIRED_QUANTITY", "item_id"),
        ("order_items", "product_id", "REQUIRED_PRODUCT_ID", "item_id"),
        ("payments", "payment_date", "REQUIRED_PAYMENT_DATE", "payment_id"),
    ],
    ids=[
        "customer-name",
        "product-name",
        "order-customer",
        "item-quantity",
        "item-product",
        "payment-date",
    ],
)
def test_required_null_inputs(entity: str, field: str, rule: str, pk: str) -> None:
    row = small_rows(entity)[0]
    row[field] = ""
    outcome = validate_rows(source_frame(entity, [row]), entity)
    assert rejection_rules_for(outcome.rejected, pk, row[pk]) == [rule]


@pytest.mark.parametrize(
    "entity",
    ["customers", "products", "orders", "order_items", "payments"],
)
def test_empty_inputs_are_typed_and_accounted(entity: str) -> None:
    outcome = validate_rows(source_frame(entity, []), entity)
    assert outcome.rows_checked == 0
    assert outcome.valid.empty
    assert outcome.rejected.empty
    assert outcome.rule_counts == {}
