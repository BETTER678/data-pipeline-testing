"""Unit tests for structural, row-level and cross-entity validation."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.schemas import ENTITY_SCHEMAS
from src.validation import (
    SchemaValidationError,
    check_foreign_keys,
    check_order_totals,
    check_payment_amounts,
    merge_rule_counts,
    rejection_rules,
    validate_rows,
    validate_source_structure,
)
from tests.helpers import rejection_rules_for, small_rows, source_frame

pytestmark = pytest.mark.unit


def _customer(email: str = "a@example.com", name: str = "Alice") -> dict:
    return {
        "customer_id": "CUST-001",
        "name": name,
        "email": email,
        "country": "US",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
    }


def test_required_blank_and_optional_blank() -> None:
    frame = source_frame(
        "customers",
        [
            _customer(name=""),
            _customer(email=""),
            {**_customer(), "customer_id": "CUST-002"},
        ],
    )
    outcome = validate_rows(frame, "customers")
    assert outcome.rejected_rows == 1
    assert outcome.rows_checked == 3
    assert rejection_rules_for(outcome.rejected, "customer_id", "CUST-001") == ["REQUIRED_NAME"]
    assert set(outcome.valid["customer_id"]) == {"CUST-001", "CUST-002"}
    # Blank optional email is allowed and becomes null.
    blank_email = outcome.valid[outcome.valid["customer_id"] == "CUST-001"].iloc[0]
    assert blank_email["email"] is None


def test_integer_and_float_coercion_and_ranges() -> None:
    items = source_frame(
        "order_items",
        [
            {
                "item_id": "ITEM-1",
                "order_id": "ORD-1",
                "product_id": "PROD-1",
                "quantity": "1",
                "unit_price": "0.00",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "item_id": "ITEM-2",
                "order_id": "ORD-1",
                "product_id": "PROD-1",
                "quantity": "10000",
                "unit_price": "100000.00",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "item_id": "ITEM-3",
                "order_id": "ORD-1",
                "product_id": "PROD-1",
                "quantity": "0",
                "unit_price": "-0.01",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "item_id": "ITEM-4",
                "order_id": "ORD-1",
                "product_id": "PROD-1",
                "quantity": "10001",
                "unit_price": "100000.01",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ],
    )
    outcome = validate_rows(items, "order_items")
    assert set(outcome.valid["item_id"]) == {"ITEM-1", "ITEM-2"}
    assert outcome.valid["quantity"].dtype == "int64"
    assert outcome.valid["unit_price"].dtype == "float64"
    assert rejection_rules_for(outcome.rejected, "item_id", "ITEM-3") == [
        "RANGE_QUANTITY",
        "RANGE_UNIT_PRICE",
    ]
    assert rejection_rules_for(outcome.rejected, "item_id", "ITEM-4") == [
        "RANGE_QUANTITY",
        "RANGE_UNIT_PRICE",
    ]


def test_type_errors_are_reported_once() -> None:
    products = source_frame(
        "products",
        [
            {
                "product_id": "PROD-1",
                "name": "Mouse",
                "category": "Electronics",
                "unit_price": "abc",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "product_id": "PROD-2",
                "name": "Keyboard",
                "category": "Electronics",
                "unit_price": "NaN",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ],
    )
    outcome = validate_rows(products, "products")
    assert outcome.rejected_rows == 2
    assert rejection_rules_for(outcome.rejected, "product_id", "PROD-1") == ["TYPE_UNIT_PRICE"]
    assert rejection_rules_for(outcome.rejected, "product_id", "PROD-2") == ["TYPE_UNIT_PRICE"]


def test_timestamp_format_and_temporal_order() -> None:
    orders = source_frame(
        "orders",
        [
            {
                "order_id": "ORD-1",
                "customer_id": "CUST-001",
                "order_date": "2026-01-02T00:00:00Z",
                "status": "PAID",
                "total_amount": "10.00",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "order_id": "ORD-2",
                "customer_id": "CUST-001",
                "order_date": "2026-13-01T00:00:00Z",
                "status": "PAID",
                "total_amount": "10.00",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "order_id": "ORD-3",
                "customer_id": "CUST-001",
                "order_date": "2026-01-01T00:00:00",
                "status": "PAID",
                "total_amount": "10.00",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ],
    )
    outcome = validate_rows(orders, "orders")
    assert outcome.rejected_rows == 3
    assert rejection_rules_for(outcome.rejected, "order_id", "ORD-1") == ["TEMPORAL_UPDATED_AT"]
    assert rejection_rules_for(outcome.rejected, "order_id", "ORD-2") == ["FORMAT_ORDER_DATE"]
    assert rejection_rules_for(outcome.rejected, "order_id", "ORD-3") == ["FORMAT_ORDER_DATE"]


def test_enum_domains_are_case_insensitive_and_normalised() -> None:
    orders = source_frame(
        "orders",
        [
            {
                "order_id": "ORD-1",
                "customer_id": "CUST-001",
                "order_date": "2026-01-01T00:00:00Z",
                "status": " paid ",
                "total_amount": "10.00",
                "currency": " usd ",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        ],
    )
    outcome = validate_rows(orders, "orders")
    assert outcome.valid.iloc[0]["status"] == "PAID"
    assert outcome.valid.iloc[0]["currency"] == "USD"


def test_invalid_enum_and_invalid_pattern() -> None:
    customers = source_frame(
        "customers",
        [
            _customer(email="not-an-email"),
            {**_customer(), "customer_id": "CUST 002"},
        ],
    )
    outcome = validate_rows(customers, "customers")
    assert rejection_rules_for(outcome.rejected, "customer_id", "CUST-001") == ["FORMAT_EMAIL"]
    assert rejection_rules_for(outcome.rejected, "customer_id", "CUST 002") == [
        "FORMAT_CUSTOMER_ID"
    ]


def test_multiple_errors_on_one_row_are_retained() -> None:
    rows = small_rows("payments")
    row = dict(rows[0])
    row["amount"] = "abc"
    row["payment_date"] = ""
    row["status"] = "NOPE"
    outcome = validate_rows(source_frame("payments", [row]), "payments")
    rules = rejection_rules_for(outcome.rejected, "payment_id", row["payment_id"])
    assert set(rules) == {
        "TYPE_AMOUNT",
        "REQUIRED_PAYMENT_DATE",
        "DOMAIN_STATUS",
    }


def test_source_structure_schema_drift_and_missing_columns() -> None:
    frame = source_frame("customers", [_customer()])
    validate_source_structure(frame, "customers")

    extra = frame.copy()
    extra["unexpected"] = "x"
    with pytest.raises(SchemaValidationError, match="unexpected columns"):
        validate_source_structure(extra, "customers")

    missing = frame.drop(columns=["email"])
    with pytest.raises(SchemaValidationError, match="missing required columns"):
        validate_source_structure(missing, "customers")

    with pytest.raises(SchemaValidationError, match="unknown entity"):
        validate_source_structure(frame, "invoices")


def test_empty_batch_has_typed_columns() -> None:
    frame = source_frame("order_items", [])
    outcome = validate_rows(frame, "order_items")
    assert outcome.rows_checked == 0
    assert outcome.valid.empty
    assert outcome.valid["quantity"].dtype == "int64"
    assert outcome.valid["unit_price"].dtype == "float64"
    assert outcome.rejected.empty


def test_foreign_key_check_quarantines_orphans() -> None:
    orders = source_frame(
        "orders",
        [
            {
                "order_id": "ORD-1",
                "customer_id": "CUST-404",
                "order_date": "2026-01-01T00:00:00Z",
                "status": "PAID",
                "total_amount": "10.00",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        ],
    )
    valid, rejected = check_foreign_keys(orders, "customer_id", {"CUST-001"}, "customers")
    assert valid.empty
    assert len(rejected) == 1
    assert rejection_rules_for(rejected, "order_id", "ORD-1") == ["FK_CUSTOMER_ID"]


def test_order_total_and_missing_items_rules() -> None:
    orders = source_frame(
        "orders",
        [
            {
                "order_id": "ORD-1",
                "customer_id": "CUST-001",
                "order_date": "2026-01-01T00:00:00Z",
                "status": "PAID",
                "total_amount": "100.00",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "order_id": "ORD-2",
                "customer_id": "CUST-001",
                "order_date": "2026-01-01T00:00:00Z",
                "status": "PAID",
                "total_amount": "12.75",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ],
    )
    items = source_frame(
        "order_items",
        [
            {
                "item_id": "ITEM-1",
                "order_id": "ORD-1",
                "product_id": "PROD-1",
                "quantity": "1",
                "unit_price": "25.50",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "item_id": "ITEM-2",
                "order_id": "ORD-2",
                "product_id": "PROD-1",
                "quantity": "1",
                "unit_price": "12.75",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ],
    )
    valid, rejected = check_order_totals(orders, items)
    assert list(valid["order_id"]) == ["ORD-2"]
    assert rejection_rules_for(rejected, "order_id", "ORD-1") == ["BUSINESS_ORDER_TOTAL_MISMATCH"]

    valid, rejected = check_order_totals(orders.iloc[[1]], pd.DataFrame())
    assert valid.empty
    assert rejection_rules_for(rejected, "order_id", "ORD-2") == ["BUSINESS_ORDER_WITHOUT_ITEMS"]


def test_payment_tolerance_and_mismatch() -> None:
    orders = source_frame(
        "orders",
        [
            {
                "order_id": "ORD-1",
                "customer_id": "CUST-001",
                "order_date": "2026-01-01T00:00:00Z",
                "status": "PAID",
                "total_amount": "100.00",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        ],
    )
    payments = source_frame(
        "payments",
        [
            {
                "payment_id": "PAY-1",
                "order_id": "ORD-1",
                "amount": "100.50",
                "payment_method": "CARD",
                "payment_date": "2026-01-01T00:00:00Z",
                "status": "CAPTURED",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "payment_id": "PAY-2",
                "order_id": "ORD-1",
                "amount": "101.01",
                "payment_method": "CARD",
                "payment_date": "2026-01-01T00:00:00Z",
                "status": "CAPTURED",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "payment_id": "PAY-3",
                "order_id": "ORD-1",
                "amount": "500.00",
                "payment_method": "CARD",
                "payment_date": "2026-01-01T00:00:00Z",
                "status": "FAILED",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ],
    )
    valid, rejected = check_payment_amounts(payments, orders)
    assert set(valid["payment_id"]) == {"PAY-1", "PAY-3"}
    assert rejection_rules_for(rejected, "payment_id", "PAY-2") == ["BUSINESS_PAYMENT_MISMATCH"]


def test_payment_amounts_use_published_totals_for_late_payments() -> None:
    payments = source_frame("payments", [small_rows("payments")[0]])
    payments.loc[0, "order_id"] = "ORD-LATE"
    payments.loc[0, "amount"] = "25.00"
    valid, rejected = check_payment_amounts(
        payments,
        source_frame("orders", []),
        published_order_totals={"ORD-LATE": 100.0},
    )
    assert valid.empty
    payment_id = str(payments.iloc[0]["payment_id"])
    assert rejection_rules_for(rejected, "payment_id", payment_id) == ["BUSINESS_PAYMENT_MISMATCH"]


def test_batch_order_totals_take_precedence_over_published_totals() -> None:
    payments = source_frame("payments", [small_rows("payments")[0]])
    payments.loc[0, "amount"] = "100.00"
    orders = source_frame(
        "orders",
        [
            {
                "order_id": "ORD-001",
                "customer_id": "CUST-001",
                "order_date": "2026-01-01T00:00:00Z",
                "status": "PAID",
                "total_amount": "100.00",
                "currency": "USD",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        ],
    )
    valid, rejected = check_payment_amounts(
        payments,
        orders,
        published_order_totals={"ORD-001": 10.0},
    )
    assert list(valid["payment_id"]) == list(payments["payment_id"])
    assert rejected.empty


def test_rule_helpers_and_merge() -> None:
    merged = merge_rule_counts({"A": 1, "B": 2}, {"B": 3})
    assert merged == {"A": 1, "B": 5}
    frame = source_frame("customers", [_customer(name="")])
    outcome = validate_rows(frame, "customers")
    assert "REQUIRED_NAME" in rejection_rules(outcome.rejected)
    assert json.loads(outcome.rejected.iloc[0]["_reject_rules"]) == ["REQUIRED_NAME"]
    assert ENTITY_SCHEMAS["customers"].primary_key == "customer_id"
