"""The 18 required data-quality edge cases (EC-01 .. EC-18).

Each test is deliberately small and named after its edge-case id so the matrix in
``docs/edge_case_matrix.md`` maps one-to-one to executable evidence.
"""

from __future__ import annotations

import pytest

from src.deduplication import deduplicate
from src.publishing import publish_run, table_counts
from src.transformations import transform, transform_customers
from src.validation import (
    SchemaValidationError,
    check_foreign_keys,
    check_order_totals,
    check_payment_amounts,
    validate_rows,
    validate_source_structure,
)
from tests.helpers import (
    rejection_rules_for,
    source_frame,
    valid_minimal_frames,
)

pytestmark = [pytest.mark.data_quality, pytest.mark.edge_case]


def _customer(**overrides) -> dict:
    row = {
        "customer_id": "CUST-001",
        "name": "Alice",
        "email": "alice@example.com",
        "country": "US",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
    }
    row.update(overrides)
    return row


def _product(**overrides) -> dict:
    row = {
        "product_id": "PROD-001",
        "name": "Mouse",
        "category": "Electronics",
        "unit_price": "10.00",
        "currency": "USD",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    row.update(overrides)
    return row


def _order(**overrides) -> dict:
    row = {
        "order_id": "ORD-001",
        "customer_id": "CUST-001",
        "order_date": "2026-01-03T00:00:00Z",
        "status": "PAID",
        "total_amount": "10.00",
        "currency": "USD",
        "updated_at": "2026-01-04T00:00:00Z",
    }
    row.update(overrides)
    return row


def _item(**overrides) -> dict:
    row = {
        "item_id": "ITEM-001",
        "order_id": "ORD-001",
        "product_id": "PROD-001",
        "quantity": "1",
        "unit_price": "10.00",
        "updated_at": "2026-01-03T01:00:00Z",
    }
    row.update(overrides)
    return row


def _payment(**overrides) -> dict:
    row = {
        "payment_id": "PAY-001",
        "order_id": "ORD-001",
        "amount": "10.00",
        "payment_method": "CARD",
        "payment_date": "2026-01-03T02:00:00Z",
        "status": "CAPTURED",
        "updated_at": "2026-01-04T02:00:00Z",
    }
    row.update(overrides)
    return row


def test_ec01_missing_required_value() -> None:
    """EC-01: a required business field is blank/null."""

    outcome = validate_rows(source_frame("customers", [_customer(name="")]), "customers")
    assert outcome.rejected_rows == 1
    assert rejection_rules_for(outcome.rejected, "customer_id", "CUST-001") == ["REQUIRED_NAME"]


def test_ec02_optional_null_is_allowed() -> None:
    """EC-02: a nullable optional field may be blank without rejection."""

    outcome = validate_rows(source_frame("customers", [_customer(email="")]), "customers")
    assert outcome.rejected.empty
    assert outcome.valid.iloc[0]["email"] is None


@pytest.mark.parametrize(
    "created_at",
    [
        "2026-13-01T09:00:00Z",
        "2026-01-01T09:00:00",
        "2026-01-01 09:00:00Z",
        "2026-01-01T09:00:00.1234567Z",
    ],
    ids=["impossible-month", "missing-timezone", "space-separator", "too-many-fractions"],
)
def test_ec03_malformed_timestamp(created_at: str) -> None:
    """EC-03: impossible calendar dates and malformed ISO timestamps are rejected."""

    outcome = validate_rows(
        source_frame("customers", [_customer(created_at=created_at)]),
        "customers",
    )
    assert outcome.rejected_rows == 1
    assert rejection_rules_for(outcome.rejected, "customer_id", "CUST-001") == ["FORMAT_CREATED_AT"]


def test_ec04_wrong_data_type() -> None:
    """EC-04: non-numeric text in a numeric column is rejected."""

    outcome = validate_rows(
        source_frame(
            "products",
            [
                _product(unit_price="abc"),
                _product(product_id="PROD-002", unit_price="NaN"),
            ],
        ),
        "products",
    )
    assert outcome.rejected_rows == 2
    assert rejection_rules_for(outcome.rejected, "product_id", "PROD-001") == ["TYPE_UNIT_PRICE"]


@pytest.mark.parametrize(
    ("entity", "field", "value", "valid", "rule"),
    [
        ("products", "unit_price", "-0.01", False, "RANGE_UNIT_PRICE"),
        ("products", "unit_price", "100000.01", False, "RANGE_UNIT_PRICE"),
        ("products", "unit_price", "100000.00", True, ""),
        ("order_items", "quantity", "0", False, "RANGE_QUANTITY"),
        ("order_items", "quantity", "10001", False, "RANGE_QUANTITY"),
        ("order_items", "quantity", "10000", True, ""),
    ],
    ids=[
        "unit-price-below-min",
        "unit-price-above-max",
        "unit-price-at-max",
        "quantity-below-min",
        "quantity-above-max",
        "quantity-at-max",
    ],
)
def test_ec05_out_of_range_values(
    entity: str, field: str, value: str, valid: bool, rule: str
) -> None:
    """EC-05: numeric boundaries and out-of-range values are enforced."""

    row = _product(**{field: value}) if entity == "products" else _item(**{field: value})
    pk = "product_id" if entity == "products" else "item_id"
    outcome = validate_rows(source_frame(entity, [row]), entity)
    if valid:
        assert outcome.rejected.empty
        assert float(outcome.valid.iloc[0][field]) == float(value)
    else:
        assert outcome.rejected_rows == 1
        assert rejection_rules_for(outcome.rejected, pk, str(row[pk])) == [rule]


@pytest.mark.parametrize(
    ("entity", "field", "value", "rule", "pk"),
    [
        ("orders", "status", "PENDING_PAYMENT", "DOMAIN_STATUS", "order_id"),
        ("orders", "currency", "XYZ", "DOMAIN_CURRENCY", "order_id"),
        ("payments", "payment_method", "CRYPTO", "DOMAIN_PAYMENT_METHOD", "payment_id"),
        ("payments", "status", "UNKNOWN", "DOMAIN_STATUS", "payment_id"),
    ],
    ids=["order-status", "order-currency", "payment-method", "payment-status"],
)
def test_ec06_invalid_enum_domain(entity: str, field: str, value: str, rule: str, pk: str) -> None:
    """EC-06: values outside an allowed enum are rejected."""

    row = _order(**{field: value}) if entity == "orders" else _payment(**{field: value})
    outcome = validate_rows(source_frame(entity, [row]), entity)
    assert rejection_rules_for(outcome.rejected, pk, str(row[pk])) == [rule]


def test_ec07_invalid_pattern_and_length() -> None:
    """EC-07: invalid email/ID patterns and overlong text are rejected."""

    outcome = validate_rows(
        source_frame(
            "customers",
            [
                _customer(email="not-an-email"),
                _customer(customer_id="CUST 002"),
                _customer(customer_id="CUST-003", name="x" * 121),
            ],
        ),
        "customers",
    )
    assert rejection_rules_for(outcome.rejected, "customer_id", "CUST-001") == ["FORMAT_EMAIL"]
    assert rejection_rules_for(outcome.rejected, "customer_id", "CUST 002") == [
        "FORMAT_CUSTOMER_ID"
    ]
    assert rejection_rules_for(outcome.rejected, "customer_id", "CUST-003") == ["FORMAT_NAME"]


def test_ec08_exact_duplicate() -> None:
    """EC-08: an exact duplicate primary-key payload is removed once."""

    outcome = deduplicate(source_frame("customers", [_customer(), _customer()]), "customers")
    assert len(outcome.canonical) == 1
    assert outcome.duplicate_counts == {"DEDUP_EXACT": 1}


def test_ec09_same_key_same_version_conflict() -> None:
    """EC-09: same key/version with different payloads is quarantined."""

    outcome = deduplicate(
        source_frame(
            "customers",
            [
                _customer(email="one@example.com"),
                _customer(email="two@example.com"),
            ],
        ),
        "customers",
    )
    assert outcome.canonical.empty
    assert len(outcome.conflicts) == 2
    assert outcome.duplicate_counts == {"DEDUP_VERSION_CONFLICT": 2}


def test_ec10_superseded_version() -> None:
    """EC-10: the newest updated_at wins; older versions are archived."""

    outcome = deduplicate(
        source_frame(
            "customers",
            [
                _customer(email="old@example.com", updated_at="2026-01-02T00:00:00Z"),
                _customer(email="new@example.com", updated_at="2026-01-05T00:00:00Z"),
            ],
        ),
        "customers",
    )
    assert len(outcome.canonical) == 1
    assert outcome.canonical.iloc[0]["email"] == "new@example.com"
    assert outcome.duplicate_counts == {"DEDUP_SUPERSEDED": 1}


def test_ec11_late_arriving_record(empty_engine) -> None:
    """EC-11: an older version arriving after a newer one is a no-op."""

    publish_run(empty_engine, valid_minimal_frames(), {}, {}, run_id="run-1")
    older = transform(
        validate_rows(
            source_frame(
                "customers",
                [_customer(updated_at="2026-01-01T12:00:00Z")],
            ),
            "customers",
        ).valid,
        "customers",
    )
    outcome = publish_run(empty_engine, {"customers": older}, {}, {}, run_id="run-2")
    assert outcome.entity_stats["customers"].late_arriving == 1
    assert table_counts(empty_engine)["record_history"] == 1


def test_ec12_foreign_key_violation() -> None:
    """EC-12: child rows referencing unaccepted parents are rejected."""

    orders, rejected = check_foreign_keys(
        source_frame("orders", [_order(customer_id="CUST-404")]),
        "customer_id",
        {"CUST-001"},
        "customers",
    )
    assert orders.empty
    assert rejection_rules_for(rejected, "order_id", "ORD-001") == ["FK_CUSTOMER_ID"]

    items, rejected = check_foreign_keys(
        source_frame("order_items", [_item(product_id="PROD-404")]),
        "product_id",
        {"PROD-001"},
        "products",
    )
    assert items.empty
    assert rejection_rules_for(rejected, "item_id", "ITEM-001") == ["FK_PRODUCT_ID"]


def test_ec13_order_without_items() -> None:
    """EC-13: an accepted order must have at least one accepted item."""

    orders, rejected = check_order_totals(
        source_frame("orders", [_order()]), source_frame("order_items", [])
    )
    assert orders.empty
    assert rejection_rules_for(rejected, "order_id", "ORD-001") == ["BUSINESS_ORDER_WITHOUT_ITEMS"]


def test_ec14_order_total_mismatch() -> None:
    """EC-14: order total must equal the sum of accepted line totals."""

    orders, rejected = check_order_totals(
        source_frame("orders", [_order(total_amount="99.99")]),
        source_frame("order_items", [_item()]),
    )
    assert orders.empty
    assert rejection_rules_for(rejected, "order_id", "ORD-001") == ["BUSINESS_ORDER_TOTAL_MISMATCH"]


@pytest.mark.parametrize(
    ("amount", "valid"),
    [
        ("10.00", True),
        ("10.10", True),
        ("10.11", False),
        ("9.90", True),
        ("9.89", False),
        ("25.00", False),
    ],
    ids=[
        "exact",
        "at-tolerance-above",
        "outside-tolerance-above",
        "at-tolerance-below",
        "outside-tolerance-below",
        "gross-mismatch",
    ],
)
def test_ec15_payment_amount_mismatch(amount: str, valid: bool) -> None:
    """EC-15: a captured payment must match its order within tolerance."""

    payments, rejected = check_payment_amounts(
        source_frame("payments", [_payment(amount=amount)]),
        source_frame("orders", [_order(total_amount="10.00")]),
    )
    if valid:
        assert list(payments["payment_id"]) == ["PAY-001"]
        assert rejected.empty
    else:
        assert payments.empty
        assert rejection_rules_for(rejected, "payment_id", "PAY-001") == [
            "BUSINESS_PAYMENT_MISMATCH"
        ]


def test_ec16_schema_drift() -> None:
    """EC-16: missing/extra columns stop the batch instead of drifting."""

    frame = source_frame("customers", [_customer()])
    validate_source_structure(frame, "customers")

    extra = frame.copy()
    extra["loyalty_tier"] = "gold"
    with pytest.raises(SchemaValidationError, match="unexpected columns"):
        validate_source_structure(extra, "customers")

    with pytest.raises(SchemaValidationError, match="missing required columns"):
        validate_source_structure(frame.drop(columns=["updated_at"]), "customers")


def test_ec17_whitespace_and_case_normalisation() -> None:
    """EC-17: text is trimmed and enums/emails are canonicalised."""

    customers = transform_customers(
        source_frame(
            "customers",
            [
                _customer(
                    customer_id=" CUST-001 ",
                    name=" Alice ",
                    email=" ALICE@EXAMPLE.COM ",
                    country=" us ",
                )
            ],
        )
    )
    assert customers.iloc[0]["customer_id"] == "CUST-001"
    assert customers.iloc[0]["name"] == "Alice"
    assert customers.iloc[0]["email"] == "alice@example.com"
    assert customers.iloc[0]["country"] == "US"

    orders = transform(
        validate_rows(
            source_frame("orders", [_order(status=" paid ", currency=" usd ")]),
            "orders",
        ).valid,
        "orders",
    )
    assert orders.iloc[0]["status"] == "PAID"
    assert orders.iloc[0]["currency"] == "USD"


def test_ec18_temporal_inconsistency() -> None:
    """EC-18: updated_at cannot precede the business event timestamp."""

    orders = validate_rows(
        source_frame(
            "orders",
            [_order(order_date="2026-01-05T00:00:00Z", updated_at="2026-01-04T00:00:00Z")],
        ),
        "orders",
    )
    assert rejection_rules_for(orders.rejected, "order_id", "ORD-001") == ["TEMPORAL_UPDATED_AT"]

    payments = validate_rows(
        source_frame(
            "payments",
            [
                _payment(
                    payment_date="2026-01-05T00:00:00Z",
                    updated_at="2026-01-04T00:00:00Z",
                )
            ],
        ),
        "payments",
    )
    assert rejection_rules_for(payments.rejected, "payment_id", "PAY-001") == [
        "TEMPORAL_UPDATED_AT"
    ]
