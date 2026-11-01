"""E-commerce domain edge cases beyond the generic EC-01..EC-18 matrix."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from src.publishing import publish_run
from src.reconciliation import run_target_checks
from src.transformations import transform
from src.validation import check_payment_amounts, validate_rows
from tests.helpers import source_frame, valid_minimal_frames

pytestmark = [pytest.mark.data_quality, pytest.mark.domain]


def _payment(amount: str) -> dict:
    return {
        "payment_id": "PAY-001",
        "order_id": "ORD-001",
        "amount": amount,
        "payment_method": "CARD",
        "payment_date": "2026-01-03T02:00:00Z",
        "status": "CAPTURED",
        "updated_at": "2026-01-04T02:00:00Z",
    }


def _order(total: str) -> dict:
    return {
        "order_id": "ORD-001",
        "customer_id": "CUST-001",
        "order_date": "2026-01-03T00:00:00Z",
        "status": "PAID",
        "total_amount": total,
        "currency": "USD",
        "updated_at": "2026-01-04T00:00:00Z",
    }


def test_domain_zero_total_free_order_is_legitimate(empty_engine) -> None:
    frames = valid_minimal_frames()
    order_rows = [
        {
            "order_id": "ORD-001",
            "customer_id": "CUST-001",
            "order_date": "2026-01-03T00:00:00Z",
            "status": "PAID",
            "total_amount": "0.00",
            "currency": "USD",
            "updated_at": "2026-01-04T00:00:00Z",
        }
    ]
    item_rows = [
        {
            "item_id": "ITEM-001",
            "order_id": "ORD-001",
            "product_id": "PROD-001",
            "quantity": "1",
            "unit_price": "0.00",
            "updated_at": "2026-01-03T01:00:00Z",
        }
    ]
    for entity, rows in (("orders", order_rows), ("order_items", item_rows)):
        outcome = validate_rows(source_frame(entity, rows), entity)
        assert outcome.rejected.empty
        frames[entity] = transform(outcome.valid, entity)
    frames["payments"] = frames["payments"].iloc[0:0].copy()

    publish_run(empty_engine, frames, {}, {}, run_id="run-free")
    with empty_engine.connect() as connection:
        checks = run_target_checks(connection)
    assert checks["ok"] is True
    assert checks["aggregates"]["order_total_sum"] == 0.0
    assert checks["checks"]["payments.underpaid_orders"]["hard"] is False


def test_domain_split_captured_payments_cannot_overpay(empty_engine) -> None:
    publish_run(empty_engine, valid_minimal_frames(), {}, {}, run_id="run-1")
    with empty_engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO payments (
                    payment_id, order_id, amount, payment_method, payment_date,
                    status, updated_at, _source_file, _source_row_number,
                    _run_id, _ingested_at, _pipeline_version, _record_hash
                ) VALUES (
                    'PAY-002', 'ORD-001', 5.00, 'PAYPAL',
                    '2026-01-03T03:00:00.000000Z', 'CAPTURED',
                    '2026-01-04T03:00:00.000000Z', 'payments.csv', 2,
                    'manual', '2026-01-01T00:00:00+00:00', '1.0.0',
                    '0000000000000000000000000000000000000000000000000000000000000000'
                )
                """
            )
        )
    with empty_engine.connect() as connection:
        checks = run_target_checks(connection)
    # Each payment is individually below the order total, but the aggregate is
    # over it.  The target check must catch the split-payment case.
    assert checks["ok"] is False
    assert checks["checks"]["payments.no_overpayment"]["value"] == 1


def test_domain_payment_tolerance_boundary() -> None:
    payments = source_frame(
        "payments",
        [_payment("10.10"), {**_payment("10.11"), "payment_id": "PAY-002"}],
    )
    orders = source_frame("orders", [_order("10.00")])
    valid, rejected = check_payment_amounts(payments, orders)
    assert list(valid["payment_id"]) == ["PAY-001"]
    assert list(rejected["payment_id"]) == ["PAY-002"]


def test_domain_currency_and_payment_method_domains() -> None:
    orders = validate_rows(source_frame("orders", [_order("10.00")]), "orders")
    assert orders.rejected.empty

    bad_currency = validate_rows(
        source_frame(
            "orders",
            [{**_order("10.00"), "currency": "JPY"}],
        ),
        "orders",
    )
    assert len(bad_currency.rejected) == 1

    bad_method = validate_rows(
        source_frame(
            "payments",
            [{**_payment("10.00"), "payment_method": "CRYPTO"}],
        ),
        "payments",
    )
    assert len(bad_method.rejected) == 1
