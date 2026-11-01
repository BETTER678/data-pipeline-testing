"""Reproducible synthetic e-commerce data.

Two modes:
* ``small``: a hand-labelled 79-row dataset containing every required boundary,
  duplicate, null, malformed, FK and domain scenario.  Expected outcomes live in
  ``data/expected/small_expected.json``, never in the pipeline input.
* ``scalable``: seeded, configurable base data plus injected invalid, null,
  duplicate and late-arriving rows.  Base data is valid by construction, so the
  expected target counts are exact.

Only synthetic data is produced; no real customer information is used.
"""

from __future__ import annotations

import csv
import json
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from faker import Faker

from src.config import ENTITIES


@dataclass(frozen=True)
class ScalableConfig:
    customers: int = 500
    products: int = 200
    orders: int = 2_500
    seed: int = 42
    invalid_pct: float = 0.02
    duplicate_pct: float = 0.01
    null_pct: float = 0.01
    late_pct: float = 0.01


def _stamp(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_csv(path: Path, columns: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


# ---------------------------------------------------------------------------
# Curated small dataset
# ---------------------------------------------------------------------------
# fmt: off
def _small_customer_rows() -> list[dict[str, Any]]:
    return [
        {"customer_id": "CUST-001", "name": "Alice Smith", "email": "alice@example.com", "country": "US", "created_at": "2025-12-01T09:00:00Z", "updated_at": "2025-12-01T09:00:00Z"},
        {"customer_id": "CUST-002", "name": "Bob Jones", "email": "bob@example.com", "country": "GB", "created_at": "2025-12-02T10:00:00Z", "updated_at": "2025-12-03T11:00:00Z"},
        {"customer_id": "CUST-003", "name": "Carol White", "email": "carol@example.com", "country": "DE", "created_at": "2026-01-01T08:30:00Z", "updated_at": "2026-01-01T08:30:00Z"},
        {"customer_id": "CUST-004", "name": "Dan Brown", "email": "", "country": "US", "created_at": "2025-12-05T09:00:00Z", "updated_at": "2025-12-05T09:00:00Z"},
        {"customer_id": "CUST-005", "name": "Eve Black", "email": "eve@example.com", "country": "FR", "created_at": "2025-12-06T09:00:00Z", "updated_at": "2025-12-06T09:00:00Z"},
        {"customer_id": "CUST-006", "name": "Frank Green", "email": "", "country": "US", "created_at": "2025-12-07T09:00:00Z", "updated_at": "2025-12-07T09:00:00Z"},
        {"customer_id": "CUST-007", "name": "", "email": "bad-name@example.com", "country": "US", "created_at": "2025-12-08T09:00:00Z", "updated_at": "2025-12-08T09:00:00Z"},
        {"customer_id": "CUST-008", "name": "Grace Hall", "email": "grace@example.com", "country": "US", "created_at": "not-a-timestamp", "updated_at": "2025-12-09T09:00:00Z"},
        {"customer_id": "CUST-009", "name": "Henry Lee", "email": "henry@example.com", "country": "CA", "created_at": "2025-12-10T09:00:00Z", "updated_at": "2026-01-20T09:00:00Z"},
        {"customer_id": "CUST-009", "name": "Henry Lee", "email": "henry.old@example.com", "country": "CA", "created_at": "2025-12-10T09:00:00Z", "updated_at": "2025-12-15T09:00:00Z"},
        {"customer_id": "CUST-010", "name": "Ivy King", "email": "ivy@example.com", "country": "US", "created_at": "2026-02-01T09:00:00Z", "updated_at": "2026-01-01T09:00:00Z"},
        {"customer_id": "CUST-002", "name": "Bob Jones", "email": "bob@example.com", "country": "GB", "created_at": "2025-12-02T10:00:00Z", "updated_at": "2025-12-03T11:00:00Z"},
        {"customer_id": "CUST-002", "name": "Bob Jones", "email": "bob.new@example.com", "country": "GB", "created_at": "2025-12-02T10:00:00Z", "updated_at": "2025-12-05T12:00:00Z"},
    ]

def _small_product_rows() -> list[dict[str, Any]]:
    return [
        {"product_id": "PROD-001", "name": "Wireless Mouse", "category": "Electronics", "unit_price": "25.50", "currency": "USD", "updated_at": "2025-12-01T09:00:00Z"},
        {"product_id": "PROD-002", "name": "Mechanical Keyboard", "category": "Electronics", "unit_price": "89.99", "currency": "USD", "updated_at": "2025-12-01T09:00:00Z"},
        {"product_id": "PROD-003", "name": "Sticker Pack", "category": "Accessories", "unit_price": "0.00", "currency": "USD", "updated_at": "2025-12-01T09:00:00Z"},
        {"product_id": "PROD-004", "name": "Luxury Pen", "category": "Luxury", "unit_price": "100000.00", "currency": "USD", "updated_at": "2025-12-01T09:00:00Z"},
        {"product_id": "PROD-005", "name": "Negative Price", "category": "Bad", "unit_price": "-0.01", "currency": "USD", "updated_at": "2025-12-01T09:00:00Z"},
        {"product_id": "PROD-006", "name": "", "category": "Bad", "unit_price": "10.00", "currency": "USD", "updated_at": "2025-12-01T09:00:00Z"},
        {"product_id": "PROD-007", "name": "Bad Price", "category": "Bad", "unit_price": "abc", "currency": "USD", "updated_at": "2025-12-01T09:00:00Z"},
        {"product_id": "PROD-008", "name": "Bad Currency", "category": "Bad", "unit_price": "10.00", "currency": "XYZ", "updated_at": "2025-12-01T09:00:00Z"},
        {"product_id": "PROD-009", "name": "Missing Version", "category": "Bad", "unit_price": "10.00", "currency": "USD", "updated_at": ""},
        {"product_id": "PROD-001", "name": "Wireless Mouse", "category": "Electronics", "unit_price": "25.50", "currency": "USD", "updated_at": "2025-12-01T09:00:00Z"},
    ]

def _small_order_rows() -> list[dict[str, Any]]:
    return [
        {"order_id": "ORD-1001", "customer_id": "CUST-001", "order_date": "2026-01-05T10:00:00Z", "status": "PAID", "total_amount": "140.99", "currency": "USD", "updated_at": "2026-01-05T10:10:00Z"},
        {"order_id": "ORD-1002", "customer_id": "CUST-002", "order_date": "2026-01-06T11:00:00Z", "status": "PAID", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-06T11:10:00Z"},
        {"order_id": "ORD-1003", "customer_id": "CUST-003", "order_date": "2026-01-07T12:00:00Z", "status": "PENDING", "total_amount": "0.00", "currency": "USD", "updated_at": "2026-01-07T12:05:00Z"},
        {"order_id": "ORD-1004", "customer_id": "CUST-004", "order_date": "2026-01-08T13:00:00Z", "status": "PAID", "total_amount": "200000.00", "currency": "USD", "updated_at": "2026-01-08T13:10:00Z"},
        {"order_id": "ORD-1005", "customer_id": "CUST-005", "order_date": "2026-01-09T14:00:00Z", "status": "PAID", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-09T14:10:00Z"},
        {"order_id": "ORD-1005", "customer_id": "CUST-005", "order_date": "2026-01-09T14:00:00Z", "status": "PAID", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-09T14:10:00Z"},
        {"order_id": "ORD-1005", "customer_id": "CUST-005", "order_date": "2026-01-09T14:00:00Z", "status": "SHIPPED", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-10T09:00:00Z"},
        {"order_id": "ORD-1006", "customer_id": "CUST-007", "order_date": "2026-01-10T10:00:00Z", "status": "PAID", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-10T10:05:00Z"},
        {"order_id": "ORD-1007", "customer_id": "CUST-001", "order_date": "2026-01-11T10:00:00Z", "status": "PAID", "total_amount": "999.99", "currency": "USD", "updated_at": "2026-01-11T10:05:00Z"},
        {"order_id": "ORD-1008", "customer_id": "CUST-001", "order_date": "2026-01-12T10:00:00Z", "status": "PENDING_PAYMENT", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-12T10:05:00Z"},
        {"order_id": "ORD-1009", "customer_id": "CUST-001", "order_date": "05-01-2026", "status": "PAID", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-13T10:05:00Z"},
        {"order_id": "ORD-1010", "customer_id": "CUST-001", "order_date": "2026-01-14T10:00:00Z", "status": "PAID", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-14T10:05:00Z"},
        {"order_id": "ORD-1011", "customer_id": "CUST-001", "order_date": "2026-01-15T10:00:00Z", "status": "PAID", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-01T10:00:00Z"},
        {"order_id": "ORD-1012", "customer_id": "CUST-001", "order_date": "2026-01-16T10:00:00Z", "status": "PAID", "total_amount": "-5.00", "currency": "USD", "updated_at": "2026-01-16T10:05:00Z"},
        {"order_id": "ORD-1013", "customer_id": "CUST-999", "order_date": "2026-01-17T10:00:00Z", "status": "PAID", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-17T10:05:00Z"},
        {"order_id": "ORD-1014", "customer_id": "CUST-006", "order_date": "2026-01-18T10:00:00Z", "status": "PAID", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-19T10:00:00Z"},
        {"order_id": "ORD-1014", "customer_id": "CUST-006", "order_date": "2026-01-18T10:00:00Z", "status": "PENDING", "total_amount": "25.50", "currency": "USD", "updated_at": "2026-01-18T12:00:00Z"},
    ]

def _small_order_item_rows() -> list[dict[str, Any]]:
    return [
        {"item_id": "ITEM-1001", "order_id": "ORD-1001", "product_id": "PROD-001", "quantity": "2", "unit_price": "25.50", "updated_at": "2026-01-05T10:02:00Z"},
        {"item_id": "ITEM-1002", "order_id": "ORD-1001", "product_id": "PROD-002", "quantity": "1", "unit_price": "89.99", "updated_at": "2026-01-05T10:02:00Z"},
        {"item_id": "ITEM-1003", "order_id": "ORD-1002", "product_id": "PROD-001", "quantity": "1", "unit_price": "25.50", "updated_at": "2026-01-06T11:02:00Z"},
        {"item_id": "ITEM-1004", "order_id": "ORD-1003", "product_id": "PROD-003", "quantity": "1", "unit_price": "0.00", "updated_at": "2026-01-07T12:02:00Z"},
        {"item_id": "ITEM-1005", "order_id": "ORD-1004", "product_id": "PROD-004", "quantity": "2", "unit_price": "100000.00", "updated_at": "2026-01-08T13:02:00Z"},
        {"item_id": "ITEM-1006", "order_id": "ORD-1005", "product_id": "PROD-001", "quantity": "1", "unit_price": "25.50", "updated_at": "2026-01-09T14:01:00Z"},
        {"item_id": "ITEM-1007", "order_id": "ORD-1006", "product_id": "PROD-001", "quantity": "1", "unit_price": "25.50", "updated_at": "2026-01-10T10:01:00Z"},
        {"item_id": "ITEM-1008", "order_id": "ORD-1007", "product_id": "PROD-001", "quantity": "1", "unit_price": "25.50", "updated_at": "2026-01-11T10:01:00Z"},
        {"item_id": "ITEM-1009", "order_id": "ORD-1011", "product_id": "PROD-001", "quantity": "1", "unit_price": "25.50", "updated_at": "2026-01-15T10:01:00Z"},
        {"item_id": "ITEM-1010", "order_id": "ORD-1013", "product_id": "PROD-001", "quantity": "1", "unit_price": "25.50", "updated_at": "2026-01-17T10:01:00Z"},
        {"item_id": "ITEM-1011", "order_id": "ORD-1014", "product_id": "PROD-001", "quantity": "1", "unit_price": "25.50", "updated_at": "2026-01-18T10:01:00Z"},
        {"item_id": "ITEM-1100", "order_id": "ORD-1001", "product_id": "PROD-001", "quantity": "0", "unit_price": "25.50", "updated_at": "2026-01-05T10:03:00Z"},
        {"item_id": "ITEM-1101", "order_id": "ORD-1002", "product_id": "PROD-001", "quantity": "two", "unit_price": "25.50", "updated_at": "2026-01-06T11:03:00Z"},
        {"item_id": "ITEM-1102", "order_id": "ORD-1002", "product_id": "PROD-999", "quantity": "1", "unit_price": "25.50", "updated_at": "2026-01-06T11:03:00Z"},
        {"item_id": "ITEM-1103", "order_id": "ORD-9999", "product_id": "PROD-001", "quantity": "1", "unit_price": "25.50", "updated_at": "2026-01-06T11:03:00Z"},
        {"item_id": "ITEM-1104", "order_id": "ORD-1004", "product_id": "PROD-001", "quantity": "10001", "unit_price": "25.50", "updated_at": "2026-01-08T13:03:00Z"},
        {"item_id": "ITEM-1105", "order_id": "ORD-1004", "product_id": "PROD-001", "quantity": "10000", "unit_price": "0.00", "updated_at": "2026-01-08T13:03:00Z"},
        {"item_id": "ITEM-1106", "order_id": "ORD-1003", "product_id": "PROD-002", "quantity": "1.5", "unit_price": "89.99", "updated_at": "2026-01-07T12:03:00Z"},
        {"item_id": "ITEM-1001", "order_id": "ORD-1001", "product_id": "PROD-001", "quantity": "2", "unit_price": "25.50", "updated_at": "2026-01-05T10:02:00Z"},
        {"item_id": "ITEM-1109", "order_id": "ORD-1002", "product_id": "PROD-002", "quantity": "", "unit_price": "89.99", "updated_at": "2026-01-06T11:04:00Z"},
        {"item_id": "ITEM-1110", "order_id": "ORD-1002", "product_id": "", "quantity": "1", "unit_price": "10.00", "updated_at": "2026-01-06T11:04:00Z"},
    ]

def _small_payment_rows() -> list[dict[str, Any]]:
    return [
        {"payment_id": "PAY-2001", "order_id": "ORD-1001", "amount": "140.99", "payment_method": "CARD", "payment_date": "2026-01-05T10:05:00Z", "status": "CAPTURED", "updated_at": "2026-01-05T10:06:00Z"},
        {"payment_id": "PAY-2002", "order_id": "ORD-1002", "amount": "25.50", "payment_method": "CARD", "payment_date": "2026-01-06T11:05:00Z", "status": "CAPTURED", "updated_at": "2026-01-06T11:06:00Z"},
        {"payment_id": "PAY-2003", "order_id": "ORD-1004", "amount": "200000.00", "payment_method": "BANK_TRANSFER", "payment_date": "2026-01-08T13:05:00Z", "status": "CAPTURED", "updated_at": "2026-01-08T13:06:00Z"},
        {"payment_id": "PAY-2004", "order_id": "ORD-1005", "amount": "25.50", "payment_method": "PAYPAL", "payment_date": "2026-01-09T14:05:00Z", "status": "CAPTURED", "updated_at": "2026-01-09T14:06:00Z"},
        {"payment_id": "PAY-2005", "order_id": "ORD-1014", "amount": "25.50", "payment_method": "CARD", "payment_date": "2026-01-18T10:05:00Z", "status": "CAPTURED", "updated_at": "2026-01-18T10:06:00Z"},
        {"payment_id": "PAY-2006", "order_id": "ORD-1001", "amount": "100.00", "payment_method": "CARD", "payment_date": "2026-01-05T10:07:00Z", "status": "CAPTURED", "updated_at": "2026-01-05T10:08:00Z"},
        {"payment_id": "PAY-2007", "order_id": "ORD-1002", "amount": "abc", "payment_method": "CARD", "payment_date": "2026-01-06T11:07:00Z", "status": "CAPTURED", "updated_at": "2026-01-06T11:08:00Z"},
        {"payment_id": "PAY-2008", "order_id": "ORD-9999", "amount": "25.50", "payment_method": "CARD", "payment_date": "2026-01-06T11:09:00Z", "status": "CAPTURED", "updated_at": "2026-01-06T11:10:00Z"},
        {"payment_id": "PAY-2009", "order_id": "ORD-1002", "amount": "25.50", "payment_method": "CARD", "payment_date": "", "status": "CAPTURED", "updated_at": "2026-01-06T11:12:00Z"},
        {"payment_id": "PAY-2010", "order_id": "ORD-1002", "amount": "25.50", "payment_method": "CARD", "payment_date": "2026-01-06T11:13:00Z", "status": "UNKNOWN", "updated_at": "2026-01-06T11:14:00Z"},
        {"payment_id": "PAY-2002", "order_id": "ORD-1002", "amount": "25.50", "payment_method": "CARD", "payment_date": "2026-01-06T11:05:00Z", "status": "CAPTURED", "updated_at": "2026-01-06T11:06:00Z"},
        {"payment_id": "PAY-2012", "order_id": "ORD-1005", "amount": "25.50", "payment_method": "CARD", "payment_date": "2026-13-01T10:00:00Z", "status": "CAPTURED", "updated_at": "2026-01-13T10:00:00Z"},
        {"payment_id": "PAY-2013", "order_id": "ORD-1006", "amount": "25.50", "payment_method": "CARD", "payment_date": "2026-01-10T10:05:00Z", "status": "CAPTURED", "updated_at": "2026-01-10T10:06:00Z"},
        {"payment_id": "PAY-2014", "order_id": "ORD-1003", "amount": "0.00", "payment_method": "CARD", "payment_date": "2026-01-07T12:05:00Z", "status": "CAPTURED", "updated_at": "2026-01-07T12:06:00Z"},
        {"payment_id": "PAY-2015", "order_id": "ORD-1002", "amount": "25.50", "payment_method": "CARD", "payment_date": "2026-01-06T11:15:00Z", "status": "FAILED", "updated_at": "2026-01-06T11:16:00Z"},
        {"payment_id": "PAY-2016", "order_id": "ORD-1005", "amount": "25.50", "payment_method": "CARD", "payment_date": "2026-01-09T14:07:00Z", "status": "FAILED", "updated_at": "2026-01-09T14:08:00Z"},
        {"payment_id": "PAY-2016", "order_id": "ORD-1005", "amount": "25.50", "payment_method": "CARD", "payment_date": "2026-01-09T14:07:00Z", "status": "REFUNDED", "updated_at": "2026-01-11T09:00:00Z"},
        {"payment_id": "PAY-2018", "order_id": "ORD-1002", "amount": "25.50", "payment_method": "CARD", "payment_date": "2026-01-20T10:00:00Z", "status": "CAPTURED", "updated_at": "2026-01-19T10:00:00Z"},
    ]

# fmt: on


def _small_frames() -> dict[str, tuple[list[str], list[dict[str, Any]]]]:
    customers_columns = ["customer_id", "name", "email", "country", "created_at", "updated_at"]
    products_columns = ["product_id", "name", "category", "unit_price", "currency", "updated_at"]
    orders_columns = [
        "order_id",
        "customer_id",
        "order_date",
        "status",
        "total_amount",
        "currency",
        "updated_at",
    ]
    order_items_columns = [
        "item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "updated_at",
    ]
    payments_columns = [
        "payment_id",
        "order_id",
        "amount",
        "payment_method",
        "payment_date",
        "status",
        "updated_at",
    ]
    return {
        "customers": (customers_columns, _small_customer_rows()),
        "products": (products_columns, _small_product_rows()),
        "orders": (orders_columns, _small_order_rows()),
        "order_items": (order_items_columns, _small_order_item_rows()),
        "payments": (payments_columns, _small_payment_rows()),
    }


def _small_expected_customers() -> dict[str, Any]:
    return {
        "source_rows": 13,
        "valid_rows": 7,
        "rejected_rows": 3,
        "duplicate_rows": 3,
        "duplicate_rule_counts": {"DEDUP_EXACT": 1, "DEDUP_SUPERSEDED": 2},
        "canonical_ids": [
            "CUST-001",
            "CUST-002",
            "CUST-003",
            "CUST-004",
            "CUST-005",
            "CUST-006",
            "CUST-009",
        ],
        "rejected_ids": ["CUST-007", "CUST-008", "CUST-010"],
        "rejected_rules": {
            "CUST-007": ["REQUIRED_NAME"],
            "CUST-008": ["FORMAT_CREATED_AT"],
            "CUST-010": ["TEMPORAL_UPDATED_AT"],
        },
        "canonical_overrides": {
            "CUST-002": {"email": "bob.new@example.com"},
            "CUST-009": {"email": "henry@example.com"},
        },
    }


def _small_expected_products() -> dict[str, Any]:
    return {
        "source_rows": 10,
        "valid_rows": 4,
        "rejected_rows": 5,
        "duplicate_rows": 1,
        "duplicate_rule_counts": {"DEDUP_EXACT": 1},
        "canonical_ids": ["PROD-001", "PROD-002", "PROD-003", "PROD-004"],
        "rejected_ids": ["PROD-005", "PROD-006", "PROD-007", "PROD-008", "PROD-009"],
        "rejected_rules": {
            "PROD-005": ["RANGE_UNIT_PRICE"],
            "PROD-006": ["REQUIRED_NAME"],
            "PROD-007": ["TYPE_UNIT_PRICE"],
            "PROD-008": ["DOMAIN_CURRENCY"],
            "PROD-009": ["REQUIRED_UPDATED_AT"],
        },
    }


def _small_expected_orders() -> dict[str, Any]:
    return {
        "source_rows": 17,
        "valid_rows": 6,
        "rejected_rows": 8,
        "duplicate_rows": 3,
        "duplicate_rule_counts": {"DEDUP_EXACT": 1, "DEDUP_SUPERSEDED": 2},
        "canonical_ids": [
            "ORD-1001",
            "ORD-1002",
            "ORD-1003",
            "ORD-1004",
            "ORD-1005",
            "ORD-1014",
        ],
        "rejected_ids": [
            "ORD-1006",
            "ORD-1007",
            "ORD-1008",
            "ORD-1009",
            "ORD-1010",
            "ORD-1011",
            "ORD-1012",
            "ORD-1013",
        ],
        "rejected_rules": {
            "ORD-1006": ["FK_CUSTOMER_ID"],
            "ORD-1007": ["BUSINESS_ORDER_TOTAL_MISMATCH"],
            "ORD-1008": ["DOMAIN_STATUS"],
            "ORD-1009": ["FORMAT_ORDER_DATE"],
            "ORD-1010": ["BUSINESS_ORDER_WITHOUT_ITEMS"],
            "ORD-1011": ["TEMPORAL_UPDATED_AT"],
            "ORD-1012": ["RANGE_TOTAL_AMOUNT"],
            "ORD-1013": ["FK_CUSTOMER_ID"],
        },
        "canonical_overrides": {
            "ORD-1005": {"status": "SHIPPED"},
            "ORD-1014": {"status": "PAID"},
        },
    }


def _small_expected_order_items() -> dict[str, Any]:
    return {
        "source_rows": 21,
        "valid_rows": 8,
        "rejected_rows": 12,
        "duplicate_rows": 1,
        "duplicate_rule_counts": {"DEDUP_EXACT": 1},
        "canonical_ids": [
            "ITEM-1001",
            "ITEM-1002",
            "ITEM-1003",
            "ITEM-1004",
            "ITEM-1005",
            "ITEM-1006",
            "ITEM-1011",
            "ITEM-1105",
        ],
        "rejected_ids": [
            "ITEM-1007",
            "ITEM-1008",
            "ITEM-1009",
            "ITEM-1010",
            "ITEM-1100",
            "ITEM-1101",
            "ITEM-1102",
            "ITEM-1103",
            "ITEM-1104",
            "ITEM-1106",
            "ITEM-1109",
            "ITEM-1110",
        ],
        "rejected_rules": {
            "ITEM-1007": ["FK_ORDER_ID"],
            "ITEM-1008": ["FK_ORDER_ID"],
            "ITEM-1009": ["FK_ORDER_ID"],
            "ITEM-1010": ["FK_ORDER_ID"],
            "ITEM-1100": ["RANGE_QUANTITY"],
            "ITEM-1101": ["TYPE_QUANTITY"],
            "ITEM-1102": ["FK_PRODUCT_ID"],
            "ITEM-1103": ["FK_ORDER_ID"],
            "ITEM-1104": ["RANGE_QUANTITY"],
            "ITEM-1106": ["TYPE_QUANTITY"],
            "ITEM-1109": ["REQUIRED_QUANTITY"],
            "ITEM-1110": ["REQUIRED_PRODUCT_ID"],
        },
    }


def _small_expected_payments() -> dict[str, Any]:
    return {
        "source_rows": 18,
        "valid_rows": 7,
        "rejected_rows": 9,
        "duplicate_rows": 2,
        "duplicate_rule_counts": {"DEDUP_EXACT": 1, "DEDUP_SUPERSEDED": 1},
        "canonical_ids": [
            "PAY-2001",
            "PAY-2002",
            "PAY-2003",
            "PAY-2004",
            "PAY-2005",
            "PAY-2015",
            "PAY-2016",
        ],
        "rejected_ids": [
            "PAY-2006",
            "PAY-2007",
            "PAY-2008",
            "PAY-2009",
            "PAY-2010",
            "PAY-2012",
            "PAY-2013",
            "PAY-2014",
            "PAY-2018",
        ],
        "rejected_rules": {
            "PAY-2006": ["BUSINESS_PAYMENT_MISMATCH"],
            "PAY-2007": ["TYPE_AMOUNT"],
            "PAY-2008": ["FK_ORDER_ID"],
            "PAY-2009": ["REQUIRED_PAYMENT_DATE"],
            "PAY-2010": ["DOMAIN_STATUS"],
            "PAY-2012": ["FORMAT_PAYMENT_DATE"],
            "PAY-2013": ["FK_ORDER_ID"],
            "PAY-2014": ["RANGE_AMOUNT"],
            "PAY-2018": ["TEMPORAL_UPDATED_AT"],
        },
        "canonical_overrides": {"PAY-2016": {"status": "REFUNDED"}},
    }


def _small_expected_entities() -> dict[str, dict[str, Any]]:
    return {
        "customers": _small_expected_customers(),
        "products": _small_expected_products(),
        "orders": _small_expected_orders(),
        "order_items": _small_expected_order_items(),
        "payments": _small_expected_payments(),
    }


def small_expected() -> dict[str, Any]:
    """Return the deterministic expected-answer contract for the curated sample."""
    return {
        "dataset": "small",
        "seed": 42,
        "source_rows_total": 79,
        "entities": _small_expected_entities(),
        "target_counts": {
            "customers": 7,
            "products": 4,
            "orders": 6,
            "order_items": 8,
            "payments": 7,
        },
        "target_aggregates": {
            "order_total_sum": 200217.49,
            "line_total_sum": 200217.49,
            "captured_payment_sum": 200217.49,
        },
        "expected_status": "PARTIAL_SUCCESS",
        "expected_quarantine_rows": 37,
        "expected_history_rows": 10,
    }


def generate_small(
    output_dir: str | Path, expected_dir: str | Path | None = None
) -> dict[str, Any]:
    """Generate the deterministic 79-row sample and expected contract."""
    output_dir = Path(output_dir)
    frames = _small_frames()
    for entity, (columns, rows) in frames.items():
        _write_csv(output_dir / f"{entity}.csv", columns, rows)
    expected = small_expected()
    if expected_dir is not None:
        expected_dir = Path(expected_dir)
        expected_dir.mkdir(parents=True, exist_ok=True)
        (expected_dir / "small_expected.json").write_text(
            json.dumps(expected, indent=2), encoding="utf-8", newline="\n"
        )
    return expected


# ---------------------------------------------------------------------------
# Scalable generator
# ---------------------------------------------------------------------------
def _count(total: int, pct: float) -> int:
    return int(math.ceil(total * pct)) if pct > 0 else 0


def _build_customers(
    config: ScalableConfig,
    rng: np.random.Generator,
    fake: Faker,
    countries: list[str],
) -> list[dict[str, Any]]:
    customers: list[dict[str, Any]] = []
    for index in range(1, config.customers + 1):
        created = datetime(2025, 1, 1, tzinfo=UTC) + timedelta(
            seconds=int(rng.integers(0, 365 * 24 * 3600))
        )
        updated = created + timedelta(seconds=int(rng.integers(0, 30 * 24 * 3600)))
        customers.append(
            {
                "customer_id": f"CUST-{index:06d}",
                "name": fake.name(),
                "email": fake.email(),
                "country": str(rng.choice(countries)),
                "created_at": _stamp(created),
                "updated_at": _stamp(updated),
            }
        )
    return customers


def _build_products(
    config: ScalableConfig,
    rng: np.random.Generator,
    fake: Faker,
    categories: list[str],
) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    for index in range(1, config.products + 1):
        updated = datetime(2025, 6, 1, tzinfo=UTC) + timedelta(
            seconds=int(rng.integers(0, 180 * 24 * 3600))
        )
        products.append(
            {
                "product_id": f"PROD-{index:06d}",
                "name": f"{fake.word().title()} {index}",
                "category": str(rng.choice(categories)),
                "unit_price": f"{round(float(rng.uniform(1.0, 500.0)), 2):.2f}",
                "currency": "USD",
                "updated_at": _stamp(updated),
            }
        )
    return products


def _build_one_order(
    index: int,
    rng: np.random.Generator,
    customer_ids: list[str],
    product_ids: list[str],
    product_prices: dict[str, float],
    methods: list[str],
    item_seq: int,
    payment_seq: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    order_id = f"ORD-{index:06d}"
    order_date = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(
        seconds=int(rng.integers(0, 89 * 24 * 3600))
    )
    item_count = int(rng.integers(1, 4))
    total = 0.0
    item_rows: list[dict[str, Any]] = []
    for _ in range(item_count):
        product_id = str(rng.choice(product_ids))
        quantity = int(rng.integers(1, 5))
        price = product_prices[product_id]
        total += round(quantity * price, 2)
        item_rows.append(
            {
                "item_id": f"ITEM-{item_seq:08d}",
                "order_id": order_id,
                "product_id": product_id,
                "quantity": str(quantity),
                "unit_price": f"{price:.2f}",
                "updated_at": _stamp(order_date + timedelta(minutes=30)),
            }
        )
        item_seq += 1
    total = round(total, 2)
    order = {
        "order_id": order_id,
        "customer_id": str(rng.choice(customer_ids)),
        "order_date": _stamp(order_date),
        "status": str(rng.choice(["PAID", "SHIPPED", "DELIVERED"])),
        "total_amount": f"{total:.2f}",
        "currency": "USD",
        "updated_at": _stamp(order_date + timedelta(hours=2)),
    }
    payment_date = order_date + timedelta(hours=1)
    payment = {
        "payment_id": f"PAY-{payment_seq:06d}",
        "order_id": order_id,
        "amount": f"{total:.2f}",
        "payment_method": str(rng.choice(methods)),
        "payment_date": _stamp(payment_date),
        "status": "CAPTURED",
        "updated_at": _stamp(payment_date + timedelta(hours=2)),
    }
    return order, item_rows, payment


def _build_orders(
    config: ScalableConfig,
    rng: np.random.Generator,
    customers: list[dict[str, Any]],
    products: list[dict[str, Any]],
    methods: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    customer_ids = [row["customer_id"] for row in customers]
    product_ids = [row["product_id"] for row in products]
    product_prices = {row["product_id"]: float(row["unit_price"]) for row in products}
    orders: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    payments: list[dict[str, Any]] = []
    item_seq = 1
    payment_seq = 1
    for index in range(1, config.orders + 1):
        order, item_rows, payment = _build_one_order(
            index,
            rng,
            customer_ids,
            product_ids,
            product_prices,
            methods,
            item_seq,
            payment_seq,
        )
        orders.append(order)
        items.extend(item_rows)
        payments.append(payment)
        item_seq += len(item_rows)
        payment_seq += 1
    return orders, items, payments


def _build_base_entities(
    config: ScalableConfig,
    rng: np.random.Generator,
    fake: Faker,
) -> dict[str, list[dict[str, Any]]]:
    """Generate valid base rows in the original RNG call order."""

    countries = ["US", "GB", "DE", "FR", "CA", "NL"]
    categories = ["Electronics", "Accessories", "Home", "Books", "Sports", "Luxury"]
    methods = ["CARD", "BANK_TRANSFER", "PAYPAL"]
    customers = _build_customers(config, rng, fake, countries)
    products = _build_products(config, rng, fake, categories)
    orders, items, payments = _build_orders(config, rng, customers, products, methods)
    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": items,
        "payments": payments,
    }


def _inject_customer_issues(
    entities: dict[str, list[dict[str, Any]]],
    invalid_counts: dict[str, int],
    null_counts: dict[str, int],
) -> None:
    customers = entities["customers"]
    modes = [
        lambda i: {"name": ""},
        lambda i: {"created_at": "not-a-date"},
        lambda i: {
            "updated_at": "2025-01-01T00:00:00Z",
            "created_at": "2025-06-01T00:00:00Z",
        },
        lambda i: {"email": "not-an-email"},
    ]
    for n in range(invalid_counts["customers"]):
        row = dict(customers[n % len(customers)])
        row["customer_id"] = f"BAD-CUST-{n:06d}"
        row.update(modes[n % len(modes)](n))
        customers.append(row)
    for n in range(null_counts["customers"]):
        row = dict(customers[n % len(customers)])
        row["customer_id"] = f"NULL-CUST-{n:06d}"
        row["name"] = ""
        customers.append(row)


def _inject_product_issues(
    entities: dict[str, list[dict[str, Any]]],
    invalid_counts: dict[str, int],
    null_counts: dict[str, int],
) -> None:
    products = entities["products"]
    modes = [
        lambda: {"name": ""},
        lambda: {"unit_price": "-1.00"},
        lambda: {"unit_price": "100000.01"},
        lambda: {"currency": "XYZ"},
        lambda: {"updated_at": ""},
    ]
    for n in range(invalid_counts["products"]):
        row = dict(products[n % len(products)])
        row["product_id"] = f"BAD-PROD-{n:06d}"
        row.update(modes[n % len(modes)]())
        products.append(row)
    for n in range(null_counts["products"]):
        row = dict(products[n % len(products)])
        row["product_id"] = f"NULL-PROD-{n:06d}"
        row["updated_at"] = ""
        products.append(row)


def _inject_order_issues(
    entities: dict[str, list[dict[str, Any]]],
    invalid_counts: dict[str, int],
    null_counts: dict[str, int],
) -> None:
    orders = entities["orders"]
    modes = [
        lambda row: {"total_amount": "-1.00"},
        lambda row: {"status": "NOT_A_STATUS"},
        lambda row: {"order_date": "01-01-2026"},
        lambda row: {"updated_at": "2025-01-01T00:00:00Z"},
        lambda row: {"customer_id": "CUST-999999"},
        lambda row: {"total_amount": "25.50"},
    ]
    for n in range(invalid_counts["orders"]):
        row = dict(orders[n % len(orders)])
        row["order_id"] = f"BAD-ORD-{n:06d}"
        row.update(modes[n % len(modes)](row))
        orders.append(row)
    for n in range(null_counts["orders"]):
        row = dict(orders[n % len(orders)])
        row["order_id"] = f"NULL-ORD-{n:06d}"
        row["customer_id"] = ""
        orders.append(row)


def _inject_item_issues(
    entities: dict[str, list[dict[str, Any]]],
    invalid_counts: dict[str, int],
    null_counts: dict[str, int],
) -> None:
    items = entities["order_items"]
    modes = [
        lambda row: {"quantity": "0"},
        lambda row: {"quantity": "10001"},
        lambda row: {"quantity": "two"},
        lambda row: {"quantity": ""},
        lambda row: {"product_id": "PROD-999999"},
        lambda row: {"order_id": "ORD-999999"},
    ]
    for n in range(invalid_counts["order_items"]):
        row = dict(items[n % len(items)])
        row["item_id"] = f"BAD-ITEM-{n:08d}"
        row.update(modes[n % len(modes)](row))
        items.append(row)
    for n in range(null_counts["order_items"]):
        row = dict(items[n % len(items)])
        row["item_id"] = f"NULL-ITEM-{n:08d}"
        row["product_id"] = ""
        items.append(row)


def _inject_payment_issues(
    entities: dict[str, list[dict[str, Any]]],
    invalid_counts: dict[str, int],
    null_counts: dict[str, int],
) -> None:
    payments = entities["payments"]
    valid_orders = [
        order for order in entities["orders"] if not order["order_id"].startswith(("BAD-", "NULL-"))
    ]
    modes = [
        lambda row: {"amount": "0.00"},
        lambda row: {"amount": "abc"},
        lambda row: {"payment_date": ""},
        lambda row: {"status": "UNKNOWN"},
        lambda row: {"order_id": "ORD-999999"},
        lambda row: {"amount": f"{float(row['amount']) + 100:.2f}"},
        lambda row: {"updated_at": "2025-01-01T00:00:00Z"},
    ]
    for n in range(invalid_counts["payments"]):
        source = valid_orders[n % len(valid_orders)]
        row = {
            "payment_id": f"BAD-PAY-{n:06d}",
            "order_id": source["order_id"],
            "amount": source["total_amount"],
            "payment_method": "CARD",
            "payment_date": "2026-02-01T10:00:00Z",
            "status": "CAPTURED",
            "updated_at": "2026-02-01T11:00:00Z",
        }
        row.update(modes[n % len(modes)](row))
        payments.append(row)
    for n in range(null_counts["payments"]):
        source = valid_orders[n % len(valid_orders)]
        payments.append(
            {
                "payment_id": f"NULL-PAY-{n:06d}",
                "order_id": source["order_id"],
                "amount": "",
                "payment_method": "CARD",
                "payment_date": "2026-02-01T10:00:00Z",
                "status": "CAPTURED",
                "updated_at": "2026-02-01T11:00:00Z",
            }
        )


def _inject_invalid_rows(
    entities: dict[str, list[dict[str, Any]]],
    invalid_counts: dict[str, int],
    null_counts: dict[str, int],
) -> None:
    """Append invalid and null rows with unique keys and no target effects."""

    _inject_customer_issues(entities, invalid_counts, null_counts)
    _inject_product_issues(entities, invalid_counts, null_counts)
    _inject_order_issues(entities, invalid_counts, null_counts)
    _inject_item_issues(entities, invalid_counts, null_counts)
    _inject_payment_issues(entities, invalid_counts, null_counts)


def _inject_duplicates(
    entities: dict[str, list[dict[str, Any]]],
    base_counts: dict[str, int],
    duplicate_counts: dict[str, int],
    rng: np.random.Generator,
) -> None:
    """Append exact duplicate copies of base rows."""

    for entity in ENTITIES:
        rows = entities[entity]
        for _ in range(duplicate_counts[entity]):
            rows.append(dict(rows[int(rng.integers(0, base_counts[entity]))]))


def _inject_late_versions(
    entities: dict[str, list[dict[str, Any]]],
    base_counts: dict[str, int],
    late_counts: dict[str, int],
    rng: np.random.Generator,
) -> None:
    """Append older versions that must be archived, not applied."""

    orders = entities["orders"]
    for _ in range(late_counts["orders"]):
        base = orders[int(rng.integers(0, base_counts["orders"]))]
        old = dict(base)
        old["status"] = "PENDING"
        old_dt = datetime.fromisoformat(base["order_date"].replace("Z", "+00:00")) + timedelta(
            minutes=30
        )
        old["updated_at"] = _stamp(old_dt)
        orders.append(old)

    payments = entities["payments"]
    for _ in range(late_counts["payments"]):
        base = payments[int(rng.integers(0, base_counts["payments"]))]
        old = dict(base)
        old["status"] = "REFUNDED"
        old_dt = datetime.fromisoformat(base["payment_date"].replace("Z", "+00:00")) + timedelta(
            minutes=30
        )
        old["updated_at"] = _stamp(old_dt)
        payments.append(old)


def _scalable_columns() -> dict[str, list[str]]:
    return {
        "customers": [
            "customer_id",
            "name",
            "email",
            "country",
            "created_at",
            "updated_at",
        ],
        "products": [
            "product_id",
            "name",
            "category",
            "unit_price",
            "currency",
            "updated_at",
        ],
        "orders": [
            "order_id",
            "customer_id",
            "order_date",
            "status",
            "total_amount",
            "currency",
            "updated_at",
        ],
        "order_items": [
            "item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "updated_at",
        ],
        "payments": [
            "payment_id",
            "order_id",
            "amount",
            "payment_method",
            "payment_date",
            "status",
            "updated_at",
        ],
    }


def _scalable_expected(
    config: ScalableConfig,
    source_counts: dict[str, int],
    base_counts: dict[str, int],
    invalid_counts: dict[str, int],
    null_counts: dict[str, int],
    duplicate_counts: dict[str, int],
    late_counts: dict[str, int],
) -> dict[str, Any]:
    duplicate_totals = {
        entity: duplicate_counts[entity] + late_counts.get(entity, 0) for entity in ENTITIES
    }
    rejected_totals = {entity: invalid_counts[entity] + null_counts[entity] for entity in ENTITIES}
    return {
        "dataset": "scalable",
        "seed": config.seed,
        "config": {
            "customers": config.customers,
            "products": config.products,
            "orders": config.orders,
            "invalid_pct": config.invalid_pct,
            "duplicate_pct": config.duplicate_pct,
            "null_pct": config.null_pct,
            "late_pct": config.late_pct,
        },
        "base_counts": base_counts,
        "source_rows_total": sum(source_counts.values()),
        "source_rows": source_counts,
        "injected": {
            "invalid_rows": invalid_counts,
            "null_rows": null_counts,
            "exact_duplicate_rows": duplicate_counts,
            "late_rows": late_counts,
        },
        "expected": {
            "target_counts": base_counts,
            "rejected_rows": rejected_totals,
            "duplicate_rows": duplicate_totals,
            "valid_rows": base_counts,
            "source_rows": source_counts,
            "status": "PARTIAL_SUCCESS" if any(rejected_totals.values()) else "SUCCESS",
        },
    }


def _write_scalable_output(
    output_dir: Path,
    expected_dir: str | Path | None,
    config: ScalableConfig,
    entities: dict[str, list[dict[str, Any]]],
    base_counts: dict[str, int],
    invalid_counts: dict[str, int],
    null_counts: dict[str, int],
    duplicate_counts: dict[str, int],
    late_counts: dict[str, int],
) -> dict[str, Any]:
    columns = _scalable_columns()
    for entity in ENTITIES:
        _write_csv(output_dir / f"{entity}.csv", columns[entity], entities[entity])
    source_counts = {entity: len(entities[entity]) for entity in ENTITIES}
    expected = _scalable_expected(
        config,
        source_counts,
        base_counts,
        invalid_counts,
        null_counts,
        duplicate_counts,
        late_counts,
    )
    if expected_dir is not None:
        expected_path = Path(expected_dir)
        expected_path.mkdir(parents=True, exist_ok=True)
        (expected_path / "large_expected.json").write_text(
            json.dumps(expected, indent=2), encoding="utf-8", newline="\n"
        )
    return expected


def generate_scalable(
    output_dir: str | Path,
    config: ScalableConfig | None = None,
    expected_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Generate a deterministic, deliberately imperfect large batch."""

    config = config or ScalableConfig()
    output_dir = Path(output_dir)
    rng = np.random.default_rng(config.seed)
    fake = Faker()
    Faker.seed(config.seed)
    fake.seed_instance(config.seed)

    entities = _build_base_entities(config, rng, fake)
    base_counts = {entity: len(entities[entity]) for entity in ENTITIES}
    invalid_counts = {
        entity: _count(base_counts[entity], config.invalid_pct) for entity in ENTITIES
    }
    null_counts = {entity: _count(base_counts[entity], config.null_pct) for entity in ENTITIES}
    duplicate_counts = {
        entity: _count(base_counts[entity], config.duplicate_pct) for entity in ENTITIES
    }
    late_counts = {
        "orders": _count(base_counts["orders"], config.late_pct),
        "payments": _count(base_counts["payments"], config.late_pct),
    }

    _inject_invalid_rows(entities, invalid_counts, null_counts)
    _inject_duplicates(entities, base_counts, duplicate_counts, rng)
    _inject_late_versions(entities, base_counts, late_counts, rng)

    return _write_scalable_output(
        output_dir,
        expected_dir,
        config,
        entities,
        base_counts,
        invalid_counts,
        null_counts,
        duplicate_counts,
        late_counts,
    )
