# Data Dictionary

This document defines the source contracts, target tables, keys, constraints, lineage,
and audit structures used by the e-commerce order analytics pipeline.

All source CSVs are read as UTF-8 text (`dtype=str`) so that malformed values are
visible to validation instead of being silently coerced. The types below are the
**validated business types** enforced before publishing.

---

## 1. Record lifecycle definitions

| Term | Meaning |
|---|---|
| **Source record** | One physical CSV row plus `_source_file`, `_source_row_number`, `_run_id`, `_ingested_at`. |
| **Validated record** | A source row that passed structural, type, format, range, domain, FK, and business rules. |
| **Transformed record** | A validated row after normalization, derived fields, and `_record_hash` calculation. |
| **Rejected record** | A source row that failed at least one rule. It is kept in `quarantine` with every failed rule, human-readable reason, and the raw business payload. |
| **Duplicate record** | A non-canonical copy/version removed from the current batch. Exact duplicates and superseded versions are archived in `record_history`; ambiguous same-key/same-version conflicts are quarantined. |
| **Published record** | A transformed, validated row inserted or updated in the target in the current transaction. |

A row is never silently dropped: every source row is either published/updated,
unchanged, late-arriving (archived), a duplicate (archived), or rejected
(quarantined). Reconciliation proves this identity for every run.

---

## 2. Entity relationships and grain

```text
customers 1 ──── * orders 1 ──── * order_items * ──── 1 products
                       │
                       └──── * payments
```

| Entity | Primary key | Foreign keys | Grain |
|---|---|---|---|
| `customers` | `customer_id` | — | One current row per customer; historical versions in `record_history`. |
| `products` | `product_id` | — | One current row per product; historical versions in `record_history`. |
| `orders` | `order_id` | `customer_id → customers.customer_id` | One current row per order event/version. |
| `order_items` | `item_id` | `order_id → orders.order_id`, `product_id → products.product_id` | One current row per order line/version. |
| `payments` | `payment_id` | `order_id → orders.order_id` | One current row per payment event/version. Multiple payments per order are allowed (split payments). |

Relationships are enforced in SQLite with `PRAGMA foreign_keys=ON` and target
integrity checks. Child rows are also rejected before publish if their parent is
absent from both the accepted rows in the current batch and the referenced keys
already committed by earlier runs. Only referenced keys are fetched, in chunks of
400, so a later incremental batch can reference an existing parent without
scanning full target tables. An order and all of its items must arrive in the
same batch; payments may arrive later and are checked against the committed order
total.

---

## 3. Common rules

| Rule | Value |
|---|---|
| ID format | `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` |
| Timestamp format | ISO-8601 with timezone, e.g. `2026-01-05T10:00:00Z`; transformed to UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ` |
| Currency | `USD`, `EUR`, `GBP` |
| Money rounding | Two decimals in transformed target fields |
| Text handling | Trimmed; emails lower-cased; enums/currency upper-cased |
| Null handling | Blank strings are treated as null in validation; optional fields become SQL `NULL` |
| Order statuses | `PENDING`, `PAID`, `SHIPPED`, `DELIVERED`, `CANCELLED`, `REFUNDED` |
| Payment statuses | `CAPTURED`, `FAILED`, `REFUNDED` |
| Payment methods | `CARD`, `BANK_TRANSFER`, `PAYPAL` |
| Version field | `updated_at`; newest version wins for a business key |
| Record hash | SHA-256 of the length-prefixed business payload; lineage excluded |

---

## 4. `customers`

**Source file:** `customers.csv`  
**Target table:** `customers`  
**Primary key:** `customer_id`  
**Grain:** one current row per customer.

| Column | Source type | Target type | Nullable | Constraints / notes |
|---|---|---|---|---|
| `customer_id` | text | TEXT | no | Primary key, ID pattern, max 64. |
| `name` | text | TEXT | no | Required, max 120 after trim. |
| `email` | text | TEXT | yes | If present, `name@example.com` pattern, max 254; lower-cased. |
| `country` | text | TEXT | yes | If present, exactly two letters; upper-cased. |
| `created_at` | text | TEXT | no | Valid ISO-8601 timestamp. |
| `updated_at` | text | TEXT | no | Valid ISO-8601, must be `>= created_at`; version field. |

**Defaults:** none. **Uniqueness:** `customer_id`. **Derived fields:** none.  
**Business constraint:** a customer version cannot be older than its creation timestamp.

---

## 5. `products`

**Source file:** `products.csv`  
**Target table:** `products`  
**Primary key:** `product_id`  
**Grain:** one current row per product.

| Column | Source type | Target type | Nullable | Constraints / notes |
|---|---|---|---|---|
| `product_id` | text | TEXT | no | Primary key, ID pattern, max 64. |
| `name` | text | TEXT | no | Required, max 120. |
| `category` | text | TEXT | no | Required, max 80. |
| `unit_price` | text | REAL | no | Decimal, `0 <= price <= 100000`. |
| `currency` | text | TEXT | no | `USD`/`EUR`/`GBP`. |
| `updated_at` | text | TEXT | no | Valid ISO-8601; version field. |

**Defaults:** none. **Uniqueness:** `product_id`. **Derived fields:** none.

---

## 6. `orders`

**Source file:** `orders.csv`  
**Target table:** `orders`  
**Primary key:** `order_id`  
**Grain:** one current row per order event/version.

| Column | Source type | Target type | Nullable | Constraints / notes |
|---|---|---|---|---|
| `order_id` | text | TEXT | no | Primary key, ID pattern, max 64. |
| `customer_id` | text | TEXT | no | FK to `customers.customer_id`; parent must be accepted in the run. |
| `order_date` | text | TEXT | no | Valid ISO-8601 business event timestamp. |
| `status` | text | TEXT | no | Allowed order status enum. |
| `total_amount` | text | REAL | no | Decimal, `0 <= total <= 1,000,000,000`. |
| `currency` | text | TEXT | no | `USD`/`EUR`/`GBP`. |
| `updated_at` | text | TEXT | no | Valid ISO-8601, must be `>= order_date`; version field. |
| `order_year_month` | derived | TEXT | no | `YYYY-MM` from normalized `order_date`; used for monthly analytics. |

**Defaults:** none. **Uniqueness:** `order_id`. **Derived field:** `order_year_month`.  
**Business rules:** each accepted order must have at least one accepted item; order
total must equal the sum of accepted line totals within `0.01`.

---

## 7. `order_items`

**Source file:** `order_items.csv`  
**Target table:** `order_items`  
**Primary key:** `item_id`  
**Grain:** one current row per order line/version.

| Column | Source type | Target type | Nullable | Constraints / notes |
|---|---|---|---|---|
| `item_id` | text | TEXT | no | Primary key, ID pattern, max 64. |
| `order_id` | text | TEXT | no | FK to `orders.order_id`; parent must be accepted. |
| `product_id` | text | TEXT | no | FK to `products.product_id`; parent must be accepted. |
| `quantity` | text | INTEGER | no | Integer, `1 <= quantity <= 10000`; no fractional quantities. |
| `unit_price` | text | REAL | no | Decimal, `0 <= price <= 100000`. |
| `updated_at` | text | TEXT | no | Valid ISO-8601; version field. |
| `line_total` | derived | REAL | no | `round(quantity * unit_price, 2)`. |

**Defaults:** none. **Uniqueness:** `item_id`. **Derived field:** `line_total`.

---

## 8. `payments`

**Source file:** `payments.csv`  
**Target table:** `payments`  
**Primary key:** `payment_id`  
**Grain:** one current row per payment event/version.

| Column | Source type | Target type | Nullable | Constraints / notes |
|---|---|---|---|---|
| `payment_id` | text | TEXT | no | Primary key, ID pattern, max 64. |
| `order_id` | text | TEXT | no | FK to `orders.order_id`; parent must be accepted. |
| `amount` | text | REAL | no | Decimal, `0 < amount <= 1,000,000,000`. |
| `payment_method` | text | TEXT | no | `CARD`, `BANK_TRANSFER`, `PAYPAL`. |
| `payment_date` | text | TEXT | no | Valid ISO-8601 business event timestamp. |
| `status` | text | TEXT | no | `CAPTURED`, `FAILED`, `REFUNDED`. |
| `updated_at` | text | TEXT | no | Valid ISO-8601, must be `>= payment_date`; version field. |

**Defaults:** none. **Uniqueness:** `payment_id`. **Derived fields:** none.  
**Business rules:** captured payment total per order must not exceed the order total;
a single captured payment may differ from the order total by at most 1% (split
payments are evaluated at target level). For a late payment whose order is absent
from the batch, the committed order total is used; a batch total overrides a
committed total when both exist. Underpayment is informational, not a hard
failure.

---

## 9. Target lineage and derived columns

Every target entity has these additional columns:

| Column | Type | Description |
|---|---|---|
| `_source_file` | TEXT | Source CSV basename, for traceability. |
| `_source_row_number` | INTEGER | 1-based physical row number in the source CSV. |
| `_run_id` | TEXT | Run that produced the currently published version. |
| `_ingested_at` | TEXT | UTC timestamp when the source was read. |
| `_pipeline_version` | TEXT | Pipeline version (`1.0.0`). |
| `_record_hash` | TEXT | SHA-256 of the business payload; used for exact-copy and version comparisons. |

Derived fields are `orders.order_year_month`, `order_items.line_total`, and the
record hash (all entities). There are no database defaults for business columns.

---

## 10. Audit and operational tables

### `quarantine`

One row per deterministic rejected-source occurrence (unique
`quarantine_id`). Re-seeing the same rejection upserts and increments
`occurrence_count`; it is not duplicated.

| Column | Type | Description |
|---|---|---|
| `quarantine_id` | TEXT PK | SHA-256 of entity, source file/row, sorted rules, and raw payload. |
| `entity` | TEXT | Source entity name. |
| `source_record_id` | TEXT | Business primary key when available, else `row:<n>`. |
| `source_file` | TEXT | Source file. |
| `source_row_number` | INTEGER | Physical source row. |
| `first_run_id` | TEXT | First run that rejected this occurrence. |
| `last_run_id` | TEXT | Most recent run that rejected it. |
| `rejection_rules` | TEXT (JSON) | All failed rule codes. |
| `rejection_reasons` | TEXT (JSON) | Human-readable explanation per rule. |
| `raw_record` | TEXT (JSON) | Original business payload (lineage excluded). |
| `first_rejected_at` | TEXT | First rejection timestamp. |
| `last_rejected_at` | TEXT | Last rejection timestamp. |
| `occurrence_count` | INTEGER | Number of times this exact rejection was observed. |

### `record_history`

One row per archived duplicate or replaced version, keyed deterministically by
entity, record, version, hash, and reason.

| Column | Type | Description |
|---|---|---|
| `history_id` | TEXT PK | Deterministic SHA-256 archive key. |
| `entity` | TEXT | Entity name. |
| `record_id` | TEXT | Business key. |
| `version_ts` | TEXT | Version timestamp of the archived row. |
| `record_hash` | TEXT | Archived payload hash. |
| `reason` | TEXT | `EXACT_DUPLICATE`, `SUPERSEDED_VERSION`, `LATE_ARRIVING_OLDER_VERSION`, `REPLACED_BY_NEWER_VERSION`. |
| `first_run_id` / `last_seen_run_id` | TEXT | Run traceability. |
| `source_file` / `source_row_number` | TEXT / INTEGER | Original lineage. |
| `payload` | TEXT (JSON) | Archived business payload. |
| `first_archived_at` / `last_archived_at` | TEXT | Archive timestamps. |
| `occurrence_count` | INTEGER | Re-observation count (idempotent upsert). |

### `pipeline_runs`

| Column | Type | Description |
|---|---|---|
| `run_id` | TEXT PK | Explicit pipeline run identifier. |
| `pipeline_version` | TEXT | Code version. |
| `source_file` | TEXT | Source directory/file descriptor. |
| `start_timestamp`, `end_timestamp` | TEXT | Run window. |
| `duration_seconds` | REAL | Wall-clock duration. |
| `input_rows`, `valid_rows`, `rejected_rows`, `duplicate_rows`, `published_rows`, `unchanged_rows`, `late_arriving_rows` | INTEGER | Run counters. |
| `error_count`, `retry_attempts` | INTEGER | Failure/retry counters. |
| `status` | TEXT | `SUCCESS`, `PARTIAL_SUCCESS`, or `FAILED`. |
| `failure_reason` | TEXT | Error summary when failed. |
| `metrics_json` | TEXT (JSON) | Full machine-readable run report. |

### `pipeline_runs` status semantics

- `SUCCESS`: no rejected rows and no errors.
- `PARTIAL_SUCCESS`: valid data was published, but at least one row was rejected
  or quarantined. This is expected for the curated sample (37 rejects).
- `FAILED`: the run aborted; the publish transaction rolled back. Validation
  evidence may still be written, but no target rows from that attempt remain.

---

## 11. Source-to-target type mapping

| Business type | Source representation | Validation | Target type |
|---|---|---|---|
| string | CSV text | trim/pattern/length/domain | TEXT |
| integer | CSV text | integer parse, range | INTEGER |
| float/money | CSV text | numeric parse, range, rounding | REAL |
| timestamp | CSV text | ISO-8601 parse, timezone, temporal order | TEXT (UTC ISO-8601) |

This intentionally keeps ingestion permissive and validation strict: the pipeline can
explain exactly why a row is bad rather than failing the whole file on a single cell.
