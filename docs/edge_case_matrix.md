# Edge-Case Coverage Matrix

The challenge requires 18 explicitly named edge cases plus at least two
domain-specific cases. This document maps the **original 1-18 list** to
executable tests; the repository also has a separate `EC-01..EC-18` naming
scheme for data-quality rules, which is intentionally different. Do not confuse
the two numbers: the table below uses the challenge's original list.

Execution evidence:

```text
.\.venv\Scripts\python.exe -m pytest -q
155 passed in 39.00s
```

The captured output is tracked at `docs/evidence/pytest_results.txt`. Every row
below is a passing test in the final suite. "Actual" describes the
assertion-verified result, not a hypothetical.

## 1. Required 18 edge cases

| # | Scenario | Executable test | Input | Expected behaviour | Actual result | Status |
|---:|---|---|---|---|---|---|
| 1 | Happy path | `tests/integration/test_edge_case_pipeline.py::test_happy_path_publishes_every_entity` | One valid row for each of the five entities | `SUCCESS`; five accepted/published rows; reconciliation OK | `SUCCESS`; input=valid=published=5; each target count=1; reconciliation `ok=True` | PASS |
| 2 | Empty batch | `tests/integration/test_edge_case_pipeline.py::test_empty_batch_is_success_and_publishes_nothing` | Header-only CSVs for all entities | `SUCCESS`; nothing published; no rejection | input=0, published=0, all target counts=0, reconciliation `ok=True` | PASS |
| 3 | Malformed input file | `tests/unit/test_ingestion.py::test_malformed_csv_is_rejected`, `::test_non_utf8_file_is_rejected`, `::test_duplicate_header_is_rejected` | Unterminated quoted CSV; invalid UTF-8 bytes; duplicate header | `IngestionError`; fail before publish | Each malformed case raises `IngestionError` with the specific reason; no target write | PASS |
| 4 | Missing required column | `tests/data_quality/test_edge_cases.py::test_ec16_schema_drift` | Customers frame with `updated_at` removed | Structural validation fails | `SchemaValidationError` matching `missing required columns` | PASS |
| 5 | Unexpected schema change | `tests/integration/test_failure_recovery.py::test_schema_drift_fails_without_publishing`; `tests/data_quality/test_edge_cases.py::test_ec16_schema_drift` | Extra `loyalty_tier` column | Fail loudly; zero published rows | Run `FAILED`; error contains `unexpected columns`; all entity counts=0; one run metric row | PASS |
| 6 | Null required fields | `tests/data_quality/test_edge_cases.py::test_ec01_missing_required_value`; `tests/unit/test_validation.py::test_required_blank_and_optional_blank` | `customers.name=""`; optional email blank | Reject required blank; allow optional null | 1 rejected with `REQUIRED_NAME`; optional email accepted as `None` | PASS |
| 7 | Invalid data types | `tests/data_quality/test_edge_cases.py::test_ec04_wrong_data_type` | Product `unit_price="abc"` and `"NaN"` | Reject non-numeric numeric fields | 2 rejected, rule `TYPE_UNIT_PRICE` | PASS |
| 8 | Duplicate primary keys | `tests/data_quality/test_edge_cases.py::test_ec10_superseded_version`; `tests/unit/test_deduplication.py::test_newer_version_wins_and_older_is_superseded` | Same customer key twice with different `updated_at` | One current row; older version archived | 1 canonical row; newest email retained; `DEDUP_SUPERSEDED=1` | PASS |
| 9 | Duplicate events | `tests/data_quality/test_edge_cases.py::test_ec08_exact_duplicate`; `tests/unit/test_deduplication.py::test_exact_duplicate_keeps_one_copy` | Same customer event exactly twice | One copy retained; one duplicate removed | 1 canonical row; 1 duplicate; `DEDUP_EXACT=1`; duplicate audit rule recorded | PASS |
| 10 | Invalid reference keys | `tests/data_quality/test_edge_cases.py::test_ec12_foreign_key_violation`; `tests/unit/test_validation.py::test_foreign_key_check_quarantines_orphans` | Order references `CUST-404`; item references `PROD-404` | Reject orphan rows before publish | Order rejected `FK_CUSTOMER_ID`; item rejected `FK_PRODUCT_ID`; no valid rows | PASS |
| 11 | Out-of-range values | `tests/data_quality/test_edge_cases.py::test_ec05_out_of_range_values` | Price `-0.01`/`100000.01`; quantity `0`/`10001` | Reject values outside configured ranges | Prices rejected `RANGE_UNIT_PRICE`; quantities rejected `RANGE_QUANTITY` | PASS |
| 12 | Boundary values | `tests/data_quality/test_edge_cases.py::test_ec05_out_of_range_values`; `tests/data_quality/test_domain_rules.py::test_domain_payment_tolerance_boundary` | Quantity exactly 10,000; price 0/100,000; payment 10.10 vs 10.11 on a 10.00 order | Boundary values accepted; just-over boundaries rejected | Quantity 10,000 accepted; 10.10 accepted; 10.11 rejected (1% tolerance) | PASS |
| 13 | Late-arriving records | `tests/data_quality/test_edge_cases.py::test_ec11_late_arriving_record`; `tests/unit/test_publishing.py::test_older_version_is_late_arriving_and_does_not_overwrite` | Publish newer version, then send older `updated_at` | Older version archived; current value unchanged | `late_arriving=1`; history count=1; newer target retained | PASS |
| 14 | Out-of-order records | `tests/unit/test_deduplication.py::test_older_version_arriving_after_newer_is_superseded`; `::test_newer_version_wins_and_older_is_superseded` | Newer row appears before older row in the same batch | Newest `updated_at` wins regardless of row order | Newer payload canonical; exactly 1 `DEDUP_SUPERSEDED`; same result in both input orders | PASS |
| 15 | Partial pipeline failure | `tests/integration/test_failure_recovery.py::test_target_failure_rolls_back_and_a_later_run_recovers`; `::test_missing_source_file_fails_without_publishing` | Force target-check failure after publish work; or remove `orders.csv` | Whole run rolls back; failure report; no partial entity set | `FAILED`; all five target counts=0; quarantine=0; history=0; one failed run row; later run recovers fully | PASS |
| 16 | Retry after failure | `tests/integration/test_failure_recovery.py::test_transient_publish_failure_is_retried_once`; `tests/unit/test_publishing.py::test_retry_call_retries_only_transient_errors` | One-time `database is locked` on insert | Retry transient error; publish exactly once | `retry_attempts=1`, publish attempts=2, business counts = 7/6/8/7; no duplicated rows | PASS |
| 17 | Identical input rerun | `tests/integration/test_pipeline_small.py::test_small_pipeline_rerun_is_idempotent`; demo `demo-small-002`; `tests/regression/test_regression.py::test_target_snapshot_is_unchanged_by_rerun` | Same 79-row source batch, second run ID | No new business rows; target unchanged; audit upserted | 0 published, 32 unchanged, 37 quarantine, 10 history, occurrence_range=(2,2), target snapshot identical | PASS |
| 18 | Corrected previously rejected records | `tests/integration/test_edge_case_pipeline.py::test_corrected_previously_rejected_record_is_published_on_rerun` | First run has blank customer name; second run corrects it | Corrected row publishes; prior quarantine evidence retained | First: 4 rejects, one product only; second: 4 published + 1 unchanged; all five target counts=1; quarantine remains 4 | PASS |

## 2. Domain-specific edge cases

All tests below are `PASS`; they are the extra e-commerce rules beyond the
generic 18.

| ID | Scenario | Executable test | Input | Expected behaviour | Actual result | Status |
|---|---|---|---|---|---|---|
| DOM-1 | Order total does not match accepted line items | `tests/data_quality/test_edge_cases.py::test_ec14_order_total_mismatch` | Order total 99.99; one accepted line of 10.00 | Reject order | Rejected with `BUSINESS_ORDER_TOTAL_MISMATCH` | PASS |
| DOM-2 | Payment exceeds/mismatches order | `tests/data_quality/test_edge_cases.py::test_ec15_payment_amount_mismatch` | 25.00 captured payment for 10.00 order | Reject payment | Rejected with `BUSINESS_PAYMENT_MISMATCH` | PASS |
| DOM-3 | Order has no accepted items | `tests/data_quality/test_edge_cases.py::test_ec13_order_without_items` | Valid order header, empty item set | Reject order | Rejected with `BUSINESS_ORDER_WITHOUT_ITEMS` | PASS |
| DOM-4 | Orphan order/customer | `tests/data_quality/test_edge_cases.py::test_ec12_foreign_key_violation` | Order references nonexistent customer | Quarantine orphan | `FK_CUSTOMER_ID` | PASS |
| DOM-5 | Zero/negative quantity | `tests/data_quality/test_edge_cases.py::test_ec05_out_of_range_values` | Quantity 0 and 10,001; 10,000 boundary | Reject zero/negative/over-max; accept exact max | 0 and 10,001 rejected `RANGE_QUANTITY`; 10,000 accepted | PASS |
| DOM-6 | Zero-total free order is legitimate | `tests/data_quality/test_domain_rules.py::test_domain_zero_total_free_order_is_legitimate` | Order and line both 0.00; no payment | Publish; do not fail "underpaid" as a hard rule | Target checks pass; aggregate sum 0.0; underpaid check is informational (`hard=False`) | PASS |
| DOM-7 | Split captured payments overpay | `tests/data_quality/test_domain_rules.py::test_domain_split_captured_payments_cannot_overpay` | Two captured payments individually below order total but summing above it | Aggregate target check fails | `payments.no_overpayment` value=1; overall target checks `ok=False` | PASS |
| DOM-8 | Currency/payment-method domain | `tests/data_quality/test_domain_rules.py::test_domain_currency_and_payment_method_domains` | Currency `JPY`; method `CRYPTO` | Reject unsupported domain values | One rejected order, one rejected payment | PASS |
| DOM-9 | Payment tolerance boundary | `tests/data_quality/test_domain_rules.py::test_domain_payment_tolerance_boundary` | 10.10 and 10.11 against 10.00 order | 1% tolerance accepts 10.10, rejects 10.11 | Exactly that: PAY-001 valid, PAY-002 rejected | PASS |
| DOM-10 | Cross-batch parent references | `tests/integration/test_edge_case_pipeline.py::test_second_batch_can_reference_parents_already_in_target`; `tests/unit/test_publishing.py::test_fetch_existing_keys_chunks_large_reference_sets` | Batch 2 adds an order for a batch-1 customer and an item for a batch-1 product; a lookup contains 401 referenced keys | Accept against committed target parents; chunk at 400 keys; target FK checks stay clean | Second run `SUCCESS`; 5/5 published; 0 rejected; all target counts=2; 401-key lookup returns only requested committed keys | PASS |
| DOM-11 | Same key, same version, different payload | `tests/unit/test_deduplication.py::test_same_key_and_version_with_different_payload_conflicts`; `tests/integration/test_edge_case_pipeline.py::test_same_key_same_version_different_payload_is_quarantined_not_failed` | Two customer rows with the same key/version but different names | Quarantine both, do not crash, keep reconciliation OK | Run `PARTIAL_SUCCESS`; 2 quarantined rows with `DEDUP_VERSION_CONFLICT`; 0 published; reconciliation `ok=True` | PASS |
| DOM-12 | Late payment against committed order total | `tests/integration/test_edge_case_pipeline.py::test_late_payment_is_validated_against_published_order_totals`; `tests/unit/test_validation.py::test_payment_amounts_use_published_totals_for_late_payments`; `::test_batch_order_totals_take_precedence_over_published_totals` | First batch publishes `ORD-1001` total 140.99; second batch contains only a payment | Use committed total for the missing order; reject wrong 1.00; accept exact and within 1%; reject just outside tolerance; batch total wins when present | `1.00` and `139.58` -> `PARTIAL_SUCCESS` with 1 quarantine and 0 payments; `140.99` and `139.59` -> `SUCCESS` with 1 payment | PASS |
| DOM-13 | Reconciliation failure rolls back publish | `tests/integration/test_failure_recovery.py::test_reconciliation_failure_rolls_back_and_a_later_run_recovers`; `tests/unit/test_publishing.py::test_verify_failure_rolls_back_everything_and_retry_succeeds` | Force the post-target `verify` callback to raise | Roll back all business/audit rows; mark run `FAILED`; retry succeeds | After failure all five target tables are empty and `pipeline_runs.status='FAILED'`; retry publishes 5 rows and reconciles | PASS |
| DOM-14 | Cancellation is recorded and re-raised | `tests/integration/test_failure_recovery.py::test_cancellation_marks_run_failed_and_is_re_raised` | Patch the transform stage to raise `KeyboardInterrupt` or `SystemExit` | Record `FAILED` without swallowing the cancellation | Both exceptions propagate; target tables empty; one `FAILED` run row | PASS |

## 3. How to reproduce the edge-case evidence

```powershell
# Just the 18 required data-quality tests plus domain rules
.\.venv\Scripts\python.exe -m pytest tests/data_quality -v

# The end-to-end + failure/recovery evidence
.\.venv\Scripts\python.exe -m pytest tests/integration -v

# Entire final suite
.\.venv\Scripts\python.exe -m pytest -q
```

The named `test_ec01..test_ec18` tests are the data-quality rule suite (a
different numbering from the challenge list). The mapping above is the
authoritative crosswalk for the challenge's 18 required scenarios.
