# Testing Strategy and Execution Evidence

## 1. Philosophy

Tests verify **behaviour and output**, not merely that a function can execute.
The suite is organised around the lifecycle:

- unit tests isolate a rule, transformation, dedup policy, retry classifier, or
  publish operation;
- integration tests exercise ingestion through target publish and recovery;
- data-quality tests assert contract violations, quarantine rules, and domain
  constraints;
- regression tests lock deterministic generation, expected labels, aggregates,
  target snapshots, and evidence sanitization/LF.

Expected outputs live in `data/expected/small_expected.json`, separate from
`data/raw/`. Pipeline code never reads the expected file.

## 2. Suite composition

| Suite | Tests | Focus |
|---|---:|---|
| Unit | 70 | CSV parsing/header checks, validation and boundary tables, dedup equivalence/conflicts, transformations, publishing/retry/verify, chunked lookups, reconciliation, reporting/monitoring |
| Integration | 20 | Full small pipeline, happy/empty/correction paths, cross-batch parents, late payments, reconciliation rollback, cancellation, scalable accounting, missing source, schema drift, retry |
| Data quality | 60 | 18 named EC rule tests, parameterised boundary matrices, domain rules, foreign keys, totals, payments, free orders |
| Regression | 5 | Expected-file drift, byte-deterministic small/large generation, rerun target snapshot, evidence sanitization/LF |
| **Total** | **155** | |

Fixtures create isolated temporary directories and SQLite databases. The small
end-to-end fixture uses the deterministic 79-row generator. In-memory databases
use `StaticPool` so foreign-key pragmas and committed state persist for the test.

## 3. Requirement-to-test map

| Requirement area | Primary tests |
|---|---|
| Source ingestion and lineage | `tests/unit/test_ingestion.py` |
| Structural schema, required/unexpected columns | `tests/unit/test_validation.py::test_source_structure_schema_drift_and_missing_columns`, `tests/data_quality/test_edge_cases.py::test_ec16_schema_drift` |
| Null/required/type/format/range/domain | `tests/unit/test_validation.py`, `tests/data_quality/test_edge_cases.py::test_ec01..test_ec07` |
| Duplicate exact/version/conflict policy | `tests/unit/test_deduplication.py`, `tests/unit/test_pipeline_helpers.py`, `tests/data_quality/test_edge_cases.py::test_ec08..test_ec10` |
| Foreign-key, cross-batch parents, late payments | `tests/unit/test_validation.py`, `tests/unit/test_publishing.py`, `tests/data_quality/test_edge_cases.py::test_ec12..test_ec15`, `tests/data_quality/test_domain_rules.py` |
| Transformation and target contract | `tests/unit/test_transformations.py` |
| Atomic publish, verification, rollback, retry | `tests/unit/test_publishing.py`, `tests/integration/test_failure_recovery.py` |
| End-to-end contract and artifacts | `tests/integration/test_pipeline_small.py`, `tests/integration/test_edge_case_pipeline.py` |
| Scalable accounting | `tests/integration/test_pipeline_scalable.py` |
| Reconciliation | `tests/unit/test_reconciliation.py`, all end-to-end tests |
| Monitoring/reporting | `tests/unit/test_reporting_monitoring.py` |
| Determinism and regression | `tests/regression/test_regression.py` |
| 18 required edge cases | `docs/edge_case_matrix.md` |
| Two+ domain edge cases | `tests/data_quality/test_domain_rules.py` and EC-13/14/15 |

## 4. Unit-test highlights

- **Ingestion**: lineage columns, missing file, zero-byte file, duplicate header,
  blank header, header-only empty batch, malformed quoted CSV, non-UTF-8 bytes,
  UTF-8 BOM.
- **Validation**: blank required vs optional null, integer/float coercion,
  one error per field, multiple errors per row, timestamp format/timezone,
  temporal order, enums, patterns, max lengths, foreign keys, order totals,
  missing items, payment tolerance, committed totals for late payments, batch
  totals taking precedence.
- **Boundary tables**: parameterised quantity, unit-price, payment-amount,
  payment-tolerance, timestamp-format, enum, required-null, and empty-batch cases
  with explicit pytest IDs.
- **Deduplication**: exact duplicate, newer/older version in either input order,
  same-key/same-version conflict, non-key similarity is not deduplicated, empty
  frame, hash stability and ambiguity resistance, vectorized vs naive equivalence
  for all five entities.
- **Publishing**: insert then idempotent rerun, newer version updates and
  archives, older version is late-arriving, same-version conflict quarantined,
  target failure rolls back, verify/reconciliation failure rolls back, transient
  retry, permanent no-retry, quarantine upsert, history upsert, 401-key chunked
  parent lookups.
- **Reconciliation**: empty target, valid target aggregates, overpayment,
  source-accounting imbalance, target-failure status, audit table counts.
- **Reporting/monitoring**: run status, failure report, quarantine/duplicate CSV,
  processed snapshots, DQ report.
- **Transformations**: text normalization, derived year-month and line total,
  target metadata/hash, Pandera accept/reject cases.

## 5. Integration-test highlights

1. Valid five-entity batch publishes all entities and reconciles.
2. Empty headers are a successful no-op.
3. A rejected record corrected in a later run publishes, while old quarantine
   evidence remains.
4. Small 79-row pipeline matches every expected count, canonical ID, rejection
   rule, aggregate, audit count, and target check.
5. Small run writes every artifact.
6. Identical rerun publishes zero, reports 32 unchanged, and keeps audit counts
   at 37 quarantine / 10 history.
7. Missing source file fails without publishing anything.
8. Unexpected schema column fails without publishing.
9. Transient lock retry publishes exactly once.
10. Forced target failure rolls back everything; a later run recovers.
11. Scalable batch accounts for every source row.
12. A second batch can publish orders/items that reference customers and
    products already committed by the first batch.
13. A payment-only batch is checked against the committed order total: exact and
    within-tolerance payments publish; wrong and just-outside payments quarantine
    without mutating existing payments.
14. A forced reconciliation failure rolls back all five tables and marks the run
    `FAILED`; the retry succeeds. `KeyboardInterrupt` and `SystemExit` are
    recorded as `FAILED` and re-raised.

## 6. Data-quality and domain tests

The `test_ec01..test_ec18` tests cover required field, optional null, malformed
timestamp, wrong type, ranges, enum, pattern/length, exact duplicate, version
conflict, superseded version, late arrival, FK, order without items, total
mismatch, payment mismatch, schema drift, normalization, and temporal
consistency.

Additional domain tests cover:

- zero-total free order remains legitimate;
- split captured payments cannot overpay in aggregate;
- payment tolerance boundary (10.10 accepted vs 10.11 rejected on a 10.00 order);
- currency and payment-method domains.

See `docs/edge_case_matrix.md` for the challenge's 18 required scenarios mapped
to test nodes and actual outcomes.

## 7. Regression tests

- committed `small_expected.json` equals the deterministic generator output;
- two independent small generations are byte-identical;
- two independent scalable generations are byte-identical;
- a target-row snapshot hash is unchanged by an identical rerun.

These tests protect against silent schema/transformation changes and accidental
duplication across runs.

## 8. Final execution results

Plain suite:

```text
$ .\.venv\Scripts\python.exe -m pytest -q
155 passed in 39.00s
```

Coverage run:

```text
$ .\.venv\Scripts\python.exe -m pytest -q --cov=src --cov-report=term \
    --cov-report=json:reports/coverage.json
155 passed, 73 warnings in 63.65s
TOTAL 2162 statements, 94 missed, 96% coverage
```

The 73 warnings were Python `ResourceWarning: unclosed database` messages from
test-owned SQLAlchemy engines; the coverage-enabled run was slower and the
garbage collector surfaced connections that a plain run did not report. There
were **no test failures, errors, or skips**. A future cleanup can dispose every
test engine explicitly; it does not affect pipeline correctness.

`reports/pytest_results.txt` contains the captured plain run;
`reports/pytest_coverage_results.txt` contains the coverage run; and
`reports/coverage.json` is the machine-readable coverage artifact. Curated copies
of the test and coverage output are tracked in `docs/evidence/`.

## 9. Coverage by module

| Module | Statements | Miss | Coverage |
|---|---:|---:|---:|
| `src/config.py` | 59 | 5 | 92% |
| `src/deduplication.py` | 174 | 13 | 93% |
| `src/generator.py` | 257 | 0 | 100% |
| `src/ingestion.py` | 113 | 14 | 88% |
| `src/monitoring.py` | 163 | 17 | 90% |
| `src/pipeline.py` | 261 | 10 | 96% |
| `src/publishing.py` | 325 | 12 | 96% |
| `src/reconciliation.py` | 153 | 4 | 97% |
| `src/reporting.py` | 141 | 2 | 99% |
| `src/schemas.py` | 98 | 3 | 97% |
| `src/transformations.py` | 89 | 4 | 96% |
| `src/validation.py` | 329 | 10 | 97% |
| **Total** | **2,162** | **94** | **96%** |

The remaining gaps are mostly defensive/unreachable branches, reporting edge
paths, and config overrides rather than untested core behaviour.

## 9.1 Fresh-checkout verification

A throwaway Git repository containing only the source tree, the tracked expected
contract, and `docs/evidence/` was committed and cloned into an empty directory.
The clone contained no `.venv`, `data/raw/`, `data/processed/`, or `reports/`.
Following the README setup from that clone (create `.venv`, install
`requirements.txt`, run tests) produced:

```text
155 passed in 40.53s
```

The README quickstart was then executed twice from the same clone. The first run
reported 79 input, 32 published, 37 rejected, 10 duplicate and 32 target rows;
the rerun reported 0 published and 32 unchanged. Both verification runs passed
45/45 checks, and `final_db_checks.py` returned `unique_pk=true`,
`no_fk_violations=true`, `aggregates_match=true`, and
`audit_not_appended_on_rerun=true`.

## 10. Commands

```powershell
# Fast verification
.\.venv\Scripts\python.exe -m pytest -q

# Per suite
.\.venv\Scripts\python.exe -m pytest tests/unit -q
.\.venv\Scripts\python.exe -m pytest tests/integration -q
.\.venv\Scripts\python.exe -m pytest tests/data_quality -q
.\.venv\Scripts\python.exe -m pytest tests/regression -q

# Explicit required edge cases
.\.venv\Scripts\python.exe -m pytest tests/data_quality/test_edge_cases.py -v

# Coverage
.\.venv\Scripts\python.exe -m pytest -q --cov=src --cov-report=term `
  --cov-report=json:reports/coverage.json
```

## 11. What is intentionally not tested

- Real network/cloud failure modes (no network is used).
- Multi-process SQLite writer contention (local single-writer design).
- Browser/UI tests (no UI).
- Property-based generation at arbitrary scale (benchmark and two fixed sizes are
  measured instead).
- Clock-based freshness alarms (no scheduler in this slice).

These are documented limitations, not silent omissions.
