# Design and Trade-off Note

## 1. Scope and engineering posture

This is a **vertical slice**, not a cluster deployment. The target is one local
Python process, five CSV entities, a SQLite warehouse, and reproducible evidence.
The design optimizes for correctness, inspectability, and fast feedback across
the full lifecycle. Production concerns that do not change the core semantics
(a scheduler, distributed storage, centralized observability) are deliberately
left as documented extension points rather than simulated with unnecessary
infrastructure.

The two non-negotiable invariants are:

1. **No silent loss.** Every source row ends in exactly one accounted state:
   published/updated/unchanged, late-arriving, duplicate/archived, or
   quarantined.
2. **No partial batch.** All entities, audit writes, and target checks commit in
   one transaction or none of them do.

## 2. Why this stack

| Decision | Rationale | Trade-off |
|---|---|---|
| Python 3.11+ | Readable, standard in data engineering, strong test tooling. | Runtime performance is lower than compiled languages; not material at this scale. |
| pandas | Excellent for typed, column-oriented batch validation and vectorized transformation. | Holds the batch in memory and has per-call overhead on tiny batches. |
| Pandera | Adds a second, declarative target contract after hand-written source rules. | Two validation layers must be kept coherent; tests check both. |
| SQLAlchemy + SQLite | Real transactions, foreign keys, indexes, and SQL checks with zero services. | Not a production multi-writer warehouse. |
| pytest | Fixtures, parametrization, markers, coverage, and clear assertion output. | No orchestration or data-observability platform included. |
| Faker + NumPy | Realistic, seeded synthetic values with repeatable generation. | Synthetic distributions are plausible, not a model of a real merchant. |
| Matplotlib | Optional DQ chart with no dashboard service. | A single PNG is evidence, not interactive BI. |

The stack is intentionally boring. The challenge rewards a complete, testable
slice more than a distributed system that cannot be verified locally.

## 3. Why this data model

The model follows the natural order lifecycle and keeps a stable business key
for every entity:

```text
customers 1 --- * orders 1 --- * order_items * --- 1 products
                         |
                         * payments
```

- **Order header vs order lines.** Keeping `orders` and `order_items` separate
  allows an order-total rule to be checked against the sum of accepted lines,
  which is one of the most valuable domain checks.
- **Multiple payments per order.** Split payments are legitimate. A per-payment
  amount check is not enough; the target also checks aggregate captured payments
  against the order total.
- **Business primary keys, not surrogate row IDs.** `order_id`, `item_id`, and
  `payment_id` are the stable identities received from source systems. The
  version column (`updated_at`) decides which payload is current.
- **Lineage columns on every target row.** `_source_file`,
  `_source_row_number`, `_run_id`, `_ingested_at`, `_pipeline_version`, and
  `_record_hash` make accepted and rejected data traceable to the exact source
  row and code version.
- **Audit tables are first-class.** `quarantine` and `record_history` are not
  side files only; they are queryable target tables with deterministic IDs and
  occurrence counts.
- **Referential integrity spans batches.** A later batch may reference a
  customer, product, or order already committed by an earlier batch. The
  pipeline unions the current canonical parent keys with a read-only snapshot of
  committed target keys before applying child-row FK checks, while target
  `PRAGMA foreign_key_check` still validates the final database.

`docs/data_dictionary.md` contains the full field-level contract, including
nullability and derived fields.

## 4. Duplicate handling

### Policy

The business key is the entity primary key. The version is `updated_at`.

| Situation | Action | Evidence |
|---|---|---|
| Identical payload, including version | Keep one copy; remove the rest | `DEDUP_EXACT`, duplicate CSV/history |
| Same key, different versions | Greatest `updated_at` is canonical; older versions archived | `DEDUP_SUPERSEDED`, history |
| Same key and same version, different payload | Quarantine all rows for that key | `DEDUP_VERSION_CONFLICT` |
| Same non-key attributes but different keys | Keep all rows | No dedup action |

The last row is important: two customers with the same name and country are not
duplicates. Deduplication is never based on a fuzzy match or on non-key
attributes.

### Determinism

- Record hashes are SHA-256 over length-prefixed business values, excluding
  lineage and rejection metadata. This avoids ambiguous concatenations such as
  `("ab", "c")` vs `("a", "bc")`.
- Sorting is stable and uses `(primary_key, updated_at, source_row_number)` so
  repeated runs and input permutations produce a deterministic winner.
- A same-key/same-version payload conflict is not resolved by "first row wins".
  Silently choosing an arbitrary payload would be a data-quality failure; the
  key is quarantined for investigation.

### Why latest `updated_at` instead of last-seen?

Source order is not a reliable truth signal: an older record can arrive after a
newer one. Using the version timestamp makes dedup independent of file order.
`_source_row_number` is only a deterministic tie-breaker for otherwise identical
versions, never a replacement for the business version.

### Late-arriving records

A version older than the current target row is archived in `record_history` with
reason `LATE_ARRIVING`; it does not overwrite current state. A version newer than
the target updates the current row and archives the replaced payload. This
preserves history without allowing stale extracts to roll the warehouse back.

## 5. Quarantine design

Invalid records are never dropped. Each rejected row receives:

- source entity and source record identifier (business key where available);
- source filename, source row number, and run ID;
- one or more failed rule codes;
- one or more human-readable reasons;
- raw business payload and rejection timestamp;
- deterministic `quarantine_id` based on entity, source identifier, and payload.

Multiple rules can fail on the same row, and all are retained. The rejection CSV
is a convenience export; the SQLite `quarantine` table is the durable audit
record.

Quarantine is an **upsert** keyed by its deterministic ID. A rerun of the same
bad batch keeps 37 rows and increments `occurrence_count`; it does not append a
second 37. A corrected record is published on a later run, while its historical
quarantine evidence remains available.

## 6. Transaction and failure model

`publish_run()` performs:

1. begin one SQLite transaction;
2. insert/update current rows in dependency order;
3. archive replaced versions and in-batch duplicates;
4. upsert quarantine evidence;
5. run SQL target checks (PK, FK, ranges, timestamps, aggregates, business
   rules);
6. invoke the optional `verify` callback, build entity metrics, and build the
   reconciliation report inside the same transaction;
7. commit only if every hard check and the callback succeed; otherwise raise and
   roll back.

Why one transaction instead of per-entity commits? A customer update can change
whether an order is valid; a failed target check must not leave customers
published while orders are absent. Putting reconciliation in the callback closes
the last window: a reconciliation exception rolls back the published rows rather
than leaving a committed run without a valid accounting report. Monitor metrics
are recorded only after commit, so a rolled-back attempt is not reported as a
successful publish.

Retry classification is explicit:

- **Transient**: SQLite `database is locked`/busy/timeout, wrapped as
  `OperationalError`, retried with exponential backoff.
- **Permanent**: schema drift, validation failures, constraint violations,
  target-integrity failures, callback/reconciliation failures. These are not
  retried because repeating them cannot change the result and can hide the real
  defect.
- **Cancellation**: `KeyboardInterrupt` and `SystemExit` are not caught by the
  ordinary `except Exception` path. The pipeline records the run as `FAILED`
  with its error and then re-raises, preserving operator intent.

Tests cover transient retry, permanent no-retry, target-check rollback,
reconciliation rollback plus a successful retry, cancellation re-raise, and a
later successful recovery run.

### Restart and backfill

A failed attempt rolls back the entire transaction, so restart is simply a new
run over the same source directory with a new `run_id`. Already committed
business rows are unchanged; audit occurrence counts advance. Backfill is
operator-driven: point `--source-dir` at an immutable historical batch and supply a
new run ID. Version comparison via `updated_at` keeps late/out-of-order rows from
overwriting newer target data, and same-version conflicts remain quarantined.
There is deliberately no scheduler or watermark in this local implementation.

Incremental batches use the committed target as a parent-key source, not as a
reason to rescan every target row:

- orders may reference customers already published by an earlier run;
- order items may reference orders and products already published, but an order
  and all of its items must be in the same batch because `orders.have_items`
  would reject an order whose items are deferred;
- payments may arrive after their orders; their amount is checked against the
  committed order total when the order is absent from the batch;
- a batch total wins over a committed total when both exist;
- there is no delete/tombstone protocol: missing source keys are not interpreted
  as deletions.

The referenced-key lookup sends chunked `WHERE ... IN (...)` queries of at most
400 keys. This avoids loading the full target tables while keeping SQL variable
limits bounded.

## 7. Idempotency model

Idempotency is layered, not a single `INSERT OR REPLACE`:

| Layer | Mechanism | Effect |
|---|---|---|
| Business rows | PK match + payload hash + version | Identical rerun = unchanged; no duplicate business rows. |
| Older versions | Compare target `updated_at` | Archive, do not overwrite current data. |
| Quarantine | Deterministic ID + upsert | Audit set stable; occurrence count advances. |
| History | Deterministic ID + upsert | No duplicate history rows on replay. |
| Run metrics | `pipeline_runs.run_id` primary key | Reusing a run ID replaces that run's metrics. |
| Files | Per-run directory + latest copy | No append-only growth from reruns. |

The explicit non-idempotent surface is audit telemetry: `occurrence_count`,
`last_run_id`, and timestamps advance when the same evidence is seen again.
That is intentional and documented. Reusing a `run_id` also overwrites that
run's artifacts; callers that need immutable history should use a fresh run ID.

## 8. Ordering of validation and publish

The pipeline profiles rows in this order:

1. structural schema;
2. per-row type/format/range/domain/null checks;
3. version dedup;
4. fetch referenced parent keys from the committed target, chunked at 400 keys,
   unioned with the current batch's keys;
5. product FK;
6. customer FK;
7. order-total vs accepted items;
8. order FK for items (using only accepted orders);
9. payment order FK;
10. payment amount vs accepted orders, plus committed totals for late payment
    order IDs that are absent from the batch;
11. transform + Pandera target contract;
12. atomic publish + SQL checks + in-transaction reconciliation.

Why validate order totals before the order-FK filter for items? It prevents a
rejected order's items from contributing to a total that would otherwise look
valid. The exact sequence is visible in `src/pipeline.py` and covered by the small
dataset's expected contract.

## 9. Expectations and test oracles

`data/expected/small_expected.json` is a static expected-answer label set,
generated separately from `data/raw/` and never read by pipeline logic. It
contains expected counts, canonical IDs, rejected IDs/rules, and aggregates.
Regression tests prove the committed expected file still matches the deterministic
generator, and end-to-end tests compare the warehouse to it. This avoids the
circularity of testing the pipeline only against its own output.

## 10. Performance choice

The first implementation used a pure-Python `groupby(...).transform("max")` for
latest-version selection. At 100k rows it was a clear hot spot. The production
implementation now:

- sorts by key/version/row number;
- uses `drop_duplicates` and a key-to-version map;
- vectorizes duplicate detection with `duplicated` and NumPy masks;
- avoids per-row DataFrame extraction in rejection construction.

A naive row-wise reference remains in `src/deduplication.py` for benchmarking and
equivalence testing. Medians of three runs on the reference machine: 12.75x
faster at 10k rows and 9.20x at 100k, with equivalent outputs. Peak traced Python
allocation at 100k was 82.77 MB vectorised versus 67.21 MB naive, a reasonable
time-for-memory trade-off documented in `docs/performance.md`. End-to-end runs
are recorded for the 79-row small batch and 10k/100k batches, not only the dedup
hot spot.

## 11. Limitations and production path

Key limitations:

- full-batch in-memory processing;
- single SQLite writer;
- local files/JSONL instead of a scheduler and observability backend;
- no encryption/auth because the environment is local and synthetic;
- no delete/tombstone protocol, so missing source keys never imply deletion;
- an order and its items must arrive together; only late payments may reference a
  committed order across batches;
- version conflicts require human/upstream resolution rather than automatic
  selection.

A production evolution would keep the validation/dedup policy but move to:

1. object storage + a manifest for source files;
2. a scheduler with an explicit `run_id` and retry policy;
3. PostgreSQL staging + merge/upsert, or a lakehouse table format;
4. partitioned processing for batches larger than memory;
5. centralized metrics, traces, and alert thresholds;
6. a replay/backfill service driven by immutable run and source manifests.

The core rules and invariants are intentionally isolated in pure modules so
these changes can replace infrastructure without rewriting validation semantics.
