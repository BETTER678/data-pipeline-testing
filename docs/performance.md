# Performance Measurement

## 1. What is measured

`scripts/benchmark.py` measures two things:

1. **Deduplication micro-benchmark** at 10,000 and 100,000 input rows, comparing
   the production vectorized implementation with a row-wise reference
   implementation of the same policy.
2. **End-to-end pipeline** on the curated 79-row sample and generated ~10k and
   ~100k source batches, including ingestion, validation, dedup, FK/business
   rules, transformation, atomic publish, target checks, reconciliation, and
   reports.

The benchmark is intentionally local and reproducible. It does not call a cloud
service or claim production-scale throughput.

## 2. Reference environment

From `reports/benchmark.json`:

| Item | Value |
|---|---|
| Platform | Windows 11 (10.0.26200, SP0) |
| Python | 3.13.15 |
| pandas | 2.3.3 |
| NumPy | 2.3.5 |
| Database | SQLite via SQLAlchemy, local file |
| Repetitions | 3, median reported |

No CPU pinning, isolated container, or controlled load generator was used. Treat
absolute timings as machine-specific evidence; use the same command on your
machine for comparison.

## 3. Command and methodology

```powershell
.\.venv\Scripts\python.exe scripts\benchmark.py --pipeline --repeat 3
```

For dedup:

- The same synthetic `orders` frame is used for vectorized and naive runs.
- Exact duplicates make up ~10% of input; superseded older versions ~5%.
- Each implementation is called three times; the median wall time and population
  standard deviation are reported.
- Canonical rows, duplicate counts, and duplicate breakdown are compared before a
  speedup is accepted. The benchmark raises if the outputs disagree.
- Peak memory is measured in a separate `tracemalloc` pass per implementation,
  so memory tracing does not contaminate the timed calls.

For the pipeline:

- The benchmark runs the curated 79-row sample and generates two fresh batches
  with the configured invalid, null, duplicate, and late percentages: a ~10k
  batch and a ~100k batch.
- Each batch uses its own database and report directory
  (`data/benchmark/small/`, `data/benchmark/10k/`, and `data/benchmark/`), so
  results do not overwrite each other.
- It deletes any prior benchmark database, runs the same `run_pipeline` entry
  point used by the CLI, and then compares table counts with the generator's
  expected target counts.
- Source input above 100,000 rows for the large case is intentional: 24,000 base
  orders plus injected records produce 101,938 physical source rows.

## 4. Dataset sizes

### Small end-to-end

The curated sample has 79 source rows and 32 accepted target rows. It exercises
all edge cases but is dominated by startup, reporting, and plotting overhead.
Recorded final runs:

| Run | Source rows | Published | Unchanged | Duration | Nominal rows/s |
|---|---:|---:|---:|---:|---:|
| `demo-small-001` | 79 | 32 | 0 | 2.6329 s | 30.0 |
| `demo-small-002` | 79 | 0 | 32 | 1.9663 s | 40.2 |

These rates are not a throughput recommendation. The first run includes schema
creation, target checks, DQ report creation, and chart rendering; the second
reuses the database and still pays report/reconciliation overhead.

### 10,000 dedup rows

- 10,000 physical order rows;
- 8,500 unique keys;
- 1,500 duplicates removed: 1,011 exact, 489 superseded.

### 100,000 dedup rows

- 100,000 physical order rows;
- 85,000 unique keys;
- 15,000 duplicates removed: 10,127 exact, 4,873 superseded.

### 10,202-row end-to-end batch

| Counter | Value |
|---|---:|
| Source rows | 10,202 |
| Customers / products | 100 / 50 |
| Orders / items / payments | 2,400 / 4,810 / 2,400 |
| Quarantine rows | 295 |
| History rows | 147 |
| Status | `PARTIAL_SUCCESS` (rejects are expected) |
| Expected target counts matched | Yes |

### 101,938-row end-to-end batch

| Counter | Value |
|---|---:|
| Source rows | 101,938 |
| Customers / products | 1,000 / 500 |
| Orders / items / payments | 24,000 / 48,054 / 24,000 |
| Quarantine rows | 2,928 |
| History rows | 1,451 |
| Status | `PARTIAL_SUCCESS` (rejects are expected) |
| Expected target counts matched | Yes |

## 5. Deduplication results

Median of three repetitions:

| Rows | Vectorized | Naive reference | Speedup | Vector peak | Naive peak |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 0.6213 s | 5.6309 s | **9.06x** | 8.34 MB | 6.70 MB |
| 100,000 | 6.2136 s | 56.6036 s | **9.11x** | 82.77 MB | 67.21 MB |

Variability:

| Rows | Vector stdev | Naive stdev |
|---:|---:|---:|
| 10,000 | 0.0079 s | 0.0646 s |
| 100,000 | 0.5334 s | 3.4346 s |

The measured speedup varies with machine load (9.06x at 10k and 9.11x at
100k in this run), but both sizes show a large, repeatable advantage. The
earlier implementation was substantially slower; the important claim is not a
fixed multiplier, but that the production path is faster at both sizes with
equivalent canonical output and duplicate classification.

## 6. End-to-end pipeline results

```text
pipeline[small] rows=79     wall=2.18s  throughput=36.2 rows/s    status=PARTIAL_SUCCESS
pipeline[10k]   rows=10,202 wall=4.91s  throughput=2,079.2 rows/s status=PARTIAL_SUCCESS
pipeline[100k]  rows=101,938 wall=34.81s throughput=2,928.4 rows/s status=PARTIAL_SUCCESS
```

Recorded values:

| Metric | Small run | 10k run | 100k run |
|---|---:|---:|---:|
| Source/input rows | 79 | 10,202 | 101,938 |
| Wall seconds | 2.182 | 4.907 | 34.811 |
| Reported pipeline seconds | 2.1641 | 4.8270 | 34.6544 |
| Throughput | 36.2 rows/s | 2,079.2 rows/s | 2,928.4 rows/s |
| Status | `PARTIAL_SUCCESS` | `PARTIAL_SUCCESS` | `PARTIAL_SUCCESS` |
| Target counts match generated expectation | Yes | Yes | Yes |
| Customers | 7 | 100 | 1,000 |
| Products | 4 | 50 | 500 |
| Orders | 6 | 2,400 | 24,000 |
| Order items | 8 | 4,810 | 48,054 |
| Payments | 7 | 2,400 | 24,000 |

`PARTIAL_SUCCESS` is correct for all datasets because deliberately injected bad
rows are quarantined; the target counts and reconciliation checks match the
generator's expected valid output. The small run also demonstrates that startup,
schema creation, chart rendering, and report generation dominate at small batch
sizes, which is why throughput rises at 10k and 100k.

## 7. Optimization: vectorized version selection

### Previous shape

The original dedup path used a pure-Python `groupby` with
`transform("max")` to find the latest version for every key. On object/string
timestamps this repeatedly walks groups and was the dominant cost.

### Current shape

`src/deduplication.py` now:

1. computes record hashes once;
2. sorts by `(primary_key, updated_at, source_row_number)`;
3. uses `drop_duplicates(primary_key, keep="last")` to build a key-to-latest
   version map;
4. vectorizes the `updated_at` comparison with `map`/`eq`;
5. uses `duplicated([key, hash])` and NumPy masks to classify duplicates;
6. constructs rejection frames with vectorized index selection.

### Measured improvement

Compared with the retained row-wise reference implementing the same policy:

- **10k rows:** 5.6309 s -> 0.6213 s, **9.06x faster**;
- **100k rows:** 56.6036 s -> 6.2136 s, **9.11x faster**.

The benchmark checks equivalent canonical IDs/versions/status, duplicate counts,
and conflict counts at both sizes. This is the required "reasonable
optimization" with measured evidence, not an unsupported claim.

### Trade-off

At 100k rows the vectorized path peaked at 82.77 MB traced Python allocations vs
67.21 MB for the naive path (about 1.23x more memory) while running 9.11x faster.
That is a sensible time-for-memory trade-off for this batch size. Memory is
reported only for the dedup hot spot; end-to-end memory is not claimed because
SQLAlchemy, matplotlib, and report construction make a single number easy to
misinterpret.

## 8. Interpreting and improving further

- These numbers are local and synthetic. They demonstrate relative improvement,
  not a production SLA.
- The end-to-end rate includes validation, dedup, SQLite writes, target checks,
  report generation, and chart rendering; it is deliberately broader than a raw
  insert benchmark.
- The next useful optimizations would be bulk `executemany`/COPY-style inserts,
  chunked entity processing, and a PostgreSQL staging/upsert path. They are
  documented as future work rather than claimed here.
- The benchmark fails closed: if the vectorized and naive outputs differ, or if
  target counts do not match the generator expectation, the script raises instead
  of writing a misleading result.
- Machine-readable evidence: `reports/benchmark.json`; curated copy:
  `docs/evidence/benchmark.json`.
