"""Reproducible performance benchmark for the pipeline.

Two measurements are produced:

1. Deduplication micro-benchmark at ~10k and ~100k rows, comparing the
   production vectorised implementation with the row-wise reference
   implementation that mirrors the same semantics.  Both outputs are compared
   for equivalence so the speedup is not bought with different results.
2. Optional end-to-end runs on the curated 79-row small dataset and the
   generated ~10k/100k datasets, including ingestion, validation, deduplication,
   referential/business rules, transformation, publish, target checks and
   reporting.

Usage::

    python scripts/benchmark.py                    # dedup only
    python scripts/benchmark.py --pipeline         # dedup + end-to-end small/10k/100k
    python scripts/benchmark.py --sizes 10000 100000 --repeat 3

Results are written to ``reports/benchmark.json`` and a readable summary is
printed to stdout.  All numbers are measured on the machine that runs the
script; no numbers are hard-coded.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import time
import tracemalloc
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import PipelineSettings  # noqa: E402
from src.deduplication import deduplicate, deduplicate_naive  # noqa: E402
from src.generator import ScalableConfig, generate_scalable, generate_small  # noqa: E402
from src.pipeline import run_pipeline  # noqa: E402
from src.publishing import get_engine, table_counts  # noqa: E402


def _synthetic_orders(rows: int, seed: int = 42) -> pd.DataFrame:
    """Build an order frame with exact duplicates and superseded versions."""

    duplicate_count = int(round(rows * 0.10))
    late_count = int(round(rows * 0.05))
    base_count = rows - duplicate_count - late_count
    if base_count <= 0:
        raise ValueError("rows must be large enough to benchmark")

    rng = np.random.default_rng(seed)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    records: list[dict[str, Any]] = []
    for index in range(base_count):
        order_date = start + timedelta(seconds=index * 37)
        records.append(
            {
                "order_id": f"ORD-{index:08d}",
                "customer_id": f"CUST-{(index % 5000):06d}",
                "order_date": order_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "status": "PAID",
                "total_amount": f"{(index % 900 + 1) + 0.99:.2f}",
                "currency": "USD",
                "updated_at": (order_date + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )

    # Exact duplicates keep the same primary key and complete payload.
    for _ in range(duplicate_count):
        records.append(dict(records[int(rng.integers(0, base_count))]))

    # Older versions share the primary key but carry a different payload and
    # an updated_at earlier than the base version.
    for _ in range(late_count):
        source = records[int(rng.integers(0, base_count))]
        older = dict(source)
        older["status"] = "PENDING"
        order_date = datetime.fromisoformat(source["order_date"].replace("Z", "+00:00"))
        older["updated_at"] = (order_date + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        records.append(older)

    frame = pd.DataFrame(records)
    frame["_source_file"] = "synthetic_orders.csv"
    frame["_source_row_number"] = np.arange(1, len(frame) + 1, dtype="int64")
    frame["_run_id"] = "benchmark"
    frame["_ingested_at"] = "2026-01-01T00:00:00+00:00"
    return frame


def _canonical_signature(outcome) -> list[tuple[str, str, str]]:
    frame = outcome.canonical.sort_values("order_id")
    return [
        (str(row.order_id), str(row.updated_at), str(row.status))
        for row in frame.itertuples(index=False)
    ]


def _time_call(func, repeat: int) -> tuple[float, float, Any]:
    timings: list[float] = []
    result = None
    for _ in range(repeat):
        started = time.perf_counter()
        result = func()
        timings.append(time.perf_counter() - started)
    return statistics.median(timings), statistics.pstdev(timings), result


def _peak_memory_mb(func) -> tuple[float, Any]:
    """Measure Python allocation peak for one call (separate from timing)."""

    tracemalloc.start()
    try:
        result = func()
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return round(peak / (1024 * 1024), 2), result


def dedup_benchmark(sizes: list[int], repeat: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for size in sizes:
        frame = _synthetic_orders(size)
        vector_time, vector_std, vector_outcome = _time_call(
            lambda frame=frame: deduplicate(frame, "orders"), repeat
        )
        naive_time, naive_std, naive_outcome = _time_call(
            lambda frame=frame: deduplicate_naive(frame, "orders"), repeat
        )
        equivalent = (
            _canonical_signature(vector_outcome) == _canonical_signature(naive_outcome)
            and len(vector_outcome.duplicates) == len(naive_outcome.duplicates)
            and vector_outcome.duplicate_counts == naive_outcome.duplicate_counts
        )
        if not equivalent:
            raise AssertionError(f"vectorised and naive dedup disagree at {size} rows")
        vector_peak_mb, _ = _peak_memory_mb(lambda frame=frame: deduplicate(frame, "orders"))
        naive_peak_mb, _ = _peak_memory_mb(lambda frame=frame: deduplicate_naive(frame, "orders"))
        results.append(
            {
                "rows": len(frame),
                "unique_keys": int(frame["order_id"].nunique()),
                "vectorised_seconds_median": round(vector_time, 4),
                "vectorised_seconds_stdev": round(vector_std, 4),
                "naive_seconds_median": round(naive_time, 4),
                "naive_seconds_stdev": round(naive_std, 4),
                "speedup_x": round(naive_time / vector_time, 2) if vector_time else None,
                "vectorised_peak_mb": vector_peak_mb,
                "naive_peak_mb": naive_peak_mb,
                "removed_duplicates": len(vector_outcome.duplicates),
                "duplicate_breakdown": vector_outcome.duplicate_counts,
                "canonical_rows": len(vector_outcome.canonical),
                "equivalent_outputs": equivalent,
                "repeat": repeat,
            }
        )
        print(
            f"dedup rows={len(frame):>7,} vector={vector_time:8.3f}s "
            f"naive={naive_time:8.3f}s speedup={naive_time / vector_time:5.2f}x"
        )
    return results


def _pipeline_config(customers: int, products: int, orders: int) -> ScalableConfig:
    """Build a scalable config for one end-to-end benchmark size."""

    return ScalableConfig(
        customers=customers,
        products=products,
        orders=orders,
        seed=42,
        invalid_pct=0.02,
        duplicate_pct=0.01,
        null_pct=0.01,
        late_pct=0.01,
    )


PIPELINE_BENCHMARKS = (
    (
        "10k",
        _pipeline_config(customers=100, products=50, orders=2_400),
        ROOT / "data" / "benchmark" / "10k",
        ROOT / "reports" / "benchmark" / "10k",
        "benchmark-10k",
    ),
    (
        "100k",
        # Base rows: 1,000 customers + 500 products + 24,000 orders
        # + ~48,000 items + 24,000 payments = 97,500. Injected invalid/null/
        # duplicate rows push the source above 100,000.
        _pipeline_config(customers=1_000, products=500, orders=24_000),
        ROOT / "data" / "benchmark",
        ROOT / "reports" / "benchmark",
        "benchmark-large",
    ),
)


def _run_benchmark(
    label: str,
    data_dir: Path,
    reports_dir: Path,
    run_id: str,
    expected_target: dict[str, int],
) -> dict[str, Any]:
    """Run one already-generated dataset through the complete pipeline."""

    raw_dir = data_dir / "raw"
    expected_dir = data_dir / "expected"
    db_path = data_dir / "warehouse.db"
    for path in (db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")):
        if path.exists():
            path.unlink()
    reports_dir.mkdir(parents=True, exist_ok=True)
    settings = PipelineSettings(
        raw_dir=raw_dir,
        processed_dir=data_dir / "processed",
        rejected_dir=data_dir / "rejected",
        expected_dir=expected_dir,
        reports_dir=reports_dir,
        db_url=f"sqlite:///{db_path.as_posix()}",
        max_attempts=3,
        retry_base_delay=0.2,
        log_level="WARNING",
    )
    started = time.perf_counter()
    result = run_pipeline(settings=settings, run_id=run_id)
    elapsed = time.perf_counter() - started
    counts = table_counts(get_engine(settings.db_url))
    counts_match = all(counts[entity] == expected_target[entity] for entity in expected_target)
    if result.status == "FAILED":
        raise AssertionError(f"benchmark pipeline failed: {result.error}")
    if not counts_match:
        raise AssertionError(
            f"benchmark target counts differ from generated expectation: "
            f"{counts} vs {expected_target}"
        )
    payload = {
        "label": label,
        "source_rows": result.metrics["input_rows"],
        "wall_seconds": round(elapsed, 3),
        "pipeline_reported_seconds": result.metrics["duration_seconds"],
        "status": result.status,
        "counts": counts,
        "expected_target_counts": expected_target,
        "counts_match": counts_match,
        "throughput_rows_per_second": (
            round(result.metrics["input_rows"] / elapsed, 1) if elapsed else None
        ),
        "run_id": result.run_id,
    }
    print(
        f"pipeline[{label}] rows={payload['source_rows']:,} wall={elapsed:.2f}s "
        f"throughput={payload['throughput_rows_per_second']:,} rows/s "
        f"status={result.status}"
    )
    return payload


def pipeline_benchmark(
    label: str,
    config: ScalableConfig,
    data_dir: Path,
    reports_dir: Path,
    run_id: str,
) -> dict[str, Any]:
    """Generate and run one scalable pipeline benchmark."""

    data_dir.mkdir(parents=True, exist_ok=True)
    expected = generate_scalable(data_dir / "raw", config, data_dir / "expected")
    return _run_benchmark(
        label,
        data_dir,
        reports_dir,
        run_id,
        expected["expected"]["target_counts"],
    )


def small_pipeline_benchmark() -> dict[str, Any]:
    """Run the curated 79-row dataset through the complete pipeline."""

    data_dir = ROOT / "data" / "benchmark" / "small"
    data_dir.mkdir(parents=True, exist_ok=True)
    expected = generate_small(data_dir / "raw", data_dir / "expected")
    return _run_benchmark(
        "small",
        data_dir,
        ROOT / "reports" / "benchmark" / "small",
        "benchmark-small",
        expected["target_counts"],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", type=int, default=[10_000, 100_000])
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument(
        "--pipeline",
        action="store_true",
        help="Also run full pipelines on generated ~10k and ~100k datasets.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "reports" / "benchmark.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.repeat < 1:
        raise SystemExit("--repeat must be >= 1")
    pipeline_runs: list[dict[str, Any]] = []
    if args.pipeline:
        pipeline_runs.append(small_pipeline_benchmark())
        for label, config, data_dir, reports_dir, run_id in PIPELINE_BENCHMARKS:
            pipeline_runs.append(
                pipeline_benchmark(
                    label,
                    config,
                    data_dir,
                    reports_dir,
                    run_id,
                )
            )

    payload: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "pandas": pd.__version__,
            "numpy": np.__version__,
        },
        "deduplication": dedup_benchmark(args.sizes, args.repeat),
        # ``pipeline`` remains the ~100k run for backward compatibility with
        # earlier reports; ``pipeline_runs`` carries both measured sizes.
        "pipeline": pipeline_runs[-1] if pipeline_runs else None,
        "pipeline_runs": pipeline_runs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"benchmark written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
