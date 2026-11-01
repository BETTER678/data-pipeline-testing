"""Run the complete e-commerce order pipeline from the command line."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import PipelineSettings, default_settings  # noqa: E402
from src.pipeline import run_pipeline  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    settings = default_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=settings.raw_dir.parent,
        help="Base data directory (expects raw/ inside).",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=None,
        help="Direct source CSV directory; overrides --data-dir/raw.",
    )
    parser.add_argument("--db-path", type=Path, default=settings.db_path)
    parser.add_argument("--reports-dir", type=Path, default=settings.reports_dir)
    parser.add_argument("--run-id", type=str, default=None)
    parser.add_argument("--log-level", type=str, default=settings.log_level)
    parser.add_argument(
        "--fail-on-partial",
        action="store_true",
        help="Exit non-zero when the run is PARTIAL_SUCCESS (default: only FAILED exits 1).",
    )
    parser.add_argument("--entities", nargs="+", default=None)
    return parser.parse_args()


def main() -> int:
    """Run the pipeline from command-line paths."""
    args = parse_args()
    data_dir = args.data_dir
    settings = PipelineSettings(
        raw_dir=data_dir / "raw",
        processed_dir=data_dir / "processed",
        rejected_dir=data_dir / "rejected",
        expected_dir=data_dir / "expected",
        reports_dir=args.reports_dir,
        db_url=(
            f"sqlite:///{args.db_path.as_posix()}" if args.db_path else default_settings().db_url
        ),
        log_level=args.log_level,
    )
    result = run_pipeline(
        settings=settings,
        run_id=args.run_id,
        source_dir=args.source_dir,
        entities=args.entities,
    )
    summary = {
        "run_id": result.run_id,
        "status": result.status,
        "input_rows": result.metrics.get("input_rows"),
        "valid_rows": result.metrics.get("valid_rows"),
        "rejected_rows": result.metrics.get("rejected_rows"),
        "duplicate_rows": result.metrics.get("duplicate_rows"),
        "published_rows": result.metrics.get("published_rows"),
        "unchanged_rows": result.metrics.get("unchanged_rows"),
        "late_arriving_rows": result.metrics.get("late_arriving_rows"),
        "duration_seconds": result.metrics.get("duration_seconds"),
        "error": result.error or None,
        "artifacts": {key: str(value) for key, value in result.artifacts.items()},
    }
    print(json.dumps(summary, indent=2))
    if result.status == "FAILED":
        return 1
    if args.fail_on_partial and result.status == "PARTIAL_SUCCESS":
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
