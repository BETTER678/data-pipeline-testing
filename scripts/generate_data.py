"""Generate reproducible small or scalable synthetic source data."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import default_settings  # noqa: E402
from src.generator import (  # noqa: E402
    ScalableConfig,
    generate_scalable,
    generate_small,
)


def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    settings = default_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preset", choices=["small", "large", "custom"], default="small")
    parser.add_argument("--output-dir", type=Path, default=settings.raw_dir)
    parser.add_argument("--expected-dir", type=Path, default=settings.expected_dir)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--products", type=int, default=200)
    parser.add_argument("--orders", type=int, default=2_500)
    parser.add_argument("--invalid-pct", type=float, default=0.02)
    parser.add_argument("--duplicate-pct", type=float, default=0.01)
    parser.add_argument("--null-pct", type=float, default=0.01)
    parser.add_argument("--late-pct", type=float, default=0.01)
    return parser.parse_args()


def main() -> int:
    """Generate the requested dataset and write its expected contract."""
    args = parse_args()
    if args.preset == "small":
        summary = generate_small(args.output_dir, args.expected_dir)
    else:
        summary = generate_scalable(
            args.output_dir,
            ScalableConfig(
                customers=args.customers,
                products=args.products,
                orders=args.orders,
                seed=args.seed,
                invalid_pct=args.invalid_pct,
                duplicate_pct=args.duplicate_pct,
                null_pct=args.null_pct,
                late_pct=args.late_pct,
            ),
            args.expected_dir,
        )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
