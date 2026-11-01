"""Verify a finished run against the static expected contract.

Checks performed:

* run status and per-entity source/valid/rejected/duplicate counts;
* canonical primary keys in the warehouse;
* rejected primary keys and their quarantine rules;
* target aggregates (order totals, line totals, captured payments);
* target integrity checks (PK, FK, ranges, timestamps, business rules);
* reconciliation balance and quarantine/history row counts.

The expected file is data, never source input, so this script is independent of
the generator.  Exit code 0 means every check passed.

Usage::

    python scripts/verify_outputs.py \
        --expected data/expected/small_expected.json \
        --db-path data/processed/warehouse.db
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import ENTITIES, PRIMARY_KEYS  # noqa: E402
from src.publishing import get_engine  # noqa: E402
from src.reconciliation import run_target_checks  # noqa: E402


class Verifier:
    def __init__(self, expected: dict[str, Any], db_path: Path, run_id: str | None):
        self.expected = expected
        self.engine = get_engine(f"sqlite:///{db_path.as_posix()}")
        self.checks: list[dict[str, Any]] = []
        self.run_id = run_id or self._latest_run_id()

    def _latest_run_id(self) -> str:
        with self.engine.connect() as connection:
            value = connection.execute(
                text(
                    "SELECT run_id FROM pipeline_runs "
                    "ORDER BY end_timestamp DESC, run_id DESC LIMIT 1"
                )
            ).scalar()
        if value is None:
            raise SystemExit("No pipeline run found in pipeline_runs")
        return str(value)

    def check(self, name: str, actual: Any, expected: Any) -> None:
        passed = actual == expected
        self.checks.append(
            {
                "check": name,
                "actual": actual,
                "expected": expected,
                "passed": passed,
            }
        )

    def check_close(
        self, name: str, actual: float, expected: float, tolerance: float = 0.01
    ) -> None:
        passed = abs(float(actual) - float(expected)) <= tolerance
        self.checks.append(
            {
                "check": name,
                "actual": actual,
                "expected": expected,
                "tolerance": tolerance,
                "passed": passed,
            }
        )

    def _run_row(self) -> dict[str, Any]:
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT run_id, status, input_rows, valid_rows, rejected_rows, "
                        "duplicate_rows, published_rows, unchanged_rows, "
                        "late_arriving_rows, failure_reason, metrics_json "
                        "FROM pipeline_runs WHERE run_id = :run_id"
                    ),
                    {"run_id": self.run_id},
                )
                .mappings()
                .first()
            )
        if row is None:
            raise SystemExit(f"Run {self.run_id!r} not found in pipeline_runs")
        result = dict(row)
        result["metrics"] = json.loads(result.pop("metrics_json"))
        return result

    def _quarantine(self) -> tuple[int, dict[str, set[str]]]:
        with self.engine.connect() as connection:
            # Audit rows are upserted by design, so a rerun updates last_run_id
            # instead of appending.  Verify the global expected set: exactly the
            # same 37 quarantined source records after one or many runs.
            rows = (
                connection.execute(text("SELECT source_record_id, rejection_rules FROM quarantine"))
                .mappings()
                .all()
            )
        rules: dict[str, set[str]] = {}
        for row in rows:
            rules.setdefault(str(row["source_record_id"]), set()).update(
                json.loads(row["rejection_rules"] or "[]")
            )
        return len(rows), rules

    def _target_ids(self, entity: str) -> list[str]:
        pk = PRIMARY_KEYS[entity]
        with self.engine.connect() as connection:
            rows = connection.execute(text(f"SELECT {pk} FROM {entity} ORDER BY {pk}")).scalars()
        return [str(value) for value in rows]

    def _aggregates(self) -> dict[str, float]:
        with self.engine.connect() as connection:
            order_total = connection.execute(
                text("SELECT COALESCE(SUM(total_amount), 0) FROM orders")
            ).scalar()
            line_total = connection.execute(
                text("SELECT COALESCE(SUM(line_total), 0) FROM order_items")
            ).scalar()
            captured = connection.execute(
                text("SELECT COALESCE(SUM(amount), 0) FROM payments " "WHERE status = 'CAPTURED'")
            ).scalar()
        return {
            "order_total_sum": round(float(order_total or 0), 2),
            "line_total_sum": round(float(line_total or 0), 2),
            "captured_payment_sum": round(float(captured or 0), 2),
        }

    def _expected_quarantine_rows(self) -> int:
        return int(self.expected.get("expected_quarantine_rows", 0))

    def _expected_history_rows(self) -> int:
        return int(self.expected.get("expected_history_rows", 0))

    def verify(self) -> dict[str, Any]:
        run = self._run_row()
        expected_status = self.expected.get("expected_status")
        if expected_status:
            self.check("run.status", run["status"], expected_status)
        self.check("run.run_id", self.run_id, self.run_id)

        metrics = run["metrics"]
        for entity in ENTITIES:
            expected_entity = self.expected["entities"][entity]
            actual_metrics = metrics.get("entities", {}).get(entity, {})
            for key in ("source_rows", "valid_rows", "rejected_rows", "duplicate_rows"):
                self.check(
                    f"{entity}.{key}",
                    int(actual_metrics.get(key, -1)),
                    int(expected_entity[key]),
                )
            self.check(
                f"{entity}.canonical_ids",
                self._target_ids(entity),
                sorted(expected_entity["canonical_ids"]),
            )
            for record_id, overrides in expected_entity.get("canonical_overrides", {}).items():
                pk = PRIMARY_KEYS[entity]
                columns = ", ".join(overrides)
                with self.engine.connect() as connection:
                    row = (
                        connection.execute(
                            text(f"SELECT {columns} FROM {entity} WHERE {pk} = :key"),
                            {"key": record_id},
                        )
                        .mappings()
                        .first()
                    )
                self.check(
                    f"{entity}.canonical_overrides.{record_id}",
                    dict(row) if row else None,
                    overrides,
                )

        quarantine_rows, quarantine_rules = self._quarantine()
        self.check(
            "quarantine.row_count",
            quarantine_rows,
            self._expected_quarantine_rows(),
        )
        for entity in ENTITIES:
            expected_rejected = self.expected["entities"][entity]
            actual_rules = {
                record_id: sorted(rules)
                for record_id, rules in quarantine_rules.items()
                if record_id in set(expected_rejected["rejected_ids"])
            }
            expected_rules = {
                record_id: sorted(rules)
                for record_id, rules in expected_rejected.get("rejected_rules", {}).items()
            }
            self.check(
                f"{entity}.rejected_rules",
                actual_rules,
                expected_rules,
            )

        expected_aggregates = self.expected.get("target_aggregates", {})
        actual_aggregates = self._aggregates()
        for name, value in expected_aggregates.items():
            self.check_close(f"aggregates.{name}", actual_aggregates[name], value)

        with self.engine.connect() as connection:
            target_checks = run_target_checks(connection)
            # The history table is also upserted, so check its invariant size
            # rather than ownership by the latest run.
            history_rows = int(
                connection.execute(text("SELECT COUNT(*) FROM record_history")).scalar() or 0
            )
        self.check("target_checks.ok", target_checks["ok"], True)
        self.check("history.row_count", history_rows, self._expected_history_rows())

        reconciliation = metrics.get("reconciliation", {})
        if reconciliation:
            self.check("reconciliation.ok", reconciliation.get("ok"), True)
            self.check(
                "reconciliation.source_accounting_balanced",
                reconciliation.get("source_accounting_balanced"),
                True,
            )

        passed = all(item["passed"] for item in self.checks)
        return {
            "run_id": self.run_id,
            "passed": passed,
            "checks": self.checks,
            "target_checks": target_checks,
        }


def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--expected",
        type=Path,
        default=ROOT / "data" / "expected" / "small_expected.json",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=ROOT / "data" / "processed" / "warehouse.db",
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "reports" / "verification.json",
    )
    return parser.parse_args()


def main() -> int:
    """Verify a run against the expected contract and write JSON."""
    args = parse_args()
    expected = json.loads(args.expected.read_text(encoding="utf-8"))
    verifier = Verifier(expected, args.db_path, args.run_id)
    result = verifier.verify()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")

    for item in result["checks"]:
        marker = "PASS" if item["passed"] else "FAIL"
        print(f"[{marker}] {item['check']}")
        if not item["passed"]:
            print(f"       actual={item['actual']!r}")
            print(f"       expected={item['expected']!r}")
    print(
        f"\n{sum(1 for item in result['checks'] if item['passed'])}"
        f"/{len(result['checks'])} checks passed for run {result['run_id']}"
    )
    print(f"verification written to {args.output}")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
