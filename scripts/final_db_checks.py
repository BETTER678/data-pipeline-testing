"""Run direct SQLite integrity checks and write a compact JSON evidence file.

This complements the pipeline's in-transaction target checks with independent
queries against the final database: row counts, duplicate primary keys, foreign
keys, aggregates, audit counts, and the recorded run rows.

Usage:
    python scripts/final_db_checks.py
    python scripts/final_db_checks.py --db-path data/processed/warehouse.db \
        --output reports/final_database_checks.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import ENTITIES, FOREIGN_KEYS, PRIMARY_KEYS, default_settings  # noqa: E402
from src.publishing import get_engine  # noqa: E402


def _scalar(connection, sql: str):
    return connection.execute(text(sql)).scalar()


def collect_checks(db_path: Path) -> dict:
    """Collect independent database integrity checks."""
    engine = get_engine(f"sqlite:///{db_path.as_posix()}")
    with engine.connect() as connection:
        row_counts = {
            entity: int(_scalar(connection, f"SELECT COUNT(*) FROM {entity}") or 0)
            for entity in ENTITIES
        }
        pk_duplicate_groups = {}
        for entity in ENTITIES:
            pk = PRIMARY_KEYS[entity]
            pk_duplicate_groups[entity] = int(
                _scalar(
                    connection,
                    f"SELECT COUNT(*) FROM (SELECT {pk} FROM {entity} "
                    f"GROUP BY {pk} HAVING COUNT(*) > 1)",
                )
                or 0
            )

        foreign_key_check = [
            dict(row._mapping) for row in connection.execute(text("PRAGMA foreign_key_check"))
        ]
        orphans = {}
        for child, mapping in FOREIGN_KEYS.items():
            for column, (parent, parent_pk) in mapping.items():
                orphans[f"{child}.{column}"] = int(
                    _scalar(
                        connection,
                        f"SELECT COUNT(*) FROM {child} c LEFT JOIN {parent} p "
                        f"ON c.{column} = p.{parent_pk} "
                        f"WHERE p.{parent_pk} IS NULL",
                    )
                    or 0
                )

        order_total_sum = float(
            _scalar(connection, "SELECT COALESCE(SUM(total_amount), 0) FROM orders") or 0
        )
        line_total_sum = float(
            _scalar(connection, "SELECT COALESCE(SUM(line_total), 0) FROM order_items") or 0
        )
        captured_payment_sum = float(
            _scalar(
                connection,
                "SELECT COALESCE(SUM(amount), 0) FROM payments " "WHERE status = 'CAPTURED'",
            )
            or 0
        )
        aggregates = {
            "order_total_sum": round(order_total_sum, 2),
            "line_total_sum": round(line_total_sum, 2),
            "captured_payment_sum": round(captured_payment_sum, 2),
        }

        audit = {
            "quarantine_rows": int(_scalar(connection, "SELECT COUNT(*) FROM quarantine") or 0),
            "record_history_rows": int(
                _scalar(connection, "SELECT COUNT(*) FROM record_history") or 0
            ),
            "quarantine_occurrence_range": list(
                connection.execute(
                    text(
                        "SELECT COALESCE(MIN(occurrence_count), 0), "
                        "COALESCE(MAX(occurrence_count), 0) FROM quarantine"
                    )
                ).one()
            ),
            "history_occurrence_range": list(
                connection.execute(
                    text(
                        "SELECT COALESCE(MIN(occurrence_count), 0), "
                        "COALESCE(MAX(occurrence_count), 0) FROM record_history"
                    )
                ).one()
            ),
        }

        pipeline_runs = [
            dict(row._mapping)
            for row in connection.execute(
                text(
                    "SELECT run_id, status, input_rows, rejected_rows, "
                    "duplicate_rows, published_rows, unchanged_rows "
                    "FROM pipeline_runs ORDER BY start_timestamp"
                )
            )
        ]
        quarantine_sample = [
            dict(row._mapping)
            for row in connection.execute(
                text(
                    "SELECT entity, source_record_id, rejection_rules, last_run_id "
                    "FROM quarantine ORDER BY entity, source_record_id LIMIT 5"
                )
            )
        ]

    integrity = {
        "unique_pk": all(value == 0 for value in pk_duplicate_groups.values()),
        "no_fk_violations": not foreign_key_check and all(value == 0 for value in orphans.values()),
        "aggregates_match": len(
            {
                aggregates["order_total_sum"],
                aggregates["line_total_sum"],
                aggregates["captured_payment_sum"],
            }
        )
        == 1,
        "audit_not_appended_on_rerun": (
            all(
                value == 2
                for key in ("quarantine_occurrence_range", "history_occurrence_range")
                for value in audit[key]
            )
            if audit["quarantine_rows"] and audit["record_history_rows"]
            else False
        ),
    }
    return {
        "database": db_path.as_posix(),
        "row_counts": row_counts,
        "pk_duplicate_groups": pk_duplicate_groups,
        "foreign_key_check": foreign_key_check,
        "orphans": orphans,
        "aggregates": aggregates,
        "audit": audit,
        "pipeline_runs": pipeline_runs,
        "quarantine_sample": quarantine_sample,
        "integrity": integrity,
    }


def main() -> int:
    """Run the database checks and write JSON output."""
    settings = default_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=settings.db_path)
    parser.add_argument(
        "--output",
        type=Path,
        default=settings.reports_dir / "final_database_checks.json",
    )
    args = parser.parse_args()
    payload = collect_checks(args.db_path)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload["integrity"], indent=2))
    print(f"Wrote {args.output}")
    return 0 if all(payload["integrity"].values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
