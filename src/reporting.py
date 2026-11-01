"""Machine-readable and human-readable run artifacts.

Every run writes:
* accepted datasets (per-run snapshot + latest copy);
* quarantine and duplicate audit CSVs;
* data-quality JSON/CSV and an optional chart;
* a failure trace when the run aborts.
"""

from __future__ import annotations

import json
import traceback
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import ENTITIES
from src.monitoring import get_logger

logger = get_logger(__name__)

RULE_DESCRIPTIONS: dict[str, str] = {
    "REQUIRED_NAME": "A required customer/product name was blank.",
    "REQUIRED_QUANTITY": "A required order-item quantity was blank.",
    "REQUIRED_PRODUCT_ID": "A required order-item product reference was blank.",
    "REQUIRED_PAYMENT_DATE": "A required payment date was blank.",
    "REQUIRED_UPDATED_AT": "A required version timestamp was blank.",
    "TYPE_QUANTITY": "Quantity was not an integer.",
    "TYPE_UNIT_PRICE": "Unit price was not a decimal number.",
    "TYPE_AMOUNT": "Payment amount was not a decimal number.",
    "FORMAT_TIMESTAMP": "A timestamp was not ISO-8601 with timezone.",
    "FORMAT_ORDER_DATE": "Order date was not ISO-8601 with timezone.",
    "FORMAT_PAYMENT_DATE": "Payment date was not ISO-8601 with timezone.",
    "FORMAT_CREATED_AT": "Customer creation timestamp was not ISO-8601.",
    "FORMAT_UPDATED_AT": "Version timestamp was not ISO-8601.",
    "TEMPORAL_UPDATED_AT": "updated_at was earlier than the event date.",
    "RANGE_QUANTITY": "Quantity was outside 1..10,000.",
    "RANGE_UNIT_PRICE": "Unit price was outside 0..100,000.",
    "RANGE_TOTAL_AMOUNT": "Order total was negative or above the configured cap.",
    "RANGE_AMOUNT": "Payment amount must be greater than zero.",
    "DOMAIN_CURRENCY": "Currency was not one of USD/EUR/GBP.",
    "DOMAIN_STATUS": "Status was not in the entity's allowed domain.",
    "DOMAIN_PAYMENT_METHOD": "Payment method was not recognised.",
    "FK_CUSTOMER_ID": "Order references a customer that was not accepted.",
    "FK_ORDER_ID": "Item/payment references an order that was not accepted.",
    "FK_PRODUCT_ID": "Order item references a product that was not accepted.",
    "BUSINESS_ORDER_WITHOUT_ITEMS": "An order had no accepted line items.",
    "BUSINESS_ORDER_TOTAL_MISMATCH": "Order total != sum of accepted line totals.",
    "BUSINESS_PAYMENT_MISMATCH": "Captured payment differs from order total by >1%.",
    "DEDUP_EXACT": "Exact duplicate of a record in the same batch.",
    "DEDUP_SUPERSEDED": "Older/out-of-order version superseded by updated_at.",
    "DEDUP_VERSION_CONFLICT": "Same key/version has multiple different payloads.",
    "PUBLISH_VERSION_CONFLICT": "Target already has this key/version with a different payload.",
}


def _ensure(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_processed_snapshots(
    canonical_by_entity: Mapping[str, pd.DataFrame],
    processed_dir: Path,
    run_id: str,
) -> dict[str, Path]:
    """Write accepted per-run and latest CSV snapshots."""
    run_dir = _ensure(Path(processed_dir) / run_id)
    outputs: dict[str, Path] = {}
    for entity in ENTITIES:
        frame = canonical_by_entity.get(entity, pd.DataFrame())
        path = run_dir / f"{entity}.csv"
        frame.to_csv(path, index=False, encoding="utf-8")
        latest = Path(processed_dir) / f"latest_{entity}.csv"
        frame.to_csv(latest, index=False, encoding="utf-8")
        outputs[entity] = path
    logger.info("Processed snapshots written run_id=%s dir=%s", run_id, run_dir)
    return outputs


def _combined_rejects(
    rejected_by_entity: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    frames = []
    for entity, frame in rejected_by_entity.items():
        if frame is None or frame.empty:
            continue
        copy = frame.copy()
        copy.insert(0, "entity", entity)
        if "source_record_id" not in copy.columns:
            copy["source_record_id"] = _source_record_id(copy, entity)
        if "source_file" not in copy.columns and "_source_file" in copy.columns:
            copy["source_file"] = copy["_source_file"]
        if "source_row_number" not in copy.columns and "_source_row_number" in copy.columns:
            copy["source_row_number"] = copy["_source_row_number"]
        frames.append(copy)
    if not frames:
        return pd.DataFrame(columns=["entity", *_base_reject_columns()])
    return pd.concat(frames, ignore_index=True, sort=False)


def _base_reject_columns() -> list[str]:
    return [
        "source_record_id",
        "source_file",
        "source_row_number",
        *_reject_metadata(),
    ]


def _reject_metadata() -> list[str]:
    return ["_reject_rules", "_reject_reasons", "_rejected_at", "_raw_record"]


def _source_record_id(frame: pd.DataFrame, entity: str) -> pd.Series:
    pk = {
        "customers": "customer_id",
        "products": "product_id",
        "orders": "order_id",
        "order_items": "item_id",
        "payments": "payment_id",
    }.get(entity)
    if pk and pk in frame.columns:
        return frame[pk].fillna("").astype(str)
    return "row:" + frame.get("_source_row_number", pd.Series(dtype=str)).astype(str)


def write_quarantine_csv(
    rejected_by_entity: Mapping[str, pd.DataFrame],
    rejected_dir: Path,
    run_id: str,
) -> Path:
    """Write the run's quarantine rows to a CSV artifact."""
    rejected_dir = _ensure(Path(rejected_dir))
    path = rejected_dir / f"quarantine_{run_id}.csv"
    combined = _combined_rejects(rejected_by_entity)
    if combined.empty:
        combined = pd.DataFrame(
            columns=[
                "entity",
                "source_record_id",
                "source_file",
                "source_row_number",
                "_reject_rules",
                "_reject_reasons",
                "_rejected_at",
                "_raw_record",
                "record_payload",
            ]
        )
    else:
        # ensure common traceability columns are present in a stable order
        for column in ("source_record_id", "source_file", "source_row_number"):
            if column not in combined.columns:
                combined[column] = ""
        ordered = [
            "entity",
            "source_record_id",
            "source_file",
            "source_row_number",
            "_reject_rules",
            "_reject_reasons",
            "_rejected_at",
            "_raw_record",
        ]
        remaining = [c for c in combined.columns if c not in ordered]
        combined = combined[[*ordered, *remaining]]
    combined.to_csv(path, index=False, encoding="utf-8")
    logger.info("Quarantine CSV written path=%s rows=%d", path, len(combined))
    return path


def write_duplicates_csv(
    duplicates_by_entity: Mapping[str, pd.DataFrame],
    rejected_dir: Path,
    run_id: str,
) -> Path:
    """Write the run's duplicate evidence rows to a CSV artifact."""
    rejected_dir = _ensure(Path(rejected_dir))
    path = rejected_dir / f"duplicates_{run_id}.csv"
    combined = _combined_rejects(duplicates_by_entity)
    if combined.empty:
        combined = pd.DataFrame(columns=["entity", "_reject_rules", "_reject_reasons"])
    combined.to_csv(path, index=False, encoding="utf-8")
    logger.info("Duplicate audit CSV written path=%s rows=%d", path, len(combined))
    return path


def _dq_entities(entity_metrics: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    entities: dict[str, dict[str, Any]] = {}
    for entity in ENTITIES:
        metrics = entity_metrics.get(entity, {})
        source = int(metrics.get("source_rows", 0))
        rejected = int(metrics.get("rejected_rows", 0))
        duplicates = int(metrics.get("duplicate_rows", 0))
        valid = int(metrics.get("valid_rows", 0))
        rules = {rule: int(count) for rule, count in metrics.get("rule_counts", {}).items()}
        entities[entity] = {
            "source_rows": source,
            "valid_rows": valid,
            "rejected_rows": rejected,
            "duplicate_rows": duplicates,
            "pass_rate_pct": round((valid / source) * 100, 2) if source else 100.0,
            "rejection_rate_pct": round((rejected / source) * 100, 2) if source else 0.0,
            "rule_counts": rules,
            "rule_descriptions": {
                rule: RULE_DESCRIPTIONS.get(rule, "Custom validation rule") for rule in rules
            },
            "null_counts": metrics.get("null_counts", {}),
        }
    return entities


def _dq_totals(entities: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    total_source = sum(item["source_rows"] for item in entities.values())
    total_valid = sum(item["valid_rows"] for item in entities.values())
    return {
        "source_rows": total_source,
        "rejected_rows": sum(item["rejected_rows"] for item in entities.values()),
        "duplicate_rows": sum(item["duplicate_rows"] for item in entities.values()),
        "valid_rows": total_valid,
        "overall_pass_rate_pct": (
            round((total_valid / total_source) * 100, 2) if total_source else 100.0
        ),
    }


def _dq_rule_rows(entities: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "entity": entity,
            "rule": rule,
            "count": count,
            "description": RULE_DESCRIPTIONS.get(rule, "Custom validation rule"),
        }
        for entity, item in entities.items()
        for rule, count in item["rule_counts"].items()
    ]


def write_dq_report(
    run_id: str,
    entity_metrics: Mapping[str, Mapping[str, Any]],
    reports_dir: Path,
) -> dict[str, Path]:
    """Write the data-quality JSON and CSV reports."""
    reports_dir = _ensure(Path(reports_dir))
    entities = _dq_entities(entity_metrics)
    totals = _dq_totals(entities)
    report = {
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "PASS" if totals["rejected_rows"] == 0 else "WARN",
        "totals": totals,
        "entities": entities,
    }
    json_path = reports_dir / f"dq_report_{run_id}.json"
    csv_path = reports_dir / f"dq_report_{run_id}.csv"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    pd.DataFrame(
        _dq_rule_rows(entities), columns=["entity", "rule", "count", "description"]
    ).to_csv(csv_path, index=False, encoding="utf-8")
    chart = write_dq_chart(entities, reports_dir, run_id)
    result = {"json": json_path, "csv": csv_path}
    if chart is not None:
        result["chart"] = chart
    logger.info("DQ report written run_id=%s", run_id)
    return result


def write_dq_chart(
    entities: Mapping[str, Mapping[str, Any]], reports_dir: Path, run_id: str
) -> Path | None:
    """Render the optional data-quality summary chart."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        names = list(entities)
        valid = [entities[name]["valid_rows"] for name in names]
        rejected = [entities[name]["rejected_rows"] for name in names]
        duplicates = [entities[name]["duplicate_rows"] for name in names]
        x = range(len(names))
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.bar(x, valid, label="accepted", color="#2E86AB")
        ax.bar(x, rejected, bottom=valid, label="rejected", color="#D1495B")
        bottom = [valid[i] + rejected[i] for i in range(len(names))]
        ax.bar(x, duplicates, bottom=bottom, label="duplicates", color="#EDAE49")
        ax.set_xticks(list(x))
        ax.set_xticklabels(names, rotation=20, ha="right")
        ax.set_ylabel("rows")
        ax.set_title(f"Source rows by outcome — {run_id[:8]}")
        ax.legend()
        fig.tight_layout()
        path = Path(reports_dir) / f"dq_summary_{run_id}.png"
        fig.savefig(path, dpi=120)
        plt.close(fig)
        return path
    except Exception as exc:  # pragma: no cover - optional visualization
        logger.warning("Could not generate DQ chart: %s", exc)
        return None


def write_failure_report(
    run_id: str,
    reports_dir: Path,
    error: BaseException | str,
    context: Mapping[str, Any] | None = None,
) -> Path:
    """Write a machine-readable failure report for a run."""
    reports_dir = _ensure(Path(reports_dir))
    path = reports_dir / f"failure_{run_id}.json"
    payload = {
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "error_type": type(error).__name__ if isinstance(error, BaseException) else "Error",
        "error": str(error),
        "traceback": traceback.format_exc() if isinstance(error, BaseException) else "",
        "context": context or {},
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    logger.error("Failure report written path=%s", path)
    return path
