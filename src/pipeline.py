"""End-to-end pipeline orchestration.

Source CSV -> ingestion -> structural schema -> row validation -> deduplication
-> referential integrity -> domain business rules -> transformation ->
transactional publish -> target checks -> reconciliation -> reports.
"""

from __future__ import annotations

import uuid
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from src import config
from src.config import PipelineSettings, default_settings
from src.deduplication import DedupOutcome, deduplicate
from src.ingestion import IngestionError, ingest_batch
from src.monitoring import PipelineMonitor, configure_logging, get_logger
from src.publishing import (
    PublishOutcome,
    fetch_existing_keys,
    fetch_published_order_totals,
    get_engine,
    init_db,
    publish_run,
    retry_call,
    save_run_metrics,
)
from src.reconciliation import (
    ReconciliationError,
    build_reconciliation,
    write_reconciliation_report,
)
from src.reporting import (
    write_dq_report,
    write_duplicates_csv,
    write_failure_report,
    write_processed_snapshots,
    write_quarantine_csv,
)
from src.schemas import validate_target_frame
from src.transformations import transform
from src.validation import (
    RowValidationOutcome,
    check_foreign_keys,
    check_order_totals,
    check_payment_amounts,
    concat_rejections,
    merge_rule_counts,
    rejection_rules,
    validate_rows,
    validate_source_structure,
)

logger = get_logger(__name__)


@dataclass
class RunResult:
    metrics: dict[str, Any]
    reconciliation: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)
    error: str = ""

    @property
    def status(self) -> str:
        return str(self.metrics.get("status", "FAILED"))

    @property
    def run_id(self) -> str:
        return str(self.metrics.get("run_id", ""))


@dataclass
class EntityStage:
    entity: str
    raw_rows: int
    row_validation: RowValidationOutcome
    dedup: DedupOutcome
    rejected_frames: list[pd.DataFrame] = field(default_factory=list)
    canonical: pd.DataFrame = field(default_factory=pd.DataFrame)


@dataclass
class PublishVerification:
    """Results computed inside the publish transaction for post-commit use."""

    entity_metrics: dict[str, dict[str, Any]] = field(default_factory=dict)
    reconciliation: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineContext:
    """Immutable run configuration shared by pipeline stages."""

    settings: PipelineSettings
    run_id: str
    source_dir: Path
    entity_list: list[str]
    monitor: PipelineMonitor
    engine: Any


@dataclass
class PipelineState:
    """Mutable state threaded through the pipeline stages."""

    ingestions: dict[str, Any] = field(default_factory=dict)
    stages: dict[str, EntityStage] = field(default_factory=dict)
    final_frames: dict[str, pd.DataFrame] = field(default_factory=dict)
    duplicates_by_entity: dict[str, pd.DataFrame] = field(default_factory=dict)
    rejected_by_entity: dict[str, pd.DataFrame] = field(default_factory=dict)
    transformed: dict[str, pd.DataFrame] = field(default_factory=dict)
    verification: PublishVerification = field(default_factory=PublishVerification)
    outcome: PublishOutcome | None = None
    attempts: int = 0
    reconciliation: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)


def _rule_counts(frames: Iterable[pd.DataFrame]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for frame in frames:
        for rule in rejection_rules(frame):
            counter[rule] += 1
    return dict(sorted(counter.items()))


def _entity_metrics(
    stage: EntityStage,
    final_frame: pd.DataFrame,
    publish_stats,
    publish_conflict_rows: int,
    source_file: str,
) -> dict[str, Any]:
    row_rejected = len(stage.row_validation.rejected)
    dedup_conflicts = len(stage.dedup.conflicts)
    cross_rejected = sum(len(frame) for frame in stage.rejected_frames)
    rejected = row_rejected + dedup_conflicts + cross_rejected + publish_conflict_rows
    duplicates = len(stage.dedup.duplicates)
    valid = publish_stats.published + publish_stats.unchanged + publish_stats.late_arriving
    source_rows = stage.raw_rows

    if rejected + duplicates + valid != source_rows:
        raise ReconciliationError(
            f"Source accounting failed for {stage.entity}: source={source_rows} "
            f"rejected={rejected} duplicates={duplicates} valid={valid}"
        )
    if valid != len(final_frame) - publish_conflict_rows:
        raise ReconciliationError(
            f"Publish accounting failed for {stage.entity}: valid={valid} "
            f"canonical={len(final_frame)} conflicts={publish_conflict_rows}"
        )

    rules = merge_rule_counts(
        stage.row_validation.rule_counts,
        _rule_counts(stage.rejected_frames),
        _rule_counts([stage.dedup.conflicts]),
    )
    if publish_conflict_rows:
        rules["PUBLISH_VERSION_CONFLICT"] = (
            rules.get("PUBLISH_VERSION_CONFLICT", 0) + publish_conflict_rows
        )

    return {
        "source_rows": source_rows,
        "row_rejected_rows": row_rejected,
        "dedup_conflict_rows": dedup_conflicts,
        "cross_entity_rejected_rows": cross_rejected,
        "publish_conflict_rows": publish_conflict_rows,
        "rejected_rows": rejected,
        "duplicate_rows": duplicates,
        "valid_rows": valid,
        "inserted_rows": publish_stats.inserted,
        "updated_rows": publish_stats.updated,
        "published_rows": publish_stats.published,
        "unchanged_rows": publish_stats.unchanged,
        "late_arriving_rows": publish_stats.late_arriving,
        "duplicate_rule_counts": dict(stage.dedup.duplicate_counts),
        "rule_counts": rules,
        "null_counts": stage.row_validation.null_counts,
        "source_file": source_file,
    }


def _apply_rule(
    frames: list[pd.DataFrame],
    frame: pd.DataFrame,
    rejected: pd.DataFrame,
) -> pd.DataFrame:
    if rejected is not None and not rejected.empty:
        frames.append(rejected)
    return frame


def _ingest_sources(
    source_dir: Path,
    run_id: str,
    entity_list: list[str],
    monitor: PipelineMonitor,
) -> dict[str, Any]:
    ingestions = ingest_batch(source_dir, run_id=run_id, entities=entity_list)
    for entity, ingestion in ingestions.items():
        monitor.record_ingestion(
            entity, Path(ingestion.metadata.path), ingestion.metadata.row_count
        )
    return ingestions


def _validate_and_deduplicate(
    ingestions: dict[str, Any],
    entity_list: list[str],
) -> tuple[dict[str, EntityStage], dict[str, pd.DataFrame]]:
    stages: dict[str, EntityStage] = {}
    duplicates_by_entity: dict[str, pd.DataFrame] = {}

    for entity in entity_list:
        ingestion = ingestions.get(entity)
        if ingestion is None:
            raise IngestionError(entity, Path(f"{entity}.csv"), "missing entity")
        raw = ingestion.frame
        validate_source_structure(raw, entity)
        row_outcome = validate_rows(raw, entity)
        dedup = deduplicate(row_outcome.valid, entity)
        stages[entity] = EntityStage(
            entity=entity,
            raw_rows=len(raw),
            row_validation=row_outcome,
            dedup=dedup,
            rejected_frames=[],
            canonical=dedup.canonical,
        )
        duplicates_by_entity[entity] = dedup.duplicates

    return stages, duplicates_by_entity


def _referenced_parent_keys(stages: dict[str, EntityStage]) -> dict[str, set[str]]:
    """Collect only the parent keys this batch actually references."""

    def key_set(entity: str, column: str) -> set[str]:
        frame = stages[entity].canonical
        return set(frame[column].astype(str)) if not frame.empty else set()

    return {
        "customers": key_set("orders", "customer_id"),
        "products": key_set("order_items", "product_id"),
        "orders": key_set("order_items", "order_id") | key_set("payments", "order_id"),
    }


def _fetch_target_parent_keys(engine, stages: dict[str, EntityStage]) -> dict[str, set[str]]:
    referenced = _referenced_parent_keys(stages)
    return {
        entity: fetch_existing_keys(engine, entity, keys) for entity, keys in referenced.items()
    }


def _apply_cross_entity_rules(
    stages: dict[str, EntityStage],
    target_parent_keys: dict[str, set[str]],
    engine,
) -> dict[str, pd.DataFrame]:
    customers = stages["customers"].canonical
    products = stages["products"].canonical
    customer_ids = target_parent_keys["customers"] | (
        set(customers["customer_id"].astype(str)) if not customers.empty else set()
    )
    product_ids = target_parent_keys["products"] | (
        set(products["product_id"].astype(str)) if not products.empty else set()
    )

    items = stages["order_items"].canonical
    items, product_fk_rejected = check_foreign_keys(items, "product_id", product_ids, "products")
    _apply_rule(stages["order_items"].rejected_frames, items, product_fk_rejected)

    orders = stages["orders"].canonical
    orders, customer_fk_rejected = check_foreign_keys(
        orders, "customer_id", customer_ids, "customers"
    )
    _apply_rule(stages["orders"].rejected_frames, orders, customer_fk_rejected)

    orders, total_rejected = check_order_totals(orders, items)
    _apply_rule(stages["orders"].rejected_frames, orders, total_rejected)

    valid_order_ids = target_parent_keys["orders"] | (
        set(orders["order_id"].astype(str)) if not orders.empty else set()
    )
    items, order_fk_rejected = check_foreign_keys(items, "order_id", valid_order_ids, "orders")
    _apply_rule(stages["order_items"].rejected_frames, items, order_fk_rejected)

    payments = stages["payments"].canonical
    payments, order_fk_pay_rejected = check_foreign_keys(
        payments, "order_id", valid_order_ids, "orders"
    )
    _apply_rule(stages["payments"].rejected_frames, payments, order_fk_pay_rejected)

    batch_order_ids = set(orders["order_id"].astype(str)) if not orders.empty else set()
    late_payment_order_ids = (
        set(payments["order_id"].astype(str)) - batch_order_ids if not payments.empty else set()
    )
    published_order_totals = fetch_published_order_totals(engine, late_payment_order_ids)
    payments, payment_amount_rejected = check_payment_amounts(
        payments,
        orders,
        published_order_totals=published_order_totals,
    )
    _apply_rule(stages["payments"].rejected_frames, payments, payment_amount_rejected)

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": items,
        "payments": payments,
    }


def _build_rejection_evidence(
    stages: dict[str, EntityStage],
    final_frames: dict[str, pd.DataFrame],
    duplicates_by_entity: dict[str, pd.DataFrame],
    entity_list: list[str],
) -> dict[str, pd.DataFrame]:
    rejected_by_entity: dict[str, pd.DataFrame] = {}
    for entity in entity_list:
        stage = stages[entity]
        rejected_by_entity[entity] = concat_rejections(
            [
                stage.row_validation.rejected,
                stage.dedup.conflicts,
                *stage.rejected_frames,
            ]
        )
        accounted = (
            len(rejected_by_entity[entity])
            + len(duplicates_by_entity[entity])
            + len(final_frames[entity])
        )
        if accounted != stage.raw_rows:
            raise ReconciliationError(
                f"Pre-publish source accounting failed for {entity}: "
                f"source={stage.raw_rows} rejected={len(rejected_by_entity[entity])} "
                f"duplicates={len(duplicates_by_entity[entity])} "
                f"canonical={len(final_frames[entity])}"
            )
    return rejected_by_entity


def _transform_and_validate(
    final_frames: dict[str, pd.DataFrame],
    entity_list: list[str],
) -> dict[str, pd.DataFrame]:
    transformed: dict[str, pd.DataFrame] = {}
    for entity in entity_list:
        transformed[entity] = transform(final_frames[entity], entity)
        validate_target_frame(transformed[entity], entity)
    return transformed


def _publish_with_retry(
    engine,
    transformed: dict[str, pd.DataFrame],
    rejected_by_entity: dict[str, pd.DataFrame],
    duplicates_by_entity: dict[str, pd.DataFrame],
    run_id: str,
    settings: PipelineSettings,
    monitor: PipelineMonitor,
    verify=None,
) -> tuple[PublishOutcome, int]:
    outcome, attempts = retry_call(
        lambda: publish_run(
            engine,
            transformed,
            rejected_by_entity,
            duplicates_by_entity,
            run_id=run_id,
            verify=verify,
        ),
        max_attempts=settings.max_attempts,
        base_delay=settings.retry_base_delay,
        on_retry=lambda attempt, exc: monitor.record_retry(attempt),
    )
    monitor.logger.info("Publish committed run_id=%s attempts=%d", run_id, attempts)
    return outcome, attempts


def _merge_publish_conflicts(
    rejected_by_entity: dict[str, pd.DataFrame],
    outcome: PublishOutcome,
    entity_list: list[str],
) -> dict[str, pd.DataFrame]:
    merged = dict(rejected_by_entity)
    for entity in entity_list:
        conflicts = outcome.publish_conflicts.get(entity, pd.DataFrame())
        if conflicts is not None and not conflicts.empty:
            merged[entity] = pd.concat([merged[entity], conflicts], ignore_index=True)
    return merged


def _entity_metrics_for_run(
    stages: dict[str, EntityStage],
    transformed: dict[str, pd.DataFrame],
    outcome: PublishOutcome,
    ingestions: dict[str, Any],
    entity_list: list[str],
) -> dict[str, dict[str, Any]]:
    return {
        entity: _entity_metrics(
            stages[entity],
            transformed[entity],
            outcome.entity_stats[entity],
            int(outcome.entity_stats[entity].conflicts),
            str(ingestions[entity].metadata.path),
        )
        for entity in entity_list
    }


def _build_verify(
    verification: PublishVerification,
    *,
    run_id: str,
    stages: dict[str, EntityStage],
    transformed: dict[str, pd.DataFrame],
    ingestions: dict[str, Any],
    entity_list: list[str],
):
    """Build the callback that reconciles inside the publish transaction."""

    def verify(outcome: PublishOutcome) -> None:
        verification.entity_metrics = _entity_metrics_for_run(
            stages,
            transformed,
            outcome,
            ingestions,
            entity_list,
        )
        verification.reconciliation = build_reconciliation(
            run_id=run_id,
            entity_metrics=verification.entity_metrics,
            target_checks=outcome.target_checks,
        )
        if not verification.reconciliation["ok"]:
            raise ReconciliationError("Source accounting or target integrity reconciliation failed")

    return verify


def _write_success_artifacts(
    transformed: dict[str, pd.DataFrame],
    rejected_by_entity: dict[str, pd.DataFrame],
    duplicates_by_entity: dict[str, pd.DataFrame],
    reconciliation: dict[str, Any],
    outcome: PublishOutcome,
    run_id: str,
    monitor: PipelineMonitor,
    settings: PipelineSettings,
) -> dict[str, Any]:
    artifacts: dict[str, Any] = {}
    artifacts["processed"] = write_processed_snapshots(transformed, settings.processed_dir, run_id)
    artifacts["quarantine_csv"] = write_quarantine_csv(
        rejected_by_entity, settings.rejected_dir, run_id
    )
    artifacts["duplicates_csv"] = write_duplicates_csv(
        duplicates_by_entity, settings.rejected_dir, run_id
    )
    artifacts["reconciliation"] = write_reconciliation_report(
        reconciliation, settings.reports_dir, run_id
    )
    artifacts["dq_report"] = write_dq_report(run_id, monitor.entity_metrics, settings.reports_dir)
    artifacts["run_report"] = settings.reports_dir / f"run_report_{run_id}.json"
    artifacts["history_rows"] = outcome.history_written
    artifacts["quarantine_rows"] = outcome.quarantine_written
    return artifacts


def _record_failure(exc: BaseException, ctx: PipelineContext, state: PipelineState) -> RunResult:
    ctx.monitor.record_error(f"{type(exc).__name__}: {exc}")
    ctx.monitor.mark_failed(str(exc))
    logger.exception("Pipeline failed run_id=%s", ctx.run_id)

    # Preserve validation evidence even when the publish transaction failed.
    try:
        if state.rejected_by_entity:
            state.artifacts["quarantine_csv"] = write_quarantine_csv(
                state.rejected_by_entity, ctx.settings.rejected_dir, ctx.run_id
            )
        if state.duplicates_by_entity:
            state.artifacts["duplicates_csv"] = write_duplicates_csv(
                state.duplicates_by_entity, ctx.settings.rejected_dir, ctx.run_id
            )
    except Exception as artifact_exc:  # pragma: no cover - best effort
        logger.warning("Could not write failure artifacts: %s", artifact_exc)

    metrics = ctx.monitor.finish()
    try:
        save_run_metrics(metrics, ctx.engine)
    except Exception as metric_exc:  # pragma: no cover - best effort
        logger.error("Could not save failed run metrics: %s", metric_exc)
    state.artifacts["failure_report"] = write_failure_report(
        ctx.run_id,
        ctx.settings.reports_dir,
        exc,
        context={
            "source_dir": str(ctx.source_dir),
            "db_url": ctx.settings.db_url,
            "entity_metrics": ctx.monitor.entity_metrics,
        },
    )
    return RunResult(
        metrics=metrics,
        reconciliation={},
        artifacts=state.artifacts,
        error=f"{type(exc).__name__}: {exc}",
    )


def _ingest_stage(ctx: PipelineContext, state: PipelineState) -> None:
    state.ingestions = _ingest_sources(ctx.source_dir, ctx.run_id, ctx.entity_list, ctx.monitor)


def _validate_stage(ctx: PipelineContext, state: PipelineState) -> None:
    state.stages, state.duplicates_by_entity = _validate_and_deduplicate(
        state.ingestions, ctx.entity_list
    )


def _enforce_integrity_stage(ctx: PipelineContext, state: PipelineState) -> None:
    target_parent_keys = _fetch_target_parent_keys(ctx.engine, state.stages)
    logger.info(
        "Existing target parents customers=%d products=%d orders=%d",
        len(target_parent_keys["customers"]),
        len(target_parent_keys["products"]),
        len(target_parent_keys["orders"]),
    )
    state.final_frames = _apply_cross_entity_rules(state.stages, target_parent_keys, ctx.engine)


def _collect_evidence_stage(ctx: PipelineContext, state: PipelineState) -> None:
    state.rejected_by_entity = _build_rejection_evidence(
        state.stages,
        state.final_frames,
        state.duplicates_by_entity,
        ctx.entity_list,
    )


def _transform_stage(ctx: PipelineContext, state: PipelineState) -> None:
    state.transformed = _transform_and_validate(state.final_frames, ctx.entity_list)


def _publish_stage(ctx: PipelineContext, state: PipelineState) -> None:
    verify = _build_verify(
        state.verification,
        run_id=ctx.run_id,
        stages=state.stages,
        transformed=state.transformed,
        ingestions=state.ingestions,
        entity_list=ctx.entity_list,
    )
    state.outcome, state.attempts = _publish_with_retry(
        ctx.engine,
        state.transformed,
        state.rejected_by_entity,
        state.duplicates_by_entity,
        ctx.run_id,
        ctx.settings,
        ctx.monitor,
        verify=verify,
    )
    state.rejected_by_entity = _merge_publish_conflicts(
        state.rejected_by_entity, state.outcome, ctx.entity_list
    )

    # Monitor metrics accumulate only after the transaction has committed.
    for entity in ctx.entity_list:
        ctx.monitor.record_entity_metrics(entity, state.verification.entity_metrics[entity])
    state.reconciliation = state.verification.reconciliation


def _write_artifacts_stage(ctx: PipelineContext, state: PipelineState) -> RunResult:
    assert state.outcome is not None
    state.artifacts = _write_success_artifacts(
        state.transformed,
        state.rejected_by_entity,
        state.duplicates_by_entity,
        state.reconciliation,
        state.outcome,
        ctx.run_id,
        ctx.monitor,
        ctx.settings,
    )
    metrics = ctx.monitor.finalize_with_reconciliation(state.reconciliation)
    save_run_metrics(metrics, ctx.engine)
    state.artifacts["publish_attempts"] = state.attempts
    logger.info(
        "Pipeline complete run_id=%s status=%s published=%d rejected=%d",
        ctx.run_id,
        metrics["status"],
        metrics["published_rows"],
        metrics["rejected_rows"],
    )
    return RunResult(
        metrics=metrics,
        reconciliation=state.reconciliation,
        artifacts=state.artifacts,
    )


def run_pipeline(
    settings: PipelineSettings | None = None,
    run_id: str | None = None,
    source_dir: str | Path | None = None,
    entities: Iterable[str] | None = None,
) -> RunResult:
    """Run the complete pipeline and return metrics plus artifact paths."""

    settings = settings or default_settings()
    settings.ensure_directories()
    run_id = run_id or str(uuid.uuid4())
    source_dir = Path(source_dir) if source_dir else settings.raw_dir
    entity_list = list(entities) if entities else list(config.ENTITIES)
    configure_logging(settings.reports_dir / "pipeline.log", settings.log_level)
    monitor = PipelineMonitor(
        run_id=run_id,
        source_file=str(source_dir),
        reports_dir=settings.reports_dir,
    )
    monitor.logger.info(
        "Pipeline start run_id=%s source_dir=%s db_url=%s",
        run_id,
        source_dir,
        settings.db_url,
    )
    ctx = PipelineContext(
        settings=settings,
        run_id=run_id,
        source_dir=source_dir,
        entity_list=entity_list,
        monitor=monitor,
        engine=None,
    )
    state = PipelineState()

    try:
        ctx.engine = get_engine(settings.db_url)
        init_db(ctx.engine)
        _ingest_stage(ctx, state)
        _validate_stage(ctx, state)
        _enforce_integrity_stage(ctx, state)
        _collect_evidence_stage(ctx, state)
        _transform_stage(ctx, state)
        _publish_stage(ctx, state)
        return _write_artifacts_stage(ctx, state)
    except Exception as exc:
        return _record_failure(exc, ctx, state)
    except (KeyboardInterrupt, SystemExit) as exc:
        try:
            _record_failure(exc, ctx, state)
        finally:
            raise


def run_from_cli_paths(
    data_dir: str | Path,
    reports_dir: str | Path,
    db_path: str | Path,
    run_id: str | None = None,
) -> RunResult:
    """Convenience helper used by scripts and tests."""

    data_dir = Path(data_dir)
    settings = PipelineSettings(
        raw_dir=data_dir / "raw",
        processed_dir=data_dir / "processed",
        rejected_dir=data_dir / "rejected",
        expected_dir=data_dir / "expected",
        reports_dir=Path(reports_dir),
        db_url=f"sqlite:///{Path(db_path).as_posix()}",
    )
    return run_pipeline(settings=settings, run_id=run_id)
