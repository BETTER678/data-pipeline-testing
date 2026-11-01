"""Central configuration for the e-commerce order pipeline.

All paths and business thresholds are defined here so that the pipeline can be
moved or embedded in tests without code changes.  No credentials or secrets are
used: the default target is a local SQLite database.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
REJECTED_DIR = DATA_DIR / "rejected"
EXPECTED_DIR = DATA_DIR / "expected"
REPORTS_DIR = PROJECT_ROOT / "reports"

PIPELINE_VERSION = "1.0.0"
DEFAULT_DB_FILENAME = "warehouse.db"

# ---------------------------------------------------------------------------
# Entities and keys
# ---------------------------------------------------------------------------
ENTITIES = ("customers", "products", "orders", "order_items", "payments")
PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "item_id",
    "payments": "payment_id",
}
FOREIGN_KEYS = {
    "orders": {"customer_id": ("customers", "customer_id")},
    "order_items": {
        "order_id": ("orders", "order_id"),
        "product_id": ("products", "product_id"),
    },
    "payments": {"order_id": ("orders", "order_id")},
}

# ---------------------------------------------------------------------------
# Business rules
# ---------------------------------------------------------------------------
MIN_QUANTITY = 1
MAX_QUANTITY = 10_000
MIN_UNIT_PRICE = 0.0
MAX_UNIT_PRICE = 100_000.0
MIN_AMOUNT_EXCLUSIVE = 0.0
MAX_AMOUNT = 1_000_000_000.0
MAX_ORDER_TOTAL = 1_000_000_000.0
MONEY_TOLERANCE_ABS = 0.01
PAYMENT_TOLERANCE_PCT = 0.01
ALLOWED_CURRENCIES = frozenset({"USD", "EUR", "GBP"})
ORDER_STATUSES = frozenset({"PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"})
PAYMENT_STATUSES = frozenset({"CAPTURED", "FAILED", "REFUNDED"})
PAYMENT_METHODS = frozenset({"CARD", "BANK_TRANSFER", "PAYPAL"})

# Columns added by ingestion.  They are lineage columns, not source business
# columns, and are permitted by schema validation.
INGEST_LINEAGE_COLUMNS = (
    "_source_file",
    "_source_row_number",
    "_run_id",
    "_ingested_at",
)
TARGET_LINEAGE_COLUMNS = (
    "_source_file",
    "_source_row_number",
    "_run_id",
    "_ingested_at",
    "_pipeline_version",
    "_record_hash",
)
REJECT_COLUMNS = ("_reject_rules", "_reject_reasons", "_rejected_at")


@dataclass(frozen=True)
class PipelineSettings:
    """Runtime paths and retry settings.

    ``db_url`` defaults to a SQLite file inside ``processed_dir``.  Tests pass a
    temporary directory and database URL to isolate every run.
    """

    raw_dir: Path = RAW_DIR
    processed_dir: Path = PROCESSED_DIR
    rejected_dir: Path = REJECTED_DIR
    reports_dir: Path = REPORTS_DIR
    expected_dir: Path = EXPECTED_DIR
    db_url: str = f"sqlite:///{(PROCESSED_DIR / DEFAULT_DB_FILENAME).as_posix()}"
    max_attempts: int = 3
    retry_base_delay: float = 0.2
    log_level: str = "INFO"
    extra: dict = field(default_factory=dict)

    @property
    def db_path(self) -> Path | None:
        if self.db_url.startswith("sqlite:///"):
            return Path(self.db_url.removeprefix("sqlite:///"))
        return None

    def ensure_directories(self) -> None:
        for directory in (
            self.raw_dir,
            self.processed_dir,
            self.rejected_dir,
            self.reports_dir,
            self.expected_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        if self.db_path is not None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)


def default_settings() -> PipelineSettings:
    """Build settings, allowing optional environment overrides."""

    data_dir = Path(os.getenv("PIPELINE_DATA_DIR", str(DATA_DIR)))
    reports_dir = Path(os.getenv("PIPELINE_REPORTS_DIR", str(REPORTS_DIR)))
    db_url = os.getenv(
        "PIPELINE_DB_URL",
        f"sqlite:///{(data_dir / 'processed' / DEFAULT_DB_FILENAME).as_posix()}",
    )
    return PipelineSettings(
        raw_dir=data_dir / "raw",
        processed_dir=data_dir / "processed",
        rejected_dir=data_dir / "rejected",
        expected_dir=data_dir / "expected",
        reports_dir=reports_dir,
        db_url=db_url,
        log_level=os.getenv("PIPELINE_LOG_LEVEL", "INFO"),
    )
