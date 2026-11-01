"""Deterministic normalization and derived fields.

The functions are pure: the input frame is never modified.  Timestamps are
normalized to UTC ISO-8601 with microseconds, text is trimmed, enums/currency
are upper-cased, emails are lower-cased, and derived money fields are rounded to
two decimals.
"""

from __future__ import annotations

import pandas as pd

from src.config import PIPELINE_VERSION
from src.deduplication import attach_record_hash
from src.monitoring import get_logger
from src.schemas import target_columns

logger = get_logger(__name__)


def _to_utc_iso(series: pd.Series) -> pd.Series:
    if len(series) == 0:
        return pd.Series(dtype="object")
    parsed = pd.to_datetime(series, format="ISO8601", utc=True, errors="coerce")
    return parsed.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _clean_text(series: pd.Series, *, lower: bool = False, upper: bool = False) -> pd.Series:
    def clean(value):
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        text = str(value).strip()
        if lower:
            return text.lower()
        if upper:
            return text.upper()
        return text

    return series.map(clean).astype(object)


def transform_customers(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["customer_id"] = _clean_text(out["customer_id"])
    out["name"] = _clean_text(out["name"])
    out["email"] = _clean_text(out["email"], lower=True)
    out["country"] = _clean_text(out["country"], upper=True)
    out["created_at"] = _to_utc_iso(out["created_at"])
    out["updated_at"] = _to_utc_iso(out["updated_at"])
    return out


def transform_products(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["product_id"] = _clean_text(out["product_id"])
    out["name"] = _clean_text(out["name"])
    out["category"] = _clean_text(out["category"])
    out["currency"] = _clean_text(out["currency"], upper=True)
    out["unit_price"] = pd.to_numeric(out["unit_price"], errors="raise").astype(float).round(2)
    out["updated_at"] = _to_utc_iso(out["updated_at"])
    return out


def transform_orders(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["order_id"] = _clean_text(out["order_id"])
    out["customer_id"] = _clean_text(out["customer_id"])
    out["status"] = _clean_text(out["status"], upper=True)
    out["currency"] = _clean_text(out["currency"], upper=True)
    out["order_date"] = _to_utc_iso(out["order_date"])
    out["updated_at"] = _to_utc_iso(out["updated_at"])
    out["total_amount"] = pd.to_numeric(out["total_amount"], errors="raise").astype(float).round(2)
    parsed = pd.to_datetime(out["order_date"], format="ISO8601", utc=True, errors="coerce")
    out["order_year_month"] = parsed.dt.strftime("%Y-%m").astype(object)
    return out


def transform_order_items(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["item_id"] = _clean_text(out["item_id"])
    out["order_id"] = _clean_text(out["order_id"])
    out["product_id"] = _clean_text(out["product_id"])
    out["quantity"] = pd.to_numeric(out["quantity"], errors="raise").astype("int64")
    out["unit_price"] = pd.to_numeric(out["unit_price"], errors="raise").astype(float).round(2)
    out["line_total"] = (out["quantity"].astype(float) * out["unit_price"]).round(2)
    out["updated_at"] = _to_utc_iso(out["updated_at"])
    return out


def transform_payments(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["payment_id"] = _clean_text(out["payment_id"])
    out["order_id"] = _clean_text(out["order_id"])
    out["payment_method"] = _clean_text(out["payment_method"], upper=True)
    out["status"] = _clean_text(out["status"], upper=True)
    out["amount"] = pd.to_numeric(out["amount"], errors="raise").astype(float).round(2)
    out["payment_date"] = _to_utc_iso(out["payment_date"])
    out["updated_at"] = _to_utc_iso(out["updated_at"])
    return out


_TRANSFORMS = {
    "customers": transform_customers,
    "products": transform_products,
    "orders": transform_orders,
    "order_items": transform_order_items,
    "payments": transform_payments,
}


def transform(frame: pd.DataFrame, entity: str) -> pd.DataFrame:
    """Transform one accepted entity frame and attach lineage/record hash."""

    if entity not in _TRANSFORMS:
        raise ValueError(f"No transformation for entity: {entity!r}")

    out = _TRANSFORMS[entity](frame)
    out["_pipeline_version"] = PIPELINE_VERSION
    out = attach_record_hash(out, entity)

    # Stable, documented column order.  Any missing lineage column is a
    # programming error and is intentionally not silently invented.
    columns = [column for column in target_columns(entity) if column in out.columns]
    out = out[columns].copy()
    logger.debug("Transformed entity=%s rows=%d", entity, len(out))
    return out
