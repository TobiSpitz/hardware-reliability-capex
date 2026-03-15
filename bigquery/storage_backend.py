"""
Storage backend that abstracts local filesystem vs Google Cloud Storage,
with optional BigQuery write-through for the capex_analytics dataset.

When the GCS_BUCKET environment variable is set, writes go to that GCS bucket.
Reads for core analytical CSV aliases (capex_clean/capex_by_station/spares_catalog)
can be served from BigQuery first, so Cloud Run dashboards use the DB as source
of truth while still keeping compatibility fallbacks.

When BQ_ANALYTICS_DATASET is set (or BigQuery helpers are called explicitly),
pipeline exports are also written to BigQuery tables via ``bq_dataset``.
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

_GCS_BUCKET: str = os.environ.get("GCS_BUCKET", "")
_LOCAL_DATA_DIR: Path = Path(__file__).resolve().parent / "data"

# Lazy-initialised GCS client (only imported when needed)
_gcs_client = None
_gcs_bucket_obj = None

_CSV_TO_BQ_TABLE: dict[str, str] = {
    "capex_clean.csv": "po_lines",
    "capex_by_station.csv": "station_summary",
    "spares_catalog.csv": "spares_catalog",
}


def _get_bucket():
    global _gcs_client, _gcs_bucket_obj
    if _gcs_bucket_obj is None:
        from google.cloud import storage as gcs
        _gcs_client = gcs.Client()
        _gcs_bucket_obj = _gcs_client.bucket(_GCS_BUCKET)
    return _gcs_bucket_obj


def is_remote() -> bool:
    return bool(_GCS_BUCKET)


def _truthy(raw: str | None) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _prefer_bq_for_mapped_csv_reads() -> bool:
    raw = os.environ.get("PREFER_BIGQUERY_MAPPED_CSV", "")
    if str(raw).strip():
        return _truthy(raw)
    # In Cloud Run/GCS mode, default to BigQuery-first for core datasets.
    return is_remote()


def _allow_mapped_csv_read_fallback() -> bool:
    raw = os.environ.get("ALLOW_MAPPED_CSV_FALLBACK", "")
    if str(raw).strip():
        return _truthy(raw)
    # Local dev defaults to fallback; cloud defaults to strict DB source-of-truth.
    return not is_remote()


def _write_mapped_csv_to_bigquery() -> bool:
    raw = os.environ.get("WRITE_MAPPED_CSV_TO_BIGQUERY", "")
    if str(raw).strip():
        return _truthy(raw)
    # In cloud, keep BQ in sync for mapped datasets.
    return is_remote()


def _write_mapped_csv_to_bigquery_strict() -> bool:
    raw = os.environ.get("WRITE_MAPPED_CSV_TO_BIGQUERY_STRICT", "")
    if str(raw).strip():
        return _truthy(raw)
    return is_remote()


def _fill_text_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize BigQuery extension dtypes for dashboard consumption.

    - String/object columns: fill NaN with ""
    - Nullable integer columns (Int64 etc.): convert to float64, fill NaN with 0
    - Float columns: fill NaN with 0 (prevents invalid JSON NaN)
    - Boolean columns: convert to object so NaN becomes "" not <NA>
    """
    if df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        dtype = out[col].dtype
        if pd.api.types.is_object_dtype(dtype) or pd.api.types.is_string_dtype(dtype):
            out[col] = out[col].fillna("")
        elif hasattr(dtype, "name") and dtype.name in (
            "Int8", "Int16", "Int32", "Int64",
            "UInt8", "UInt16", "UInt32", "UInt64",
        ):
            out[col] = out[col].astype("float64").fillna(0)
        elif pd.api.types.is_float_dtype(dtype):
            out[col] = out[col].fillna(0)
        elif hasattr(dtype, "name") and dtype.name == "boolean":
            out[col] = out[col].astype("object").fillna("")
    return out


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def read_csv(name: str) -> pd.DataFrame:
    """Read a CSV by filename (e.g. ``capex_clean.csv``)."""
    mapped_table = _CSV_TO_BQ_TABLE.get(name)
    if mapped_table and _prefer_bq_for_mapped_csv_reads():
        try:
            return _fill_text_nulls(read_from_bigquery(mapped_table))
        except Exception:
            if not _allow_mapped_csv_read_fallback():
                raise

    if is_remote():
        blob = _get_bucket().blob(name)
        if not blob.exists():
            return pd.DataFrame()
        content = blob.download_as_text(encoding="utf-8-sig")
        return pd.read_csv(io.StringIO(content), encoding="utf-8-sig").fillna("")
    else:
        path = _LOCAL_DATA_DIR / name
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path, encoding="utf-8-sig").fillna("")


def write_csv(name: str, df: pd.DataFrame) -> str:
    """Write a DataFrame as CSV. Returns the path or GCS URI written to."""
    if is_remote():
        blob = _get_bucket().blob(name)
        blob.upload_from_string(
            df.to_csv(index=False, encoding="utf-8-sig"),
            content_type="text/csv",
        )
        dest = f"gs://{_GCS_BUCKET}/{name}"
    else:
        _LOCAL_DATA_DIR.mkdir(exist_ok=True)
        path = _LOCAL_DATA_DIR / name
        df.to_csv(path, index=False, encoding="utf-8-sig")
        dest = str(path)

    if _CSV_TO_BQ_TABLE.get(name) and _write_mapped_csv_to_bigquery():
        try:
            write_to_bigquery(name, df, write_disposition="WRITE_TRUNCATE")
        except Exception:
            if _write_mapped_csv_to_bigquery_strict():
                raise

    return dest


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def read_json(name: str) -> Any:
    """Read a JSON file by name. Returns parsed object or empty dict/list."""
    if is_remote():
        blob = _get_bucket().blob(name)
        if not blob.exists():
            return {}
        content = blob.download_as_text(encoding="utf-8")
        return json.loads(content)
    else:
        path = _LOCAL_DATA_DIR / name
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as f:
            return json.load(f)


def write_json(name: str, data: Any) -> str:
    """Write a JSON-serialisable object. Returns path or GCS URI."""
    if is_remote():
        blob = _get_bucket().blob(name)
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type="application/json",
        )
        return f"gs://{_GCS_BUCKET}/{name}"
    else:
        _LOCAL_DATA_DIR.mkdir(exist_ok=True)
        path = _LOCAL_DATA_DIR / name
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return str(path)


def file_exists(name: str) -> bool:
    if is_remote():
        return _get_bucket().blob(name).exists()
    else:
        return (_LOCAL_DATA_DIR / name).exists()


def local_data_dir() -> Path:
    """Return the local data directory (for pipeline steps that need local temp files)."""
    _LOCAL_DATA_DIR.mkdir(exist_ok=True)
    return _LOCAL_DATA_DIR


# ---------------------------------------------------------------------------
# BigQuery helpers -- thin wrappers around bq_dataset for convenience
# ---------------------------------------------------------------------------

def write_to_bigquery(
    csv_name: str,
    df: pd.DataFrame,
    *,
    write_disposition: str = "WRITE_TRUNCATE",
) -> int:
    """Write a DataFrame to the BigQuery table that corresponds to a CSV name.

    Returns the number of rows written, or 0 if the CSV has no BQ mapping.
    """
    table_name = _CSV_TO_BQ_TABLE.get(csv_name)
    if table_name is None:
        return 0
    import bq_dataset
    return bq_dataset.write_table(table_name, df, write_disposition=write_disposition)


def read_from_bigquery(table_name: str, where: str = "") -> pd.DataFrame:
    """Read a table from the capex_analytics BigQuery dataset."""
    import bq_dataset
    return bq_dataset.read_table(table_name, where=where)


# ---------------------------------------------------------------------------
# Push local clean data to GCS (for use after a local pipeline run)
# ---------------------------------------------------------------------------

# Files produced by the pipeline that should be pushed to cloud for dashboard/review.
CLEAN_DATA_FILES: list[str] = [
    "capex_clean.csv",
    "capex_by_station.csv",
    "spares_catalog.csv",
    "bf1_stations.json",
    "forecast_overrides.json",
    "dashboard_settings.json",
    "station_overrides.json",
    "ramp_accounting.json",
]


def push_clean_data_to_gcs(bucket_name: str) -> list[str]:
    """Upload clean data files from local data/ to the given GCS bucket.

    Returns the list of gs:// URIs uploaded. Skips files that do not exist locally.
    """
    from google.cloud import storage as gcs

    client = gcs.Client()
    bucket = client.bucket(bucket_name)
    data_dir = local_data_dir()
    uploaded: list[str] = []

    for name in CLEAN_DATA_FILES:
        path = data_dir / name
        if not path.exists():
            continue
        blob = bucket.blob(name)
        content_type = "text/csv" if name.endswith(".csv") else "application/json"
        blob.upload_from_filename(str(path), content_type=content_type)
        uri = f"gs://{bucket_name}/{name}"
        uploaded.append(uri)

    return uploaded
