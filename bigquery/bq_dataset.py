"""
BigQuery dataset and table management for capex_analytics.

Defines table schemas, creates/ensures tables exist, and provides
write helpers for the pipeline export step.

Dataset: mfg-eng-19197.capex_analytics
"""
from __future__ import annotations

import os

import pandas as pd
from google.cloud import bigquery

ANALYTICS_PROJECT = os.environ.get("BQ_ANALYTICS_PROJECT", "mfg-eng-19197")
ANALYTICS_DATASET = os.environ.get("BQ_ANALYTICS_DATASET", "capex_analytics")
FULL_DATASET_ID = f"{ANALYTICS_PROJECT}.{ANALYTICS_DATASET}"

QUERY_PROJECT = os.environ.get(
    "BQ_QUERY_PROJECT",
    os.environ.get("ODOO_SOURCE_PROJECT", "gtm-analytics-447201"),
)
ODOO_SOURCE_PROJECT = os.environ.get("ODOO_SOURCE_PROJECT", "gtm-analytics-447201")
ODOO_SOURCE_DATASET = os.environ.get("ODOO_SOURCE_DATASET", "odoo_public")

_service_bq_client: bigquery.Client | None = None
_source_bq_client: bigquery.Client | None = None


def _get_service_client() -> bigquery.Client:
    global _service_bq_client
    if _service_bq_client is None:
        _service_bq_client = bigquery.Client(project=ANALYTICS_PROJECT)
    return _service_bq_client


def _get_client() -> bigquery.Client:
    """Return BigQuery client scoped to the analytics project.

    For interactive web requests, prefer the signed-in user's Google credentials
    (when available). For jobs/scripts, fall back to service account credentials.
    """
    try:
        from user_google_auth import get_signed_in_user_credentials

        user_creds = get_signed_in_user_credentials()
        if user_creds is not None:
            return bigquery.Client(project=ANALYTICS_PROJECT, credentials=user_creds)
    except Exception:
        pass
    return _get_service_client()


def _get_source_service_client() -> bigquery.Client:
    """Return a service-account BigQuery client for Odoo source queries."""
    global _source_bq_client
    if _source_bq_client is None:
        _source_bq_client = bigquery.Client(project=QUERY_PROJECT)
    return _source_bq_client


def get_source_client(*, oauth_token: str | None = None) -> bigquery.Client:
    """Return a BigQuery client for querying the Odoo source project.

    When *oauth_token* is provided (e.g. from a signed-in dashboard user),
    a short-lived user-credential client is returned. Otherwise the cached
    service-account client is used.
    """
    token = (oauth_token or "").strip()
    if token:
        try:
            from google.oauth2.credentials import Credentials

            creds = Credentials(
                token=token,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            return bigquery.Client(project=QUERY_PROJECT, credentials=creds)
        except Exception:
            pass
    return _get_source_service_client()


def source_table(name: str) -> str:
    """Return a fully-qualified backtick-quoted BigQuery table reference."""
    return f"`{ODOO_SOURCE_PROJECT}.{ODOO_SOURCE_DATASET}.{name}`"


def run_source_query(sql: str) -> pd.DataFrame:
    """Execute SQL against the Odoo source project using service-account credentials."""
    client = _get_source_service_client()
    return client.query(sql).to_dataframe()


# ---------------------------------------------------------------------------
# Table schemas
# ---------------------------------------------------------------------------

_PO_LINES_SCHEMA = [
    bigquery.SchemaField("line_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("po_number", "STRING"),
    bigquery.SchemaField("po_id", "INTEGER"),
    bigquery.SchemaField("date_order", "STRING"),
    bigquery.SchemaField("po_state", "STRING"),
    bigquery.SchemaField("po_invoice_status", "STRING"),
    bigquery.SchemaField("po_receipt_status", "STRING"),
    bigquery.SchemaField("vendor_name", "STRING"),
    bigquery.SchemaField("vendor_ref", "STRING"),
    bigquery.SchemaField("product_category", "STRING"),
    bigquery.SchemaField("item_description", "STRING"),
    bigquery.SchemaField("is_capex", "BOOLEAN"),
    bigquery.SchemaField("station_id", "STRING"),
    bigquery.SchemaField("station_name", "STRING"),
    bigquery.SchemaField("mapping_confidence", "STRING"),
    bigquery.SchemaField("mapping_reason", "STRING"),
    bigquery.SchemaField("mapping_status", "STRING"),
    bigquery.SchemaField("mfg_subcategory", "STRING"),
    bigquery.SchemaField("subcat_confidence", "FLOAT64"),
    bigquery.SchemaField("subcat_reason", "STRING"),
    bigquery.SchemaField("is_mfg", "BOOLEAN"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("product_qty", "FLOAT64"),
    bigquery.SchemaField("qty_received", "FLOAT64"),
    bigquery.SchemaField("product_uom", "STRING"),
    bigquery.SchemaField("price_unit", "FLOAT64"),
    bigquery.SchemaField("price_subtotal", "FLOAT64"),
    bigquery.SchemaField("price_tax", "FLOAT64"),
    bigquery.SchemaField("price_total", "FLOAT64"),
    bigquery.SchemaField("bill_count", "INTEGER"),
    bigquery.SchemaField("bill_amount_total", "FLOAT64"),
    bigquery.SchemaField("bill_amount_paid", "FLOAT64"),
    bigquery.SchemaField("bill_amount_open", "FLOAT64"),
    bigquery.SchemaField("bill_payment_status", "STRING"),
    bigquery.SchemaField("project_name", "STRING"),
    bigquery.SchemaField("created_by_name", "STRING"),
    bigquery.SchemaField("po_amount_total", "FLOAT64"),
    bigquery.SchemaField("po_notes", "STRING"),
    bigquery.SchemaField("part_numbers", "STRING"),
    bigquery.SchemaField("line_type", "STRING"),
]

_STATION_SUMMARY_SCHEMA = [
    bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("station_name", "STRING"),
    bigquery.SchemaField("owner", "STRING"),
    bigquery.SchemaField("forecasted_cost", "FLOAT64"),
    bigquery.SchemaField("actual_spend", "FLOAT64"),
    bigquery.SchemaField("variance", "FLOAT64"),
    bigquery.SchemaField("variance_pct", "FLOAT64"),
    bigquery.SchemaField("odoo_spend", "FLOAT64"),
    bigquery.SchemaField("ramp_spend", "FLOAT64"),
    bigquery.SchemaField("manual_spend", "FLOAT64"),
    bigquery.SchemaField("actual_with_tax", "FLOAT64"),
    bigquery.SchemaField("line_count", "INTEGER"),
]

_SPARES_CATALOG_SCHEMA = [
    bigquery.SchemaField("item_description", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("product_category", "STRING"),
    bigquery.SchemaField("mfg_subcategory", "STRING"),
    bigquery.SchemaField("mfg_subcategories", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("vendor_names", "STRING"),
    bigquery.SchemaField("station_ids", "STRING"),
    bigquery.SchemaField("total_qty_ordered", "FLOAT64"),
    bigquery.SchemaField("avg_unit_price", "FLOAT64"),
    bigquery.SchemaField("total_spend", "FLOAT64"),
    bigquery.SchemaField("last_order_date", "STRING"),
    bigquery.SchemaField("part_numbers", "STRING"),
    bigquery.SchemaField("item_bucket", "STRING"),
    bigquery.SchemaField("po_or_contact", "STRING"),
]

_OVERRIDES_SCHEMA = [
    bigquery.SchemaField("line_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("override_type", "STRING"),
    bigquery.SchemaField("station_id", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("project_name", "STRING"),
    bigquery.SchemaField("forecasted_cost", "FLOAT64"),
    bigquery.SchemaField("updated_by", "STRING"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
]

_CLASSIFICATION_REVIEWS_SCHEMA = [
    bigquery.SchemaField("review_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("line_id", "STRING"),
    bigquery.SchemaField("po_number", "STRING"),
    bigquery.SchemaField("vendor_name", "STRING"),
    bigquery.SchemaField("item_description", "STRING"),
    bigquery.SchemaField("price_subtotal", "FLOAT64"),
    bigquery.SchemaField("project_name", "STRING"),
    bigquery.SchemaField("product_category", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("date_order", "STRING"),
    bigquery.SchemaField("rule_station", "STRING"),
    bigquery.SchemaField("rule_subcat", "STRING"),
    bigquery.SchemaField("rule_confidence", "FLOAT64"),
    bigquery.SchemaField("rule_mapping_status", "STRING"),
    bigquery.SchemaField("llm_station", "STRING"),
    bigquery.SchemaField("llm_subcat", "STRING"),
    bigquery.SchemaField("llm_confidence", "FLOAT64"),
    bigquery.SchemaField("llm_reasoning", "STRING"),
    bigquery.SchemaField("human_decision", "STRING"),
    bigquery.SchemaField("reviewed_by", "STRING"),
    bigquery.SchemaField("reviewed_at", "TIMESTAMP"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

_CLASSIFICATION_FEEDBACK_SCHEMA = [
    bigquery.SchemaField("feedback_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("line_id", "STRING"),
    bigquery.SchemaField("vendor_name", "STRING"),
    bigquery.SchemaField("item_description", "STRING"),
    bigquery.SchemaField("price_subtotal", "FLOAT64"),
    bigquery.SchemaField("final_station_id", "STRING"),
    bigquery.SchemaField("final_subcategory", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("created_by", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

_PAYMENTS_SCHEMA = [
    bigquery.SchemaField("po_number", "STRING"),
    bigquery.SchemaField("po_line_id", "STRING"),
    bigquery.SchemaField("vendor_name", "STRING"),
    bigquery.SchemaField("line_description", "STRING"),
    bigquery.SchemaField("line_amount", "FLOAT64"),
    bigquery.SchemaField("date_order", "STRING"),
    bigquery.SchemaField("payment_term_name", "STRING"),
    bigquery.SchemaField("bill_id", "STRING"),
    bigquery.SchemaField("bill_number", "STRING"),
    bigquery.SchemaField("bill_state", "STRING"),
    bigquery.SchemaField("bill_payment_state", "STRING"),
    bigquery.SchemaField("bill_posting_date", "STRING"),
    bigquery.SchemaField("bill_invoice_date", "STRING"),
    bigquery.SchemaField("bill_due_date", "STRING"),
    bigquery.SchemaField("bill_amount", "FLOAT64"),
    bigquery.SchemaField("bill_open_amount", "FLOAT64"),
    bigquery.SchemaField("payment_date", "STRING"),
    bigquery.SchemaField("payment_ref", "STRING"),
    bigquery.SchemaField("payment_amount", "FLOAT64"),
    bigquery.SchemaField("days_po_to_payment", "INTEGER"),
    bigquery.SchemaField("days_bill_to_payment", "INTEGER"),
    bigquery.SchemaField("computed_term_days", "INTEGER"),
]

_PAYMENT_MILESTONES_SCHEMA = [
    bigquery.SchemaField("milestone_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("po_number", "STRING"),
    bigquery.SchemaField("vendor_name", "STRING"),
    bigquery.SchemaField("station_id", "STRING"),
    bigquery.SchemaField("milestone_label", "STRING"),
    bigquery.SchemaField("expected_date", "DATE"),
    bigquery.SchemaField("expected_amount", "FLOAT64"),
    bigquery.SchemaField("actual_date", "DATE"),
    bigquery.SchemaField("actual_amount", "FLOAT64"),
    bigquery.SchemaField("template_id", "STRING"),
    bigquery.SchemaField("source", "STRING"),
]

_PAYMENT_TEMPLATES_SCHEMA = [
    bigquery.SchemaField("template_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("milestones_json", "STRING"),
    bigquery.SchemaField("vendor_name", "STRING"),
    bigquery.SchemaField("line_prefix", "STRING"),
    bigquery.SchemaField("created_by", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
]

_CASHFLOW_PROJECTIONS_SCHEMA = [
    bigquery.SchemaField("projection_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("po_number", "STRING"),
    bigquery.SchemaField("vendor_name", "STRING"),
    bigquery.SchemaField("station_id", "STRING"),
    bigquery.SchemaField("line", "STRING"),
    bigquery.SchemaField("milestone_label", "STRING"),
    bigquery.SchemaField("expected_date", "DATE"),
    bigquery.SchemaField("expected_amount", "FLOAT64"),
    bigquery.SchemaField("actual_date", "DATE"),
    bigquery.SchemaField("actual_amount", "FLOAT64"),
    bigquery.SchemaField("source", "STRING"),
]

TABLE_SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "po_lines": _PO_LINES_SCHEMA,
    "station_summary": _STATION_SUMMARY_SCHEMA,
    "spares_catalog": _SPARES_CATALOG_SCHEMA,
    "overrides": _OVERRIDES_SCHEMA,
    "classification_reviews": _CLASSIFICATION_REVIEWS_SCHEMA,
    "classification_feedback": _CLASSIFICATION_FEEDBACK_SCHEMA,
    "payments": _PAYMENTS_SCHEMA,
    "payment_milestones": _PAYMENT_MILESTONES_SCHEMA,
    "payment_templates": _PAYMENT_TEMPLATES_SCHEMA,
    "cashflow_projections": _CASHFLOW_PROJECTIONS_SCHEMA,
}


# ---------------------------------------------------------------------------
# Dataset + table lifecycle
# ---------------------------------------------------------------------------

def ensure_dataset() -> str:
    """Create the analytics dataset if it does not exist. Returns dataset ID."""
    client = _get_client()
    dataset_ref = bigquery.DatasetReference(ANALYTICS_PROJECT, ANALYTICS_DATASET)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)
    return FULL_DATASET_ID


def ensure_table(table_name: str) -> bigquery.Table:
    """Create a table if it does not exist. Returns the Table object."""
    if table_name not in TABLE_SCHEMAS:
        raise ValueError(f"Unknown table: {table_name}. Known: {sorted(TABLE_SCHEMAS)}")
    client = _get_client()
    table_id = f"{FULL_DATASET_ID}.{table_name}"
    table = bigquery.Table(table_id, schema=TABLE_SCHEMAS[table_name])
    return client.create_table(table, exists_ok=True)


def ensure_all_tables() -> list[str]:
    """Create dataset and all tables. Returns list of fully-qualified table IDs."""
    ensure_dataset()
    created = []
    for name in TABLE_SCHEMAS:
        tbl = ensure_table(name)
        created.append(tbl.full_table_id)
    return created


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def _coerce_types(df: pd.DataFrame, schema: list[bigquery.SchemaField]) -> pd.DataFrame:
    """Best-effort type coercion so load_table_from_dataframe doesn't choke."""
    df = df.copy()
    schema_map = {f.name: f.field_type for f in schema}

    for col in df.columns:
        expected = schema_map.get(col)
        if expected is None:
            continue
        if expected in ("FLOAT64", "FLOAT"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif expected in ("INTEGER", "INT64"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].where(df[col].isna(), df[col].astype("Int64"))
        elif expected == "BOOLEAN":
            df[col] = df[col].map(
                lambda v: True if str(v).lower() in ("true", "1", "yes") else
                (False if str(v).lower() in ("false", "0", "no", "") else None)
            )
        elif expected == "STRING":
            df[col] = df[col].fillna("").astype(str)
        elif expected == "TIMESTAMP":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif expected == "DATE":
            df[col] = pd.to_datetime(df[col], errors="coerce")
            if hasattr(df[col], "dt"):
                df[col] = df[col].dt.date

    return df


def write_table(
    table_name: str,
    df: pd.DataFrame,
    *,
    write_disposition: str = "WRITE_TRUNCATE",
) -> int:
    """Write a DataFrame to a BigQuery table.

    Args:
        table_name: Name within the capex_analytics dataset.
        df: Data to write.
        write_disposition: WRITE_TRUNCATE (replace) or WRITE_APPEND.

    Returns:
        Number of rows written.
    """
    if df.empty:
        return 0

    if table_name not in TABLE_SCHEMAS:
        raise ValueError(f"Unknown table: {table_name}")

    schema = TABLE_SCHEMAS[table_name]
    schema_cols = {f.name for f in schema}
    df_cols = set(df.columns)

    keep_cols = [c for c in df.columns if c in schema_cols]
    df = df[keep_cols]

    df = _coerce_types(df, schema)

    client = _get_client()
    table_id = f"{FULL_DATASET_ID}.{table_name}"
    ensure_table(table_name)

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    return job.output_rows or len(df)


def read_table(table_name: str, where: str = "") -> pd.DataFrame:
    """Read a full table or filtered subset from BigQuery."""
    client = _get_client()
    table_id = f"{FULL_DATASET_ID}.{table_name}"
    query = f"SELECT * FROM `{table_id}`"
    if where:
        query += f" WHERE {where}"
    return client.query(query).to_dataframe()


def run_query(sql: str) -> pd.DataFrame:
    """Execute arbitrary SQL against the analytics project."""
    client = _get_client()
    return client.query(sql).to_dataframe()
