"""
Fetch POs created by listed team members in the last 7 months.
Cleans data and exports CSV without: date_approve, project_analytic_id,
assigned_project_id, dest_address_id, origin, currency_id, company_id,
po_updated_date, po_created_date.
"""
from __future__ import annotations

import pathlib

import bq_dataset

import storage_backend as store
from capex_pipeline import DEFAULT_CREATOR_NAMES
from po_export_utils import clean_po_dataframe

SQL_FILE = pathlib.Path(__file__).resolve().parent / "po_by_creators_last_7m.sql"
OUT_CSV = pathlib.Path(__file__).resolve().parent / "po_creators_last_7m.csv"

COLUMNS_TO_DROP = [
    "date_approve",
    "project_analytic_id",
    "assigned_project_id",
    "dest_address_id",
    "origin",
    "currency_id",
    "company_id",
    "po_updated_date",
    "po_created_date",
]


def _load_creator_names() -> list[str]:
    settings = store.read_json("dashboard_settings.json")
    if isinstance(settings, dict) and isinstance(settings.get("po_creator_names"), list):
        names = [str(v).strip().lower() for v in settings.get("po_creator_names", []) if str(v).strip()]
        if names:
            return names
    return DEFAULT_CREATOR_NAMES


def _format_creator_names_sql(names: list[str]) -> str:
    escaped = [n.replace("'", "''") for n in names]
    return ", ".join(f"'{name}'" for name in escaped)


def main() -> None:
    creator_names = _load_creator_names()
    creator_sql = _format_creator_names_sql(creator_names)

    odoo_ref = f"{bq_dataset.ODOO_SOURCE_PROJECT}.{bq_dataset.ODOO_SOURCE_DATASET}"
    client = bq_dataset.get_source_client()
    query_text = SQL_FILE.read_text(encoding="utf-8")
    query_text = "\n".join(
        line for line in query_text.splitlines()
        if not line.strip().startswith("--")
    ).strip().rstrip(";")
    query_text = query_text.replace("{odoo_source}", odoo_ref)
    query_text = query_text.replace("{creator_names}", creator_sql)

    print(f"Running POs by creators on {odoo_ref}...")
    print(f"Creator filter size: {len(creator_names)}")
    df = client.query(query_text).to_dataframe()
    print(f"Rows: {len(df)}")
    if df.empty:
        print("No POs found.")
        return

    df = clean_po_dataframe(df)
    for col in COLUMNS_TO_DROP:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"Exported to: {OUT_CSV}")
    print("Columns:", list(df.columns))


if __name__ == "__main__":
    main()
