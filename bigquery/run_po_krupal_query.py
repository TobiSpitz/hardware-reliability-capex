"""
Run the "POs by Krupal Patel" BigQuery query and print or export results.
Uses Application Default Credentials (run: gcloud auth application-default login).
Exports cleaned/formatted CSV (project names, dates, numbers, no HTML in notes).
"""
from __future__ import annotations

import pathlib

import bq_dataset
from po_export_utils import clean_po_dataframe

SQL_FILE = pathlib.Path(__file__).resolve().parent / "po_by_krupal_patel.sql"


def main() -> None:
    odoo_ref = f"{bq_dataset.ODOO_SOURCE_PROJECT}.{bq_dataset.ODOO_SOURCE_DATASET}"
    client = bq_dataset.get_source_client()
    query_text = SQL_FILE.read_text(encoding="utf-8")
    query_text = "\n".join(
        line for line in query_text.splitlines()
        if not line.strip().startswith("--")
    ).strip().rstrip(";")
    query_text = query_text.replace("{odoo_source}", odoo_ref)

    print(f"Running PO-by-Krupal-Patel query on {odoo_ref}...")
    df = client.query(query_text).to_dataframe()
    print(f"Rows: {len(df)}")
    if df.empty:
        print("No POs found for Krupal Patel (check res_users/res_partner name match).")
        return
    df = clean_po_dataframe(df)
    print()
    print(df.to_string())

    out_csv = pathlib.Path(__file__).resolve().parent / "po_krupal_patel.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\nExported to: {out_csv}")


if __name__ == "__main__":
    main()
