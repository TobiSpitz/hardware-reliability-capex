"""
Fetch a single PO by number from BigQuery, clean/format, and export to CSV.
Usage: python run_po_by_number.py [PO_NUMBER]
Example: python run_po_by_number.py PO12060
"""
from __future__ import annotations

import pathlib
import sys

import bq_dataset
from po_export_utils import clean_po_dataframe

SQL_FILE = pathlib.Path(__file__).resolve().parent / "po_by_number.sql"


def main() -> None:
    po_number = (sys.argv[1] if len(sys.argv) > 1 else "PO12060").strip().upper()
    if not po_number.startswith("PO"):
        po_number = f"PO{po_number}"

    odoo_ref = f"{bq_dataset.ODOO_SOURCE_PROJECT}.{bq_dataset.ODOO_SOURCE_DATASET}"
    client = bq_dataset.get_source_client()
    query_text = SQL_FILE.read_text(encoding="utf-8")
    query_text = "\n".join(
        line for line in query_text.splitlines()
        if not line.strip().startswith("--")
    ).strip().rstrip(";")
    query_text = query_text.replace("{odoo_source}", odoo_ref)
    query_text = query_text.replace("'PO12060'", f"'{po_number}'")

    print(f"Fetching {po_number} from {odoo_ref}...")
    df = client.query(query_text).to_dataframe()
    if df.empty:
        print(f"No data found for {po_number}.")
        return

    print(f"Rows (lines): {len(df)}")
    df = clean_po_dataframe(df)
    out_csv = pathlib.Path(__file__).resolve().parent / f"po_{po_number.lower()}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"Exported to: {out_csv}")
    print()
    print(df.to_string())


if __name__ == "__main__":
    main()
