"""
CAPEX Pipeline -- single entry point for the entire data refresh cycle.

Usage:
    python capex_pipeline.py                    # full refresh (re-queries BigQuery)
    python capex_pipeline.py --incremental      # upsert: update financials, preserve classifications
    python capex_pipeline.py --skip-bq          # skip BigQuery, reprocess existing CSV
    python capex_pipeline.py --dashboard        # run pipeline then launch dashboard
    python capex_pipeline.py --review           # run pipeline then launch review UI
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd

import storage_backend as store
from mfg_subcategory import classify_dataframe as classify_mfg_subcategories
from po_export_utils import (
    apply_overrides,
    auto_map_stations,
    classify_item_bucket,
    classify_line_type,
    clean_po_dataframe,
    extract_deposit_info,
    extract_part_numbers,
    load_and_normalize_ramp,
    load_bf1_stations,
    merge_section_headers,
    split_product_category,
    tag_capex_flag,
)

import bq_dataset
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = store.local_data_dir()
SQL_FILE = BASE_DIR / "po_by_creators_last_7m.sql"
PAYMENT_SQL_FILE = BASE_DIR / "payment_details.sql"
RAMP_ODOO_SQL_FILE = BASE_DIR / "ramp_from_odoo.sql"
RAMP_CSV = BASE_DIR.parent / "Ramp" / "Ramp Data Andy Org.csv"
EXCEL_FILE = BASE_DIR.parent / "Gen3 Mfg BF1.xlsx"

COLUMNS_TO_DROP = [
    "date_approve", "project_analytic_id", "assigned_project_id",
    "dest_address_id", "origin", "currency_id", "company_id",
    "po_updated_date", "po_created_date",
]

# Default creator names (Andy Ross org). Overridden by dashboard_settings.json.
def _safe_fillna(df: pd.DataFrame, fill_value: str = "") -> pd.DataFrame:
    """Fill NaN only in string/object columns; leave typed columns (Int64, etc.) alone."""
    out = df.copy()
    for col in out.columns:
        dtype = out[col].dtype
        if pd.api.types.is_object_dtype(dtype) or pd.api.types.is_string_dtype(dtype):
            out[col] = out[col].fillna(fill_value)
        elif hasattr(dtype, "name") and dtype.name in ("Int8", "Int16", "Int32", "Int64",
                                                         "UInt8", "UInt16", "UInt32", "UInt64"):
            pass
        elif pd.api.types.is_bool_dtype(dtype):
            pass
        elif pd.api.types.is_float_dtype(dtype):
            pass
        else:
            try:
                out[col] = out[col].fillna(fill_value)
            except (ValueError, TypeError):
                out[col] = out[col].astype("object").where(out[col].notna(), fill_value)
    return out


DEFAULT_CREATOR_NAMES: list[str] = [
    "alex mitchell", "ali nik-ahd", "amber platt", "andy ross", "avi anklesaria",
    "benjamin munoz", "brandon dillard", "brian connellan", "callum marsh",
    "chris johnston", "christopher george", "christopher vega", "daleian gopee",
    "diya nair", "eduardo martinez v.", "edward pienkowski", "emerson walter",
    "eric martinez", "evan pickar", "ezra doron", "jamie steele mcdonald",
    "jens emil clausen", "jimmy kiel", "juan manrique", "kelsea allenbaugh",
    "krupal patel", "kyle morgan", "kyle wozniak", "loren grabowski", "luis gastelum",
    "maintenance bot", "markia darby", "mike webb", "rene santos", "reyes mata",
    "scott rossi", "vitor ayres", "zach patterson", "zack de la rosa anderson",
]


def _get_bigquery_client():
    """Build a BigQuery client for Odoo/source pulls.

    Delegates to ``bq_dataset.get_source_client`` which handles optional user
    OAuth (when injected by the dashboard refresh endpoint) and falls back to
    the service-account credentials.
    """
    token = str(os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN", "") or "").strip() or None
    return bq_dataset.get_source_client(oauth_token=token)


def _load_creator_names() -> list[str]:
    """Load PO creator names from settings, falling back to defaults."""
    settings = store.read_json("dashboard_settings.json")
    if isinstance(settings, dict) and settings.get("po_creator_names"):
        names = settings["po_creator_names"]
        if isinstance(names, list) and names:
            return [str(n).strip().lower() for n in names if str(n).strip()]
    return DEFAULT_CREATOR_NAMES


def _format_creator_names_sql(names: list[str]) -> str:
    """Format a list of names into a SQL IN-list string."""
    escaped = [n.replace("'", "''") for n in names]
    return ", ".join(f"'{name}'" for name in escaped)


DEFAULT_PROJECT_CODES: list[str] = [
    "BF1-Facilities and Infrastructure",
    "BF1-Inverter Line 1",
    "BF1-Maintenance and Spares",
    "BF1-Manufacturing IT Systems",
    "BF1-Module Line 1",
    "BF1-Module Line 2",
    "BF1-NPI & Pilot Equipment",
    "BF1-Prototype R&D Lines",
    "BF1-Quality Equipment",
    "BF1-Warehousing and Material Handling",
    "CIP-BF1-",
    "CIP-BF2-",
]


def _load_project_codes() -> list[str]:
    """Load CAPEX project codes from settings, falling back to defaults.

    Codes can be exact matches (e.g., 'BF1-Module Line 1') or prefixes
    (e.g., 'CIP-BF1-' / 'CIP-BF2-' match all CIP station codes).
    """
    settings = store.read_json("dashboard_settings.json")
    if isinstance(settings, dict) and settings.get("capex_project_codes"):
        codes = settings["capex_project_codes"]
        if isinstance(codes, list) and codes:
            return [str(c).strip() for c in codes if str(c).strip()]
    return DEFAULT_PROJECT_CODES


def _format_project_codes_sql(codes: list[str]) -> str:
    """Build a SQL WHERE clause fragment that matches project codes.

    Odoo stores project names as JSON: {"en_US": "CIP-BF2-MOD3-ST33000-03 : ..."}
    so we use LIKE patterns against the name field.
    """
    conditions = []
    for code in codes:
        escaped = code.replace("'", "''")
        conditions.append(f"LOWER(name) LIKE '%{escaped.lower()}%'")
    return " OR ".join(conditions) if conditions else "FALSE"


ENRICHMENT_COLUMNS: list[str] = [
    "station_id", "station_name", "mapping_confidence", "mapping_reason", "mapping_status",
    "mfg_subcategory", "subcat_confidence", "subcat_reason", "is_mfg",
    "is_capex", "line_type", "deposit_pct", "deposit_amount", "milestone_terms",
    "part_numbers",
]


def load_previous_enrichments() -> pd.DataFrame:
    """Load enrichment columns from the last pipeline export, keyed by line_id.

    Tries BigQuery po_lines table first, falls back to capex_clean.csv.
    Returns a DataFrame indexed by line_id with only enrichment columns.
    """
    prev = pd.DataFrame()
    try:
        prev = bq_dataset.read_table("po_lines")
    except Exception:
        pass

    if prev.empty:
        prev = store.read_csv("capex_clean.csv")

    if prev.empty or "line_id" not in prev.columns:
        return pd.DataFrame()

    keep = ["line_id"] + [c for c in ENRICHMENT_COLUMNS if c in prev.columns]
    prev = prev[keep].copy()
    for col in prev.columns:
        prev[col] = prev[col].astype("object")
    prev["line_id"] = prev["line_id"].astype(str).str.strip()
    prev = prev[prev["line_id"] != ""]
    prev = prev.drop_duplicates(subset=["line_id"], keep="last")
    return prev


def merge_with_enrichments(
    fresh: pd.DataFrame,
    enrichments: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Merge fresh Odoo/Ramp data with previous enrichments.

    For existing line_ids: source columns come from fresh, enrichment columns
    are carried forward from previous export.
    For new line_ids: enrichment columns are left blank (will be classified).

    Returns:
        (merged_df, stats) where stats has keys: new, updated, unchanged, removed.
    """
    if enrichments.empty or "line_id" not in fresh.columns:
        fresh["_is_new"] = True
        return fresh, {"new": len(fresh), "updated": 0, "unchanged": 0, "removed": 0}

    fresh = fresh.copy()
    fresh["line_id"] = fresh["line_id"].astype(str).str.strip()

    fresh_ids = set(fresh["line_id"]) - {""}
    prev_ids = set(enrichments["line_id"])

    new_ids = fresh_ids - prev_ids
    existing_ids = fresh_ids & prev_ids
    removed_ids = prev_ids - fresh_ids

    enrich_cols = [c for c in enrichments.columns if c != "line_id"]
    for col in enrich_cols:
        if col not in fresh.columns:
            fresh[col] = ""

    enrich_map = enrichments.set_index("line_id")

    updated_count = 0
    unchanged_count = 0

    for col in enrich_cols:
        fresh[col] = fresh[col].astype("object")

    for idx in fresh.index:
        lid = str(fresh.at[idx, "line_id"])
        if lid in existing_ids and lid in enrich_map.index:
            row_enrich = enrich_map.loc[lid]
            if isinstance(row_enrich, pd.DataFrame):
                row_enrich = row_enrich.iloc[0]
            for col in enrich_cols:
                val = row_enrich.get(col, "")
                if pd.notna(val) and str(val).strip():
                    fresh.at[idx, col] = val

    price_cols = ["price_subtotal", "product_qty", "po_state"]
    prev_financials = enrichments.copy()
    for pc in price_cols:
        if pc not in prev_financials.columns:
            prev_financials = pd.DataFrame()
            break

    fresh["_is_new"] = fresh["line_id"].apply(lambda lid: str(lid).strip() in new_ids or str(lid).strip() == "")

    for lid in existing_ids:
        mask = (fresh["line_id"].astype(str).str.strip() == lid) & (~fresh["_is_new"])
        if not mask.any():
            continue
        unchanged_count += int(mask.sum())

    updated_count = len(existing_ids)
    unchanged_count = 0

    stats = {
        "new": len(new_ids),
        "updated": updated_count,
        "unchanged": unchanged_count,
        "removed": len(removed_ids),
    }

    return fresh, stats


def _step(num: int | str, msg: str) -> None:
    print(f"\n{'='*60}")
    print(f"  Step {num}: {msg}")
    print(f"{'='*60}")


def _odoo_source_ref() -> str:
    return f"{bq_dataset.ODOO_SOURCE_PROJECT}.{bq_dataset.ODOO_SOURCE_DATASET}"


def _read_sql_template(path: Path) -> str:
    sql = path.read_text(encoding="utf-8")
    return "\n".join(
        line for line in sql.splitlines()
        if not line.strip().startswith("--")
    ).strip().rstrip(";")


def _render_sql(path: Path, replacements: dict[str, str] | None = None) -> str:
    sql = _read_sql_template(path)
    mapping = {"odoo_source": _odoo_source_ref()}
    if replacements:
        mapping.update(replacements)
    for key, value in mapping.items():
        sql = sql.replace("{" + key + "}", str(value))
    return sql


RAMP_ACCOUNTING_SQL_TEMPLATE = """
SELECT
  payment_state,
  COUNT(*) AS entry_count,
  SUM(ABS(amount_total_signed)) AS total_amount,
  SUM(GREATEST(ABS(amount_total_signed) - ABS(amount_residual_signed), 0)) AS amount_paid,
  SUM(ABS(amount_residual_signed)) AS amount_open
FROM `{odoo_source}.account_move`
WHERE x_para_ramp_external_id IS NOT NULL
  AND move_type IN ('in_invoice', 'in_refund')
  AND IFNULL(_fivetran_deleted, FALSE) = FALSE
  AND state = 'posted'
GROUP BY payment_state
""".strip()


def step1_pull_bigquery() -> pd.DataFrame:
    """Pull fresh Odoo PO data from BigQuery."""
    _step(1, "Pull fresh Odoo data from BigQuery")

    creator_names = _load_creator_names()
    print(f"  Creator filter: {len(creator_names)} people")

    print(f"  Odoo source: {_odoo_source_ref()}")
    client = _get_bigquery_client()

    creator_sql = _format_creator_names_sql(creator_names)
    query_text = _render_sql(SQL_FILE, {"creator_names": creator_sql})

    print("  Executing query...")
    df = client.query(query_text).to_dataframe()
    print(f"  Rows pulled: {len(df)}")

    raw_path = store.write_csv("po_creators_last_7m.csv", df)
    print(f"  Saved raw: {raw_path}")

    _pull_ramp_accounting(client)
    _pull_payment_details(client)
    _pull_ramp_from_odoo(client)

    return df


def _pull_ramp_accounting(client: "bigquery.Client") -> None:
    """Pull Ramp-linked accounting entries from account_move and save summary."""
    print("  Pulling Ramp accounting data (account_move)...")
    rows = client.query(RAMP_ACCOUNTING_SQL_TEMPLATE.replace("{odoo_source}", _odoo_source_ref())).to_dataframe()

    summary: dict[str, float | int] = {
        "available": True,
        "total_entries": 0,
        "total_amount": 0.0,
        "amount_paid": 0.0,
        "amount_open": 0.0,
        "by_state": {},
    }
    for _, r in rows.iterrows():
        state = str(r["payment_state"] or "unknown")
        count = int(r["entry_count"])
        total = float(r["total_amount"] or 0)
        paid = float(r["amount_paid"] or 0)
        amount_open = float(r["amount_open"] or 0)
        summary["total_entries"] += count
        summary["total_amount"] += total
        summary["amount_paid"] += paid
        summary["amount_open"] += amount_open
        summary["by_state"][state] = {
            "count": count,
            "amount": total,
            "paid": paid,
            "open": amount_open,
        }

    total = summary["total_amount"]
    summary["paid_pct"] = round(summary["amount_paid"] / total * 100, 1) if total else 0.0

    dest = store.write_json("ramp_accounting.json", summary)
    print(f"  Ramp accounting: {summary['total_entries']} entries, "
          f"${summary['total_amount']:,.0f} total, "
          f"${summary['amount_paid']:,.0f} paid ({summary['paid_pct']}%), "
          f"${summary['amount_open']:,.0f} open -> {dest}")


def _pull_payment_details(client: "bigquery.Client") -> None:
    """Pull payment detail data (PO -> bill -> payment traces) and save locally.

    Scoped to the same creator list as the main PO query.
    """
    if not PAYMENT_SQL_FILE.exists():
        print("  Payment details SQL not found, skipping.")
        return
    print("  Pulling payment details...")
    try:
        creator_names = _load_creator_names()
        creator_sql = _format_creator_names_sql(creator_names)

        sql = _render_sql(PAYMENT_SQL_FILE, {"creator_names": creator_sql})

        df = client.query(sql).to_dataframe()
        dest = store.write_csv("payment_details.csv", df)
        print(f"  Payment details: {len(df)} rows (scoped to {len(creator_names)} buyers) -> {dest}")
    except Exception as exc:
        print(f"  WARNING: Payment details pull failed ({exc}). Continuing without it.")


def _pull_ramp_from_odoo(client: "bigquery.Client") -> None:
    """Pull Ramp credit card line-level detail from Odoo's account_move tables.

    Filtered by project codes matching the CAPEX dataset (from settings).
    """
    if not RAMP_ODOO_SQL_FILE.exists():
        print("  Ramp Odoo SQL not found, skipping.")
        return
    print("  Pulling Ramp transactions from Odoo (filtered by project codes)...")
    try:
        project_codes = _load_project_codes()
        project_filter_sql = _format_project_codes_sql(project_codes)
        print(f"  Project code filter: {len(project_codes)} codes")

        sql = _render_sql(RAMP_ODOO_SQL_FILE, {"project_code_filters": project_filter_sql})

        df = client.query(sql).to_dataframe()
        dest = store.write_csv("ramp_from_odoo.csv", df)
        print(f"  Ramp from Odoo: {len(df)} lines, "
              f"{df['vendor_name'].nunique()} vendors -> {dest}")
    except Exception as exc:
        print(f"  WARNING: Ramp Odoo pull failed ({exc}). Continuing with CSV fallback.")


def step1_load_existing() -> pd.DataFrame:
    """Load existing Odoo CSV (skip BigQuery)."""
    _step(1, "Load existing Odoo CSV (--skip-bq)")
    df = store.read_csv("po_creators_last_7m.csv")
    if df.empty:
        old_path = BASE_DIR / "po_creators_last_7m.csv"
        if old_path.exists():
            print(f"  Migrating from {old_path}")
            df = pd.read_csv(old_path, encoding="utf-8-sig")
            store.write_csv("po_creators_last_7m.csv", df)
            return df
        print("  ERROR: No existing CSV found. Run without --skip-bq first.")
        sys.exit(1)
    print(f"  Loaded: {len(df)} rows")
    return df


def step2_load_ramp() -> pd.DataFrame:
    """Load and normalize Ramp CC data from CSV."""
    _step(2, "Load + filter Ramp CSV")
    if not RAMP_CSV.exists():
        print(f"  WARNING: Ramp CSV not found at {RAMP_CSV}, skipping.")
        return pd.DataFrame()
    ramp = load_and_normalize_ramp(RAMP_CSV)
    print(f"  Ramp rows after filter: {len(ramp)}")
    return ramp


def step3_load_stations() -> tuple[list[dict], list[dict]]:
    """Load station master from planning Excel (BF1/BF2 when available)."""
    _step(3, "Load station master from Excel")
    if not EXCEL_FILE.exists():
        print(f"  WARNING: Excel not found at {EXCEL_FILE}.")
        cached = store.read_json("bf1_stations.json")
        if isinstance(cached, dict):
            stations = cached.get("stations", [])
            cost_breakdown = cached.get("cost_breakdown", [])
            if isinstance(stations, list) and isinstance(cost_breakdown, list) and stations:
                print(f"  Using cached bf1_stations.json ({len(stations)} stations, {len(cost_breakdown)} cost rows).")
                return stations, cost_breakdown
        print("  No cached station metadata found; station mapping and forecast seeding will be empty.")
        return [], []
    stations, cost_breakdown = load_bf1_stations(EXCEL_FILE)
    print(f"  Stations: {len(stations)}, Cost breakdown rows: {len(cost_breakdown)}")

    store.write_json("bf1_stations.json", {"stations": stations, "cost_breakdown": cost_breakdown})
    return stations, cost_breakdown


def step4_clean_odoo(df: pd.DataFrame) -> pd.DataFrame:
    """Clean Odoo data: format, split categories, merge headers, extract parts."""
    _step(4, "Clean Odoo (split categories, merge headers, extract part numbers)")
    df = clean_po_dataframe(df)
    for col in COLUMNS_TO_DROP:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    df["source"] = "odoo"
    df = split_product_category(df)
    df = merge_section_headers(df)
    df["part_numbers"] = df["item_description"].apply(extract_part_numbers)
    print(f"  Odoo rows after cleaning: {len(df)}")
    return df


def _load_payment_details_for_status() -> pd.DataFrame:
    """Load payment details used to compute PO-level payment status v2."""
    df = store.read_csv("payment_details.csv")
    if df.empty:
        return df

    for col in ("bill_amount", "bill_open_amount", "payment_amount", "line_amount"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    for col in ("po_number", "bill_id", "bill_payment_state", "payment_ref", "line_description", "payment_date"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    return df


def _compute_po_payment_status_v2(odoo_df: pd.DataFrame, payment_df: pd.DataFrame) -> pd.DataFrame:
    """Compute robust PO-level payment status from bill + payment evidence."""
    if odoo_df.empty or "po_number" not in odoo_df.columns:
        return pd.DataFrame()

    po_scope = odoo_df.copy()
    po_scope["po_number"] = po_scope["po_number"].fillna("").astype(str).str.strip()
    po_scope = po_scope[po_scope["po_number"] != ""]
    if po_scope.empty:
        return pd.DataFrame()

    po_scope["po_amount_total"] = pd.to_numeric(po_scope.get("po_amount_total", 0), errors="coerce").fillna(0.0)
    po_totals = (
        po_scope.groupby("po_number", as_index=False)["po_amount_total"]
        .max()
        .rename(columns={"po_amount_total": "po_total"})
    )

    if payment_df.empty or "po_number" not in payment_df.columns:
        out = po_totals.copy()
        out["po_payment_status_v2"] = "no_bill"
        out["bill_count_v2"] = 0
        out["bill_amount_total_v2"] = 0.0
        out["bill_amount_paid_v2"] = 0.0
        out["bill_amount_open_v2"] = 0.0
        out["payment_event_count_v2"] = 0
        out["has_unbilled_payment_signal"] = False
        out["has_deposit_signal"] = False
        out["payment_status_confidence"] = "low"
        out["payment_evidence_notes"] = "no payment_details evidence"
        return out

    pay = payment_df.copy()
    pay["po_number"] = pay["po_number"].fillna("").astype(str).str.strip()
    pay = pay[pay["po_number"] != ""]
    if pay.empty:
        return pd.DataFrame()

    bill_rows = pay[pay.get("bill_id", "").astype(str).str.strip() != ""].copy()
    if not bill_rows.empty:
        bill_rows = bill_rows.sort_values(["po_number", "bill_id", "bill_invoice_date"], na_position="last")
        bill_rows = bill_rows.drop_duplicates(subset=["po_number", "bill_id"], keep="last")

    payment_rows = pay[
        (pd.to_numeric(pay.get("payment_amount", 0), errors="coerce").fillna(0) > 0)
        | (pay.get("payment_date", "").astype(str).str.strip() != "")
    ].copy()
    if not payment_rows.empty:
        payment_rows["payment_amount"] = pd.to_numeric(payment_rows.get("payment_amount", 0), errors="coerce").fillna(0.0)
        payment_rows = payment_rows.drop_duplicates(
            subset=["po_number", "payment_date", "payment_amount", "payment_ref"],
            keep="last",
        )

    dep_re = re.compile(r"(deposit|down[\s-]?payment|advance|prepay)", re.IGNORECASE)
    text_dep = pay.apply(
        lambda r: bool(dep_re.search(str(r.get("line_description", "")))) or bool(dep_re.search(str(r.get("payment_ref", "")))),
        axis=1,
    )
    dep_signal = (
        pd.DataFrame({"po_number": pay["po_number"], "has_deposit_signal": text_dep})
        .groupby("po_number", as_index=False)["has_deposit_signal"]
        .max()
    )

    bills_agg = pd.DataFrame(columns=["po_number", "bill_count_v2", "bill_amount_total_v2", "bill_amount_open_v2", "_state_buckets"])
    if not bill_rows.empty:
        def _state_bucket(val: str) -> str:
            s = str(val or "").strip().lower()
            if s == "paid":
                return "paid"
            if s in {"partial", "in_payment"}:
                return "partial"
            if s in {"not_paid", "reversed"}:
                return "unpaid"
            return ""

        tmp = bill_rows.copy()
        tmp["_state_bucket"] = tmp.get("bill_payment_state", "").astype(str).apply(_state_bucket)
        bills_agg = tmp.groupby("po_number", as_index=False).agg(
            bill_count_v2=("bill_id", "nunique"),
            bill_amount_total_v2=("bill_amount", "sum"),
            bill_amount_open_v2=("bill_open_amount", "sum"),
            _state_buckets=("_state_bucket", lambda x: ",".join(sorted(set(v for v in x if v)))),
        )

    pay_agg = pd.DataFrame(columns=["po_number", "bill_amount_paid_v2", "payment_event_count_v2"])
    if not payment_rows.empty:
        pay_agg = payment_rows.groupby("po_number", as_index=False).agg(
            bill_amount_paid_v2=("payment_amount", "sum"),
            payment_event_count_v2=("payment_amount", "size"),
        )

    out = po_totals.merge(bills_agg, on="po_number", how="left").merge(pay_agg, on="po_number", how="left").merge(
        dep_signal, on="po_number", how="left"
    )
    for col in ("bill_count_v2", "payment_event_count_v2"):
        out[col] = pd.to_numeric(out.get(col, 0), errors="coerce").fillna(0).astype(int)
    for col in ("bill_amount_total_v2", "bill_amount_open_v2", "bill_amount_paid_v2", "po_total"):
        out[col] = pd.to_numeric(out.get(col, 0), errors="coerce").fillna(0.0)
    out["has_deposit_signal"] = out.get("has_deposit_signal", False).fillna(False).astype(bool)
    out["_state_buckets"] = out.get("_state_buckets", "").fillna("").astype(str)

    statuses: list[str] = []
    notes: list[str] = []
    confidence: list[str] = []
    unbilled_flags: list[bool] = []

    for _, row in out.iterrows():
        po_total = float(row.get("po_total", 0.0) or 0.0)
        billed_total = float(row.get("bill_amount_total_v2", 0.0) or 0.0)
        open_total = float(row.get("bill_amount_open_v2", 0.0) or 0.0)
        paid_total = float(row.get("bill_amount_paid_v2", 0.0) or 0.0)
        bill_count = int(row.get("bill_count_v2", 0) or 0)
        pay_count = int(row.get("payment_event_count_v2", 0) or 0)
        buckets = [b for b in str(row.get("_state_buckets", "")).split(",") if b]

        tol = max(1.0, po_total * 0.01) if po_total > 0 else 1.0
        fully_paid = po_total > 0 and paid_total > 0 and (paid_total + tol) >= po_total
        has_unbilled_payment_signal = paid_total > 0 and billed_total <= 0

        if fully_paid:
            status = "paid"
        elif paid_total > 0 and billed_total <= 0:
            status = "partial"
        elif paid_total > 0:
            status = "partial" if open_total > tol else "paid"
        elif billed_total > 0:
            if open_total > tol:
                status = "partial" if "partial" in buckets else "unpaid"
            else:
                status = "paid"
        else:
            status = "no_bill"

        if status not in {"paid", "no_bill"} and len(set(buckets)) >= 2:
            status = "mixed"

        evidence = []
        if bill_count:
            evidence.append(f"{bill_count} bill(s)")
        if pay_count:
            evidence.append(f"{pay_count} payment event(s)")
        if has_unbilled_payment_signal:
            evidence.append("payment without linked bill")
        if bool(row.get("has_deposit_signal", False)):
            evidence.append("deposit/advance text signal")
        notes.append("; ".join(evidence) if evidence else "no payment evidence")

        if fully_paid or bill_count > 0:
            conf = "high"
        elif has_unbilled_payment_signal or bool(row.get("has_deposit_signal", False)):
            conf = "medium"
        else:
            conf = "low"

        statuses.append(status)
        confidence.append(conf)
        unbilled_flags.append(bool(has_unbilled_payment_signal))

    out["po_payment_status_v2"] = statuses
    out["payment_status_confidence"] = confidence
    out["payment_evidence_notes"] = notes
    out["has_unbilled_payment_signal"] = unbilled_flags
    out.drop(columns=["_state_buckets"], inplace=True, errors="ignore")
    return out


def step4b_apply_payment_status_v2(odoo: pd.DataFrame, payment_df: pd.DataFrame) -> pd.DataFrame:
    """Apply PO-level payment status reconciliation onto Odoo rows."""
    _step("4b", "Compute PO payment status v2 (bill + payment evidence)")
    if odoo.empty or "po_number" not in odoo.columns:
        return odoo

    status_df = _compute_po_payment_status_v2(odoo, payment_df)
    if status_df.empty:
        print("  No payment evidence available; keeping original bill_payment_status.")
        return odoo

    merged = odoo.merge(status_df, on="po_number", how="left")
    if "po_payment_status_v2" in merged.columns:
        merged["bill_payment_status"] = merged["po_payment_status_v2"].fillna(
            merged.get("bill_payment_status", "")
        )

    status_counts = (
        merged[["po_number", "po_payment_status_v2"]]
        .drop_duplicates(subset=["po_number"])["po_payment_status_v2"]
        .value_counts(dropna=False)
        .to_dict()
    )
    print(f"  Payment status v2 applied to {len(status_df)} POs -> {status_counts}")
    return merged


def step5_normalize_ramp(ramp: pd.DataFrame) -> pd.DataFrame:
    """Ensure Ramp has all needed columns and part numbers."""
    _step(5, "Normalize Ramp into Odoo schema")
    if ramp.empty:
        return ramp
    ramp["part_numbers"] = "[]"
    ramp["line_type"] = "spend"
    print(f"  Ramp rows: {len(ramp)}")
    return ramp


def step6_concatenate(odoo: pd.DataFrame, ramp: pd.DataFrame) -> pd.DataFrame:
    """Concatenate Odoo + Ramp into unified DataFrame."""
    _step(6, "Concatenate Odoo + Ramp")
    for col in odoo.columns:
        if pd.api.types.is_integer_dtype(odoo[col]):
            odoo[col] = odoo[col].astype("object")
        elif pd.api.types.is_float_dtype(odoo[col]):
            odoo[col] = odoo[col].astype("object")
    unified = pd.concat([odoo, ramp], ignore_index=True, sort=False)
    for col in unified.columns:
        if pd.api.types.is_datetime64_any_dtype(unified[col]) or hasattr(unified[col], "dt"):
            unified[col] = unified[col].astype("object").where(unified[col].notna(), None)
    unified = _safe_fillna(unified, "")
    print(f"  Unified rows: {len(unified)} (Odoo: {len(odoo)}, Ramp: {len(ramp)})")
    return unified


def step7_map_stations(
    df: pd.DataFrame,
    stations: list[dict],
    cost_breakdown: list[dict],
    *,
    incremental: bool = False,
) -> pd.DataFrame:
    """Run 3-tier station mapping agent.

    In incremental mode, only processes rows where _is_new is True.
    """
    _step(7, "Run 3-tier station mapping agent" + (" (new lines only)" if incremental else ""))

    if incremental and "_is_new" in df.columns:
        new_mask = df["_is_new"].astype(bool)
        existing = df[~new_mask].copy()
        new_rows = df[new_mask].copy()
        print(f"  Incremental: {len(new_rows)} new lines to classify, {len(existing)} preserved")

        if new_rows.empty:
            print("  No new lines to classify.")
            return df

        new_rows = classify_line_type(new_rows)
        new_rows = tag_capex_flag(new_rows)
        if stations:
            new_rows = auto_map_stations(new_rows, stations, cost_breakdown)
        else:
            new_rows["station_id"] = ""
            new_rows["station_name"] = ""
            new_rows["mapping_confidence"] = "none"
            new_rows["mapping_reason"] = "no station data loaded"

        df = pd.concat([existing, new_rows], ignore_index=True, sort=False)
    else:
        df = classify_line_type(df)
        df = tag_capex_flag(df)
        if stations:
            df = auto_map_stations(df, stations, cost_breakdown)
        else:
            df["station_id"] = ""
            df["station_name"] = ""
            df["mapping_confidence"] = "none"
            df["mapping_reason"] = "no station data loaded"

    spend = df[df.get("line_type", pd.Series(dtype=str)).astype(str) == "spend"]
    for conf in ("high", "medium", "low", "none"):
        count = len(spend[spend.get("mapping_confidence", "").astype(str) == conf])
        sub = spend[spend.get("mapping_confidence", "").astype(str) == conf]["price_subtotal"]
        total = pd.to_numeric(sub, errors="coerce").sum()
        print(f"  {conf:>6}: {count:>5} lines  (${total:>14,.2f})")
    return df


def step8_apply_overrides(
    df: pd.DataFrame,
    stations: list[dict],
    *,
    incremental: bool = False,
) -> pd.DataFrame:
    """Apply human corrections from station_overrides.json.

    In incremental mode, only applies to new lines (existing lines already
    have their override-applied classifications preserved).
    """
    _step(8, "Apply station_overrides.json (human corrections)" + (" (new lines only)" if incremental else ""))
    overrides_dict = store.read_json("station_overrides.json")
    if not isinstance(overrides_dict, dict):
        overrides_dict = {}

    if incremental and "_is_new" in df.columns:
        new_mask = df["_is_new"].astype(bool)
        existing = df[~new_mask].copy()
        new_rows = df[new_mask].copy()

        if not new_rows.empty:
            new_rows = apply_overrides(new_rows, overrides_dict, stations)
        df = pd.concat([existing, new_rows], ignore_index=True, sort=False)
        print(f"  Applied overrides to {len(new_rows)} new lines ({len(existing)} preserved)")
    else:
        df = apply_overrides(df, overrides_dict, stations)
        n = len(overrides_dict)
        if n:
            print(f"  Applied {n} human overrides")
        else:
            print("  No overrides found (first run)")
    return df


def step9_classify_subcategories(df: pd.DataFrame, *, incremental: bool = False) -> pd.DataFrame:
    """Assign manufacturing sub-categories to each spend line.

    In incremental mode, only classifies new lines.
    """
    _step(9, "Classify manufacturing sub-categories" + (" (new lines only)" if incremental else ""))

    if incremental and "_is_new" in df.columns:
        new_mask = df["_is_new"].astype(bool)
        existing = df[~new_mask].copy()
        new_rows = df[new_mask].copy()
        print(f"  Incremental: {len(new_rows)} new lines to classify, {len(existing)} preserved")

        if not new_rows.empty:
            new_rows = classify_mfg_subcategories(new_rows)

        df = pd.concat([existing, new_rows], ignore_index=True, sort=False)
    else:
        df = classify_mfg_subcategories(df)

    spend = df[df.get("line_type", pd.Series(dtype=str)).astype(str) == "spend"]
    if "mfg_subcategory" in spend.columns:
        by_sc = spend.groupby("mfg_subcategory")["price_subtotal"].apply(
            lambda x: pd.to_numeric(x, errors="coerce").sum()
        ).sort_values(ascending=False)
        for sc, total in by_sc.items():
            count = len(spend[spend["mfg_subcategory"] == sc])
            print(f"  {sc:>40}: {count:>5} lines  (${total:>14,.2f})")

        mfg = spend[spend["is_mfg"] == True]
        mfg_total = pd.to_numeric(mfg["price_subtotal"], errors="coerce").sum()
        print(f"\n  Manufacturing spend: ${mfg_total:>14,.2f}")
    return df


def _load_existing_ramp_rows() -> pd.DataFrame:
    """Load existing ramp rows from the previous export so cloud refreshes don't lose them."""
    existing = pd.DataFrame()
    try:
        existing = bq_dataset.read_table("po_lines")
    except Exception:
        pass
    if existing.empty:
        existing = store.read_csv("capex_clean.csv")
    if existing.empty or "source" not in existing.columns:
        return pd.DataFrame()
    ramp = existing[existing["source"].astype(str) == "ramp"].copy()
    for col in ramp.columns:
        ramp[col] = ramp[col].astype("object")
    return _safe_fillna(ramp, "")


def _load_existing_manual_rows() -> pd.DataFrame:
    """Load existing manual rows from capex_clean.csv so re-exports preserve them."""
    existing = store.read_csv("capex_clean.csv")
    if existing.empty or "source" not in existing.columns:
        return pd.DataFrame()
    manual = existing[existing["source"].astype(str) == "manual"].copy()
    if "line_type" in manual.columns:
        manual = manual[manual["line_type"] == "spend"]
    return _safe_fillna(manual, "")


def _load_forecast_overrides() -> dict[str, float]:
    """Load forecast overrides keyed by normalized station_id."""
    raw = store.read_json("forecast_overrides.json")
    if not isinstance(raw, dict):
        return {}
    overrides: dict[str, float] = {}
    for sid, value in raw.items():
        sid_key = str(sid).strip().upper()
        if not sid_key:
            continue
        try:
            overrides[sid_key] = float(value)
        except (TypeError, ValueError):
            continue
    return overrides


def step10_export(df: pd.DataFrame, stations: list[dict], *, write_bq: bool = False) -> None:
    """Export all CSVs to data/ directory, optionally also to BigQuery."""
    _step(10, "Export all CSVs" + (" + BigQuery" if write_bq else ""))

    spend = df[df["line_type"] == "spend"].copy()

    confirmed_states = {"purchase", "sent"}
    if "po_state" in spend.columns:
        spend = spend[spend["po_state"].isin(confirmed_states) | (spend["source"] == "ramp")]

    manual_existing = _load_existing_manual_rows()
    if not manual_existing.empty:
        for col in manual_existing.columns:
            if col not in spend.columns:
                spend[col] = ""
        for col in spend.columns:
            if col not in manual_existing.columns:
                manual_existing[col] = ""
        spend = pd.concat([spend, manual_existing[spend.columns]], ignore_index=True, sort=False)
        if "line_id" in spend.columns:
            spend = spend.drop_duplicates(subset=["line_id"], keep="last")
        print(f"  Preserved manual rows: {len(manual_existing)}")

    col_order = [
        "source", "po_number", "date_order", "po_state", "po_invoice_status", "po_receipt_status",
        "vendor_name", "vendor_ref",
        "product_category", "item_description", "is_capex",
        "station_id", "station_name", "mapping_confidence", "mapping_reason", "mapping_status",
        "mfg_subcategory", "subcat_confidence", "subcat_reason", "is_mfg",
        "product_id", "product_qty", "qty_received", "product_uom",
        "price_unit", "price_subtotal", "price_tax", "price_total",
        "bill_count", "bill_amount_total", "bill_amount_paid", "bill_amount_open", "bill_payment_status",
        "po_payment_status_v2", "bill_count_v2", "bill_amount_total_v2", "bill_amount_paid_v2", "bill_amount_open_v2",
        "has_unbilled_payment_signal", "has_deposit_signal", "payment_status_confidence", "payment_evidence_notes",
        "project_name", "created_by_name",
        "po_amount_total", "po_notes", "part_numbers", "line_id",
    ]
    available = [c for c in col_order if c in spend.columns]
    extra = [c for c in spend.columns if c not in col_order]
    spend = spend[available + extra]

    clean_dest = store.write_csv("capex_clean.csv", spend)
    print(f"  capex_clean.csv: {len(spend)} rows -> {clean_dest}")

    # --- capex_by_station.csv ---
    station_name_map = {s["station_id"]: s["process_name"] for s in stations}
    station_owner_map = {s["station_id"]: s["owner"] for s in stations}
    station_forecast_map = {s["station_id"]: s["forecasted_cost"] for s in stations}

    mapped = spend[spend["station_id"] != ""].copy()
    mapped["_subtotal"] = pd.to_numeric(mapped["price_subtotal"], errors="coerce").fillna(0)
    mapped["_total"] = pd.to_numeric(mapped["price_total"], errors="coerce").fillna(0)

    if not mapped.empty:
        by_station = mapped.groupby("station_id").agg(
            line_count=("_subtotal", "size"),
            actual_spend=("_subtotal", "sum"),
            actual_with_tax=("_total", "sum"),
            odoo_spend=("_subtotal", lambda x: x[mapped.loc[x.index, "source"] == "odoo"].sum()),
            ramp_spend=("_subtotal", lambda x: x[mapped.loc[x.index, "source"] == "ramp"].sum()),
            manual_spend=("_subtotal", lambda x: x[mapped.loc[x.index, "source"] == "manual"].sum()),
        ).reset_index()
    else:
        by_station = pd.DataFrame(columns=[
            "station_id", "line_count", "actual_spend", "actual_with_tax",
            "odoo_spend", "ramp_spend", "manual_spend",
        ])

    all_sids = set(s["station_id"] for s in stations)
    mapped_sids = set(by_station["station_id"]) if not by_station.empty else set()
    missing = all_sids - mapped_sids
    if missing:
        missing_rows = pd.DataFrame([{
            "station_id": sid, "line_count": 0, "actual_spend": 0,
            "actual_with_tax": 0, "odoo_spend": 0, "ramp_spend": 0, "manual_spend": 0,
        } for sid in missing])
        by_station = pd.concat([by_station, missing_rows], ignore_index=True)

    existing_station_names = by_station.get("station_name", pd.Series([""] * len(by_station)))
    by_station["station_name"] = by_station["station_id"].map(station_name_map).fillna(existing_station_names).fillna("")
    by_station["owner"] = by_station["station_id"].map(station_owner_map).fillna("")
    by_station["forecasted_cost"] = by_station["station_id"].map(station_forecast_map).fillna(0)
    forecast_overrides = _load_forecast_overrides()
    if forecast_overrides:
        sid_keys = by_station["station_id"].fillna("").astype(str).str.strip().str.upper()
        applied_rows = 0
        for sid_key, override_value in forecast_overrides.items():
            mask = sid_keys == sid_key
            if mask.any():
                by_station.loc[mask, "forecasted_cost"] = override_value
                applied_rows += int(mask.sum())
        print(f"  Applied forecast overrides: {applied_rows}")
    by_station["variance"] = by_station["actual_spend"] - by_station["forecasted_cost"]
    by_station["variance_pct"] = (
        by_station["variance"] / by_station["forecasted_cost"].replace(0, float("nan")) * 100
    ).round(1).fillna(0)

    by_station = by_station.sort_values("station_id")
    col_order_station = [
        "station_id", "station_name", "owner",
        "forecasted_cost", "actual_spend", "variance", "variance_pct",
        "odoo_spend", "ramp_spend", "manual_spend", "actual_with_tax", "line_count",
    ]
    by_station = by_station[[c for c in col_order_station if c in by_station.columns]]

    station_dest = store.write_csv("capex_by_station.csv", by_station)
    print(f"  capex_by_station.csv: {len(by_station)} stations -> {station_dest}")

    # --- spares_catalog.csv (Odoo + Ramp) ---
    catalog_spend = spend[spend["item_description"] != ""].copy()
    catalog_spend["_subtotal"] = pd.to_numeric(catalog_spend["price_subtotal"], errors="coerce").fillna(0)
    catalog_spend["_qty"] = pd.to_numeric(catalog_spend["product_qty"], errors="coerce").fillna(0)
    catalog_spend["_unit"] = pd.to_numeric(catalog_spend["price_unit"], errors="coerce").fillna(0)

    def _po_or_contact(group: pd.DataFrame) -> str:
        """PO numbers for Odoo rows, contact names for Ramp rows."""
        odoo_pos = sorted(set(
            r["po_number"] for _, r in group.iterrows()
            if r["source"] == "odoo" and r["po_number"]
        ))
        ramp_contacts = sorted(set(
            r["created_by_name"] for _, r in group.iterrows()
            if r["source"] == "ramp" and r.get("created_by_name")
        ))
        parts = []
        if odoo_pos:
            parts.extend(odoo_pos)
        if ramp_contacts:
            parts.extend(f"(Ramp: {c})" for c in ramp_contacts)
        return ", ".join(parts)

    if not catalog_spend.empty:
        spares = catalog_spend.groupby("item_description").agg(
            product_category=("product_category", "first"),
            mfg_subcategory=("mfg_subcategory", "first"),
            mfg_subcategories=("mfg_subcategory", lambda x: ", ".join(sorted(set(s for s in x if str(s).strip())))),
            source=("source", lambda x: ", ".join(sorted(set(x)))),
            vendor_names=("vendor_name", lambda x: ", ".join(sorted(set(x)))),
            station_ids=("station_id", lambda x: ", ".join(sorted(set(s for s in x if s)))),
            total_qty_ordered=("_qty", "sum"),
            avg_unit_price=("_unit", "mean"),
            total_spend=("_subtotal", "sum"),
            last_order_date=("date_order", "max"),
            part_numbers=("part_numbers", "first"),
        ).reset_index()

        po_contact = catalog_spend.groupby("item_description").apply(
            _po_or_contact, include_groups=False,
        ).rename("po_or_contact")
        spares = spares.merge(po_contact, on="item_description", how="left")

        spares["avg_unit_price"] = spares["avg_unit_price"].round(2)
        spares["item_bucket"] = spares.apply(
            lambda r: classify_item_bucket(
                r["item_description"],
                r["product_category"],
                r["avg_unit_price"],
                r["total_spend"],
            ),
            axis=1,
        )
        spares = spares.sort_values("total_spend", ascending=False)

        bucket_counts = spares["item_bucket"].value_counts()
        for bucket, count in bucket_counts.items():
            bucket_spend = spares.loc[spares["item_bucket"] == bucket, "total_spend"].sum()
            print(f"    {bucket:>25}: {count:>4} items  (${bucket_spend:>14,.2f})")

        odoo_only = len(spares[spares["source"] == "odoo"])
        ramp_only = len(spares[spares["source"] == "ramp"])
        both = len(spares[spares["source"].str.contains(",")])
        print(f"    Sources: {odoo_only} odoo-only, {ramp_only} ramp-only, {both} both")
    else:
        spares = pd.DataFrame()

    spares_dest = store.write_csv("spares_catalog.csv", spares)
    print(f"  spares_catalog.csv: {len(spares)} items -> {spares_dest}")

    # --- BigQuery write-through (optional) ---
    if write_bq:
        _step(10, "Writing to BigQuery (capex_analytics)")
        try:
            bq_dataset.ensure_all_tables()

            n = store.write_to_bigquery("capex_clean.csv", spend)
            print(f"  po_lines: {n} rows")

            n = store.write_to_bigquery("capex_by_station.csv", by_station)
            print(f"  station_summary: {n} rows")

            if not spares.empty:
                n = store.write_to_bigquery("spares_catalog.csv", spares)
                print(f"  spares_catalog: {n} rows")

            payment_csv = store.read_csv("payment_details.csv")
            if not payment_csv.empty:
                n = bq_dataset.write_table("payments", payment_csv)
                print(f"  payments: {n} rows")

            print("  BigQuery write complete.")
        except Exception as exc:
            print(f"  WARNING: BigQuery write failed ({exc}). CSV export succeeded.")


def step11_summary(df: pd.DataFrame) -> None:
    """Print final summary statistics."""
    _step(11, "Summary")
    spend = df[df["line_type"] == "spend"]
    total_sub = pd.to_numeric(spend["price_subtotal"], errors="coerce").sum()

    odoo_count = len(spend[spend["source"] == "odoo"])
    ramp_count = len(spend[spend["source"] == "ramp"])
    manual_count = len(spend[spend["source"] == "manual"])

    auto_mapped = spend[spend["mapping_status"] == "auto"]
    confirmed = spend[spend["mapping_status"] == "confirmed"]
    unmapped = spend[spend["mapping_status"] == "unmapped"]
    non_prod = spend[spend["mapping_reason"].str.contains("non_prod|pilot_npi", case=False, na=False)]

    auto_spend = pd.to_numeric(auto_mapped["price_subtotal"], errors="coerce").sum()
    confirmed_spend = pd.to_numeric(confirmed["price_subtotal"], errors="coerce").sum()
    unmapped_spend = pd.to_numeric(unmapped["price_subtotal"], errors="coerce").sum()
    non_prod_spend = pd.to_numeric(non_prod["price_subtotal"], errors="coerce").sum()

    print(f"""
=== CAPEX Pipeline Complete ===
Total spend lines:    {len(spend):>6}  (${total_sub:>14,.2f})
  Odoo PO lines:      {odoo_count:>6}
  Ramp CC lines:       {ramp_count:>6}
  Manual PO lines:     {manual_count:>6}

Station mapping:
  auto-mapped:         {len(auto_mapped):>6}  (${auto_spend:>14,.2f})
  human-confirmed:     {len(confirmed):>6}  (${confirmed_spend:>14,.2f})
  needs review:        {len(unmapped):>6}  (${unmapped_spend:>14,.2f})
  non_prod/pilot:      {len(non_prod):>6}  (${non_prod_spend:>14,.2f})

Exported to: {DATA_DIR}/
  - capex_clean.csv
  - capex_by_station.csv
  - spares_catalog.csv
""")


def step6b_merge_enrichments(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Load previous enrichments and merge with fresh data."""
    _step("6b", "Merge with previous enrichments (incremental)")
    enrichments = load_previous_enrichments()
    if enrichments.empty:
        print("  No previous enrichments found. All lines will be classified as new.")
        df["_is_new"] = True
        return df, {"new": len(df), "updated": 0, "unchanged": 0, "removed": 0}
    print(f"  Previous enrichments: {len(enrichments)} line_ids loaded")
    merged, stats = merge_with_enrichments(df, enrichments)
    new_count = int(merged["_is_new"].sum()) if "_is_new" in merged.columns else 0
    existing_count = len(merged) - new_count
    print(f"  New lines: {stats['new']}")
    print(f"  Existing (financials updated, classifications preserved): {stats['updated']}")
    print(f"  Removed from Odoo (no longer in query): {stats['removed']}")
    return merged, stats


def main() -> None:
    parser = argparse.ArgumentParser(description="CAPEX Pipeline")
    parser.add_argument("--skip-bq", action="store_true",
                        help="Skip BigQuery pull, reprocess existing CSV")
    parser.add_argument("--incremental", action="store_true",
                        help="Upsert: update financials from Odoo, preserve existing classifications")
    parser.add_argument("--write-bq", action="store_true",
                        help="Also write clean data to BigQuery capex_analytics dataset")
    parser.add_argument("--dashboard", action="store_true",
                        help="Launch dashboard after pipeline")
    parser.add_argument("--review", action="store_true",
                        help="Launch review UI after pipeline")
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)
    incremental = args.incremental

    if args.skip_bq:
        odoo_raw = step1_load_existing()
    else:
        odoo_raw = step1_pull_bigquery()

    ramp_raw = step2_load_ramp()
    stations, cost_breakdown = step3_load_stations()
    payment_detail_df = _load_payment_details_for_status()
    odoo = step4_clean_odoo(odoo_raw)
    odoo = step4b_apply_payment_status_v2(odoo, payment_detail_df)
    ramp = step5_normalize_ramp(ramp_raw)

    if ramp.empty and incremental:
        print("\n  WARNING: No fresh Ramp data available. Preserving existing Ramp rows from previous export.")
        existing_ramp = _load_existing_ramp_rows()
        if not existing_ramp.empty:
            ramp = existing_ramp
            print(f"  Carried forward {len(ramp)} existing Ramp rows.")

    unified = step6_concatenate(odoo, ramp)

    merge_stats: dict[str, int] = {}
    if incremental:
        unified, merge_stats = step6b_merge_enrichments(unified)

    unified = step7_map_stations(unified, stations, cost_breakdown, incremental=incremental)
    unified = step8_apply_overrides(unified, stations, incremental=incremental)
    unified = step9_classify_subcategories(unified, incremental=incremental)

    if "_is_new" in unified.columns:
        unified.drop(columns=["_is_new"], inplace=True)

    step10_export(unified, stations, write_bq=args.write_bq)
    step11_summary(unified)

    if incremental and merge_stats:
        print(f"\n  Incremental sync: {merge_stats['new']} new, "
              f"{merge_stats['updated']} updated, "
              f"{merge_stats['removed']} removed from Odoo")

    if args.dashboard:
        print("Launching dashboard on http://localhost:5050 ...")
        subprocess.Popen(
            [sys.executable, str(BASE_DIR / "capex_dashboard.py")],
            cwd=str(BASE_DIR),
        )
    if args.review:
        print("Launching review UI on http://localhost:5051 ...")
        subprocess.Popen(
            [sys.executable, str(BASE_DIR / "station_review_app.py")],
            cwd=str(BASE_DIR),
        )


if __name__ == "__main__":
    main()
