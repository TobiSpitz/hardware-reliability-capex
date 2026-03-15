"""
CAPEX Analytics Dashboard -- Flask app with Plotly.js charts and DataTables.

Run: python capex_dashboard.py
Open: http://localhost:5050
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from threading import Lock

import pandas as pd
from flask import Flask, jsonify, render_template_string, request

import storage_backend as store
from access_control import (
    current_user_email,
    EDITORS_KEY,
    OWNER_KEY,
    RESTRICT_KEY,
    ensure_access_defaults,
    get_access_context,
    is_company_email,
    load_settings_with_access_defaults,
    normalize_email,
    normalize_email_list,
)
from auth import get_google_access_token, init_auth

app = Flask(__name__)
init_auth(app)


# ---------------------------------------------------------------------------
# Data loading -- delegates to storage_backend (local or GCS)
# ---------------------------------------------------------------------------

_CSV_CACHE_TTL_SEC = float(
    os.environ.get("CSV_CACHE_TTL_SEC", "20" if store.is_remote() else "0")
)
_CSV_CACHE: dict[str, tuple[float, pd.DataFrame]] = {}
_CSV_CACHE_LOCK = Lock()


def _load_csv(name: str) -> pd.DataFrame:
    if _CSV_CACHE_TTL_SEC <= 0:
        return store.read_csv(name)

    now = time.time()
    with _CSV_CACHE_LOCK:
        cached = _CSV_CACHE.get(name)
        if cached and (now - cached[0]) <= _CSV_CACHE_TTL_SEC:
            return cached[1].copy(deep=True)

    fresh = store.read_csv(name)
    with _CSV_CACHE_LOCK:
        _CSV_CACHE[name] = (now, fresh)
    return fresh.copy(deep=True)


def _load_stations_json() -> list[dict]:
    data = store.read_json("bf1_stations.json")
    return data.get("stations", []) if isinstance(data, dict) else []


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

import re as _re

_CELL_TO_MOD = {
    "CELL1": "MOD1",
    "CELL2": "MOD2",
    "CELL3": "MOD3",
}


def _extract_mod(sid: str) -> str:
    """Raw module extraction -- CELL stays as CELL."""
    m = _re.match(r"(BASE\d+)-(MOD\d+|CELL\d+|INV\d+)", str(sid))
    return f"{m.group(1)}-{m.group(2)}" if m else ""


def _extract_line(sid: str) -> str:
    """Nest CELLs under their parent MOD for budget/line grouping.

    BASE1-CELL1 → BASE1-MOD1, BASE1-CELL2 → BASE1-MOD2, etc.
    INV stays as-is.
    """
    m = _re.match(r"(BASE\d+)-(MOD\d+|CELL\d+|INV\d+)", str(sid))
    if not m:
        return ""
    base, unit = m.group(1), m.group(2)
    parent = _CELL_TO_MOD.get(unit, unit)
    return f"{base}-{parent}"


# Sentinel labels for non-production buckets in the line filter (must match URL param).
LINE_PILOT_NPI = "Pilot / NPI"
LINE_NON_PROD = "Non-Prod"
LINE_UNMAPPED = "Needs review"


def _apply_line_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Filter rows by the ?lines= query param (comma-separated line list).

    Uses _extract_line so filtering by BASE1-MOD1 also includes BASE1-CELL1.
    Includes Pilot/NPI, Non-Prod, and Needs review as selectable lines.
    """
    raw = request.args.get("lines", "")
    if not raw:
        return df
    allowed = {s.strip() for s in raw.split(",") if s.strip()}
    if not allowed:
        return df
    df = df.copy()
    reason = df.get("mapping_reason", pd.Series([""] * len(df))).fillna("").astype(str)
    status = df.get("mapping_status", pd.Series([""] * len(df))).fillna("").astype(str)
    lines = []
    for idx in df.index:
        sid = str(df.at[idx, "station_id"])
        ln = _extract_line(sid)
        if ln:
            lines.append(ln)
        elif status.at[idx] == "pilot_npi" or "pilot_npi" in reason.at[idx]:
            lines.append(LINE_PILOT_NPI)
        elif status.at[idx] == "non_prod" or "non_prod" in reason.at[idx]:
            lines.append(LINE_NON_PROD)
        else:
            lines.append(LINE_UNMAPPED)
    df["_line"] = lines
    return df[df["_line"].isin(allowed)].drop(columns=["_line"])


def _all_lines(df: pd.DataFrame) -> list[str]:
    """Unique lines for the filter: production lines (CELLs rolled into MODs) plus Pilot/NPI, Non-Prod, Needs review.
    Order: non-BASE first (Pilot/NPI, Non-Prod, Needs review), then sorted BASE* lines.
    """
    production = sorted({_extract_line(str(sid)) for sid in df["station_id"] if _extract_line(str(sid))})
    reason = df.get("mapping_reason", pd.Series([""] * len(df))).fillna("").astype(str)
    status = df.get("mapping_status", pd.Series([""] * len(df))).fillna("").astype(str)
    empty_station = df["station_id"].fillna("").astype(str).str.strip() == ""
    has_pilot = ((status == "pilot_npi") | reason.str.contains("pilot_npi", na=False)).any()
    has_non_prod = ((status == "non_prod") | reason.str.contains("non_prod", na=False)).any()
    has_unmapped = (empty_station & (status != "pilot_npi") & ~reason.str.contains("pilot_npi", na=False)
                   & (status != "non_prod") & ~reason.str.contains("non_prod", na=False)).any()
    result = []
    if has_pilot:
        result.append(LINE_PILOT_NPI)
    if has_non_prod:
        result.append(LINE_NON_PROD)
    if has_unmapped:
        result.append(LINE_UNMAPPED)
    result.extend(production)
    return result


DEFAULT_BF1_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1FEEF_ev62ttt-SRZAIG3i824_jHyySuPN0IE_6BNWb0/"
    "edit?gid=657859777#gid=657859777"
)
DEFAULT_BF2_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1ngtchHeES3R3QmffpizlD8l4j3U8LtJO8BYTClqvq3E/"
    "edit?gid=657859777#gid=657859777"
)
RFQ_SYSTEM_PROMPT_TEMPLATE = Path(__file__).resolve().parent / "prompts" / "rfq_system.txt"


def _build_forecasting_rows(by_station: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """Build grouped station forecast rows for the Forecasting tab."""
    if by_station.empty:
        return [], []
    work = by_station.copy()
    if "station_id" not in work.columns:
        return [], []

    work["station_id"] = work["station_id"].fillna("").astype(str).str.strip()
    work = work[work["station_id"] != ""]
    if work.empty:
        return [], []

    work["_line"] = work["station_id"].apply(_extract_line)
    work["_line"] = work["_line"].replace("", "Other")
    work["forecasted_cost"] = pd.to_numeric(work.get("forecasted_cost", 0), errors="coerce").fillna(0.0)
    work["actual_spend"] = pd.to_numeric(work.get("actual_spend", 0), errors="coerce").fillna(0.0)
    work["variance"] = work["actual_spend"] - work["forecasted_cost"]
    work["_base"] = work["station_id"].str.extract(r"^(BASE\d+)-", expand=False).fillna("")

    raw_lines = request.args.get("lines", "")
    if raw_lines:
        allowed = {s.strip() for s in raw_lines.split(",") if s.strip()}
        work = work[work["_line"].isin(allowed)]

    overrides = store.read_json("forecast_overrides.json")
    locked_keys = {str(k).strip().upper() for k in (overrides or {})} if isinstance(overrides, dict) else set()

    work = work.sort_values(["_line", "station_id"])
    rows = [{
        "line": str(r["_line"]),
        "base": str(r["_base"]),
        "station_id": str(r["station_id"]),
        "station_name": str(r.get("station_name", "")),
        "owner": str(r.get("owner", "")),
        "forecasted_cost": float(r["forecasted_cost"]),
        "actual_spend": float(r["actual_spend"]),
        "variance": float(r["variance"]),
        "is_locked": str(r["station_id"]).strip().upper() in locked_keys,
    } for _, r in work.iterrows()]

    grp = work.groupby("_line").agg(
        station_count=("station_id", "size"),
        total_forecast=("forecasted_cost", "sum"),
        total_actual=("actual_spend", "sum"),
    ).reset_index().sort_values("_line")
    groups = [{
        "line": str(r["_line"]),
        "station_count": int(r["station_count"]),
        "total_forecast": float(r["total_forecast"]),
        "total_actual": float(r["total_actual"]),
        "total_variance": float(r["total_actual"] - r["total_forecast"]),
    } for _, r in grp.iterrows()]
    return rows, groups


def _apply_forecast_updates(
    update_values: dict[str, float],
    *,
    update_overrides: bool = True,
    locked_station_ids: set[str] | None = None,
) -> dict[str, object]:
    """Persist forecast updates into overrides JSON and capex_by_station.csv."""
    if not update_values:
        return {
            "updated_count": 0,
            "updated_station_ids": [],
            "unmatched_station_ids": [],
            "locked_skipped_station_ids": [],
        }

    by_station = _load_csv("capex_by_station.csv")
    if by_station.empty or "station_id" not in by_station.columns:
        unmatched = sorted({str(s).strip().upper() for s in update_values.keys() if str(s).strip()})
        return {
            "updated_count": 0,
            "updated_station_ids": [],
            "unmatched_station_ids": unmatched,
            "locked_skipped_station_ids": [],
        }

    station_lookup: dict[str, list[int]] = {}
    station_display: dict[str, str] = {}
    for idx, sid in by_station["station_id"].items():
        sid_str = str(sid).strip()
        sid_key = sid_str.upper()
        if not sid_key:
            continue
        station_lookup.setdefault(sid_key, []).append(idx)
        station_display[sid_key] = sid_str

    locked_keys = {str(s).strip().upper() for s in (locked_station_ids or set()) if str(s).strip()}
    updated_keys: set[str] = set()
    unmatched_keys: set[str] = set()
    locked_skipped_keys: set[str] = set()

    for sid, raw_val in update_values.items():
        sid_key = str(sid).strip().upper()
        if not sid_key:
            continue
        if sid_key in locked_keys:
            locked_skipped_keys.add(sid_key)
            continue
        if sid_key not in station_lookup:
            unmatched_keys.add(sid_key)
            continue

        value = float(raw_val)
        idx_list = station_lookup[sid_key]
        actual = pd.to_numeric(by_station.loc[idx_list, "actual_spend"], errors="coerce").fillna(0.0)
        by_station.loc[idx_list, "forecasted_cost"] = value
        by_station.loc[idx_list, "variance"] = actual - value
        if value == 0:
            by_station.loc[idx_list, "variance_pct"] = 0.0
        else:
            by_station.loc[idx_list, "variance_pct"] = ((actual - value) / value * 100).round(1)
        updated_keys.add(sid_key)

    if updated_keys:
        store.write_csv("capex_by_station.csv", by_station)
        if update_overrides:
            overrides = store.read_json("forecast_overrides.json")
            if not isinstance(overrides, dict):
                overrides = {}
            for sid_key in sorted(updated_keys):
                canonical_sid = station_display.get(sid_key, sid_key)
                idx = station_lookup[sid_key][0]
                val = float(by_station.at[idx, "forecasted_cost"])
                overrides[canonical_sid] = val
            store.write_json("forecast_overrides.json", overrides)

    updated_station_ids = [station_display.get(k, k) for k in sorted(updated_keys)]
    unmatched_station_ids = [station_display.get(k, k) for k in sorted(unmatched_keys)]
    locked_skipped_station_ids = [station_display.get(k, k) for k in sorted(locked_skipped_keys)]
    return {
        "updated_count": len(updated_station_ids),
        "updated_station_ids": updated_station_ids,
        "unmatched_station_ids": unmatched_station_ids,
        "locked_skipped_station_ids": locked_skipped_station_ids,
    }


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.route("/api/modules")
def api_modules():
    """Return the list of production lines for the global filter (CELLs nested under MODs).

    Includes lines from both capex_clean.csv (spend) and capex_by_station.csv
    (forecasted) so that lines with budget but no spend yet still appear.
    """
    df = _load_csv("capex_clean.csv")
    lines = _all_lines(df)
    by_station = _load_csv("capex_by_station.csv")
    if not by_station.empty and "station_id" in by_station.columns:
        budget_lines = {_extract_line(str(sid)) for sid in by_station["station_id"] if _extract_line(str(sid))}
        existing = set(lines)
        for bl in sorted(budget_lines):
            if bl not in existing:
                lines.append(bl)
    return jsonify(lines)


@app.route("/api/summary")
def api_summary():
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify({})
    df = _apply_line_filter(df)

    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)
    df["_total"] = pd.to_numeric(df["price_total"], errors="coerce").fillna(0)

    total_committed = float(df["_sub"].sum())
    odoo_total = float(df.loc[df["source"] == "odoo", "_sub"].sum())
    ramp_total = float(df.loc[df["source"] == "ramp", "_sub"].sum())
    active_pos = int(df["po_number"].nunique())
    _vendor_col = next((c for c in df.columns if c.strip().lower() == "vendor_name"), None)
    unique_vendors = int(df[_vendor_col].nunique()) if _vendor_col else 0
    odoo_count = int((df["source"] == "odoo").sum())
    ramp_count = int((df["source"] == "ramp").sum())

    by_station = _load_csv("capex_by_station.csv")
    raw_lines = request.args.get("lines", "")
    if raw_lines and not by_station.empty:
        allowed = {s.strip() for s in raw_lines.split(",") if s.strip()}
        by_station = by_station[by_station["station_id"].apply(_extract_line).isin(allowed)]
    forecasted = float(pd.to_numeric(by_station["forecasted_cost"], errors="coerce").sum()) if "forecasted_cost" in by_station.columns else 0
    variance = total_committed - forecasted
    pct_spent = (total_committed / forecasted * 100) if forecasted else 0

    df["_date"] = pd.to_datetime(df["date_order"], errors="coerce")
    dated = df.dropna(subset=["_date"]).copy()
    dated["_month"] = dated["_date"].dt.to_period("M").astype(str)
    monthly = dated.groupby(["_month", "source"])["_sub"].sum().reset_index()
    monthly_data: dict[str, dict] = {}
    for _, r in monthly.iterrows():
        m = r["_month"]
        if m not in monthly_data:
            monthly_data[m] = {"month": m, "odoo": 0, "ramp": 0}
        monthly_data[m][r["source"]] = float(r["_sub"])
    undated_ramp = float(df.loc[(df["source"] == "ramp") & df["_date"].isna(), "_sub"].sum())
    monthly_list = sorted(monthly_data.values(), key=lambda x: x["month"])

    dated["_line"] = dated["station_id"].apply(_extract_line)
    monthly_by_line: dict[str, dict[str, float]] = {}
    for ln in sorted(set(l for l in dated["_line"] if l)):
        sub = dated[dated["_line"] == ln].groupby("_month")["_sub"].sum()
        monthly_by_line[ln] = {str(m): float(v) for m, v in sub.items()}

    cat_spend = df.groupby("product_category")["_sub"].sum().reset_index()
    cat_spend = cat_spend[cat_spend["product_category"] != ""]
    cat_spend = cat_spend.sort_values("_sub", ascending=False)
    cat_data = [{"category": r["product_category"], "spend": float(r["_sub"])} for _, r in cat_spend.iterrows()]

    subcat_col = "mfg_subcategory" if "mfg_subcategory" in df.columns else None
    subcat_data: list[dict] = []
    mfg_total = 0.0
    non_mfg_total = 0.0
    if subcat_col:
        sc_spend = df[df[subcat_col] != ""].groupby(subcat_col)["_sub"].sum().reset_index()
        sc_spend = sc_spend.sort_values("_sub", ascending=False)
        subcat_data = [{"subcategory": r[subcat_col], "spend": float(r["_sub"])} for _, r in sc_spend.iterrows()]
        is_mfg = df.get("is_mfg")
        if is_mfg is not None:
            mfg_total = float(df.loc[df["is_mfg"] == True, "_sub"].sum())
            non_mfg_total = float(df.loc[df["is_mfg"] != True, "_sub"].sum())

    # Top 15 vendors (use column if present; show "(No name)" for blank so chart has data)
    vendor_col = next((c for c in df.columns if c.strip().lower() == "vendor_name"), None)
    if vendor_col and not df.empty:
        _vn = df[vendor_col].fillna("").astype(str).str.strip().replace("", "(No name)")
        vendor_spend = df.assign(_vn=_vn).groupby("_vn")["_sub"].sum().reset_index().rename(columns={"_vn": "vendor_name"}).sort_values("_sub", ascending=False).head(15)
        vendor_data = [{"vendor": r["vendor_name"], "spend": float(r["_sub"])} for _, r in vendor_spend.iterrows()]
    else:
        vendor_data = []

    conf_counts = df["mapping_confidence"].value_counts().to_dict()

    mapping_detail: dict[str, list] = {}
    for conf_level in ["high", "medium", "low", "none"]:
        sub = df[df["mapping_confidence"] == conf_level].copy()
        if sub.empty:
            continue
        by_proj = sub.groupby("project_name")["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(10)
        mapping_detail[conf_level] = [
            {"project": r["project_name"] or "(no project)", "spend": float(r["_sub"]), "count": int(sub[sub["project_name"] == r["project_name"]].shape[0])}
            for _, r in by_proj.iterrows()
        ]

    line_data = []
    if not by_station.empty and "station_id" in by_station.columns:
        by_station["_mod"] = by_station["station_id"].apply(lambda s: _extract_line(s) or "Other")
        mod_agg = by_station.groupby("_mod").agg(
            forecasted=("forecasted_cost", lambda x: pd.to_numeric(x, errors="coerce").sum()),
            actual=("actual_spend", lambda x: pd.to_numeric(x, errors="coerce").sum()),
        ).reset_index()
        line_data = [{"line": r["_mod"], "forecasted": float(r["forecasted"]), "actual": float(r["actual"])} for _, r in mod_agg.iterrows()]

    source_compare = {
        "odoo": {"total": odoo_total, "count": odoo_count, "avg": odoo_total / odoo_count if odoo_count else 0},
        "ramp": {"total": ramp_total, "count": ramp_count, "avg": ramp_total / ramp_count if ramp_count else 0},
    }
    odoo_cats = df[df["source"] == "odoo"].groupby("product_category")["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(8)
    ramp_cats = df[df["source"] == "ramp"].groupby("product_category")["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(8)
    source_compare["odoo_categories"] = [{"cat": r["product_category"] or "(none)", "spend": float(r["_sub"])} for _, r in odoo_cats.iterrows()]
    source_compare["ramp_categories"] = [{"cat": r["product_category"] or "(none)", "spend": float(r["_sub"])} for _, r in ramp_cats.iterrows()]
    if subcat_col:
        odoo_sc = df[df["source"] == "odoo"].groupby(subcat_col)["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(10)
        ramp_sc = df[df["source"] == "ramp"].groupby(subcat_col)["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(10)
        source_compare["odoo_subcats"] = [{"cat": r[subcat_col], "spend": float(r["_sub"])} for _, r in odoo_sc.iterrows()]
        source_compare["ramp_subcats"] = [{"cat": r[subcat_col], "spend": float(r["_sub"])} for _, r in ramp_sc.iterrows()]

    payment_summary: dict = {"available": False}
    odoo_df = df[df["source"] == "odoo"].copy()
    status_col = "po_payment_status_v2" if "po_payment_status_v2" in odoo_df.columns else "bill_payment_status"
    if not odoo_df.empty and status_col in odoo_df.columns:
        pay_state = odoo_df[status_col].fillna("").astype(str).str.strip().replace("", "no_bill")
        paid_spend = float(odoo_df.loc[pay_state == "paid", "_sub"].sum())
        partial_spend = float(odoo_df.loc[pay_state == "partial", "_sub"].sum())
        unpaid_spend = float(odoo_df.loc[pay_state == "unpaid", "_sub"].sum())
        no_bill_spend = float(odoo_df.loc[pay_state == "no_bill", "_sub"].sum())
        mixed_spend = float(odoo_df.loc[pay_state == "mixed", "_sub"].sum())
        billed_spend = paid_spend + partial_spend + unpaid_spend + mixed_spend
        open_spend = unpaid_spend + partial_spend + mixed_spend
        odoo_committed = float(odoo_df["_sub"].sum())
        payment_summary = {
            "available": True,
            "status_source": status_col,
            "odoo_committed": odoo_committed,
            "paid_spend": paid_spend,
            "partial_spend": partial_spend,
            "unpaid_spend": unpaid_spend,
            "no_bill_spend": no_bill_spend,
            "mixed_spend": mixed_spend,
            "open_spend": open_spend,
            "billed_spend": billed_spend,
            "paid_spend_pct": (paid_spend / odoo_committed * 100) if odoo_committed else 0.0,
            "billed_spend_pct": (billed_spend / odoo_committed * 100) if odoo_committed else 0.0,
        }

    # Spend by employee (use column if present; show "(No name)" for blank so chart has data)
    emp_col = next((c for c in df.columns if c.strip().lower() == "created_by_name"), None)
    if emp_col and not df.empty:
        _en = df[emp_col].fillna("").astype(str).str.strip().replace("", "(No name)")
        emp_agg = df.assign(_en=_en).groupby("_en").agg(
            spend=("_sub", "sum"), count=("_sub", "size"), pos=("po_number", "nunique"),
        ).reset_index().rename(columns={"_en": "created_by_name"}).sort_values("spend", ascending=False).head(15)
        emp_data = [{"name": r["created_by_name"], "spend": float(r["spend"]), "count": int(r["count"]), "pos": int(r["pos"])} for _, r in emp_agg.iterrows()]
    else:
        emp_data = []

    ramp_df = df[df["source"] == "ramp"].copy()
    ramp_payment: dict = {"available": False}
    if not ramp_df.empty:
        ramp_spend = float(ramp_df["_sub"].sum())
        ramp_txn_count = int(ramp_df["po_number"].nunique())
        ramp_payment = {
            "available": True,
            "total_amount": ramp_spend,
            "txn_count": ramp_txn_count,
            "paid_pct": 100.0,
            "card_charged": ramp_spend,
        }

    return jsonify({
        "total_committed": total_committed,
        "odoo_total": odoo_total,
        "ramp_total": ramp_total,
        "forecasted_budget": forecasted,
        "variance": variance,
        "pct_spent": round(pct_spent, 1),
        "active_pos": active_pos,
        "unique_vendors": unique_vendors,
        "monthly_trend": monthly_list,
        "monthly_by_line": monthly_by_line,
        "undated_ramp": undated_ramp,
        "category_spend": cat_data,
        "subcategory_spend": subcat_data,
        "mfg_total": mfg_total,
        "non_mfg_total": non_mfg_total,
        "top_vendors": vendor_data,
        "top_employees": emp_data,
        "mapping_quality": conf_counts,
        "mapping_detail": mapping_detail,
        "budget_vs_actual": line_data,
        "source_compare": source_compare,
        "payment": payment_summary,
        "ramp_payment": ramp_payment,
    })


@app.route("/api/payment-evidence")
def api_payment_evidence():
    """PO-level payment evidence table for status transparency."""
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify({"rows": [], "count": 0})
    df = _apply_line_filter(df)
    if "source" in df.columns:
        df = df[df["source"] == "odoo"].copy()
    if df.empty or "po_number" not in df.columns:
        return jsonify({"rows": [], "count": 0})

    for c in ("price_subtotal", "po_amount_total", "bill_amount_total_v2", "bill_amount_paid_v2", "bill_amount_open_v2"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    status_col = "po_payment_status_v2" if "po_payment_status_v2" in df.columns else "bill_payment_status"

    def _first_nonempty(series: pd.Series, default: str = "") -> str:
        vals = series.fillna("").astype(str).str.strip()
        vals = vals[vals != ""]
        return vals.iloc[0] if not vals.empty else default

    def _num_max(group: pd.DataFrame, col: str, default: float = 0.0) -> float:
        if col not in group.columns:
            return float(default)
        ser = pd.to_numeric(group[col], errors="coerce").fillna(default)
        return float(ser.max() if len(ser) else default)

    def _bool_any(group: pd.DataFrame, col: str) -> bool:
        if col not in group.columns:
            return False
        ser = group[col]
        try:
            return bool(ser.fillna(False).astype(bool).any())
        except Exception:
            return bool(
                ser.fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
                .isin({"1", "true", "yes", "y"})
                .any()
            )

    rows: list[dict] = []
    for po, g in df.groupby("po_number"):
        po_total = _num_max(g, "po_amount_total", 0.0)
        spend_total = float(pd.to_numeric(g.get("price_subtotal", 0), errors="coerce").sum())
        billed = _num_max(g, "bill_amount_total_v2", 0.0)
        paid = _num_max(g, "bill_amount_paid_v2", 0.0)
        open_amt = _num_max(g, "bill_amount_open_v2", 0.0)
        rows.append({
            "po_number": str(po),
            "vendor_name": _first_nonempty(g.get("vendor_name", pd.Series(dtype=str))),
            "created_by_name": _first_nonempty(g.get("created_by_name", pd.Series(dtype=str))),
            "po_total": po_total,
            "line_spend_total": spend_total,
            "billed_total_v2": billed,
            "paid_total_v2": paid,
            "open_total_v2": open_amt,
            "paid_pct_of_po": (paid / po_total * 100) if po_total else 0.0,
            "status_v2": _first_nonempty(g.get(status_col, pd.Series(dtype=str)), "no_bill"),
            "confidence": _first_nonempty(g.get("payment_status_confidence", pd.Series(dtype=str))),
            "has_unbilled_signal": _bool_any(g, "has_unbilled_payment_signal"),
            "has_deposit_signal": _bool_any(g, "has_deposit_signal"),
            "evidence_notes": _first_nonempty(g.get("payment_evidence_notes", pd.Series(dtype=str))),
        })

    rows.sort(key=lambda r: r["po_total"], reverse=True)
    limit = request.args.get("limit", 250, type=int) or 250
    limit = max(1, min(limit, 1000))
    return jsonify({"rows": rows[:limit], "count": len(rows)})


@app.route("/api/stations")
def api_stations():
    by_station = _load_csv("capex_by_station.csv")
    if by_station.empty:
        return jsonify([])
    raw_lines = request.args.get("lines", "")
    if raw_lines:
        allowed = {s.strip() for s in raw_lines.split(",") if s.strip()}
        by_station = by_station[by_station["station_id"].apply(_extract_line).isin(allowed)]
    return jsonify(by_station.to_dict(orient="records"))


@app.route("/api/forecasting")
def api_forecasting():
    """Return station rows grouped by line for the Forecasting tab."""
    by_station = _load_csv("capex_by_station.csv")
    rows, groups = _build_forecasting_rows(by_station)
    return jsonify({"rows": rows, "groups": groups})


@app.route("/api/station/<station_id>")
def api_station_detail(station_id: str):
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify({"lines": [], "vendors": [], "timeline": []})

    sub = df[df["station_id"] == station_id].copy()
    sub["price_subtotal"] = pd.to_numeric(sub["price_subtotal"], errors="coerce").fillna(0)

    lines = sub.to_dict(orient="records")

    vendor_agg = sub.groupby("vendor_name")["price_subtotal"].sum().reset_index()
    vendors = [{"vendor": r["vendor_name"], "spend": float(r["price_subtotal"])} for _, r in vendor_agg.iterrows()]

    sub["_date"] = pd.to_datetime(sub["date_order"], errors="coerce")
    timeline = sub.dropna(subset=["_date"]).sort_values("_date")
    timeline_data = [{
        "date": row["_date"].strftime("%Y-%m-%d"),
        "po": str(row.get("po_number", "")),
        "desc": str(row.get("item_description", ""))[:60],
        "vendor": str(row.get("vendor_name", "")),
        "amount": float(row["price_subtotal"]),
    } for _, row in timeline.iterrows()]

    by_station = _load_csv("capex_by_station.csv")
    station_row = by_station[by_station["station_id"] == station_id]
    meta = station_row.iloc[0].to_dict() if not station_row.empty else {}

    return jsonify({
        "meta": meta,
        "lines": lines[:500],
        "vendors": vendors,
        "timeline": timeline_data,
    })


@app.route("/api/vendors")
def api_vendors():
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify([])
    df = _apply_line_filter(df)
    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)

    vendor_agg = df.groupby("vendor_name").agg(
        spend=("_sub", "sum"),
        po_count=("po_number", "nunique"),
        stations=("station_id", lambda x: ", ".join(sorted(set(s for s in x if s)))),
    ).reset_index().sort_values("spend", ascending=False)

    return jsonify(vendor_agg.to_dict(orient="records"))


@app.route("/api/vendor/<vendor_name>")
def api_vendor_detail(vendor_name: str):
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify([])
    sub = df[df["vendor_name"] == vendor_name]
    return jsonify(sub.head(500).to_dict(orient="records"))


@app.route("/api/spares")
def api_spares():
    df = _load_csv("spares_catalog.csv")
    if df.empty:
        return jsonify([])
    return jsonify(df.to_dict(orient="records"))


@app.route("/api/transactions")
def api_transactions():
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify([])
    df = _apply_line_filter(df)
    return jsonify(df.to_dict(orient="records"))


@app.route("/api/timeline")
def api_timeline():
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify({"weekly": [], "monthly_cat": [], "cumulative": [], "monthly_source": []})
    df = _apply_line_filter(df)
    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)
    df["_date"] = pd.to_datetime(df["date_order"], errors="coerce")
    dated = df.dropna(subset=["_date"]).copy()

    dated["_week"] = dated["_date"].dt.isocalendar().week.astype(int)
    dated["_year"] = dated["_date"].dt.year
    weekly = dated.groupby(["_year", "_week"]).agg(spend=("_sub", "sum"), count=("_sub", "size")).reset_index()
    weekly["label"] = weekly.apply(lambda r: f"{int(r['_year'])}-W{int(r['_week']):02d}", axis=1)
    weekly_data = [{"week": r["label"], "spend": float(r["spend"]), "count": int(r["count"])} for _, r in weekly.iterrows()]

    dated["_month"] = dated["_date"].dt.to_period("M").astype(str)
    monthly_cat = dated.groupby(["_month", "product_category"])["_sub"].sum().reset_index()
    mc_data = [{"month": r["_month"], "category": r["product_category"], "spend": float(r["_sub"])} for _, r in monthly_cat.iterrows()]

    daily = dated.groupby(dated["_date"].dt.date)["_sub"].sum().sort_index().cumsum()
    cum_data = [{"date": str(d), "cumulative": float(v)} for d, v in daily.items()]

    monthly_src = dated.groupby(["_month", "source"])["_sub"].sum().reset_index()
    ms_data = [{"month": r["_month"], "source": r["source"], "spend": float(r["_sub"])} for _, r in monthly_src.iterrows()]

    msc_data: list[dict] = []
    if "mfg_subcategory" in dated.columns:
        monthly_sc = dated.groupby(["_month", "mfg_subcategory"])["_sub"].sum().reset_index()
        msc_data = [{"month": r["_month"], "subcategory": r["mfg_subcategory"], "spend": float(r["_sub"])} for _, r in monthly_sc.iterrows()]

    undated_spend = float(df.loc[df["_date"].isna(), "_sub"].sum())

    return jsonify({
        "weekly": weekly_data,
        "monthly_cat": mc_data,
        "monthly_subcat": msc_data,
        "cumulative": cum_data,
        "monthly_source": ms_data,
        "undated_spend": undated_spend,
    })


# ---------------------------------------------------------------------------
# Dashboard HTML
# ---------------------------------------------------------------------------

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Base Power - Mfg Budgeting</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.html5.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/jszip@3.10.1/dist/jszip.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>
<link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.4.2/css/buttons.dataTables.min.css">
<style>
:root{--bg:#1A1A1A;--surface:#242422;--surface2:#32312F;--text:#F0EEEB;--muted:#9E9C98;--accent:#B2DD79;--accent-dark:#1A1A1A;--green:#B2DD79;--green-bright:#D0F585;--yellow:#F7C33C;--red:#D1531D;--blue:#048EE5;--border:#3E3D3A;--disabled:#32312F;--secondary:#3E3D3A}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--text);display:flex;min-height:100vh}
.sidebar{width:230px;background:var(--surface);border-right:1px solid var(--border);flex-shrink:0;position:fixed;height:100vh;overflow-y:auto;display:flex;flex-direction:column}
.sidebar-brand{padding:20px 18px 16px;border-bottom:1px solid var(--border)}
.sidebar-brand h2{font-size:15px;font-weight:700;color:var(--green);letter-spacing:.5px}
.sidebar-brand .sub{font-size:10px;color:var(--muted);margin-top:2px;text-transform:uppercase;letter-spacing:1px}
.nav-section-title{padding:10px 18px 6px;font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.9px;font-weight:700}
.nav-section-title.with-divider{border-top:1px solid var(--border);margin-top:6px;padding-top:12px}
.nav-item{display:flex;align-items:center;gap:10px;padding:11px 18px;color:var(--muted);text-decoration:none;font-size:13px;cursor:pointer;border-left:3px solid transparent;transition:all .15s}
.nav-item:hover{background:var(--surface2);color:var(--text)}
.nav-item.active{color:var(--green);border-left-color:var(--green);background:rgba(178,221,121,.06);font-weight:600}
.nav-item .icon{font-size:16px;width:20px;text-align:center}
.main{margin-left:230px;flex:1;padding:28px 32px;min-width:0}
.page{display:none;animation:fadeIn .2s ease}
.page.active{display:block}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.page-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:24px}
.page-title{font-size:20px;font-weight:700;color:var(--text)}
.page-subtitle{font-size:12px;color:var(--muted);margin-top:2px}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px;margin-bottom:24px}
.kpi{background:var(--surface);border-radius:10px;padding:18px 20px;border:1px solid var(--border);transition:border-color .15s}
.kpi:hover{border-color:var(--green)}
.kpi .label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;font-weight:600}
.kpi .value{font-size:24px;font-weight:700;margin-top:6px;font-variant-numeric:tabular-nums}
.kpi .sub{font-size:11px;color:var(--muted);margin-top:3px}
.chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:24px}
.chart-card{background:var(--surface);border-radius:10px;padding:18px;border:1px solid var(--border);overflow:hidden}
.chart-card.full{grid-column:1/-1}
.chart-card h3{font-size:12px;margin-bottom:14px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;font-weight:600;padding-right:120px}
.js-plotly-plot .plotly .modebar{
    background:rgba(36,36,34,.9)!important;
    border:1px solid var(--border)!important;
    border-radius:6px!important;
    padding:2px!important;
}
.chart-card .js-plotly-plot .plotly .modebar{
    top:-30px!important;
    right:0!important;
    z-index:20!important;
}
.filter-bar{display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap;align-items:center}
.filter-bar select,.filter-bar input{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:8px 14px;font-size:13px;outline:none;transition:border-color .15s}
.filter-bar select:focus,.filter-bar input:focus{border-color:var(--green)}
.filter-bar label{font-size:11px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.5px}
table.dataTable{color:var(--text)!important;background:var(--surface)!important;border-collapse:collapse!important;width:100%!important;font-size:12px!important}
table.dataTable{table-layout:fixed!important}
div.dataTables_scrollHead table,div.dataTables_scrollBody table{table-layout:fixed!important}
table.dataTable thead th{background:var(--surface2)!important;color:var(--muted)!important;border-bottom:1px solid var(--border)!important;font-size:11px!important;padding:10px 8px!important;text-transform:uppercase;letter-spacing:.3px;font-weight:600;overflow:hidden;min-width:60px}
table.dataTable tbody td{border-bottom:1px solid rgba(62,61,58,.5)!important;padding:8px!important;max-width:350px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
table.dataTable tbody tr:hover{background:rgba(178,221,121,.05)!important}
body.tables-expanded table.dataTable tbody td{max-width:none!important;white-space:normal!important;overflow:visible!important;text-overflow:clip!important;word-break:break-word}
body.tables-expanded table.dataTable{table-layout:auto!important}
body.tables-expanded div.dataTables_scrollHead table,
body.tables-expanded div.dataTables_scrollBody table{table-layout:auto!important}
body.tables-expanded table.dataTable thead th{white-space:nowrap!important}
#detail-table-wrap.detail-expanded table#detail-tbl.dataTable{table-layout:auto!important;width:max-content!important;min-width:100%!important}
#detail-table-wrap.detail-expanded table#detail-tbl.dataTable thead th{width:auto!important;min-width:110px!important;white-space:nowrap!important}
#detail-table-wrap.detail-expanded table#detail-tbl.dataTable tbody td{width:auto!important;max-width:none!important;white-space:normal!important;overflow:visible!important;text-overflow:clip!important;word-break:break-word}
.detail-native-toolbar{display:flex;gap:10px;align-items:center;justify-content:space-between;margin:6px 0 10px;flex-wrap:wrap}
.detail-native-toolbar .left{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.detail-native-toolbar .right{font-size:12px;color:var(--muted)}
.detail-native-search{min-width:260px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:8px 10px;font-size:12px}
.detail-native-wrap{border:1px solid var(--border);border-radius:8px;overflow:auto;max-height:70vh;background:var(--surface)}
table.detail-native-table{color:var(--text);background:var(--surface);border-collapse:collapse;width:max-content;min-width:100%;font-size:12px;table-layout:auto}
table.detail-native-table thead th{position:sticky;top:0;z-index:2;background:var(--surface2);color:var(--muted);border-bottom:1px solid var(--border);font-size:11px;padding:10px 8px;text-transform:uppercase;letter-spacing:.3px;font-weight:600;white-space:nowrap}
table.detail-native-table tbody td{border-bottom:1px solid rgba(62,61,58,.5);padding:8px;max-width:none;white-space:normal;overflow:visible;text-overflow:clip;word-break:break-word;vertical-align:top}
table.detail-native-table tbody tr:hover{background:rgba(178,221,121,.05)}
.dataTables_wrapper .dataTables_filter input{background:var(--surface2)!important;color:var(--text)!important;border:1px solid var(--border)!important;border-radius:6px;padding:6px 10px}
.dataTables_wrapper .dataTables_length select{background:var(--surface2)!important;color:var(--text)!important;border:1px solid var(--border)!important}
.dataTables_wrapper .dataTables_info,.dataTables_wrapper .dataTables_paginate{color:var(--muted)!important;font-size:11px!important}
.dataTables_wrapper .dataTables_paginate .paginate_button{color:var(--muted)!important}
.dataTables_wrapper .dataTables_paginate .paginate_button.current{background:var(--green)!important;color:var(--accent-dark)!important;border:none!important;border-radius:4px;font-weight:700}
.dataTables_wrapper .dataTables_paginate .paginate_button:hover{background:var(--surface2)!important;color:var(--text)!important}
dt.buttons-csv{background:var(--green)!important;color:var(--accent-dark)!important;border:none!important;font-weight:600!important;border-radius:4px!important}
.station-select{min-width:350px}
.sidebar-footer{padding:16px 18px;margin-top:auto;border-top:1px solid var(--border)}
.btn-refresh{width:100%;padding:10px;background:var(--green);color:var(--accent-dark);border:none;border-radius:6px;cursor:pointer;font-size:12px;font-weight:700;letter-spacing:.3px;transition:all .15s}
.btn-refresh:hover{background:var(--green-bright)}
.btn-refresh:disabled{background:var(--disabled);color:var(--muted);cursor:wait}
.toast{position:fixed;bottom:24px;right:24px;background:var(--green);color:var(--accent-dark);padding:12px 24px;border-radius:8px;font-weight:700;font-size:13px;display:none;z-index:999;box-shadow:0 4px 20px rgba(0,0,0,.4)}
.forecast-input{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 8px;width:110px;font-size:12px;font-variant-numeric:tabular-nums;text-align:right}
.forecast-input:focus{border-color:var(--green);outline:none}
.forecast-save{background:var(--green);color:var(--accent-dark);border:none;border-radius:4px;padding:3px 10px;font-size:11px;font-weight:700;cursor:pointer;margin-left:4px}
.forecast-save:hover{background:var(--green-bright)}
.forecast-saved{color:var(--green);font-size:11px;font-weight:600;margin-left:6px}
.forecast-lock{background:transparent;color:var(--green);border:1px solid var(--green);border-radius:4px;padding:1px 8px;font-size:10px;font-weight:700;cursor:pointer;margin-left:6px}
.forecast-lock:hover{background:rgba(178,221,121,.12)}
.forecast-unlock{background:transparent;color:var(--yellow);border:1px solid var(--yellow);border-radius:4px;padding:1px 8px;font-size:10px;font-weight:700;cursor:pointer;margin-left:6px}
.forecast-unlock:hover{background:rgba(247,195,60,.12)}
.dollar{font-variant-numeric:tabular-nums}
.dollar-positive{color:var(--green)}
.dollar-negative{color:var(--red)}
.drill-panel{background:var(--surface2);border-radius:8px;padding:16px;margin-top:12px;display:none;max-height:300px;overflow-y:auto}
.drill-panel h4{font-size:12px;color:var(--green);margin-bottom:10px;text-transform:uppercase}
.drill-row{display:flex;justify-content:space-between;padding:4px 0;font-size:12px;border-bottom:1px solid rgba(62,61,58,.3)}
.drill-row .dr-name{color:var(--text);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.drill-row .dr-val{color:var(--muted);margin-left:12px;white-space:nowrap}
.source-badge{display:inline-block;padding:2px 8px;border-radius:3px;font-size:10px;font-weight:700;text-transform:uppercase}
.source-badge.odoo{background:rgba(178,221,121,.15);color:var(--green)}
.source-badge.ramp{background:rgba(4,142,229,.15);color:var(--blue)}
table.dataTable tfoot th{padding:4px 4px!important;background:var(--surface2)!important}
table.dataTable tfoot input{width:100%;padding:4px 6px;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:3px;font-size:10px;outline:none;box-sizing:border-box}
table.dataTable tfoot input:focus{border-color:var(--green)}
table.dataTable tfoot input::placeholder{color:var(--muted);font-size:10px}
.dataTables_wrapper table.dataTable thead tr.dt-filter-row th{background:var(--surface)!important;padding:4px 6px!important;border-bottom:1px solid var(--border)!important}
.dataTables_wrapper table.dataTable thead tr.dt-filter-row input{width:100%;padding:4px 6px;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:3px;font-size:10px;outline:none;box-sizing:border-box}
.dataTables_wrapper table.dataTable thead tr.dt-filter-row input:focus{border-color:var(--green)}
.dt-resizable thead th{position:relative;overflow:visible!important}
.col-resizer{position:absolute;top:0;right:0;width:14px;height:100%;cursor:col-resize;z-index:3;touch-action:none}
.col-resizer:hover{background:rgba(178,221,121,.18)}
body.col-resize-active{cursor:col-resize;user-select:none}
.asset-mode-btn{background:var(--surface2);color:var(--muted);border:1px solid var(--border);border-radius:4px;padding:4px 12px;font-size:11px;font-weight:600;cursor:pointer;transition:all .15s}
.asset-mode-btn.active{background:rgba(178,221,121,.15);color:var(--green);border-color:var(--green)}
.asset-mode-btn:hover{border-color:var(--green)}
.asset-date{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:3px;padding:2px 4px;width:105px;font-size:10px;cursor:pointer}
.asset-date:focus{border-color:var(--green);outline:none}
.asset-date::-webkit-calendar-picker-indicator{filter:invert(.7)}
.about-hero{background:linear-gradient(135deg,rgba(178,221,121,.14),rgba(4,142,229,.12));border:1px solid var(--border);border-radius:12px;padding:20px 22px;margin-bottom:16px}
.about-hero h3{font-size:18px;color:var(--text);margin-bottom:8px}
.about-hero p{font-size:13px;color:var(--text);line-height:1.5;max-width:980px}
.about-pill-row{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
.about-pill{display:inline-block;padding:5px 10px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:.2px}
.about-pill.ok{background:rgba(178,221,121,.2);color:var(--green)}
.about-pill.warn{background:rgba(247,195,60,.18);color:var(--yellow)}
.about-pill.note{background:rgba(4,142,229,.18);color:var(--blue)}
.about-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;margin-bottom:16px}
.about-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px}
.about-card h4{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px}
.about-card ul{margin:0;padding-left:18px}
.about-card li{font-size:12px;line-height:1.45;color:var(--text);margin:5px 0}
.about-timeline{display:grid;gap:8px}
.about-step{display:flex;align-items:flex-start;gap:10px;padding:10px 12px;background:var(--surface);border:1px solid var(--border);border-radius:8px}
.about-step .num{width:24px;height:24px;border-radius:50%;background:rgba(178,221,121,.2);color:var(--green);display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;flex-shrink:0}
.about-step .body{font-size:12px;color:var(--text);line-height:1.45}
.about-step .body strong{display:block;color:var(--text);margin-bottom:2px}
.about-rules{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.about-flow{display:grid;gap:10px}
.about-flow-row{display:flex;align-items:stretch;gap:8px;flex-wrap:wrap}
.about-node{min-width:180px;flex:1;background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:12px}
.about-node h5{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}
.about-node p{font-size:12px;line-height:1.4}
.about-arrow{align-self:center;color:var(--green);font-weight:700;font-size:16px;padding:0 2px}
.about-tech-table{width:100%;border-collapse:collapse}
.about-tech-table th,.about-tech-table td{border-bottom:1px solid var(--border);padding:8px 6px;text-align:left;vertical-align:top}
.about-tech-table th{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.4px}
.about-tech-table td{font-size:12px;line-height:1.4}
.about-code{display:block;font-family:Consolas,monospace;font-size:11px;color:var(--green);margin-top:3px;word-break:break-word}
.about-metric-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin-bottom:16px}
.about-metric{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px}
.about-metric .k{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px}
.about-metric .v{font-size:18px;font-weight:700;color:var(--text);margin-top:4px}
.about-metric .d{font-size:11px;color:var(--muted);margin-top:3px;line-height:1.3}
.about-score-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
.about-score-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px}
.about-score-card h4{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px}
.about-bar-row{margin:8px 0}
.about-bar-row .lbl{font-size:11px;color:var(--text);margin-bottom:4px}
.about-bar{height:10px;border-radius:999px;background:var(--surface2);overflow:hidden;border:1px solid var(--border)}
.about-bar span{display:block;height:100%;background:linear-gradient(90deg,var(--green),var(--blue))}
.about-journey{display:flex;align-items:stretch;gap:8px;flex-wrap:wrap}
.about-stage{min-width:190px;flex:1;background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px}
.about-stage h5{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}
.about-stage p{font-size:12px;line-height:1.4}
.about-connector{align-self:center;color:var(--green);font-weight:700;font-size:16px}
.about-details{margin-top:10px;background:var(--surface);border:1px solid var(--border);border-radius:10px}
.about-details summary{cursor:pointer;padding:10px 12px;font-size:12px;color:var(--green);font-weight:700;list-style:none}
.about-details summary::-webkit-details-marker{display:none}
.about-details-body{padding:0 12px 12px}
@media(max-width:1200px){.about-rules{grid-template-columns:1fr}}
.mobile-header{display:none;position:fixed;top:0;left:0;right:0;z-index:200;background:var(--surface);border-bottom:1px solid var(--border);padding:10px 16px;align-items:center;gap:12px}
.mobile-header .hamburger{background:none;border:none;color:var(--green);font-size:22px;cursor:pointer;padding:4px 8px;line-height:1}
.mobile-header .brand{font-size:14px;font-weight:700;color:var(--green);letter-spacing:.4px}
.mobile-header .brand span{color:var(--muted);font-weight:400;font-size:11px;margin-left:6px;text-transform:uppercase;letter-spacing:.8px}
.sidebar-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:299}
@media(max-width:1024px){
.sidebar{transform:translateX(-100%);z-index:300;transition:transform .25s ease;width:260px}
.sidebar.open{transform:translateX(0)}
.sidebar-overlay.visible{display:block}
.mobile-header{display:flex}
.main{margin-left:0;padding:68px 16px 24px}
.chart-grid{grid-template-columns:1fr}
.kpi-row{grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px}
.page-title{font-size:17px}
.page-subtitle{font-size:11px}
.chart-card{padding:14px}
.chart-card h3{font-size:11px;padding-right:0}
.filter-bar{gap:8px}
.filter-bar select,.filter-bar input{font-size:12px;padding:6px 10px}
.about-grid{grid-template-columns:1fr}
.about-flow-row{flex-direction:column}
.about-arrow{transform:rotate(90deg);text-align:center}
.about-journey{flex-direction:column}
.about-rules{grid-template-columns:1fr}
.about-metric-grid{grid-template-columns:repeat(auto-fit,minmax(160px,1fr))}
.detail-native-toolbar{flex-direction:column;align-items:stretch}
.detail-native-search{min-width:unset;width:100%}
.station-select{min-width:unset;width:100%}
}
@media(max-width:600px){
.main{padding:64px 10px 20px}
.kpi-row{grid-template-columns:1fr 1fr;gap:8px}
.kpi{padding:12px 14px}
.kpi .value{font-size:18px}
.kpi .label{font-size:9px}
.chart-card{padding:10px;border-radius:8px}
.page-header{flex-direction:column;align-items:flex-start;gap:4px}
table.dataTable thead th{font-size:10px!important;padding:6px 4px!important}
table.dataTable tbody td{font-size:11px!important;padding:6px 4px!important}
.toast{bottom:12px;right:12px;left:12px;text-align:center}
.forecast-input{width:80px;font-size:11px}
}
</style>
</head>
<body>

<div class="mobile-header">
    <button class="hamburger" onclick="toggleSidebar()" aria-label="Menu">&#9776;</button>
    <div class="brand">MFG BUDGETING<span>Base Power</span></div>
</div>
<div class="sidebar-overlay" onclick="toggleSidebar()"></div>

<div class="sidebar">
    <div class="sidebar-brand"><h2>MFG BUDGETING</h2><div class="sub">Base Power Company</div></div>
    <div class="nav-section-title">Overview</div>
    <a class="nav-item active" onclick="showPage('executive',this)"><span class="icon">&#128200;</span> Executive Summary</a>
    <a class="nav-item" onclick="showPage('source',this)"><span class="icon">&#8644;</span> Odoo vs Ramp</a>
    <a class="nav-item" onclick="showPage('timeline',this)"><span class="icon">&#128197;</span> Spend Timeline</a>
    <a class="nav-item" onclick="showPage('detail',this)"><span class="icon">&#128203;</span> Transactions (All)</a>

    <div class="nav-section-title with-divider">Analysis</div>
    <a class="nav-item" onclick="showPage('stations',this)"><span class="icon">&#128295;</span> Station Drilldown</a>
    <a class="nav-item" onclick="showPage('vendors',this)"><span class="icon">&#128188;</span> Vendor Analysis</a>
    <a class="nav-item" onclick="showPage('assets',this)"><span class="icon">&#127970;</span> Asset Tracking</a>
    <a class="nav-item" onclick="showPage('spares',this)"><span class="icon">&#128230;</span> Materials &amp; Spares</a>
    <a class="nav-item" onclick="showPage('projects',this)"><span class="icon">&#9670;</span> Other Projects</a>
    <a class="nav-item" onclick="showPage('uniteco',this)"><span class="icon">&#9879;</span> Unit Economics</a>

    <div class="nav-section-title with-divider">Planning &amp; Cashflow</div>
    <a class="nav-item" onclick="showPage('forecasting',this)"><span class="icon">&#128202;</span> Forecasting</a>
    <a class="nav-item" onclick="showPage('v2milestones',this)"><span class="icon">&#128336;</span> Payment Timeline</a>
    <a class="nav-item" onclick="showPage('v2templates',this)"><span class="icon">&#128221;</span> Milestone Templates</a>
    <a class="nav-item" onclick="showPage('airfq',this)"><span class="icon">&#129302;</span> AI-RFQ Gen</a>
    <a class="nav-item" onclick="showPage('v2cashflow',this)"><span class="icon">&#128176;</span> Cashflow Forecast</a>

    <div class="nav-section-title with-divider">Admin</div>
    <a class="nav-item" onclick="showPage('settings',this)"><span class="icon">&#9881;</span> Settings</a>
    <a class="nav-item" onclick="showPage('v2reviews',this)"><span class="icon">&#9998;</span> Classification Review</a>
    <a class="nav-item" onclick="showPage('about',this)"><span class="icon">&#8505;</span> About This Tool</a>
    <div style="padding:12px 18px;border-top:1px solid var(--border)">
        <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;font-weight:600;margin-bottom:8px">Filter by Line</div>
        <div id="line-filter-checks" style="max-height:180px;overflow-y:auto"></div>
        <div style="margin-top:6px;display:flex;gap:6px">
            <button onclick="toggleAllLines(true)" style="flex:1;padding:4px;background:var(--green);color:var(--accent-dark);border:none;border-radius:4px;font-size:10px;font-weight:700;cursor:pointer">All</button>
            <button onclick="toggleAllLines(false)" style="flex:1;padding:4px;background:var(--secondary);color:var(--text);border:none;border-radius:4px;font-size:10px;font-weight:700;cursor:pointer">None</button>
        </div>
        <button id="btn-table-expand" onclick="toggleTableExpand()" style="margin-top:8px;width:100%;padding:6px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:4px;font-size:10px;font-weight:700;cursor:pointer">Expand Tables</button>
    </div>
    <div class="sidebar-footer"></div>
</div>

<div class="main">

<!-- EXECUTIVE -->
<div class="page active" id="page-executive">
    <div class="page-header"><div><div class="page-title">Executive Summary</div><div class="page-subtitle">BF1 Manufacturing CAPEX Overview</div></div></div>
    <div class="kpi-row" id="kpis"></div>
    <div class="chart-grid">
        <div class="chart-card full"><h3>Budget vs Actual by Module</h3><div id="chart-budget"></div></div>
        <div class="chart-card"><h3>Monthly Spend Trend (Odoo + Ramp)</h3><div id="chart-monthly"></div></div>
        <div class="chart-card"><h3>Spend by Mfg Sub-Category</h3><div id="chart-subcategory"></div></div>
        <div class="chart-card"><h3>Top 15 Vendors</h3><div id="chart-vendors"></div></div>
        <div class="chart-card"><h3>Odoo PO Payment Status</h3><div id="chart-payment-status"></div></div>
        <div class="chart-card"><h3>Ramp CC Payment Status</h3><div id="chart-ramp-payment"></div></div>
        <div class="chart-card full"><h3>Spend by Employee</h3><div id="chart-employees"></div></div>
        <div class="chart-card full"><h3>Payment Evidence (PO-Level)</h3><div id="payment-evidence-wrap"></div></div>
    </div>
</div>

<!-- ABOUT -->
<div class="page" id="page-about">
    <div class="page-header"><div><div class="page-title">About &amp; Operating Guide</div><div class="page-subtitle">What this dashboard does, how data moves, and how to run it with confidence</div></div></div>

    <div class="about-hero">
        <h3>Prime-time operating view</h3>
        <p>
            This dashboard is a manufacturing CAPEX control tower for Odoo purchase orders, Ramp card spend, payment timelines, and cashflow forecasting.
            Use this page as a quick runbook for data trust, refresh cadence, and workflow.
        </p>
        <div class="about-pill-row">
            <span class="about-pill note">Data model: batch refresh + incremental updates</span>
            <span class="about-pill warn">Refresh trigger: Settings &rarr; Data Management</span>
            <span class="about-pill ok">Outputs: executive KPIs, drilldowns, milestones, cashflow</span>
        </div>
    </div>

    <div class="about-metric-grid">
        <div class="about-metric"><div class="k">Primary Sources</div><div class="v">Odoo + Ramp</div><div class="d">PO lines, bill/payment evidence, and card spend context.</div></div>
        <div class="about-metric"><div class="k">Storage Pattern</div><div class="v">Analytics Backend</div><div class="d">Unified cleaned dataset feeds dashboard APIs and export views.</div></div>
        <div class="about-metric"><div class="k">Classification Model</div><div class="v">Rules + LLM Review</div><div class="d">Deterministic defaults with review queue for disagreements.</div></div>
        <div class="about-metric"><div class="k">Planning Model</div><div class="v">Actual + Template</div><div class="d">Cashflow forecast combines actuals and milestone templates.</div></div>
    </div>

    <div class="chart-card full" style="margin-bottom:14px">
        <h3>Navigation Guide</h3>
        <div class="about-grid">
            <div class="about-card">
                <h4>Overview</h4>
                <ul>
                    <li><strong>Executive Summary</strong>: top-line KPIs, trend views, payment evidence.</li>
                    <li><strong>Odoo vs Ramp</strong>: source comparison and payment accounting split.</li>
                    <li><strong>Spend Timeline</strong>: monthly/weekly trend and source-mix drilldowns.</li>
                    <li><strong>Transactions (All)</strong>: complete row-level transaction ledger.</li>
                </ul>
            </div>
            <div class="about-card">
                <h4>Analysis</h4>
                <ul>
                    <li><strong>Station Drilldown</strong>: station-level spend, vendors, and BOM detail.</li>
                    <li><strong>Vendor Analysis</strong>: concentration and vendor-to-station patterns.</li>
                    <li><strong>Asset Tracking</strong>: equipment lifecycle and installation status.</li>
                    <li><strong>Materials &amp; Spares</strong>: deduped catalog, pricing, and sourcing.</li>
                </ul>
            </div>
            <div class="about-card">
                <h4>Planning &amp; Cashflow</h4>
                <ul>
                    <li><strong>Forecasting</strong>: line and station forecast baselines.</li>
                    <li><strong>Payment Timeline</strong>: PO payment sequence and timing behavior.</li>
                    <li><strong>Milestone Templates</strong>: expected payment plans by PO.</li>
                    <li><strong>Cashflow Forecast</strong>: actual paid vs projected outflow.</li>
                </ul>
        </div>
            <div class="about-card">
                <h4>Admin</h4>
                <ul>
                    <li><strong>Settings</strong>: data refresh, filters, AI prompts, and operations.</li>
                    <li><strong>Classification Review</strong>: resolve AI/rules mismatches.</li>
                    <li><strong>About &amp; Operating Guide</strong>: workflow, trust model, limitations.</li>
                </ul>
    </div>
        </div>
    </div>

    <div class="chart-card full" style="margin-bottom:14px">
        <h3>Data Flow (High Level)</h3>
            <div class="about-flow">
                <div class="about-flow-row">
                <div class="about-node"><h5>Inputs</h5><p>Odoo PO + billing/payment context, Ramp spend feed, manual additions, and settings controls.</p></div>
                    <div class="about-arrow">&#8594;</div>
                <div class="about-node"><h5>Processing</h5><p>Cleanup, dedupe, station mapping, subcategory classification, and payment status reconciliation.</p></div>
                    <div class="about-arrow">&#8594;</div>
                <div class="about-node"><h5>Outputs</h5><p>Unified analytics tables that power dashboard charts, drilldowns, and exportable tables.</p></div>
                    <div class="about-arrow">&#8594;</div>
                <div class="about-node"><h5>Planning Layer</h5><p>Template milestones + actual payment events roll into timeline and cashflow forecasts.</p></div>
                </div>
            </div>
        </div>

    <div class="about-rules">
        <div class="about-card">
            <h4>Operator Runbook</h4>
            <div class="about-timeline">
                <div class="about-step"><div class="num">1</div><div class="body"><strong>Refresh data</strong>Run incremental refresh from <code>Settings &rarr; Data Management</code> before analysis sessions.</div></div>
                <div class="about-step"><div class="num">2</div><div class="body"><strong>Verify coverage</strong>Check Executive Summary and Odoo vs Ramp splits for source balance and obvious anomalies.</div></div>
                <div class="about-step"><div class="num">3</div><div class="body"><strong>Review classification</strong>Use <code>Classification Review</code> to accept/fix mismatches and improve future runs.</div></div>
                <div class="about-step"><div class="num">4</div><div class="body"><strong>Maintain templates</strong>Keep <code>Milestone Templates</code> aligned to expected payment terms for major POs.</div></div>
                <div class="about-step"><div class="num">5</div><div class="body"><strong>Publish forecast</strong>Use <code>Cashflow Forecast</code> + drilldowns and export snapshots for stakeholder reporting.</div></div>
        </div>
    </div>

        <div class="about-card">
            <h4>Data Trust Model</h4>
            <ul>
                <li><strong>High confidence</strong>: raw identifiers, amounts, PO/vendor fields, and posted payment evidence.</li>
                <li><strong>Medium confidence</strong>: mapped station/subcategory when rule signals are partial.</li>
                <li><strong>Scenario confidence</strong>: projected cashflow from templates and expected dates.</li>
                <li><strong>Control point</strong>: human edits and review decisions always take precedence over auto output.</li>
            </ul>
        <details class="about-details">
                <summary>Show technical reference</summary>
            <div class="about-details-body">
                <table class="about-tech-table">
                    <thead>
                            <tr><th>Layer</th><th>Key logic</th><th>Purpose</th></tr>
                    </thead>
                    <tbody>
                            <tr><td>Extract</td><td><span class="about-code">po_by_creators_last_7m.sql</span></td><td>Scoping by configured creators and recent date window.</td></tr>
                            <tr><td>Cleanup</td><td><span class="about-code">step4_clean_odoo()</span></td><td>Standardized text/date/amount fields for consistent analytics.</td></tr>
                            <tr><td>Mapping</td><td><span class="about-code">step7_map_stations()</span></td><td>Station assignment and confidence scoring.</td></tr>
                            <tr><td>Overrides</td><td><span class="about-code">step8_apply_overrides()</span></td><td>Human corrections override auto mappings.</td></tr>
                            <tr><td>Classification</td><td><span class="about-code">step9_classify_subcategories()</span></td><td>Manufacturing subcategory assignment.</td></tr>
                            <tr><td>Export</td><td><span class="about-code">step10_export()</span></td><td>Materialized outputs used by dashboard APIs.</td></tr>
                    </tbody>
                </table>
            </div>
        </details>
    </div>
    </div>
</div>

<!-- ODOO vs RAMP -->
<div class="page" id="page-source">
    <div class="page-header"><div><div class="page-title">Odoo vs Ramp Comparison</div><div class="page-subtitle">Purchase Orders vs Credit Card spend breakdown</div></div></div>
    <div class="kpi-row" id="source-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>Odoo PO - Sub-Categories</h3><div id="chart-odoo-subcats"></div></div>
        <div class="chart-card"><h3>Ramp CC - Sub-Categories</h3><div id="chart-ramp-subcats"></div></div>
        <div class="chart-card"><h3>Odoo PO Billing</h3><div id="chart-src-odoo-billing"></div></div>
        <div class="chart-card"><h3>Ramp CC Accounting</h3><div id="chart-src-ramp-billing"></div></div>
        <div class="chart-card full"><h3>Monthly Spend by Source</h3><div id="chart-source-monthly"></div></div>
        <div class="chart-card full"><h3>Payment Evidence (PO-Level)</h3><div id="source-payment-evidence-wrap"></div></div>
    </div>
</div>

<!-- STATIONS -->
<div class="page" id="page-stations">
    <div class="page-header"><div><div class="page-title">Station Drilldown</div><div class="page-subtitle">Select a station to view detailed spend and materials</div></div></div>
    <div class="filter-bar">
        <label>Station:</label>
        <select class="station-select" id="stationSelect" onchange="loadStationDetail()"></select>
    </div>
    <div class="kpi-row" id="station-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>Vendor Breakdown</h3><div id="chart-station-vendors"></div></div>
        <div class="chart-card"><h3>Order Timeline</h3><div id="chart-station-timeline"></div></div>
        <div class="chart-card full"><h3>Materials / BOM</h3><div id="station-bom-table"></div></div>
    </div>
</div>

<!-- FORECASTING -->
<div class="page" id="page-forecasting">
    <div class="page-header"><div><div class="page-title">Forecasting</div><div class="page-subtitle">Edit station forecasts grouped by line and refresh from BF1/BF2 Google Sheets</div></div></div>
    <div class="chart-card" style="margin-bottom:12px">
        <h3>Forecast Sources</h3>
        <div style="display:grid;grid-template-columns:120px 1fr;gap:8px;align-items:center">
            <label style="font-size:12px;color:var(--muted)">BF1 Sheet URL</label>
            <input id="forecast-bf1-url" class="forecast-input" style="width:100%;text-align:left" placeholder="https://docs.google.com/spreadsheets/d/..."/>
            <label style="font-size:12px;color:var(--muted)">BF2 Sheet URL</label>
            <input id="forecast-bf2-url" class="forecast-input" style="width:100%;text-align:left" placeholder="https://docs.google.com/spreadsheets/d/..."/>
        </div>
        <div style="margin-top:12px;display:flex;gap:10px;align-items:center">
            <button class="btn-refresh" id="forecast-refresh-btn" style="width:auto;padding:10px 24px" onclick="refreshForecastFromSheets()">Refresh from Sheets</button>
            <button class="btn-refresh" style="width:auto;padding:10px 24px;background:var(--surface2);color:var(--text);border:1px solid var(--border)" onclick="reauthGoogleForSheets()">Re-auth Google</button>
            <button class="btn-refresh" style="width:auto;padding:10px 24px" onclick="saveForecastingBulk()">Save Forecasts</button>
            <button class="btn-refresh" style="width:auto;padding:10px 24px;background:var(--surface2);color:var(--text);border:1px solid var(--border)" onclick="lockAllForecastOverrides()">Lock All</button>
            <button class="btn-refresh" style="width:auto;padding:10px 24px;background:var(--surface2);color:var(--text);border:1px solid var(--border)" onclick="unlockAllForecastOverrides()">Unlock All</button>
            <span id="forecast-ok" class="forecast-saved" style="display:none">Forecasts saved</span>
        </div>
        <div id="forecast-refresh-msg" style="margin-top:10px;font-size:12px;color:var(--muted)"></div>
    </div>
    <div id="forecast-table-wrap"></div>
</div>

<!-- VENDORS -->
<div class="page" id="page-vendors">
    <div class="page-header"><div><div class="page-title">Vendor Analysis</div><div class="page-subtitle">Spend concentration and vendor-station relationships</div></div></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>Top Vendor Concentration</h3><div id="chart-vendor-conc"></div></div>
        <div class="chart-card"><h3>Vendor-Station Spend (Top 10 Vendors x Top Stations)</h3><div id="chart-vendor-heatmap"></div></div>
        <div class="chart-card full"><h3>All Vendors</h3><div id="vendor-table-wrap"></div></div>
    </div>
</div>

<!-- ASSETS -->
<div class="page" id="page-assets">
    <div class="page-header"><div><div class="page-title">Asset Tracking</div><div class="page-subtitle">Station-level capital asset register — physical equipment on the floor</div></div></div>
    <div class="filter-bar" id="asset-filters">
        <label>Owner:</label><select id="assetOwnerFilter" onchange="filterAssets()"><option value="">All Owners</option></select>
        <label>Status:</label><select id="assetStatusFilter" onchange="filterAssets()"><option value="">All</option><option value="Ordered">Ordered</option><option value="Shipped">Shipped</option><option value="Received">Received</option><option value="Installed">Installed</option><option value="Commissioned">Commissioned</option></select>
        <label>Vendor:</label><select id="assetVendorFilter" onchange="filterAssets()"><option value="">All Vendors</option></select>
        <label>Show:</label>
        <span style="display:inline-flex;gap:2px">
            <button class="asset-mode-btn active" id="assetModeAsset" onclick="setAssetMode('asset')">Asset Value</button>
            <button class="asset-mode-btn" id="assetModeTotal" onclick="setAssetMode('total')">Total Investment</button>
        </span>
    </div>
    <div id="asset-subcat-chips" style="margin-bottom:8px;display:flex;flex-wrap:wrap;gap:6px"></div>
    <div class="kpi-row" id="asset-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card full"><h3>Station Investment</h3><div id="chart-asset-bars"></div></div>
        <div class="chart-card"><h3>Station Status</h3><div id="chart-asset-delivery"></div></div>
        <div class="chart-card"><h3>Spend Composition</h3><div id="chart-asset-composition"></div></div>
    </div>
    <div class="chart-card full" style="margin-top:12px"><h3>Station Asset Register</h3><div id="asset-table-wrap"></div></div>
</div>

<!-- SPARES -->
<div class="page" id="page-spares">
    <div class="page-header"><div><div class="page-title">Materials &amp; Spares Catalog</div><div class="page-subtitle">Deduplicated items with part numbers and sourcing info</div></div></div>
    <div class="filter-bar" id="spares-filters">
        <label>Bucket:</label><select id="sparesBucketFilter" onchange="filterSpares()"><option value="">All Buckets</option></select>
        <label>Station:</label><select id="sparesStationFilter" onchange="filterSpares()"><option value="">All Stations</option></select>
        <label>Sub-Category:</label><select id="sparesSubcatFilter" onchange="filterSpares()"><option value="">All Sub-Categories</option></select>
        <label>Category:</label><select id="sparesCatFilter" onchange="filterSpares()"><option value="">All Categories</option></select>
        <label>Vendor:</label><select id="sparesVendorFilter" onchange="filterSpares()"><option value="">All Vendors</option></select>
    </div>
    <div id="spares-bucket-summary" style="margin-bottom:16px"></div>
    <div id="spares-table-wrap"></div>
</div>

<!-- DETAIL -->
<div class="page" id="page-detail">
    <div class="page-header"><div><div class="page-title">Full Transaction Detail</div><div class="page-subtitle">All CAPEX line items from Odoo POs and Ramp credit card</div></div>
        <button id="btn-detail-expand" class="btn-refresh" style="width:auto;padding:8px 16px;background:var(--surface2);color:var(--text);border:1px solid var(--border)" onclick="toggleDetailExpand()">Expand Table</button>
    </div>
    <div id="detail-table-wrap"></div>
</div>

<!-- TIMELINE -->
<div class="page" id="page-timeline">
    <div class="page-header"><div><div class="page-title">Spend Timeline</div><div class="page-subtitle">Temporal patterns and cumulative spend tracking</div></div></div>
    <div class="chart-grid">
        <div class="chart-card full"><h3>Cumulative Spend (S-Curve)</h3><div id="chart-cumulative"></div></div>
        <div class="chart-card full"><h3>Monthly Spend by Source</h3><div id="chart-timeline-source"></div></div>
        <div class="chart-card full"><h3>Weekly Spend (bar height = total, color = intensity)</h3><div id="chart-weekly"></div></div>
        <div class="chart-card full"><h3>Monthly Spend by Sub-Category</h3><div id="chart-monthly-subcat"></div></div>
        <div class="chart-card full"><h3>Monthly Spend by GL Category (legacy)</h3><div id="chart-monthly-cat"></div></div>
    </div>
</div>

<!-- OTHER PROJECTS -->
<div class="page" id="page-projects">
    <div class="page-header"><div><div class="page-title">Other Projects</div><div class="page-subtitle">NPI, Pilot, Facilities, Quality, IT, Maintenance, and unmapped spend</div></div></div>
    <div class="kpi-row" id="proj-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>Spend by Project</h3><div id="chart-proj-breakdown"></div></div>
        <div class="chart-card"><h3>Top Vendors (Non-Production)</h3><div id="chart-proj-vendors"></div></div>
        <div class="chart-card full"><h3>Monthly Non-Production Spend</h3><div id="chart-proj-monthly"></div></div>
        <div class="chart-card full"><h3>Transaction Detail</h3><div id="proj-detail-wrap"></div></div>
    </div>
</div>

<!-- UNIT ECONOMICS -->
<div class="page" id="page-uniteco">
    <div class="page-header"><div><div class="page-title">Unit Economics</div><div class="page-subtitle">$/GWh and ft&sup2;/GWh by production line (configure capacities in Settings)</div></div></div>
    <div class="kpi-row" id="ue-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card"><h3>$/GWh by Line (Forecast)</h3><div id="chart-ue-dollar"></div></div>
        <div class="chart-card"><h3>Forecast Spend by Line</h3><div id="chart-ue-compare"></div></div>
        <div class="chart-card"><h3>ft&sup2;/GWh by Line</h3><div id="chart-ue-sqft"></div></div>
        <div class="chart-card"><h3>$/GWh Composition (Forecast)</h3><div id="chart-ue-stack"></div></div>
        <div class="chart-card full"><h3>Line Detail (Forecast Basis)</h3><div id="ue-table-wrap"></div></div>
    </div>
</div>

<!-- V2: CLASSIFICATION REVIEW QUEUE -->
<div class="page" id="page-v2reviews">
    <div class="page-header"><div><div class="page-title">Classification Review Queue</div><div class="page-subtitle">LLM disagreements with rule engine &mdash; sorted by dollar impact</div></div>
        <div style="display:flex;gap:8px">
            <button class="btn-refresh" style="width:auto;padding:8px 18px" onclick="triggerLLMReview()">Run LLM Review</button>
            <button class="btn-refresh" style="width:auto;padding:8px 18px;background:var(--surface2);color:var(--text)" onclick="loadReviews()">Refresh</button>
        </div>
    </div>
    <div class="kpi-row" id="review-kpis"></div>
    <div class="chart-card full">
        <h3>Pending Reviews <span id="review-count" style="font-size:12px;color:var(--muted);font-weight:400"></span></h3>
        <div id="review-table-wrap" style="overflow-x:auto"></div>
    </div>
</div>

<!-- V2: PAYMENT MILESTONES -->
<div class="page" id="page-v2milestones">
    <div class="page-header"><div><div class="page-title">Payment Timeline</div><div class="page-subtitle">PO payment timelines &mdash; from creation to final payment</div></div>
        <button class="btn-refresh" style="width:auto;padding:8px 18px" onclick="loadMilestones()">Refresh</button>
    </div>
    <div class="kpi-row" id="milestone-kpis"></div>
    <div id="milestone-note" style="margin:0 20px 12px;padding:10px 12px;background:var(--surface2);border:1px solid var(--border);border-radius:8px;font-size:11px;color:var(--muted)"></div>
    <div class="chart-card full"><h3>PO Payment Timeline</h3><div id="chart-po-timeline" style="min-height:500px;max-height:80vh;overflow-y:auto"></div></div>
    <div class="chart-card full"><h3>PO Payment Ledger (Raw Events)</h3><div id="vendor-profile-table-wrap" style="overflow-x:auto"></div></div>
    <div class="chart-card full"><h3>Vendor Payment Profiles</h3><div id="line-profile-table-wrap" style="overflow-x:auto"></div></div>
</div>

<!-- V2: PAYMENT TEMPLATES -->
<div class="page" id="page-v2templates">
    <div class="page-header"><div><div class="page-title">Milestone Templates</div><div class="page-subtitle">Define payment milestone schedules per PO for cashflow projection</div></div>
        <div style="display:flex;gap:8px">
            <button class="btn-refresh" style="width:auto;padding:8px 18px;background:var(--surface2);color:var(--text)" onclick="showNewTemplateForm()">+ New Template</button>
        </div>
    </div>
    <div id="gen-milestone-status" style="display:none;padding:8px 16px;margin-bottom:12px;font-size:12px;border-radius:6px;background:var(--surface2)"></div>
    <div class="chart-card full">
        <h3>Template Library <span id="template-count" style="font-size:12px;color:var(--muted);font-weight:400"></span></h3>
        <div id="template-table-wrap" style="overflow-x:auto"></div>
    </div>
    <div class="chart-card full" id="template-editor" style="display:none">
        <h3 id="template-editor-title">New Payment Template</h3>
        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:16px;align-items:flex-end">
            <div style="position:relative"><label style="font-size:11px;color:var(--muted)">Search PO (number, vendor, or buyer)</label><br>
                <input class="forecast-input" id="tpl-po-search" style="width:360px" placeholder="Type to search..." oninput="filterPoList()" onfocus="document.getElementById('tpl-po-dropdown').style.display='block'">
                <input type="hidden" id="tpl-po">
                <div id="tpl-po-dropdown" style="display:none;position:absolute;top:100%;left:0;width:500px;max-height:280px;overflow-y:auto;background:var(--surface);border:1px solid var(--border);border-radius:8px;z-index:50;box-shadow:0 8px 24px rgba(0,0,0,.4);margin-top:2px"></div>
            </div>
            <div><label style="font-size:11px;color:var(--muted)">Vendor</label><br><input class="forecast-input" id="tpl-vendor" style="width:200px" readonly></div>
            <div><label style="font-size:11px;color:var(--muted)">PO Total</label><br><input class="forecast-input" id="tpl-total" style="width:140px;font-weight:700;color:var(--green)" readonly></div>
            <div><label style="font-size:11px;color:var(--muted)">Template Name</label><br><input class="forecast-input" id="tpl-name" style="width:200px" placeholder="e.g. Fanuc deposit schedule"></div>
        </div>
        <p style="font-size:12px;color:var(--muted);margin-bottom:12px">Define each payment milestone. Dollar amounts auto-calculate from PO total &times; percentage.</p>
        <table style="width:100%;border-collapse:collapse;margin-bottom:8px" id="tpl-ms-table">
            <thead><tr style="border-bottom:1px solid var(--border)">
                <th style="text-align:left;padding:6px 8px;color:var(--muted);font-size:10px;text-transform:uppercase;width:160px">Milestone</th>
                <th style="text-align:left;padding:6px 8px;color:var(--muted);font-size:10px;text-transform:uppercase;width:160px">Expected Date</th>
                <th style="text-align:right;padding:6px 8px;color:var(--muted);font-size:10px;text-transform:uppercase;width:80px">% of PO</th>
                <th style="text-align:right;padding:6px 8px;color:var(--muted);font-size:10px;text-transform:uppercase;width:110px">Amount</th>
                <th style="width:40px"></th>
            </tr></thead>
            <tbody id="tpl-milestones"></tbody>
            <tfoot><tr style="border-top:2px solid var(--border)">
                <td style="padding:8px;font-weight:700;font-size:12px" colspan="2">Total</td>
                <td style="padding:8px;text-align:right;font-weight:700;font-size:12px"><span id="tpl-pct-total">0</span>%</td>
                <td style="padding:8px;text-align:right;font-weight:700;font-size:12px;color:var(--green)"><span id="tpl-amt-total">$0</span></td>
                <td></td>
            </tr></tfoot>
        </table>
        <button class="btn-refresh" style="width:auto;padding:6px 14px;background:var(--surface2);color:var(--text);font-size:12px" onclick="addMilestoneRow()">+ Add Milestone</button>
        <div style="margin-top:16px;display:flex;gap:10px;align-items:center">
            <button class="btn-refresh" style="width:auto;padding:10px 24px" onclick="saveTemplate()">Save Template</button>
            <button class="btn-refresh" style="width:auto;padding:10px 24px;background:var(--surface2);color:var(--text)" onclick="hideTemplateEditor()">Cancel</button>
            <span id="tpl-saved" class="forecast-saved" style="display:none">Template saved</span>
        </div>
    </div>
</div>

<!-- V2: AI RFQ GENERATOR -->
<div class="page" id="page-airfq">
    <div class="page-header"><div><div class="page-title">AI-RFQ Gen</div><div class="page-subtitle">Generate Odoo-ready RFQ CSV from vendor quote PDF with validation and preview</div></div>
        <div style="display:flex;gap:8px">
            <button class="btn-refresh" style="width:auto;padding:8px 18px;background:var(--surface2);color:var(--text)" onclick="loadAirRfq(true)">Refresh Lookups</button>
        </div>
    </div>
    <div class="chart-grid">
        <div class="chart-card">
            <h3>Quote Inputs</h3>
            <div style="display:grid;grid-template-columns:1fr;gap:10px">
                <div>
                    <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Vendor</label>
                    <input class="forecast-input" id="airfq-vendor" list="airfq-vendors-list" placeholder="e.g. Balluff" style="width:100%">
                    <datalist id="airfq-vendors-list"></datalist>
                </div>
                <div>
                    <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Reference PO (optional)</label>
                    <input class="forecast-input" id="airfq-reference-po" placeholder="e.g. PO11808" style="width:100%">
                </div>
                <div>
                    <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Saved RFQs</label>
                    <div style="display:flex;gap:8px;align-items:center">
                        <select id="airfq-history-select" class="forecast-input" style="flex:1;min-width:0"></select>
                        <button class="btn-refresh" style="width:auto;padding:8px 10px;background:var(--surface);color:var(--text);border:1px solid var(--border)" onclick="loadSelectedAirRfqHistory()">Load</button>
                    </div>
                </div>
                <div>
                    <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Quote PDF</label>
                    <input id="airfq-pdf" type="file" accept=".pdf" style="width:100%;font-size:12px;color:var(--muted)">
                </div>
                <div>
                    <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Prompt / Update Instructions</label>
                    <textarea id="airfq-prompt" style="width:100%;height:140px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:10px;font-family:Consolas,monospace;font-size:12px;resize:vertical;line-height:1.45" placeholder="Describe RFQ intent. Example: Split into 3 lines qty 1 each and map to 3 different project codes."></textarea>
                </div>
                <div>
                    <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Payment Milestones Note (optional)</label>
                    <textarea id="airfq-payment-note" style="width:100%;height:70px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:10px;font-family:Consolas,monospace;font-size:12px;resize:vertical;line-height:1.4" placeholder="Example: 50% deposit with PO, 40% on FAT, 10% on SAT."></textarea>
                </div>
            </div>
            <div style="margin-top:10px;padding:10px;border:1px solid var(--border);border-radius:8px;background:var(--surface2)">
                <div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px">Header Pickers (Lookup-backed)</div>
                <div style="display:grid;grid-template-columns:repeat(3,minmax(180px,1fr));gap:8px">
                    <div>
                        <label style="font-size:10px;color:var(--muted)">Project</label>
                        <input class="forecast-input" id="airfq-header-project" list="airfq-projects-list" placeholder="Select project" style="width:100%">
                        <datalist id="airfq-projects-list"></datalist>
                    </div>
                    <div>
                        <label style="font-size:10px;color:var(--muted)">Deliver To</label>
                        <input class="forecast-input" id="airfq-header-deliver" list="airfq-deliver-list" placeholder="Select receiving operation" style="width:100%">
                        <datalist id="airfq-deliver-list"></datalist>
                    </div>
                    <div>
                        <label style="font-size:10px;color:var(--muted)">Default Tax (new/empty lines)</label>
                        <input class="forecast-input" id="airfq-default-tax" list="airfq-taxes-list" placeholder="Select tax label" style="width:100%">
                        <datalist id="airfq-taxes-list"></datalist>
                    </div>
                </div>
                <div style="margin-top:8px">
                    <button class="btn-refresh" style="width:auto;padding:8px 14px;background:var(--surface);color:var(--text);border:1px solid var(--border)" onclick="applyAirRfqHeaderEdits()">Apply Header Picks</button>
                </div>
            </div>
            <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:12px">
                <button class="btn-refresh" style="width:auto;padding:10px 16px" onclick="generateAirRfq(false)" id="btn-airfq-generate">AI Generate</button>
                <button class="btn-refresh" style="width:auto;padding:10px 16px;background:var(--surface);color:var(--text);border:1px solid var(--border)" onclick="generateAirRfq(true)" id="btn-airfq-regenerate">Regenerate</button>
                <button class="btn-refresh" style="width:auto;padding:10px 16px;background:var(--surface2);color:var(--text)" onclick="downloadAirRfqCsv()" id="btn-airfq-download" disabled>Download CSV</button>
                <button class="btn-refresh" style="width:auto;padding:10px 16px;background:var(--surface2);color:var(--text)" onclick="resetAirRfqForm()">Reset</button>
            </div>
            <div id="airfq-status" style="margin-top:10px;font-size:11px;color:var(--muted);line-height:1.4"></div>
        </div>
        <div class="chart-card">
            <h3>Validation Summary</h3>
            <div id="airfq-validation" style="font-size:12px;color:var(--text);line-height:1.45"></div>
            <div style="margin-top:12px;padding:10px;border:1px solid var(--border);border-radius:8px;background:var(--surface2)">
                <div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px">How to import CSV in Odoo</div>
                <ol style="margin:0;padding-left:18px;font-size:12px;line-height:1.55;color:var(--text)">
                    <li>Open <a href="https://erp.basepowercompany.com/odoo/purchase" target="_blank" rel="noopener" style="color:var(--green)">Odoo Purchase</a>.</li>
                    <li>In <strong>Request for Quotation</strong>, click the gear icon (<strong>Actions</strong>).</li>
                    <li>Select <strong>Import Records</strong>.</li>
                    <li>Click <strong>Upload Data file</strong>.</li>
                    <li>Browse and load your generated CSV.</li>
                    <li>Click <strong>Test</strong> to validate data mapping.</li>
                    <li>If test passes, click <strong>Import</strong>.</li>
                </ol>
            </div>
        </div>
        <div class="chart-card full">
            <h3>Line Editor</h3>
            <p style="font-size:11px;color:var(--muted);margin-bottom:8px">Edit line splits, quantities, projects, and taxes before import. Then click <strong>Apply Line Edits</strong>.</p>
            <div style="margin-bottom:10px;padding:10px;border:1px solid var(--border);border-radius:8px;background:var(--surface2)">
                <div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px">Global Column Apply (all rows)</div>
                <div style="display:grid;grid-template-columns:repeat(4,minmax(170px,1fr));gap:8px">
                    <input class="forecast-input" id="airfq-bulk-product" placeholder="Product (overwrite all)">
                    <input class="forecast-input" id="airfq-bulk-project" list="airfq-projects-list" placeholder="Project (overwrite all)">
                    <input class="forecast-input" id="airfq-bulk-uom" placeholder="UoM (overwrite all)">
                    <input class="forecast-input" id="airfq-bulk-tax" list="airfq-taxes-list" placeholder="Tax (overwrite all)">
                    <input class="forecast-input" id="airfq-bulk-qty" type="number" step="0.01" min="0" placeholder="Qty (overwrite all)">
                    <input class="forecast-input" id="airfq-bulk-price" type="number" step="0.01" min="0" placeholder="Unit price (overwrite all)">
                    <input class="forecast-input" id="airfq-bulk-desc-prefix" placeholder="Description prefix (prepend)">
                    <input class="forecast-input" id="airfq-bulk-desc-suffix" placeholder="Description suffix (append)">
                </div>
                <div style="margin-top:8px">
                    <button class="btn-refresh" style="width:auto;padding:8px 14px;background:var(--surface);color:var(--text);border:1px solid var(--border)" onclick="applyAirRfqBulkEdits()">Apply Global to All Rows</button>
                </div>
            </div>
            <div id="airfq-line-editor"></div>
            <div style="margin-top:8px">
                <button class="btn-refresh" style="width:auto;padding:8px 14px;background:var(--surface);color:var(--text);border:1px solid var(--border)" onclick="applyAirRfqLineEdits()">Apply Line Edits</button>
            </div>
        </div>
        <div class="chart-card full">
            <h3>RFQ Preview (Odoo-like)</h3>
            <div id="airfq-preview"></div>
        </div>
        <div class="chart-card full">
            <h3>Generated CSV</h3>
            <textarea id="airfq-csv" readonly style="width:100%;height:220px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:10px;font-family:Consolas,monospace;font-size:11px;resize:vertical;line-height:1.35"></textarea>
        </div>
    </div>
</div>

<!-- V2: CASHFLOW PROJECTION -->
<div class="page" id="page-v2cashflow">
    <div class="page-header"><div><div class="page-title">Cashflow Forecast</div><div class="page-subtitle">Expected cash outflows &mdash; actual payments + projected from templates</div></div>
        <div style="display:flex;gap:8px;align-items:center">
            <label style="font-size:11px;color:var(--muted)">View</label>
            <select id="cf-granularity" class="forecast-input" style="width:130px;text-align:left" onchange="loadCashflow()">
                <option value="month" selected>Month</option>
                <option value="workweek">Workweek</option>
                <option value="quarter">Quarter</option>
            </select>
        </div>
    </div>
    <div class="kpi-row" id="cashflow-kpis"></div>
    <div class="chart-grid">
        <div class="chart-card full"><h3 id="cf-main-title">Monthly Cash Outflow</h3><div id="chart-cf-monthly" style="min-height:350px"></div></div>
        <div class="chart-card full"><h3>Cumulative Spend Curve</h3><div id="chart-cf-cumulative" style="min-height:350px"></div></div>
    </div>
    <div class="chart-card full"><h3>Cashflow Event Ledger</h3><div id="cf-weekly-table-wrap" style="overflow-x:auto"></div></div>
</div>

<!-- SETTINGS -->
<div class="page" id="page-settings">
    <div class="page-header"><div><div class="page-title">Settings</div><div class="page-subtitle">Configure pipeline, data sources, and line capacities</div></div></div>
    <div class="chart-card" style="max-width:900px">
        <h3>Access Control</h3>
        <p id="settings-access-summary" style="font-size:12px;color:var(--muted);margin-bottom:12px">Loading access policy...</p>
        <div style="display:grid;grid-template-columns:1fr;gap:12px">
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Settings Owner (full control)</label>
                <input id="settings-owner-email" type="email" style="width:100%;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:10px 12px;font-size:12px" placeholder="owner@basepowercompany.com"/>
            </div>
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Settings Editors (one email per line)</label>
                <textarea id="settings-editor-emails" style="width:100%;height:120px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:monospace;font-size:12px;resize:vertical;line-height:1.5" placeholder="editor1@basepowercompany.com&#10;editor2@basepowercompany.com"></textarea>
                <div style="font-size:11px;color:var(--muted);margin-top:6px">Editors can modify major settings and run data refresh/upload operations. Owner can also manage this access list.</div>
            </div>
            <div style="display:flex;align-items:flex-start;gap:12px;margin-top:8px">
                <input type="checkbox" id="settings-restrict-access" style="margin-top:4px;accent-color:var(--green)"/>
                <div>
                    <label for="settings-restrict-access" style="font-size:13px;font-weight:600;cursor:pointer">Restrict access to owners and editors only</label>
                    <div style="font-size:11px;color:var(--muted);margin-top:4px">When enabled, only the owner and editors above can access the dashboard. Others will see an &quot;Access denied&quot; page with an option to request access.</div>
                </div>
            </div>
        </div>
    </div>
    <div class="chart-card" style="max-width:900px">
        <h3>Operations Automation</h3>
        <p style="font-size:12px;color:var(--muted);margin-bottom:12px">Define the scheduler cadence and alert recipients used for cloud operations setup.</p>
        <div style="display:grid;grid-template-columns:repeat(2,minmax(220px,1fr));gap:12px;margin-bottom:12px">
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Daily Refresh Cron (UTC)</label>
                <input id="settings-refresh-cron" type="text" style="width:100%;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:10px 12px;font-size:12px" placeholder="0 8 * * *"/>
            </div>
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Scheduler Timezone</label>
                <input id="settings-refresh-timezone" type="text" style="width:100%;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:10px 12px;font-size:12px" placeholder="Etc/UTC"/>
            </div>
        </div>
        <div>
            <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Alert Emails (one per line)</label>
            <textarea id="settings-alert-emails" style="width:100%;height:100px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:monospace;font-size:12px;resize:vertical;line-height:1.5" placeholder="you@basepowercompany.com"></textarea>
        </div>
    </div>
    <div class="chart-card" style="max-width:800px">
        <h3>Data Management</h3>
        <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:16px">
            <div style="flex:1;min-width:220px;background:var(--surface2);border-radius:8px;padding:16px">
                <div style="font-size:13px;font-weight:600;margin-bottom:6px">Refresh from Odoo</div>
                <p style="font-size:11px;color:var(--muted);margin-bottom:10px">Pull fresh PO + payment data from BigQuery and merge incrementally (preserves classifications).</p>
                <button class="btn-refresh" style="width:100%;padding:10px" onclick="refreshFromOdoo()" id="btn-refresh-odoo">Refresh Data</button>
                <div id="refresh-status" style="font-size:11px;color:var(--muted);margin-top:8px;display:none"></div>
            </div>
            <div style="flex:1;min-width:220px;background:var(--surface2);border-radius:8px;padding:16px">
                <div style="font-size:13px;font-weight:600;margin-bottom:6px">Upload Ramp CSV</div>
                <p style="font-size:11px;color:var(--muted);margin-bottom:10px">Upload a new Ramp export. Duplicates are skipped automatically; only new transactions are appended.</p>
                <input type="file" id="ramp-csv-file" accept=".csv" style="font-size:11px;color:var(--muted);margin-bottom:8px;width:100%">
                <button class="btn-refresh" style="width:100%;padding:10px;background:var(--surface);color:var(--text);border:1px solid var(--border)" onclick="uploadRampCsv()" id="btn-upload-ramp">Upload &amp; Append</button>
                <div id="upload-status" style="font-size:11px;color:var(--muted);margin-top:8px;display:none"></div>
            </div>
            <div style="flex:1;min-width:220px;background:var(--surface2);border-radius:8px;padding:16px">
                <div style="font-size:13px;font-weight:600;margin-bottom:6px">Manual PO Entry</div>
                <p style="font-size:11px;color:var(--muted);margin-bottom:10px">Add or edit manual PO line items that aren't in Odoo or Ramp.</p>
                <button class="btn-refresh" style="width:100%;padding:10px;background:var(--surface);color:var(--text);border:1px solid var(--border)" onclick="window.open('/api/v2/manual-po-redirect','_blank')">Open Manual PO Form</button>
            </div>
            <div style="flex:1;min-width:220px;background:var(--surface2);border-radius:8px;padding:16px">
                <div style="font-size:13px;font-weight:600;margin-bottom:6px">AI Operations</div>
                <p style="font-size:11px;color:var(--muted);margin-bottom:10px">Run AI tasks from settings. Milestone generation appends only new POs and preserves existing templates.</p>
                <div style="display:flex;gap:8px;flex-direction:column">
                    <button class="btn-refresh" style="width:100%;padding:10px" onclick="openClassificationReviewFromSettings()">Open Classification Review</button>
                    <button class="btn-refresh" style="width:100%;padding:10px;background:var(--surface);color:var(--text);border:1px solid var(--border)" onclick="generateMilestoneTemplates()" id="btn-gen-milestones">Generate AI Milestones (Append New Only)</button>
                </div>
                <div id="gen-milestone-status-settings" style="display:none;font-size:11px;color:var(--muted);margin-top:8px"></div>
            </div>
        </div>
    </div>
    <div class="chart-card" style="max-width:800px">
        <h3>PO Creator Names (Buyer Filter)</h3>
        <p style="font-size:12px;color:var(--muted);margin-bottom:12px">These names control which POs are pulled from Odoo. Only POs created by people in this list are included in the dashboard. One name per line, case-insensitive.</p>
        <textarea id="settings-creators" style="width:100%;height:200px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:monospace;font-size:12px;resize:vertical;line-height:1.6" placeholder="Loading..."></textarea>
        <div style="margin-top:8px;display:flex;gap:10px;align-items:center">
            <span id="creator-count" style="font-size:12px;color:var(--muted)"></span>
        </div>
    </div>
    <div class="chart-card" style="max-width:1000px">
        <h3>Milestone AI Prompt Settings</h3>
        <p style="font-size:12px;color:var(--muted);margin-bottom:12px">Control milestone generation behavior without code changes. Keep <code>{today}</code> in the system prompt; optional placeholders: <code>{program_context}</code>.</p>
        <div style="display:grid;grid-template-columns:1fr;gap:12px">
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Program Context (editable assumptions/schedule)</label>
                <textarea id="settings-milestone-program-context" style="width:100%;height:140px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:Consolas,monospace;font-size:12px;resize:vertical;line-height:1.5" placeholder="Example: Module 1 SOP window, Module 2 SOP window, INV1 SOP, supplier FAT/SAT dates"></textarea>
            </div>
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Milestone AI System Prompt Template</label>
                <textarea id="settings-milestone-system-prompt" style="width:100%;height:260px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:Consolas,monospace;font-size:12px;resize:vertical;line-height:1.5" placeholder="System prompt template used by the AI"></textarea>
            </div>
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Milestone AI User Prefix (optional)</label>
                <textarea id="settings-milestone-user-prefix" style="width:100%;height:90px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:Consolas,monospace;font-size:12px;resize:vertical;line-height:1.5" placeholder="Optional extra instruction prepended to each generation request"></textarea>
            </div>
        </div>
    </div>
    <div class="chart-card" style="max-width:1000px">
        <h3>Classification AI Prompt Settings</h3>
        <p style="font-size:12px;color:var(--muted);margin-bottom:12px">Use this only if you want stronger manufacturing context for station/sub-category classification. Optional placeholder in prompt: <code>{domain_context}</code>.</p>
        <div style="display:grid;grid-template-columns:1fr;gap:12px">
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Classification Domain Context</label>
                <textarea id="settings-classification-domain-context" style="width:100%;height:170px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:Consolas,monospace;font-size:12px;resize:vertical;line-height:1.5" placeholder="Base Power BF1 manufacturing context, line/station intent, and vendor patterns"></textarea>
            </div>
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">Classification AI System Prompt Template</label>
                <textarea id="settings-classification-system-prompt" style="width:100%;height:260px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:Consolas,monospace;font-size:12px;resize:vertical;line-height:1.5" placeholder="System prompt template used by classification review AI"></textarea>
            </div>
        </div>
    </div>
    <div class="chart-card" style="max-width:1000px">
        <h3>AI RFQ Prompt Settings</h3>
        <p style="font-size:12px;color:var(--muted);margin-bottom:12px">These settings control PDF-to-RFQ generation, validation mode, and deterministic regeneration behavior.</p>
        <div style="display:grid;grid-template-columns:repeat(2,minmax(240px,1fr));gap:12px;margin-bottom:12px">
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">RFQ AI Provider</label>
                <select id="settings-rfq-provider" class="forecast-input" style="width:100%;text-align:left">
                    <option value="gemini">gemini</option>
                    <option value="openai">openai</option>
                    <option value="anthropic">anthropic</option>
                </select>
            </div>
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">RFQ Validation Mode</label>
                <select id="settings-rfq-validation-mode" class="forecast-input" style="width:100%;text-align:left">
                    <option value="bq_only">bq_only (default)</option>
                </select>
            </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr;gap:12px">
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">RFQ AI User Prefix (optional)</label>
                <textarea id="settings-rfq-user-prefix" style="width:100%;height:90px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:Consolas,monospace;font-size:12px;resize:vertical;line-height:1.45" placeholder="Optional instruction prepended to each RFQ generation request"></textarea>
            </div>
            <div>
                <label style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px">RFQ AI System Prompt Template</label>
                <textarea id="settings-rfq-system-prompt" style="width:100%;height:260px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:12px;font-family:Consolas,monospace;font-size:12px;resize:vertical;line-height:1.5" placeholder="System prompt template used by AI-RFQ Gen"></textarea>
            </div>
        </div>
    </div>
    <div class="chart-card" style="max-width:800px">
        <h3>Line Capacity &amp; Floor Area</h3>
        <p style="font-size:12px;color:var(--muted);margin-bottom:16px">Enter the GWh capacity and floor area (ft&sup2;) for each production line. These values are used to compute $/GWh and ft&sup2;/GWh metrics on the Unit Economics page.</p>
        <div id="settings-lines"></div>
    </div>
    <div style="margin-top:16px;display:flex;gap:10px;align-items:center;padding:0 20px">
        <button id="btn-save-settings" class="btn-refresh" style="width:auto;padding:10px 24px" onclick="saveSettings()">Save All Settings</button>
            <span id="settings-ok" class="forecast-saved" style="display:none">Settings saved</span>
    </div>
</div>

</div>
<div id="drill-overlay" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.6);z-index:900" onclick="closeDrill()"></div>
<div id="drill-panel" style="display:none;position:fixed;top:40px;right:20px;bottom:40px;width:65%;max-width:1000px;background:var(--surface);border:1px solid var(--border);border-radius:12px;z-index:901;overflow:hidden;box-shadow:0 8px 40px rgba(0,0,0,.5);flex-direction:column">
    <div style="padding:16px 20px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;flex-shrink:0">
        <div><div id="drill-title" style="font-size:16px;font-weight:700;color:var(--green)"></div><div id="drill-sub" style="font-size:11px;color:var(--muted);margin-top:2px"></div></div>
        <button onclick="closeDrill()" style="background:var(--surface2);color:var(--text);border:none;border-radius:6px;padding:6px 14px;cursor:pointer;font-size:12px;font-weight:600">Close</button>
    </div>
    <div style="flex:1;overflow:auto;padding:16px 20px" id="drill-body"></div>
</div>
<div class="toast" id="toast"></div>

<script>
const C={green:'#B2DD79',greenBright:'#D0F585',red:'#D1531D',yellow:'#F7C33C',blue:'#048EE5',surface:'#242422',surface2:'#32312F',text:'#F0EEEB',muted:'#9E9C98',border:'#3E3D3A'};
const PL={paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2}};
const GRAPH_EXPORT_BUTTONS=[
    {
        name:'Export chart data as CSV',
        icon:Plotly.Icons.disk,
        click:function(gd){ exportGraphData(gd.id,'csv'); },
    },
    {
        name:'Export chart data as Excel',
        icon:Plotly.Icons.disk,
        click:function(gd){ exportGraphData(gd.id,'xlsx'); },
    },
];
const PC={responsive:true,displayModeBar:true,displaylogo:false,modeBarButtonsToAdd:GRAPH_EXPORT_BUTTONS};
let dtI={}, summaryCache=null, sparesData=[];
let detailTableExpanded=false;
let detailExpandedRows=[], detailExpandedCols=[];
let forecastOriginal={};
let allModules=[], activeModules=new Set();
const DEFAULT_UNCHECKED_LINES=new Set(['pilot / npi','needs review','non-prod']);
const ENABLE_TABLE_COL_RESIZE=false;

async function initLineFilter(){
    const res=await fetch('/api/modules');
    allModules=await res.json();
    // Default: uncheck BASE2 and non-production buckets.
    activeModules=new Set(allModules.filter(m=>{
        if(typeof m!=='string')return false;
        const normalized=m.trim().toLowerCase();
        if(m.startsWith('BASE2-'))return false;
        if(DEFAULT_UNCHECKED_LINES.has(normalized))return false;
        return true;
    }));
    renderLineChecks();
}
function renderLineChecks(){
    const wrap=document.getElementById('line-filter-checks');
    wrap.innerHTML=allModules.map(m=>{
        const checked=activeModules.has(m)?'checked':'';
        return `<label style="display:flex;align-items:center;gap:6px;padding:3px 0;font-size:12px;color:var(--text);cursor:pointer"><input type="checkbox" ${checked} onchange="toggleLine('${m}',this.checked)" style="accent-color:var(--green)"/>${m}</label>`;
    }).join('');
}
function toggleLine(mod,on){
    if(on)activeModules.add(mod);else activeModules.delete(mod);
    reloadCurrentPage();
}
function toggleAllLines(on){
    if(on)activeModules=new Set(allModules);else activeModules.clear();
    renderLineChecks();
    reloadCurrentPage();
}
function setTableExpand(on){
    document.body.classList.toggle('tables-expanded',!!on);
    const btn=document.getElementById('btn-table-expand');
    if(btn){
        btn.textContent=on?'Collapse Tables':'Expand Tables';
        btn.style.borderColor=on?'var(--green)':'var(--border)';
        btn.style.color=on?'var(--green)':'var(--text)';
    }
}
function toggleTableExpand(){
    const next=!document.body.classList.contains('tables-expanded');
    setTableExpand(next);
    localStorage.setItem('capex_tables_expanded',next?'1':'0');
    Object.values(dtI).forEach(dt=>{try{dt.columns.adjust().draw(false);}catch(e){}});
}
function initUiPrefs(){
    const expanded=localStorage.getItem('capex_tables_expanded')==='1';
    setTableExpand(expanded);
    detailTableExpanded=localStorage.getItem('capex_detail_expanded')==='1';
    setDetailExpand(detailTableExpanded);
}
function setDetailExpand(on){
    detailTableExpanded=!!on;
    const wrap=document.getElementById('detail-table-wrap');
    if(wrap)wrap.classList.toggle('detail-expanded',detailTableExpanded);
    const btn=document.getElementById('btn-detail-expand');
    if(btn){
        btn.textContent=detailTableExpanded?'Collapse Table':'Expand Table';
        btn.style.borderColor=detailTableExpanded?'var(--green)':'var(--border)';
        btn.style.color=detailTableExpanded?'var(--green)':'var(--text)';
    }
}
function toggleDetailExpand(){
    const next=!detailTableExpanded;
    setDetailExpand(next);
    localStorage.setItem('capex_detail_expanded',next?'1':'0');
    loadDetail();
}
function detailRowHtml(r){
    let row='<tr>';
    detailExpandedCols.forEach(c=>{
        const v=r[c]!==undefined?r[c]:'';
        if(c==='price_subtotal'||c==='price_total')row+=`<td class="dollar">${fmtF$(parseFloat(v)||0)}</td>`;
        else if(c==='source')row+=`<td><span class="source-badge ${htmlEsc(v)}">${htmlEsc(v)}</span></td>`;
        else row+=`<td>${htmlEsc(v)}</td>`;
    });
    row+='</tr>';
    return row;
}
function applyDetailExpandedFilter(){
    const input=document.getElementById('detail-native-search');
    const q=(input&&input.value?input.value:'').trim().toLowerCase();
    const rows=!q?detailExpandedRows:detailExpandedRows.filter(r=>detailExpandedCols.some(c=>String(r[c]??'').toLowerCase().includes(q)));
    const body=document.getElementById('detail-native-body');
    if(body)body.innerHTML=rows.map(detailRowHtml).join('');
    const count=document.getElementById('detail-native-count');
    if(count)count.textContent=`${rows.length.toLocaleString()} / ${detailExpandedRows.length.toLocaleString()} rows`;
}
function exportDetailExpandedCsv(){
    const rows=detailExpandedRows.map(r=>{
        const out={};
        detailExpandedCols.forEach(c=>{out[c]=r[c]!==undefined?r[c]:'';});
        return out;
    });
    const stamp=new Date().toISOString().slice(0,19).replace(/[:T]/g,'-');
    downloadBlob(`capex_full_transactions_${stamp}.csv`,new Blob([rowsToCsv(rows)],{type:'text/csv;charset=utf-8;'}));
    showToast('CSV export complete');
}
function exportDetailExpandedExcel(){
    if(typeof XLSX==='undefined'){
        showToast('Excel library unavailable');
        return;
    }
    const rows=detailExpandedRows.map(r=>{
        const out={};
        detailExpandedCols.forEach(c=>{out[c]=r[c]!==undefined?r[c]:'';});
        return out;
    });
    const stamp=new Date().toISOString().slice(0,19).replace(/[:T]/g,'-');
    const wb=XLSX.utils.book_new();
    const ws=XLSX.utils.json_to_sheet(rows.length?rows:[{info:'No rows'}]);
    XLSX.utils.book_append_sheet(wb,ws,'full_transactions');
    const out=XLSX.write(wb,{bookType:'xlsx',type:'array'});
    downloadBlob(`capex_full_transactions_${stamp}.xlsx`,new Blob([out],{type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}));
    showToast('Excel export complete');
}
function renderDetailExpanded(data,cols,labels){
    detailExpandedRows=Array.isArray(data)?data:[];
    detailExpandedCols=[...cols];
    let html='<div class="detail-native-toolbar"><div class="left">';
    html+='<input id="detail-native-search" class="detail-native-search" type="text" placeholder="Search all columns..." oninput="applyDetailExpandedFilter()"/>';
    html+='<button class="btn-refresh" style="width:auto;padding:8px 14px" onclick="exportDetailExpandedCsv()">CSV</button>';
    html+='<button class="btn-refresh" style="width:auto;padding:8px 14px;background:var(--surface2);color:var(--text);border:1px solid var(--border)" onclick="exportDetailExpandedExcel()">Excel</button>';
    html+='</div><div class="right" id="detail-native-count"></div></div>';
    html+='<div class="detail-native-wrap"><table class="detail-native-table"><thead><tr>';
    labels.forEach(l=>{html+=`<th>${htmlEsc(l)}</th>`;});
    html+='</tr></thead><tbody id="detail-native-body"></tbody></table></div>';
    document.getElementById('detail-table-wrap').innerHTML=html;
    applyDetailExpandedFilter();
}
function lineQS(){
    if(activeModules.size===0)return'lines=__none__'; // None filter: show no data
    if(activeModules.size===allModules.length)return'';
    return'lines='+[...activeModules].join(',');
}
function apiUrl(path){
    const qs=lineQS();
    if(!qs)return path;
    return path.includes('?') ? (path+'&'+qs) : (path+'?'+qs);
}
function reloadCurrentPage(){
    summaryCache=null;
    const active=document.querySelector('.page.active');
    if(!active)return;
    const id=active.id.replace('page-','');
    if(id==='executive')loadExecutive();
    else if(id==='source')loadSource();
    else if(id==='stations')loadStations();
    else if(id==='forecasting')loadForecasting();
    else if(id==='vendors')loadVendors();
    else if(id==='assets')loadAssets();
    else if(id==='spares')loadSpares();
    else if(id==='detail')loadDetail();
    else if(id==='timeline')loadTimeline();
    else if(id==='projects')loadProjects();
    else if(id==='v2milestones')loadMilestones();
    else if(id==='airfq')loadAirRfq();
    else if(id==='v2cashflow')loadCashflow();
}

function fmt$(v){if(v==null||isNaN(v))return'$0';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e6)return s+'$'+(a/1e6).toFixed(2)+'M';if(a>=1e3)return s+'$'+(a/1e3).toFixed(1)+'K';return s+'$'+a.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}
function fmtMoney2(v){if(v==null||isNaN(v))return'$0.00';const n=Number(v)||0;const s=n<0?'-':'';const a=Math.abs(n);return s+'$'+a.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}
function fmtDateMMDDYYYY(v){
    if(!v)return'';
    const d=new Date(v);
    if(Number.isNaN(d.getTime()))return'';
    const mm=String(d.getMonth()+1).padStart(2,'0');
    const dd=String(d.getDate()).padStart(2,'0');
    const yyyy=d.getFullYear();
    return `${mm}/${dd}/${yyyy}`;
}
function htmlEsc(v){
    return String(v??'')
        .replace(/&/g,'&amp;')
        .replace(/</g,'&lt;')
        .replace(/>/g,'&gt;')
        .replace(/"/g,'&quot;')
        .replace(/'/g,'&#39;');
}

function dtOpts(base){
    const baseInit=base&&typeof base.initComplete==='function'?base.initComplete:null;
    return Object.assign({scrollX:true,autoWidth:false},base,{
        initComplete:function(){
            const api=this.api();
            const tableNode=api.table().node();
            const thead=tableNode?tableNode.querySelector('thead'):null;
            if(thead){
                let filterRow=thead.querySelector('tr.dt-filter-row');
                const headerCells=thead.querySelectorAll('tr:first-child th');
                if(!filterRow){
                    filterRow=document.createElement('tr');
                    filterRow.className='dt-filter-row';
                    for(let i=0;i<headerCells.length;i++){
                        const th=document.createElement('th');
                        th.innerHTML='<input type="text" placeholder="Filter..."/>';
                        filterRow.appendChild(th);
                    }
                    thead.appendChild(filterRow);
                }
                api.columns().every(function(idx){
                    const col=this;
                    const input=filterRow.children[idx]?.querySelector('input');
                    if(!input)return;
                    input.onkeyup=input.onchange=function(){
                        if(col.search()!==this.value)col.search(this.value).draw();
                    };
                });
            }
            if(!ENABLE_TABLE_COL_RESIZE&&tableNode){
                const wrap=tableNode.closest('.dataTables_wrapper');
                if(wrap){
                    wrap.querySelectorAll('.col-resizer').forEach(el=>el.remove());
                    wrap.querySelectorAll('table.dt-resizable').forEach(t=>t.classList.remove('dt-resizable'));
                }
            }
            if(ENABLE_TABLE_COL_RESIZE&&tableNode&&tableNode.id){
                enableTableColumnResize(api,tableNode.id);
                if(!tableNode.dataset.resizeBound){
                    api.on('draw',()=>enableTableColumnResize(api,tableNode.id));
                    tableNode.dataset.resizeBound='1';
                }
            }
            if(baseInit)baseInit.call(this);
        }
    });
}

function enableTableColumnResize(dtApi,tableId){
    const table=document.getElementById(tableId);
    if(!table)return;
    const wrap=table.closest('.dataTables_wrapper');
    if(!wrap)return;
    const headTable=wrap.querySelector('.dataTables_scrollHead table');
    const bodyTable=wrap.querySelector('.dataTables_scrollBody table');
    if(!headTable||!bodyTable)return;

    // Resize handles only belong on the primary header row (not filter row).
    const headers=[...headTable.querySelectorAll('thead tr:first-child th')];
    if(!headers.length)return;
    headTable.classList.add('dt-resizable');

    const headCols=[...headTable.querySelectorAll('colgroup col')];
    const bodyCols=[...bodyTable.querySelectorAll('colgroup col')];
    const STORAGE_KEY='dashboard_table_col_widths_'+tableId;

    const applyWidth=(idx,w)=>{
        const width=Math.max(70,Math.round(w));
        if(headCols[idx])headCols[idx].style.width=width+'px';
        if(bodyCols[idx])bodyCols[idx].style.width=width+'px';
        if(headers[idx])headers[idx].style.width=width+'px';
        // Keep width assignment scoped to the target colgroup column only.
        // Applying min/max widths to every cell causes cross-column resizing artifacts.
        if(headers[idx]){
            headers[idx].style.minWidth=width+'px';
            headers[idx].style.maxWidth=width+'px';
        }
    };
    const saveWidths=()=>{
        const payload=headers.map(h=>Math.round(h.getBoundingClientRect().width));
        localStorage.setItem(STORAGE_KEY,JSON.stringify(payload));
    };

    try{
        const saved=JSON.parse(localStorage.getItem(STORAGE_KEY)||'[]');
        if(Array.isArray(saved)&&saved.length){
            saved.forEach((w,i)=>{if(Number.isFinite(w)&&w>0)applyWidth(i,w);});
        }
    }catch(_){}

    headers.forEach((th,idx)=>{
        th.querySelectorAll('.col-resizer').forEach(el=>el.remove());
        const startResize=(startX,startW)=>{
            document.body.classList.add('col-resize-active');
            const onMove=(ev)=>applyWidth(idx,startW+(ev.clientX-startX));
            const onUp=()=>{
                document.removeEventListener('mousemove',onMove);
                document.removeEventListener('mouseup',onUp);
                document.body.classList.remove('col-resize-active');
                saveWidths();
            };
            document.addEventListener('mousemove',onMove);
            document.addEventListener('mouseup',onUp);
        };
        const handle=document.createElement('span');
        handle.className='col-resizer';
        handle.title='Drag to resize column';
        handle.addEventListener('mousedown',(e)=>{
            e.preventDefault();
            e.stopPropagation();
            startResize(e.clientX,th.getBoundingClientRect().width);
        });
        th.appendChild(handle);
    });
}
function fmtF$(v){if(v==null||isNaN(v))return'$0.00';return(v<0?'-':'')+'$'+Math.abs(v).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}
function fmtPct(v){return(v||0).toFixed(1)+'%'}
function vc(v){return v>0?'dollar-negative':'dollar-positive'}

function safeFileName(name){
    return String(name||'export').replace(/[^a-z0-9_-]+/gi,'_').replace(/^_+|_+$/g,'').toLowerCase()||'export';
}
function safeSheetName(name){
    const cleaned=String(name||'Sheet1').replace(/[\\/*?:\[\]]/g,' ').replace(/\s+/g,' ').trim();
    return (cleaned||'Sheet1').slice(0,31);
}
function normalizeRows(rows){
    if(!Array.isArray(rows))return [];
    if(!rows.length)return [];
    if(typeof rows[0]==='object'&&rows[0]!==null&&!Array.isArray(rows[0]))return rows;
    return rows.map((v,i)=>({index:i,value:v}));
}
function rowsToCsv(rows){
    const data=normalizeRows(rows);
    if(!data.length)return 'no_data\n';
    const keys=[...new Set(data.flatMap(r=>Object.keys(r)))];
    const esc=(v)=>{
        if(v===null||v===undefined)return '';
        const s=String(v);
        if(/[",\n]/.test(s))return '"'+s.replace(/"/g,'""')+'"';
        return s;
    };
    const out=[keys.join(',')];
    data.forEach(r=>out.push(keys.map(k=>esc(r[k])).join(',')));
    return out.join('\n')+'\n';
}
function flattenToDatasets(prefix,payload){
    const datasets=[];
    if(Array.isArray(payload)){
        datasets.push({name:prefix,rows:normalizeRows(payload)});
        return datasets;
    }
    if(payload&&typeof payload==='object'){
        const metaRows=[];
        Object.entries(payload).forEach(([k,v])=>{
            if(Array.isArray(v)){
                datasets.push({name:prefix+'_'+k,rows:normalizeRows(v)});
            }else if(v&&typeof v==='object'){
                const objRows=Object.entries(v).map(([kk,vv])=>({key:kk,value:typeof vv==='object'?JSON.stringify(vv):vv}));
                datasets.push({name:prefix+'_'+k,rows:objRows});
            }else{
                metaRows.push({key:k,value:v});
            }
        });
        if(metaRows.length)datasets.push({name:prefix+'_meta',rows:metaRows});
        return datasets;
    }
    datasets.push({name:prefix+'_value',rows:[{value:payload}]});
    return datasets;
}
function downloadBlob(fileName,blob){
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a');
    a.href=url;
    a.download=fileName;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(()=>URL.revokeObjectURL(url),1000);
}
function setExportBusy(isBusy,msg){
    const csvBtn=document.getElementById('export-csv-btn');
    const xlsxBtn=document.getElementById('export-xlsx-btn');
    const note=document.getElementById('export-note');
    if(csvBtn)csvBtn.disabled=!!isBusy;
    if(xlsxBtn)xlsxBtn.disabled=!!isBusy;
    if(note&&msg)note.textContent=msg;
}
function endpointBaseName(path){
    const clean=String(path||'data').split('?')[0].replace(/^\/+|\/+$/g,'');
    return safeFileName(clean.replace(/\//g,'_'));
}
async function collectExportDatasets(pageId){
    const endpoints=[];
    const sidEl=document.getElementById('stationSelect');
    const stationId=sidEl?(sidEl.value||'').trim():'';
    if(pageId==='executive'||pageId==='source')endpoints.push(apiUrl('/api/summary'));
    else if(pageId==='stations'){
        endpoints.push(apiUrl('/api/stations'));
        if(stationId)endpoints.push('/api/station/'+encodeURIComponent(stationId));
    }else if(pageId==='forecasting'){
        endpoints.push(apiUrl('/api/forecasting'));
        endpoints.push('/api/settings');
    }else if(pageId==='vendors'){
        endpoints.push(apiUrl('/api/vendors'));
        endpoints.push(apiUrl('/api/transactions'));
    }else if(pageId==='assets'){
        endpoints.push(apiUrl('/api/assets'));
        endpoints.push('/api/asset-status');
    }else if(pageId==='spares')endpoints.push(apiUrl('/api/spares'));
    else if(pageId==='detail')endpoints.push(apiUrl('/api/transactions'));
    else if(pageId==='timeline')endpoints.push(apiUrl('/api/timeline'));
    else if(pageId==='projects')endpoints.push('/api/projects');
    else if(pageId==='uniteco')endpoints.push('/api/unit-economics');
    else if(pageId==='settings'){
        endpoints.push('/api/settings');
        endpoints.push('/api/modules');
    }else{
        endpoints.push(apiUrl('/api/summary'));
    }

    const datasets=[];
    for(const ep of endpoints){
        const res=await fetch(ep);
        if(!res.ok)throw new Error('Failed to fetch '+ep+' ('+res.status+')');
        const payload=await res.json();
        flattenToDatasets(endpointBaseName(ep),payload).forEach(ds=>datasets.push(ds));
    }
    return datasets.filter(ds=>Array.isArray(ds.rows));
}
async function exportCurrentPage(format){
    const active=document.querySelector('.page.active');
    const pageId=active?active.id.replace('page-',''):'executive';
    try{
        setExportBusy(true,'Preparing '+format.toUpperCase()+' export...');
        const datasets=await collectExportDatasets(pageId);
        if(!datasets.length){
            setExportBusy(false,'No data available to export for this tab.');
            showToast('No data to export');
            return;
        }
        const stamp=new Date().toISOString().slice(0,19).replace(/[:T]/g,'-');
        const base='capex_'+safeFileName(pageId)+'_'+stamp;
        if(format==='xlsx'){
            if(typeof XLSX==='undefined')throw new Error('Excel library unavailable');
            const wb=XLSX.utils.book_new();
            datasets.forEach(ds=>{
                const rows=normalizeRows(ds.rows);
                const ws=XLSX.utils.json_to_sheet(rows.length?rows:[{info:'No rows'}]);
                XLSX.utils.book_append_sheet(wb,ws,safeSheetName(ds.name));
            });
            const out=XLSX.write(wb,{bookType:'xlsx',type:'array'});
            downloadBlob(base+'.xlsx',new Blob([out],{type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}));
            setExportBusy(false,'Excel export ready: '+datasets.length+' sheet(s).');
            showToast('Excel export complete');
            return;
        }
        if(typeof JSZip==='undefined')throw new Error('CSV zip library unavailable');
        const zip=new JSZip();
        datasets.forEach(ds=>zip.file(safeFileName(ds.name)+'.csv',rowsToCsv(ds.rows)));
        const blob=await zip.generateAsync({type:'blob'});
        downloadBlob(base+'_csv.zip',blob);
        setExportBusy(false,'CSV export ready: '+datasets.length+' file(s).');
        showToast('CSV export complete');
    }catch(err){
        setExportBusy(false,'Export failed. Please retry.');
        showToast('Export failed');
    }
}

function toPlainArray(v){
    if(Array.isArray(v))return v;
    if(v&&typeof v==='object'&&ArrayBuffer.isView(v))return Array.from(v);
    return [];
}
function traceRows(trace, idx){
    const tname=trace&&trace.name?String(trace.name):('trace_'+(idx+1));
    const rows=[];
    const x=toPlainArray(trace.x);
    const y=toPlainArray(trace.y);
    if(y.length||x.length){
        const n=Math.max(y.length,x.length);
        for(let i=0;i<n;i++){
            rows.push({
                trace:tname,
                point_index:i,
                x:(x[i]!==undefined?x[i]:i),
                y:(y[i]!==undefined?y[i]:''),
            });
        }
        return rows;
    }
    const labels=toPlainArray(trace.labels);
    const values=toPlainArray(trace.values);
    if(labels.length||values.length){
        const n=Math.max(labels.length,values.length);
        for(let i=0;i<n;i++){
            rows.push({
                trace:tname,
                point_index:i,
                label:(labels[i]!==undefined?labels[i]:''),
                value:(values[i]!==undefined?values[i]:''),
                parent:(toPlainArray(trace.parents)[i]!==undefined?toPlainArray(trace.parents)[i]:''),
            });
        }
        return rows;
    }
    const z=Array.isArray(trace.z)?trace.z:[];
    if(z.length&&Array.isArray(z[0])){
        const hzX=toPlainArray(trace.x);
        const hzY=toPlainArray(trace.y);
        for(let yi=0;yi<z.length;yi++){
            const row=z[yi];
            for(let xi=0;xi<row.length;xi++){
                rows.push({
                    trace:tname,
                    x:(hzX[xi]!==undefined?hzX[xi]:xi),
                    y:(hzY[yi]!==undefined?hzY[yi]:yi),
                    z:row[xi],
                });
            }
        }
        return rows;
    }
    return [{trace:tname,info:'No plottable arrays found in trace'}];
}
function exportGraphData(graphId,format){
    const el=document.getElementById(graphId);
    if(!el||!el.data||!el.data.length){
        showToast('No chart data to export');
        return;
    }
    const rows=[];
    el.data.forEach((tr,idx)=>traceRows(tr,idx).forEach(r=>rows.push(r)));
    const stamp=new Date().toISOString().slice(0,19).replace(/[:T]/g,'-');
    const base='capex_'+safeFileName(graphId)+'_'+stamp;
    if(format==='xlsx'){
        if(typeof XLSX==='undefined'){
            showToast('Excel library unavailable');
            return;
        }
        const wb=XLSX.utils.book_new();
        const ws=XLSX.utils.json_to_sheet(rows.length?rows:[{info:'No rows'}]);
        XLSX.utils.book_append_sheet(wb,ws,safeSheetName(graphId));
        const out=XLSX.write(wb,{bookType:'xlsx',type:'array'});
        downloadBlob(base+'.xlsx',new Blob([out],{type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}));
        showToast('Graph exported to Excel');
        return;
    }
    const csv=rowsToCsv(rows);
    downloadBlob(base+'.csv',new Blob([csv],{type:'text/csv;charset=utf-8;'}));
    showToast('Graph exported to CSV');
}
/* ====== DRILL-DOWN ====== */
let drillDT=null;
async function openDrill(title,params){
    const qs=Object.entries(params).filter(([k,v])=>v).map(([k,v])=>k+'='+encodeURIComponent(v)).join('&');
    const res=await fetch('/api/drilldown?'+qs);
    const d=await res.json();
    document.getElementById('drill-title').textContent=title;
    document.getElementById('drill-sub').textContent=d.count+' items | '+fmtF$(d.total)+' total';
    const cols=['source','po_number','date_order','vendor_name','mfg_subcategory','item_description','station_id','project_name','mapping_confidence','payment_status_display','price_subtotal','created_by_name'];
    const labels=['Src','PO','Date','Vendor','Sub-Cat','Description','Station','Project','Conf','Pay','Subtotal','By'];
    let html='<table id="drill-tbl" class="display compact" style="width:100%"><thead><tr>';
    labels.forEach(l=>{html+='<th>'+l+'</th>';});
    html+='</tr></thead><tfoot><tr>';
    labels.forEach(()=>{html+='<th></th>';});
    html+='</tr></tfoot><tbody>';
    (d.rows||[]).forEach(r=>{
        const row={...r};
        row.payment_status_display=(row.po_payment_status_v2||row.bill_payment_status||'');
        html+='<tr>';
        cols.forEach(c=>{
            const v=row[c]!=null?row[c]:'';
            if(c==='price_subtotal')html+='<td class="dollar">'+fmtF$(parseFloat(v)||0)+'</td>';
            else if(c==='source')html+='<td><span class="source-badge '+v+'">'+v+'</span></td>';
            else html+='<td>'+v+'</td>';
        });
        html+='</tr>';
    });
    html+='</tbody></table>';
    document.getElementById('drill-body').innerHTML=html;
    if(drillDT)drillDT.destroy();
    drillDT=$('#drill-tbl').DataTable(dtOpts({pageLength:25,order:[[10,'desc']],dom:'Bfrtip',buttons:['csv','excel']}));
    document.getElementById('drill-overlay').style.display='block';
    document.getElementById('drill-panel').style.display='flex';
}
function closeDrill(){
    document.getElementById('drill-overlay').style.display='none';
    document.getElementById('drill-panel').style.display='none';
    if(drillDT){drillDT.destroy();drillDT=null;}
}

function toggleSidebar(){
    const sb=document.querySelector('.sidebar');
    const ov=document.querySelector('.sidebar-overlay');
    const open=sb.classList.toggle('open');
    ov.classList.toggle('visible',open);
    document.body.style.overflow=open?'hidden':'';
}
function closeSidebarIfMobile(){
    if(window.innerWidth<=1024){
        const sb=document.querySelector('.sidebar');
        const ov=document.querySelector('.sidebar-overlay');
        sb.classList.remove('open');
        ov.classList.remove('visible');
        document.body.style.overflow='';
    }
}
function showPage(id,el){
    document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
    document.getElementById('page-'+id).classList.add('active');
    if(el)el.classList.add('active');
    closeSidebarIfMobile();
    if(id==='about')loadAbout();
    if(id==='source')loadSource();
    if(id==='stations'&&!document.getElementById('stationSelect').value)loadStations();
    if(id==='forecasting')loadForecasting();
    if(id==='vendors')loadVendors();
    if(id==='assets')loadAssets();
    if(id==='spares')loadSpares();
    if(id==='detail')loadDetail();
    if(id==='timeline')loadTimeline();
    if(id==='projects')loadProjects();
    if(id==='uniteco')loadUnitEconomics();
    if(id==='v2reviews')loadReviews();
    if(id==='v2milestones')loadMilestones();
    if(id==='v2templates')loadTemplates();
    if(id==='airfq')loadAirRfq();
    if(id==='v2cashflow')loadCashflow();
    if(id==='settings')loadSettings();
}

function loadAbout(){
    // Static page today; hook kept for future lightweight dynamic badges if needed.
}

/* ====== EXECUTIVE ====== */
async function loadExecutive(){
    const res=await fetch(apiUrl('/api/summary'));const d=await res.json();
    if(!d.total_committed)return;
    summaryCache=d;
    const pay=d.payment||{};
    const rpay=d.ramp_payment||{};

    document.getElementById('kpis').innerHTML=`
        <div class="kpi"><div class="label">Total Committed</div><div class="value dollar">${fmt$(d.total_committed)}</div><div class="sub"><span class="source-badge odoo">Odoo ${fmt$(d.odoo_total)}</span> <span class="source-badge ramp">Ramp ${fmt$(d.ramp_total)}</span></div></div>
        <div class="kpi"><div class="label">Forecasted Budget</div><div class="value dollar">${fmt$(d.forecasted_budget)}</div></div>
        <div class="kpi"><div class="label">Variance</div><div class="value ${vc(d.variance)}">${fmt$(d.variance)}</div><div class="sub">${d.variance>0?'Over':'Under'} budget</div></div>
        <div class="kpi"><div class="label">% Budget Spent</div><div class="value">${fmtPct(d.pct_spent)}</div></div>
        <div class="kpi"><div class="label">Mfg Spend</div><div class="value dollar" style="color:var(--green)">${fmt$(d.mfg_total||0)}</div><div class="sub">Non-Mfg: ${fmt$(d.non_mfg_total||0)}</div></div>
        <div class="kpi"><div class="label">Odoo Billed</div><div class="value">${fmtPct(pay.billed_spend_pct||0)}</div><div class="sub">Paid: ${fmt$(pay.paid_spend||0)} &middot; Open: ${fmt$(pay.open_spend||0)}</div></div>
        <div class="kpi"><div class="label">Ramp CC</div><div class="value dollar" style="color:var(--blue)">${fmt$(rpay.total_amount||0)}</div><div class="sub">${rpay.available?(rpay.txn_count||0)+' transactions &middot; Card Charged':'N/A'}</div></div>
        <div class="kpi"><div class="label">Active POs</div><div class="value">${(d.active_pos||0).toLocaleString()}</div></div>`;

    // Budget vs Actual -- grouped by MOD with proper left margin
    if(d.budget_vs_actual&&d.budget_vs_actual.length){
        const lines=d.budget_vs_actual.sort((a,b)=>a.line.localeCompare(b.line));
        const maxLabel=Math.max(...lines.map(l=>l.line.length));
        Plotly.newPlot('chart-budget',[
            {y:lines.map(l=>l.line),x:lines.map(l=>l.forecasted),type:'bar',orientation:'h',name:'Forecasted',marker:{color:C.surface2},text:lines.map(l=>fmt$(l.forecasted)),textposition:'outside',textfont:{color:C.muted,size:10}},
            {y:lines.map(l=>l.line),x:lines.map(l=>l.actual),type:'bar',orientation:'h',name:'Actual',marker:{color:C.green},text:lines.map(l=>fmt$(l.actual)),textposition:'outside',textfont:{color:C.green,size:10}},
        ],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},barmode:'group',height:Math.max(300,lines.length*50),margin:{l:Math.max(140,maxLabel*8),r:80,t:30,b:40},legend:{font:{color:C.muted},x:0.7,y:1.1,orientation:'h'},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2}},PC);
        document.getElementById('chart-budget').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const ln=ev.points[0].y;openDrill('Line: '+ln,{line:ln});}});
    }

    // Monthly trend with line overlay
    if(d.monthly_trend&&d.monthly_trend.length){
        const months=d.monthly_trend.map(m=>m.month);
        const odooY=d.monthly_trend.map(m=>m.odoo||0);
        const rampY=d.monthly_trend.map(m=>m.ramp||0);
        const traces=[
            {x:months,y:odooY,type:'bar',name:'Odoo PO',marker:{color:C.green},hovertemplate:'%{x}<br>Odoo: %{y:$,.0f}<extra></extra>'},
            {x:months,y:rampY,type:'bar',name:'Ramp CC',marker:{color:C.blue},hovertemplate:'%{x}<br>Ramp: %{y:$,.0f}<extra></extra>'},
        ];
        const lineColors=['#E8A838','#D0F585','#048EE5','#D1531D','#9B7ED8','#F06292','#4DD0E1'];
        if(d.monthly_by_line){
            Object.entries(d.monthly_by_line).sort().forEach(([ln,mData],i)=>{
                traces.push({x:months,y:months.map(m=>mData[m]||0),type:'scatter',mode:'lines+markers',name:ln,line:{width:2,color:lineColors[i%lineColors.length],dash:i<3?'solid':'dash'},marker:{size:5},hovertemplate:'%{x}<br>'+ln+': %{y:$,.0f}<extra></extra>'});
            });
        }
        Plotly.newPlot('chart-monthly',traces,{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},barmode:'stack',height:350,legend:{font:{color:C.muted,size:10},x:0,y:1.2,orientation:'h'},margin:{l:65,r:15,t:60,b:60},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2,tickangle:-45}},PC);
        document.getElementById('chart-monthly').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const m=p.x;const params={month:m};if(p.data.name&&p.data.name.startsWith('BASE'))params.line=p.data.name;else if(p.data.name==='Ramp CC')params.source='ramp';else if(p.data.name==='Odoo PO')params.source='odoo';openDrill(m+(params.line?' | '+params.line:params.source?' | '+params.source:''),params);}});
    } else {
        document.getElementById('chart-monthly').innerHTML='<p style="color:var(--muted);padding:20px">No monthly data available.</p>';
    }

    // Sub-category treemap
    const scData=d.subcategory_spend||d.category_spend||[];
    if(scData.length){
        const isSubcat=!!d.subcategory_spend;
        const items=scData.slice(0,13);
        const scColors={'Process Equipment':'#8abb55','Controls & Electrical':'#048EE5','Mechanical & Structural':'#E8A838','Design & Engineering Services':'#9B7ED8','Integration & Commissioning':'#F06292','Quality & Metrology':'#4DD0E1','Software & Licenses':'#CE93D8','MFG Tools & Shop Supplies':'#A1887F','Consumables':'#FFB74D','Shipping & Freight':'#78909C','Facilities & Office':'#555','IT Equipment':'#607D8B','General & Administrative':'#455A64'};
        Plotly.newPlot('chart-subcategory',[{
            type:'treemap',
            labels:items.map(c=>isSubcat?c.subcategory:c.category.replace('Non-Inventory: ','')),
            parents:items.map(()=>''),values:items.map(c=>c.spend),
            textinfo:'label+value',texttemplate:'%{label}<br>%{value:$,.0f}',
            marker:{colors:items.map(c=>isSubcat?(scColors[c.subcategory]||'#555'):`hsl(${90+items.indexOf(c)*20},55%,52%)`)}
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:8,r:8,t:8,b:8}},PC);
        document.getElementById('chart-subcategory').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const lbl=ev.points[0].label;openDrill('Sub-Category: '+lbl,isSubcat?{subcategory:lbl}:{category:scData.find(c=>c.category.replace('Non-Inventory: ','')==lbl)?.category||lbl});}});
    }

    // Odoo payment status donut (linked vendor-bill states)
    if(pay.available){
        const labels=['Paid','Partial','Unpaid','No Bill','Mixed'];
        const values=[pay.paid_spend||0,pay.partial_spend||0,pay.unpaid_spend||0,pay.no_bill_spend||0,pay.mixed_spend||0];
        const colors=[C.green,C.yellow,C.red,'#666','#9B7ED8'];
        Plotly.newPlot('chart-payment-status',[{
            labels,values,type:'pie',hole:.5,marker:{colors},
            textinfo:'label+percent',textfont:{color:C.text,size:11},
            hovertemplate:'%{label}<br>%{value:$,.0f}<br>%{percent}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:8,r:8,t:8,b:8}},PC);
        document.getElementById('chart-payment-status').on('plotly_click',function(ev){
            if(!ev.points||!ev.points.length)return;
            const label=(ev.points[0].label||'').toLowerCase();
            const map={'paid':'paid','partial':'partial','unpaid':'unpaid','no bill':'no_bill','mixed':'mixed'};
            openDrill('Payment: '+ev.points[0].label,{source:'odoo',payment_status:map[label]||''});
        });
    }else{
        document.getElementById('chart-payment-status').innerHTML='<p style="color:var(--muted);padding:20px">Payment status not available in this dataset.</p>';
    }

    // Ramp CC payment -- all CC charges are paid at swipe
    if(rpay.available&&rpay.total_amount>0){
        const rLabels=['Card Charged'];
        const rValues=[rpay.card_charged||0];
        const rColors=[C.blue];
        Plotly.newPlot('chart-ramp-payment',[{
            labels:rLabels,values:rValues,type:'pie',hole:.5,marker:{colors:rColors},
            textinfo:'label+value',texttemplate:'%{label}<br>%{value:$,.0f}',
            textfont:{color:C.text,size:12},
            hovertemplate:'%{label}<br>%{value:$,.0f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:8,r:8,t:8,b:8},
            annotations:[{text:(rpay.txn_count||0)+' txns',showarrow:false,font:{size:13,color:C.text},x:0.5,y:0.5}]
        },PC);
    }else{
        document.getElementById('chart-ramp-payment').innerHTML='<p style="color:var(--muted);padding:20px">No Ramp transactions in this dataset.</p>';
    }

    // Top vendors -- generous left margin
    if(d.top_vendors&&d.top_vendors.length){
        const v=[...d.top_vendors].reverse();
        const maxLen=Math.max(...v.map(x=>x.vendor.length));
        Plotly.newPlot('chart-vendors',[{
            y:v.map(x=>x.vendor),x:v.map(x=>x.spend),
            type:'bar',orientation:'h',marker:{color:C.green},
            text:v.map(x=>fmt$(x.spend)),textposition:'outside',textfont:{color:C.muted,size:10},
            hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(350,v.length*32),margin:{l:Math.max(180,maxLen*7),r:80,t:20,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2}},PC);
        document.getElementById('chart-vendors').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const vn=ev.points[0].y;openDrill('Vendor: '+vn,{vendor:vn});}});
    }

    // Spend by Employee
    if(d.top_employees&&d.top_employees.length){
        const e=[...d.top_employees].reverse();
        Plotly.newPlot('chart-employees',[
            {y:e.map(x=>x.name),x:e.map(x=>x.spend),type:'bar',orientation:'h',name:'Spend',marker:{color:C.green},text:e.map(x=>fmt$(x.spend)+' ('+x.pos+' POs)'),textposition:'outside',textfont:{color:C.muted,size:10},hovertemplate:'%{y}<br>Spend: %{x:$,.0f}<extra></extra>'},
        ],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(350,e.length*30),margin:{l:160,r:100,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
        document.getElementById('chart-employees').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const nm=ev.points[0].y;openDrill('Employee: '+nm,{employee:nm});}});
    }
    loadPaymentEvidence('payment-evidence-wrap','payment-evidence-tbl');
}

/* ====== ODOO vs RAMP ====== */
async function loadSource(){
    if(!summaryCache){const r=await fetch(apiUrl('/api/summary'));summaryCache=await r.json();}
    const d=summaryCache;const sc=d.source_compare||{};
    const pay=d.payment||{};
    const rpay=d.ramp_payment||{};
    document.getElementById('source-kpis').innerHTML=`
        <div class="kpi"><div class="label">Odoo PO Total</div><div class="value dollar">${fmt$(d.odoo_total)}</div><div class="sub">${(sc.odoo?.count||0).toLocaleString()} line items &middot; avg ${fmtF$(sc.odoo?.avg||0)}</div></div>
        <div class="kpi"><div class="label">Ramp CC Total</div><div class="value dollar" style="color:var(--blue)">${fmt$(d.ramp_total)}</div><div class="sub">${(sc.ramp?.count||0).toLocaleString()} transactions &middot; avg ${fmtF$(sc.ramp?.avg||0)}</div></div>
        <div class="kpi"><div class="label">Odoo Share</div><div class="value">${fmtPct(d.odoo_total/(d.total_committed||1)*100)}</div></div>
        <div class="kpi"><div class="label">Ramp Share</div><div class="value">${fmtPct(d.ramp_total/(d.total_committed||1)*100)}</div></div>
        <div class="kpi"><div class="label">Odoo Billed</div><div class="value dollar">${fmt$(pay.billed_spend||0)}</div><div class="sub">Paid: ${fmt$(pay.paid_spend||0)} &middot; Open: ${fmt$(pay.open_spend||0)} &middot; No Bill: ${fmt$(pay.no_bill_spend||0)}</div></div>
        <div class="kpi"><div class="label">Ramp CC Txns</div><div class="value" style="color:var(--blue)">${(rpay.txn_count||0).toLocaleString()}</div><div class="sub">${fmt$(rpay.total_amount||0)} &middot; Card Charged</div></div>`;

    const osc=sc.odoo_subcats||sc.odoo_categories||[];
    const isOSC=!!sc.odoo_subcats;
    if(osc.length){
        const ocR=[...osc].reverse();
        Plotly.newPlot('chart-odoo-subcats',[{y:ocR.map(c=>isOSC?c.cat:c.cat.replace('Non-Inventory: ','')),x:ocR.map(c=>c.spend),type:'bar',orientation:'h',marker:{color:C.green},text:ocR.map(c=>fmt$(c.spend)),textposition:'outside',textfont:{color:C.muted,size:10},hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'}],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(280,osc.length*35),margin:{l:210,r:70,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
        document.getElementById('chart-odoo-subcats').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const lbl=ev.points[0].y;openDrill('Odoo: '+lbl,isOSC?{source:'odoo',subcategory:lbl}:{source:'odoo',category:osc.find(c=>c.cat.replace('Non-Inventory: ','')==lbl)?.cat||lbl});}});
    }
    const rsc=sc.ramp_subcats||sc.ramp_categories||[];
    const isRSC=!!sc.ramp_subcats;
    if(rsc.length){
        const rcR=[...rsc].reverse();
        Plotly.newPlot('chart-ramp-subcats',[{y:rcR.map(c=>isRSC?c.cat:c.cat.replace('Non-Inventory: ','')),x:rcR.map(c=>c.spend),type:'bar',orientation:'h',marker:{color:C.blue},text:rcR.map(c=>fmt$(c.spend)),textposition:'outside',textfont:{color:C.muted,size:10},hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'}],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(280,rsc.length*35),margin:{l:210,r:70,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
        document.getElementById('chart-ramp-subcats').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const lbl=ev.points[0].y;openDrill('Ramp: '+lbl,isRSC?{source:'ramp',subcategory:lbl}:{source:'ramp',category:rsc.find(c=>c.cat.replace('Non-Inventory: ','')==lbl)?.cat||lbl});}});
    }

    // Odoo PO billing donut on source page
    if(pay.available){
        Plotly.newPlot('chart-src-odoo-billing',[{
            labels:['Paid','Partial','Unpaid','No Bill','Mixed'],
            values:[pay.paid_spend||0,pay.partial_spend||0,pay.unpaid_spend||0,pay.no_bill_spend||0,pay.mixed_spend||0],
            type:'pie',hole:.5,marker:{colors:[C.green,C.yellow,C.red,'#666','#9B7ED8']},
            textinfo:'label+percent',textfont:{color:C.text,size:11},
            hovertemplate:'%{label}<br>%{value:$,.0f}<br>%{percent}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:8,r:8,t:8,b:8},
            annotations:[{text:fmtPct(pay.paid_spend_pct||0)+' paid',showarrow:false,font:{size:13,color:C.text},x:0.5,y:0.5}]
        },PC);
        document.getElementById('chart-src-odoo-billing').on('plotly_click',function(ev){
            if(!ev.points||!ev.points.length)return;
            const label=(ev.points[0].label||'').toLowerCase();
            const map={'paid':'paid','partial':'partial','unpaid':'unpaid','no bill':'no_bill','mixed':'mixed'};
            openDrill('Odoo Payment: '+ev.points[0].label,{source:'odoo',payment_status:map[label]||''});
        });
    }

    // Ramp CC accounting -- all CC charges are paid at swipe
    if(rpay.available&&rpay.total_amount>0){
        Plotly.newPlot('chart-src-ramp-billing',[{
            labels:['Card Charged'],
            values:[rpay.card_charged||0],
            type:'pie',hole:.5,marker:{colors:[C.blue]},
            textinfo:'label+value',texttemplate:'%{label}<br>%{value:$,.0f}',
            textfont:{color:C.text,size:12},
            hovertemplate:'%{label}<br>%{value:$,.0f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:8,r:8,t:8,b:8},
            annotations:[{text:(rpay.txn_count||0)+' transactions',showarrow:false,font:{size:13,color:C.text},x:0.5,y:0.5}]
        },PC);
    }else{
        document.getElementById('chart-src-ramp-billing').innerHTML='<p style="color:var(--muted);padding:20px">No Ramp transactions in this dataset.</p>';
    }

    if(d.monthly_trend&&d.monthly_trend.length){
        const months=d.monthly_trend.map(m=>m.month);
        Plotly.newPlot('chart-source-monthly',[
            {x:months,y:d.monthly_trend.map(m=>m.odoo||0),type:'bar',name:'Odoo PO',marker:{color:C.green},hovertemplate:'%{x}<br>Odoo: %{y:$,.0f}<extra></extra>'},
            {x:months,y:d.monthly_trend.map(m=>m.ramp||0),type:'bar',name:'Ramp CC',marker:{color:C.blue},hovertemplate:'%{x}<br>Ramp: %{y:$,.0f}<extra></extra>'},
        ],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},barmode:'group',height:350,legend:{font:{color:C.muted},x:0,y:1.15,orientation:'h'},margin:{l:65,r:15,t:40,b:60},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2}},PC);
        document.getElementById('chart-source-monthly').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const src=p.data.name==='Ramp CC'?'ramp':'odoo';openDrill(p.x+' | '+p.data.name,{month:p.x,source:src});}});
    }
    loadPaymentEvidence('source-payment-evidence-wrap','source-payment-evidence-tbl');
}

async function loadPaymentEvidence(wrapId,tableId){
    const wrap=document.getElementById(wrapId);
    if(!wrap)return;
    const res=await fetch(apiUrl('/api/payment-evidence?limit=400'));
    const d=await res.json();
    const rows=d.rows||[];
    if(!rows.length){
        wrap.innerHTML='<p style="color:var(--muted);padding:20px">No payment evidence rows for current filter.</p>';
        return;
    }
    let html='<table class="display compact" id="'+tableId+'" style="width:100%"><thead><tr><th>PO</th><th>Vendor</th><th>Buyer</th><th>Status</th><th>Confidence</th><th>PO Total</th><th>Paid</th><th>Open</th><th>Paid %</th><th>Signals</th><th>Evidence</th></tr></thead><tbody>';
    rows.forEach(r=>{
        const signals=[r.has_deposit_signal?'deposit':'',r.has_unbilled_signal?'unbilled_pay':'' ].filter(Boolean).join(', ')||'--';
        html+=`<tr><td style="font-weight:700;color:var(--green)">${r.po_number||''}</td><td>${r.vendor_name||''}</td><td>${r.created_by_name||''}</td><td>${r.status_v2||''}</td><td>${r.confidence||''}</td><td style="text-align:right" data-order="${r.po_total||0}">${fmt$(r.po_total||0)}</td><td style="text-align:right" data-order="${r.paid_total_v2||0}">${fmt$(r.paid_total_v2||0)}</td><td style="text-align:right" data-order="${r.open_total_v2||0}">${fmt$(r.open_total_v2||0)}</td><td style="text-align:right" data-order="${r.paid_pct_of_po||0}">${(r.paid_pct_of_po||0).toFixed(1)}%</td><td>${signals}</td><td style="max-width:380px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${(r.evidence_notes||'').replace(/"/g,'&quot;')}">${r.evidence_notes||''}</td></tr>`;
    });
    html+='</tbody></table>';
    wrap.innerHTML=html;
    if(dtI[tableId])try{dtI[tableId].destroy();}catch(e){}
    dtI[tableId]=$("#"+tableId).DataTable(dtOpts({pageLength:20,order:[[5,'desc']]}));
}

/* ====== STATIONS ====== */
async function loadStations(){
    const res=await fetch(apiUrl('/api/stations'));const stations=await res.json();
    const sel=document.getElementById('stationSelect');
    sel.innerHTML='<option value="">-- select a station --</option>';

    // Group: BASE -> MOD/CELL/INV -> stations
    const tree={};
    stations.forEach(s=>{
        const sid=s.station_id||'';
        const m=sid.match(/^(BASE\d+)-(MOD\d+|CELL\d+|INV\d+)/);
        const mod=m?m[1]+'-'+m[2]:'Other';
        if(!tree[mod])tree[mod]=[];
        tree[mod].push(s);
    });
    // Sort: CELL under MOD grouping
    Object.keys(tree).sort().forEach(mod=>{
        const og=document.createElement('optgroup');
        og.label=mod;
        tree[mod].sort((a,b)=>(a.station_id||'').localeCompare(b.station_id||'')).forEach(s=>{
            const o=document.createElement('option');
            o.value=s.station_id;
            o.textContent=s.station_id+(s.station_name?' - '+s.station_name:'');
            og.appendChild(o);
        });
        sel.appendChild(og);
    });
}

async function loadStationDetail(){
    const sid=document.getElementById('stationSelect').value;
    if(!sid)return;
    const res=await fetch('/api/station/'+encodeURIComponent(sid));const d=await res.json();
    const m=d.meta||{};
    const forecast=parseFloat(m.forecasted_cost)||0;
    const actual=parseFloat(m.actual_spend)||0;
    const variance=actual-forecast;

    document.getElementById('station-kpis').innerHTML=`
        <div class="kpi"><div class="label">Station</div><div class="value" style="font-size:16px">${sid}</div><div class="sub">${m.station_name||''} &middot; Owner: ${m.owner||'--'}</div></div>
        <div class="kpi"><div class="label">Forecasted</div><div class="value dollar">${fmtF$(forecast)}</div>
            <div class="sub"><input class="forecast-input" id="fc-edit" type="number" step="100" value="${forecast.toFixed(0)}"/><button class="forecast-save" onclick="saveForecast('${sid}')">Save</button><span class="forecast-saved" id="fc-ok" style="display:none">Saved</span></div></div>
        <div class="kpi"><div class="label">Actual Spend</div><div class="value dollar">${fmtF$(actual)}</div></div>
        <div class="kpi"><div class="label">Variance</div><div class="value ${vc(variance)}">${fmtF$(variance)}</div><div class="sub">${variance>0?'Over':'Under'} budget</div></div>
        <div class="kpi"><div class="label">Line Items</div><div class="value">${(m.line_count||0).toLocaleString()}</div></div>`;

    if(d.vendors&&d.vendors.length){
        const vSorted=[...d.vendors].sort((a,b)=>a.spend-b.spend);
        Plotly.newPlot('chart-station-vendors',[{
            y:vSorted.map(v=>v.vendor.length>30?v.vendor.substring(0,30)+'...':v.vendor),
            x:vSorted.map(v=>v.spend),type:'bar',orientation:'h',marker:{color:C.green},
            text:vSorted.map(v=>fmt$(v.spend)),textposition:'outside',textfont:{color:C.muted,size:10},
            hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(250,vSorted.length*30),margin:{l:Math.max(160,Math.max(...vSorted.map(v=>v.vendor.length))*6),r:70,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
    } else { document.getElementById('chart-station-vendors').innerHTML='<p style="color:var(--muted);padding:20px">No vendor data for this station.</p>'; }

    if(d.timeline&&d.timeline.length){
        Plotly.newPlot('chart-station-timeline',[{
            x:d.timeline.map(t=>t.date),y:d.timeline.map(t=>t.amount),
            text:d.timeline.map(t=>(t.po||'')+': '+(t.vendor||'')+' - '+(t.desc||'')),
            type:'scatter',mode:'markers+lines',
            marker:{size:10,color:C.green,line:{color:C.surface,width:1}},
            line:{color:'rgba(178,221,121,0.3)',width:1},
            hovertemplate:'%{x}<br>%{text}<br>%{y:$,.2f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:70,r:20,t:20,b:50},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2}},PC);
        document.getElementById('chart-station-timeline').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const m=p.x.substring(0,7);openDrill(sid+' | '+p.x,{station:sid,month:m});}});
    } else { document.getElementById('chart-station-timeline').innerHTML='<p style="color:var(--muted);padding:20px">No dated orders for this station.</p>'; }

    let bom='<table id="station-bom" class="display compact" style="width:100%"><thead><tr><th>Description</th><th>Sub-Category</th><th>Vendor</th><th>Qty</th><th>Unit Price</th><th>Subtotal</th><th>PO</th><th>Parts</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    (d.lines||[]).forEach(r=>{
        let parts='';try{if(r.part_numbers&&r.part_numbers!=='[]')parts=JSON.parse(r.part_numbers).map(p=>p.value).join(', ');}catch(e){}
        bom+=`<tr><td>${r.item_description||''}</td><td>${r.mfg_subcategory||(r.product_category||'').replace('Non-Inventory: ','')}</td><td>${r.vendor_name||''}</td><td>${r.product_qty||''}</td><td>${fmtF$(parseFloat(r.price_unit)||0)}</td><td>${fmtF$(parseFloat(r.price_subtotal)||0)}</td><td>${r.po_number||''}</td><td>${parts}</td></tr>`;
    });
    bom+='</tbody></table>';
    document.getElementById('station-bom-table').innerHTML=bom;
    if(dtI['station-bom'])dtI['station-bom'].destroy();
    dtI['station-bom']=$('#station-bom').DataTable(dtOpts({pageLength:25,order:[[5,'desc']],dom:'Bfrtip',buttons:['csv','excel']}));
}

async function saveForecast(sid){
    const val=parseFloat(document.getElementById('fc-edit').value);
    if(isNaN(val))return;
    const res=await fetch('/api/forecast',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({station_id:sid,forecasted_cost:val})});
    const d=await res.json();
    if(d.ok){document.getElementById('fc-ok').style.display='inline';setTimeout(()=>{document.getElementById('fc-ok').style.display='none';},2000);showToast('Forecast saved');}
}

/* ====== FORECASTING ====== */
function setForecastRefreshMessage(msg,isError){
    const el=document.getElementById('forecast-refresh-msg');
    if(!el)return;
    el.textContent=msg||'';
    el.style.color=isError?C.red:C.muted;
}

async function loadForecasting(){
    const [dataRes,settingsRes]=await Promise.all([
        fetch(apiUrl('/api/forecasting')),
        fetch('/api/settings'),
    ]);
    const payload=await dataRes.json();
    const settings=await settingsRes.json();
    const rows=payload.rows||[];
    const groups=payload.groups||[];
    document.getElementById('forecast-bf1-url').value=settings.bf1_sheet_url||'';
    document.getElementById('forecast-bf2-url').value=settings.bf2_sheet_url||'';

    forecastOriginal={};
    if(!rows.length){
        document.getElementById('forecast-table-wrap').innerHTML='<p style="color:var(--muted);padding:18px">No stations found for the current line filter.</p>';
        return;
    }

    const groupMap={};
    groups.forEach(g=>{groupMap[g.line]=g;});

    let html='<table id="forecast-table" class="display compact" style="width:100%"><thead><tr><th style="min-width:170px">Station</th><th>Station Name</th><th>Owner</th><th>Forecast</th><th>Actual</th><th>Variance</th></tr></thead><tbody>';
    let currentLine='';
    rows.forEach(r=>{
        const line=r.line||'Other';
        if(line!==currentLine){
            currentLine=line;
            const g=groupMap[line]||{};
            html+=`<tr><td colspan="6" style="background:var(--surface2);color:var(--green);font-weight:700;padding:8px 10px">${line} &middot; Stations: ${(g.station_count||0).toLocaleString()} &middot; Forecast: ${fmtF$(g.total_forecast||0)} &middot; Actual: ${fmtF$(g.total_actual||0)}</td></tr>`;
        }
        const sid=r.station_id||'';
        const forecast=parseFloat(r.forecasted_cost)||0;
        const actual=parseFloat(r.actual_spend)||0;
        const variance=parseFloat(r.variance)||actual-forecast;
        forecastOriginal[sid]=forecast;
        const lockHtml=r.is_locked
            ? `<span title="Locked by manual override. Sheets refresh will not change this value." style="display:inline-block;margin-left:6px;font-size:12px;color:#E8A838">&#128274;</span><button class="forecast-unlock" type="button" data-station-id="${sid}" onclick="unlockForecastOverride(this.dataset.stationId)">Unlock</button>`
            : `<button class="forecast-lock" type="button" data-station-id="${sid}" onclick="lockForecastOverride(this.dataset.stationId)">Lock</button>`;
        html+=`<tr>
            <td style="font-weight:600;color:var(--green)">${sid}${lockHtml}</td>
            <td>${r.station_name||''}</td>
            <td>${r.owner||''}</td>
            <td><input class="forecast-input forecast-edit" data-station-id="${sid}" type="number" min="0" step="100" value="${forecast.toFixed(2)}"/></td>
            <td class="dollar">${fmtF$(actual)}</td>
            <td class="${vc(variance)}">${fmtF$(variance)}</td>
        </tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('forecast-table-wrap').innerHTML=html;
}

async function lockForecastOverride(stationId){
    const sid=(stationId||'').trim();
    if(!sid)return;
    try{
        const res=await fetch('/api/forecast/lock',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({station_id:sid}),
        });
        const d=await res.json();
        if(!res.ok||!d.ok){
            setForecastRefreshMessage(d.error||'Failed to lock override',true);
            return;
        }
        setForecastRefreshMessage(`Locked override for ${sid}. Sheets refresh will skip this row.`,false);
        showToast('Forecast override locked');
        await loadForecasting();
    }catch(err){
        setForecastRefreshMessage('Failed to lock override',true);
    }
}

function forecastTableStationIds(){
    return [...document.querySelectorAll('#forecast-table-wrap input.forecast-edit')]
        .map(inp=>(inp.dataset.stationId||'').trim())
        .filter(Boolean);
}

async function lockAllForecastOverrides(){
    const stationIds=forecastTableStationIds();
    if(!stationIds.length){
        showToast('No forecast rows to lock');
        return;
    }
    if(!window.confirm(`Lock overrides for ${stationIds.length} row${stationIds.length===1?'':'s'}?`))return;
    try{
        const res=await fetch('/api/forecast/lock_all',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({station_ids:stationIds}),
        });
        const d=await res.json();
        if(!res.ok||!d.ok){
            setForecastRefreshMessage(d.error||'Failed to lock all overrides',true);
            return;
        }
        let msg=`Locked ${d.locked_count||0} row${(d.locked_count||0)===1?'':'s'}.`;
        if((d.not_found_count||0)>0)msg+=` ${d.not_found_count} row${d.not_found_count===1?' was':'s were'} not found.`;
        setForecastRefreshMessage(msg,false);
        showToast('Forecast overrides locked');
        await loadForecasting();
    }catch(err){
        setForecastRefreshMessage('Failed to lock all overrides',true);
    }
}

async function unlockForecastOverride(stationId){
    const sid=(stationId||'').trim();
    if(!sid)return;
    if(!window.confirm(`Unlock forecast override for ${sid}?`))return;
    try{
        const res=await fetch('/api/forecast/unlock',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({station_id:sid}),
        });
        const d=await res.json();
        if(!res.ok||!d.ok){
            setForecastRefreshMessage(d.error||'Failed to unlock override',true);
            return;
        }
        setForecastRefreshMessage(`Unlocked override for ${sid}. Sheets refresh can update this row again.`,false);
        showToast('Forecast override unlocked');
        await loadForecasting();
    }catch(err){
        setForecastRefreshMessage('Failed to unlock override',true);
    }
}

async function unlockAllForecastOverrides(){
    const stationIds=forecastTableStationIds();
    if(!stationIds.length){
        showToast('No forecast rows to unlock');
        return;
    }
    if(!window.confirm(`Unlock overrides for ${stationIds.length} row${stationIds.length===1?'':'s'}?`))return;
    try{
        const res=await fetch('/api/forecast/unlock_all',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({station_ids:stationIds}),
        });
        const d=await res.json();
        if(!res.ok||!d.ok){
            setForecastRefreshMessage(d.error||'Failed to unlock all overrides',true);
            return;
        }
        let msg=`Unlocked ${d.removed_count||0} row${(d.removed_count||0)===1?'':'s'}.`;
        if((d.not_found_count||0)>0)msg+=` ${d.not_found_count} row${d.not_found_count===1?' had':'s had'} no lock.`;
        setForecastRefreshMessage(msg,false);
        showToast('Forecast overrides unlocked');
        await loadForecasting();
    }catch(err){
        setForecastRefreshMessage('Failed to unlock all overrides',true);
    }
}

async function saveForecastingBulk(){
    const inputs=[...document.querySelectorAll('#forecast-table-wrap input.forecast-edit')];
    if(!inputs.length){
        showToast('No forecast rows to save');
        return;
    }
    const updates=[];
    let invalid=0;
    inputs.forEach(inp=>{
        const sid=inp.dataset.stationId||'';
        const val=parseFloat(inp.value);
        if(!sid||isNaN(val)||val<0){
            invalid+=1;
            return;
        }
        const before=forecastOriginal[sid];
        if(before==null||Math.abs(before-val)>0.0001){
            updates.push({station_id:sid,forecasted_cost:val});
        }
    });
    if(!updates.length){
        showToast(invalid?'No valid forecast edits found':'No forecast changes to save');
        return;
    }
    const res=await fetch('/api/forecast/bulk',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({updates}),
    });
    const d=await res.json();
    if(!d.ok){
        setForecastRefreshMessage(d.error||'Failed to save forecasts',true);
        return;
    }
    document.getElementById('forecast-ok').style.display='inline';
    setTimeout(()=>{document.getElementById('forecast-ok').style.display='none';},2500);
    setForecastRefreshMessage(`Saved ${d.updated_count||0} updates (${d.skipped_count||0} skipped).`,false);
    showToast('Forecasts saved');
    await loadForecasting();
}

async function refreshForecastFromSheets(){
    const bf1=document.getElementById('forecast-bf1-url').value.trim();
    const bf2=document.getElementById('forecast-bf2-url').value.trim();
    const btn=document.getElementById('forecast-refresh-btn');
    if(!bf1||!bf2){
        setForecastRefreshMessage('Please provide both BF1 and BF2 sheet URLs.',true);
        return;
    }
    btn.disabled=true;
    btn.textContent='Refreshing...';
    setForecastRefreshMessage('Refreshing forecast values from Google Sheets...',false);
    try{
        const res=await fetch('/api/forecast/refresh',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({bf1_sheet_url:bf1,bf2_sheet_url:bf2}),
        });
        const d=await res.json();
        const errs=(d.errors||[]);
        if(!res.ok||!d.ok){
            const msg=errs.length?errs.join(' | '):(d.error||'Refresh failed');
            setForecastRefreshMessage(msg,true);
            return;
        }
        const bf1Applied=d.bf1&&d.bf1.applied_updates?d.bf1.applied_updates:0;
        const bf2Applied=d.bf2&&d.bf2.applied_updates?d.bf2.applied_updates:0;
        const lockedSkipped=d.locked_skipped_count||0;
        let msg=`Updated ${d.updated_count||0} stations (BF1: ${bf1Applied}, BF2: ${bf2Applied}).`;
        if(lockedSkipped)msg+=` Preserved ${lockedSkipped} locked override${lockedSkipped===1?'':'s'}.`;
        if(errs.length)msg+=` Warnings: ${errs.join(' | ')}`;
        setForecastRefreshMessage(msg,errs.length>0);
        showToast('Forecast refresh complete');
        await loadForecasting();
    }catch(err){
        setForecastRefreshMessage('Refresh failed. Please sign in again and confirm sheet access.',true);
    }finally{
        btn.disabled=false;
        btn.textContent='Refresh from Sheets';
    }
}

function reauthGoogleForSheets(){
    window.location.href='/auth/logout';
}

/* ====== VENDORS ====== */
async function loadVendors(){
    const res=await fetch(apiUrl('/api/vendors'));const vendors=await res.json();
    if(!vendors.length)return;

    const top5=vendors.slice(0,5);
    const t5=[...top5].reverse();
    Plotly.newPlot('chart-vendor-conc',[{
        y:t5.map(v=>v.vendor_name.length>30?v.vendor_name.substring(0,30)+'...':v.vendor_name),
        x:t5.map(v=>v.spend),type:'bar',orientation:'h',marker:{color:C.green},
        text:t5.map(v=>fmt$(v.spend)),textposition:'outside',textfont:{color:C.muted,size:10},
        hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'
    }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(250,t5.length*45),margin:{l:200,r:70,t:10,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
    document.getElementById('chart-vendor-conc').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const vn=top5.find(v=>v.vendor_name.startsWith(ev.points[0].y.replace('...','')));openDrill('Vendor: '+(vn?vn.vendor_name:ev.points[0].y),{vendor:vn?vn.vendor_name:ev.points[0].y});}});

    // Heatmap: top 10 vendors x top 10 stations with actual spend
    const txRes=await fetch(apiUrl('/api/transactions'));const txData=await txRes.json();
    const topV=vendors.slice(0,10).map(v=>v.vendor_name);
    const stationSpend={};
    txData.forEach(t=>{if(t.station_id)stationSpend[t.station_id]=(stationSpend[t.station_id]||0)+(parseFloat(t.price_subtotal)||0);});
    const topS=Object.entries(stationSpend).sort((a,b)=>b[1]-a[1]).slice(0,12).map(e=>e[0]);
    if(topV.length&&topS.length){
        const z=topV.map(v=>topS.map(s=>txData.filter(t=>t.vendor_name===v&&t.station_id===s).reduce((sum,t)=>sum+(parseFloat(t.price_subtotal)||0),0)));
        const maxZ=Math.max(...z.flat().filter(v=>v>0),1);
        Plotly.newPlot('chart-vendor-heatmap',[{
            z,x:topS,y:topV.map(v=>v.length>28?v.substring(0,28)+'...':v),
            type:'heatmap',
            colorscale:[[0,'#1A1A1A'],[0.01,'#2a3520'],[0.15,'#3d5a28'],[0.4,'#5a8a35'],[0.7,'#8abb55'],[1,'#D0F585']],
            hoverongaps:false,
            hovertemplate:'%{y}<br>%{x}<br>%{z:$,.0f}<extra></extra>',
            zmin:0,zmax:maxZ,
            colorbar:{tickprefix:'$',tickformat:',.0s',tickfont:{color:C.muted},outlinewidth:0}
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(350,topV.length*35),margin:{l:220,r:20,t:20,b:100},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickangle:-45}},PC);
        document.getElementById('chart-vendor-heatmap').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const vIdx=p.pointIndex[0];const sIdx=p.pointIndex[1];const vn=topV[vIdx]||'';const sn=topS[sIdx]||'';openDrill(vn.substring(0,25)+' x '+sn,{vendor:vn,station:sn});}});
    }

    let html='<table id="vendor-tbl" class="display compact" style="width:100%"><thead><tr><th>Vendor</th><th>Spend</th><th>PO Count</th><th>Stations</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    vendors.forEach(v=>{html+=`<tr><td>${v.vendor_name}</td><td class="dollar">${fmtF$(v.spend)}</td><td>${v.po_count}</td><td>${v.stations||''}</td></tr>`;});
    html+='</tbody></table>';
    document.getElementById('vendor-table-wrap').innerHTML=html;
    if(dtI['vendor-tbl'])dtI['vendor-tbl'].destroy();
    dtI['vendor-tbl']=$('#vendor-tbl').DataTable(dtOpts({pageLength:25,order:[[1,'desc']],dom:'Bfrtip',buttons:['csv','excel']}));
}

/* ====== ASSETS ====== */
let assetsData=[],assetMode='asset',assetSubcatActive=new Set();
const scColorMap={'Process Equipment':'#8abb55','Controls & Electrical':'#048EE5','Mechanical & Structural':'#E8A838','Design & Engineering Services':'#9B7ED8','Integration & Commissioning':'#F06292','Quality & Metrology':'#4DD0E1','Software & Licenses':'#CE93D8','MFG Tools & Shop Supplies':'#A1887F','Consumables':'#FFB74D','Shipping & Freight':'#78909C','Facilities & Office':'#555','IT Equipment':'#607D8B','General & Administrative':'#455A64'};
const statusColors={Ordered:'#78909C',Shipped:'#048EE5',Received:'#E8A838',Installed:'#B2DD79',Commissioned:'#4DD0E1'};
function setAssetMode(mode){
    assetMode=mode;
    document.getElementById('assetModeAsset').classList.toggle('active',mode==='asset');
    document.getElementById('assetModeTotal').classList.toggle('active',mode==='total');
    if(assetsData.length)filterAssets();
}
function toggleAssetSubcat(sc){
    if(assetSubcatActive.has(sc))assetSubcatActive.delete(sc);else assetSubcatActive.add(sc);
    renderSubcatChips();filterAssets();
}
function clearAssetSubcats(){assetSubcatActive.clear();renderSubcatChips();filterAssets();}
function renderSubcatChips(){
    const allSc=new Set();
    assetsData.forEach(r=>(r.sc_breakdown||[]).forEach(b=>{if(b.subcategory)allSc.add(b.subcategory);}));
    let html='';
    [...allSc].sort().forEach(sc=>{
        const active=assetSubcatActive.has(sc);const col=scColorMap[sc]||'#555';
        html+=`<span onclick="toggleAssetSubcat('${sc}')" style="cursor:pointer;display:inline-block;padding:4px 10px;border-radius:4px;font-size:11px;font-weight:600;border:1px solid ${active?col:'var(--border)'};background:${active?'rgba(178,221,121,.12)':'var(--surface)'};color:${active?col:'var(--muted)'};transition:all .15s">${sc}</span>`;
    });
    if(assetSubcatActive.size)html+=`<span onclick="clearAssetSubcats()" style="cursor:pointer;display:inline-block;padding:4px 10px;border-radius:4px;font-size:11px;font-weight:600;border:1px solid var(--border);color:var(--muted)">Clear All</span>`;
    document.getElementById('asset-subcat-chips').innerHTML=html;
}
async function saveAssetDate(sid,milestone,val){
    const res=await fetch('/api/asset-status',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({station_id:sid,milestone:milestone,date:val})});
    const d=await res.json();
    if(d.ok){
        const row=assetsData.find(r=>r.station_id===sid);
        if(row){row.status=d.status;row['date_'+milestone]=val;}
        showToast(sid+' '+milestone+' saved');
        filterAssets();
    }
}
async function loadAssets(){
    const res=await fetch(apiUrl('/api/assets'));const d=await res.json();
    if(!d.stations||!d.stations.length){document.getElementById('asset-kpis').innerHTML='<div class="kpi"><div class="label">No Data</div></div>';return;}
    assetsData=d.stations;
    const k=d.kpis;

    const owners=new Set(),vendors=new Set();
    d.stations.forEach(r=>{if(r.owner)owners.add(r.owner);if(r.primary_vendor)vendors.add(r.primary_vendor);});
    const oSel=document.getElementById('assetOwnerFilter');
    const vSel=document.getElementById('assetVendorFilter');
    oSel.innerHTML='<option value="">All Owners</option>';
    vSel.innerHTML='<option value="">All Vendors</option>';
    [...owners].sort().forEach(o=>{const el=document.createElement('option');el.value=o;el.textContent=o;oSel.appendChild(el);});
    [...vendors].sort().forEach(v=>{const el=document.createElement('option');el.value=v;el.textContent=v;vSel.appendChild(el);});

    const sc=k.status_counts||{};
    let statusKpis='';
    ['Ordered','Shipped','Received','Installed','Commissioned'].forEach(s=>{
        const cnt=sc[s]||0;if(cnt||s==='Ordered')statusKpis+=`<div class="kpi"><div class="label">${s}</div><div class="value" style="color:${statusColors[s]||'var(--muted)'}">${cnt}</div></div>`;
    });
    document.getElementById('asset-kpis').innerHTML=`
        <div class="kpi"><div class="label">Stations Tracked</div><div class="value">${k.station_count}</div></div>
        <div class="kpi"><div class="label">Total Asset Value</div><div class="value dollar" style="color:var(--green)">${fmt$(k.total_asset_value)}</div><div class="sub">Physical equipment only</div></div>
        <div class="kpi"><div class="label">Total Investment</div><div class="value dollar">${fmt$(k.total_investment)}</div><div class="sub">Incl. services, shipping, etc.</div></div>
        <div class="kpi"><div class="label">Services</div><div class="value dollar" style="color:#9B7ED8">${fmt$(k.services_total)}</div></div>
        ${statusKpis}`;

    renderSubcatChips();filterAssets();
}
function filterAssets(){
    const of=document.getElementById('assetOwnerFilter').value;
    const sf=document.getElementById('assetStatusFilter').value;
    const vf=document.getElementById('assetVendorFilter').value;
    let data=assetsData;
    if(of)data=data.filter(r=>r.owner===of);
    if(sf)data=data.filter(r=>r.status===sf);
    if(vf)data=data.filter(r=>r.primary_vendor===vf);
    if(assetSubcatActive.size){
        data=data.filter(r=>{
            const scs=new Set((r.sc_breakdown||[]).map(b=>b.subcategory));
            for(const sc of assetSubcatActive){if(scs.has(sc))return true;}return false;
        });
    }
    renderAssets(data);
}
function renderAssets(data){
    const valKey=assetMode==='asset'?'asset_value':'total_investment';
    const valLabel=assetMode==='asset'?'Asset Value':'Total Investment';

    const sorted=[...data].sort((a,b)=>b[valKey]-a[valKey]);
    const top=sorted.slice(0,30);
    if(top.length){
        const labels=top.map(r=>(r.station_id.replace(/^BASE\d+-\w+-/,'')+' '+r.station_name).substring(0,35));
        const maxLbl=Math.max(...labels.map(l=>l.length));
        const linePalette=['#B2DD79','#048EE5','#E8A838','#9B7ED8','#F06292','#4DD0E1','#FFB74D','#78909C','#CE93D8','#A1887F'];
        const lines=[...new Set(top.map(r=>r.line||'Unknown'))].sort();
        const lineColor={};
        lines.forEach((ln,i)=>{lineColor[ln]=linePalette[i%linePalette.length];});
        Plotly.newPlot('chart-asset-bars',[
            {
                y:labels,
                x:top.map(r=>r[valKey]),
                customdata:top.map(r=>r.line||'Unknown'),
                type:'bar',
                orientation:'h',
                name:valLabel,
                marker:{color:top.map(r=>lineColor[r.line||'Unknown']),line:{color:'rgba(0,0,0,.35)',width:0.5}},
                text:top.map(r=>fmt$(r[valKey])),
                textposition:'outside',
                textfont:{color:C.muted,size:10},
                hovertemplate:'%{y}<br>Line: %{customdata}<br>%{x:$,.0f}<extra></extra>'
            }
        ],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:10},height:Math.max(400,top.length*28),margin:{l:Math.max(180,maxLbl*7),r:80,t:20,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2,tickprefix:'$',tickformat:',.0s'}},PC);
        document.getElementById('chart-asset-bars').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const idx=ev.points[0].pointIndex;const sid=top[idx].station_id;openDrill('Station: '+sid,{station:sid});}});
    }

    const stCounts={};
    data.forEach(r=>{stCounts[r.status]=(stCounts[r.status]||0)+1;});
    const stLabels=Object.keys(stCounts),stVals=Object.values(stCounts);
    Plotly.newPlot('chart-asset-delivery',[{
        type:'pie',labels:stLabels,values:stVals,hole:.5,
        marker:{colors:stLabels.map(s=>statusColors[s]||'#555')},
        textinfo:'label+value',textfont:{size:11},
        hovertemplate:'%{label}: %{value} stations<extra></extra>'
    }],{paper_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:20,r:20,t:20,b:20},showlegend:false},PC);

    const totAsset=data.reduce((s,r)=>s+r.asset_value,0);
    const totSvc=data.reduce((s,r)=>s+r.services_cost,0);
    const totShip=data.reduce((s,r)=>s+r.shipping_cost,0);
    const totCons=data.reduce((s,r)=>s+r.consumables_cost,0);
    const totAll=data.reduce((s,r)=>s+r.total_investment,0);
    const totOther=totAll-totAsset-totSvc-totShip-totCons;
    Plotly.newPlot('chart-asset-composition',[{
        type:'pie',labels:['Physical Asset','Services & Labor','Shipping','Consumables','Other'],
        values:[totAsset,totSvc,totShip,totCons,totOther>0?totOther:0],hole:.45,
        marker:{colors:[C.green,'#9B7ED8','#78909C','#FFB74D','#555']},
        textinfo:'label+percent',textfont:{size:11},
        hovertemplate:'%{label}<br>%{value:$,.0f}<br>%{percent}<extra></extra>'
    }],{paper_bgcolor:C.surface,font:{color:C.text,size:11},margin:{l:20,r:20,t:20,b:20},showlegend:false},PC);

    let html='<table id="asset-tbl" class="display compact" style="width:100%"><thead><tr>';
    const cols=['Station','Name','Line','Owner','OEM Vendor',''+valLabel,'Services','POs','Status','Ordered','Shipped','Received','Installed','Commissioned','Sub-Categories'];
    cols.forEach(c=>{html+='<th>'+c+'</th>';});
    html+='</tr></thead><tfoot><tr>';
    cols.forEach(()=>{html+='<th></th>';});
    html+='</tr></tfoot><tbody>';
    data.forEach(r=>{
        const sc=statusColors[r.status]||'var(--muted)';
        const scTags=(r.sc_breakdown||[]).map(b=>{const col=scColorMap[b.subcategory]||'#555';return `<span style="display:inline-block;padding:1px 6px;border-radius:3px;font-size:9px;font-weight:600;background:${col}22;color:${col};margin:1px 2px;white-space:nowrap">${b.subcategory} ${fmt$(b.spend)}</span>`;}).join('');
        const sid=r.station_id;
        const dtCell=(ms)=>`<input class="asset-date" type="date" value="${r['date_'+ms]||''}" onchange="saveAssetDate('${sid}','${ms}',this.value)"/>`;
        html+=`<tr>
            <td>${sid}</td>
            <td>${r.station_name}</td>
            <td>${r.line}</td>
            <td>${r.owner}</td>
            <td>${r.primary_vendor}</td>
            <td class="dollar">${fmtF$(r[valKey])}</td>
            <td class="dollar">${fmtF$(r.services_cost)}</td>
            <td>${r.po_count}</td>
            <td style="color:${sc};font-weight:700">${r.status}</td>
            <td>${dtCell('ordered')}</td>
            <td>${dtCell('shipped')}</td>
            <td>${dtCell('received')}</td>
            <td>${dtCell('installed')}</td>
            <td>${dtCell('commissioned')}</td>
            <td>${scTags}</td>
        </tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('asset-table-wrap').innerHTML=html;
    if(dtI['asset-tbl'])dtI['asset-tbl'].destroy();
    dtI['asset-tbl']=$('#asset-tbl').DataTable(dtOpts({pageLength:50,order:[[0,'asc']],dom:'Bfrtip',buttons:['csv','excel']}));
}

/* ====== SPARES ====== */
const SPARES_DEFAULT_BUCKET='';
async function loadSpares(){
    const res=await fetch('/api/spares');sparesData=await res.json();
    if(!sparesData.length){document.getElementById('spares-table-wrap').innerHTML='<p style="color:var(--muted)">No spares data.</p>';return;}

    const buckets=new Set(),stations=new Set(),cats=new Set(),subcats=new Set(),vendors=new Set();
    sparesData.forEach(r=>{
        if(r.item_bucket)buckets.add(r.item_bucket);
        if(r.station_ids)(r.station_ids+'').split(',').forEach(s=>{s=s.trim();if(s)stations.add(s);});
        if(r.product_category)cats.add(r.product_category);
        if(r.mfg_subcategory)subcats.add(r.mfg_subcategory);
        if(r.mfg_subcategories)(r.mfg_subcategories+'').split(',').forEach(sc=>{sc=sc.trim();if(sc)subcats.add(sc);});
        if(r.vendor_names)(r.vendor_names+'').split(',').forEach(v=>{v=v.trim();if(v)vendors.add(v);});
    });
    const bSel=document.getElementById('sparesBucketFilter');
    const sSel=document.getElementById('sparesStationFilter');
    const scSel=document.getElementById('sparesSubcatFilter');
    const cSel=document.getElementById('sparesCatFilter');
    const vSel=document.getElementById('sparesVendorFilter');
    bSel.innerHTML='<option value="">All Buckets</option>';
    sSel.innerHTML='<option value="">All Stations</option>';
    scSel.innerHTML='<option value="">All Sub-Categories</option>';
    cSel.innerHTML='<option value="">All Categories</option>';
    vSel.innerHTML='<option value="">All Vendors</option>';
    [...buckets].sort().forEach(b=>{const o=document.createElement('option');o.value=b;o.textContent=b;if(b===SPARES_DEFAULT_BUCKET)o.selected=true;bSel.appendChild(o);});
    [...stations].sort().forEach(s=>{const o=document.createElement('option');o.value=s;o.textContent=s;sSel.appendChild(o);});
    [...subcats].sort().forEach(sc=>{const o=document.createElement('option');o.value=sc;o.textContent=sc;scSel.appendChild(o);});
    [...cats].sort().forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c.replace('Non-Inventory: ','');cSel.appendChild(o);});
    [...vendors].sort().forEach(v=>{const o=document.createElement('option');o.value=v;o.textContent=v;vSel.appendChild(o);});

    if(SPARES_DEFAULT_BUCKET && buckets.has(SPARES_DEFAULT_BUCKET))bSel.value=SPARES_DEFAULT_BUCKET;
    renderBucketSummary();
    filterSpares();
}
function renderBucketSummary(){
    const summary={};
    sparesData.forEach(r=>{
        const primary=(r.mfg_subcategory||'').trim();
        const fallback=(r.mfg_subcategories||'').split(',').map(s=>s.trim()).filter(Boolean)[0]||'Uncategorized';
        const sc=primary||fallback;
        if(!summary[sc])summary[sc]={count:0,spend:0};
        summary[sc].count++;
        summary[sc].spend+=(r.total_spend||0);
    });
    const sorted=Object.entries(summary).sort((a,b)=>b[1].spend-a[1].spend);
    const scColors={'Process Equipment':'#8abb55','Controls & Electrical':'#048EE5','Mechanical & Structural':'#E8A838','Design & Engineering Services':'#9B7ED8','Integration & Commissioning':'#F06292','Quality & Metrology':'#4DD0E1','Software & Licenses':'#CE93D8','MFG Tools & Shop Supplies':'#A1887F','Consumables':'#FFB74D','Shipping & Freight':'#78909C','Facilities & Office':'#555','IT Equipment':'#607D8B','General & Administrative':'#455A64','Uncategorized':'#555'};
    let html='<div style="display:flex;flex-wrap:wrap;gap:8px">';
    sorted.forEach(([sc,d])=>{
        const col=scColors[sc]||'#555';
        const active=document.getElementById('sparesSubcatFilter').value;
        const sel=active===sc;
        html+=`<div onclick="document.getElementById('sparesSubcatFilter').value='${sc}';filterSpares();renderBucketSummary();" style="cursor:pointer;padding:8px 14px;background:${sel?'rgba(178,221,121,.12)':'var(--surface)'};border:1px solid ${sel?col:'var(--border)'};border-radius:8px;min-width:130px;transition:all .15s">`;
        html+=`<div style="font-size:10px;color:${col};font-weight:700;text-transform:uppercase;letter-spacing:.5px">${sc}</div>`;
        html+=`<div style="font-size:16px;font-weight:700;margin-top:2px">${fmtF$(d.spend)}</div>`;
        html+=`<div style="font-size:11px;color:var(--muted)">${d.count} items</div></div>`;
    });
    html+='<div onclick="document.getElementById(\'sparesSubcatFilter\').value=\'\';filterSpares();renderBucketSummary();" style="cursor:pointer;padding:8px 14px;background:var(--surface);border:1px solid var(--border);border-radius:8px;display:flex;align-items:center"><div style="font-size:11px;color:var(--muted);font-weight:600">Show All</div></div>';
    html+='</div>';
    document.getElementById('spares-bucket-summary').innerHTML=html;
}
function filterSpares(){
    const bf=document.getElementById('sparesBucketFilter').value;
    const sf=document.getElementById('sparesStationFilter').value;
    const scf=document.getElementById('sparesSubcatFilter').value;
    const cf=document.getElementById('sparesCatFilter').value;
    const vf=document.getElementById('sparesVendorFilter').value;
    let data=sparesData;
    if(bf)data=data.filter(r=>r.item_bucket===bf);
    if(sf)data=data.filter(r=>(r.station_ids+'').includes(sf));
    if(scf)data=data.filter(r=>(r.mfg_subcategory===scf)||((r.mfg_subcategories+'').includes(scf)));
    if(cf)data=data.filter(r=>r.product_category===cf);
    if(vf)data=data.filter(r=>(r.vendor_names+'').includes(vf));
    renderBucketSummary();
    renderSpares(data);
}
function renderSpares(data){
    let html='<table id="spares-tbl" class="display compact" style="width:100%"><thead><tr><th>Description</th><th>Bucket</th><th>Sub-Category</th><th>Source</th><th>Category</th><th>Vendors</th><th>Stations</th><th>Qty</th><th>Avg Price</th><th>Total Spend</th><th>PO / Contact</th><th>Last Order</th><th>Parts</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    data.forEach(r=>{
        let parts='';try{if(r.part_numbers&&r.part_numbers!=='[]')parts=JSON.parse(r.part_numbers).map(p=>p.value).join(', ');}catch(e){}
        const src=(r.source||'').split(',').map(s=>s.trim()).map(s=>`<span class="source-badge ${s}">${s}</span>`).join(' ');
        const sc=(r.mfg_subcategories||r.mfg_subcategory||'');
        html+=`<tr><td>${r.item_description||''}</td><td>${r.item_bucket||''}</td><td>${sc}</td><td>${src}</td><td>${(r.product_category||'').replace('Non-Inventory: ','')}</td><td>${r.vendor_names||''}</td><td>${r.station_ids||''}</td><td>${r.total_qty_ordered||''}</td><td class="dollar">${fmtF$(r.avg_unit_price)}</td><td class="dollar">${fmtF$(r.total_spend)}</td><td>${r.po_or_contact||''}</td><td>${r.last_order_date||''}</td><td>${parts}</td></tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('spares-table-wrap').innerHTML=html;
    if(dtI['spares-tbl'])dtI['spares-tbl'].destroy();
    dtI['spares-tbl']=$('#spares-tbl').DataTable(dtOpts({pageLength:25,order:[[9,'desc']],dom:'Bfrtip',buttons:['csv','excel']}));
}

/* ====== DETAIL ====== */
async function loadDetail(){
    const res=await fetch(apiUrl('/api/transactions'));const data=await res.json();
    setDetailExpand(detailTableExpanded);
    if(!data.length){document.getElementById('detail-table-wrap').innerHTML='<p style="color:var(--muted)">No data.</p>';return;}
    const cols=['source','po_number','date_order','vendor_name','mfg_subcategory','item_description','station_id','mapping_confidence','price_subtotal','price_total','project_name','created_by_name'];
    const labels=['Source','PO','Date','Vendor','Sub-Category','Description','Station','Confidence','Subtotal','Total','Project','Created By'];
    if(detailTableExpanded){
        if(dtI['detail-tbl']){
            try{dtI['detail-tbl'].destroy();}catch(e){}
            dtI['detail-tbl']=null;
        }
        renderDetailExpanded(data,cols,labels);
        return;
    }
    let html='<table id="detail-tbl" class="display compact" style="width:100%"><thead><tr>';
    labels.forEach(l=>{html+=`<th>${l}</th>`;});
    html+='</tr></thead><tfoot><tr>';
    labels.forEach(()=>{html+='<th></th>';});
    html+='</tr></tfoot><tbody>';
    data.forEach(r=>{
        html+='<tr>';
        cols.forEach(c=>{
            const v=r[c]!==undefined?r[c]:'';
            if(c==='price_subtotal'||c==='price_total')html+=`<td class="dollar">${fmtF$(parseFloat(v)||0)}</td>`;
            else if(c==='source')html+=`<td><span class="source-badge ${v}">${v}</span></td>`;
            else html+=`<td>${v}</td>`;
        });
        html+='</tr>';
    });
    html+='</tbody></table>';
    document.getElementById('detail-table-wrap').innerHTML=html;
    if(dtI['detail-tbl'])dtI['detail-tbl'].destroy();
    dtI['detail-tbl']=$('#detail-tbl').DataTable(dtOpts({
        pageLength:50,
        order:[[8,'desc']],
        dom:'Bfrtip',
        buttons:['csv','excel'],
        scrollX:true,
        autoWidth:false
    }));
}

/* ====== TIMELINE ====== */
async function loadTimeline(){
    const res=await fetch(apiUrl('/api/timeline'));const d=await res.json();

    const L=function(h,m){return{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:h,margin:m,yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2}};};

    if(d.cumulative&&d.cumulative.length){
        Plotly.newPlot('chart-cumulative',[{
            x:d.cumulative.map(c=>c.date),y:d.cumulative.map(c=>c.cumulative),
            type:'scatter',mode:'lines',fill:'tozeroy',
            line:{color:C.green,width:2.5},fillcolor:'rgba(178,221,121,0.08)',
            hovertemplate:'%{x}<br>%{y:$,.0f}<extra></extra>'
        }],L(350,{l:80,r:20,t:20,b:50}),PC);
    }

    if(d.monthly_source&&d.monthly_source.length){
        const months=[...new Set(d.monthly_source.map(m=>m.month))].sort();
        const odooY=months.map(m=>{const r=d.monthly_source.find(x=>x.month===m&&x.source==='odoo');return r?r.spend:0;});
        const rampY=months.map(m=>{const r=d.monthly_source.find(x=>x.month===m&&x.source==='ramp');return r?r.spend:0;});
        const lay=L(300,{l:70,r:20,t:40,b:50});lay.barmode='group';lay.legend={font:{color:C.muted},x:0,y:1.15,orientation:'h'};
        Plotly.newPlot('chart-timeline-source',[
            {x:months,y:odooY,type:'bar',name:'Odoo PO',marker:{color:C.green},hovertemplate:'%{x}<br>Odoo: %{y:$,.0f}<extra></extra>'},
            {x:months,y:rampY,type:'bar',name:'Ramp CC',marker:{color:C.blue},hovertemplate:'%{x}<br>Ramp: %{y:$,.0f}<extra></extra>'},
        ],lay,PC);
        document.getElementById('chart-timeline-source').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const src=p.data.name==='Ramp CC'?'ramp':'odoo';openDrill(p.x+' | '+p.data.name,{month:p.x,source:src});}});
    }

    if(d.weekly&&d.weekly.length){
        const lay=L(300,{l:70,r:20,t:20,b:60});lay.xaxis.tickangle=-45;
        Plotly.newPlot('chart-weekly',[{
            x:d.weekly.map(w=>w.week),y:d.weekly.map(w=>w.spend),type:'bar',
            marker:{color:d.weekly.map(w=>w.spend>500000?C.red:w.spend>200000?C.yellow:w.spend>50000?C.blue:C.green)},
            text:d.weekly.map(w=>w.count+' items'),hovertemplate:'%{x}<br>%{y:$,.0f}<br>%{text}<extra></extra>'
        }],lay,PC);
        document.getElementById('chart-weekly').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const wk=ev.points[0].x;openDrill('Week: '+wk,{week:wk});}});
    }

    const scColors2={'Process Equipment':'#8abb55','Controls & Electrical':'#048EE5','Mechanical & Structural':'#E8A838','Design & Engineering Services':'#9B7ED8','Integration & Commissioning':'#F06292','Quality & Metrology':'#4DD0E1','Software & Licenses':'#CE93D8','MFG Tools & Shop Supplies':'#A1887F','Consumables':'#FFB74D','Shipping & Freight':'#78909C','Facilities & Office':'#555','IT Equipment':'#607D8B','General & Administrative':'#455A64'};
    if(d.monthly_subcat&&d.monthly_subcat.length){
        const scs=[...new Set(d.monthly_subcat.map(m=>m.subcategory))];
        const scMonths=[...new Set(d.monthly_subcat.map(m=>m.month))].sort();
        const scTraces=scs.map((sc,i)=>({
            x:scMonths,y:scMonths.map(m=>{const row=d.monthly_subcat.find(r=>r.month===m&&r.subcategory===sc);return row?row.spend:0;}),
            name:sc,type:'bar',marker:{color:scColors2[sc]||`hsl(${90+i*25},55%,52%)`}
        }));
        const scLay=L(400,{l:70,r:20,t:20,b:50});scLay.barmode='stack';scLay.legend={font:{color:C.muted,size:10}};
        Plotly.newPlot('chart-monthly-subcat',scTraces,scLay,PC);
        document.getElementById('chart-monthly-subcat').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];openDrill(p.x+' | '+p.data.name,{month:p.x,subcategory:p.data.name});}});
    }

    if(d.monthly_cat&&d.monthly_cat.length){
        const cats=[...new Set(d.monthly_cat.map(m=>m.category))];
        const months=[...new Set(d.monthly_cat.map(m=>m.month))].sort();
        const traces=cats.slice(0,10).map((cat,i)=>({
            x:months,y:months.map(m=>{const row=d.monthly_cat.find(r=>r.month===m&&r.category===cat);return row?row.spend:0;}),
            name:cat.replace('Non-Inventory: ',''),type:'bar',marker:{color:`hsl(${90+i*25},55%,${50+i*2}%)`}
        }));
        const lay=L(400,{l:70,r:20,t:20,b:50});lay.barmode='stack';lay.legend={font:{color:C.muted,size:10}};
        Plotly.newPlot('chart-monthly-cat',traces,lay,PC);
        document.getElementById('chart-monthly-cat').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const p=ev.points[0];const catName=p.data.name;const fullCat=d.monthly_cat.find(r=>r.category.replace('Non-Inventory: ','')==catName);openDrill(p.x+' | '+catName,{month:p.x,category:fullCat?fullCat.category:catName});}});
    }
}

/* ====== OTHER PROJECTS ====== */
async function loadProjects(){
    const res=await fetch('/api/projects');const d=await res.json();
    if(!d.projects||!d.projects.length){document.getElementById('proj-kpis').innerHTML='<p style="color:var(--muted)">No non-production project data.</p>';return;}

    document.getElementById('proj-kpis').innerHTML=`
        <div class="kpi"><div class="label">Non-Production Spend</div><div class="value dollar">${fmt$(d.total_spend)}</div><div class="sub">${(d.total_lines||0).toLocaleString()} line items</div></div>
        <div class="kpi"><div class="label">Project Categories</div><div class="value">${d.projects.length}</div></div>
        <div class="kpi"><div class="label">Largest Project</div><div class="value" style="font-size:14px">${d.projects[0].name}</div><div class="sub">${fmt$(d.projects[0].spend)}</div></div>`;

    // Breakdown bar chart
    const p=d.projects.slice(0,12);
    Plotly.newPlot('chart-proj-breakdown',[{
        y:p.map(x=>x.name.length>35?x.name.substring(0,35)+'...':x.name).reverse(),
        x:p.map(x=>x.spend).reverse(),
        type:'bar',orientation:'h',marker:{color:C.green},
        text:p.map(x=>fmt$(x.spend)+' ('+x.count+' items)').reverse(),textposition:'outside',textfont:{color:C.muted,size:10},
        hovertemplate:'%{y}<br>%{x:$,.0f}<extra></extra>'
    }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(300,p.length*35),margin:{l:250,r:100,t:20,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2}},PC);
    document.getElementById('chart-proj-breakdown').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const lbl=ev.points[0].y;const proj=d.projects.find(x=>x.name.startsWith(lbl.replace('...','')));const pn=proj?proj.name:lbl;openDrill('Project: '+pn,{project:pn});}});

    if(d.top_vendors&&d.top_vendors.length){
        const v=[...d.top_vendors].reverse();
        Plotly.newPlot('chart-proj-vendors',[{
            y:v.map(x=>x.vendor.length>28?x.vendor.substring(0,28)+'...':x.vendor),
            x:v.map(x=>x.spend),type:'bar',orientation:'h',marker:{color:C.blue},
            text:v.map(x=>fmt$(x.spend)),textposition:'outside',textfont:{color:C.muted,size:10}
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:Math.max(250,v.length*30),margin:{l:200,r:80,t:20,b:30},yaxis:{gridcolor:C.surface2,automargin:true},xaxis:{gridcolor:C.surface2}},PC);
        document.getElementById('chart-proj-vendors').on('plotly_click',function(ev){if(ev.points&&ev.points.length){const vn=d.top_vendors.find(x=>x.vendor.startsWith(ev.points[0].y.replace('...','')));openDrill('Vendor: '+(vn?vn.vendor:ev.points[0].y),{vendor:vn?vn.vendor:ev.points[0].y});}});
    }

    // Monthly
    if(d.monthly&&d.monthly.length){
        const pm=d.monthly.sort((a,b)=>a.month.localeCompare(b.month));
        Plotly.newPlot('chart-proj-monthly',[{
            x:pm.map(m=>m.month),y:pm.map(m=>m.spend),
            type:'bar',marker:{color:C.yellow},
            text:pm.map(m=>fmt$(m.spend)),textposition:'outside',textfont:{color:C.muted,size:10},
            hovertemplate:'%{x}<br>%{y:$,.0f}<extra></extra>'
        }],{paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:300,margin:{l:70,r:60,t:30,b:60},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,type:'category',tickangle:-45}},PC);
    }

    // Detail table
    if(d.details&&d.details.length){
        const cols=['source','po_number','date_order','vendor_name','item_description','station_id','project_name','price_subtotal','created_by_name'];
        const labels=['Source','PO','Date','Vendor','Description','Category','Project','Subtotal','Created By'];
        let html='<table id="proj-tbl" class="display compact" style="width:100%"><thead><tr>';
        labels.forEach(l=>{html+=`<th>${l}</th>`;});
        html+='</tr></thead><tfoot><tr>';
        labels.forEach(()=>{html+='<th></th>';});
        html+='</tr></tfoot><tbody>';
        d.details.forEach(r=>{
            html+='<tr>';
            cols.forEach(c=>{
                const v=r[c]!==undefined?r[c]:'';
                if(c==='price_subtotal')html+=`<td class="dollar">${fmtF$(parseFloat(v)||0)}</td>`;
                else if(c==='source')html+=`<td><span class="source-badge ${v}">${v}</span></td>`;
                else html+=`<td>${v}</td>`;
            });
            html+='</tr>';
        });
        html+='</tbody></table>';
        document.getElementById('proj-detail-wrap').innerHTML=html;
        if(dtI['proj-tbl'])dtI['proj-tbl'].destroy();
        dtI['proj-tbl']=$('#proj-tbl').DataTable(dtOpts({pageLength:25,order:[[7,'desc']],dom:'Bfrtip',buttons:['csv','excel'],scrollX:true}));
    }
}

/* ====== V2: CLASSIFICATION REVIEW QUEUE ====== */
async function loadReviews(){
    const res=await fetch('/api/v2/classification-reviews');const d=await res.json();
    const reviews=d.reviews||[];
    document.getElementById('review-count').textContent='('+reviews.length+' pending)';
    document.getElementById('review-kpis').innerHTML=`
        <div class="kpi"><div class="label">Pending Reviews</div><div class="value">${reviews.length}</div></div>
        <div class="kpi"><div class="label">Total $ at Stake</div><div class="value dollar">${fmt$(reviews.reduce((s,r)=>s+(r.price_subtotal||0),0))}</div></div>`;
    if(!reviews.length){
        document.getElementById('review-table-wrap').innerHTML='<p style="color:var(--muted);padding:20px">No pending reviews. Run an LLM review to check classifications.</p>';
        return;
    }
    const SUBCATS=['Process Equipment','Controls & Electrical','Mechanical & Structural','Consumables','MFG Tools & Shop Supplies','Design & Engineering Services','Integration & Commissioning','Quality & Metrology','Software & Licenses','Shipping & Freight','Facilities & Office','IT Equipment','General & Administrative'];
    reviews.sort((a,b)=>(b.price_subtotal||0)-(a.price_subtotal||0));
    let html='<table class="display" id="review-tbl" style="width:100%"><thead><tr><th>PO</th><th>PO Date</th><th>Vendor</th><th>Item Detail</th><th>Amount</th><th>Rule</th><th>LLM Suggestion</th><th>Actions</th></tr></thead><tbody>';
    reviews.forEach((r,idx)=>{
        const esc=s=>(s||'').replace(/'/g,"\\'").replace(/"/g,'&quot;');
        const poInfo=`<span style="font-weight:600;color:var(--green)">${r.po_number||''}</span><br><span style="font-size:10px;color:var(--muted)">${r.source||'odoo'}</span>`;
        const rawDate=(r.date_order||'').slice(0,10);
        const sortDate=rawDate||'';
        const itemDetail=`<div style="font-size:12px;max-width:300px"><div style="margin-bottom:2px" title="${esc(r.item_description)}">${r.item_description||''}</div><div style="font-size:10px;color:var(--muted)">Project: ${r.project_name||'--'}</div><div style="font-size:10px;color:var(--muted)">Category: ${r.product_category||'--'}</div></div>`;
        const ruleInfo=`${r.rule_subcat||'?'}<br><span style="font-size:10px;color:var(--muted)">station: ${r.rule_station||'none'}</span><br><span style="font-size:10px;color:var(--muted)">${r.rule_mapping_status||''}</span>`;
        const llmInfo=`<span style="color:var(--green);font-weight:600">${r.llm_subcat||'?'}</span><br><span style="font-size:10px;color:var(--muted)">station: ${r.llm_station||'none'}</span><br><span style="font-size:10px;color:var(--muted)">${r.llm_reasoning||''}</span>`;
        html+=`<tr id="rv-row-${idx}">
            <td style="font-size:12px">${poInfo}</td>
            <td data-order="${sortDate}" style="font-size:12px;white-space:nowrap">${rawDate?fmtDateMMDDYYYY(rawDate):'--'}</td>
            <td style="font-weight:600">${r.vendor_name||''}</td>
            <td>${itemDetail}</td>
            <td style="text-align:right" data-order="${r.price_subtotal||0}">${fmt$(r.price_subtotal||0)}</td>
            <td style="font-size:12px">${ruleInfo}</td>
            <td style="font-size:12px">${llmInfo}</td>
            <td style="min-width:280px">
                <div style="display:flex;gap:4px;margin-bottom:4px">
                    <button onclick="submitReview('${r.review_id}','llm_accepted','${esc(r.llm_station)}','${esc(r.llm_subcat)}',${idx})" style="padding:4px 10px;background:var(--green);color:var(--accent-dark);border:none;border-radius:4px;font-size:11px;cursor:pointer;font-weight:700">Accept LLM</button>
                    <button onclick="submitReview('${r.review_id}','rule_confirmed','${esc(r.rule_station)}','${esc(r.rule_subcat)}',${idx})" style="padding:4px 10px;background:var(--surface2);color:var(--text);border:none;border-radius:4px;font-size:11px;cursor:pointer">Keep Rule</button>
                </div>
                <div style="display:flex;gap:4px;align-items:center">
                    <input class="forecast-input" id="rv-station-${idx}" placeholder="Station ID" value="${r.llm_station||r.rule_station||''}" style="width:130px;font-size:11px;padding:3px 6px">
                    <select class="forecast-input" id="rv-subcat-${idx}" style="width:110px;font-size:11px;padding:3px 4px">
                        ${SUBCATS.map(sc=>'<option'+(sc===(r.llm_subcat||r.rule_subcat)?' selected':'')+'>'+sc+'</option>').join('')}
                    </select>
                    <button onclick="submitOverride('${r.review_id}',${idx})" style="padding:3px 8px;background:var(--blue);color:white;border:none;border-radius:4px;font-size:11px;cursor:pointer;font-weight:600">Override</button>
                </div>
            </td></tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('review-table-wrap').innerHTML=html;
    if(dtI['review-tbl'])dtI['review-tbl'].destroy();
    dtI['review-tbl']=$('#review-tbl').DataTable(dtOpts({pageLength:25,order:[[1,'desc']],columnDefs:[{orderable:false,targets:7}]}));
}
async function submitReview(reviewId,decision,stationId,subcat,idx){
    const row=document.getElementById('rv-row-'+idx);
    const res=await fetch('/api/v2/classification-feedback',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({review_id:reviewId,decision:decision,final_station_id:stationId,final_subcategory:subcat})});
    const d=await res.json();
    if(d.status==='ok'){row.style.opacity='0.3';row.style.pointerEvents='none';showToast('Feedback saved: '+decision);}
}
async function submitOverride(reviewId,idx){
    const station=document.getElementById('rv-station-'+idx).value.trim();
    const subcat=document.getElementById('rv-subcat-'+idx).value;
    if(!subcat){showToast('Select a subcategory');return;}
    const row=document.getElementById('rv-row-'+idx);
    const res=await fetch('/api/v2/classification-feedback',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({review_id:reviewId,decision:'human_override',final_station_id:station,final_subcategory:subcat})});
    const d=await res.json();
    if(d.status==='ok'){row.style.opacity='0.3';row.style.pointerEvents='none';showToast('Override saved: '+subcat+(station?' @ '+station:''));}
}
async function triggerLLMReview(){
    showToast('Starting LLM review...');
    const res=await fetch('/api/v2/run-classification-review',{method:'POST'});
    const d=await res.json();
    if(d.error){showToast('Error: '+d.error);return;}
    showToast('Review complete: '+d.disagreements+' disagreements found');
    loadReviews();
}

/* ====== V2: PAYMENT MILESTONES ====== */
async function loadMilestones(){
    const [tlRes,vpRes]=await Promise.all([fetch(apiUrl('/api/v2/po-timelines')),fetch(apiUrl('/api/v2/vendor-profiles'))]);
    const tlData=await tlRes.json();const vpData=await vpRes.json();
    const timelines=tlData.timelines||[];const profiles=vpData.profiles||[];

    const withPay=timelines.filter(t=>t.payment_count>0);
    const totalPaid=withPay.reduce((s,t)=>s+(t.total_paid||0),0);
    const avgCycle=withPay.length?Math.round(withPay.reduce((s,t)=>s+(t.total_cycle_days||0),0)/withPay.length):0;

    document.getElementById('milestone-kpis').innerHTML=`
        <div class="kpi"><div class="label">POs with Payments</div><div class="value">${withPay.length}</div></div>
        <div class="kpi"><div class="label">Total Paid</div><div class="value dollar">${fmt$(totalPaid)}</div></div>
        <div class="kpi"><div class="label">Avg Payment Cycle</div><div class="value">${avgCycle} days</div></div>
        <div class="kpi"><div class="label">Vendor Profiles</div><div class="value">${profiles.length}</div></div>`;
    document.getElementById('milestone-note').innerHTML='All events shown here are <strong>actual observed payments</strong> from Odoo bill/payment history. "Final" is marked only when cumulative paid amount reaches the PO amount (with small tolerance); otherwise latest row is not final.';

    if(withPay.length){
        const allPOs=withPay.sort((a,b)=>(b.total_amount||0)-(a.total_amount||0));
        const traces=[];
        const MS_DAY=24*60*60*1000;
        allPOs.forEach((t,i)=>{
            const start=t.po_date?new Date(t.po_date):null;
            if(!start)return;
            const events=(t.milestones||[])
                .filter(ms=>ms.label!=='PO Created'&&ms.date)
                .sort((a,b)=>new Date(a.date)-new Date(b.date));
            if(events.length>1){
                traces.push({
                    x:events.map(ms=>ms.date),
                    y:events.map(()=>t.po_number+' ('+t.vendor_name+')'),
                    mode:'lines',
                    line:{color:'rgba(240,238,235,.24)',width:1},
                    hoverinfo:'skip',
                    showlegend:false,
                });
            }
            events.forEach((ms,idx)=>{
                if(ms.label==='PO Created')return;
                const d=ms.date?new Date(ms.date):null;
                if(!d)return;
                const prev=idx>0?new Date(events[idx-1].date):(start||null);
                const delta=(prev&&d)?Math.round((d-prev)/MS_DAY):null;
                const toStart=(start&&d)?Math.round((d-start)/MS_DAY):null;
                traces.push({
                    x:[d],
                    y:[t.po_number+' ('+t.vendor_name+')'],
                    mode:'markers+text',
                    text:[idx>0&&delta!=null?('→ '+delta+'d'):''],
                    textposition:'top center',
                    textfont:{size:9,color:C.muted},
                    marker:{size:10,color:ms.label.includes('Deposit')?C.yellow:ms.label.includes('Final')?C.green:C.blue},
                    name:ms.label,
                    showlegend:false,
                    hovertemplate:'%{y}<br>'+ms.label+'<br>Date: '+fmtDateMMDDYYYY(ms.date)+'<br>Amount: '+fmt$(ms.amount||0)+'<br>Since PO: '+(toStart==null?'--':toStart+' days')+'<br>Since prior payment: '+(delta==null?'--':delta+' days')+'<extra></extra>',
                });
            });
        });
        const chartH=Math.max(500,allPOs.length*30);
        document.getElementById('chart-po-timeline').style.height=chartH+'px';
        document.getElementById('chart-po-timeline').style.overflowY='auto';
        Plotly.newPlot('chart-po-timeline',traces,{...PL,height:chartH,margin:{l:280,r:40,t:20,b:40},yaxis:{...PL.yaxis,autorange:'reversed'},xaxis:{...PL.xaxis,type:'date'}},{...PC,scrollZoom:true});
        document.getElementById('chart-po-timeline').on('plotly_click',function(ev){
            if(!ev.points||!ev.points.length)return;
            const label=ev.points[0].y||'';
            const po=label.split(' (')[0].trim();
            if(po)openDrill('PO: '+po,{po:po});
        });
    } else {
        document.getElementById('chart-po-timeline').innerHTML='<p style="color:var(--muted);padding:30px;text-align:center">No payment data yet. Run a full pipeline to pull payment details.</p>';
    }

    const paymentRows=[];
    withPay.forEach(t=>{
        const poDate=t.po_date?new Date(t.po_date):null;
        const events=(t.milestones||[])
            .filter(ms=>ms.label!=='PO Created'&&ms.date)
            .sort((a,b)=>new Date(a.date)-new Date(b.date));
        events.forEach((ms,idx)=>{
            const d=ms.date?new Date(ms.date):null;
            const prev=idx>0?new Date(events[idx-1].date):(poDate||null);
            const daysSincePO=(poDate&&d)?Math.round((d-poDate)/(24*60*60*1000)):null;
            const deltaPrev=(prev&&d)?Math.round((d-prev)/(24*60*60*1000)):null;
            const amt=Number(ms.amount||0);
            const poAmt=Number(t.total_amount||0);
            const finalConfirmed=Boolean(ms.is_final_confirmed)||Boolean(t.is_final_confirmed&&idx===events.length-1&&poAmt>0);
            paymentRows.push({
                po_number:t.po_number||'',
                vendor_name:t.vendor_name||'',
                po_amount:poAmt,
                payment_date:fmtDateMMDDYYYY(ms.date),
                payment_date_sort:d&&!Number.isNaN(d.getTime())?d.toISOString().slice(0,10):'',
                amount:amt,
                pct_paid:poAmt?((amt/poAmt)*100):0,
                days_since_po:daysSincePO,
                delta_prev:idx===0?null:deltaPrev,
                is_final:finalConfirmed,
                label:ms.label||'Payment',
            });
        });
    });
    paymentRows.sort((a,b)=>{
        if(a.po_number!==b.po_number)return String(a.po_number).localeCompare(String(b.po_number));
        return String(a.payment_date_sort).localeCompare(String(b.payment_date_sort));
    });
    if(paymentRows.length){
        let phtml='<table class="display" id="pm-raw-tbl" style="width:100%"><thead><tr><th>PO</th><th>Vendor</th><th>PO Amount</th><th>Payment Date</th><th>Amount</th><th>% of PO</th><th>Time Since PO</th><th>Δ vs Prior</th><th>Final?</th><th>Label</th></tr></thead><tbody>';
        paymentRows.forEach(r=>{
            phtml+=`<tr><td style="font-weight:700;color:var(--green)">${r.po_number}</td><td>${r.vendor_name}</td><td style="text-align:right" data-order="${r.po_amount}">${fmt$(r.po_amount)}</td><td data-order="${r.payment_date_sort||''}">${r.payment_date||'--'}</td><td style="text-align:right" data-order="${r.amount}">${fmt$(r.amount)}</td><td style="text-align:right" data-order="${r.pct_paid}">${r.pct_paid.toFixed(1)}%</td><td data-order="${r.days_since_po==null?-1:r.days_since_po}">${r.days_since_po==null?'--':r.days_since_po+' days'}</td><td data-order="${r.delta_prev==null?-1:r.delta_prev}">${r.delta_prev==null?'--':'→ '+r.delta_prev+' days'}</td><td>${r.is_final?'<span style="color:var(--green);font-weight:700">Yes</span>':'No'}</td><td>${r.label||''}</td></tr>`;
        });
        phtml+='</tbody></table>';
        document.getElementById('vendor-profile-table-wrap').innerHTML=phtml;
        if(dtI['pm-raw-tbl'])dtI['pm-raw-tbl'].destroy();
        dtI['pm-raw-tbl']=$('#pm-raw-tbl').DataTable(dtOpts({pageLength:25,order:[[0,'asc'],[3,'asc']]}));
    } else {
        document.getElementById('vendor-profile-table-wrap').innerHTML='<p style="color:var(--muted);padding:20px">No raw payment events available for the current line filter.</p>';
    }

    if(profiles.length){
        let vhtml='<table class="display" id="vp-tbl" style="width:100%"><thead><tr><th>Vendor</th><th>POs</th><th>Total Spend</th><th>Avg Cycle (days)</th><th>Avg Payments</th><th>Avg Deposit %</th></tr></thead><tbody>';
        profiles.forEach(p=>{vhtml+=`<tr><td style="font-weight:600">${p.vendor_name}</td><td>${p.po_count}</td><td style="text-align:right" data-order="${p.total_spend}">${fmt$(p.total_spend)}</td><td>${p.avg_cycle_days}</td><td>${p.avg_payment_count}</td><td>${p.avg_deposit_pct}%</td></tr>`;});
        vhtml+='</tbody></table>';
        document.getElementById('line-profile-table-wrap').innerHTML=vhtml;
        if(dtI['vp-tbl'])dtI['vp-tbl'].destroy();
        dtI['vp-tbl']=$('#vp-tbl').DataTable(dtOpts({pageLength:12,order:[[2,'desc']]}));
    } else {
        document.getElementById('line-profile-table-wrap').innerHTML='<p style="color:var(--muted);padding:20px">No vendor profiles for this filter.</p>';
    }
}

/* ====== V2: PAYMENT TEMPLATES ====== */
function openClassificationReviewFromSettings(){
    const nav=document.querySelector('.nav-item[onclick*="v2reviews"]');
    showPage('v2reviews',nav||null);
}
async function generateMilestoneTemplates(){
    if(!settingsAccess.can_edit_settings){
        showToast('You do not have permission to run AI milestone generation');
        return;
    }
    const btn=document.getElementById('btn-gen-milestones');
    const statusMain=document.getElementById('gen-milestone-status');
    const statusSettings=document.getElementById('gen-milestone-status-settings');
    const setStatus=(msg,color)=>{
        [statusMain,statusSettings].forEach(s=>{
            if(!s)return;
            s.style.display='block';
            s.textContent=msg;
            s.style.color=color;
        });
    };
    btn.disabled=true;btn.textContent='Generating...';
    setStatus('Running AI milestone generation for all $25K+ CAPEX POs. New POs will be appended only; existing templates are preserved.', 'var(--muted)');
    try{
        const res=await fetch('/api/v2/generate-milestones',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});
        const d=await res.json();
        if(d.error){setStatus('Error: '+d.error,'var(--red)');showToast('Generation failed');}
        else{
            const msg='Generated '+d.generated+' templates, saved '+d.saved+' new drafts';
            setStatus(msg,'var(--green)');
            showToast(msg);loadTemplates();
        }
    }catch(e){setStatus('Error: '+e.message,'var(--red)');}
    btn.disabled=false;btn.textContent='Generate AI Milestones (Append New Only)';
}
let tplMilestoneCount=0,tplPoData=[];
async function loadTemplates(){
    const res=await fetch('/api/v2/payment-templates');const d=await res.json();
    const templates=d.templates||[];
    document.getElementById('template-count').textContent='('+templates.length+' templates)';
    if(!templates.length){
        document.getElementById('template-table-wrap').innerHTML='<p style="color:var(--muted);padding:20px">No templates yet. Click "+ New Template" to define payment milestones for a PO.</p>';
        return;
    }
    window._tplCache=templates;
    let html='<table class="display" id="tpl-tbl" style="width:100%"><thead><tr><th>PO</th><th>Vendor</th><th>PO Total</th><th>Name</th><th>Milestones</th><th>Actions</th></tr></thead><tbody>';
    templates.forEach((t,idx)=>{
        const ms=t.milestones||[];
        const msList=(ms||[]).map(m=>{
            const d=(m.expected_date||m.date||'').slice(0,10);
            const s=m.status==='paid'?' <span style="color:var(--green);font-weight:700">[paid]</span>':'';
            return `<li style="margin:2px 0">${m.label||'Milestone'} - ${Number(m.pct||0).toFixed(0)}% - ${d||'--'}${s}</li>`;
        }).join('');
        const msHtml=msList?`<ul style="margin:0;padding-left:16px;font-size:11px;line-height:1.35">${msList}</ul>`:'--';
        const esc=s=>(s||'').replace(/'/g,"\\'");
        html+=`<tr><td style="font-weight:600;color:var(--green)">${t.po_number||'--'}</td><td>${t.vendor_name||'--'}</td><td style="text-align:right" data-order="${t.total_amount||0}">${fmt$(t.total_amount||0)}</td><td style="font-size:12px">${t.name||''}<br><span style="font-size:10px;color:var(--muted)">${t.source==='ai_generated'?'AI draft':'manual'}</span></td><td style="font-size:11px;max-width:420px">${msHtml}</td><td style="white-space:nowrap"><button onclick="editTemplate(${idx})" style="padding:4px 10px;background:var(--green);color:var(--accent-dark);border:none;border-radius:4px;font-size:11px;cursor:pointer;font-weight:700">Edit</button> <button onclick="deleteTemplate('${esc(t.template_id)}')" style="padding:4px 10px;background:var(--surface2);color:var(--red);border:none;border-radius:4px;font-size:11px;cursor:pointer">Delete</button></td></tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('template-table-wrap').innerHTML=html;
    if(dtI['tpl-tbl'])dtI['tpl-tbl'].destroy();
    dtI['tpl-tbl']=$('#tpl-tbl').DataTable(dtOpts({pageLength:10,order:[[2,'desc']],columnDefs:[{targets:2,type:'num'},{orderable:false,targets:5}]}));
}
async function showNewTemplateForm(){
    window._editingTemplateId=null;
    document.getElementById('template-editor').style.display='block';
    document.getElementById('template-editor-title').textContent='New Payment Template';
    document.getElementById('tpl-name').value='';
    document.getElementById('tpl-vendor').value='';
    document.getElementById('tpl-total').value='';
    document.getElementById('tpl-po').value='';
    document.getElementById('tpl-po-search').value='';
    tplMilestoneCount=0;
    document.getElementById('tpl-milestones').innerHTML='';
    updateTplTotals();
    if(!tplPoData.length){
        const res=await fetch('/api/v2/po-list');const d=await res.json();
        tplPoData=d.pos||[];
    }
    document.addEventListener('click',e=>{
        if(!e.target.closest('#tpl-po-search')&&!e.target.closest('#tpl-po-dropdown'))
            document.getElementById('tpl-po-dropdown').style.display='none';
    });
}
function filterPoList(){
    const q=document.getElementById('tpl-po-search').value.toLowerCase().trim();
    const dd=document.getElementById('tpl-po-dropdown');
    if(!q||q.length<2){dd.innerHTML='<div style="padding:12px;color:var(--muted);font-size:12px">Type at least 2 characters...</div>';dd.style.display='block';return;}
    const matches=tplPoData.filter(p=>{
        const haystack=(p.po_number+' '+p.vendor_name+' '+(p.date_order||'')+' '+(p.station_id||'')+' '+(p.created_by_name||'')+' '+(p.project_name||'')).toLowerCase();
        return haystack.includes(q);
    }).slice(0,30);
    if(!matches.length){dd.innerHTML='<div style="padding:12px;color:var(--muted);font-size:12px">No POs matching "'+q+'"</div>';dd.style.display='block';return;}
    let html='';
    matches.forEach(p=>{
        const amt=Math.round(p.total_amount||0).toLocaleString();
        html+=`<div onclick="selectPo('${p.po_number}','${(p.vendor_name||'').replace(/'/g,"\\'")}',${p.total_amount||0})" style="padding:8px 12px;cursor:pointer;border-bottom:1px solid var(--border);font-size:12px;display:flex;justify-content:space-between;align-items:center" onmouseenter="this.style.background='var(--surface2)'" onmouseleave="this.style.background=''">
            <div><span style="font-weight:700;color:var(--green)">${p.po_number}</span> <span style="color:var(--muted)">|</span> ${(p.vendor_name||'').slice(0,30)}<br><span style="font-size:10px;color:var(--muted)">${(p.date_order||'').slice(0,10)} &bull; ${p.created_by_name||'?'} &bull; ${p.station_id||p.project_name||'no project'}</span></div>
            <div style="font-weight:700;white-space:nowrap">$${amt}</div>
        </div>`;
    });
    if(tplPoData.filter(p=>(p.po_number+' '+p.vendor_name).toLowerCase().includes(q)).length>30)
        html+='<div style="padding:8px 12px;color:var(--muted);font-size:11px;text-align:center">Showing first 30 of more results...</div>';
    dd.innerHTML=html;dd.style.display='block';
}
function selectPo(poNum,vendor,total){
    document.getElementById('tpl-po').value=poNum;
    document.getElementById('tpl-po-search').value=poNum+' - '+vendor;
    document.getElementById('tpl-po-dropdown').style.display='none';
    document.getElementById('tpl-vendor').value=vendor;
    document.getElementById('tpl-total').value='$'+Math.round(total).toLocaleString();
    document.getElementById('tpl-name').value=poNum+' milestones';
    tplMilestoneCount=0;
    document.getElementById('tpl-milestones').innerHTML='';
    addMilestoneRow('Deposit','',30);
    addMilestoneRow('Progress','',40);
    addMilestoneRow('Final','',30);
}
function hideTemplateEditor(){document.getElementById('template-editor').style.display='none';}
function addMilestoneRow(label,date,pct){
    tplMilestoneCount++;
    const tr=document.createElement('tr');
    tr.style.borderBottom='1px solid rgba(62,61,58,.3)';
    tr.innerHTML=`<td style="padding:6px 8px"><input class="forecast-input" value="${label||''}" placeholder="e.g. Deposit, Delivery, Final" style="width:100%;font-size:13px"></td>
        <td style="padding:6px 8px"><input class="forecast-input" type="date" value="${date||''}" style="width:100%;font-size:13px"></td>
        <td style="padding:6px 8px"><input class="forecast-input" type="number" value="${pct||0}" step="5" min="0" max="100" placeholder="%" style="width:100%;text-align:right;font-size:13px" oninput="updateTplTotals()"></td>
        <td style="padding:6px 8px;text-align:right"><span class="ms-amt" style="font-size:13px;color:var(--green);font-weight:600">$0</span></td>
        <td style="padding:6px 4px;text-align:center"><button onclick="this.closest('tr').remove();updateTplTotals()" style="background:none;color:var(--red);border:none;cursor:pointer;font-size:14px;font-weight:700;padding:2px 6px" title="Remove">&times;</button></td>`;
    document.getElementById('tpl-milestones').appendChild(tr);
    updateTplTotals();
}
function updateTplTotals(){
    const totalStr=(document.getElementById('tpl-total').value||'').replace(/[$,]/g,'');
    const poTotal=parseFloat(totalStr)||0;
    let pctSum=0;
    document.querySelectorAll('#tpl-milestones > tr').forEach(tr=>{
        const inputs=tr.querySelectorAll('input');
        const pct=parseFloat(inputs[2]?.value)||0;
        const amt=poTotal*pct/100;
        pctSum+=pct;
        const amtSpan=tr.querySelector('.ms-amt');
        if(amtSpan)amtSpan.textContent=fmt$(amt);
    });
    document.getElementById('tpl-pct-total').textContent=pctSum.toFixed(0);
    document.getElementById('tpl-pct-total').style.color=Math.abs(pctSum-100)<1?'var(--green)':'var(--red)';
    document.getElementById('tpl-amt-total').textContent=fmt$(poTotal*pctSum/100);
}
async function saveTemplate(){
    const poNum=document.getElementById('tpl-po').value;
    if(!poNum){showToast('Please select a PO first');return;}
    const totalStr=(document.getElementById('tpl-total').value||'').replace(/[$,]/g,'');
    const poTotal=parseFloat(totalStr)||0;
    const milestones=[];
    document.querySelectorAll('#tpl-milestones > tr').forEach(tr=>{
        const inputs=tr.querySelectorAll('input');
        if(inputs.length>=3&&inputs[0].value.trim()){
            const pct=parseFloat(inputs[2].value)||0;
            milestones.push({label:inputs[0].value.trim(),date:inputs[1].value||'',pct:pct,amount:Math.round(poTotal*pct/100*100)/100});
        }
    });
    const totalPct=milestones.reduce((s,m)=>s+m.pct,0);
    if(Math.abs(totalPct-100)>1){showToast('Milestone percentages must sum to ~100% (currently '+totalPct.toFixed(0)+'%)');return;}
    const body={po_number:poNum,vendor_name:document.getElementById('tpl-vendor').value,total_amount:poTotal,name:document.getElementById('tpl-name').value.trim()||poNum+' milestones',milestones:milestones};
    if(window._editingTemplateId)body.template_id=window._editingTemplateId;
    const res=await fetch('/api/v2/payment-templates',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d=await res.json();
    if(d.status==='ok'){
        document.getElementById('tpl-saved').style.display='inline';
        setTimeout(()=>{document.getElementById('tpl-saved').style.display='none';},2500);
        window._editingTemplateId=null;
        showToast('Template saved for '+poNum);hideTemplateEditor();loadTemplates();
    } else {showToast('Error: '+(d.error||'unknown'));}
}
function editTemplate(idx){
    const t=window._tplCache[idx];
    if(!t)return;
    document.getElementById('template-editor').style.display='block';
    document.getElementById('template-editor-title').textContent='Edit: '+t.po_number+' - '+(t.vendor_name||'').slice(0,25);
    window._editingTemplateId=t.template_id;
    document.getElementById('tpl-po').value=t.po_number||'';
    document.getElementById('tpl-po-search').value=(t.po_number||'')+' - '+(t.vendor_name||'');
    document.getElementById('tpl-vendor').value=t.vendor_name||'';
    document.getElementById('tpl-total').value='$'+Math.round(t.total_amount||0).toLocaleString();
    document.getElementById('tpl-name').value=t.name||'';
    tplMilestoneCount=0;
    document.getElementById('tpl-milestones').innerHTML='';
    (t.milestones||[]).forEach(ms=>{
        addMilestoneRow(ms.label,ms.expected_date||ms.date||'',ms.pct||0);
    });
    updateTplTotals();
    document.getElementById('template-editor').scrollIntoView({behavior:'smooth'});
}
async function deleteTemplate(templateId){
    if(!confirm('Delete this template?'))return;
    const res=await fetch('/api/v2/payment-templates/'+encodeURIComponent(templateId),{method:'DELETE'});
    const d=await res.json();
    if(d.status==='ok'){showToast('Template deleted');loadTemplates();}
    else{showToast('Error: '+(d.error||'unknown'));}
}

/* ====== V2: CASHFLOW PROJECTION ====== */
async function loadCashflow(){
    const res=await fetch(apiUrl('/api/v2/cashflow'));const d=await res.json();
    const monthly=d.monthly||[];const cumul=d.cumulative||[];const weekly=d.weekly||[];
    const gran=(document.getElementById('cf-granularity')?.value||'month');

    document.getElementById('cashflow-kpis').innerHTML=`
        <div class="kpi"><div class="label">Total Rows</div><div class="value">${d.total_rows||0}</div></div>
        <div class="kpi"><div class="label">Actual Payments</div><div class="value">${d.actuals||0}</div></div>
        <div class="kpi"><div class="label">Projected</div><div class="value">${d.projected||0}</div></div>
        <div class="kpi"><div class="label">Line Filter</div><div class="value">${activeModules.size===allModules.length?'All':activeModules.size}</div></div>`;

    if(!monthly.length){
        document.getElementById('chart-cf-monthly').innerHTML='<p style="color:var(--muted);padding:30px;text-align:center">No cashflow data. Run a full pipeline and create payment templates.</p>';
        document.getElementById('chart-cf-cumulative').innerHTML='';
        document.getElementById('cf-weekly-table-wrap').innerHTML='';
        return;
    }

    let labels=[],actuals=[],projected=[],mainTitle='Monthly Cash Outflow';
    if(gran==='workweek'){
        const wk=(weekly||[]).slice().sort((a,b)=>String(a.year_week||'').localeCompare(String(b.year_week||'')));
        labels=wk.map(w=>w.year_week||'');
        actuals=wk.map(w=>(w.items||[]).reduce((s,i)=>s+((i.source==='historical')?(i.amount||0):0),0));
        projected=wk.map(w=>(w.items||[]).reduce((s,i)=>s+((i.source==='projected')?(i.amount||0):0),0));
        mainTitle='Workweek Cash Outflow';
    }else if(gran==='quarter'){
        const qMap={};
        (monthly||[]).forEach(m=>{
            const mth=String(m.month||'');
            const y=mth.slice(0,4);
            const mm=parseInt(mth.slice(5,7),10);
            const q=(Number.isFinite(mm)&&mm>=1&&mm<=12)?Math.floor((mm-1)/3)+1:1;
            const key=`${y}-Q${q}`;
            if(!qMap[key])qMap[key]={actual:0,projected:0};
            qMap[key].actual+=(m.actual||0);
            qMap[key].projected+=(m.projected||0);
        });
        labels=Object.keys(qMap).sort();
        actuals=labels.map(k=>qMap[k].actual||0);
        projected=labels.map(k=>qMap[k].projected||0);
        mainTitle='Quarterly Cash Outflow';
    }else{
        labels=monthly.map(m=>m.month);
        actuals=monthly.map(m=>m.actual);
        projected=monthly.map(m=>m.projected);
    }
    document.getElementById('cf-main-title').textContent=mainTitle;

    Plotly.newPlot('chart-cf-monthly',[
        {x:labels,y:actuals,type:'bar',name:'Actual',marker:{color:C.green}},
        {x:labels,y:projected,type:'bar',name:'Projected',marker:{color:C.blue,opacity:0.6}},
    ],{...PL,barmode:'stack',height:350,margin:{l:70,r:20,t:20,b:60},yaxis:{...PL.yaxis,tickprefix:'$',tickformat:',.0s'},xaxis:{...PL.xaxis,tickangle:-30},legend:{x:0,y:1.1,orientation:'h',font:{size:11}}},PC);
    document.getElementById('chart-cf-monthly').on('plotly_click',function(ev){
        if(!ev.points||!ev.points.length)return;
        if(gran!=='month'){showToast('Drill-down is available in Month view');return;}
        openCashflowDrill(ev.points[0].x,'all');
    });

    const cMonths=[...labels];
    const cActual=[];const cTotal=[];let runA=0;let runT=0;
    for(let i=0;i<labels.length;i++){runA+=actuals[i]||0;runT+=(actuals[i]||0)+(projected[i]||0);cActual.push(runA);cTotal.push(runT);}
    Plotly.newPlot('chart-cf-cumulative',[
        {x:cMonths,y:cActual,mode:'lines+markers',name:'Cumulative Actual',line:{color:C.green,width:2},marker:{size:6}},
        {x:cMonths,y:cTotal,mode:'lines+markers',name:'Cumulative Total',line:{color:C.blue,width:2,dash:'dot'},marker:{size:6}},
    ],{...PL,height:350,margin:{l:70,r:20,t:20,b:60},yaxis:{...PL.yaxis,tickprefix:'$',tickformat:',.0s'},xaxis:{...PL.xaxis,tickangle:-30},legend:{x:0,y:1.1,orientation:'h',font:{size:11}}},PC);
    document.getElementById('chart-cf-cumulative').on('plotly_click',function(ev){
        if(!ev.points||!ev.points.length)return;
        if(gran!=='month'){showToast('Drill-down is available in Month view');return;}
        openCashflowDrill(ev.points[0].x,'all');
    });

    if(weekly.length){
        const rows=[];
        weekly.forEach(w=>{
            (w.items||[]).forEach(i=>{
                rows.push({
                    week:w.year_week||'',
                    po:i.po_number||'',
                    vendor:i.vendor_name||'',
                    line:i.line||'',
                    milestone:i.milestone||'',
                    date:(i.expected_date||'').slice(0,10),
                    amount:parseFloat(i.amount||0),
                    source:i.source||'',
                });
            });
        });
        let whtml='<table class="display" id="cf-weekly-tbl" style="width:100%"><thead><tr><th>Week</th><th>PO</th><th>Vendor</th><th>Line</th><th>Milestone</th><th>Date</th><th>Amount</th><th>Type</th></tr></thead><tbody>';
        rows.forEach(r=>{
            const type=r.source==='historical'?'Actual':'Projected';
            whtml+=`<tr><td>${r.week}</td><td style="font-weight:700;color:var(--green)">${r.po}</td><td>${r.vendor}</td><td>${r.line||'--'}</td><td>${r.milestone}</td><td>${r.date||'--'}</td><td style="text-align:right" data-order="${r.amount}">${fmt$(r.amount)}</td><td>${type}</td></tr>`;
        });
        whtml+='</tbody></table>';
        document.getElementById('cf-weekly-table-wrap').innerHTML=whtml;
        if(dtI['cf-weekly-tbl'])dtI['cf-weekly-tbl'].destroy();
        dtI['cf-weekly-tbl']=$('#cf-weekly-tbl').DataTable(dtOpts({pageLength:25,order:[[0,'desc'],[6,'desc']]}));
    } else {
        document.getElementById('cf-weekly-table-wrap').innerHTML='<p style="color:var(--muted);padding:20px">No cashflow ledger rows for this line filter.</p>';
    }
}

/* ====== CASHFLOW DRILL-DOWN ====== */
async function openCashflowDrill(month,source){
    const res=await fetch(apiUrl('/api/v2/cashflow-drilldown?month='+encodeURIComponent(month)+'&source='+encodeURIComponent(source)));
    const d=await res.json();
    const items=d.items||[];
    const title=month+(source==='all'?'':' ('+source+')');
    const sub=items.length+' items, '+fmt$(items.reduce((s,i)=>s+(i.expected_amount||0),0));
    document.getElementById('drill-title').textContent='Cashflow: '+title;
    document.getElementById('drill-sub').textContent=sub;
    if(!items.length){
        document.getElementById('drill-body').innerHTML='<p style="color:var(--muted);padding:20px">No items for this month.</p>';
    } else {
        let html='<table class="display" id="cf-drill-tbl" style="width:100%"><thead><tr><th>PO</th><th>Vendor</th><th>Milestone</th><th>Date</th><th>Amount</th><th>Status</th></tr></thead><tbody>';
        items.forEach(i=>{
            let st='<span style="color:var(--blue)">Projected</span>';
            if(i.source==='historical'){
                if(i.record_type==='actual') st='<span style="color:var(--green)">Actual Paid</span>';
                else if(i.record_type==='template') st='<span style="color:#B9770E">Template Paid</span>';
                else st='<span style="color:var(--green)">Paid</span>';
            }
            html+=`<tr><td style="font-weight:600">${i.po_number||''}</td><td>${(i.vendor_name||'').slice(0,30)}</td><td>${i.milestone_label||''}</td><td>${(i.expected_date||'').slice(0,10)}</td><td style="text-align:right" data-order="${i.expected_amount||0}">${fmt$(i.expected_amount||0)}</td><td>${st}</td></tr>`;
        });
        html+='</tbody></table>';
        document.getElementById('drill-body').innerHTML=html;
        if(dtI['cf-drill-tbl'])try{dtI['cf-drill-tbl'].destroy();}catch(e){}
        dtI['cf-drill-tbl']=$('#cf-drill-tbl').DataTable(dtOpts({pageLength:25,order:[[4,'desc']]}));
    }
    document.getElementById('drill-overlay').style.display='block';
    document.getElementById('drill-panel').style.display='flex';
}

/* ====== DATA MANAGEMENT ====== */
async function refreshFromOdoo(){
    if(!settingsAccess.can_edit_settings){
        showToast('You do not have permission to run refresh');
        return;
    }
    const btn=document.getElementById('btn-refresh-odoo');
    const status=document.getElementById('refresh-status');
    btn.disabled=true;btn.textContent='Refreshing...';
    status.style.display='block';status.textContent='Running incremental pipeline...';status.style.color='var(--muted)';
    try{
        const res=await fetch('/api/v2/refresh-data',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});
        const d=await res.json();
        if(d.error){status.textContent='Error: '+d.error;status.style.color='var(--red)';showToast('Refresh failed');}
        else{
            const msg=`Done: ${d.new||0} new, ${d.updated||0} updated, ${d.removed||0} removed`;
            status.textContent=msg;status.style.color='var(--green)';
            showToast(msg);
        }
    }catch(e){status.textContent='Error: '+e.message;status.style.color='var(--red)';}
    btn.disabled=false;btn.textContent='Refresh Data';
}
async function uploadRampCsv(){
    if(!settingsAccess.can_edit_settings){
        showToast('You do not have permission to upload Ramp CSV');
        return;
    }
    const fileInput=document.getElementById('ramp-csv-file');
    const btn=document.getElementById('btn-upload-ramp');
    const status=document.getElementById('upload-status');
    if(!fileInput.files||!fileInput.files[0]){showToast('Select a CSV file first');return;}
    const file=fileInput.files[0];
    if(!file.name.toLowerCase().endsWith('.csv')){showToast('File must be a .csv');return;}
    btn.disabled=true;btn.textContent='Uploading...';
    status.style.display='block';status.textContent='Processing '+file.name+'...';status.style.color='var(--muted)';
    try{
        const form=new FormData();form.append('file',file);
        const res=await fetch('/api/v2/upload-ramp-csv',{method:'POST',body:form});
        const d=await res.json();
        if(d.error){status.textContent='Error: '+d.error;status.style.color='var(--red)';showToast('Upload failed: '+d.error);}
        else{
            const msg=`Appended ${d.new_rows||0} new transactions (${d.skipped||0} duplicates skipped)`;
            status.textContent=msg;status.style.color='var(--green)';
            showToast(msg);fileInput.value='';
        }
    }catch(e){status.textContent='Error: '+e.message;status.style.color='var(--red)';}
    btn.disabled=false;btn.textContent='Upload & Append';
}

/* ====== AI RFQ GEN ====== */
let airfqState={revisionContext:null,csvText:'',csvFilename:'rfq_ai_generated.csv',lookupValues:{},lastValidation:null,lastPreview:null,historyItems:[],progressTimer:null,progressStartedAt:0,progressPhase:''};
function setAirRfqStatus(msg,color){
    const el=document.getElementById('airfq-status');
    if(!el)return;
    el.textContent=msg||'';
    el.style.color=color||'var(--muted)';
}
function stopAirRfqProgress(){
    if(airfqState.progressTimer){
        clearInterval(airfqState.progressTimer);
        airfqState.progressTimer=null;
    }
    airfqState.progressStartedAt=0;
    airfqState.progressPhase='';
}
function startAirRfqProgress(phase){
    stopAirRfqProgress();
    airfqState.progressPhase=phase||'Generating';
    airfqState.progressStartedAt=Date.now();
    const tick=()=>{
        const elapsed=Math.max(0,Math.floor((Date.now()-airfqState.progressStartedAt)/1000));
        const p=airfqState.progressPhase||'Generating';
        setAirRfqStatus(`${p} RFQ... ${elapsed}s elapsed (typical 15-60s depending on PDF and model).`,'var(--muted)');
    };
    tick();
    airfqState.progressTimer=setInterval(tick,1000);
}
function csvEscape(v){
    const s=String(v==null?'':v);
    if(/[",\n]/.test(s))return '"'+s.replace(/"/g,'""')+'"';
    return s;
}
function buildAirRfqCsvFromDraft(draft){
    if(!draft||!draft.header||!Array.isArray(draft.lines))return '';
    const headers=['External ID','Vendor','Vendor Reference','Order Deadline','Expected Arrival','Ask confirmation','Deliver To','Project','Terms and Conditions','Order Lines / Product','Order Lines / Display Type','Order Lines / Description','Order Lines / Project','Order Lines / Quantity','Order Lines / Unit of Measure','Order Lines / Unit Price','Order Lines / Taxes'];
    const h=draft.header||{};
    const externalId=(h.external_id||('rfq_ai_'+Math.random().toString(16).slice(2,10)));
    const out=[headers.join(',')];
    draft.lines.forEach(l=>{
        const isNote=((l.display_type||'').toLowerCase()==='line_note')||String(l.description||'').toLowerCase().startsWith('payment:');
        const taxes=Array.isArray(l.taxes)?l.taxes.filter(Boolean).join(','):(l.taxes||'');
        const row=[
            externalId,
            h.vendor||'',
            h.vendor_reference||'',
            h.order_deadline||'',
            h.expected_arrival||'',
            Number(h.ask_confirmation?1:0),
            h.deliver_to||'',
            h.project||'',
            h.terms_and_conditions||'',
            isNote?'':(l.product||''),
            isNote?'line_note':'',
            l.description||'',
            isNote?'':(l.project||''),
            isNote?'':Number(l.quantity||0),
            isNote?'':(l.uom||'Unit'),
            isNote?'':Number(l.unit_price||0),
            isNote?'':taxes,
        ];
        out.push(row.map(csvEscape).join(','));
    });
    return out.join('\n')+'\n';
}
function taxRateFromLabel(label){
    const m=String(label||'').match(/(\d+(?:\.\d+)?)\s*%/);
    if(!m)return 0;
    return (parseFloat(m[1])||0)/100;
}
function buildAirRfqPreviewFromDraft(draft){
    const h=draft?.header||{};
    const lines=Array.isArray(draft?.lines)?draft.lines:[];
    let untaxed=0,tax=0;
    const pvLines=lines.map((l,idx)=>{
        const isNote=((l.display_type||'').toLowerCase()==='line_note')||String(l.description||'').toLowerCase().startsWith('payment:');
        const qty=Number(l.quantity||0);
        const unit=Number(l.unit_price||0);
        const subtotal=isNote?0:(qty*unit);
        const taxLabel=Array.isArray(l.taxes)&&l.taxes.length?(l.taxes[0]||''):(l.taxes||'');
        const rate=isNote?0:taxRateFromLabel(taxLabel);
        const taxAmt=isNote?0:(subtotal*rate);
        untaxed+=subtotal;tax+=taxAmt;
        return{
            line_no:idx+1,product:l.product||'',description:l.description||'',project:l.project||'',
            display_type:isNote?'line_note':'',
            quantity:qty,uom:l.uom||'Unit',unit_price:unit,tax_label:taxLabel,tax_rate:rate,
            line_subtotal:subtotal,line_tax:taxAmt,line_total:subtotal+taxAmt,
        };
    });
    return{
        header:{
            vendor:h.vendor||'',vendor_reference:h.vendor_reference||'',order_deadline:h.order_deadline||'',
            expected_arrival:h.expected_arrival||'',ask_confirmation:Number(h.ask_confirmation?1:0),
            deliver_to:h.deliver_to||'',project:h.project||'',terms_and_conditions:h.terms_and_conditions||'',
        },
        lines:pvLines,
        totals:{untaxed_amount:untaxed,tax_amount:tax,total_amount:untaxed+tax},
    };
}
function populateAirRfqLookupInputs(values){
    const v=(values&&typeof values==='object')?values:{};
    const vendors=Array.isArray(v.vendors)?v.vendors:[];
    const projects=Array.isArray(v.projects)?v.projects:[];
    const taxes=Array.isArray(v.taxes)?v.taxes:[];
    const deliver=Array.isArray(v.deliver_to)?v.deliver_to:[];
    const VENDOR_LIST_CAP=5000;
    const PROJECT_LIST_CAP=20000;
    const TAX_LIST_CAP=2000;
    const DELIVER_LIST_CAP=1000;
    const vendorList=document.getElementById('airfq-vendors-list');
    if(vendorList)vendorList.innerHTML=vendors.slice(0,VENDOR_LIST_CAP).map(x=>`<option value="${htmlEsc(x)}"></option>`).join('');
    const projList=document.getElementById('airfq-projects-list');
    if(projList)projList.innerHTML=projects.slice(0,PROJECT_LIST_CAP).map(x=>`<option value="${htmlEsc(x)}"></option>`).join('');
    const taxList=document.getElementById('airfq-taxes-list');
    if(taxList)taxList.innerHTML=taxes.slice(0,TAX_LIST_CAP).map(x=>`<option value="${htmlEsc(x)}"></option>`).join('');
    const delList=document.getElementById('airfq-deliver-list');
    if(delList)delList.innerHTML=deliver.slice(0,DELIVER_LIST_CAP).map(x=>`<option value="${htmlEsc(x)}"></option>`).join('');
}
async function loadAirRfqHistory(){
    try{
        const res=await fetch('/api/v2/ai-rfq/history');
        const d=await res.json();
        const items=Array.isArray(d.items)?d.items:[];
        airfqState.historyItems=items;
        const sel=document.getElementById('airfq-history-select');
        if(!sel)return;
        let html='<option value="">Select previous RFQ...</option>';
        items.slice(0,200).forEach(item=>{
            const ts=(item.created_at||'').replace('T',' ').slice(0,19);
            const vendor=item.vendor||'';
            const csv=item.csv_filename||'';
            const err=Number(item.blocking_error_count||0);
            html+=`<option value="${htmlEsc(item.id||'')}">${htmlEsc(ts)} | ${htmlEsc(vendor)} | ${htmlEsc(csv)}${err>0?' | ERR':''}</option>`;
        });
        sel.innerHTML=html;
    }catch(_e){
        // non-blocking
    }
}
async function loadSelectedAirRfqHistory(){
    const sel=document.getElementById('airfq-history-select');
    const id=(sel?.value||'').trim();
    if(!id){showToast('Select a saved RFQ first');return;}
    try{
        const res=await fetch('/api/v2/ai-rfq/history/'+encodeURIComponent(id));
        const d=await res.json();
        if(d.error||!d.entry){showToast('Unable to load selected RFQ');return;}
        const e=d.entry||{};
        const vendor=(e.vendor||'').trim();
        const prompt=(e.prompt||'').trim();
        const paymentNote=(e.payment_milestones_note||'').trim();
        const refPo=(e.meta&&e.meta.reference_po?String(e.meta.reference_po):'').trim();
        const rev=e.revision_context||{};
        const revObj=(rev&&typeof rev==='object')?rev:{};
        if(!revObj.last_draft&&e.draft&&typeof e.draft==='object')revObj.last_draft=e.draft;
        airfqState.revisionContext=Object.keys(revObj).length?revObj:null;
        airfqState.csvText=e.csv_text||'';
        airfqState.csvFilename=e.csv_filename||'rfq_ai_generated.csv';
        airfqState.lastValidation=e.validation||{};
        airfqState.lastPreview=e.preview||{};
        const vIn=document.getElementById('airfq-vendor');if(vIn)vIn.value=vendor;
        const pIn=document.getElementById('airfq-prompt');if(pIn)pIn.value=prompt;
        const pm=document.getElementById('airfq-payment-note');if(pm)pm.value=paymentNote;
        const rIn=document.getElementById('airfq-reference-po');if(rIn)rIn.value=refPo;
        renderAirRfqValidation(airfqState.lastValidation||{});
        refreshAirRfqDerivedViews();
        setAirRfqStatus('Loaded saved RFQ from history.','var(--green)');
    }catch(e){
        setAirRfqStatus('History load error: '+e.message,'var(--red)');
    }
}
function syncAirRfqHeaderPickers(draft){
    const h=draft?.header||{};
    const lines=Array.isArray(draft?.lines)?draft.lines:[];
    const firstTax=(lines[0]&&Array.isArray(lines[0].taxes)&&lines[0].taxes.length)?lines[0].taxes[0]:'';
    const p=document.getElementById('airfq-header-project');if(p)p.value=h.project||'';
    const d=document.getElementById('airfq-header-deliver');if(d)d.value=h.deliver_to||'';
    const t=document.getElementById('airfq-default-tax');if(t)t.value=firstTax||'';
}
function renderAirRfqLineEditor(draft){
    const wrap=document.getElementById('airfq-line-editor');
    if(!wrap)return;
    const lines=Array.isArray(draft?.lines)?draft.lines:[];
    if(!lines.length){
        wrap.innerHTML='<p style="color:var(--muted);padding:12px">No lines available.</p>';
        return;
    }
    let html='<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse" id="airfq-line-editor-tbl"><thead><tr style="border-bottom:1px solid var(--border)"><th style="padding:6px 4px;color:var(--muted);font-size:10px">#</th><th style="padding:6px 4px;color:var(--muted);font-size:10px">Type</th><th style="padding:6px 4px;color:var(--muted);font-size:10px">Product</th><th style="padding:6px 4px;color:var(--muted);font-size:10px">Description</th><th style="padding:6px 4px;color:var(--muted);font-size:10px">Project</th><th style="padding:6px 4px;color:var(--muted);font-size:10px">Qty</th><th style="padding:6px 4px;color:var(--muted);font-size:10px">UoM</th><th style="padding:6px 4px;color:var(--muted);font-size:10px">Unit Price</th><th style="padding:6px 4px;color:var(--muted);font-size:10px">Tax</th><th style="padding:6px 4px;color:var(--muted);font-size:10px"></th></tr></thead><tbody>';
    lines.forEach((l,idx)=>{
        const isNote=((l.display_type||'').toLowerCase()==='line_note')||String(l.description||'').toLowerCase().startsWith('payment:');
        const tax=Array.isArray(l.taxes)&&l.taxes.length?l.taxes[0]:'';
        html+=`<tr data-line-idx="${idx}" data-display-type="${isNote?'line_note':''}" style="border-bottom:1px solid rgba(62,61,58,.3)">
            <td style="padding:6px 4px">${idx+1}</td>
            <td style="padding:6px 4px;color:${isNote?'var(--yellow)':'var(--muted)'}">${isNote?'Note':'Spend'}</td>
            <td style="padding:6px 4px"><input class="forecast-input airfq-li-product" value="${htmlEsc(l.product||'')}" style="width:220px" ${isNote?'disabled':''}></td>
            <td style="padding:6px 4px"><input class="forecast-input airfq-li-desc" value="${htmlEsc(l.description||'')}" style="width:260px"></td>
            <td style="padding:6px 4px"><input class="forecast-input airfq-li-project" list="airfq-projects-list" value="${htmlEsc(l.project||'')}" style="width:260px" ${isNote?'disabled':''}></td>
            <td style="padding:6px 4px"><input class="forecast-input airfq-li-qty" type="number" step="0.01" min="0" value="${Number(l.quantity||0)}" style="width:90px;text-align:right" ${isNote?'disabled':''}></td>
            <td style="padding:6px 4px"><input class="forecast-input airfq-li-uom" value="${htmlEsc(l.uom||'Unit')}" style="width:90px" ${isNote?'disabled':''}></td>
            <td style="padding:6px 4px"><input class="forecast-input airfq-li-price" type="number" step="0.01" min="0" value="${Number(l.unit_price||0)}" style="width:110px;text-align:right" ${isNote?'disabled':''}></td>
            <td style="padding:6px 4px"><input class="forecast-input airfq-li-tax" list="airfq-taxes-list" value="${htmlEsc(tax||'')}" style="width:220px" ${isNote?'disabled':''}></td>
            <td style="padding:6px 4px"><button style="padding:4px 8px;background:var(--surface2);color:var(--red);border:1px solid var(--border);border-radius:4px;cursor:pointer" onclick="removeAirRfqLine(${idx})">Remove</button></td>
        </tr>`;
    });
    html+='</tbody></table></div>';
    html+='<div style="margin-top:8px"><button class="btn-refresh" style="width:auto;padding:6px 12px;background:var(--surface2);color:var(--text)" onclick="addAirRfqLine()">+ Add Line</button></div>';
    wrap.innerHTML=html;
}
function refreshAirRfqDerivedViews(){
    const draft=airfqState.revisionContext?.last_draft;
    if(!draft)return;
    airfqState.csvText=buildAirRfqCsvFromDraft(draft);
    airfqState.lastPreview=buildAirRfqPreviewFromDraft(draft);
    document.getElementById('airfq-csv').value=airfqState.csvText||'';
    renderAirRfqPreview(airfqState.lastPreview||{});
    renderAirRfqLineEditor(draft);
    syncAirRfqHeaderPickers(draft);
    const btnDownload=document.getElementById('btn-airfq-download');
    if(btnDownload)btnDownload.disabled=!airfqState.csvText;
}
function applyAirRfqHeaderEdits(){
    const draft=airfqState.revisionContext?.last_draft;
    if(!draft||!draft.header){showToast('Generate RFQ first');return;}
    const project=(document.getElementById('airfq-header-project').value||'').trim();
    const deliver=(document.getElementById('airfq-header-deliver').value||'').trim();
    const defaultTax=(document.getElementById('airfq-default-tax').value||'').trim();
    if(project)draft.header.project=project;
    if(deliver)draft.header.deliver_to=deliver;
    if(Array.isArray(draft.lines)){
        draft.lines=draft.lines.map(l=>{
            const next={...l};
            const isNote=((next.display_type||'').toLowerCase()==='line_note')||String(next.description||'').toLowerCase().startsWith('payment:');
            if(!isNote&&!next.project)next.project=draft.header.project||'';
            if(!isNote&&defaultTax&&(!Array.isArray(next.taxes)||!next.taxes.length))next.taxes=[defaultTax];
            return next;
        });
    }
    refreshAirRfqDerivedViews();
    setAirRfqStatus('Applied header edits locally. Click Regenerate to re-run AI with context if needed.','var(--green)');
}
function applyAirRfqLineEdits(){
    const draft=airfqState.revisionContext?.last_draft;
    if(!draft){showToast('Generate RFQ first');return;}
    const rows=[...document.querySelectorAll('#airfq-line-editor-tbl tbody tr[data-line-idx]')];
    if(!rows.length){showToast('No editable lines found');return;}
    const lines=[];
    rows.forEach(r=>{
        const isNote=((r.getAttribute('data-display-type')||'').toLowerCase()==='line_note');
        const product=(r.querySelector('.airfq-li-product')?.value||'').trim();
        const desc=(r.querySelector('.airfq-li-desc')?.value||'').trim();
        const project=(r.querySelector('.airfq-li-project')?.value||'').trim()||(draft.header?.project||'');
        const qty=parseFloat((r.querySelector('.airfq-li-qty')?.value||'0'))||0;
        const uom=(r.querySelector('.airfq-li-uom')?.value||'Unit').trim()||'Unit';
        const price=parseFloat((r.querySelector('.airfq-li-price')?.value||'0'))||0;
        const tax=(r.querySelector('.airfq-li-tax')?.value||'').trim();
        if(!product&&!desc)return;
        if(isNote){
            lines.push({product:'',description:desc||'Payment: note',project:'',display_type:'line_note',quantity:0,uom:'',unit_price:0,taxes:[]});
            return;
        }
        lines.push({product:product||'Non-Inventory: Construction in Process',description:desc||'RFQ line',project,display_type:'',quantity:qty>0?qty:1,uom,unit_price:price>=0?price:0,taxes:tax?[tax]:[]});
    });
    if(!lines.length){showToast('At least one line is required');return;}
    draft.lines=lines;
    refreshAirRfqDerivedViews();
    setAirRfqStatus('Applied line edits locally. You can download CSV or click Regenerate for AI refinement.','var(--green)');
}
function applyAirRfqBulkEdits(){
    const draft=airfqState.revisionContext?.last_draft;
    if(!draft||!Array.isArray(draft.lines)||!draft.lines.length){showToast('Generate RFQ first');return;}
    const bulkProduct=(document.getElementById('airfq-bulk-product')?.value||'').trim();
    const bulkProject=(document.getElementById('airfq-bulk-project')?.value||'').trim();
    const bulkUom=(document.getElementById('airfq-bulk-uom')?.value||'').trim();
    const bulkTax=(document.getElementById('airfq-bulk-tax')?.value||'').trim();
    const bulkQtyRaw=(document.getElementById('airfq-bulk-qty')?.value||'').trim();
    const bulkPriceRaw=(document.getElementById('airfq-bulk-price')?.value||'').trim();
    const descPrefix=(document.getElementById('airfq-bulk-desc-prefix')?.value||'').trim();
    const descSuffix=(document.getElementById('airfq-bulk-desc-suffix')?.value||'').trim();
    const hasQty=bulkQtyRaw!=='';
    const hasPrice=bulkPriceRaw!=='';
    const bulkQty=hasQty?(parseFloat(bulkQtyRaw)||0):null;
    const bulkPrice=hasPrice?(parseFloat(bulkPriceRaw)||0):null;
    let touched=0;
    draft.lines=draft.lines.map(line=>{
        const next={...(line||{})};
        const isNote=((next.display_type||'').toLowerCase()==='line_note')||String(next.description||'').toLowerCase().startsWith('payment:');
        if(isNote)return next;
        if(bulkProduct){next.product=bulkProduct;touched++;}
        if(bulkProject){next.project=bulkProject;touched++;}
        if(bulkUom){next.uom=bulkUom;touched++;}
        if(bulkTax){next.taxes=[bulkTax];touched++;}
        if(hasQty){next.quantity=bulkQty&&bulkQty>0?bulkQty:1;touched++;}
        if(hasPrice){next.unit_price=bulkPrice&&bulkPrice>=0?bulkPrice:0;touched++;}
        const baseDesc=(next.description||'RFQ line').trim();
        if(descPrefix||descSuffix){
            next.description=`${descPrefix?descPrefix+' ':''}${baseDesc}${descSuffix?' '+descSuffix:''}`.trim();
            touched++;
        }
        return next;
    });
    refreshAirRfqDerivedViews();
    setAirRfqStatus(`Applied global column edits to ${draft.lines.length} row(s).`,'var(--green)');
    if(!touched)showToast('No global values provided; nothing changed');
}
function addAirRfqLine(){
    const draft=airfqState.revisionContext?.last_draft;
    if(!draft){showToast('Generate RFQ first');return;}
    if(!Array.isArray(draft.lines))draft.lines=[];
    const defaultTax=(document.getElementById('airfq-default-tax').value||'').trim();
    draft.lines.push({
        product:'Non-Inventory: Construction in Process',
        description:'New line',
        project:draft.header?.project||'',
        display_type:'',
        quantity:1,
        uom:'Unit',
        unit_price:0,
        taxes:defaultTax?[defaultTax]:[],
    });
    refreshAirRfqDerivedViews();
}
function removeAirRfqLine(idx){
    const draft=airfqState.revisionContext?.last_draft;
    if(!draft||!Array.isArray(draft.lines))return;
    draft.lines=draft.lines.filter((_,i)=>i!==idx);
    if(!draft.lines.length){
        draft.lines=[{product:'Non-Inventory: Construction in Process',description:'RFQ line',project:draft.header?.project||'',display_type:'',quantity:1,uom:'Unit',unit_price:0,taxes:[]}];
    }
    refreshAirRfqDerivedViews();
}
function renderAirRfqValidation(validation){
    const wrap=document.getElementById('airfq-validation');
    if(!wrap)return;
    const errors=(validation&&Array.isArray(validation.errors))?validation.errors:[];
    const warnings=(validation&&Array.isArray(validation.warnings))?validation.warnings:[];
    if(!errors.length&&!warnings.length){
        wrap.innerHTML='<p style="color:var(--green);font-weight:600">No validation issues.</p>';
        return;
    }
    let html='';
    if(errors.length){
        html+='<div style="margin-bottom:10px"><div style="font-size:11px;color:var(--red);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Blocking Errors</div><ul style="margin:0;padding-left:18px">';
        errors.forEach(e=>{
            const row=e.row!=null?` [line ${Number(e.row)+1}]`:'';
            const cands=Array.isArray(e.candidates)&&e.candidates.length?` Candidates: ${e.candidates.join(' | ')}`:'';
            html+=`<li style="margin-bottom:4px;color:var(--red)">${htmlEsc(e.field||'field')}${row}: ${htmlEsc(e.message||'error')}${htmlEsc(cands)}</li>`;
        });
        html+='</ul></div>';
    }
    if(warnings.length){
        html+='<div><div style="font-size:11px;color:var(--yellow);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Warnings</div><ul style="margin:0;padding-left:18px">';
        warnings.forEach(w=>{
            const row=w.row!=null?` [line ${Number(w.row)+1}]`:'';
            html+=`<li style="margin-bottom:4px;color:var(--yellow)">${htmlEsc(w.field||'field')}${row}: ${htmlEsc(w.message||'warning')}</li>`;
        });
        html+='</ul></div>';
    }
    wrap.innerHTML=html;
}
function renderAirRfqPreview(preview){
    const wrap=document.getElementById('airfq-preview');
    if(!wrap)return;
    if(!preview||!preview.header){
        wrap.innerHTML='<p style="color:var(--muted);padding:14px">No preview yet. Generate an RFQ first.</p>';
        return;
    }
    const h=preview.header||{};
    const lines=Array.isArray(preview.lines)?preview.lines:[];
    const t=preview.totals||{};
    let html='<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin-bottom:12px">';
    html+=`<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:10px"><div style="font-size:10px;color:var(--muted);text-transform:uppercase">Vendor</div><div style="font-size:13px;font-weight:600">${htmlEsc(h.vendor||'')}</div></div>`;
    html+=`<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:10px"><div style="font-size:10px;color:var(--muted);text-transform:uppercase">Vendor Reference</div><div style="font-size:13px;font-weight:600">${htmlEsc(h.vendor_reference||'')}</div></div>`;
    html+=`<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:10px"><div style="font-size:10px;color:var(--muted);text-transform:uppercase">Deliver To</div><div style="font-size:13px;font-weight:600">${htmlEsc(h.deliver_to||'')}</div></div>`;
    html+=`<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:10px"><div style="font-size:10px;color:var(--muted);text-transform:uppercase">Project</div><div style="font-size:13px;font-weight:600">${htmlEsc(h.project||'')}</div></div>`;
    html+='</div>';
    html+='<div style="overflow-x:auto"><table class="display" id="airfq-preview-tbl" style="width:100%"><thead><tr><th>#</th><th>Type</th><th>Product</th><th>Description</th><th>Project</th><th>Qty</th><th>UoM</th><th>Unit Price</th><th>Tax</th><th>Subtotal</th><th>Total</th></tr></thead><tbody>';
    lines.forEach(l=>{
        const isNote=(l.display_type||'')==='line_note';
        html+=`<tr><td>${l.line_no||''}</td><td>${isNote?'Note':'Spend'}</td><td>${htmlEsc(l.product||'')}</td><td>${htmlEsc(l.description||'')}</td><td>${htmlEsc(l.project||'')}</td><td style="text-align:right">${isNote?'':Number(l.quantity||0).toFixed(2)}</td><td>${isNote?'':htmlEsc(l.uom||'')}</td><td style="text-align:right" data-order="${l.unit_price||0}">${isNote?'':fmtMoney2(l.unit_price||0)}</td><td>${isNote?'':htmlEsc(l.tax_label||'')}</td><td style="text-align:right" data-order="${l.line_subtotal||0}">${fmtMoney2(l.line_subtotal||0)}</td><td style="text-align:right" data-order="${l.line_total||0}">${fmtMoney2(l.line_total||0)}</td></tr>`;
    });
    html+='</tbody></table></div>';
    html+=`<div style="display:flex;gap:20px;justify-content:flex-end;margin-top:10px;font-size:13px"><div>Untaxed: <strong>${fmtMoney2(t.untaxed_amount||0)}</strong></div><div>Tax: <strong>${fmtMoney2(t.tax_amount||0)}</strong></div><div>Total: <strong style="color:var(--green)">${fmtMoney2(t.total_amount||0)}</strong></div></div>`;
    wrap.innerHTML=html;
    if(dtI['airfq-preview-tbl'])dtI['airfq-preview-tbl'].destroy();
    dtI['airfq-preview-tbl']=$('#airfq-preview-tbl').DataTable(dtOpts({pageLength:25,order:[[0,'asc']]}));
}
async function loadAirRfq(forceLive){
    try{
        const isForce=Boolean(forceLive);
        setAirRfqStatus(isForce?'Refreshing lookup cache...':'Loading lookup values...','var(--muted)');
        const qs=isForce?'?mode=bq_only&force_refresh=1':'?mode=bq_only';
        const res=await fetch('/api/v2/ai-rfq/lookups'+qs);
        const d=await res.json();
        if(d.error){setAirRfqStatus('Lookup warning: '+d.error,'var(--yellow)');return;}
        const values=(d.values&&typeof d.values==='object')?d.values:{};
        airfqState.lookupValues=values;
        const vendors=Array.isArray(values.vendors)?values.vendors:[];
        populateAirRfqLookupInputs(values);
        loadAirRfqHistory();
        const warnings=Array.isArray(d.warnings)?d.warnings:[];
        if(warnings.length)setAirRfqStatus('Lookups loaded with warnings: '+warnings.join(' | '),'var(--yellow)');
        else setAirRfqStatus(`Lookups loaded (${vendors.length} vendors).`,'var(--green)');
        if(airfqState.revisionContext?.last_draft)refreshAirRfqDerivedViews();
    }catch(e){
        setAirRfqStatus('Lookup error: '+e.message,'var(--red)');
    }
}
async function generateAirRfq(isRegenerate){
    const vendor=(document.getElementById('airfq-vendor').value||'').trim();
    const referencePo=(document.getElementById('airfq-reference-po').value||'').trim().toUpperCase();
    const paymentNote=(document.getElementById('airfq-payment-note').value||'').trim();
    const basePrompt=(document.getElementById('airfq-prompt').value||'').trim();
    const prompt=referencePo?`${basePrompt}\nReference PO: ${referencePo}`.trim():basePrompt;
    const fileInput=document.getElementById('airfq-pdf');
    const file=(fileInput&&fileInput.files&&fileInput.files[0])?fileInput.files[0]:null;
    const btnGenerate=document.getElementById('btn-airfq-generate');
    const btnRegenerate=document.getElementById('btn-airfq-regenerate');
    const btnDownload=document.getElementById('btn-airfq-download');
    if(!vendor&&!airfqState.revisionContext?.vendor){showToast('Vendor is required');return;}
    const knownVendors=(airfqState.lookupValues&&Array.isArray(airfqState.lookupValues.vendors))?airfqState.lookupValues.vendors:[];
    if(vendor&&knownVendors.length){
        const exact=knownVendors.includes(vendor)?vendor:(knownVendors.find(v=>(v||'').toLowerCase()===vendor.toLowerCase())||'');
        if(exact){
            document.getElementById('airfq-vendor').value=exact;
        }else{
            setAirRfqStatus('Vendor must match a known vendor name exactly. Pick from lookup list or refresh BQ lookups.','var(--red)');
            showToast('Vendor not found in lookup list');
            return;
        }
    }
    if(!isRegenerate&&!file&&!airfqState.revisionContext?.quote_text){showToast('Attach a quote PDF for initial generation');return;}
    if(isRegenerate&&!airfqState.revisionContext&&!file){showToast('Run AI Generate first or attach a PDF');return;}
    const endpoint=isRegenerate?'/api/v2/ai-rfq/regenerate':'/api/v2/ai-rfq/generate';
    btnGenerate.disabled=true;btnRegenerate.disabled=true;btnDownload.disabled=true;
    startAirRfqProgress(isRegenerate?'Regenerating':'Generating');
    const startedAt=Date.now();
    try{
        const headerDeliver=(document.getElementById('airfq-header-deliver').value||'').trim();
        const headerProject=(document.getElementById('airfq-header-project').value||'').trim();
        const form=new FormData();
        form.append('vendor',vendor||airfqState.revisionContext?.vendor||'');
        form.append('prompt',prompt);
        form.append('payment_milestones_note',paymentNote);
        if(headerDeliver)form.append('deliver_to',headerDeliver);
        if(headerProject)form.append('header_project',headerProject);
        if(airfqState.revisionContext)form.append('prior_context',JSON.stringify(airfqState.revisionContext));
        if(file)form.append('file',file);
        const res=await fetch(endpoint,{method:'POST',body:form});
        const d=await res.json();
        if(d.error){setAirRfqStatus('Error: '+d.error,'var(--red)');showToast('AI RFQ failed');return;}
        airfqState.revisionContext=d.revision_context||null;
        airfqState.csvText=d.csv_text||'';
        airfqState.csvFilename=d.csv_filename||'rfq_ai_generated.csv';
        airfqState.lastValidation=d.validation||{};
        renderAirRfqValidation(d.validation||{});
        airfqState.lastPreview=d.preview||{};
        refreshAirRfqDerivedViews();
        loadAirRfqHistory();
        const elapsed=((Date.now()-startedAt)/1000);
        const totalS=Number(d.meta?.timing?.total_s||elapsed).toFixed(1);
        const llmS=Number(d.meta?.timing?.llm_s||0).toFixed(1);
        const blocking=Number(d.validation?.blocking_error_count||0);
        const warningCount=Array.isArray(d.validation?.warnings)?d.validation.warnings.length:0;
        if(blocking>0)setAirRfqStatus(`Generated in ${totalS}s (LLM ${llmS}s) with ${blocking} blocking validation issue(s).`,'var(--red)');
        else if(warningCount>0)setAirRfqStatus(`Generated in ${totalS}s (LLM ${llmS}s) with ${warningCount} warning(s).`,'var(--yellow)');
        else setAirRfqStatus(`RFQ generated and validated successfully in ${totalS}s (LLM ${llmS}s).`,'var(--green)');
    }catch(e){
        setAirRfqStatus('Error: '+e.message,'var(--red)');
    }finally{
        stopAirRfqProgress();
        btnGenerate.disabled=false;btnRegenerate.disabled=false;
        btnDownload.disabled=!airfqState.csvText;
    }
}
function downloadAirRfqCsv(){
    if(!airfqState.csvText){showToast('No CSV to download yet');return;}
    const blob=new Blob([airfqState.csvText],{type:'text/csv;charset=utf-8'});
    downloadBlob(airfqState.csvFilename||'rfq_ai_generated.csv',blob);
}
function resetAirRfqForm(){
    stopAirRfqProgress();
    airfqState.revisionContext=null;
    airfqState.csvText='';
    airfqState.csvFilename='rfq_ai_generated.csv';
    airfqState.lastValidation=null;
    airfqState.lastPreview=null;
    const ids=['airfq-vendor','airfq-reference-po','airfq-history-select','airfq-prompt','airfq-payment-note','airfq-header-project','airfq-header-deliver','airfq-default-tax','airfq-bulk-product','airfq-bulk-project','airfq-bulk-uom','airfq-bulk-tax','airfq-bulk-qty','airfq-bulk-price','airfq-bulk-desc-prefix','airfq-bulk-desc-suffix'];
    ids.forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
    const file=document.getElementById('airfq-pdf');if(file)file.value='';
    const csv=document.getElementById('airfq-csv');if(csv)csv.value='';
    const validation=document.getElementById('airfq-validation');if(validation)validation.innerHTML='<p style=\"color:var(--muted)\">No validation yet.</p>';
    const preview=document.getElementById('airfq-preview');if(preview)preview.innerHTML='<p style=\"color:var(--muted);padding:14px\">No preview yet. Generate an RFQ first.</p>';
    const editor=document.getElementById('airfq-line-editor');if(editor)editor.innerHTML='<p style=\"color:var(--muted);padding:12px\">No lines available.</p>';
    const btnDownload=document.getElementById('btn-airfq-download');if(btnDownload)btnDownload.disabled=true;
    setAirRfqStatus('Form reset. Ready for a new RFQ run.','var(--green)');
}

/* ====== SETTINGS ====== */
let savedSettings={};
let settingsAccess={};
function applySettingsAccessUi(){
    const canEdit=!!settingsAccess.can_edit_settings;
    const canManage=!!settingsAccess.can_manage_access;
    const role=settingsAccess.role||'viewer';
    const owner=settingsAccess.owner_email||'';
    const user=settingsAccess.user_email||'';
    const summary=document.getElementById('settings-access-summary');
    if(summary){
        const roleLabel=role.charAt(0).toUpperCase()+role.slice(1);
        summary.textContent=`Signed in as ${user||'unknown'} | Role: ${roleLabel}${owner?` | Owner: ${owner}`:''}`;
        summary.style.color=canEdit?'var(--muted)':'var(--yellow)';
    }

    document.querySelectorAll('#page-settings textarea, #page-settings input, #page-settings select').forEach(el=>{
        const id=el.id||'';
        if(id==='settings-owner-email'||id==='settings-editor-emails'||id==='settings-restrict-access'){
            el.disabled=!canManage;
        }else{
            el.disabled=!canEdit;
        }
    });

    const saveBtn=document.getElementById('btn-save-settings');
    if(saveBtn)saveBtn.disabled=!canEdit;
    const refreshBtn=document.getElementById('btn-refresh-odoo');
    if(refreshBtn)refreshBtn.disabled=!canEdit;
    const rampBtn=document.getElementById('btn-upload-ramp');
    if(rampBtn)rampBtn.disabled=!canEdit;
    const aiBtn=document.getElementById('btn-gen-milestones');
    if(aiBtn)aiBtn.disabled=!canEdit;
}
async function loadSettings(){
    const res=await fetch('/api/settings');savedSettings=await res.json();
    settingsAccess=(savedSettings._access&&typeof savedSettings._access==='object')?savedSettings._access:{};
    const mRes=await fetch('/api/modules');const mods=await mRes.json();

    /* --- Creator names textarea --- */
    const names=savedSettings.po_creator_names||[];
    const ta=document.getElementById('settings-creators');
    ta.value=names.join('\n');
    ta.addEventListener('input',()=>{
        const count=ta.value.split('\n').filter(l=>l.trim()).length;
        document.getElementById('creator-count').textContent=count+' names';
    });
    document.getElementById('creator-count').textContent=names.length+' names';

    /* --- Milestone AI prompt settings --- */
    document.getElementById('settings-milestone-program-context').value=savedSettings.milestone_ai_program_context||'';
    document.getElementById('settings-milestone-system-prompt').value=savedSettings.milestone_ai_system_prompt||'';
    document.getElementById('settings-milestone-user-prefix').value=savedSettings.milestone_ai_user_prefix||'';
    document.getElementById('settings-classification-domain-context').value=savedSettings.classification_ai_domain_context||'';
    document.getElementById('settings-classification-system-prompt').value=savedSettings.classification_ai_system_prompt||'';
    document.getElementById('settings-rfq-provider').value=savedSettings.rfq_ai_provider||'gemini';
    document.getElementById('settings-rfq-validation-mode').value=savedSettings.rfq_validation_mode||'bq_only';
    document.getElementById('settings-rfq-user-prefix').value=savedSettings.rfq_ai_user_prefix||'';
    document.getElementById('settings-rfq-system-prompt').value=savedSettings.rfq_ai_system_prompt||'';
    document.getElementById('settings-owner-email').value=savedSettings.settings_owner_email||'';
    document.getElementById('settings-editor-emails').value=(savedSettings.settings_editor_emails||[]).join('\n');
    const restrictEl=document.getElementById('settings-restrict-access');
    if(restrictEl)restrictEl.checked=!!savedSettings.restrict_access_to_editors_only;
    document.getElementById('settings-refresh-cron').value=savedSettings.ops_refresh_cron||'0 8 * * *';
    document.getElementById('settings-refresh-timezone').value=savedSettings.ops_refresh_timezone||'Etc/UTC';
    document.getElementById('settings-alert-emails').value=(savedSettings.ops_alert_emails||[]).join('\n');

    /* --- Line capacities table --- */
    const caps=savedSettings.line_capacities||{};
    const sqfts=savedSettings.line_sqft||{};
    let html='<table style="width:100%;border-collapse:collapse"><thead><tr style="border-bottom:1px solid var(--border)"><th style="text-align:left;padding:8px;color:var(--muted);font-size:11px;text-transform:uppercase">Line</th><th style="text-align:left;padding:8px;color:var(--muted);font-size:11px;text-transform:uppercase">Capacity (GWh)</th><th style="text-align:left;padding:8px;color:var(--muted);font-size:11px;text-transform:uppercase">Floor Area (ft&sup2;)</th></tr></thead><tbody>';
    mods.forEach(m=>{
        html+=`<tr style="border-bottom:1px solid rgba(62,61,58,.3)"><td style="padding:8px;font-size:13px;font-weight:600;color:var(--green)">${m}</td><td style="padding:8px"><input class="forecast-input" id="cap-${m}" type="number" step="0.1" min="0" value="${caps[m]||''}" placeholder="0" style="width:120px"/></td><td style="padding:8px"><input class="forecast-input" id="sqft-${m}" type="number" step="100" min="0" value="${sqfts[m]||''}" placeholder="0" style="width:120px"/></td></tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('settings-lines').innerHTML=html;
    applySettingsAccessUi();
}
async function saveSettings(){
    if(!settingsAccess.can_edit_settings){
        showToast('You do not have permission to edit settings');
        return;
    }
    const mRes=await fetch('/api/modules');const mods=await mRes.json();

    /* Collect creator names from textarea */
    const rawNames=document.getElementById('settings-creators').value;
    const creatorNames=rawNames.split('\n').map(n=>n.trim().toLowerCase()).filter(n=>n.length>0);

    /* Collect line capacities */
    const caps={},sqfts={};
    mods.forEach(m=>{
        const cv=parseFloat(document.getElementById('cap-'+m).value);
        const sv=parseFloat(document.getElementById('sqft-'+m).value);
        if(!isNaN(cv)&&cv>0)caps[m]=cv;
        if(!isNaN(sv)&&sv>0)sqfts[m]=sv;
    });

    const body={
        po_creator_names:creatorNames,
        line_capacities:caps,
        line_sqft:sqfts,
        milestone_ai_program_context:(document.getElementById('settings-milestone-program-context').value||'').trim(),
        milestone_ai_system_prompt:(document.getElementById('settings-milestone-system-prompt').value||'').trim(),
        milestone_ai_user_prefix:(document.getElementById('settings-milestone-user-prefix').value||'').trim(),
        classification_ai_domain_context:(document.getElementById('settings-classification-domain-context').value||'').trim(),
        classification_ai_system_prompt:(document.getElementById('settings-classification-system-prompt').value||'').trim(),
        rfq_ai_provider:(document.getElementById('settings-rfq-provider').value||'gemini').trim(),
        rfq_validation_mode:(document.getElementById('settings-rfq-validation-mode').value||'bq_only').trim(),
        rfq_ai_user_prefix:(document.getElementById('settings-rfq-user-prefix').value||'').trim(),
        rfq_ai_system_prompt:(document.getElementById('settings-rfq-system-prompt').value||'').trim(),
        ops_refresh_cron:(document.getElementById('settings-refresh-cron').value||'0 8 * * *').trim(),
        ops_refresh_timezone:(document.getElementById('settings-refresh-timezone').value||'Etc/UTC').trim(),
        ops_alert_emails:(document.getElementById('settings-alert-emails').value||'')
            .split('\n')
            .map(v=>v.trim().toLowerCase())
            .filter(v=>v.length>0),
    };
    if(settingsAccess.can_manage_access){
        body.settings_owner_email=(document.getElementById('settings-owner-email').value||'').trim().toLowerCase();
        body.settings_editor_emails=(document.getElementById('settings-editor-emails').value||'')
            .split('\n')
            .map(v=>v.trim().toLowerCase())
            .filter(v=>v.length>0);
        const restrictEl=document.getElementById('settings-restrict-access');
        body.restrict_access_to_editors_only=!!(restrictEl&&restrictEl.checked);
    }
    const res=await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d=await res.json();
    if(!res.ok||d.error){
        showToast(d.error||('Settings save failed ('+res.status+')'));
        return;
    }
    if(d.ok){
        document.getElementById('settings-ok').style.display='inline';
        document.getElementById('creator-count').textContent=creatorNames.length+' names';
        setTimeout(()=>{document.getElementById('settings-ok').style.display='none';},2500);
        showToast('Settings saved ('+creatorNames.length+' creators, capacities updated)');
        if(d.access&&typeof d.access==='object'){
            settingsAccess=d.access;
            applySettingsAccessUi();
        }
    }
}

/* ====== UNIT ECONOMICS ====== */
async function loadUnitEconomics(){
    const res=await fetch('/api/unit-economics');const d=await res.json();
    const t=d.totals||{};
    const lines=d.lines||[];
    const hasData=lines.some(l=>l.gwh>0);

    document.getElementById('ue-kpis').innerHTML=`
        <div class="kpi"><div class="label">Total Forecasted Spend</div><div class="value dollar">${fmt$(t.total_spend)}</div></div>
        <div class="kpi"><div class="label">Hub Capacity (max MOD vs INV)</div><div class="value">${(t.total_gwh||0).toFixed(1)} GWh</div><div style="font-size:11px;color:var(--muted);margin-top:6px">Line sum: ${(t.total_line_gwh||0).toFixed(1)} GWh</div></div>
        <div class="kpi"><div class="label">Avg Forecast $/GWh</div><div class="value dollar">${t.avg_dollar_per_gwh?fmt$(t.avg_dollar_per_gwh):'<span style=\"color:var(--muted);font-size:14px\">Set capacities in Settings</span>'}</div></div>
        <div class="kpi"><div class="label">Total Floor Area</div><div class="value">${t.total_sqft?(t.total_sqft).toLocaleString()+' ft&sup2;':'--'}</div></div>
        <div class="kpi"><div class="label">Avg ft&sup2;/GWh</div><div class="value">${t.avg_sqft_per_gwh?(t.avg_sqft_per_gwh).toLocaleString(undefined,{maximumFractionDigits:0})+' ft&sup2;':'--'}</div></div>`;

    if(!hasData){
        ['chart-ue-dollar','chart-ue-compare','chart-ue-sqft','chart-ue-stack'].forEach(id=>{document.getElementById(id).innerHTML='<p style="color:var(--muted);padding:30px;text-align:center">Configure line capacities in Settings to see unit economics.</p>';});
        document.getElementById('ue-table-wrap').innerHTML='';
        return;
    }

    const configured=lines.filter(l=>l.gwh>0).sort((a,b)=>a.line.localeCompare(b.line));

    const UL=function(m,extra){const o={paper_bgcolor:C.surface,plot_bgcolor:C.surface,font:{color:C.text,size:11},height:350,margin:m||{l:70,r:20,t:20,b:90},yaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickprefix:'$',tickformat:',.0s'},xaxis:{gridcolor:C.surface2,zerolinecolor:C.surface2,tickangle:-30}};if(extra)Object.assign(o,extra);return o;};

    // $/GWh bar chart
    Plotly.newPlot('chart-ue-dollar',[{
        x:configured.map(l=>l.line),y:configured.map(l=>l.forecast_per_gwh||0),
        type:'bar',marker:{color:C.green},
        text:configured.map(l=>l.forecast_per_gwh?fmt$(l.forecast_per_gwh):''),textposition:'outside',textfont:{color:C.muted,size:10},
        hovertemplate:'%{x}<br>%{y:$,.0f}/GWh<extra></extra>'
    }],UL({l:70,r:60,t:20,b:90}),PC);

    // Forecast spend by line
    const cmpLay=UL({l:70,r:20,t:20,b:90});
    Plotly.newPlot('chart-ue-compare',[
        {x:configured.map(l=>l.line),y:configured.map(l=>l.forecasted||0),type:'bar',name:'Forecasted Spend',marker:{color:C.surface2},text:configured.map(l=>fmt$(l.forecasted||0)),textposition:'outside',textfont:{color:C.muted,size:10}},
    ],cmpLay,PC);

    // ft²/GWh
    const withSqft=configured.filter(l=>l.sqft_per_gwh);
    if(withSqft.length){
        const sqLay=UL({l:70,r:60,t:20,b:90});sqLay.yaxis.tickprefix='';sqLay.yaxis.tickformat=',.0f';
        Plotly.newPlot('chart-ue-sqft',[{
            x:withSqft.map(l=>l.line),y:withSqft.map(l=>l.sqft_per_gwh),
            type:'bar',marker:{color:C.blue},
            text:withSqft.map(l=>(l.sqft_per_gwh||0).toLocaleString(undefined,{maximumFractionDigits:0})+' ft\u00B2'),textposition:'outside',textfont:{color:C.muted,size:10}
        }],sqLay,PC);
    } else {
        document.getElementById('chart-ue-sqft').innerHTML='<p style="color:var(--muted);padding:30px;text-align:center">Set floor area in Settings to see ft\u00B2/GWh.</p>';
    }

    // Forecast-only $/GWh composition
    const stkLay=UL({l:70,r:20,t:20,b:90});
    Plotly.newPlot('chart-ue-stack',[
        {x:configured.map(l=>l.line),y:configured.map(l=>(l.forecast_per_gwh||0)),type:'bar',name:'Forecast $/GWh',marker:{color:C.green},text:configured.map(l=>l.forecast_per_gwh?fmt$(l.forecast_per_gwh):''),textposition:'outside',textfont:{color:C.muted,size:10}},
    ],stkLay,PC);

    // Detail table
    let html='<table id="ue-tbl" class="display compact" style="width:100%"><thead><tr><th>Line</th><th>GWh</th><th>Forecasted Spend</th><th>$/GWh (Forecast)</th><th>ft&sup2;</th><th>ft&sup2;/GWh</th><th>Stations</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    configured.forEach(l=>{
        html+=`<tr><td style="font-weight:600;color:var(--green)">${l.line}</td><td>${l.gwh.toFixed(1)}</td><td class="dollar">${fmtF$(l.forecasted)}</td><td class="dollar">${l.forecast_per_gwh?fmtF$(l.forecast_per_gwh):'--'}</td><td>${l.sqft?(l.sqft).toLocaleString():''}</td><td>${l.sqft_per_gwh?(l.sqft_per_gwh).toLocaleString(undefined,{maximumFractionDigits:0}):''}</td><td>${l.station_count}</td></tr>`;
    });
    html+='</tbody></table>';
    document.getElementById('ue-table-wrap').innerHTML=html;
    if(dtI['ue-tbl'])dtI['ue-tbl'].destroy();
    dtI['ue-tbl']=$('#ue-tbl').DataTable(dtOpts({pageLength:25,order:[[3,'desc']],dom:'Bfrtip',buttons:['csv','excel']}));
}

function showToast(msg){const t=document.getElementById('toast');t.textContent=msg;t.style.display='block';setTimeout(()=>{t.style.display='none';},3000);}
function initFromHash(){
    const hash=window.location.hash.slice(1);
    if(hash&&['executive','about','source','stations','forecasting','vendors','spares','detail','timeline','projects','uniteco','settings','airfq'].includes(hash)){
        const navItem=document.querySelector(`.nav-item[onclick*="showPage('${hash}')"]`);
        showPage(hash,navItem);
    }else{
        loadExecutive();
    }
}
window.addEventListener('hashchange',initFromHash);
// Wait for line filter default selection before loading data
initUiPrefs();
initLineFilter().then(function(){ initFromHash(); });
</script>
</body>
</html>"""


@app.route("/api/projects")
def api_projects():
    """Return spend data for non-production projects (pilot, NPI, facilities, etc.)."""
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify({"projects": [], "monthly": [], "top_vendors": [], "details": []})

    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)
    df["_mod"] = df["station_id"].apply(_extract_mod)

    non_prod = df[
        (df["_mod"] == "") &
        (df["station_id"] != "")
    ].copy()

    prod_unmapped = df[
        (df["_mod"] == "") &
        (df["station_id"] == "")
    ].copy()

    project_groups = {}
    for _, row in non_prod.iterrows():
        key = row["station_id"]
        if key not in project_groups:
            project_groups[key] = {"name": key, "spend": 0, "count": 0, "vendors": set()}
        project_groups[key]["spend"] += row["_sub"]
        project_groups[key]["count"] += 1
        if row["vendor_name"]:
            project_groups[key]["vendors"].add(row["vendor_name"])

    by_proj_name = prod_unmapped.groupby("project_name").agg(
        spend=("_sub", "sum"), count=("_sub", "size"),
    ).reset_index().sort_values("spend", ascending=False)
    for _, row in by_proj_name.iterrows():
        pn = row["project_name"] or "(unmapped)"
        if pn not in project_groups:
            project_groups[pn] = {"name": pn, "spend": 0, "count": 0, "vendors": set()}
        project_groups[pn]["spend"] += row["spend"]
        project_groups[pn]["count"] += int(row["count"])

    projects = sorted(project_groups.values(), key=lambda x: -x["spend"])
    for p in projects:
        p["vendors"] = len(p["vendors"]) if isinstance(p["vendors"], set) else 0

    all_other = pd.concat([non_prod, prod_unmapped], ignore_index=True)
    all_other["_date"] = pd.to_datetime(all_other["date_order"], errors="coerce")
    dated = all_other.dropna(subset=["_date"]).copy()
    dated["_month"] = dated["_date"].dt.to_period("M").astype(str)

    monthly = dated.groupby("_month")["_sub"].sum().reset_index()
    monthly_data = [{"month": r["_month"], "spend": float(r["_sub"])} for _, r in monthly.iterrows()]

    vendor_agg = all_other.groupby("vendor_name")["_sub"].sum().reset_index().sort_values("_sub", ascending=False).head(10)
    vendor_data = [{"vendor": r["vendor_name"], "spend": float(r["_sub"])} for _, r in vendor_agg.iterrows()]

    detail_cols = ["source", "po_number", "date_order", "vendor_name", "item_description",
                   "station_id", "project_name", "price_subtotal", "created_by_name"]
    details = all_other.sort_values("_sub", ascending=False).head(500)
    detail_data = [{c: row.get(c, "") for c in detail_cols} for _, row in details.iterrows()]

    return jsonify({
        "projects": projects,
        "monthly": monthly_data,
        "top_vendors": vendor_data,
        "details": detail_data,
        "total_spend": float(all_other["_sub"].sum()),
        "total_lines": len(all_other),
    })


@app.route("/api/drilldown")
def api_drilldown():
    """Flexible drill-down: filter transactions by any field combo via query params."""
    df = _load_csv("capex_clean.csv")
    if df.empty:
        return jsonify([])

    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)
    df["_date"] = pd.to_datetime(df["date_order"], errors="coerce")
    df["_line"] = df["station_id"].apply(_extract_line)
    df["_month"] = df["_date"].dt.to_period("M").astype(str)

    vendor = request.args.get("vendor", "")
    station = request.args.get("station", "")
    line = request.args.get("line", "")
    month = request.args.get("month", "")
    category = request.args.get("category", "")
    source = request.args.get("source", "")
    confidence = request.args.get("confidence", "")
    project = request.args.get("project", "")
    week = request.args.get("week", "")
    employee = request.args.get("employee", "")
    subcategory = request.args.get("subcategory", "")
    payment_status = request.args.get("payment_status", "")

    if vendor:
        df = df[df["vendor_name"] == vendor]
    if station:
        df = df[df["station_id"] == station]
    if line:
        df = df[df["_line"] == line]
    if month:
        df = df[df["_month"] == month]
    if category:
        df = df[df["product_category"] == category]
    if source:
        df = df[df["source"] == source]
    if confidence:
        df = df[df["mapping_confidence"] == confidence]
    if project:
        df = df[df["project_name"] == project]
    if employee:
        df = df[df["created_by_name"] == employee]
    if subcategory and "mfg_subcategory" in df.columns:
        df = df[df["mfg_subcategory"] == subcategory]
    status_col = "po_payment_status_v2" if "po_payment_status_v2" in df.columns else "bill_payment_status"
    if payment_status and status_col in df.columns:
        bps = df[status_col].fillna("").astype(str).str.strip().replace("", "no_bill")
        df = df[bps == payment_status]
    if week:
        df["_week"] = df["_date"].dt.isocalendar().apply(
            lambda r: f"{int(r['year'])}-W{int(r['week']):02d}" if pd.notna(r["year"]) else "", axis=1
        )
        df = df[df["_week"] == week]

    total = float(df["_sub"].sum())
    count = len(df)

    cols = ["source", "po_number", "date_order", "vendor_name", "mfg_subcategory",
            "item_description", "station_id", "project_name", "mapping_confidence",
            "bill_payment_status", "po_payment_status_v2", "price_subtotal", "price_total", "created_by_name"]
    rows = df.sort_values("_sub", ascending=False).head(200)
    records = [{c: row.get(c, "") for c in cols} for _, row in rows.iterrows()]

    return jsonify({"total": total, "count": count, "rows": records})


@app.route("/api/forecast", methods=["POST"])
def api_forecast_update():
    """Save a forecast override for a station."""
    denied = _require_settings_editor()
    if denied:
        return denied
    body = request.get_json(force=True)
    station_id = body.get("station_id", "")
    new_forecast = body.get("forecasted_cost")
    if not station_id or new_forecast is None:
        return jsonify({"ok": False, "error": "station_id and forecasted_cost required"}), 400

    try:
        value = float(new_forecast)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "forecasted_cost must be numeric"}), 400
    if value < 0:
        return jsonify({"ok": False, "error": "forecasted_cost must be >= 0"}), 400

    result = _apply_forecast_updates({str(station_id): value})
    if result["updated_count"] == 0:
        return jsonify({
            "ok": False,
            "error": "station_id not found in capex_by_station.csv",
            "unmatched_station_ids": result["unmatched_station_ids"],
        }), 404
    return jsonify({"ok": True, **result})


@app.route("/api/forecast/bulk", methods=["POST"])
def api_forecast_bulk_update():
    """Save many station forecast overrides in one request."""
    denied = _require_settings_editor()
    if denied:
        return denied
    body = request.get_json(force=True)
    raw_updates = body.get("updates", [])
    if not isinstance(raw_updates, list):
        return jsonify({"ok": False, "error": "updates must be an array"}), 400

    parsed_updates: dict[str, float] = {}
    skipped_invalid = 0
    for item in raw_updates:
        if not isinstance(item, dict):
            skipped_invalid += 1
            continue
        sid = str(item.get("station_id", "")).strip()
        raw_val = item.get("forecasted_cost")
        if not sid or raw_val is None:
            skipped_invalid += 1
            continue
        try:
            val = float(raw_val)
        except (TypeError, ValueError):
            skipped_invalid += 1
            continue
        if val < 0:
            skipped_invalid += 1
            continue
        parsed_updates[sid] = val

    if not parsed_updates:
        return jsonify({"ok": False, "error": "No valid updates supplied."}), 400

    result = _apply_forecast_updates(parsed_updates)
    skipped_total = skipped_invalid + len(result["unmatched_station_ids"])
    return jsonify({
        "ok": True,
        **result,
        "skipped_count": skipped_total,
        "invalid_count": skipped_invalid,
    })


def _normalize_station_ids(raw_station_ids: object) -> list[str]:
    if not isinstance(raw_station_ids, list):
        return []
    norm: list[str] = []
    seen: set[str] = set()
    for sid in raw_station_ids:
        key = str(sid).strip().upper()
        if not key or key in seen:
            continue
        seen.add(key)
        norm.append(key)
    return norm


def _lock_forecast_overrides(station_ids: list[str] | None = None) -> dict[str, object]:
    by_station = _load_csv("capex_by_station.csv")
    if by_station.empty or "station_id" not in by_station.columns:
        return {
            "locked_count": 0,
            "not_found_count": len(station_ids or []),
            "locked_station_ids": [],
            "not_found_station_ids": station_ids or [],
        }

    station_map: dict[str, tuple[str, float]] = {}
    for _, row in by_station.iterrows():
        sid = str(row.get("station_id", "")).strip()
        if not sid:
            continue
        sid_key = sid.upper()
        forecast_value = float(pd.to_numeric(row.get("forecasted_cost", 0), errors="coerce") or 0.0)
        station_map[sid_key] = (sid, forecast_value)

    targets = set(station_ids or station_map.keys())
    overrides = store.read_json("forecast_overrides.json")
    if not isinstance(overrides, dict):
        overrides = {}

    # Keep one canonical key per station_id when lock state is updated.
    for existing_key in list(overrides.keys()):
        if str(existing_key).strip().upper() in targets:
            overrides.pop(existing_key, None)

    locked_station_ids: list[str] = []
    not_found_station_ids: list[str] = []
    for sid_key in sorted(targets):
        data = station_map.get(sid_key)
        if not data:
            not_found_station_ids.append(sid_key)
            continue
        canonical_sid, forecast_value = data
        overrides[canonical_sid] = forecast_value
        locked_station_ids.append(canonical_sid)

    store.write_json("forecast_overrides.json", overrides)
    return {
        "locked_count": len(locked_station_ids),
        "not_found_count": len(not_found_station_ids),
        "locked_station_ids": locked_station_ids,
        "not_found_station_ids": not_found_station_ids,
    }


def _unlock_forecast_overrides(station_ids: list[str] | None = None) -> dict[str, object]:
    overrides = store.read_json("forecast_overrides.json")
    if not isinstance(overrides, dict):
        overrides = {}

    if station_ids is None:
        removed_station_ids = sorted(str(k).strip() for k in overrides.keys() if str(k).strip())
        removed_count = len(removed_station_ids)
        overrides = {}
        store.write_json("forecast_overrides.json", overrides)
        return {
            "removed_count": removed_count,
            "not_found_count": 0,
            "removed_station_ids": removed_station_ids,
            "not_found_station_ids": [],
        }

    targets = set(station_ids)
    removed_station_ids: list[str] = []
    for existing_key in list(overrides.keys()):
        if str(existing_key).strip().upper() in targets:
            removed_station_ids.append(str(existing_key).strip())
            overrides.pop(existing_key, None)

    removed_keys = {sid.upper() for sid in removed_station_ids}
    not_found_station_ids = [sid for sid in sorted(targets) if sid not in removed_keys]
    store.write_json("forecast_overrides.json", overrides)
    return {
        "removed_count": len(removed_station_ids),
        "not_found_count": len(not_found_station_ids),
        "removed_station_ids": removed_station_ids,
        "not_found_station_ids": not_found_station_ids,
    }


@app.route("/api/forecast/unlock", methods=["POST"])
def api_forecast_unlock():
    """Remove a manual forecast override lock for a station."""
    denied = _require_settings_editor()
    if denied:
        return denied
    body = request.get_json(force=True)
    station_id = str(body.get("station_id", "")).strip()
    if not station_id:
        return jsonify({"ok": False, "error": "station_id required"}), 400

    result = _unlock_forecast_overrides([station_id.upper()])
    if result["removed_count"] == 0:
        return jsonify({"ok": False, "error": f"No override lock found for {station_id}"}), 404
    return jsonify({
        "ok": True,
        "station_id": station_id,
        "removed_count": result["removed_count"],
    })


@app.route("/api/forecast/lock", methods=["POST"])
def api_forecast_lock():
    """Create/refresh a manual forecast override lock for a station."""
    denied = _require_settings_editor()
    if denied:
        return denied
    body = request.get_json(force=True)
    station_id = str(body.get("station_id", "")).strip()
    if not station_id:
        return jsonify({"ok": False, "error": "station_id required"}), 400

    result = _lock_forecast_overrides([station_id.upper()])
    if result["locked_count"] == 0:
        return jsonify({"ok": False, "error": f"station_id not found: {station_id}"}), 404
    canonical_sid = result["locked_station_ids"][0]
    by_station = _load_csv("capex_by_station.csv")
    forecast_series = by_station.loc[
        by_station["station_id"].fillna("").astype(str).str.strip().str.upper() == canonical_sid.upper(),
        "forecasted_cost",
    ]
    forecast_value = float(pd.to_numeric(forecast_series.iloc[0], errors="coerce") or 0.0) if not forecast_series.empty else 0.0
    return jsonify({
        "ok": True,
        "station_id": canonical_sid,
        "forecasted_cost": forecast_value,
    })


@app.route("/api/forecast/lock_all", methods=["POST"])
def api_forecast_lock_all():
    """Lock many forecast rows (defaults to all rows when station_ids is omitted)."""
    denied = _require_settings_editor()
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    station_ids_raw = body.get("station_ids")
    station_ids = _normalize_station_ids(station_ids_raw)
    if station_ids_raw is not None and not station_ids:
        return jsonify({"ok": False, "error": "station_ids must be a non-empty array"}), 400
    result = _lock_forecast_overrides(station_ids if station_ids else None)
    return jsonify({"ok": True, **result})


@app.route("/api/forecast/unlock_all", methods=["POST"])
def api_forecast_unlock_all():
    """Unlock many forecast rows (defaults to all locks when station_ids is omitted)."""
    denied = _require_settings_editor()
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    station_ids_raw = body.get("station_ids")
    station_ids = _normalize_station_ids(station_ids_raw)
    if station_ids_raw is not None and not station_ids:
        return jsonify({"ok": False, "error": "station_ids must be a non-empty array"}), 400
    result = _unlock_forecast_overrides(station_ids if station_ids else None)
    return jsonify({"ok": True, **result})


@app.route("/api/forecast/refresh", methods=["POST"])
def api_forecast_refresh():
    """Refresh station forecast values from configured BF1/BF2 Google Sheets."""
    denied = _require_settings_editor()
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    settings = store.read_json("dashboard_settings.json")
    if not isinstance(settings, dict):
        settings = {}

    bf1_url = str(body.get("bf1_sheet_url") or settings.get("bf1_sheet_url") or DEFAULT_BF1_SHEET_URL).strip()
    bf2_url = str(body.get("bf2_sheet_url") or settings.get("bf2_sheet_url") or DEFAULT_BF2_SHEET_URL).strip()
    settings["bf1_sheet_url"] = bf1_url
    settings["bf2_sheet_url"] = bf2_url
    store.write_json("dashboard_settings.json", settings)

    try:
        from sheets_forecast_import import SheetImportError, import_forecast_updates
    except Exception as exc:  # pragma: no cover - defensive
        return jsonify({
            "ok": False,
            "updated_count": 0,
            "updated_station_ids": [],
            "unmatched_station_ids": [],
            "errors": [f"Sheets importer unavailable: {exc}"],
            "bf1": {"ok": False, "candidate_updates": 0},
            "bf2": {"ok": False, "candidate_updates": 0},
        }), 500

    access_token = get_google_access_token()
    if not access_token:
        return jsonify({
            "ok": False,
            "updated_count": 0,
            "updated_station_ids": [],
            "unmatched_station_ids": [],
            "errors": [
                "Google OAuth token unavailable. Please sign out/in again to grant Sheets access."
            ],
            "bf1": {"ok": False, "candidate_updates": 0},
            "bf2": {"ok": False, "candidate_updates": 0},
        }), 401

    errors: list[str] = []
    sheet_results: dict[str, dict] = {}
    per_sheet_updates: dict[str, dict[str, float]] = {"bf1": {}, "bf2": {}}

    for key, url, prefix in (
        ("bf1", bf1_url, "BASE1-"),
        ("bf2", bf2_url, "BASE2-"),
    ):
        try:
            imported = import_forecast_updates(url, access_token=access_token)
            updates = imported.get("updates", {})
            if not isinstance(updates, dict):
                updates = {}
            typed_updates = {str(sid).strip().upper(): float(val) for sid, val in updates.items()}
            scoped = {sid: val for sid, val in typed_updates.items() if sid.startswith(prefix)}
            out_of_scope = sorted([sid for sid in typed_updates.keys() if not sid.startswith(prefix)])
            per_sheet_updates[key] = scoped
            sheet_results[key] = {
                "ok": True,
                "candidate_updates": len(scoped),
                "out_of_scope_count": len(out_of_scope),
                "out_of_scope_samples": out_of_scope[:20],
                "diagnostics": imported.get("diagnostics", {}),
            }
        except SheetImportError as exc:
            errors.append(f"{key.upper()} import failed: {exc}")
            sheet_results[key] = {"ok": False, "error": str(exc), "candidate_updates": 0}
        except Exception as exc:  # pragma: no cover - defensive
            errors.append(f"{key.upper()} import failed: {exc}")
            sheet_results[key] = {"ok": False, "error": str(exc), "candidate_updates": 0}

    merged_updates: dict[str, float] = {}
    merged_updates.update(per_sheet_updates["bf1"])
    merged_updates.update(per_sheet_updates["bf2"])
    overrides = store.read_json("forecast_overrides.json")
    locked_station_ids = {str(k).strip().upper() for k in (overrides or {})} if isinstance(overrides, dict) else set()
    apply_result = _apply_forecast_updates(
        merged_updates,
        update_overrides=False,
        locked_station_ids=locked_station_ids,
    ) if merged_updates else {
        "updated_count": 0,
        "updated_station_ids": [],
        "unmatched_station_ids": [],
        "locked_skipped_station_ids": [],
    }
    updated_set = set(str(s).strip().upper() for s in apply_result["updated_station_ids"])
    bf1_set = set(per_sheet_updates["bf1"].keys())
    bf2_set = set(per_sheet_updates["bf2"].keys())
    sheet_results.setdefault("bf1", {})
    sheet_results.setdefault("bf2", {})
    sheet_results["bf1"]["applied_updates"] = len(updated_set & bf1_set)
    sheet_results["bf2"]["applied_updates"] = len(updated_set & bf2_set)

    ok = bool(apply_result["updated_count"] > 0) or (
        not errors and bool(merged_updates)
    )
    return jsonify({
        "ok": ok,
        "updated_count": apply_result["updated_count"],
        "updated_station_ids": apply_result["updated_station_ids"],
        "unmatched_station_ids": apply_result["unmatched_station_ids"],
        "locked_skipped_station_ids": apply_result["locked_skipped_station_ids"],
        "locked_skipped_count": len(apply_result["locked_skipped_station_ids"]),
        "errors": errors,
        "bf1": sheet_results["bf1"],
        "bf2": sheet_results["bf2"],
    })


@app.route("/api/settings", methods=["GET"])
def api_settings_get():
    """Return all dashboard settings (line capacities, sq ft, creator names, etc.)."""
    user_email = current_user_email()
    settings, changed = load_settings_with_access_defaults(bootstrap_user_email=user_email)
    if changed:
        store.write_json("dashboard_settings.json", settings)
    access = get_access_context(settings, user_email=user_email)

    settings.setdefault("bf1_sheet_url", DEFAULT_BF1_SHEET_URL)
    settings.setdefault("bf2_sheet_url", DEFAULT_BF2_SHEET_URL)
    if "po_creator_names" not in settings:
        from capex_pipeline import DEFAULT_CREATOR_NAMES
        settings["po_creator_names"] = DEFAULT_CREATOR_NAMES
    settings.setdefault("milestone_ai_program_context", "")
    settings.setdefault("milestone_ai_user_prefix", "")
    if not settings.get("milestone_ai_system_prompt"):
        try:
            from classify_agent import MILESTONE_SYSTEM_PROMPT
            settings["milestone_ai_system_prompt"] = MILESTONE_SYSTEM_PROMPT
        except Exception:
            settings["milestone_ai_system_prompt"] = ""
    settings.setdefault(
        "classification_ai_domain_context",
        (
            "Base Power is a BESS manufacturer at BF1 with Module, Cell, and Inverter lines. "
            "Station IDs follow BASE1-MODx/CELLx/INV1-STxxxxx. "
            "Prefer manufacturing station mapping for process equipment, controls/electrical, mechanical assemblies, "
            "integration/commissioning, quality/metrology, and production software. "
            "Use null station for true facility/office/IT/admin spend. "
            "Common vendors include automation integrators, tooling suppliers, controls vendors, and electrical distributors."
        ),
    )
    if not settings.get("classification_ai_system_prompt"):
        try:
            from classify_agent import SYSTEM_PROMPT_TEMPLATE
            settings["classification_ai_system_prompt"] = SYSTEM_PROMPT_TEMPLATE.read_text(encoding="utf-8")
        except Exception:
            settings["classification_ai_system_prompt"] = ""
    settings.setdefault("rfq_ai_provider", "gemini")
    settings["rfq_validation_mode"] = "bq_only"
    settings.setdefault("rfq_ai_user_prefix", "")
    if not settings.get("rfq_ai_system_prompt"):
        try:
            settings["rfq_ai_system_prompt"] = RFQ_SYSTEM_PROMPT_TEMPLATE.read_text(encoding="utf-8")
        except Exception:
            settings["rfq_ai_system_prompt"] = ""
    settings.setdefault("ops_refresh_cron", "0 8 * * *")
    settings.setdefault("ops_refresh_timezone", "Etc/UTC")
    settings.setdefault("ops_alert_emails", [])
    settings["_access"] = access
    return jsonify(settings)


@app.route("/api/settings", methods=["POST"])
def api_settings_save():
    """Save dashboard settings."""
    body = request.get_json(force=True) or {}
    user_email = current_user_email()
    settings, changed = load_settings_with_access_defaults(bootstrap_user_email=user_email)
    if changed:
        store.write_json("dashboard_settings.json", settings)
    access = get_access_context(settings, user_email=user_email)

    if not access.get("can_edit_settings"):
        return jsonify({"ok": False, "error": "Settings edit access required", "access": access}), 403

    touches_access = (OWNER_KEY in body) or (EDITORS_KEY in body) or (RESTRICT_KEY in body)
    if touches_access and not access.get("can_manage_access"):
        return jsonify({
            "ok": False,
            "error": "Only the settings owner can modify access control.",
            "access": access,
        }), 403

    if access.get("can_manage_access") and OWNER_KEY in body:
        candidate_owner = normalize_email(body.get(OWNER_KEY, ""))
        if not candidate_owner:
            return jsonify({"ok": False, "error": "settings_owner_email cannot be empty"}), 400
        if not is_company_email(candidate_owner):
            return jsonify({"ok": False, "error": "settings_owner_email must be a company email"}), 400
        settings[OWNER_KEY] = candidate_owner

    if access.get("can_manage_access") and EDITORS_KEY in body:
        settings[EDITORS_KEY] = normalize_email_list(body.get(EDITORS_KEY, []))
        if settings.get(OWNER_KEY) and settings[OWNER_KEY] not in settings[EDITORS_KEY]:
            settings[EDITORS_KEY].insert(0, settings[OWNER_KEY])

    if access.get("can_manage_access") and RESTRICT_KEY in body:
        settings[RESTRICT_KEY] = bool(body.get(RESTRICT_KEY, False))

    if "ops_alert_emails" in body:
        settings["ops_alert_emails"] = normalize_email_list(body.get("ops_alert_emails", []))
    if "ops_refresh_cron" in body:
        cron = str(body.get("ops_refresh_cron", "")).strip()
        settings["ops_refresh_cron"] = cron if cron else "0 8 * * *"
    if "ops_refresh_timezone" in body:
        tz = str(body.get("ops_refresh_timezone", "")).strip()
        settings["ops_refresh_timezone"] = tz if tz else "Etc/UTC"

    for key, value in body.items():
        if key in {OWNER_KEY, EDITORS_KEY, RESTRICT_KEY, "ops_alert_emails", "ops_refresh_cron", "ops_refresh_timezone"}:
            continue
        settings[key] = value

    settings, _ = ensure_access_defaults(settings, bootstrap_user_email=user_email)
    store.write_json("dashboard_settings.json", settings)
    return jsonify({"ok": True, "access": get_access_context(settings, user_email=user_email)})


@app.route("/api/unit-economics")
def api_unit_economics():
    """Compute $/GWh and ft²/GWh per line using saved settings."""
    settings = store.read_json("dashboard_settings.json")
    if not isinstance(settings, dict):
        settings = {}
    raw_caps = settings.get("line_capacities", {})
    raw_sqft = settings.get("line_sqft", {})

    def _resolve_settings(raw: dict) -> dict:
        """Roll old CELL keys into their parent MOD line."""
        resolved: dict[str, float] = {}
        for key, val in raw.items():
            parent = _extract_line(key + "-ST0") if _re.match(r"BASE\d+-(CELL|MOD|INV)", key) else key
            if not parent:
                parent = key
            resolved[parent] = resolved.get(parent, 0) + float(val)
        return resolved

    line_caps = _resolve_settings(raw_caps)
    line_sqft = _resolve_settings(raw_sqft)

    by_station = _load_csv("capex_by_station.csv")
    if by_station.empty:
        return jsonify({"lines": [], "totals": {}})

    by_station["_mod"] = by_station["station_id"].apply(lambda s: _extract_line(s) or "Other")
    mod_agg = by_station.groupby("_mod").agg(
        forecasted=("forecasted_cost", lambda x: pd.to_numeric(x, errors="coerce").sum()),
        stations=("station_id", "count"),
    ).reset_index()

    lines = []
    total_spend = 0.0
    total_line_gwh = 0.0
    total_sqft = 0.0
    hub_caps: dict[str, dict[str, float]] = {}
    for _, row in mod_agg.iterrows():
        mod = row["_mod"]
        if mod == "Other":
            continue
        forecasted = float(row["forecasted"])
        spend = forecasted
        gwh = float(line_caps.get(mod, 0))
        sqft = float(line_sqft.get(mod, 0))
        entry = {
            "line": mod,
            "actual_spend": spend,
            "forecasted": forecasted,
            "gwh": gwh,
            "sqft": sqft,
            "dollar_per_gwh": spend / gwh if gwh > 0 else None,
            "forecast_per_gwh": forecasted / gwh if gwh > 0 else None,
            "sqft_per_gwh": sqft / gwh if gwh > 0 else None,
            "station_count": int(row["stations"]),
        }
        lines.append(entry)
        total_spend += spend
        total_line_gwh += gwh
        total_sqft += sqft

        # Hub capacity is computed per BASE as max(sum(MOD/CELL), sum(INV)).
        m = _re.match(r"(BASE\d+)-(MOD\d+|CELL\d+|INV\d+)", mod)
        if m:
            base, unit = m.group(1), m.group(2)
            if base not in hub_caps:
                hub_caps[base] = {"mod_gwh": 0.0, "inv_gwh": 0.0}
            if unit.startswith("INV"):
                hub_caps[base]["inv_gwh"] += gwh
            else:
                hub_caps[base]["mod_gwh"] += gwh

    total_hub_gwh = 0.0
    hubs = []
    for base in sorted(hub_caps.keys()):
        mod_gwh = hub_caps[base]["mod_gwh"]
        inv_gwh = hub_caps[base]["inv_gwh"]
        hub_gwh = max(mod_gwh, inv_gwh)
        total_hub_gwh += hub_gwh
        hubs.append({
            "hub": base,
            "mod_gwh": mod_gwh,
            "inv_gwh": inv_gwh,
            "hub_gwh": hub_gwh,
        })

    totals = {
        "total_spend": total_spend,
        "total_gwh": total_hub_gwh,
        "total_line_gwh": total_line_gwh,
        "total_sqft": total_sqft,
        "avg_dollar_per_gwh": total_spend / total_hub_gwh if total_hub_gwh > 0 else None,
        "avg_sqft_per_gwh": total_sqft / total_hub_gwh if total_hub_gwh > 0 else None,
        "spend_basis": "forecasted",
        "capacity_method": "sum(max(sum_mod_gwh, sum_inv_gwh)) by BASE hub",
    }

    return jsonify({"lines": lines, "totals": totals, "hubs": hubs})


ASSET_MILESTONES = ["ordered", "shipped", "received", "installed", "commissioned"]


def _derive_status(dates: dict) -> str:
    """Derive station status from the latest milestone with a date."""
    for ms in reversed(ASSET_MILESTONES):
        if dates.get(ms):
            return ms.capitalize()
    return "Ordered"


@app.route("/api/asset-status", methods=["GET"])
def api_asset_status_get():
    data = store.read_json("asset_status.json")
    if not isinstance(data, dict):
        data = {}
    return jsonify(data)


@app.route("/api/asset-status", methods=["POST"])
def api_asset_status_save():
    body = request.get_json(force=True)
    station_id = body.get("station_id", "")
    milestone = body.get("milestone", "")
    date_val = body.get("date", "")
    if not station_id or milestone not in ASSET_MILESTONES:
        return jsonify({"ok": False, "error": "station_id and valid milestone required"}), 400

    data = store.read_json("asset_status.json")
    if not isinstance(data, dict):
        data = {}
    if station_id not in data:
        data[station_id] = {}
    data[station_id][milestone] = date_val if date_val else None
    data[station_id]["status"] = _derive_status(data[station_id])
    store.write_json("asset_status.json", data)
    return jsonify({"ok": True, "status": data[station_id]["status"]})


ASSET_SUBCATEGORIES: set[str] = {
    "Process Equipment",
    "Controls & Electrical",
    "Mechanical & Structural",
    "Quality & Metrology",
}


@app.route("/api/assets")
def api_assets():
    """Station-level asset register with spend split by mfg_subcategory."""
    df = _load_csv("capex_clean.csv")
    stations_json = _load_stations_json()
    if df.empty:
        return jsonify({"stations": [], "kpis": {}})

    df = _apply_line_filter(df)
    df["_sub"] = pd.to_numeric(df["price_subtotal"], errors="coerce").fillna(0)
    df["_total"] = pd.to_numeric(df["price_total"], errors="coerce").fillna(0)
    df["_qty"] = pd.to_numeric(df["product_qty"], errors="coerce").fillna(0)
    df["_qty_recv"] = pd.to_numeric(df["qty_received"], errors="coerce").fillna(0)

    mapped = df[df["station_id"].str.startswith("BASE", na=False)].copy()
    if mapped.empty:
        return jsonify({"stations": [], "kpis": {}})

    has_subcat = "mfg_subcategory" in mapped.columns
    if has_subcat:
        mapped["_is_asset"] = mapped["mfg_subcategory"].isin(ASSET_SUBCATEGORIES)
    else:
        mapped["_is_asset"] = True

    station_meta = {s["station_id"]: s for s in stations_json}

    groups = mapped.groupby("station_id")
    rows: list[dict] = []
    for sid, grp in groups:
        meta = station_meta.get(sid, {})
        asset_grp = grp[grp["_is_asset"]] if has_subcat else grp
        svc_grp = grp[grp["mfg_subcategory"].isin({
            "Design & Engineering Services", "Integration & Commissioning",
        })] if has_subcat else pd.DataFrame()
        ship_grp = grp[grp["mfg_subcategory"] == "Shipping & Freight"] if has_subcat else pd.DataFrame()
        consum_grp = grp[grp["mfg_subcategory"] == "Consumables"] if has_subcat else pd.DataFrame()

        total_ordered_value = float((grp["_qty"] * pd.to_numeric(grp["price_unit"], errors="coerce").fillna(0)).sum())
        total_received_value = float((grp["_qty_recv"] * pd.to_numeric(grp["price_unit"], errors="coerce").fillna(0)).sum())
        pct_recv = (total_received_value / total_ordered_value * 100) if total_ordered_value > 0 else 0.0
        if pct_recv > 100:
            pct_recv = 100.0

        if pct_recv >= 99:
            delivery = "Complete"
        elif pct_recv > 0:
            delivery = "In Progress"
        else:
            delivery = "Not Started"

        forecasted = float(meta.get("forecasted_cost", 0) or 0)
        actual = float(grp["_sub"].sum())
        asset_val = float(asset_grp["_sub"].sum()) if not asset_grp.empty else 0.0
        variance = actual - forecasted

        conf_mode = ""
        if "mapping_confidence" in grp.columns:
            conf_counts = grp["mapping_confidence"].value_counts()
            conf_mode = str(conf_counts.index[0]) if not conf_counts.empty else ""

        # Sub-category breakdown for this station
        sc_breakdown: list[dict] = []
        if has_subcat:
            for sc, sc_grp in grp.groupby("mfg_subcategory"):
                if sc:
                    sc_breakdown.append({"subcategory": str(sc), "spend": float(sc_grp["_sub"].sum())})
            sc_breakdown.sort(key=lambda x: x["spend"], reverse=True)

        rows.append({
            "station_id": sid,
            "station_name": meta.get("process_name", ""),
            "line": _extract_line(sid),
            "owner": meta.get("owner", ""),
            "primary_vendor": meta.get("vendor", ""),
            "forecasted": forecasted,
            "total_investment": actual,
            "asset_value": asset_val,
            "services_cost": float(svc_grp["_sub"].sum()) if not svc_grp.empty else 0.0,
            "consumables_cost": float(consum_grp["_sub"].sum()) if not consum_grp.empty else 0.0,
            "shipping_cost": float(ship_grp["_sub"].sum()) if not ship_grp.empty else 0.0,
            "variance": variance,
            "variance_pct": round(variance / forecasted * 100, 1) if forecasted else 0.0,
            "po_count": int(grp["po_number"].nunique()),
            "line_count": len(grp),
            "vendor_count": int(grp["vendor_name"].nunique()),
            "pct_received": round(pct_recv, 1),
            "delivery_status": delivery,
            "odoo_spend": float(grp.loc[grp["source"] == "odoo", "_sub"].sum()),
            "ramp_spend": float(grp.loc[grp["source"] == "ramp", "_sub"].sum()),
            "mapping_confidence": conf_mode,
            "sc_breakdown": sc_breakdown,
        })

    rows.sort(key=lambda r: r["total_investment"], reverse=True)

    status_data = store.read_json("asset_status.json")
    if not isinstance(status_data, dict):
        status_data = {}
    for row in rows:
        sid = row["station_id"]
        sd = status_data.get(sid, {})
        row["status"] = sd.get("status", "Ordered")
        for ms in ASSET_MILESTONES:
            row[f"date_{ms}"] = sd.get(ms) or ""

    total_asset_value = sum(r["asset_value"] for r in rows)
    total_investment = sum(r["total_investment"] for r in rows)
    status_counts: dict[str, int] = {}
    for r in rows:
        s = r["status"]
        status_counts[s] = status_counts.get(s, 0) + 1

    kpis = {
        "station_count": len(rows),
        "total_asset_value": total_asset_value,
        "total_investment": total_investment,
        "services_total": sum(r["services_cost"] for r in rows),
        "status_counts": status_counts,
    }

    return jsonify({"stations": rows, "kpis": kpis})


@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


# ---------------------------------------------------------------------------
# V2 pages: always registered when capex_v2_pages module is present.
# The /api/v2/* endpoints are additive -- they don't affect existing routes.
# APP_MODE=dashboard_v2 is only needed for Cloud Run deployment distinction.
# ---------------------------------------------------------------------------
try:
    from capex_v2_pages import register_v2_routes
    register_v2_routes(app)
    _V2_ENABLED = True
except ImportError:
    _V2_ENABLED = False


if __name__ == "__main__":
    print("Mfg Budgeting App: http://localhost:5050")
    if _V2_ENABLED:
        print("  V2 pages enabled (/api/v2/*)")
    app.run(host="0.0.0.0", port=5050, debug=True)
