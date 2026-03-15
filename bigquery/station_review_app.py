"""
Station Review UI -- Flask app for human verification of agent-proposed station mappings.

Run: python station_review_app.py
Open: http://localhost:5051
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import pandas as pd
from flask import Flask, jsonify, render_template_string, request

import storage_backend as store
from auth import init_auth

app = Flask(__name__)
init_auth(app)


# ---------------------------------------------------------------------------
# Data helpers -- delegates to storage_backend (local or GCS)
# ---------------------------------------------------------------------------

def _load_stations() -> list[dict]:
    data = store.read_json("bf1_stations.json")
    return data.get("stations", []) if isinstance(data, dict) else []


def _load_overrides() -> dict:
    data = store.read_json("station_overrides.json")
    return data if isinstance(data, dict) else {}


def _save_overrides(overrides: dict) -> None:
    store.write_json("station_overrides.json", overrides)


def _load_data() -> pd.DataFrame:
    return store.read_csv("capex_clean.csv")


MANUAL_REQUIRED_FIELDS: tuple[str, ...] = (
    "po_number",
    "date_order",
    "vendor_name",
    "item_description",
    "price_subtotal",
)

MANUAL_PROJECT_CODES: list[str] = [
    "BF1-NPI & Pilot Equipment",
    "BF1-Prototype R&D Lines",
    "BF1-Quality Equipment",
    "BF1-Facilities and Infrastructure",
    "BF1-Manufacturing IT Systems",
    "BF1-Warehousing and Material Handling",
    "BF1-Maintenance and Spares",
    "BF1-Module Line 1",
    "BF1-Module Line 2",
    "BF1-Inverter Line 1",
    "BF1-Other Allocation",
]

MANUAL_SUBCATEGORY_OPTIONS: list[str] = [
    "Process Equipment",
    "Controls & Electrical",
    "Mechanical & Structural",
    "Consumables",
    "MFG Tools & Shop Supplies",
    "Design & Engineering Services",
    "Integration & Commissioning",
    "Quality & Metrology",
    "Software & Licenses",
    "Shipping & Freight",
    "Facilities & Office",
    "IT Equipment",
    "General & Administrative",
]

MANUAL_DEFAULT_COLUMNS: list[str] = [
    "source", "po_number", "date_order", "po_state", "po_invoice_status", "po_receipt_status",
    "vendor_name", "vendor_ref",
    "product_category", "item_description", "is_capex",
    "station_id", "station_name", "mapping_confidence", "mapping_reason", "mapping_status",
    "mfg_subcategory", "subcat_confidence", "subcat_reason", "is_mfg",
    "product_id", "product_qty", "qty_received", "product_uom",
    "price_unit", "price_subtotal", "price_tax", "price_total",
    "bill_count", "bill_amount_total", "bill_amount_paid", "bill_amount_open", "bill_payment_status",
    "project_name", "created_by_name",
    "po_amount_total", "po_notes", "part_numbers", "line_id",
    "po_id", "vendor_partner_id", "vendor_email", "line_sequence", "line_description",
    "line_date_planned", "responsible_user_id", "created_by_user_id", "po_amount_untaxed",
    "po_amount_tax", "incoterm_id", "ramp_card", "ramp_department", "ramp_location",
    "ramp_category", "ramp_merchant", "line_type",
]


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null", "undefined"}:
        return ""
    return text


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = _to_str(value)
        if text == "":
            return default
        return float(text)
    except (TypeError, ValueError):
        return default


def _to_date_yyyy_mm_dd(value: Any) -> str:
    text = _to_str(value)
    if text == "":
        return ""
    ts = pd.to_datetime(text, errors="coerce")
    if pd.isna(ts):
        return ""
    return ts.strftime("%Y-%m-%d")


def _csv_safe(df: pd.DataFrame) -> pd.DataFrame:
    return df.astype("object").where(pd.notna(df), "")


def _manual_line_id(payload: dict[str, Any]) -> str:
    stable_key = "|".join(
        [
            _to_str(payload.get("po_number")).lower(),
            _to_date_yyyy_mm_dd(payload.get("date_order")),
            _to_str(payload.get("vendor_name")).lower(),
            _to_str(payload.get("item_description")).lower(),
            f"{_to_float(payload.get('price_subtotal')):.2f}",
        ]
    )
    digest = hashlib.md5(stable_key.encode("utf-8")).hexdigest()[:12].upper()
    return f"MANUAL-{digest}"


def _station_name_for(station_id: str) -> str:
    if not station_id:
        return ""
    stations = _load_stations()
    match = next((s for s in stations if _to_str(s.get("station_id")) == station_id), None)
    return _to_str(match.get("process_name")) if match else ""


def _manual_po_defaults(payload: dict[str, Any], line_id: str) -> dict[str, Any]:
    station_id = _to_str(payload.get("station_id"))
    station_name = _to_str(payload.get("station_name")) or _station_name_for(station_id)
    subtotal = _to_float(payload.get("price_subtotal"), 0.0)
    qty = _to_float(payload.get("product_qty"), 1.0)
    if qty <= 0:
        qty = 1.0
    unit = _to_float(payload.get("price_unit"), subtotal / qty if qty else subtotal)
    total = _to_float(payload.get("price_total"), subtotal)
    tax = _to_float(payload.get("price_tax"), max(total - subtotal, 0.0))
    mapping_status = _to_str(payload.get("mapping_status")) or "confirmed"

    row: dict[str, Any] = {
        "source": "manual",
        "po_number": _to_str(payload.get("po_number")),
        "date_order": _to_date_yyyy_mm_dd(payload.get("date_order")),
        "po_state": _to_str(payload.get("po_state")) or "purchase",
        "po_invoice_status": _to_str(payload.get("po_invoice_status")),
        "po_receipt_status": _to_str(payload.get("po_receipt_status")),
        "vendor_name": _to_str(payload.get("vendor_name")),
        "vendor_ref": _to_str(payload.get("vendor_ref")),
        "product_category": _to_str(payload.get("product_category")),
        "item_description": _to_str(payload.get("item_description")),
        "is_capex": True,
        "station_id": station_id,
        "station_name": station_name,
        "mapping_confidence": "manual",
        "mapping_reason": _to_str(payload.get("mapping_reason")) or "manual PO entry",
        "mapping_status": mapping_status,
        "mfg_subcategory": _to_str(payload.get("mfg_subcategory")),
        "subcat_confidence": _to_str(payload.get("subcat_confidence")),
        "subcat_reason": _to_str(payload.get("subcat_reason")),
        "is_mfg": True,
        "product_id": _to_str(payload.get("product_id")),
        "product_qty": qty,
        "qty_received": _to_float(payload.get("qty_received"), 0.0),
        "product_uom": _to_str(payload.get("product_uom")) or "Units",
        "price_unit": unit,
        "price_subtotal": subtotal,
        "price_tax": tax,
        "price_total": total if total else subtotal,
        "bill_count": _to_float(payload.get("bill_count"), 0.0),
        "bill_amount_total": _to_float(payload.get("bill_amount_total"), 0.0),
        "bill_amount_paid": _to_float(payload.get("bill_amount_paid"), 0.0),
        "bill_amount_open": _to_float(payload.get("bill_amount_open"), 0.0),
        "bill_payment_status": _to_str(payload.get("bill_payment_status")),
        "project_name": _to_str(payload.get("project_name")),
        "created_by_name": _to_str(payload.get("created_by_name")) or "manual_entry",
        "po_amount_total": _to_float(payload.get("po_amount_total"), subtotal),
        "po_notes": _to_str(payload.get("po_notes")),
        "part_numbers": _to_str(payload.get("part_numbers")) or "[]",
        "line_id": line_id,
        "po_id": _to_str(payload.get("po_id")),
        "vendor_partner_id": _to_str(payload.get("vendor_partner_id")),
        "vendor_email": _to_str(payload.get("vendor_email")),
        "line_sequence": _to_str(payload.get("line_sequence")),
        "line_description": _to_str(payload.get("line_description")) or _to_str(payload.get("item_description")),
        "line_date_planned": _to_date_yyyy_mm_dd(payload.get("line_date_planned")),
        "responsible_user_id": _to_str(payload.get("responsible_user_id")),
        "created_by_user_id": _to_str(payload.get("created_by_user_id")),
        "po_amount_untaxed": _to_float(payload.get("po_amount_untaxed"), subtotal),
        "po_amount_tax": _to_float(payload.get("po_amount_tax"), tax),
        "incoterm_id": _to_str(payload.get("incoterm_id")),
        "ramp_card": "",
        "ramp_department": "",
        "ramp_location": "",
        "ramp_category": "",
        "ramp_merchant": "",
        "line_type": "spend",
    }
    return row


def _validate_manual_payload(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in MANUAL_REQUIRED_FIELDS:
        if _to_str(payload.get(field)) == "":
            errors.append(f"{field} is required")

    if _to_date_yyyy_mm_dd(payload.get("date_order")) == "":
        errors.append("date_order must be a valid date")

    subtotal_text = _to_str(payload.get("price_subtotal"))
    try:
        float(subtotal_text)
    except (TypeError, ValueError):
        errors.append("price_subtotal must be a valid number")

    project_name = _to_str(payload.get("project_name"))
    if project_name and project_name not in MANUAL_PROJECT_CODES:
        errors.append("project_name must be one of the supported manufacturing projects")

    subcat = _to_str(payload.get("mfg_subcategory"))
    if subcat and subcat not in MANUAL_SUBCATEGORY_OPTIONS:
        errors.append("mfg_subcategory must be one of the supported sub-categories")

    return errors


def _upsert_manual_po(payload: dict[str, Any], line_id: str | None = None) -> dict[str, Any]:
    df = _load_data()
    if df.empty:
        df = pd.DataFrame(columns=MANUAL_DEFAULT_COLUMNS)

    target_line_id = _to_str(line_id) or _to_str(payload.get("line_id")) or _manual_line_id(payload)

    mask = pd.Series([False] * len(df))
    if "line_id" in df.columns and "source" in df.columns:
        mask = (df["line_id"].astype(str) == target_line_id) & (df["source"].astype(str) == "manual")

    if line_id and not bool(mask.any()):
        raise KeyError("manual row not found")

    existing: dict[str, Any] = {}
    if bool(mask.any()):
        existing = df.loc[mask].iloc[0].to_dict()

    row = _manual_po_defaults(payload, target_line_id)
    merged = {**existing, **row}

    for col in merged.keys():
        if col not in df.columns:
            df[col] = ""

    if bool(mask.any()):
        idx = df.index[mask][0]
        for col, val in merged.items():
            df.at[idx, col] = val
    else:
        df = pd.concat([df, pd.DataFrame([merged])], ignore_index=True)

    dest_cols = list(dict.fromkeys(MANUAL_DEFAULT_COLUMNS + list(df.columns)))
    for col in dest_cols:
        if col not in df.columns:
            df[col] = ""
    df = df[dest_cols]
    store.write_csv("capex_clean.csv", _csv_safe(df))
    return merged


def _update_manual_po_subcategory(line_id: str, mfg_subcategory: str) -> dict[str, Any]:
    line_id = _to_str(line_id)
    if not line_id:
        raise KeyError("manual row not found")

    normalized = _to_str(mfg_subcategory)
    if normalized and normalized not in MANUAL_SUBCATEGORY_OPTIONS:
        raise ValueError("mfg_subcategory must be one of the supported sub-categories")

    df = _load_data()
    if df.empty or "line_id" not in df.columns or "source" not in df.columns:
        raise KeyError("manual row not found")

    mask = (df["line_id"].astype(str) == line_id) & (df["source"].astype(str) == "manual")
    if not bool(mask.any()):
        raise KeyError("manual row not found")

    if "mfg_subcategory" not in df.columns:
        df["mfg_subcategory"] = ""
    if "subcat_confidence" not in df.columns:
        df["subcat_confidence"] = ""
    if "subcat_reason" not in df.columns:
        df["subcat_reason"] = ""

    idx = df.index[mask][0]
    df.at[idx, "mfg_subcategory"] = normalized
    df.at[idx, "subcat_confidence"] = "manual" if normalized else ""
    df.at[idx, "subcat_reason"] = "manual review edit" if normalized else ""
    store.write_csv("capex_clean.csv", _csv_safe(df))
    return _csv_safe(df.loc[[idx]]).to_dict(orient="records")[0]


def _delete_manual_po(line_id: str) -> bool:
    line_id = _to_str(line_id)
    if not line_id:
        return False
    df = _load_data()
    if df.empty or "line_id" not in df.columns or "source" not in df.columns:
        return False

    mask = (df["line_id"].astype(str) == line_id) & (df["source"].astype(str) == "manual")
    if not bool(mask.any()):
        return False

    df = df.loc[~mask].copy()
    store.write_csv("capex_clean.csv", _csv_safe(df))

    overrides = _load_overrides()
    if line_id in overrides:
        overrides.pop(line_id, None)
        _save_overrides(overrides)
    return True


def _list_manual_po_rows() -> list[dict[str, Any]]:
    df = _load_data()
    if df.empty or "source" not in df.columns:
        return []
    manual_df = df[df["source"].astype(str) == "manual"].copy()
    if manual_df.empty:
        return []
    if "date_order" in manual_df.columns:
        manual_df = manual_df.sort_values(["date_order", "po_number"], ascending=[False, True], na_position="last")
    return _csv_safe(manual_df).to_dict(orient="records")


# ---------------------------------------------------------------------------
# HTML Template
# ---------------------------------------------------------------------------

TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mfg Budgeting - Station Review</title>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.html5.min.js"></script>
<link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.4.2/css/buttons.dataTables.min.css">
<style>
:root{--bg:#1A1A1A;--surface:#242422;--surface2:#32312F;--text:#F0EEEB;--muted:#9E9C98;--accent:#B2DD79;--accent-dark:#1A1A1A;--green:#B2DD79;--green-bright:#D0F585;--yellow:#F7C33C;--red:#D1531D;--blue:#048EE5;--border:#3E3D3A;--disabled:#32312F}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--text)}
.header{background:var(--surface);padding:16px 24px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}
.header h1{font-size:18px;font-weight:700;color:var(--green);letter-spacing:.3px}
.header .sub{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px}
.tabs{display:flex;gap:8px;padding:12px 24px;background:var(--surface);border-bottom:1px solid var(--border);flex-wrap:wrap;align-items:center}
.tab{padding:8px 16px;border-radius:6px;cursor:pointer;font-size:13px;color:var(--muted);background:transparent;border:1px solid transparent;transition:all .15s;font-weight:600}
.tab:hover{background:var(--surface2);color:var(--text)}
.tab.active{background:var(--green);color:var(--accent-dark);border-color:var(--green)}
.tab .badge{background:var(--surface2);color:var(--text);padding:2px 8px;border-radius:10px;font-size:11px;margin-left:6px}
.tab.active .badge{background:rgba(26,26,26,.3);color:var(--accent-dark)}
.filter-sep{width:1px;height:28px;background:var(--border);margin:0 8px}
.station-filter{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 12px;font-size:12px;min-width:220px;outline:none}
.station-filter:focus{border-color:var(--green)}
.content{padding:24px;max-width:1400px;margin:0 auto}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:20px}
.stat-card{background:var(--surface);border-radius:10px;padding:16px 20px;border:1px solid var(--border)}
.stat-card .label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;font-weight:600}
.stat-card .value{font-size:22px;font-weight:700;margin-top:4px;font-variant-numeric:tabular-nums}
.po-group{background:var(--surface);border-radius:10px;margin-bottom:14px;overflow:hidden;border:1px solid var(--border)}
.po-header{padding:12px 16px;display:flex;justify-content:space-between;align-items:center;cursor:pointer;background:var(--surface2);transition:background .15s}
.po-header:hover{background:var(--border)}
.po-header .po-title{font-weight:700;font-size:14px;color:var(--green)}
.po-header .po-meta{font-size:11px;color:var(--muted)}
.po-lines{padding:0}
.line-row{display:grid;grid-template-columns:1fr 200px 100px 90px 140px;gap:10px;padding:10px 16px;border-top:1px solid rgba(62,61,58,.4);align-items:center;font-size:12px}
.line-row:hover{background:rgba(178,221,121,.03)}
.line-desc{overflow:hidden;text-overflow:ellipsis}
.line-desc .desc-text{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:block}
.line-desc .desc-meta{font-size:10px;color:var(--muted);margin-top:2px}
.conf-badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;text-transform:uppercase}
.conf-high{background:rgba(178,221,121,.15);color:var(--green)}
.conf-confirmed{background:rgba(178,221,121,.15);color:var(--green)}
.conf-medium{background:rgba(247,195,60,.15);color:var(--yellow)}
.conf-low{background:rgba(209,83,29,.15);color:var(--red)}
.conf-none{background:rgba(158,156,152,.1);color:var(--muted)}
.conf-skip{background:rgba(158,156,152,.1);color:var(--muted)}
.conf-non_prod{background:rgba(209,83,29,.15);color:var(--red)}
.conf-pilot_npi{background:rgba(247,195,60,.15);color:var(--yellow)}
select{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:6px 8px;font-size:11px;width:100%;outline:none}
select:focus{border-color:var(--green)}
.btn-group{display:flex;gap:4px;flex-wrap:wrap}
.btn{padding:4px 10px;border-radius:4px;border:none;cursor:pointer;font-size:10px;font-weight:700;transition:all .15s}
.btn-confirm{background:var(--green);color:var(--accent-dark)}
.btn-skip{background:var(--surface2);color:var(--muted)}
.btn-nonp{background:var(--red);color:#fff}
.btn-pilot{background:var(--yellow);color:var(--accent-dark)}
.btn-reclass{background:var(--blue);color:#fff}
.btn:hover{opacity:.85;transform:translateY(-1px)}
.btn-reexport{background:var(--green);color:var(--accent-dark);padding:8px 20px;font-size:12px;font-weight:700;border:none;border-radius:6px;cursor:pointer;letter-spacing:.3px}
.btn-reexport:hover{background:var(--green-bright)}
.toast{position:fixed;bottom:24px;right:24px;background:var(--green);color:var(--accent-dark);padding:12px 24px;border-radius:8px;font-weight:700;font-size:13px;display:none;z-index:999;box-shadow:0 4px 20px rgba(0,0,0,.4)}
.search-bar{width:100%;padding:10px 16px;background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:13px;margin-bottom:16px;outline:none}
.search-bar:focus{border-color:var(--green)}
.empty-state{text-align:center;padding:60px 20px;color:var(--muted);font-size:14px}
.hidden{display:none}
.po-actions{display:flex;gap:8px;padding:10px 16px;align-items:center;background:var(--surface);border-top:1px solid var(--border);flex-wrap:wrap}
.po-station-sel{min-width:220px;flex-shrink:0}
.btn-split{background:transparent;color:var(--green);border:1px solid var(--green);font-weight:700}
.source-badge{display:inline-block;padding:1px 6px;border-radius:3px;font-size:9px;font-weight:700;text-transform:uppercase;margin-right:4px}
.source-badge.odoo{background:rgba(178,221,121,.12);color:var(--green)}
.source-badge.ramp{background:rgba(4,142,229,.12);color:var(--blue)}
.source-badge.manual{background:rgba(247,195,60,.15);color:var(--yellow)}
table.dataTable{color:var(--text)!important;background:var(--surface)!important;border-collapse:collapse!important;width:100%!important;font-size:12px!important;table-layout:auto!important}
table.dataTable thead th{background:var(--surface2)!important;color:var(--muted)!important;border-bottom:1px solid var(--border)!important;font-size:11px!important;padding:8px 6px!important;text-transform:uppercase;letter-spacing:.3px;font-weight:600;overflow:hidden;min-width:50px}
table.dataTable tbody td{border-bottom:1px solid rgba(62,61,58,.4)!important;padding:6px!important;vertical-align:middle;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
table.dataTable tbody tr:hover{background:rgba(178,221,121,.05)!important}
table.dataTable tfoot th{padding:4px!important;background:var(--surface2)!important}
table.dataTable tfoot input{width:100%;padding:4px 6px;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:3px;font-size:10px;outline:none}
table.dataTable tfoot input:focus{border-color:var(--green)}
.dataTables_wrapper .dataTables_filter input{background:var(--surface2)!important;color:var(--text)!important;border:1px solid var(--border)!important;border-radius:6px;padding:6px 10px}
.dataTables_wrapper .dataTables_length select{background:var(--surface2)!important;color:var(--text)!important;border:1px solid var(--border)!important}
.dataTables_wrapper .dataTables_info,.dataTables_wrapper .dataTables_paginate{color:var(--muted)!important;font-size:11px!important}
.dataTables_wrapper .dataTables_paginate .paginate_button{color:var(--muted)!important}
.dataTables_wrapper .dataTables_paginate .paginate_button.current{background:var(--green)!important;color:var(--accent-dark)!important;border:none!important;border-radius:4px;font-weight:700}
.dataTables_wrapper .dataTables_paginate .paginate_button:hover{background:var(--surface2)!important;color:var(--text)!important}
dt.buttons-csv{background:var(--green)!important;color:var(--accent-dark)!important;border:none!important;font-weight:600!important;border-radius:4px!important}
.tbl-select{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:3px;padding:3px 4px;font-size:10px;width:100%;outline:none}
.tbl-select:focus{border-color:var(--green)}
.tbl-btn{padding:3px 8px;border-radius:3px;border:none;cursor:pointer;font-size:10px;font-weight:700}
.tbl-btn-confirm{background:var(--green);color:var(--accent-dark)}
.tbl-btn-skip{background:var(--surface2);color:var(--muted)}
.tbl-btn:hover{opacity:.85}
.tbl-saved{color:var(--green);font-size:10px;font-weight:600;display:none}
#tableViewWrap{padding:0 24px 24px}
.dt-resizable thead th{position:relative;overflow:visible!important}
.col-resizer{position:absolute;top:0;right:0;width:14px;height:100%;cursor:col-resize;z-index:3;touch-action:none}
.col-resizer:hover{background:rgba(178,221,121,.18)}
body.col-resize-active{cursor:col-resize;user-select:none}
.manual-wrap{padding:24px;max-width:1400px;margin:0 auto}
.manual-grid{display:grid;grid-template-columns:repeat(4,minmax(180px,1fr));gap:10px}
.manual-field{display:flex;flex-direction:column;gap:6px}
.manual-field label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px}
.manual-field input,.manual-field textarea,.manual-field select{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:8px 10px;font-size:12px;outline:none}
.manual-field textarea{min-height:64px;resize:vertical}
.manual-field input:focus,.manual-field textarea:focus,.manual-field select:focus{border-color:var(--green)}
.manual-actions{display:flex;gap:8px;margin-top:12px}
.manual-btn{padding:8px 14px;border:none;border-radius:6px;cursor:pointer;font-size:12px;font-weight:700}
.manual-btn.save{background:var(--green);color:var(--accent-dark)}
.manual-btn.clear{background:var(--surface2);color:var(--text)}
.manual-list{margin-top:16px;background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:auto}
.manual-table{width:100%;border-collapse:collapse;font-size:12px}
.manual-table th{background:var(--surface2);color:var(--muted);padding:8px 10px;text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:.4px}
.manual-table td{padding:8px 10px;border-top:1px solid rgba(62,61,58,.4);white-space:nowrap}
.manual-tag{font-size:10px;color:var(--green);font-weight:700}
</style>
</head>
<body>

<div class="header">
    <div><h1>MFG BUDGETING - STATION REVIEW</h1><div class="sub">Base Power Company</div></div>
    <button class="btn-reexport" onclick="reExport()">Re-export Pipeline</button>
</div>

<div class="tabs">
    <div class="tab active" data-tab="needs_review" onclick="switchTab(this)">Needs Review <span class="badge" id="badge-needs_review">0</span></div>
    <div class="tab" data-tab="agent_proposals" onclick="switchTab(this)">Agent Proposals <span class="badge" id="badge-agent_proposals">0</span></div>
    <div class="tab" data-tab="verified" onclick="switchTab(this)">Verified <span class="badge" id="badge-verified">0</span></div>
    <div class="tab" data-tab="table_view" onclick="switchTab(this)">Table View <span class="badge" id="badge-table_view">All</span></div>
    <div class="tab" data-tab="manual_po" onclick="switchTab(this)">Manual PO Entry</div>
    <div class="filter-sep"></div>
    <select class="station-filter" id="stationFilter" onchange="render()">
        <option value="">All Stations / Assignments</option>
    </select>
    <select class="station-filter" id="sourceFilter" onchange="render()" style="min-width:120px">
        <option value="">All Sources</option>
        <option value="odoo">Odoo PO</option>
        <option value="ramp">Ramp CC</option>
        <option value="manual">Manual PO</option>
    </select>
</div>

<div class="content">
    <div class="stats" id="stats"></div>
    <input type="text" class="search-bar" id="searchBar" placeholder="Search PO, vendor, description, project..." oninput="render()">
    <div id="poGroups"></div>
</div>

<div id="tableViewWrap" style="display:none">
    <div class="stats" id="tbl-stats" style="padding:24px 0 12px"></div>
    <div id="tbl-container"></div>
</div>

<div id="manualPoWrap" style="display:none">
    <div class="manual-wrap">
        <div class="stats" id="manual-stats" style="padding:0 0 12px"></div>
        <div class="po-group" style="padding:16px">
            <div class="manual-grid">
                <div class="manual-field"><label>PO Number *</label><input id="m-po-number" type="text" placeholder="PO-XXXX"></div>
                <div class="manual-field"><label>Date *</label><input id="m-date-order" type="date"></div>
                <div class="manual-field"><label>Vendor *</label><input id="m-vendor-name" type="text" placeholder="Vendor name"></div>
                <div class="manual-field"><label>Subtotal (USD) *</label><input id="m-price-subtotal" type="number" step="0.01" min="0" placeholder="0.00"></div>
                <div class="manual-field"><label>Project</label><select id="m-project-name"></select></div>
                <div class="manual-field"><label>Station</label><select id="m-station-id"></select></div>
                <div class="manual-field"><label>Mapping Status</label><select id="m-mapping-status"><option value="confirmed">confirmed</option><option value="non_prod">non_prod</option><option value="pilot_npi">pilot_npi</option><option value="skip">skip</option></select></div>
                <div class="manual-field"><label>Category</label><input id="m-product-category" type="text" placeholder="Product category"></div>
                <div class="manual-field"><label>Sub-Category</label><select id="m-mfg-subcategory"></select></div>
                <div class="manual-field" style="grid-column:1 / span 4"><label>Item Description *</label><textarea id="m-item-description" placeholder="Description"></textarea></div>
                <div class="manual-field" style="grid-column:1 / span 4"><label>Notes</label><textarea id="m-po-notes" placeholder="Optional notes"></textarea></div>
            </div>
            <input id="m-line-id" type="hidden">
            <div class="manual-actions">
                <button class="manual-btn save" onclick="saveManualPO()">Save Manual PO</button>
                <button class="manual-btn clear" onclick="clearManualForm()">Clear</button>
                <span class="manual-tag" id="manual-edit-tag"></span>
            </div>
        </div>
        <div class="manual-list">
            <table class="manual-table">
                <thead><tr><th>PO</th><th>Date</th><th>Vendor</th><th>Description</th><th>Subtotal</th><th>Project</th><th>Station</th><th>Sub-Category</th><th>Status</th><th>Actions</th></tr></thead>
                <tbody id="manual-list-body"></tbody>
            </table>
        </div>
    </div>
</div>

<div class="toast" id="toast">Saved!</div>

<script>
let DATA=[],STATIONS=[],OVERRIDES={},currentTab='needs_review',tblDT=null,projValues={},stationValues={},MANUAL_ROWS=[];
const MFG_PROJECT_CODES=[
    {id:'BF1-NPI & Pilot Equipment',label:'BF1-NPI & Pilot Equipment'},
    {id:'BF1-Prototype R&D Lines',label:'BF1-Prototype R&D Lines'},
    {id:'BF1-Quality Equipment',label:'BF1-Quality Equipment'},
    {id:'BF1-Facilities and Infrastructure',label:'BF1-Facilities and Infrastructure'},
    {id:'BF1-Manufacturing IT Systems',label:'BF1-Manufacturing IT Systems'},
    {id:'BF1-Warehousing and Material Handling',label:'BF1-Warehousing and Material Handling'},
    {id:'BF1-Maintenance and Spares',label:'BF1-Maintenance and Spares'},
    {id:'BF1-Module Line 1',label:'BF1-Module Line 1'},
    {id:'BF1-Module Line 2',label:'BF1-Module Line 2'},
    {id:'BF1-Inverter Line 1',label:'BF1-Inverter Line 1'},
    {id:'BF1-Other Allocation',label:'BF1-Other Allocation'},
];
const MFG_SUBCATEGORY_OPTIONS=[
    'Process Equipment',
    'Controls & Electrical',
    'Mechanical & Structural',
    'Consumables',
    'MFG Tools & Shop Supplies',
    'Design & Engineering Services',
    'Integration & Commissioning',
    'Quality & Metrology',
    'Software & Licenses',
    'Shipping & Freight',
    'Facilities & Office',
    'IT Equipment',
    'General & Administrative',
];

function fmt$(v){if(v==null||isNaN(v))return'$0';const a=Math.abs(v),s=v<0?'-':'';return s+'$'+a.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}
function fmtShort$(v){if(v==null||isNaN(v))return'$0';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e6)return s+'$'+(a/1e6).toFixed(2)+'M';if(a>=1e3)return s+'$'+(a/1e3).toFixed(1)+'K';return s+'$'+a.toFixed(0)}
function normalizeProjectName(v){
    if(v==null)return'';
    const s=String(v);
    if(!s||s.toLowerCase()==='nan'||s.toLowerCase()==='null'||s.toLowerCase()==='undefined')return'';
    return s;
}
function ensureSelectOption(sel,val){
    if(!sel)return;
    const safeVal=normalizeProjectName(val);
    if(!safeVal)return;
    const exists=[...sel.options].some(o=>o.value===safeVal);
    if(exists)return;
    const opt=document.createElement('option');
    opt.value=safeVal;
    opt.textContent=safeVal;
    sel.insertBefore(opt,sel.children[1]||null);
}
function subcatOptionsHtml(selected){
    const current=normalizeProjectName(selected);
    return '<option value="">--</option>'+MFG_SUBCATEGORY_OPTIONS.map(sc=>`<option value="${sc}" ${sc===current?'selected':''}>${sc}</option>`).join('');
}

async function init(){
    const [dataRes,stationsRes,overridesRes]=await Promise.all([fetch('/api/data'),fetch('/api/stations'),fetch('/api/overrides')]);
    DATA=await dataRes.json();STATIONS=await stationsRes.json();OVERRIDES=await overridesRes.json();
    buildStationFilter();
    initManualFormOptions();
    render();
}

function buildStationFilter(){
    const sel=document.getElementById('stationFilter');
    sel.innerHTML='<option value="">All Stations / Assignments</option>';
    const stations=new Set();
    DATA.forEach(r=>{
        const sid=r.station_id||'';
        const ov=getOverride(r.line_id);
        const assigned=(ov&&ov.station_id)?ov.station_id:sid;
        if(assigned)stations.add(assigned);
    });
    const sorted=[...stations].sort();
    sorted.forEach(s=>{const o=document.createElement('option');o.value=s;o.textContent=s;sel.appendChild(o);});
    const unOpt=document.createElement('option');unOpt.value='__unassigned__';unOpt.textContent='(Unassigned)';sel.appendChild(unOpt);
}

function switchTab(el){
    document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
    el.classList.add('active');currentTab=el.dataset.tab;
    if(currentTab==='table_view'){
        document.querySelector('.content').style.display='none';
        document.getElementById('tableViewWrap').style.display='block';
        document.getElementById('manualPoWrap').style.display='none';
        loadTableView();
    } else if(currentTab==='manual_po'){
        document.querySelector('.content').style.display='none';
        document.getElementById('tableViewWrap').style.display='none';
        document.getElementById('manualPoWrap').style.display='block';
        loadManualPOList();
    } else {
        document.querySelector('.content').style.display='block';
        document.getElementById('tableViewWrap').style.display='none';
        document.getElementById('manualPoWrap').style.display='none';
        render();
    }
}

function getOverride(lid){return OVERRIDES[String(lid)]||OVERRIDES[Number(lid)]||null;}

function getTabRows(){
    return DATA.filter(row=>{
        const lid=row.line_id||'';const ov=getOverride(lid);const conf=row.mapping_confidence||'none';
        if(row.line_type!=='spend')return false;
        if(currentTab==='verified')return ov!=null;
        if(currentTab==='needs_review')return !ov&&(conf==='none'||conf==='low');
        if(currentTab==='agent_proposals')return !ov&&(conf==='high'||conf==='medium');
        return false;
    });
}

function updateBadges(){
    const all=DATA.filter(r=>r.line_type==='spend');
    document.getElementById('badge-needs_review').textContent=all.filter(r=>!getOverride(r.line_id)&&(r.mapping_confidence==='none'||r.mapping_confidence==='low')).length;
    document.getElementById('badge-agent_proposals').textContent=all.filter(r=>!getOverride(r.line_id)&&(r.mapping_confidence==='high'||r.mapping_confidence==='medium')).length;
    document.getElementById('badge-verified').textContent=all.filter(r=>getOverride(r.line_id)).length;
}

function render(){
    let rows=getTabRows();
    const search=(document.getElementById('searchBar').value||'').toLowerCase();
    const stationF=document.getElementById('stationFilter').value;
    const sourceF=document.getElementById('sourceFilter').value;

    if(search)rows=rows.filter(r=>(r.po_number||'').toLowerCase().includes(search)||(r.vendor_name||'').toLowerCase().includes(search)||(r.item_description||'').toLowerCase().includes(search)||(r.project_name||'').toLowerCase().includes(search)||(r.station_id||'').toLowerCase().includes(search));
    if(sourceF)rows=rows.filter(r=>(r.source||'')===sourceF);
    if(stationF){
        if(stationF==='__unassigned__'){
            rows=rows.filter(r=>{const ov=getOverride(r.line_id);const sid=(ov&&ov.station_id)?ov.station_id:(r.station_id||'');return!sid;});
        } else {
            rows=rows.filter(r=>{const ov=getOverride(r.line_id);const sid=(ov&&ov.station_id)?ov.station_id:(r.station_id||'');return sid===stationF;});
        }
    }

    updateBadges();
    const totalSpend=rows.reduce((s,r)=>s+(parseFloat(r.price_subtotal)||0),0);
    document.getElementById('stats').innerHTML=`
        <div class="stat-card"><div class="label">Lines</div><div class="value">${rows.length.toLocaleString()}</div></div>
        <div class="stat-card"><div class="label">Total Spend</div><div class="value">${fmtShort$(totalSpend)}</div></div>
        <div class="stat-card"><div class="label">Unique POs</div><div class="value">${new Set(rows.map(r=>r.po_number)).size}</div></div>
        <div class="stat-card"><div class="label">Unique Vendors</div><div class="value">${new Set(rows.map(r=>r.vendor_name)).size}</div></div>`;

    const groups={};
    rows.forEach(r=>{const po=r.po_number||'Unknown';if(!groups[po])groups[po]={rows:[],total:0,vendor:r.vendor_name,date:r.date_order,source:r.source||''};groups[po].rows.push(r);groups[po].total+=parseFloat(r.price_subtotal)||0;});
    const sortedPOs=Object.entries(groups).sort((a,b)=>b[1].total-a[1].total);

    if(!sortedPOs.length){document.getElementById('poGroups').innerHTML='<div class="empty-state">No items match current filters.</div>';return;}

    const projOpts=MFG_PROJECT_CODES.map(p=>`<option value="${p.id}">${p.label}</option>`).join('');
    const stOpts='<optgroup label="Project Codes">'+projOpts+'</optgroup><optgroup label="BF1 Stations">'+STATIONS.map(s=>`<option value="${s.station_id}">${s.station_id} - ${s.process_name||''}</option>`).join('')+'</optgroup>';

    let html='';
    sortedPOs.forEach(([po,group])=>{
        const lids=group.rows.map(r=>r.line_id||'').join(',');
        const agentStation=group.rows[0].station_id||'';
        const project=group.rows[0].project_name||'';
        const srcBadge=group.source?`<span class="source-badge ${group.source}">${group.source}</span>`:'';

        html+=`<div class="po-group" data-po="${po}">
            <div class="po-header" onclick="toggleLines('${po}')">
                <div>${srcBadge}<span class="po-title">${po}</span>
                    <span class="po-meta">&nbsp;|&nbsp;${group.vendor}&nbsp;|&nbsp;${group.date}&nbsp;|&nbsp;${group.rows.length} lines</span>
                    <span class="po-meta" style="color:var(--green)">&nbsp;|&nbsp;${project}</span></div>
                <div style="font-weight:700;color:var(--green)">${fmtShort$(group.total)}</div>
            </div>
            <div class="po-actions" data-lids="${lids}">
                <select id="po-sel-${po}" class="po-station-sel"><option value="">-- assign entire PO --</option>${stOpts}</select>
                <script>document.getElementById('po-sel-${po}').value='${agentStation}';<\/script>
                <button class="btn btn-confirm" onclick="event.stopPropagation();savePOOverride('${po}','confirmed')">Confirm All</button>
                <button class="btn btn-skip" onclick="event.stopPropagation();savePOOverride('${po}','skip')">Skip All</button>
                <button class="btn btn-nonp" onclick="event.stopPropagation();savePOOverride('${po}','non_prod')">Non-Prod</button>
                <button class="btn btn-pilot" onclick="event.stopPropagation();savePOOverride('${po}','pilot_npi')">Pilot</button>
            </div>
            <div class="po-lines" id="lines-${po}">`;

        group.rows.forEach(r=>{
            const lid=r.line_id||'';const ov=getOverride(lid);
            const conf=ov?ov.status:(r.mapping_confidence||'none');
            const curStation=(ov&&ov.station_id)?ov.station_id:(r.station_id||'');
            html+=`<div class="line-row" data-lid="${lid}">
                <div class="line-desc">
                    <span class="desc-text" title="${(r.item_description||'').replace(/"/g,'&quot;')}">${r.item_description||r.line_description||'(no description)'}</span>
                    <span class="desc-meta">${r.product_category||''} | ${fmt$(parseFloat(r.price_subtotal||0))} | ${r.mapping_reason||''}</span></div>
                <div><select id="sel-${lid}"><option value="">-- station --</option>${stOpts}</select>
                    <script>document.getElementById('sel-${lid}').value='${curStation}';<\/script></div>
                <div><span class="conf-badge conf-${conf}">${conf}</span></div>
                <div style="font-weight:600;font-variant-numeric:tabular-nums">${fmt$(parseFloat(r.price_subtotal||0))}</div>
                <div class="btn-group">
                    <button class="btn btn-confirm" onclick="saveOverride('${lid}','confirmed')">Confirm</button>
                    <button class="btn btn-skip" onclick="saveOverride('${lid}','skip')">Skip</button>
                    <button class="btn btn-nonp" onclick="saveOverride('${lid}','non_prod')">Non-Prod</button>
                    <button class="btn btn-pilot" onclick="saveOverride('${lid}','pilot_npi')">Pilot</button>
                </div>
            </div>`;
        });
        html+='</div></div>';
    });
    document.getElementById('poGroups').innerHTML=html;
}

function toggleLines(po){const el=document.getElementById('lines-'+po);if(el)el.classList.toggle('hidden');}

async function savePOOverride(po,status){
    const sel=document.getElementById('po-sel-'+po);
    const poStationId=sel?sel.value:'';
    const poGroup=document.querySelector(`.po-group[data-po="${po}"]`);
    const actionsDiv=poGroup?poGroup.querySelector('.po-actions'):null;
    const lids=actionsDiv?actionsDiv.dataset.lids.split(','):[];
    if(!lids.length)return;

    const overridesToSend=[];
    for(const lid of lids){
        if(!lid)continue;
        const row=DATA.find(r=>String(r.line_id)===String(lid));
        const finalStation=poStationId||(row?(row.station_id||''):'');
        const stationMeta=STATIONS.find(s=>s.station_id===finalStation);
        const existPOOv=OVERRIDES[String(lid)]||{};
        OVERRIDES[String(lid)]={station_id:finalStation,status:status,project_name:existPOOv.project_name||''};
        overridesToSend.push({line_id:lid,station_id:finalStation,status:status,project_name:existPOOv.project_name||''});
        if(row){row.station_id=finalStation;if(stationMeta)row.station_name=stationMeta.process_name||'';row.mapping_confidence='confirmed';row.mapping_status=status;row.mapping_reason='human override (PO-level): '+status;}
    }

    await fetch('/api/override_batch',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({overrides:overridesToSend})});

    showToast('PO '+po+': '+lids.length+' lines → '+(poStationId||status));
    setTimeout(()=>render(),300);
}

async function saveOverride(lid,status){
    const sel=document.getElementById('sel-'+lid);
    const dropdownVal=sel?sel.value:'';
    const row=DATA.find(r=>String(r.line_id)===String(lid));
    const stationId=dropdownVal||(row?(row.station_id||''):'');

    const existLineOv=OVERRIDES[String(lid)]||{};
    OVERRIDES[String(lid)]={station_id:stationId,status:status,project_name:existLineOv.project_name||''};
    const stationMeta=STATIONS.find(s=>s.station_id===stationId);
    if(row){row.station_id=stationId;if(stationMeta)row.station_name=stationMeta.process_name||'';row.mapping_confidence='confirmed';row.mapping_status=status;row.mapping_reason='human override: '+status;}

    await fetch('/api/override',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({line_id:lid,station_id:stationId,status:status})});

    const lineEl=document.querySelector(`.line-row[data-lid="${lid}"]`);
    if(lineEl){
        lineEl.style.transition='opacity .3s';lineEl.style.opacity='.3';lineEl.style.pointerEvents='none';
        const badge=lineEl.querySelector('.conf-badge');
        if(badge){badge.className='conf-badge conf-'+status;badge.textContent=status;}
    }

    const poGroup=lineEl?lineEl.closest('.po-group'):null;
    if(poGroup&&currentTab!=='verified'){
        const allRows=poGroup.querySelectorAll('.line-row');
        const allDone=[...allRows].every(r=>getOverride(r.dataset.lid));
        if(allDone){poGroup.style.transition='opacity .4s';poGroup.style.opacity='.15';setTimeout(()=>{poGroup.style.display='none';updateBadges();},500);}
    }
    updateBadges();
    showToast('Saved: '+(stationId||status));
}

function loadTableView(){
    const allSpend=DATA.filter(r=>r.line_type==='spend');
    const totalSpend=allSpend.reduce((s,r)=>s+(parseFloat(r.price_subtotal)||0),0);
    document.getElementById('tbl-stats').innerHTML=`
        <div class="stat-card" style="display:inline-block;margin-right:12px"><div class="label">Total Lines</div><div class="value">${allSpend.length.toLocaleString()}</div></div>
        <div class="stat-card" style="display:inline-block;margin-right:12px"><div class="label">Total Spend</div><div class="value">${fmtShort$(totalSpend)}</div></div>
        <div class="stat-card" style="display:inline-block"><div class="label">Overrides</div><div class="value">${Object.keys(OVERRIDES).length}</div></div>`;

    const pOpts=MFG_PROJECT_CODES.map(p=>'<option value="'+p.id+'">'+p.label+'</option>').join('');
    const sOpts=STATIONS.map(s=>'<option value="'+s.station_id+'">'+s.station_id+'</option>').join('');
    const allOpts='<option value="">--</option><optgroup label="Projects">'+pOpts+'</optgroup><optgroup label="Stations">'+sOpts+'</optgroup>';
    const projDropOpts='<option value="">--</option>'+pOpts;

    projValues={};stationValues={};
    allSpend.forEach(r=>{
        const lid=r.line_id||'';const ov=getOverride(lid);
        projValues[lid]=normalizeProjectName((ov&&ov.project_name)?ov.project_name:(r.project_name||''));
        stationValues[lid]=(ov&&ov.station_id)?ov.station_id:(r.station_id||'');
    });

    let html='<table id="review-tbl" class="display compact" style="width:100%"><thead><tr><th>Source</th><th>PO</th><th>Date</th><th>Vendor</th><th>Description</th><th>Project</th><th>Sub-Category</th><th>Subtotal</th><th>Station</th><th>Confidence</th><th>Status</th><th>Assign</th><th>Action</th></tr></thead><tfoot><tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th></tr></tfoot><tbody>';
    allSpend.forEach(r=>{
        const lid=r.line_id||'';
        const ov=getOverride(lid);
        const curStation=(ov&&ov.station_id)?ov.station_id:(r.station_id||'');
        const status=ov?ov.status:(r.mapping_confidence||'none');
        const sub=parseFloat(r.price_subtotal)||0;
        html+=`<tr data-lid="${lid}">`;
        html+=`<td><span class="source-badge ${r.source||''}">${r.source||''}</span></td>`;
        html+=`<td>${r.po_number||''}</td>`;
        html+=`<td>${r.date_order||''}</td>`;
        html+=`<td>${r.vendor_name||''}</td>`;
        html+=`<td style="max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${(r.item_description||'').replace(/"/g,'&quot;')}">${r.item_description||''}</td>`;
        html+=`<td><select class="tbl-select tbl-project" data-lid="${lid}">${projDropOpts}</select></td>`;
        if((r.source||'')==='manual'){
            html+=`<td><select class="tbl-select tbl-subcat-manual" data-lid="${lid}">${subcatOptionsHtml(r.mfg_subcategory||'')}</select></td>`;
        }else{
            html+=`<td>${escHtml(r.mfg_subcategory||'')}</td>`;
        }
        html+=`<td style="font-variant-numeric:tabular-nums">${fmt$(sub)}</td>`;
        html+=`<td>${curStation}</td>`;
        html+=`<td><span class="conf-badge conf-${r.mapping_confidence||'none'}">${r.mapping_confidence||'none'}</span></td>`;
        html+=`<td><span class="conf-badge conf-${status}">${status}</span></td>`;
        html+=`<td><select class="tbl-select tbl-assign" data-lid="${lid}">${allOpts}</select></td>`;
        html+=`<td style="white-space:nowrap"><button class="tbl-btn tbl-btn-confirm" onclick="tblSave(this,'confirmed')">Confirm</button> <button class="tbl-btn tbl-btn-skip" onclick="tblSave(this,'skip')">Skip</button> <button class="tbl-btn" style="background:var(--red);color:#fff" onclick="tblSave(this,'non_prod')">Non-P</button> <button class="tbl-btn" style="background:var(--yellow);color:var(--accent-dark)" onclick="tblSave(this,'pilot_npi')">Pilot</button> <span class="tbl-saved">Saved</span></td>`;
        html+='</tr>';
    });
    html+='</tbody></table>';
    document.getElementById('tbl-container').innerHTML=html;
    const tblContainer=document.getElementById('tbl-container');
    tblContainer.onchange=(ev)=>{
        const target=ev.target;
        if(target&&target.classList&&target.classList.contains('tbl-project')){
            tblSaveProject(target);
            return;
        }
        if(target&&target.classList&&target.classList.contains('tbl-subcat-manual')){
            tblSaveManualSubcat(target);
        }
    };

    if(tblDT)tblDT.destroy();
    tblDT=$('#review-tbl').DataTable({
        pageLength:50,order:[[7,'desc']],dom:'Bfrtip',buttons:['csv'],scrollX:true,autoWidth:false,deferRender:true,
        drawCallback:function(){
            const pageRows=document.querySelectorAll('#review-tbl tbody tr[data-lid]');
            pageRows.forEach(tr=>{
                const lid=tr.dataset.lid||'';
                if(!lid)return;
                const ps=tr.querySelector('select.tbl-project');
                if(ps){
                    const pv=projValues[lid]||'';
                    ensureSelectOption(ps,pv);
                    ps.value=pv;
                }
                const ss=tr.querySelector('select.tbl-assign');
                if(ss){ss.value=stationValues[lid]||'';}
            });
        },
        initComplete:function(){this.api().columns().every(function(){
            const col=this;const th=$(col.footer());
            if(!th.length)return;
            $('<input type="text" placeholder="Filter..."/>').appendTo(th.empty()).on('keyup change',function(){if(col.search()!==this.value)col.search(this.value).draw();});
        });}
    });
    enableTableColumnResize(tblDT,'review-tbl');
    tblDT.on('draw.dt',()=>enableTableColumnResize(tblDT,'review-tbl'));
}

function enableTableColumnResize(dt,tableId){
    const table=document.getElementById(tableId);
    if(!table)return;
    const wrap=table.closest('.dataTables_wrapper');
    if(!wrap)return;
    const headTable=wrap.querySelector('.dataTables_scrollHead table');
    const bodyTable=wrap.querySelector('.dataTables_scrollBody table');
    if(!headTable||!bodyTable)return;

    const headers=[...headTable.querySelectorAll('thead th')];
    if(!headers.length)return;
    headTable.classList.add('dt-resizable');

    const headCols=[...headTable.querySelectorAll('colgroup col')];
    const bodyCols=[...bodyTable.querySelectorAll('colgroup col')];
    const STORAGE_KEY='review_table_col_widths_v2';
    const minByCol={0:80,1:90,2:95,3:140,4:260,5:200,6:190,7:110,8:130,9:110,10:100,11:170,12:280};

    const applyWidth=(idx,w)=>{
        const width=Math.max(minByCol[idx]||70,Math.round(w));
        if(headCols[idx])headCols[idx].style.width=width+'px';
        if(bodyCols[idx])bodyCols[idx].style.width=width+'px';
        if(headers[idx])headers[idx].style.width=width+'px';
        const n=idx+1;
        headTable.querySelectorAll(`thead th:nth-child(${n})`).forEach(el=>{
            el.style.width=width+'px';
            el.style.minWidth=width+'px';
            el.style.maxWidth=width+'px';
        });
        bodyTable.querySelectorAll(`thead th:nth-child(${n}), tbody td:nth-child(${n}), tfoot th:nth-child(${n})`).forEach(el=>{
            el.style.width=width+'px';
            el.style.minWidth=width+'px';
            el.style.maxWidth=width+'px';
        });
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
        th.addEventListener('mousedown',(e)=>{
            if(e.target!==th)return;
            const rect=th.getBoundingClientRect();
            if((rect.right-e.clientX)<=14){
                e.preventDefault();
                startResize(e.clientX,rect.width);
            }
        });
        th.appendChild(handle);
    });
}

async function tblSave(btn,status){
    const tr=btn?btn.closest('tr[data-lid]'):null;
    const lid=tr?tr.dataset.lid:'';
    if(!lid)return;
    const sel=tr.querySelector('select.tbl-assign');
    const projSel=tr.querySelector('select.tbl-project');
    const dropdownVal=sel?sel.value:'';
    const projectName=normalizeProjectName(projSel?projSel.value:'');
    const row=DATA.find(r=>String(r.line_id)===String(lid));
    const stationId=dropdownVal||(row?(row.station_id||''):'');

    projValues[lid]=projectName;stationValues[lid]=stationId;
    OVERRIDES[String(lid)]={station_id:stationId,status:status,project_name:projectName};
    const stationMeta=STATIONS.find(s=>s.station_id===stationId);
    if(row){row.station_id=stationId;if(stationMeta)row.station_name=stationMeta.process_name||'';row.mapping_confidence='confirmed';row.mapping_status=status;row.mapping_reason='human override: '+status;if(projectName)row.project_name=projectName;}

    await fetch('/api/override',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({line_id:lid,station_id:stationId,status:status,project_name:projectName})});

    if(tr){
        const cells=tr.querySelectorAll('td');
        if(cells[8])cells[8].textContent=stationId;
        if(cells[10]){cells[10].innerHTML=`<span class="conf-badge conf-${status}">${status}</span>`;}
    }
    const tok=tr?tr.querySelector('.tbl-saved'):null;
    if(tok){tok.style.display='inline';setTimeout(()=>{tok.style.display='none';},1500);}
    updateBadges();
    showToast('Saved: '+(stationId||status));
}

async function tblSaveProject(projSel){
    const tr=projSel?projSel.closest('tr[data-lid]'):null;
    const lid=tr?tr.dataset.lid:'';
    if(!lid)return;
    const projectName=normalizeProjectName(projSel?projSel.value:'');
    projValues[lid]=projectName;
    const row=DATA.find(r=>String(r.line_id)===String(lid));
    const existing=OVERRIDES[String(lid)]||{};
    const stationId=existing.station_id||(row?(row.station_id||''):'');
    const status=existing.status||'confirmed';

    OVERRIDES[String(lid)]={station_id:stationId,status:status,project_name:projectName};
    if(row)row.project_name=projectName;

    await fetch('/api/override',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({line_id:lid,station_id:stationId,status:status,project_name:projectName})});

    const tok=tr?tr.querySelector('.tbl-saved'):null;
    if(tok){tok.style.display='inline';setTimeout(()=>{tok.style.display='none';},1500);}
    updateBadges();
    showToast('Project updated');
}

async function tblSaveManualSubcat(subSel){
    const tr=subSel?subSel.closest('tr[data-lid]'):null;
    const lid=tr?tr.dataset.lid:'';
    if(!lid)return;
    const subcat=normalizeProjectName(subSel.value||'');
    const res=await fetch('/api/manual_po/'+encodeURIComponent(lid)+'/subcategory',{
        method:'PUT',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({mfg_subcategory:subcat}),
    });
    const out=await res.json();
    if(!res.ok){
        showToast(out.error||'Failed updating sub-category');
        return;
    }
    const row=DATA.find(r=>String(r.line_id)===String(lid));
    if(row)row.mfg_subcategory=subcat;
    const tok=tr?tr.querySelector('.tbl-saved'):null;
    if(tok){tok.style.display='inline';setTimeout(()=>{tok.style.display='none';},1500);}
    showToast('Sub-category updated');
}

function escHtml(v){
    return String(v==null?'':v)
        .replaceAll('&','&amp;')
        .replaceAll('<','&lt;')
        .replaceAll('>','&gt;')
        .replaceAll('"','&quot;')
        .replaceAll("'",'&#39;');
}

function initManualFormOptions(){
    const projSel=document.getElementById('m-project-name');
    if(projSel){
        projSel.innerHTML='<option value="">--</option>'+MFG_PROJECT_CODES.map(p=>`<option value="${p.id}">${p.label}</option>`).join('');
    }
    const stSel=document.getElementById('m-station-id');
    if(stSel){
        stSel.innerHTML='<option value="">--</option>'+STATIONS.map(s=>`<option value="${s.station_id}">${s.station_id} - ${s.process_name||''}</option>`).join('');
    }
    const subSel=document.getElementById('m-mfg-subcategory');
    if(subSel){
        subSel.innerHTML=subcatOptionsHtml('');
    }
    clearManualForm();
}

async function reloadData(){
    const [dataRes,overridesRes,stationsRes]=await Promise.all([fetch('/api/data'),fetch('/api/overrides'),fetch('/api/stations')]);
    DATA=await dataRes.json();
    OVERRIDES=await overridesRes.json();
    STATIONS=await stationsRes.json();
    buildStationFilter();
    initManualFormOptions();
}

function clearManualForm(){
    document.getElementById('m-line-id').value='';
    document.getElementById('m-po-number').value='';
    document.getElementById('m-date-order').value='';
    document.getElementById('m-vendor-name').value='';
    document.getElementById('m-price-subtotal').value='';
    document.getElementById('m-project-name').value='';
    document.getElementById('m-station-id').value='';
    document.getElementById('m-mapping-status').value='confirmed';
    document.getElementById('m-product-category').value='';
    document.getElementById('m-mfg-subcategory').value='';
    document.getElementById('m-item-description').value='';
    document.getElementById('m-po-notes').value='';
    document.getElementById('manual-edit-tag').textContent='';
}

function updateManualStats(){
    const rows=DATA.filter(r=>(r.source||'')==='manual'&&(r.line_type||'')==='spend');
    const totalSpend=rows.reduce((s,r)=>s+(parseFloat(r.price_subtotal)||0),0);
    document.getElementById('manual-stats').innerHTML=`
        <div class="stat-card"><div class="label">Manual Lines</div><div class="value">${rows.length.toLocaleString()}</div></div>
        <div class="stat-card"><div class="label">Manual Spend</div><div class="value">${fmtShort$(totalSpend)}</div></div>
        <div class="stat-card"><div class="label">Unique POs</div><div class="value">${new Set(rows.map(r=>r.po_number||'')).size}</div></div>`;
}

async function loadManualPOList(){
    const res=await fetch('/api/manual_po');
    MANUAL_ROWS=await res.json();
    updateManualStats();
    const body=document.getElementById('manual-list-body');
    if(!MANUAL_ROWS.length){
        body.innerHTML='<tr><td colspan="10" style="color:var(--muted)">No manual PO entries yet.</td></tr>';
        return;
    }
    body.innerHTML=MANUAL_ROWS.map(r=>`
        <tr>
            <td>${escHtml(r.po_number||'')}</td>
            <td>${escHtml(r.date_order||'')}</td>
            <td>${escHtml(r.vendor_name||'')}</td>
            <td title="${escHtml(r.item_description||'')}">${escHtml(r.item_description||'')}</td>
            <td>${fmt$(parseFloat(r.price_subtotal||0))}</td>
            <td>${escHtml(r.project_name||'')}</td>
            <td>${escHtml(r.station_id||'')}</td>
            <td>${escHtml(r.mfg_subcategory||'')}</td>
            <td>${escHtml(r.mapping_status||'')}</td>
            <td>
                <button class="tbl-btn tbl-btn-confirm" data-lid="${escHtml(r.line_id||'')}" onclick="editManualPO(this.dataset.lid)">Edit</button>
                <button class="tbl-btn" style="background:var(--red);color:#fff" data-lid="${escHtml(r.line_id||'')}" onclick="deleteManualPO(this.dataset.lid)">Delete</button>
            </td>
        </tr>`).join('');
}

function editManualPO(lineId){
    const row=MANUAL_ROWS.find(r=>String(r.line_id)===String(lineId));
    if(!row)return;
    document.getElementById('m-line-id').value=row.line_id||'';
    document.getElementById('m-po-number').value=row.po_number||'';
    document.getElementById('m-date-order').value=row.date_order||'';
    document.getElementById('m-vendor-name').value=row.vendor_name||'';
    document.getElementById('m-price-subtotal').value=row.price_subtotal||'';
    const projSel=document.getElementById('m-project-name');
    ensureSelectOption(projSel,row.project_name||'');
    projSel.value=row.project_name||'';
    const stSel=document.getElementById('m-station-id');
    ensureSelectOption(stSel,row.station_id||'');
    stSel.value=row.station_id||'';
    document.getElementById('m-mapping-status').value=row.mapping_status||'confirmed';
    document.getElementById('m-product-category').value=row.product_category||'';
    const subSel=document.getElementById('m-mfg-subcategory');
    ensureSelectOption(subSel,row.mfg_subcategory||'');
    subSel.value=row.mfg_subcategory||'';
    document.getElementById('m-item-description').value=row.item_description||'';
    document.getElementById('m-po-notes').value=row.po_notes||'';
    document.getElementById('manual-edit-tag').textContent='Editing '+(row.line_id||'');
}

async function saveManualPO(){
    const lineId=document.getElementById('m-line-id').value||'';
    const payload={
        po_number:document.getElementById('m-po-number').value||'',
        date_order:document.getElementById('m-date-order').value||'',
        vendor_name:document.getElementById('m-vendor-name').value||'',
        price_subtotal:document.getElementById('m-price-subtotal').value||'',
        project_name:document.getElementById('m-project-name').value||'',
        station_id:document.getElementById('m-station-id').value||'',
        mapping_status:document.getElementById('m-mapping-status').value||'confirmed',
        product_category:document.getElementById('m-product-category').value||'',
        mfg_subcategory:document.getElementById('m-mfg-subcategory').value||'',
        item_description:document.getElementById('m-item-description').value||'',
        po_notes:document.getElementById('m-po-notes').value||'',
    };
    if(!payload.po_number||!payload.date_order||!payload.vendor_name||!payload.price_subtotal||!payload.item_description){
        showToast('Fill required fields: PO, Date, Vendor, Description, Subtotal');
        return;
    }
    const isEdit=Boolean(lineId);
    const url=isEdit?('/api/manual_po/'+encodeURIComponent(lineId)):'/api/manual_po';
    const method=isEdit?'PUT':'POST';
    const res=await fetch(url,{method,headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    const out=await res.json();
    if(!res.ok){
        const details=out&&out.details?(' - '+out.details.join('; ')):'';
        showToast((out.error||'Failed saving manual PO')+details);
        return;
    }
    await reloadData();
    await loadManualPOList();
    if(currentTab==='table_view')loadTableView();
    clearManualForm();
    showToast(isEdit?'Manual PO updated':'Manual PO added');
}

async function deleteManualPO(lineId){
    if(!lineId)return;
    if(!confirm('Delete manual PO entry '+lineId+'?'))return;
    const res=await fetch('/api/manual_po/'+encodeURIComponent(lineId),{method:'DELETE'});
    const out=await res.json();
    if(!res.ok){
        showToast(out.error||'Failed deleting manual PO');
        return;
    }
    await reloadData();
    await loadManualPOList();
    if(currentTab==='table_view')loadTableView();
    clearManualForm();
    showToast('Manual PO deleted');
}

async function reExport(){
    showToast('Re-exporting...');
    const res=await fetch('/api/reexport',{method:'POST'});
    const data=await res.json();
    if(res.ok){
        await reloadData();
        if(currentTab==='table_view')loadTableView();
        else if(currentTab==='manual_po')await loadManualPOList();
        else render();
    }
    showToast(data.message||'Done!');
}

function showToast(msg){const t=document.getElementById('toast');t.textContent=msg;t.style.display='block';setTimeout(()=>{t.style.display='none';},2500);}

init();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template_string(TEMPLATE)


@app.route("/api/data")
def api_data():
    df = _load_data()
    if df.empty:
        return jsonify([])
    return jsonify(df.to_dict(orient="records"))


@app.route("/api/stations")
def api_stations():
    return jsonify(_load_stations())


@app.route("/api/overrides")
def api_overrides():
    return jsonify(_load_overrides())


@app.route("/api/manual_po")
def api_manual_po_list():
    return jsonify(_list_manual_po_rows())


@app.route("/api/manual_po", methods=["POST"])
def api_manual_po_create():
    body = request.get_json(silent=True) or {}
    errors = _validate_manual_payload(body)
    if errors:
        return jsonify({"error": "invalid payload", "details": errors}), 400
    row = _upsert_manual_po(body)
    return jsonify({"ok": True, "line_id": row["line_id"], "row": row}), 201


@app.route("/api/manual_po/<line_id>", methods=["PUT"])
def api_manual_po_update(line_id: str):
    body = request.get_json(silent=True) or {}
    errors = _validate_manual_payload(body)
    if errors:
        return jsonify({"error": "invalid payload", "details": errors}), 400
    try:
        row = _upsert_manual_po(body, line_id=line_id)
    except KeyError:
        return jsonify({"error": "manual row not found"}), 404
    return jsonify({"ok": True, "line_id": row["line_id"], "row": row})


@app.route("/api/manual_po/<line_id>", methods=["DELETE"])
def api_manual_po_delete(line_id: str):
    deleted = _delete_manual_po(line_id)
    if not deleted:
        return jsonify({"error": "manual row not found"}), 404
    return jsonify({"ok": True, "line_id": line_id})


@app.route("/api/manual_po/<line_id>/subcategory", methods=["PUT"])
def api_manual_po_subcategory(line_id: str):
    body = request.get_json(silent=True) or {}
    try:
        row = _update_manual_po_subcategory(line_id, body.get("mfg_subcategory", ""))
    except KeyError:
        return jsonify({"error": "manual row not found"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"ok": True, "line_id": row.get("line_id", line_id), "row": row})


@app.route("/api/override", methods=["POST"])
def api_save_override():
    body = request.get_json()
    lid = body.get("line_id", "")
    if not lid:
        return jsonify({"error": "line_id required"}), 400

    overrides = _load_overrides()
    existing = overrides.get(str(lid), {})
    existing["station_id"] = body.get("station_id", "")
    existing["status"] = body.get("status", "confirmed")
    if "project_name" in body:
        existing["project_name"] = body["project_name"]
    overrides[str(lid)] = existing
    _save_overrides(overrides)
    return jsonify({"ok": True, "total_overrides": len(overrides)})


@app.route("/api/override_batch", methods=["POST"])
def api_save_override_batch():
    body = request.get_json()

    # Support both formats:
    #   {overrides: [{line_id, station_id, status}, ...]}  (per-line stations)
    #   {line_ids: [...], station_id: "...", status: "..."}  (uniform station)
    items = body.get("overrides", [])
    if not items:
        line_ids = body.get("line_ids", [])
        station_id = body.get("station_id", "")
        status = body.get("status", "confirmed")
        items = [{"line_id": lid, "station_id": station_id, "status": status} for lid in line_ids]

    if not items:
        return jsonify({"error": "overrides or line_ids required"}), 400

    overrides = _load_overrides()
    for item in items:
        lid = item.get("line_id", "")
        if lid:
            existing = overrides.get(str(lid), {})
            existing["station_id"] = item.get("station_id", "")
            existing["status"] = item.get("status", "confirmed")
            if "project_name" in item:
                existing["project_name"] = item["project_name"]
            overrides[str(lid)] = existing
    _save_overrides(overrides)
    return jsonify({"ok": True, "count": len(items), "total_overrides": len(overrides)})


@app.route("/api/reexport", methods=["POST"])
def api_reexport():
    import subprocess
    import sys
    cmd = [sys.executable, str(Path(__file__).resolve().parent / "capex_pipeline.py"), "--skip-bq"]
    if store.is_remote():
        cmd.append("--write-bq")
    result = subprocess.run(
        cmd,
        capture_output=True, text=True, cwd=str(Path(__file__).resolve().parent),
        timeout=300,
    )
    return jsonify({
        "message": "Pipeline re-exported" if result.returncode == 0 else "Pipeline failed",
        "stdout": result.stdout[-2000:] if result.stdout else "",
        "stderr": result.stderr[-2000:] if result.stderr else "",
    })


if __name__ == "__main__":
    print("Station Review UI: http://localhost:5051")
    app.run(host="0.0.0.0", port=5051, debug=os.environ.get("FLASK_DEBUG", "1") == "1")
