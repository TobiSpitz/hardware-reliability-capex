"""
Payment Pattern Analysis -- analyzes historical PO payment timelines
to build vendor and line payment profiles for forecasting.

Produces:
  - Per-PO payment timelines (PO date -> deposit -> bills -> final payment)
  - Vendor payment profiles (avg days and % at each milestone)
  - Line payment profiles (avg cycle by production line)
  - Payment templates that can be cloned and adjusted for future POs

Usage:
    python payment_patterns.py --analyze           # analyze from BigQuery
    python payment_patterns.py --analyze --local   # analyze from local CSV
"""
from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

import storage_backend as store


# ---------------------------------------------------------------------------
# Load payment data
# ---------------------------------------------------------------------------

def load_payment_data(*, from_bq: bool = True) -> pd.DataFrame:
    """Load payment detail data from BigQuery or local CSV."""
    if from_bq:
        try:
            import bq_dataset
            return bq_dataset.read_table("payments")
        except Exception:
            pass
    return store.read_csv("payment_details.csv")


def load_po_data(*, from_bq: bool = True) -> pd.DataFrame:
    """Load PO line data for cross-referencing."""
    if from_bq:
        try:
            import bq_dataset
            return bq_dataset.read_table("po_lines")
        except Exception:
            pass
    return store.read_csv("capex_clean.csv")


# ---------------------------------------------------------------------------
# Per-PO payment timeline
# ---------------------------------------------------------------------------

def build_po_timelines(
    payments: pd.DataFrame,
    po_data: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    """Build a timeline for each PO showing key payment milestones.

    Uses explicit payment_date when available, falls back to bill_posting_date
    as a proxy for when payment occurred (since most bills show paid status).
    """
    if payments.empty:
        return []

    payments = payments.copy()
    for col in ["date_order", "bill_invoice_date", "bill_due_date", "payment_date", "bill_posting_date"]:
        if col in payments.columns:
            payments[col] = pd.to_datetime(payments[col], errors="coerce")

    for col in ["line_amount", "bill_amount", "payment_amount", "bill_open_amount"]:
        if col in payments.columns:
            payments[col] = pd.to_numeric(payments[col], errors="coerce").fillna(0)

    po_totals: dict[str, float] = {}
    if po_data is not None and not po_data.empty and "po_number" in po_data.columns:
        po_data = po_data.copy()
        po_data["po_number"] = po_data["po_number"].astype(str).str.strip()
        amount_col = None
        for candidate in ("price_subtotal", "line_amount", "amount_total", "total_amount"):
            if candidate in po_data.columns:
                amount_col = candidate
                break
        if amount_col:
            po_data[amount_col] = pd.to_numeric(po_data[amount_col], errors="coerce").fillna(0)
            grouped = po_data.groupby("po_number")[amount_col].sum()
            po_totals = {str(k): float(v) for k, v in grouped.items()}

    timelines: list[dict[str, Any]] = []

    for po_number, group in payments.groupby("po_number"):
        po_date = group["date_order"].min()
        vendor = group["vendor_name"].iloc[0] if "vendor_name" in group.columns else ""
        total_po_amount = 0.0
        po_key = str(po_number).strip()
        if po_key in po_totals and po_totals[po_key] > 0:
            total_po_amount = float(po_totals[po_key])
        elif "line_amount" in group.columns:
            # Payment detail rows can duplicate the same PO line across bills/payments;
            # dedupe by po_line_id before summing when available.
            if "po_line_id" in group.columns:
                dedup = (
                    group[["po_line_id", "line_amount"]]
                    .dropna(subset=["po_line_id"])
                    .drop_duplicates(subset=["po_line_id"])
                )
                total_po_amount = float(pd.to_numeric(dedup["line_amount"], errors="coerce").fillna(0).sum())
            if total_po_amount <= 0:
                total_po_amount = float(pd.to_numeric(group["line_amount"], errors="coerce").fillna(0).max())

        bills = group.dropna(subset=["bill_invoice_date"]).drop_duplicates(subset=["bill_id"])
        if bills.empty:
            continue

        event_dates: list[pd.Timestamp] = []
        event_amounts: list[float] = []
        event_labels: list[str] = []

        payment_rows = group.dropna(subset=["payment_date"]).drop_duplicates(
            subset=["payment_date", "payment_amount"]
        )
        if not payment_rows.empty:
            for _, pr in payment_rows.sort_values("payment_date").iterrows():
                event_dates.append(pr["payment_date"])
                event_amounts.append(float(pr["payment_amount"]))
                event_labels.append("Payment")
        else:
            paid_bills = bills[
                bills.get("bill_payment_state", pd.Series(dtype=str)).isin(["paid", "in_payment"])
            ]
            date_col = "bill_posting_date" if "bill_posting_date" in paid_bills.columns else "bill_invoice_date"
            for _, br in paid_bills.sort_values(date_col).iterrows():
                d = br[date_col]
                if pd.notna(d):
                    event_dates.append(d)
                    event_amounts.append(float(br.get("bill_amount", 0)))
                    event_labels.append("Bill Paid")

        total_paid = sum(event_amounts) if event_amounts else 0
        first_event = event_dates[0] if event_dates else None
        last_event = event_dates[-1] if event_dates else None

        days_to_first = (first_event - po_date).days if first_event and pd.notna(po_date) and pd.notna(first_event) else None
        days_to_last = (last_event - po_date).days if last_event and pd.notna(po_date) and pd.notna(last_event) else None

        milestones: list[dict[str, Any]] = []
        if pd.notna(po_date):
            milestones.append({"label": "PO Created", "date": po_date, "amount": 0, "day_offset": 0})
        completion_threshold = max(1.0, total_po_amount * 0.01)
        is_fully_paid = total_po_amount > 0 and (total_paid + completion_threshold) >= total_po_amount
        running_paid = 0.0
        for i, (d, a, lbl) in enumerate(zip(event_dates, event_amounts, event_labels)):
            offset = (d - po_date).days if pd.notna(po_date) and pd.notna(d) else None
            pct = (a / total_po_amount * 100) if total_po_amount else 0
            running_paid += float(a)
            is_last = i == len(event_dates) - 1
            if i == 0:
                label = "Deposit" if pct < 60 else "Full Payment"
            elif is_last and is_fully_paid:
                label = "Final Payment (Confirmed)"
            elif is_last and not is_fully_paid:
                label = "Latest Payment (Open Balance)"
            else:
                label = f"Payment {i+1}"
            milestones.append({
                "label": label, "date": d, "amount": a,
                "day_offset": offset, "pct_of_total": round(pct, 1),
                "cumulative_paid": round(running_paid, 2),
                "is_final_confirmed": bool(is_last and is_fully_paid),
            })

        timelines.append({
            "po_number": str(po_number),
            "vendor_name": vendor,
            "po_date": po_date,
            "total_amount": total_po_amount,
            "total_paid": total_paid,
            "remaining_balance": round(max(total_po_amount - total_paid, 0), 2),
            "is_final_confirmed": bool(is_fully_paid),
            "payment_count": len(event_dates),
            "days_to_first_payment": days_to_first,
            "days_to_final_payment": days_to_last,
            "total_cycle_days": days_to_last,
            "milestones": milestones,
        })

    return timelines


# ---------------------------------------------------------------------------
# Vendor payment profiles
# ---------------------------------------------------------------------------

def build_vendor_profiles(timelines: list[dict]) -> list[dict[str, Any]]:
    """Aggregate PO timelines by vendor into average payment profiles."""
    if not timelines:
        return []

    vendor_groups: dict[str, list[dict]] = {}
    for t in timelines:
        vendor = t.get("vendor_name", "Unknown")
        if vendor:
            vendor_groups.setdefault(vendor, []).append(t)

    profiles: list[dict[str, Any]] = []
    for vendor, vendor_timelines in vendor_groups.items():
        completed = [
            t for t in vendor_timelines
            if t.get("payment_count", 0) > 0 and t.get("total_cycle_days") is not None
        ]
        if not completed:
            continue

        avg_cycle = sum(t["total_cycle_days"] for t in completed) / len(completed)
        avg_payments = sum(t["payment_count"] for t in completed) / len(completed)
        avg_first = sum(
            t["days_to_first_payment"] for t in completed if t["days_to_first_payment"] is not None
        ) / max(1, sum(1 for t in completed if t["days_to_first_payment"] is not None))
        total_spend = sum(t.get("total_amount", 0) for t in completed)

        deposit_pcts = []
        for t in completed:
            ms = t.get("milestones", [])
            if len(ms) > 1 and ms[1].get("pct_of_total"):
                deposit_pcts.append(ms[1]["pct_of_total"])
        avg_deposit_pct = sum(deposit_pcts) / len(deposit_pcts) if deposit_pcts else 0

        profiles.append({
            "vendor_name": vendor,
            "po_count": len(completed),
            "total_spend": total_spend,
            "avg_cycle_days": round(avg_cycle),
            "avg_payment_count": round(avg_payments, 1),
            "avg_days_to_first_payment": round(avg_first),
            "avg_deposit_pct": round(avg_deposit_pct, 1),
        })

    return sorted(profiles, key=lambda p: p["total_spend"], reverse=True)


# ---------------------------------------------------------------------------
# Line payment profiles
# ---------------------------------------------------------------------------

def build_line_profiles(
    timelines: list[dict],
    po_data: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Aggregate payment timelines by production line."""
    if not timelines or po_data.empty:
        return []

    import re
    def extract_line(sid: str) -> str:
        m = re.match(r"(BASE\d+)-(MOD\d+|CELL\d+|INV\d+)", str(sid))
        return f"{m.group(1)}-{m.group(2)}" if m else ""

    po_to_line: dict[str, str] = {}
    if "station_id" in po_data.columns and "po_number" in po_data.columns:
        for _, row in po_data.iterrows():
            po = str(row.get("po_number", ""))
            sid = str(row.get("station_id", ""))
            line = extract_line(sid)
            if po and line:
                po_to_line[po] = line

    line_groups: dict[str, list[dict]] = {}
    for t in timelines:
        line = po_to_line.get(t["po_number"], "")
        if line:
            line_groups.setdefault(line, []).append(t)

    profiles: list[dict[str, Any]] = []
    for line, line_timelines in line_groups.items():
        completed = [t for t in line_timelines if t.get("total_cycle_days") is not None]
        if not completed:
            continue

        avg_cycle = sum(t["total_cycle_days"] for t in completed) / len(completed)
        total_spend = sum(t.get("total_amount", 0) for t in completed)

        profiles.append({
            "line": line,
            "po_count": len(completed),
            "total_spend": total_spend,
            "avg_cycle_days": round(avg_cycle),
        })

    return sorted(profiles, key=lambda p: p["total_spend"], reverse=True)


# ---------------------------------------------------------------------------
# Generate template from a PO pattern
# ---------------------------------------------------------------------------

def create_template_from_po(
    timeline: dict[str, Any],
    *,
    name: str = "",
) -> dict[str, Any]:
    """Create a payment template from a historical PO's actual payment pattern."""
    milestones = timeline.get("milestones", [])
    total = timeline.get("total_amount", 0)

    template_milestones = []
    for ms in milestones:
        if ms["label"] == "PO Created":
            continue
        pct = ms.get("pct_of_total", 0)
        if not pct and total and ms.get("amount"):
            pct = round(ms["amount"] / total * 100, 1)
        template_milestones.append({
            "label": ms["label"],
            "day_offset": ms.get("day_offset", 0),
            "pct": pct,
        })

    if not name:
        name = f"{timeline.get('vendor_name', 'Unknown')} - {timeline.get('po_number', '')}"

    return {
        "template_id": str(uuid.uuid4()),
        "name": name,
        "description": f"Based on {timeline['po_number']} ({timeline.get('vendor_name', '')})",
        "milestones": template_milestones,
        "vendor_name": timeline.get("vendor_name", ""),
        "source_po": timeline.get("po_number", ""),
    }


def adjust_template(
    template: dict[str, Any],
    *,
    day_scale: float = 1.0,
    pct_adjustments: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Clone and adjust a template's days and percentages.

    Args:
        template: Original template dict.
        day_scale: Multiply all day_offsets by this factor (e.g., 0.8 = 20% faster).
        pct_adjustments: Map of milestone label -> new pct.
    """
    adjusted = {**template, "template_id": str(uuid.uuid4())}
    adjusted["name"] = f"{template['name']} (adjusted)"
    adjusted["milestones"] = []

    for ms in template.get("milestones", []):
        new_ms = {**ms}
        new_ms["day_offset"] = round(ms.get("day_offset", 0) * day_scale)
        if pct_adjustments and ms["label"] in pct_adjustments:
            new_ms["pct"] = pct_adjustments[ms["label"]]
        adjusted["milestones"].append(new_ms)

    return adjusted


# ---------------------------------------------------------------------------
# Full analysis pipeline
# ---------------------------------------------------------------------------

def run_analysis(*, from_bq: bool = True) -> dict[str, Any]:
    """Run the full payment pattern analysis and return results."""
    print("=== Payment Pattern Analysis ===")

    payments = load_payment_data(from_bq=from_bq)
    print(f"  Payment records: {len(payments)}")

    if payments.empty:
        print("  No payment data available. Run pipeline with --write-bq first.")
        return {"timelines": [], "vendor_profiles": [], "line_profiles": []}

    po_data = load_po_data(from_bq=from_bq)
    print(f"  PO lines: {len(po_data)}")

    timelines = build_po_timelines(payments)
    print(f"  PO timelines: {len(timelines)}")

    vendor_profiles = build_vendor_profiles(timelines)
    print(f"  Vendor profiles: {len(vendor_profiles)}")
    for vp in vendor_profiles[:10]:
        print(f"    {vp['vendor_name']}: {vp['po_count']} POs, "
              f"avg {vp['avg_cycle_days']}d cycle, "
              f"${vp['total_spend']:,.0f}")

    line_profiles = build_line_profiles(timelines, po_data)
    print(f"  Line profiles: {len(line_profiles)}")
    for lp in line_profiles:
        print(f"    {lp['line']}: {lp['po_count']} POs, avg {lp['avg_cycle_days']}d cycle")

    store.write_json("payment_analysis.json", {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_timelines": len(timelines),
            "vendor_profiles": len(vendor_profiles),
            "line_profiles": len(line_profiles),
        },
        "vendor_profiles": vendor_profiles,
        "line_profiles": line_profiles,
    })

    return {
        "timelines": timelines,
        "vendor_profiles": vendor_profiles,
        "line_profiles": line_profiles,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Payment Pattern Analysis")
    parser.add_argument("--analyze", action="store_true", help="Run full analysis")
    parser.add_argument("--local", action="store_true", help="Use local CSV instead of BigQuery")
    args = parser.parse_args()

    if args.analyze:
        run_analysis(from_bq=not args.local)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
