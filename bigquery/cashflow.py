"""
Cashflow Projection Engine -- combines actual payment data with projected
milestones from payment templates to build a complete cashflow forecast.

Produces:
  - Monthly cashflow projections (actual + projected outflows)
  - Cumulative spend curves
  - Weekly detail breakdowns
  - Variance analysis (projected vs actual where actuals exist)

Usage:
    python cashflow.py --project             # build projections from BigQuery
    python cashflow.py --project --local     # build from local CSVs
"""
from __future__ import annotations

import argparse
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd

import storage_backend as store


# ---------------------------------------------------------------------------
# Load source data
# ---------------------------------------------------------------------------

def _load_payments(*, from_bq: bool = True) -> pd.DataFrame:
    if from_bq:
        try:
            import bq_dataset
            return bq_dataset.read_table("payments")
        except Exception:
            pass
    return store.read_csv("payment_details.csv")


def _load_milestones(*, from_bq: bool = True) -> pd.DataFrame:
    if from_bq:
        try:
            import bq_dataset
            return bq_dataset.read_table("payment_milestones")
        except Exception:
            pass
    return pd.DataFrame()


def _load_po_lines(*, from_bq: bool = True) -> pd.DataFrame:
    if from_bq:
        try:
            import bq_dataset
            return bq_dataset.read_table("po_lines")
        except Exception:
            pass
    return store.read_csv("capex_clean.csv")


def _load_templates(*, from_bq: bool = True) -> list[dict]:
    """Load templates from local JSON (source of truth)."""
    raw = store.read_json("payment_templates.json")
    return raw if isinstance(raw, list) else []


# ---------------------------------------------------------------------------
# Build cashflow projections
# ---------------------------------------------------------------------------

def _actuals_from_payments(payments: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert actual payment records into cashflow projection rows.

    Uses explicit payment_date when available, falls back to bill_posting_date
    for paid bills as a proxy for when cash left the company.
    """
    if payments.empty:
        return []

    payments = payments.copy()
    for col in ["payment_date", "date_order", "bill_posting_date", "bill_invoice_date"]:
        if col in payments.columns:
            payments[col] = pd.to_datetime(payments[col], errors="coerce")
    for col in ["payment_amount", "bill_amount"]:
        if col in payments.columns:
            payments[col] = pd.to_numeric(payments[col], errors="coerce").fillna(0)

    rows: list[dict[str, Any]] = []
    seen: set[tuple] = set()

    has_payment = payments.dropna(subset=["payment_date"])
    for _, row in has_payment.iterrows():
        key = (str(row.get("po_number", "")), str(row["payment_date"]), float(row.get("payment_amount", 0)))
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "projection_id": str(uuid.uuid4()),
            "po_number": str(row.get("po_number", "")),
            "vendor_name": str(row.get("vendor_name", "")),
            "station_id": "", "line": "",
            "milestone_label": "Actual Payment",
            "expected_date": row["payment_date"],
            "expected_amount": float(row.get("payment_amount", 0)),
            "actual_date": row["payment_date"],
            "actual_amount": float(row.get("payment_amount", 0)),
            "source": "historical",
            "record_type": "actual",
        })

    no_payment = payments[payments["payment_date"].isna()]
    if "bill_payment_state" in no_payment.columns:
        paid_bills = no_payment[
            no_payment["bill_payment_state"].isin(["paid", "in_payment"])
        ]
    else:
        paid_bills = pd.DataFrame()

    if not paid_bills.empty and "bill_id" in paid_bills.columns:
        paid_bills = paid_bills.drop_duplicates(subset=["bill_id"])
        date_col = "bill_posting_date" if "bill_posting_date" in paid_bills.columns else "bill_invoice_date"
        for _, row in paid_bills.dropna(subset=[date_col]).iterrows():
            bill_date = row[date_col]
            amount = float(row.get("bill_amount", 0))
            key = (str(row.get("po_number", "")), str(bill_date), amount)
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "projection_id": str(uuid.uuid4()),
                "po_number": str(row.get("po_number", "")),
                "vendor_name": str(row.get("vendor_name", "")),
                "station_id": "", "line": "",
                "milestone_label": "Bill Paid",
                "expected_date": bill_date,
                "expected_amount": amount,
                "actual_date": bill_date,
                "actual_amount": amount,
                "source": "historical",
                "record_type": "actual",
            })

    return rows


def _projections_from_templates(
    po_lines: pd.DataFrame,
    templates: list[dict],
) -> list[dict[str, Any]]:
    """Generate projected cashflow from per-PO payment milestone templates.

    Templates are keyed by po_number and have milestones with expected_date
    and pct/amount. Milestones marked as 'paid' are treated as actuals.
    """
    if not templates:
        return []

    import re as _re

    po_lookup: dict[str, dict] = {}
    if not po_lines.empty and "po_number" in po_lines.columns:
        for po_num, group in po_lines.groupby("po_number"):
            po_lookup[str(po_num)] = {
                "station_id": str(group["station_id"].iloc[0]) if "station_id" in group.columns else "",
                "vendor_name": str(group["vendor_name"].iloc[0]) if "vendor_name" in group.columns else "",
            }

    rows: list[dict[str, Any]] = []

    for tpl in templates:
        po_num = tpl.get("po_number", "")
        vendor = tpl.get("vendor_name", "")
        total_amount = float(tpl.get("total_amount", 0))
        po_info = po_lookup.get(po_num, {})
        station_id = po_info.get("station_id", "")

        line = ""
        m = _re.match(r"(BASE\d+)-(MOD\d+|CELL\d+|INV\d+)", station_id)
        if m:
            line = f"{m.group(1)}-{m.group(2)}"

        for ms in tpl.get("milestones", []):
            expected_date_str = ms.get("expected_date", ms.get("date", ""))
            if not expected_date_str:
                continue

            expected_dt = pd.to_datetime(expected_date_str, errors="coerce")
            if pd.isna(expected_dt):
                continue

            pct = float(ms.get("pct", 0))
            amount = float(ms.get("amount", 0))
            if not amount and pct and total_amount:
                amount = round(total_amount * pct / 100, 2)

            status = ms.get("status", "projected")
            is_paid = status == "paid"

            rows.append({
                "projection_id": str(uuid.uuid4()),
                "po_number": po_num,
                "vendor_name": vendor,
                "station_id": station_id,
                "line": line,
                "milestone_label": ms.get("label", "Payment"),
                "expected_date": expected_dt,
                "expected_amount": amount,
                "actual_date": expected_dt if is_paid else None,
                "actual_amount": amount if is_paid else None,
                "source": "historical" if is_paid else "projected",
                "record_type": "template",
            })

    return rows


def build_projections(*, from_bq: bool = True) -> pd.DataFrame:
    """Build the complete cashflow projection combining actuals and projected."""
    payments = _load_payments(from_bq=from_bq)
    po_lines = _load_po_lines(from_bq=from_bq)
    templates = _load_templates(from_bq=from_bq)

    actuals = _actuals_from_payments(payments)
    projected = _projections_from_templates(po_lines, templates)

    all_rows = actuals + projected
    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["expected_date"] = pd.to_datetime(df["expected_date"], errors="coerce")
    df["expected_amount"] = pd.to_numeric(df.get("expected_amount"), errors="coerce").fillna(0.0)
    if "actual_date" in df.columns:
        df["actual_date"] = pd.to_datetime(df["actual_date"], errors="coerce")

    # If a paid template milestone lands on the same PO/date/amount as an
    # actual payment row, keep the actual row and suppress the template copy.
    hist = df[df["source"] == "historical"].copy()
    if not hist.empty:
        hist["_po_key"] = hist["po_number"].astype(str).str.strip()
        hist["_date_key"] = hist["expected_date"].dt.strftime("%Y-%m-%d")
        hist["_amt_key"] = hist["expected_amount"].round(2)

        actual_labels = {"Actual Payment", "Bill Paid"}
        actual_rows = hist[hist["milestone_label"].astype(str).isin(actual_labels)]
        if not actual_rows.empty:
            actual_keys = set(zip(actual_rows["_po_key"], actual_rows["_date_key"], actual_rows["_amt_key"]))
            non_actual_hist = hist[~hist["milestone_label"].astype(str).isin(actual_labels)].copy()
            non_actual_hist["_dup_with_actual"] = non_actual_hist.apply(
                lambda r: (r["_po_key"], r["_date_key"], r["_amt_key"]) in actual_keys, axis=1
            )

            if non_actual_hist["_dup_with_actual"].any():
                dup_idx = set(non_actual_hist[non_actual_hist["_dup_with_actual"]].index.tolist())
                df = df.loc[~df.index.isin(dup_idx)].copy()

    return df


# ---------------------------------------------------------------------------
# Aggregation helpers (for dashboard consumption)
# ---------------------------------------------------------------------------

def monthly_cashflow(projections: pd.DataFrame) -> list[dict[str, Any]]:
    """Aggregate projections into monthly buckets."""
    if projections.empty:
        return []

    df = projections.copy()
    df["month"] = df["expected_date"].dt.to_period("M").astype(str)
    df["expected_amount"] = pd.to_numeric(df["expected_amount"], errors="coerce").fillna(0)

    result: list[dict[str, Any]] = []
    for month, group in df.groupby("month"):
        actuals = group[group["source"] == "historical"]
        projected = group[group["source"] == "projected"]

        by_line = group.groupby("line")["expected_amount"].sum().to_dict()

        result.append({
            "month": str(month),
            "total": round(group["expected_amount"].sum(), 2),
            "actual": round(actuals["expected_amount"].sum(), 2),
            "projected": round(projected["expected_amount"].sum(), 2),
            "by_line": {k: round(v, 2) for k, v in by_line.items() if k},
        })

    return sorted(result, key=lambda r: r["month"])


def cumulative_cashflow(monthly: list[dict]) -> list[dict[str, Any]]:
    """Convert monthly cashflow into a cumulative running total."""
    cumulative: list[dict[str, Any]] = []
    running_total = 0.0
    running_actual = 0.0
    running_projected = 0.0

    for m in monthly:
        running_total += m["total"]
        running_actual += m["actual"]
        running_projected += m["projected"]
        cumulative.append({
            "month": m["month"],
            "cumulative_total": round(running_total, 2),
            "cumulative_actual": round(running_actual, 2),
            "cumulative_projected": round(running_projected, 2),
        })

    return cumulative


def weekly_detail(projections: pd.DataFrame) -> list[dict[str, Any]]:
    """Break down projections into weekly buckets with PO-level detail."""
    if projections.empty:
        return []

    df = projections.copy()
    df["week"] = df["expected_date"].dt.isocalendar().week.astype(str)
    df["year"] = df["expected_date"].dt.year.astype(str)
    df["year_week"] = df["year"] + "-W" + df["week"].str.zfill(2)
    df["expected_amount"] = pd.to_numeric(df["expected_amount"], errors="coerce").fillna(0)

    result: list[dict[str, Any]] = []
    for yw, group in df.groupby("year_week"):
        items = []
        for _, row in group.iterrows():
            items.append({
                "po_number": row.get("po_number", ""),
                "vendor_name": row.get("vendor_name", ""),
                "line": row.get("line", ""),
                "milestone": row.get("milestone_label", ""),
                "expected_date": str(row.get("expected_date", ""))[:10],
                "amount": round(float(row.get("expected_amount", 0)), 2),
                "source": row.get("source", ""),
            })
        result.append({
            "year_week": str(yw),
            "total": round(group["expected_amount"].sum(), 2),
            "items": items,
        })

    return sorted(result, key=lambda r: r["year_week"])


def apply_scenario_shift(
    projections: pd.DataFrame,
    *,
    shift_days: int = 0,
) -> pd.DataFrame:
    """Shift all projected (not actual) dates by N days to model delays."""
    if projections.empty or shift_days == 0:
        return projections

    df = projections.copy()
    mask = df["source"] == "projected"
    df.loc[mask, "expected_date"] = df.loc[mask, "expected_date"] + timedelta(days=shift_days)
    return df


# ---------------------------------------------------------------------------
# Write projections to BigQuery
# ---------------------------------------------------------------------------

def save_projections(projections: pd.DataFrame, *, to_bq: bool = True) -> None:
    """Persist cashflow projections to BigQuery and/or local JSON."""
    if projections.empty:
        return

    monthly = monthly_cashflow(projections)
    cumul = cumulative_cashflow(monthly)
    weekly = weekly_detail(projections)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_rows": len(projections),
        "actuals": len(projections[projections["source"] == "historical"]),
        "projected": len(projections[projections["source"] == "projected"]),
        "monthly": monthly,
        "cumulative": cumul,
    }
    store.write_json("cashflow_summary.json", summary)
    print(f"  Saved cashflow_summary.json ({len(monthly)} months)")

    if to_bq:
        try:
            import bq_dataset
            n = bq_dataset.write_table("cashflow_projections", projections)
            print(f"  Wrote {n} rows to cashflow_projections")
        except Exception as exc:
            print(f"  WARNING: BigQuery write failed ({exc})")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def run_projections(*, from_bq: bool = True) -> dict[str, Any]:
    """Run the full cashflow projection pipeline."""
    print("=== Cashflow Projection Engine ===")

    projections = build_projections(from_bq=from_bq)
    print(f"  Total projection rows: {len(projections)}")

    if projections.empty:
        print("  No data available for projections.")
        return {"total_rows": 0}

    actuals_count = len(projections[projections["source"] == "historical"])
    projected_count = len(projections[projections["source"] == "projected"])
    print(f"  Actuals: {actuals_count}, Projected: {projected_count}")

    monthly = monthly_cashflow(projections)
    for m in monthly[:6]:
        print(f"    {m['month']}: ${m['total']:>12,.2f} "
              f"(actual: ${m['actual']:>10,.2f}, projected: ${m['projected']:>10,.2f})")
    if len(monthly) > 6:
        print(f"    ... and {len(monthly) - 6} more months")

    save_projections(projections, to_bq=from_bq)

    return {
        "total_rows": len(projections),
        "actuals": actuals_count,
        "projected": projected_count,
        "months": len(monthly),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Cashflow Projection Engine")
    parser.add_argument("--project", action="store_true", help="Build cashflow projections")
    parser.add_argument("--local", action="store_true", help="Use local CSV instead of BigQuery")
    parser.add_argument("--shift", type=int, default=0, help="Shift projected dates by N days")
    args = parser.parse_args()

    if args.project:
        result = run_projections(from_bq=not args.local)
        print(f"\n  Summary: {json.dumps(result, indent=2)}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
