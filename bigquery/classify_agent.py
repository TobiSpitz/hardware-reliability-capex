"""
LLM Classification Review Agent.

Runs as a weekly scheduled job or on-demand from the dashboard.
Reads rule-classified data from BigQuery, sends low-confidence items
to an LLM for review, and writes disagreements for human triage.

Usage:
    python classify_agent.py --review                  # full review run
    python classify_agent.py --review --provider openai # use a specific LLM
    python classify_agent.py --dry-run                  # show what would be sent
"""
from __future__ import annotations

import argparse
import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
SYSTEM_PROMPT_TEMPLATE = PROMPTS_DIR / "classification_system.txt"
SEED_EXAMPLES_FILE = PROMPTS_DIR / "classification_examples.json"

SUBCAT_CONFIDENCE_THRESHOLD = 0.60
MAPPING_CONFIDENCE_NONE = "none"
QA_SAMPLE_FRACTION = 0.10
BATCH_SIZE = 25
MAX_FEEDBACK_EXAMPLES = 50


# ---------------------------------------------------------------------------
# Station definitions for the system prompt
# ---------------------------------------------------------------------------

STATION_DEFINITIONS = {
    "ST10000": "Cell Pallet / Pallet Unload / Cell Prep",
    "ST11000": "TIM Dispense (Heatsink TIM)",
    "ST12000": "Adhesive Dispense",
    "ST13000": "PCBA Press",
    "ST14000": "PCBA Fasten",
    "ST15000": "HiPot Test (Heatsink HiPot)",
    "ST22000": "Current Collector Weld / Laser Weld (TruFiber, Precitec, LWM)",
    "ST24000": "Ground Bond Test",
    "ST25000": "BMS Calibration / Pre-FSW Functional Test",
    "ST31000": "Functional Test",
    "ST33000": "Friction Stir Weld (FSW) / Enclosure Weld",
    "ST35000": "Leak Test (LeakMaster)",
    "ST36000": "Leak Re-Test",
    "ST40000": "Packout / Tray Marriage",
}


def _format_station_definitions() -> str:
    lines = ["Station ID | Process"]
    lines.append("--- | ---")
    for sid, desc in STATION_DEFINITIONS.items():
        lines.append(
            f"BASE1-MOD1-{sid} (also MOD2, CELL1, CELL2, INV1 variants) | {desc}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Build the system prompt
# ---------------------------------------------------------------------------

def _load_feedback_examples() -> str:
    """Load recent human feedback from BigQuery as few-shot examples."""
    try:
        import bq_dataset
        df = bq_dataset.read_table(
            "classification_feedback",
            where=f"created_at IS NOT NULL ORDER BY created_at DESC LIMIT {MAX_FEEDBACK_EXAMPLES}",
        )
        if df.empty:
            raise ValueError("No feedback yet")
    except Exception:
        examples_path = SEED_EXAMPLES_FILE
        if examples_path.exists():
            examples = json.loads(examples_path.read_text(encoding="utf-8"))
            return _format_examples(examples)
        return "(No feedback examples available yet. Use your domain knowledge.)"

    records = df.to_dict(orient="records")
    return _format_examples(records)


def _format_examples(examples: list[dict]) -> str:
    if not examples:
        return "(No examples available.)"
    lines = []
    for ex in examples[:MAX_FEEDBACK_EXAMPLES]:
        vendor = ex.get("vendor_name", ex.get("vendor", ""))
        desc = ex.get("item_description", ex.get("description", ""))
        station = ex.get("final_station_id", "")
        subcat = ex.get("final_subcategory", "")
        reason = ex.get("reasoning", "")
        lines.append(
            f"- {vendor} | \"{desc}\" -> station={station or 'null'}, "
            f"subcat={subcat}" + (f" ({reason})" if reason else "")
        )
    return "\n".join(lines)


def build_system_prompt() -> str:
    """Assemble the full system prompt with station defs and feedback examples."""
    template = SYSTEM_PROMPT_TEMPLATE.read_text(encoding="utf-8")
    context = ""
    try:
        import storage_backend as store

        settings = store.read_json("dashboard_settings.json")
        if isinstance(settings, dict):
            custom = str(settings.get("classification_ai_system_prompt", "") or "").strip()
            if custom:
                template = custom
            context = str(settings.get("classification_ai_domain_context", "") or "").strip()
    except Exception:
        pass

    prompt = template.replace("{station_definitions}", _format_station_definitions())
    prompt = prompt.replace("{recent_feedback}", _load_feedback_examples())
    if "{domain_context}" in prompt:
        prompt = prompt.replace("{domain_context}", context or "(none)")
    elif context:
        prompt += f"\n\nAdditional domain context:\n{context}\n"
    return prompt


# ---------------------------------------------------------------------------
# Select items for review
# ---------------------------------------------------------------------------

def _select_items_for_review(df: pd.DataFrame) -> pd.DataFrame:
    """Pick low-confidence items + a random QA sample of high-confidence ones."""
    low_conf = df[
        (pd.to_numeric(df.get("subcat_confidence", 0), errors="coerce").fillna(0) < SUBCAT_CONFIDENCE_THRESHOLD)
        | (df.get("mapping_confidence", "").astype(str) == MAPPING_CONFIDENCE_NONE)
    ]

    high_conf = df.drop(low_conf.index)
    qa_n = max(1, int(len(high_conf) * QA_SAMPLE_FRACTION))
    qa_sample = high_conf.sample(n=min(qa_n, len(high_conf)), random_state=42) if not high_conf.empty else pd.DataFrame()

    selected = pd.concat([low_conf, qa_sample], ignore_index=True)
    return selected


def _dedup_items(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    """Deduplicate by (vendor_name, item_description). Returns unique rows and a mapping of
    dedup_key -> list of original line_ids for fanning out results."""
    df = df.copy()
    df["_dedup_key"] = (
        df["vendor_name"].fillna("").astype(str).str.lower().str.strip()
        + "|||"
        + df["item_description"].fillna("").astype(str).str.lower().str.strip()
    )

    fanout: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        key = row["_dedup_key"]
        lid = str(row.get("line_id", ""))
        fanout.setdefault(key, []).append(lid)

    unique = df.drop_duplicates(subset="_dedup_key", keep="first").copy()
    unique.drop(columns=["_dedup_key"], inplace=True)
    return unique, fanout


def _row_to_item_dict(row: pd.Series) -> dict:
    """Convert a DataFrame row to the dict format the LLM expects."""
    return {
        "vendor": str(row.get("vendor_name", "")),
        "description": str(row.get("item_description", "")),
        "price_subtotal": float(pd.to_numeric(row.get("price_subtotal", 0), errors="coerce") or 0),
        "project_name": str(row.get("project_name", "")),
        "product_category": str(row.get("product_category", "")),
        "rule_station_id": str(row.get("station_id", "")),
        "rule_station_confidence": str(row.get("mapping_confidence", "")),
        "rule_subcategory": str(row.get("mfg_subcategory", "")),
        "rule_subcat_confidence": float(
            pd.to_numeric(row.get("subcat_confidence", 0), errors="coerce") or 0
        ),
        "_po_number": str(row.get("po_number", "")),
        "_date_order": str(row.get("date_order", "")),
        "_source": str(row.get("source", "")),
        "_mapping_status": str(row.get("mapping_status", "")),
    }


# ---------------------------------------------------------------------------
# Run the review
# ---------------------------------------------------------------------------

def run_review(provider: str = "", dry_run: bool = False) -> dict[str, Any]:
    """Execute a full classification review cycle.

    Returns a summary dict with counts of items reviewed, disagreements found, etc.
    """
    import bq_dataset
    from llm_adapter import get_adapter

    print("=== LLM Classification Review ===")

    # 1. Load current data
    print("  Loading po_lines from BigQuery...")
    try:
        df = bq_dataset.read_table("po_lines")
    except Exception:
        import storage_backend as store
        df = store.read_csv("capex_clean.csv")

    if df.empty:
        print("  No data to review.")
        return {"items_reviewed": 0, "disagreements": 0}

    spend = df[df.get("line_type", pd.Series(dtype=str)).astype(str) == "spend"]
    print(f"  Spend lines: {len(spend)}")

    # 2. Select items
    selected = _select_items_for_review(spend)
    print(f"  Selected for review: {len(selected)} (low-conf + QA sample)")

    unique, fanout = _dedup_items(selected)
    print(f"  After dedup: {len(unique)} unique combos")

    if dry_run:
        print(f"\n  DRY RUN: would send {len(unique)} items in {(len(unique) + BATCH_SIZE - 1) // BATCH_SIZE} batches")
        for i, (_, row) in enumerate(unique.head(5).iterrows()):
            item = _row_to_item_dict(row)
            print(f"    [{i}] {item['vendor']}: {item['description'][:60]}...")
        return {"items_reviewed": 0, "disagreements": 0, "dry_run": True, "would_send": len(unique)}

    # 3. Build prompt
    system_prompt = build_system_prompt()
    adapter = get_adapter(provider)
    print(f"  LLM provider: {type(adapter).__name__}")

    # 4. Process in batches
    all_results = []
    items_list = [_row_to_item_dict(row) for _, row in unique.iterrows()]
    line_ids = unique["line_id"].tolist() if "line_id" in unique.columns else [""] * len(unique)

    for batch_start in range(0, len(items_list), BATCH_SIZE):
        batch = items_list[batch_start:batch_start + BATCH_SIZE]
        batch_ids = line_ids[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(items_list) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} items)...")

        try:
            results = adapter.classify_batch(system_prompt, batch)
            for result, item, lid in zip(results, batch, batch_ids):
                all_results.append((result, item, lid))
        except Exception as exc:
            print(f"    ERROR in batch {batch_num}: {exc}")
            continue

    # 5. Detect disagreements
    disagreements = []
    agreements = 0
    now = datetime.now(timezone.utc)

    for result, item, line_id in all_results:
        if result.agrees_with_rules:
            agreements += 1
            continue

        disagreements.append({
            "review_id": str(uuid.uuid4()),
            "line_id": line_id,
            "po_number": item.get("_po_number", ""),
            "vendor_name": item["vendor"],
            "item_description": item["description"],
            "price_subtotal": item["price_subtotal"],
            "project_name": item.get("project_name", ""),
            "product_category": item.get("product_category", ""),
            "source": item.get("_source", ""),
            "date_order": item.get("_date_order", ""),
            "rule_station": item["rule_station_id"],
            "rule_subcat": item["rule_subcategory"],
            "rule_confidence": item["rule_subcat_confidence"],
            "rule_mapping_status": item.get("_mapping_status", ""),
            "llm_station": result.station_id or "",
            "llm_subcat": result.mfg_subcategory,
            "llm_confidence": result.subcat_confidence,
            "llm_reasoning": result.reasoning,
            "human_decision": None,
            "reviewed_by": None,
            "reviewed_at": None,
            "created_at": now,
        })

    print(f"\n  Results: {agreements} agreements, {len(disagreements)} disagreements")

    # 6. Write disagreements to BigQuery
    if disagreements:
        review_df = pd.DataFrame(disagreements)
        try:
            bq_dataset.write_table(
                "classification_reviews",
                review_df,
                write_disposition="WRITE_APPEND",
            )
            print(f"  Wrote {len(disagreements)} disagreements to classification_reviews")
        except Exception as exc:
            print(f"  WARNING: Failed to write to BigQuery ({exc})")
            out_path = Path(__file__).resolve().parent / "data" / "classification_reviews.json"
            out_path.parent.mkdir(exist_ok=True)
            review_df.to_json(out_path, orient="records", indent=2, default_handler=str)
            print(f"  Saved locally: {out_path}")

    return {
        "items_reviewed": len(all_results),
        "agreements": agreements,
        "disagreements": len(disagreements),
        "batches": (len(items_list) + BATCH_SIZE - 1) // BATCH_SIZE,
    }


# ---------------------------------------------------------------------------
# Milestone Template Generation
# ---------------------------------------------------------------------------

MILESTONE_SYSTEM_PROMPT = """You are a manufacturing finance analyst for Base Power Company, a battery energy storage system (BESS) manufacturer.

Today's date: {today}

Your task: For each Purchase Order below, generate a payment milestone schedule.

Use these inputs to make your best estimate:
- PO notes (may contain explicit payment terms like "50% deposit, 50% on delivery")
- Line item descriptions (tells you what's being purchased -- equipment vs services vs materials)
- Existing payments already made (mark these as actual/completed milestones)
- Vendor payment history (average cycle times from past POs with this vendor)
- PO date and total amount

Guidelines for estimating milestones:
- Large equipment POs ($100K+): typically 30% deposit, 40% on delivery (3-6 months), 30% after commissioning (1-2 months after delivery)
- Integration/services POs: often progress billing monthly or 50/50
- Materials POs: usually net 30 or paid on delivery
- If explicit payment terms are in the PO notes, use those
- If vendor has payment history, use their average cycle as a guide
- For POs with existing payments, include those as completed milestones and project remaining
- Milestone percentages must sum to 100%

Respond with a JSON array. Each element:
```json
{{
  "po_number": "PO10890",
  "milestones": [
    {{"label": "Deposit", "pct": 30, "expected_date": "2025-11-15", "status": "projected", "reasoning": "brief explanation"}},
    {{"label": "Delivery", "pct": 40, "expected_date": "2026-03-15", "status": "projected", "reasoning": "brief explanation"}}
  ]
}}
```

status is either "projected" (estimated) or "paid" (already paid based on payment data provided).
"""

MILESTONE_BATCH_SIZE = 5
MIN_PO_AMOUNT = 25000


def _build_milestone_context(
    po_number: str,
    po_data: pd.DataFrame,
    payment_data: pd.DataFrame,
    vendor_profiles: dict[str, dict],
) -> dict:
    """Build the context packet for one PO to send to the LLM."""
    po_lines = po_data[po_data["po_number"] == po_number]
    if po_lines.empty:
        return {}

    first = po_lines.iloc[0]
    vendor = str(first.get("vendor_name", ""))
    total = pd.to_numeric(po_lines["price_subtotal"], errors="coerce").sum()

    top_items = []
    spend_lines = po_lines[po_lines.get("line_type", pd.Series(dtype=str)).astype(str) == "spend"]
    for _, row in spend_lines.nlargest(5, "price_subtotal" if "price_subtotal" in spend_lines.columns else spend_lines.columns[0]).iterrows():
        top_items.append({
            "description": str(row.get("item_description", row.get("line_description", "")))[:120],
            "amount": float(pd.to_numeric(row.get("price_subtotal", 0), errors="coerce") or 0),
        })

    payments = []
    if not payment_data.empty and "po_number" in payment_data.columns:
        po_payments = payment_data[payment_data["po_number"] == po_number]
        for _, row in po_payments.iterrows():
            bill_date = str(row.get("bill_invoice_date", row.get("bill_posting_date", "")))
            amt = float(pd.to_numeric(row.get("bill_amount", row.get("payment_amount", 0)), errors="coerce") or 0)
            status = str(row.get("bill_payment_state", row.get("payment_state", "")))
            if amt > 0:
                payments.append({"date": bill_date[:10], "amount": amt, "status": status})

    vendor_lower = vendor.lower().strip()
    vp = vendor_profiles.get(vendor_lower, {})

    return {
        "po_number": po_number,
        "vendor": vendor,
        "total_amount": round(total, 2),
        "date_order": str(first.get("date_order", ""))[:10],
        "po_state": str(first.get("po_state", "")),
        "po_notes": str(first.get("po_notes", ""))[:500],
        "project": str(first.get("project_name", "")),
        "top_items": top_items,
        "existing_payments": payments,
        "vendor_history": {
            "avg_cycle_days": vp.get("avg_cycle_days", 0),
            "avg_deposit_pct": vp.get("avg_deposit_pct", 0),
            "avg_payment_count": vp.get("avg_payment_count", 0),
            "past_po_count": vp.get("po_count", 0),
        } if vp else None,
    }


def _load_milestone_ai_settings() -> dict[str, str]:
    """Load milestone generation prompt settings from dashboard settings."""
    try:
        import storage_backend as store

        raw = store.read_json("dashboard_settings.json")
        settings = raw if isinstance(raw, dict) else {}
    except Exception:
        settings = {}

    return {
        "program_context": str(settings.get("milestone_ai_program_context", "") or "").strip(),
        "system_prompt": str(settings.get("milestone_ai_system_prompt", "") or "").strip(),
        "user_prefix": str(settings.get("milestone_ai_user_prefix", "") or "").strip(),
    }


def _build_milestone_system_prompt(*, today: date, program_context: str, custom_template: str) -> str:
    """Render milestone system prompt from template + runtime context."""
    template = custom_template or MILESTONE_SYSTEM_PROMPT
    prompt = template.replace("{today}", str(today))
    if "{program_context}" in prompt:
        prompt = prompt.replace("{program_context}", program_context or "(none)")
    elif program_context:
        prompt += f"\n\nProgram context:\n{program_context}\n"
    return prompt


def generate_milestones(provider: str = "", dry_run: bool = False) -> dict[str, Any]:
    """Generate payment milestone templates for all major CAPEX POs using Gemini."""
    import storage_backend as store
    from llm_adapter import get_adapter

    print("=== AI Milestone Template Generation ===")

    po_data = store.read_csv("capex_clean.csv")
    if po_data.empty:
        print("  No PO data found.")
        return {"generated": 0}

    po_data["price_subtotal"] = pd.to_numeric(po_data.get("price_subtotal", 0), errors="coerce").fillna(0)

    odoo_only = po_data[po_data["source"] == "odoo"]
    po_totals = odoo_only.groupby("po_number")["price_subtotal"].sum().reset_index()
    po_totals.columns = ["po_number", "total"]
    big_pos = po_totals[po_totals["total"] >= MIN_PO_AMOUNT].sort_values("total", ascending=False)
    print(f"  POs >= ${MIN_PO_AMOUNT:,}: {len(big_pos)}")

    payment_data = store.read_csv("payment_details.csv")
    print(f"  Payment detail rows: {len(payment_data)}")

    vendor_profiles: dict[str, dict] = {}
    try:
        from payment_patterns import load_payment_data, build_po_timelines, build_vendor_profiles
        payments_raw = load_payment_data(from_bq=False)
        if not payments_raw.empty:
            timelines = build_po_timelines(payments_raw)
            profiles = build_vendor_profiles(timelines)
            for vp in profiles:
                vendor_profiles[vp["vendor_name"].lower().strip()] = vp
    except Exception:
        pass
    print(f"  Vendor profiles loaded: {len(vendor_profiles)}")

    contexts = []
    for _, row in big_pos.iterrows():
        ctx = _build_milestone_context(row["po_number"], po_data, payment_data, vendor_profiles)
        if ctx:
            contexts.append(ctx)

    print(f"  Context packets built: {len(contexts)}")

    if dry_run:
        for ctx in contexts[:3]:
            print(f"\n  [{ctx['po_number']}] {ctx['vendor'][:30]} ${ctx['total_amount']:,.0f}")
            print(f"    Items: {len(ctx['top_items'])}, Payments: {len(ctx['existing_payments'])}, Vendor history: {bool(ctx['vendor_history'])}")
        return {"generated": 0, "dry_run": True, "would_process": len(contexts)}

    ai_settings = _load_milestone_ai_settings()
    system_prompt = _build_milestone_system_prompt(
        today=date.today(),
        program_context=ai_settings["program_context"],
        custom_template=ai_settings["system_prompt"],
    )

    from google import genai
    from google.genai.types import GenerateContentConfig
    from user_google_auth import get_signed_in_user_credentials

    user_creds = get_signed_in_user_credentials()
    client_kwargs: dict[str, Any] = {
        "vertexai": True,
        "project": "mfg-eng-19197",
        "location": "us-central1",
    }
    if user_creds is not None:
        client_kwargs["credentials"] = user_creds
    client = genai.Client(**client_kwargs)
    print(f"  Using Gemini directly for milestone generation")

    all_templates = []
    for batch_start in range(0, len(contexts), MILESTONE_BATCH_SIZE):
        batch = contexts[batch_start:batch_start + MILESTONE_BATCH_SIZE]
        batch_num = batch_start // MILESTONE_BATCH_SIZE + 1
        total_batches = (len(contexts) + MILESTONE_BATCH_SIZE - 1) // MILESTONE_BATCH_SIZE
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} POs)...")

        user_prefix = ai_settings["user_prefix"]
        user_msg = ""
        if user_prefix:
            user_msg += user_prefix.strip() + "\n\n"
        user_msg += "Generate payment milestone templates for these POs:\n\n"
        for ctx in batch:
            user_msg += json.dumps(ctx) + "\n\n"

        try:
            resp = client.models.generate_content(
                model="gemini-2.5-pro",
                contents=user_msg,
                config=GenerateContentConfig(
                    system_instruction=system_prompt,
                    max_output_tokens=8192,
                    temperature=0.1,
                    response_mime_type="application/json",
                ),
            )
            text = resp.text

            start = text.find("[")
            end = text.rfind("]") + 1
            if start >= 0 and end > start:
                parsed = json.loads(text[start:end])
                for item in parsed:
                    if isinstance(item, dict) and "po_number" in item:
                        all_templates.append(item)
                print(f"    Got {len(parsed)} templates")
            else:
                try:
                    single = json.loads(text)
                    if isinstance(single, dict) and "po_number" in single:
                        all_templates.append(single)
                        print(f"    Got 1 template")
                    elif isinstance(single, list):
                        for item in single:
                            if isinstance(item, dict) and "po_number" in item:
                                all_templates.append(item)
                        print(f"    Got {len(single)} templates")
                except json.JSONDecodeError:
                    print(f"    WARNING: Could not parse response")
        except Exception as exc:
            print(f"    ERROR in batch {batch_num}: {exc}")
            continue

    print(f"\n  Templates generated: {len(all_templates)}")

    existing_templates = store.read_json("payment_templates.json")
    if not isinstance(existing_templates, list):
        existing_templates = []

    existing_po_set = {t.get("po_number") for t in existing_templates}

    saved = 0
    for tpl in all_templates:
        po_num = tpl.get("po_number", "")
        if po_num in existing_po_set:
            continue

        milestones = tpl.get("milestones", [])
        po_ctx = next((c for c in contexts if c["po_number"] == po_num), {})
        total_amount = po_ctx.get("total_amount", 0)

        for ms in milestones:
            pct = ms.get("pct", 0)
            ms["amount"] = round(total_amount * pct / 100, 2)

        existing_templates.append({
            "template_id": str(uuid.uuid4()),
            "po_number": po_num,
            "vendor_name": po_ctx.get("vendor", ""),
            "total_amount": total_amount,
            "name": f"{po_num} milestones (AI draft)",
            "milestones": milestones,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source": "ai_generated",
        })
        existing_po_set.add(po_num)
        saved += 1

    store.write_json("payment_templates.json", existing_templates)
    print(f"  Saved {saved} new draft templates ({len(existing_templates)} total)")

    return {
        "generated": len(all_templates),
        "saved": saved,
        "skipped_existing": len(all_templates) - saved,
        "total_templates": len(existing_templates),
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="LLM Classification Review & Milestone Agent")
    parser.add_argument("--review", action="store_true", help="Run a full classification review")
    parser.add_argument("--generate-milestones", action="store_true", help="Generate payment milestone templates for major POs")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be sent without calling LLM")
    parser.add_argument("--provider", default="", help="LLM provider: gemini, anthropic, openai, vertex")
    args = parser.parse_args()

    if args.generate_milestones or (args.dry_run and not args.review):
        result = generate_milestones(provider=args.provider, dry_run=args.dry_run)
        print(f"\n  Summary: {json.dumps(result, indent=2, default=str)}")
    elif args.review or args.dry_run:
        result = run_review(provider=args.provider, dry_run=args.dry_run)
        print(f"\n  Summary: {json.dumps(result, indent=2, default=str)}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
