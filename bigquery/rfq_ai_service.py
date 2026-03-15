"""
AI RFQ generation service.

Transforms vendor quote inputs (PDF + user context) into:
- normalized RFQ draft JSON
- Odoo-import-ready CSV text
- Odoo-like preview payload
"""
from __future__ import annotations

from datetime import datetime, timedelta
import csv
import difflib
import io
import json
import os
import re
import time
from pathlib import Path
from typing import Any
import uuid

import pandas as pd

from rfq_odoo_validation import load_lookup_snapshot, validate_and_canonicalize_rfq
import storage_backend as store

DEFAULT_TERMS = (
    "Base Power Standard Terms and Conditions Apply: https://www.basepowercompany.com/t-and-c "
    "Invoices must be sent to accountspayable@basepowercompany.com "
    "If applicable, Ship Fedex Collect to Account Number: 209073270"
)

RFQ_HEADERS = [
    "External ID",
    "Vendor",
    "Vendor Reference",
    "Order Deadline",
    "Expected Arrival",
    "Ask confirmation",
    "Deliver To",
    "Project",
    "Terms and Conditions",
    "Order Lines / Product",
    "Order Lines / Display Type",
    "Order Lines / Description",
    "Order Lines / Project",
    "Order Lines / Quantity",
    "Order Lines / Unit of Measure",
    "Order Lines / Unit Price",
    "Order Lines / Taxes",
]

PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "rfq_system.txt"
EXAMPLES_PATH = Path(__file__).resolve().parent / "prompts" / "rfq_examples.json"
_REF_PO_RE = re.compile(r"\bPO\d{4,}\b", flags=re.IGNORECASE)
_MAX_TEMPLATE_LINES = 30
APPROVED_SUBCATEGORIES = [
    "Consumables",
    "Controls & Electrical",
    "Design & Engineering Services",
    "Facilities & Office",
    "General & Administrative",
    "IT Equipment",
    "Integration & Commissioning",
    "MFG Tools & Shop Supplies",
    "Mechanical & Structural",
    "Process Equipment",
    "Quality & Metrology",
    "Shipping & Freight",
    "Software & Licenses",
]
DEFAULT_SUBCATEGORY = "General & Administrative"


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _norm_match(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _norm(value).lower())


def _subcategory_key(value: Any) -> str:
    text = _norm(value).lower().replace("&", " and ")
    text = re.sub(r"\band\b", " ", text)
    return _norm_match(text)


def _canonicalize_subcategory(value: Any) -> str:
    raw = _norm(value)
    if not raw:
        return ""

    key = _subcategory_key(raw)
    approved_by_key = {_subcategory_key(v): v for v in APPROVED_SUBCATEGORIES}
    exact = approved_by_key.get(key, "")
    if exact:
        return exact

    close = difflib.get_close_matches(key, list(approved_by_key.keys()), n=1, cutoff=0.78)
    if close:
        return approved_by_key[close[0]]
    return ""


def _infer_subcategory_from_line_content(line: dict[str, Any]) -> str:
    description = _norm(line.get("description")).lower()
    product = _norm(line.get("product")).lower()
    text = f"{description} {product}"

    keyword_map: list[tuple[str, tuple[str, ...]]] = [
        ("Software & Licenses", ("software", "license", "licensing", "saas", "subscription", "cloud")),
        ("Shipping & Freight", ("shipping", "freight", "logistics", "delivery", "fedex", "ups")),
        ("Quality & Metrology", ("metrology", "inspection", "calibration", "quality", "cmm", "gauge", "vision")),
        ("Integration & Commissioning", ("integration", "commissioning", "startup", "site acceptance", "sat", "fat")),
        ("Controls & Electrical", ("electrical", "control", "plc", "sensor", "cable", "breaker", "relay", "vfd")),
        ("Mechanical & Structural", ("mechanical", "structural", "frame", "bracket", "plate", "steel", "aluminum")),
        ("Process Equipment", ("equipment", "chiller", "pump", "compressor", "conveyor", "robot", "laser")),
        ("MFG Tools & Shop Supplies", ("tool", "fixture", "jig", "shop", "wrench", "drill", "solder", "workbench")),
        ("IT Equipment", ("it ", "laptop", "desktop", "monitor", "server", "router", "network", "switch")),
        ("Facilities & Office", ("facility", "office", "furniture", "janitorial", "utilities", "hvac")),
        ("Design & Engineering Services", ("engineering service", "design service", "consulting", "consultant", "cad")),
        ("Consumables", ("consumable", "adhesive", "epoxy", "solvent", "grease", "sealant", "tape", "glove")),
    ]
    for category, keywords in keyword_map:
        if any(k in text for k in keywords):
            return category
    return ""


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _short_line_description(value: Any, max_chars: int = 96) -> str:
    text = _norm(value)
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"^non-?inventory:\s*construction in process\s*", "", text, flags=re.IGNORECASE).strip()
    if len(text) <= max_chars:
        return text
    head = text[: max_chars - 3].rstrip(" ,;:-")
    return f"{head}..."


def _extract_reference_po(user_prompt: str, prior: dict[str, Any]) -> str:
    candidates = [_norm(user_prompt)]
    history = prior.get("history", []) if isinstance(prior, dict) and isinstance(prior.get("history"), list) else []
    candidates.extend([_norm(h) for h in history[-6:]])
    for source in candidates:
        if not source:
            continue
        match = _REF_PO_RE.search(source)
        if match:
            return match.group(0).upper()
    return ""


def _explicit_reference_copy_requested(user_prompt: str) -> bool:
    prompt = _norm(user_prompt).lower()
    if not prompt:
        return False
    copy_phrases = (
        "copy reference po",
        "copy the reference po",
        "same as reference po",
        "mirror reference po",
        "clone reference po",
        "exactly like reference po",
    )
    return any(p in prompt for p in copy_phrases)


def _load_payment_terms_hint(reference_po: str, vendor: str) -> str:
    """Load milestone template for the given PO/vendor and format as standardized lines.

    Output format: "Milestone 1: 25% expected date 02/15/2026; Milestone 2: 75% ..."
    """
    po = _norm(reference_po).upper()
    if not po:
        return ""
    try:
        raw = store.read_json("payment_templates.json")
    except Exception:
        return ""
    if not isinstance(raw, list):
        return ""

    vendor_norm = _norm_match(vendor)
    for item in raw:
        if not isinstance(item, dict):
            continue
        if _norm(item.get("po_number")).upper() != po:
            continue
        row_vendor = _norm(item.get("vendor_name"))
        if vendor_norm and _norm_match(row_vendor) and _norm_match(row_vendor) != vendor_norm:
            continue
        milestones = item.get("milestones", [])
        if not isinstance(milestones, list):
            return ""
        chunks: list[str] = []
        for i, milestone in enumerate(milestones, 1):
            if not isinstance(milestone, dict):
                continue
            label = _norm(milestone.get("label")) or f"Milestone {i}"
            pct = _to_float(milestone.get("pct"), 0.0)
            if pct <= 0:
                continue
            date_str = _norm(milestone.get("expected_date"))
            if date_str:
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(date_str[:10], "%Y-%m-%d")
                    date_str = d.strftime("%m/%d/%Y")
                except Exception:
                    pass
            if date_str:
                chunks.append(f"{label}: {pct:g}% expected date {date_str}")
            else:
                chunks.append(f"{label}: {pct:g}%")
        return "; ".join(chunks[:6])
    return ""


def _load_milestone_templates_for_vendor(vendor: str) -> list[dict]:
    """Find all milestone templates matching a vendor (for auto-populating the payment note)."""
    try:
        raw = store.read_json("payment_templates.json")
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    vendor_norm = _norm_match(vendor)
    if not vendor_norm:
        return []
    matches: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        row_vendor = _norm(item.get("vendor_name"))
        if _norm_match(row_vendor) == vendor_norm:
            matches.append(item)
    return matches


def _load_vendor_context(vendor: str, user_prompt: str, prior: dict[str, Any]) -> dict[str, Any]:
    context: dict[str, Any] = {
        "reference_po": _extract_reference_po(user_prompt, prior),
        "template_po": "",
        "template_lines": [],
        "projects": [],
        "terms": "",
        "payment_terms_hint": "",
        "top_subcategory": "",
        "template_quality_score": 0.0,
        "template_quality_notes": [],
    }
    vendor_name = _norm(vendor)
    if not vendor_name:
        return context

    df = store.read_csv("capex_clean.csv")
    if df.empty or "vendor_name" not in df.columns:
        return context

    vendor_key = _norm_match(vendor_name)
    vendor_df = df[df["vendor_name"].astype(str).map(_norm_match) == vendor_key].copy()
    if vendor_df.empty:
        return context

    if "date_order" in vendor_df.columns:
        vendor_df["_date_order"] = pd.to_datetime(vendor_df["date_order"], errors="coerce")
    else:
        vendor_df["_date_order"] = pd.NaT
    vendor_df["_amount"] = pd.to_numeric(vendor_df.get("po_amount_total", 0), errors="coerce").fillna(0.0)

    po_values = vendor_df["po_number"].astype(str).str.strip()
    po_values = po_values[po_values != ""]
    if po_values.empty:
        return context

    template_po = ""
    reference_po = _norm(context.get("reference_po")).upper()
    if reference_po and (po_values.str.upper() == reference_po).any():
        template_po = reference_po
    else:
        stats = (
            vendor_df.groupby("po_number", dropna=True)
            .agg(
                row_count=("po_number", "size"),
                latest_date=("_date_order", "max"),
                amount=("_amount", "max"),
            )
            .reset_index()
        )
        if not stats.empty:
            stats = stats.sort_values(
                by=["latest_date", "amount", "row_count"],
                ascending=[False, False, False],
                na_position="last",
            )
            template_po = _norm(stats.iloc[0].get("po_number"))

    if not template_po:
        return context

    template_rows = vendor_df[vendor_df["po_number"].astype(str).str.upper() == template_po.upper()].copy()
    if template_rows.empty:
        return context
    if "line_sequence" in template_rows.columns:
        template_rows["_line_sequence"] = pd.to_numeric(template_rows["line_sequence"], errors="coerce")
        template_rows = template_rows.sort_values(by="_line_sequence", na_position="last")

    # Lightweight quality check so historical context is guidance, not blind truth.
    quality_notes: list[str] = []
    score = 0.0
    row_count = len(template_rows)
    if row_count >= 4:
        score += 0.3
    else:
        quality_notes.append("few_lines")

    prices = pd.to_numeric(template_rows.get("price_unit", 0), errors="coerce").fillna(0.0)
    non_zero_price_ratio = float((prices > 0).mean()) if row_count else 0.0
    if non_zero_price_ratio >= 0.85:
        score += 0.25
    else:
        quality_notes.append("missing_prices")

    projects_raw = template_rows.get("project_name", pd.Series(dtype=str)).astype(str).map(_norm)
    project_fill_ratio = float((projects_raw != "").mean()) if row_count else 0.0
    if project_fill_ratio >= 0.8:
        score += 0.2
    else:
        quality_notes.append("missing_project_allocation")

    terms_series = template_rows.get("po_notes", pd.Series(dtype=str)).astype(str).map(_norm)
    if any(bool(v) for v in terms_series.tolist()):
        score += 0.25
    else:
        quality_notes.append("missing_terms")

    template_lines: list[dict[str, Any]] = []
    for _, row in template_rows.iterrows():
        description = _short_line_description(row.get("item_description") or row.get("line_description"))
        template_lines.append(
            {
                "product": _norm(row.get("product_category")) or "Non-Inventory: Construction in Process",
                "description": description or "RFQ line",
                "project": _norm(row.get("project_name")),
                "quantity": max(_to_float(row.get("product_qty"), 1.0), 1.0),
                "uom": _norm(row.get("product_uom")) or "Unit",
                "unit_price": max(_to_float(row.get("price_unit"), 0.0), 0.0),
                "subcategory": _norm(row.get("mfg_subcategory")),
            }
        )
        if len(template_lines) >= _MAX_TEMPLATE_LINES:
            break

    terms = ""
    notes = template_rows.get("po_notes")
    if notes is not None:
        non_blank = notes.astype(str).map(_norm)
        non_blank = non_blank[non_blank != ""]
        if not non_blank.empty:
            terms = non_blank.mode().iloc[0] if not non_blank.mode().empty else non_blank.iloc[0]
    if not terms:
        v_notes = vendor_df.get("po_notes")
        if v_notes is not None:
            non_blank = v_notes.astype(str).map(_norm)
            non_blank = non_blank[non_blank != ""]
            if not non_blank.empty:
                terms = non_blank.mode().iloc[0] if not non_blank.mode().empty else non_blank.iloc[0]

    project_values = [_norm(p) for p in template_rows.get("project_name", pd.Series(dtype=str)).tolist()]
    projects = sorted({p for p in project_values if p}, key=lambda p: p.lower())
    all_subcats = vendor_df.get("mfg_subcategory", pd.Series(dtype=str)).astype(str).map(_norm)
    all_subcats = all_subcats[all_subcats != ""]
    top_subcategory = ""
    if not all_subcats.empty:
        top_subcategory = (
            all_subcats.mode().iloc[0] if not all_subcats.mode().empty else all_subcats.iloc[0]
        )

    context["template_po"] = template_po
    context["template_lines"] = template_lines
    context["projects"] = projects
    context["terms"] = _norm(terms)
    context["payment_terms_hint"] = _load_payment_terms_hint(template_po, vendor_name)
    context["milestone_templates"] = _load_milestone_templates_for_vendor(vendor_name)
    context["top_subcategory"] = _norm(top_subcategory)
    context["template_quality_score"] = round(score, 2)
    context["template_quality_notes"] = quality_notes
    if score < 0.45:
        # Keep only non-structural hints when template quality is weak.
        context["template_lines"] = []
    return context


def _format_odoo_dt(value: Any, fallback: datetime) -> str:
    raw = _norm(value)
    if not raw:
        return fallback.strftime("%m/%d/%Y %H:%M:%S")
    for fmt in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.strftime("%m/%d/%Y %H:%M:%S")
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.strftime("%m/%d/%Y %H:%M:%S")
    except ValueError:
        return fallback.strftime("%m/%d/%Y %H:%M:%S")


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = text.strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
        return {}
    except json.JSONDecodeError:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}
    try:
        parsed = json.loads(raw[start : end + 1])
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    if not pdf_bytes:
        return ""
    candidates: list[str] = []

    # Primary extractor: pypdf
    try:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(pdf_bytes))
        chunks: list[str] = []
        for page in reader.pages:
            chunks.append(page.extract_text() or "")
        candidates.append("\n".join(chunks).strip())
    except Exception:
        pass

    # Secondary extractor: pdfplumber (often better layout extraction)
    try:
        import pdfplumber

        chunks = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                chunks.append(page.extract_text() or "")
        candidates.append("\n".join(chunks).strip())
    except Exception:
        pass

    # Tertiary extractor: PyMuPDF text layer
    try:
        import fitz

        chunks = []
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for page in doc:
            chunks.append(page.get_text("text") or "")
        doc.close()
        candidates.append("\n".join(chunks).strip())
    except Exception:
        pass

    best = ""
    for text in candidates:
        if len(_norm(text)) > len(_norm(best)):
            best = text
    return _norm(best)


def _load_prompt_defaults() -> tuple[str, list[dict[str, Any]]]:
    system_prompt = ""
    examples: list[dict[str, Any]] = []
    try:
        system_prompt = PROMPT_PATH.read_text(encoding="utf-8")
    except Exception:
        system_prompt = ""
    try:
        raw_examples = json.loads(EXAMPLES_PATH.read_text(encoding="utf-8"))
        if isinstance(raw_examples, list):
            examples = [e for e in raw_examples if isinstance(e, dict)]
    except Exception:
        examples = []
    return system_prompt, examples


def _call_llm_json(
    *,
    provider: str,
    system_prompt: str,
    user_content: str,
    pdf_bytes: bytes | None = None,
) -> dict[str, Any]:
    selected = (provider or "gemini").strip().lower()

    if selected == "gemini":
        from google import genai
        from google.genai.types import GenerateContentConfig, Part
        from user_google_auth import get_signed_in_user_credentials

        user_creds = get_signed_in_user_credentials()
        client_kwargs: dict[str, Any] = {
            "vertexai": True,
            "project": os.environ.get("BQ_ANALYTICS_PROJECT", "mfg-eng-19197"),
            "location": os.environ.get("RFQ_AI_LOCATION", "us-central1"),
        }
        if user_creds is not None:
            client_kwargs["credentials"] = user_creds
        client = genai.Client(**client_kwargs)
        model = os.environ.get("RFQ_AI_MODEL", "gemini-2.5-pro")
        contents: Any = user_content
        if pdf_bytes:
            contents = [
                user_content,
                Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
            ]
        response = client.models.generate_content(
            model=model,
            contents=contents,
            config=GenerateContentConfig(
                system_instruction=system_prompt,
                response_mime_type="application/json",
                temperature=0.1,
                max_output_tokens=8192,
            ),
        )
        return _extract_json_object(response.text or "")

    if selected == "openai":
        import openai

        client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))
        model = os.environ.get("OPENAI_MODEL", "gpt-4o")
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
        )
        return _extract_json_object(response.choices[0].message.content or "")

    if selected == "anthropic":
        import anthropic

        client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
        model = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
        response = client.messages.create(
            model=model,
            max_tokens=8192,
            system=system_prompt,
            messages=[{"role": "user", "content": user_content}],
        )
        text = response.content[0].text if response.content else ""
        return _extract_json_object(text)

    raise ValueError(f"Unsupported RFQ AI provider: {selected}")


def _build_fallback_draft(
    vendor: str,
    prompt: str,
    lookups: dict[str, Any],
    *,
    vendor_context: dict[str, Any] | None = None,
    allow_template_lines: bool = False,
) -> dict[str, Any]:
    now = datetime.now()
    values = (lookups.get("values", {}) if isinstance(lookups, dict) else {}) or {}
    context = vendor_context if isinstance(vendor_context, dict) else {}
    default_project = (values.get("projects", []) or [""])[0] if values.get("projects") else ""
    if not default_project and context.get("projects"):
        default_project = _norm((context.get("projects") or [""])[0])
    default_deliver = (values.get("deliver_to", []) or [""])[0] if values.get("deliver_to") else ""
    default_tax = (values.get("taxes", []) or [""])[0] if values.get("taxes") else ""
    products = values.get("products", []) or []
    default_product = "Non-Inventory: Construction in Process"
    if default_product not in products and products:
        default_product = products[0]
    terms = DEFAULT_TERMS

    template_lines = (
        context.get("template_lines", [])
        if (allow_template_lines and isinstance(context.get("template_lines"), list))
        else []
    )
    normalized_template_lines: list[dict[str, Any]] = []
    for line in template_lines:
        if not isinstance(line, dict):
            continue
        taxes = [default_tax] if default_tax else []
        normalized_template_lines.append(
            {
                "product": _norm(line.get("product")) or default_product,
                "description": _short_line_description(line.get("description")) or "Vendor item",
                "project": _norm(line.get("project")) or default_project,
                "display_type": _norm(line.get("display_type")),
                "quantity": max(_to_float(line.get("quantity"), 1.0), 1.0),
                "uom": _norm(line.get("uom")) or "Unit",
                "unit_price": max(_to_float(line.get("unit_price"), 0.0), 0.0),
                "taxes": taxes,
            }
        )
        if len(normalized_template_lines) >= _MAX_TEMPLATE_LINES:
            break

    fallback_lines = (
        normalized_template_lines
        if normalized_template_lines
        else [
            {
                "product": default_product,
                "description": _short_line_description(prompt) or "Vendor item",
                "project": default_project,
                "display_type": "",
                "quantity": 1.0,
                "uom": "Unit",
                "unit_price": 0.0,
                "taxes": [default_tax] if default_tax else [],
            }
        ]
    )
    return {
        "header": {
            "vendor": vendor,
            "vendor_reference": _norm(context.get("template_po")) or f"AI-RFQ-{now.strftime('%Y%m%d')}",
            "order_deadline": (now + timedelta(days=5)).strftime("%m/%d/%Y %H:%M:%S"),
            "expected_arrival": (now + timedelta(days=10)).strftime("%m/%d/%Y %H:%M:%S"),
            "ask_confirmation": 1,
            "deliver_to": default_deliver,
            "project": "",
            "terms_and_conditions": terms,
        },
        "lines": fallback_lines,
        "notes": ["Fallback draft was used because AI output was unavailable or invalid."],
    }


def _vendor_match_errors(
    *,
    selected_vendor: str,
    quote_text: str,
    detected_vendor: str,
    known_vendors: list[str],
) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    selected = _norm(selected_vendor)
    if not selected:
        return errors

    def _punct_norm(s: str) -> str:
        return re.sub(r"[.,;:!?]+$", "", _norm(s)).strip().lower()

    if _norm(detected_vendor) and _punct_norm(detected_vendor) != _punct_norm(selected):
        errors.append(
            {
                "field": "vendor",
                "message": (
                    f"Vendor mismatch: selected '{selected}' but AI detected '{_norm(detected_vendor)}' from quote context."
                ),
                "candidates": [_norm(detected_vendor)],
            }
        )
        return errors

    if not _norm(quote_text):
        return errors

    quote_norm = _norm_match(quote_text)
    selected_norm = _norm_match(selected)
    if selected_norm and selected_norm in quote_norm:
        return errors

    candidates: list[str] = []
    for vendor in known_vendors[:5000]:
        cleaned = _norm(vendor)
        cleaned_norm = _norm_match(cleaned)
        if cleaned and cleaned_norm and cleaned_norm in quote_norm:
            candidates.append(cleaned)
        if len(candidates) >= 8:
            break

    errors.append(
        {
            "field": "vendor",
            "message": f"Selected vendor '{selected}' was not detected in the quote content.",
            "candidates": candidates,
        }
    )
    return errors


def _extract_payment_milestones_from_text(quote_text: str) -> str:
    text = _norm(quote_text)
    if not text:
        return ""
    chunks: list[str] = []
    for line in re.split(r"[\r\n]+", text):
        candidate = _norm(line)
        if not candidate:
            continue
        if not re.search(r"(payment|milestone|deposit|advance|net\s*\d+|due|acceptance|delivery)", candidate, re.IGNORECASE):
            continue
        if not re.search(r"(\d+\s*%|net\s*\d+|upon|before|after)", candidate, re.IGNORECASE):
            continue
        chunks.append(candidate)
        if len(chunks) >= 3:
            break
    merged = " | ".join(chunks)
    if len(merged) > 320:
        merged = merged[:317].rstrip(" ,;:-") + "..."
    return merged


def _merge_payment_note_into_terms(terms: str, payment_note: str) -> str:
    base = _norm(terms)
    note = _norm(payment_note)
    if not note:
        return base
    if _norm_match(note) and _norm_match(note) in _norm_match(base):
        return base
    merged = f"{base} Payment Milestones: {note}".strip()
    return re.sub(r"\s+", " ", merged).strip()


def _extract_subcat_tag(description: str) -> str:
    match = re.search(r"\[Sub-Cat:\s*([^\]]*)\]", _norm(description), flags=re.IGNORECASE)
    return _norm(match.group(1)) if match else ""


def _extract_part_number(description: str) -> str:
    text = _norm(description)
    if not text:
        return ""
    first = text.split(" ", 1)[0].strip(",:;")
    if len(first) < 3:
        return ""
    if re.search(r"[A-Za-z]", first) and re.search(r"\d", first):
        return first
    return ""


def _format_standard_description(description: str, subcategory: str) -> str:
    desc = _norm(description)
    if not desc:
        desc = "Vendor item"
    if desc.lower().startswith("payment:"):
        return desc
    stripped = re.sub(r"\s*\[Sub-Cat:\s*[^\]]*\]\s*$", "", desc, flags=re.IGNORECASE).strip()
    part = _extract_part_number(stripped)
    if part and stripped.lower().startswith(part.lower()):
        rest = _norm(stripped[len(part):])
    else:
        rest = stripped
    main = f"{part} ({rest})" if part and rest else (stripped or "Vendor item")
    subcat = _canonicalize_subcategory(subcategory) or DEFAULT_SUBCATEGORY
    return f"{main}\n[Sub-Cat: {subcat}]"


def _add_subcat_tag(description: str, subcategory: str) -> str:
    return _format_standard_description(description, subcategory)


def _infer_line_subcategory(line: dict[str, Any], vendor_context: dict[str, Any], line_index: int) -> str:
    if not isinstance(line, dict):
        return ""
    existing = _canonicalize_subcategory(_extract_subcat_tag(_norm(line.get("description"))))
    if existing:
        return existing

    template_lines = vendor_context.get("template_lines", []) if isinstance(vendor_context.get("template_lines"), list) else []
    if line_index < len(template_lines):
        t_line = template_lines[line_index]
        if isinstance(t_line, dict):
            candidate = _canonicalize_subcategory(_norm(t_line.get("subcategory")))
            if candidate:
                return candidate

    top = _canonicalize_subcategory(_norm(vendor_context.get("top_subcategory")))
    if top:
        return top

    inferred = _canonicalize_subcategory(_infer_subcategory_from_line_content(line))
    if inferred:
        return inferred
    return DEFAULT_SUBCATEGORY


def _apply_subcategory_tags(draft: dict[str, Any], vendor_context: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    data = draft if isinstance(draft, dict) else {}
    lines = data.get("lines", []) if isinstance(data.get("lines"), list) else []
    tagged_count = 0
    corrected_count = 0
    for idx, line in enumerate(lines):
        if not isinstance(line, dict):
            continue
        if _norm(line.get("display_type")).lower() in {"line_note", "note"}:
            continue
        if _norm(line.get("description")).lower().startswith("payment:"):
            continue
        existing_raw = _extract_subcat_tag(_norm(line.get("description")))
        subcat = _infer_line_subcategory(line, vendor_context, idx)
        if existing_raw and _canonicalize_subcategory(existing_raw) != subcat:
            corrected_count += 1
        line["description"] = _add_subcat_tag(_norm(line.get("description")), subcat)
        tagged_count += 1
    data["lines"] = lines
    if tagged_count:
        warnings.append(
            f"Applied approved MFG subcategory tags to {tagged_count} line(s); corrected {corrected_count} non-approved tag(s)."
        )
    return data, warnings


def _build_context_gap_suggestions(
    *,
    quote_text: str,
    user_prompt: str,
    vendor_context: dict[str, Any],
    draft: dict[str, Any],
    payment_milestones_note: str,
) -> list[str]:
    gaps: list[str] = []
    quote_available = bool(_norm(quote_text))
    ref_po = _norm(vendor_context.get("reference_po"))
    template_quality = _to_float(vendor_context.get("template_quality_score"), 0.0)

    if not quote_available:
        gaps.append("Quote PDF text was not extracted. Add key line details manually or attach OCR-friendly PDF.")
    if not ref_po:
        gaps.append("No reference PO provided. Add one if you want closer allocation matching.")
    if template_quality and template_quality < 0.6:
        gaps.append("Historical PO quality is weak; verify allocations, pricing, and payment terms carefully.")
    if len(_norm(user_prompt)) < 20:
        gaps.append("Prompt context is sparse. Add scope, split logic, and allocation intent for better output.")

    lines = draft.get("lines", []) if isinstance(draft.get("lines"), list) else []
    has_payment_note_line = any(
        isinstance(line, dict)
        and (
            _norm(line.get("display_type")).lower() in {"line_note", "note"}
            or _norm(line.get("description")).lower().startswith("payment:")
        )
        for line in lines
    )
    if not _norm(payment_milestones_note) and not has_payment_note_line:
        gaps.append("Payment milestones are missing or unclear. Provide milestone wording in context.")
    if len(lines) <= 1:
        gaps.append("Only one line detected. Confirm whether quote should be split into multiple detailed lines.")

    if len(gaps) > 8:
        gaps = gaps[:8]
    return gaps


def _normalize_payment_note(note: str) -> str:
    text = _norm(note)
    if not text:
        return ""
    text = text.replace("|", "; ")
    text = re.sub(r"\s+", " ", text).strip(" ;")
    if not text:
        return ""
    if not text.lower().startswith("payment:"):
        text = f"Payment: {text}"
    return text


def _apply_payment_note_line(
    draft: dict[str, Any],
    *,
    payment_milestones_note: str,
    quote_text: str,
    vendor_context: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    data = draft if isinstance(draft, dict) else {}
    lines = data.get("lines", []) if isinstance(data.get("lines"), list) else []
    if not lines:
        data["lines"] = lines
        return data, warnings

    preferred_note = _normalize_payment_note(_norm(payment_milestones_note))
    if not preferred_note:
        preferred_note = _normalize_payment_note(_extract_payment_milestones_from_text(quote_text))
    if not preferred_note:
        preferred_note = _normalize_payment_note(_norm(vendor_context.get("payment_terms_hint")))
    if not preferred_note:
        milestone_templates = vendor_context.get("milestone_templates", [])
        if isinstance(milestone_templates, list) and milestone_templates:
            best = milestone_templates[0]
            milestones = best.get("milestones", [])
            if isinstance(milestones, list) and milestones:
                parts: list[str] = []
                for i, ms in enumerate(milestones, 1):
                    if not isinstance(ms, dict):
                        continue
                    label = _norm(ms.get("label")) or f"Milestone {i}"
                    pct = _to_float(ms.get("pct"), 0.0)
                    if pct <= 0:
                        continue
                    date_str = _norm(ms.get("expected_date"))
                    if date_str:
                        try:
                            from datetime import datetime as _dt
                            d = _dt.strptime(date_str[:10], "%Y-%m-%d")
                            date_str = d.strftime("%m/%d/%Y")
                        except Exception:
                            pass
                    if date_str:
                        parts.append(f"{label}: {pct:g}% expected date {date_str}")
                    else:
                        parts.append(f"{label}: {pct:g}%")
                if parts:
                    preferred_note = _normalize_payment_note("; ".join(parts[:6]))
    if not preferred_note:
        return data, warnings

    note_line = {
        "product": "",
        "description": preferred_note,
        "project": "",
        "display_type": "line_note",
        "quantity": 0.0,
        "uom": "",
        "unit_price": 0.0,
        "taxes": [],
    }

    non_note_lines: list[dict[str, Any]] = []
    for line in lines:
        if not isinstance(line, dict):
            continue
        if _norm(line.get("display_type")).lower() in {"line_note", "note"}:
            continue
        if _norm(line.get("description")).lower().startswith("payment:"):
            continue
        non_note_lines.append(line)
    non_note_lines.append(note_line)
    data["lines"] = non_note_lines
    warnings.append("Added consolidated payment milestone note line at end of RFQ.")
    return data, warnings


def _fmt_seconds(seconds: float) -> float:
    return round(max(seconds, 0.0), 3)


def _enrich_draft_from_vendor_context(
    draft: dict[str, Any],
    *,
    vendor_context: dict[str, Any],
    user_prompt: str,
    quote_text: str,
    payment_milestones_note: str,
    ai_output_used: bool,
    allow_reference_copy: bool = False,
    has_primary_quote_signal: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    data = draft if isinstance(draft, dict) else {}
    header = data.get("header", {}) if isinstance(data.get("header"), dict) else {}
    lines = data.get("lines", []) if isinstance(data.get("lines"), list) else []
    template_lines = vendor_context.get("template_lines", []) if isinstance(vendor_context.get("template_lines"), list) else []
    template_po = _norm(vendor_context.get("template_po")).upper()
    reference_po = _norm(vendor_context.get("reference_po")).upper()

    no_pdf_text = not _norm(quote_text)
    prompt_has_similarity_request = any(
        token in _norm(user_prompt).lower()
        for token in ("similar", "same as", "like po", "mirror po", "match po", "use po")
    )
    use_template = (not ai_output_used) and bool(template_lines) and (
        (
            allow_reference_copy
            and reference_po
            and template_po
            and reference_po == template_po
            and (no_pdf_text or not has_primary_quote_signal)
        )
        or (
            no_pdf_text
            and (not has_primary_quote_signal)
            and (len(lines) <= 2 or prompt_has_similarity_request)
        )
    )
    if use_template:
        enriched_lines: list[dict[str, Any]] = []
        for line in template_lines:
            if not isinstance(line, dict):
                continue
            enriched_lines.append(
                {
                    "product": _norm(line.get("product")) or "Non-Inventory: Construction in Process",
                    "description": _short_line_description(line.get("description")) or "RFQ line",
                    "project": _norm(line.get("project")),
                    "display_type": _norm(line.get("display_type")),
                    "quantity": max(_to_float(line.get("quantity"), 1.0), 1.0),
                    "uom": _norm(line.get("uom")) or "Unit",
                    "unit_price": max(_to_float(line.get("unit_price"), 0.0), 0.0),
                    "taxes": list(line.get("taxes", [])) if isinstance(line.get("taxes"), list) else [],
                }
            )
            if len(enriched_lines) >= _MAX_TEMPLATE_LINES:
                break
        if enriched_lines:
            data["lines"] = enriched_lines
            lines = enriched_lines
            if template_po:
                warnings.append(f"Applied vendor historical template lines from {template_po}.")

    header["terms_and_conditions"] = DEFAULT_TERMS

    for line in lines:
        if not isinstance(line, dict):
            continue
        if _norm(line.get("display_type")).lower() in {"line_note", "note"}:
            continue
        line["description"] = _short_line_description(line.get("description")) or "Vendor item"

    data["header"] = header
    data["lines"] = lines
    return data, warnings


def _normalize_ai_output(
    ai_output: dict[str, Any],
    *,
    vendor: str,
    lookups: dict[str, Any],
) -> dict[str, Any]:
    now = datetime.now()
    values = (lookups.get("values", {}) if isinstance(lookups, dict) else {}) or {}

    payload = ai_output.get("rfq", ai_output) if isinstance(ai_output, dict) else {}
    if not isinstance(payload, dict):
        payload = {}
    header = payload.get("header", {}) if isinstance(payload.get("header"), dict) else {}
    lines = payload.get("lines", []) if isinstance(payload.get("lines"), list) else []
    notes = payload.get("notes", [])
    if not isinstance(notes, list):
        notes = []

    default_project = _norm(header.get("project"))
    default_deliver = _norm(header.get("deliver_to")) or (
        (values.get("deliver_to", []) or [""])[0] if values.get("deliver_to") else ""
    )
    default_tax = (values.get("taxes", []) or [""])[0] if values.get("taxes") else ""

    normalized_header = {
        "vendor": _norm(header.get("vendor")) or _norm(vendor),
        "vendor_reference": _norm(header.get("vendor_reference")) or f"AI-RFQ-{now.strftime('%Y%m%d')}",
        "order_deadline": _format_odoo_dt(header.get("order_deadline"), now + timedelta(days=5)),
        "expected_arrival": _format_odoo_dt(header.get("expected_arrival"), now + timedelta(days=10)),
        "ask_confirmation": int(bool(header.get("ask_confirmation", True))),
        "deliver_to": default_deliver,
        "project": default_project,
        "terms_and_conditions": DEFAULT_TERMS,
    }

    normalized_lines: list[dict[str, Any]] = []
    for line in lines:
        if not isinstance(line, dict):
            continue
        display_type = _norm(line.get("display_type")).lower()
        is_note_line = display_type in {"line_note", "note"}
        taxes = line.get("taxes", [])
        if isinstance(taxes, str):
            taxes = [taxes]
        if not isinstance(taxes, list):
            taxes = []
        taxes_clean = [_norm(t) for t in taxes if _norm(t)]
        if (not taxes_clean) and default_tax and not is_note_line:
            taxes_clean = [default_tax]

        normalized_lines.append(
            {
                "product": "" if is_note_line else (_norm(line.get("product")) or "Non-Inventory: Construction in Process"),
                "description": _norm(line.get("description")) or _norm(line.get("item")) or "Vendor item",
                "project": "" if is_note_line else (_norm(line.get("project")) or default_project),
                "display_type": "line_note" if is_note_line else "",
                "quantity": 0.0 if is_note_line else _to_float(line.get("quantity"), 1.0),
                "uom": "" if is_note_line else (_norm(line.get("uom")) or "Unit"),
                "unit_price": 0.0 if is_note_line else _to_float(line.get("unit_price"), 0.0),
                "taxes": [] if is_note_line else taxes_clean,
            }
        )

    if not normalized_lines:
        normalized_lines.append(
            {
                "product": "Non-Inventory: Construction in Process",
                "description": "Vendor item",
                "project": default_project,
                "display_type": "",
                "quantity": 1.0,
                "uom": "Unit",
                "unit_price": 0.0,
                "taxes": [default_tax] if default_tax else [],
            }
        )

    return {
        "header": normalized_header,
        "lines": normalized_lines,
        "notes": [str(n) for n in notes if _norm(n)],
    }


def _build_csv_text(draft: dict[str, Any]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=RFQ_HEADERS, extrasaction="ignore")
    writer.writeheader()

    header = draft.get("header", {}) if isinstance(draft, dict) else {}
    external_id = _norm(header.get("external_id")) or f"rfq_ai_{uuid.uuid4().hex[:8]}"
    lines = draft.get("lines", []) if isinstance(draft.get("lines"), list) else []
    for line in lines:
        if not isinstance(line, dict):
            continue
        is_note_line = _norm(line.get("display_type")).lower() in {"line_note", "note"}
        row = {
            "External ID": external_id,
            "Vendor": _norm(header.get("vendor")),
            "Vendor Reference": _norm(header.get("vendor_reference")),
            "Order Deadline": _norm(header.get("order_deadline")),
            "Expected Arrival": _norm(header.get("expected_arrival")),
            "Ask confirmation": int(bool(header.get("ask_confirmation", 1))),
            "Deliver To": _norm(header.get("deliver_to")),
            "Project": _norm(header.get("project")),
            "Terms and Conditions": _norm(header.get("terms_and_conditions")),
            "Order Lines / Product": "" if is_note_line else _norm(line.get("product")),
            "Order Lines / Display Type": "line_note" if is_note_line else "",
            "Order Lines / Description": _norm(line.get("description")),
            "Order Lines / Project": "" if is_note_line else _norm(line.get("project")),
            "Order Lines / Quantity": "" if is_note_line else _to_float(line.get("quantity"), 0.0),
            "Order Lines / Unit of Measure": "" if is_note_line else (_norm(line.get("uom")) or "Unit"),
            "Order Lines / Unit Price": "" if is_note_line else _to_float(line.get("unit_price"), 0.0),
            "Order Lines / Taxes": "" if is_note_line else ",".join([_norm(t) for t in line.get("taxes", []) if _norm(t)]),
        }
        writer.writerow(row)
    return output.getvalue()


def _tax_rate_from_label(tax_label: str) -> float:
    match = re.search(r"(\d+(?:\.\d+)?)\s*%", _norm(tax_label))
    if not match:
        return 0.0
    return _to_float(match.group(1), 0.0) / 100.0


def _build_preview(draft: dict[str, Any]) -> dict[str, Any]:
    header = draft.get("header", {}) if isinstance(draft, dict) else {}
    lines = draft.get("lines", []) if isinstance(draft.get("lines"), list) else []

    preview_lines: list[dict[str, Any]] = []
    untaxed_total = 0.0
    tax_total = 0.0
    for idx, line in enumerate(lines):
        if not isinstance(line, dict):
            continue
        is_note_line = _norm(line.get("display_type")).lower() in {"line_note", "note"}
        qty = _to_float(line.get("quantity"), 0.0)
        unit_price = _to_float(line.get("unit_price"), 0.0)
        subtotal = 0.0 if is_note_line else (qty * unit_price)
        primary_tax = _norm((line.get("taxes", []) or [""])[0]) if isinstance(line.get("taxes"), list) else ""
        tax_rate = 0.0 if is_note_line else _tax_rate_from_label(primary_tax)
        tax_amount = 0.0 if is_note_line else (subtotal * tax_rate)
        total = subtotal + tax_amount
        untaxed_total += subtotal
        tax_total += tax_amount
        preview_lines.append(
            {
                "line_no": idx + 1,
                "product": _norm(line.get("product")),
                "description": _norm(line.get("description")),
                "project": _norm(line.get("project")),
                "display_type": "line_note" if is_note_line else "",
                "quantity": qty,
                "uom": "" if is_note_line else (_norm(line.get("uom")) or "Unit"),
                "unit_price": unit_price,
                "tax_label": primary_tax,
                "tax_rate": tax_rate,
                "line_subtotal": subtotal,
                "line_tax": tax_amount,
                "line_total": total,
            }
        )

    return {
        "header": {
            "vendor": _norm(header.get("vendor")),
            "vendor_reference": _norm(header.get("vendor_reference")),
            "order_deadline": _norm(header.get("order_deadline")),
            "expected_arrival": _norm(header.get("expected_arrival")),
            "ask_confirmation": int(bool(header.get("ask_confirmation", 1))),
            "deliver_to": _norm(header.get("deliver_to")),
            "project": _norm(header.get("project")),
            "terms_and_conditions": _norm(header.get("terms_and_conditions")),
        },
        "lines": preview_lines,
        "totals": {
            "untaxed_amount": untaxed_total,
            "tax_amount": tax_total,
            "total_amount": untaxed_total + tax_total,
        },
    }


def generate_rfq_payload(
    *,
    vendor: str,
    user_prompt: str,
    payment_milestones_note: str,
    pdf_bytes: bytes | None,
    pdf_filename: str,
    prior_context: dict[str, Any] | None,
    settings: dict[str, Any] | None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    cfg = settings if isinstance(settings, dict) else {}
    prior = prior_context if isinstance(prior_context, dict) else {}
    payment_milestones_note = _norm(payment_milestones_note) or _norm(prior.get("payment_milestones_note"))

    validation_mode = _norm(cfg.get("rfq_validation_mode")) or "hybrid"
    lookup = load_lookup_snapshot(validation_mode=validation_mode)
    t_lookup = time.perf_counter()

    system_default, examples = _load_prompt_defaults()
    system_prompt = _norm(cfg.get("rfq_ai_system_prompt")) or system_default
    user_prefix = _norm(cfg.get("rfq_ai_user_prefix"))
    provider = _norm(cfg.get("rfq_ai_provider")) or "gemini"
    vendor_context = _load_vendor_context(_norm(vendor), _norm(user_prompt), prior)
    reference_copy_requested = _explicit_reference_copy_requested(_norm(user_prompt))

    quote_text = _extract_pdf_text(pdf_bytes or b"")
    if not quote_text:
        quote_text = _norm(prior.get("quote_text"))
    can_use_pdf_direct = bool(pdf_bytes) and provider.lower() == "gemini"
    has_primary_quote_signal = bool(_norm(quote_text)) or can_use_pdf_direct
    t_pdf = time.perf_counter()

    prompt_parts = [
        f"Vendor: {_norm(vendor)}",
        f"User prompt: {_norm(user_prompt)}",
        f"Payment milestones note (user): {_norm(payment_milestones_note)}",
        (
            "Reference PO policy: style/context only by default. "
            "Do NOT copy reference PO line items, quantities, or prices unless user explicitly requests copy."
        ),
        f"Reference copy explicitly requested: {str(reference_copy_requested).lower()}",
        (
            "Generation policy: prioritize quote/PDF and explicit user instructions. "
            "Use historical vendor/PO context as soft hints only; do not blindly copy prior PO mistakes."
        ),
    ]
    if user_prefix:
        prompt_parts.append(f"User prefix: {user_prefix}")
    if any(
        [
            _norm(vendor_context.get("reference_po")),
            _norm(vendor_context.get("template_po")),
            _norm(vendor_context.get("terms")),
            bool(vendor_context.get("template_lines")),
        ]
    ):
        template_lines_sample = (
            (vendor_context.get("template_lines", []) or [])[:10]
            if (reference_copy_requested and not has_primary_quote_signal)
            else []
        )
        prompt_parts.append(
            "Vendor historical context:\n"
            + json.dumps(
                {
                    "reference_po_requested": vendor_context.get("reference_po", ""),
                    "template_po": vendor_context.get("template_po", ""),
                    "reference_mode": (
                        "copy_allowed"
                        if (reference_copy_requested and not has_primary_quote_signal)
                        else "style_only"
                    ),
                    "template_quality_score": vendor_context.get("template_quality_score", 0.0),
                    "template_quality_notes": vendor_context.get("template_quality_notes", []),
                    "projects": vendor_context.get("projects", [])[:8],
                    "historical_terms": _norm(vendor_context.get("terms")),
                    "historical_payment_terms_hint": _norm(vendor_context.get("payment_terms_hint")),
                    "template_lines_sample": template_lines_sample,
                },
                ensure_ascii=True,
            )
        )
    if prior.get("last_draft"):
        prompt_parts.append("Prior draft JSON:\n" + json.dumps(prior.get("last_draft"), ensure_ascii=True))
    if lookup.get("values"):
        values = lookup["values"]
        prompt_parts.append(
            "Known Odoo-like lookup values:\n"
            + json.dumps(
                {
                    "vendors": (values.get("vendors", []) or [])[:200],
                    "products": (values.get("products", []) or [])[:200],
                    "projects": (values.get("projects", []) or [])[:200],
                    "taxes": (values.get("taxes", []) or [])[:100],
                    "deliver_to": (values.get("deliver_to", []) or [])[:100],
                    "uoms": (values.get("uoms", []) or [])[:50],
                },
                ensure_ascii=True,
            )
        )
    if examples:
        prompt_parts.append("Examples:\n" + json.dumps(examples[:3], ensure_ascii=True))
    prompt_parts.append("Quote text:\n" + quote_text[:50000])
    user_content = "\n\n".join(prompt_parts)

    model_warnings: list[str] = []
    ai_output: dict[str, Any] = {}
    requested_ref_po = _norm(vendor_context.get("reference_po")).upper()
    resolved_template_po = _norm(vendor_context.get("template_po")).upper()
    if requested_ref_po and requested_ref_po != resolved_template_po:
        model_warnings.append(
            f"Reference PO {requested_ref_po} was not found for vendor {_norm(vendor)}. Using nearest vendor history."
        )
    if quote_text or can_use_pdf_direct:
        try:
            ai_output = _call_llm_json(
                provider=provider,
                system_prompt=system_prompt,
                user_content=user_content,
                pdf_bytes=pdf_bytes,
            )
        except Exception as exc:  # pragma: no cover - provider/config dependent
            model_warnings.append(f"AI generation fallback used: {exc}")
    else:
        model_warnings.append("No PDF text was extracted; generated from prompt context only.")
    t_llm = time.perf_counter()

    if ai_output:
        draft = _normalize_ai_output(ai_output, vendor=vendor, lookups=lookup)
    else:
        draft = _build_fallback_draft(
            _norm(vendor),
            _norm(user_prompt),
            lookup,
            vendor_context=vendor_context,
            allow_template_lines=(reference_copy_requested and not has_primary_quote_signal),
        )

    draft, enrichment_warnings = _enrich_draft_from_vendor_context(
        draft,
        vendor_context=vendor_context,
        user_prompt=_norm(user_prompt),
        quote_text=quote_text,
        payment_milestones_note=_norm(payment_milestones_note),
        ai_output_used=bool(ai_output),
        allow_reference_copy=reference_copy_requested,
        has_primary_quote_signal=has_primary_quote_signal,
    )
    # Apply user-selected header overrides (deliver-to, project) so the AI
    # output doesn't silently revert the user's pick to the default.
    user_deliver_to = _norm(cfg.get("_user_deliver_to"))
    user_header_project = _norm(cfg.get("_user_header_project"))
    if user_deliver_to:
        draft.setdefault("header", {})["deliver_to"] = user_deliver_to
    if user_header_project:
        draft.setdefault("header", {})["project"] = user_header_project

    draft, payment_line_warnings = _apply_payment_note_line(
        draft,
        payment_milestones_note=_norm(payment_milestones_note),
        quote_text=quote_text,
        vendor_context=vendor_context,
    )
    draft, subcat_warnings = _apply_subcategory_tags(draft, vendor_context)
    t_enrich = time.perf_counter()

    validation = validate_and_canonicalize_rfq(draft, validation_mode=validation_mode)
    raw_payload = ai_output.get("rfq", ai_output) if isinstance(ai_output, dict) else {}
    raw_header = raw_payload.get("header", {}) if isinstance(raw_payload, dict) else {}
    detected_vendor = _norm(raw_header.get("vendor")) if isinstance(raw_header, dict) else ""
    vendor_errors = _vendor_match_errors(
        selected_vendor=vendor,
        quote_text=quote_text,
        detected_vendor=detected_vendor,
        known_vendors=(lookup.get("values", {}).get("vendors", []) if isinstance(lookup.get("values"), dict) else []),
    )
    if vendor_errors:
        validation.setdefault("errors", []).extend(vendor_errors)
        validation["blocking_error_count"] = len(validation.get("errors", []))

    for warning in model_warnings:
        validation.setdefault("warnings", []).append({"field": "ai", "message": warning})
    for warning in enrichment_warnings:
        validation.setdefault("warnings", []).append({"field": "historical_context", "message": warning})
    for warning in payment_line_warnings:
        validation.setdefault("warnings", []).append({"field": "payment_milestones", "message": warning})
    for warning in subcat_warnings:
        validation.setdefault("warnings", []).append({"field": "subcategory_tagging", "message": warning})

    canonical_draft = validation.get("draft", draft)
    context_gaps = _build_context_gap_suggestions(
        quote_text=quote_text,
        user_prompt=_norm(user_prompt),
        vendor_context=vendor_context,
        draft=canonical_draft,
        payment_milestones_note=_norm(payment_milestones_note),
    )
    for gap in context_gaps:
        validation.setdefault("warnings", []).append({"field": "context_gap", "message": gap})
    csv_text = _build_csv_text(canonical_draft)
    preview = _build_preview(canonical_draft)
    t_validate = time.perf_counter()

    history = prior.get("history", []) if isinstance(prior.get("history"), list) else []
    if _norm(user_prompt):
        history.append(_norm(user_prompt))
    history = history[-8:]

    revision_context = {
        "vendor": _norm(vendor),
        "quote_text": quote_text[:120000],
        "last_draft": canonical_draft,
        "history": history,
        "payment_milestones_note": _norm(payment_milestones_note),
    }
    external_id = _norm(canonical_draft.get("header", {}).get("external_id")) or f"rfq_ai_{uuid.uuid4().hex[:8]}"
    csv_filename = f"{external_id}.csv"
    completed_at = time.perf_counter()
    timing = {
        "lookup_s": _fmt_seconds(t_lookup - started_at),
        "pdf_extract_s": _fmt_seconds(t_pdf - t_lookup),
        "llm_s": _fmt_seconds(t_llm - t_pdf),
        "enrich_s": _fmt_seconds(t_enrich - t_llm),
        "validate_render_s": _fmt_seconds(t_validate - t_enrich),
        "total_s": _fmt_seconds(completed_at - started_at),
    }

    return {
        "status": "ok",
        "provider": provider,
        "draft": canonical_draft,
        "validation": {
            "errors": validation.get("errors", []),
            "warnings": validation.get("warnings", []),
            "blocking_error_count": validation.get("blocking_error_count", 0),
        },
        "preview": preview,
        "csv_text": csv_text,
        "csv_filename": csv_filename,
        "revision_context": revision_context,
        "lookup_values": lookup.get("values", {}),
        "pdf_filename": _norm(pdf_filename),
        "meta": {
            "timing": timing,
            "context_gaps": context_gaps,
            "reference_po": _norm(vendor_context.get("reference_po")),
            "template_po": _norm(vendor_context.get("template_po")),
            "template_quality_score": _to_float(vendor_context.get("template_quality_score"), 0.0),
        },
    }
