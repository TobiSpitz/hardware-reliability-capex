"""
RFQ lookup + validation helpers for Odoo import readiness.

Validation strategy:
- BQ mirror lookups are always available.
- Optional live Odoo XML-RPC lookups are merged when configured.
- Draft values are canonicalized to exact lookup strings where possible.
"""
from __future__ import annotations

from copy import deepcopy
import difflib
import re
import time
from typing import Any

import pandas as pd

import storage_backend as store
from odoo_client import OdooClient

DEFAULT_TAXES = [
    "Purchase: ATX Tax 8.25%",
]

DEFAULT_DELIVER_TO = [
    "tx-austin-hq-riverside: Receipts",
    "305 S Congress (Statesmen): Receipts",
]

_CACHE_TTL_SECONDS = 300
_LOOKUP_CACHE: dict[str, dict[str, Any]] = {}
_ADDRESS_WORD_RE = re.compile(
    r"\b(street|st|avenue|ave|road|rd|drive|dr|lane|ln|boulevard|blvd|court|ct|way|highway|hwy|parkway|pkwy|circle|cir|suite|ste|receipts|dock|building|bldg|run|xing|crossing|trail|trl)\b",
    re.IGNORECASE,
)
_PROJECT_CODE_RE = re.compile(
    r"^(CIP[-:]|BF\d*[-:]|[A-Z]{2,10}-[A-Z0-9])",
    re.IGNORECASE,
)


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _norm_l(value: Any) -> str:
    return _norm(value).lower()


def _sorted_unique(values: list[str]) -> list[str]:
    cleaned = sorted({_norm(v) for v in values if _norm(v)}, key=lambda v: v.lower())
    return cleaned


def _add_values(target: dict[str, set[str]], key: str, values: list[str]) -> None:
    bucket = target.setdefault(key, set())
    for value in values:
        cleaned = _norm(value)
        if cleaned:
            bucket.add(cleaned)


def _is_address_like_project(value: Any) -> bool:
    text = _norm(value)
    if not text:
        return False
    if not re.match(r"^\d", text):
        return False
    # Keep likely manufacturing/project code patterns even when numeric.
    if ":" in text or re.search(r"\b(CIP|BF\d+|ST\d+)\b", text, re.IGNORECASE):
        return False
    lower = text.lower()
    if re.search(r"\([0-9a-f]{4,}\)\s*$", lower):
        return True
    if re.match(r"^\d+\s+[nsew]\b", lower):
        return True
    return bool(_ADDRESS_WORD_RE.search(lower))


def _looks_like_project_code(value: Any) -> bool:
    text = _norm(value)
    if not text:
        return False
    if _PROJECT_CODE_RE.search(text):
        return True
    if ":" in text and re.search(r"\bST\d{3,}\b", text, re.IGNORECASE):
        return True
    return False


def _project_prefix(value: Any) -> str:
    text = _norm(value)
    if not text:
        return ""
    m = re.match(r"^([A-Za-z0-9]{2,12})[-:]", text)
    return m.group(1).upper() if m else ""


def _clean_project_values(
    values: list[str],
    *,
    preferred: set[str] | None = None,
    preferred_prefixes: set[str] | None = None,
) -> list[str]:
    preferred_norm = {_norm(v) for v in (preferred or set()) if _norm(v)}
    preferred_pref = {p.upper() for p in (preferred_prefixes or set()) if _norm(p)}
    cleaned: list[str] = []
    for value in values:
        normed = _norm(value)
        if not normed:
            continue
        if normed in preferred_norm:
            cleaned.append(normed)
            continue
        if _is_address_like_project(normed):
            continue
        # When we have a preferred/core set, only include extra values that
        # look like real project-code entries, not generic department labels.
        if preferred_norm:
            if preferred_pref:
                if _project_prefix(normed) not in preferred_pref:
                    continue
            elif not _looks_like_project_code(normed):
                continue
        cleaned.append(normed)
    return cleaned


def _cache_get(key: str) -> dict[str, Any] | None:
    item = _LOOKUP_CACHE.get(key)
    if not item:
        return None
    if (time.time() - float(item.get("ts", 0))) > _CACHE_TTL_SECONDS:
        return None
    value = item.get("value")
    return deepcopy(value) if isinstance(value, dict) else None


def _cache_set(key: str, value: dict[str, Any]) -> None:
    _LOOKUP_CACHE[key] = {"ts": time.time(), "value": deepcopy(value)}


def _source_table(name: str) -> str:
    import bq_dataset
    return bq_dataset.source_table(name)


def _run_source_query(sql: str) -> list[str]:
    """Run a SQL query against the Odoo source project via service account."""
    try:
        import bq_dataset

        df = bq_dataset.run_source_query(sql)
        if df.empty:
            return []
        values = df.iloc[:, 0].astype(str).map(_norm).tolist()
        return [v for v in values if v]
    except Exception:
        return []


def _fetch_odoo_public_lookups() -> dict[str, list[str]]:
    value_expr = (
        "COALESCE("
        "NULLIF(JSON_VALUE(SAFE.PARSE_JSON(CAST(name AS STRING)), '$.en_US'), ''), "
        "TRIM(CAST(name AS STRING))"
        ")"
    )
    vendors = _run_source_query(
        f"""
        SELECT value
        FROM (
            SELECT DISTINCT TRIM(CAST(v.name AS STRING)) AS value
            FROM {_source_table("purchase_order")} po
            JOIN {_source_table("res_partner")} v ON v.id = po.partner_id
            WHERE v.name IS NOT NULL
              AND COALESCE(v.active, TRUE) = TRUE

            UNION DISTINCT

            SELECT DISTINCT TRIM(CAST(name AS STRING)) AS value
            FROM {_source_table("res_partner")}
            WHERE COALESCE(active, TRUE) = TRUE
              AND COALESCE(SAFE_CAST(supplier_rank AS INT64), 0) > 0
              AND name IS NOT NULL
        )
        WHERE value IS NOT NULL
        ORDER BY value
        LIMIT 20000
        """
    )
    projects = _run_source_query(
        f"""
        SELECT value
        FROM (
            SELECT DISTINCT {value_expr} AS value
            FROM {_source_table("account_analytic_account")}
            WHERE COALESCE(active, TRUE) = TRUE
              AND name IS NOT NULL
        )
        WHERE value IS NOT NULL
        ORDER BY REGEXP_CONTAINS(CAST(value AS STRING), r'^\\d') ASC, value
        LIMIT 20000
        """
    )
    products = _run_source_query(
        f"""
        SELECT DISTINCT {value_expr} AS value
        FROM {_source_table("product_template")}
        WHERE COALESCE(active, TRUE) = TRUE
          AND COALESCE(purchase_ok, TRUE) = TRUE
          AND name IS NOT NULL
        ORDER BY value
        LIMIT 8000
        """
    )
    taxes = _run_source_query(
        f"""
        SELECT DISTINCT TRIM(CAST(name AS STRING)) AS value
        FROM {_source_table("account_tax")}
        WHERE COALESCE(active, TRUE) = TRUE
          AND CAST(type_tax_use AS STRING) IN ('purchase', 'none')
          AND name IS NOT NULL
        ORDER BY value
        LIMIT 2000
        """
    )
    deliver_to = _run_source_query(
        f"""
        SELECT DISTINCT TRIM(CAST(name AS STRING)) AS value
        FROM {_source_table("stock_picking_type")}
        WHERE CAST(code AS STRING) = 'incoming'
          AND name IS NOT NULL
        ORDER BY value
        LIMIT 1000
        """
    )
    uoms = _run_source_query(
        f"""
        SELECT DISTINCT {value_expr} AS value
        FROM {_source_table("uom_uom")}
        WHERE COALESCE(active, TRUE) = TRUE
          AND name IS NOT NULL
        ORDER BY value
        LIMIT 500
        """
    )

    return {
        "vendors": _sorted_unique(vendors),
        "products": _sorted_unique(products),
        "projects": _sorted_unique(projects),
        "taxes": _sorted_unique(taxes),
        "deliver_to": _sorted_unique(deliver_to),
        "uoms": _sorted_unique(uoms),
    }


def _fetch_bq_lookups(*, force_refresh: bool = False) -> dict[str, list[str]]:
    if not force_refresh:
        cached = _cache_get("bq")
        if cached:
            return cached  # type: ignore[return-value]

    values: dict[str, set[str]] = {
        "vendors": set(),
        "products": set(),
        "projects": set(),
        "taxes": set(DEFAULT_TAXES),
        "deliver_to": set(DEFAULT_DELIVER_TO),
        "uoms": {"Unit"},
    }

    source_values = _fetch_odoo_public_lookups()
    for key in values.keys():
        _add_values(values, key, source_values.get(key, []))

    df = store.read_csv("capex_clean.csv")
    if df.empty:
        out = {k: _sorted_unique(list(v)) for k, v in values.items()}
        _cache_set("bq", out)
        return out

    odoo_df = df[df.get("source", pd.Series(dtype=str)).astype(str).str.lower() == "odoo"].copy()
    if odoo_df.empty:
        odoo_df = df
    preferred_df = odoo_df
    if "is_mfg" in odoo_df.columns:
        is_mfg = odoo_df["is_mfg"].astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})
        if bool(is_mfg.any()):
            preferred_df = odoo_df[is_mfg]
    preferred_projects = set(preferred_df.get("project_name", pd.Series(dtype=str)).astype(str).map(_norm).tolist())
    preferred_prefixes = {_project_prefix(v) for v in preferred_projects if _project_prefix(v)}

    # Always merge local CAPEX mirror for project relevance / common use coverage.
    _add_values(values, "projects", list(preferred_projects))

    # Always merge local mirror vendor values so lookups include recently synced
    # vendors even when upstream source is delayed/incomplete.
    _add_values(values, "vendors", odoo_df.get("vendor_name", pd.Series(dtype=str)).astype(str).tolist())

    # Product lookup should be strict Odoo-import values; avoid noisy tokenized fallbacks.
    product_candidates = set(source_values.get("products", []))
    if not product_candidates:
        product_candidates = set(
            odoo_df.get("product_category", pd.Series(dtype=str)).astype(str).str.strip().tolist()
        )
    product_candidates.add("Non-Inventory: Construction in Process")
    _add_values(values, "products", list(product_candidates))
    values["projects"] = set(
        _clean_project_values(
            list(values.get("projects", set())),
            preferred=preferred_projects,
            preferred_prefixes=preferred_prefixes,
        )
    )

    out = {k: _sorted_unique(list(v)) for k, v in values.items()}
    _cache_set("bq", out)
    return out


def _fetch_live_odoo_lookups(*, force_refresh: bool = False) -> tuple[dict[str, list[str]], list[str]]:
    if not force_refresh:
        cached = _cache_get("live")
        if cached:
            return cached.get("values", {}), cached.get("warnings", [])

    warnings: list[str] = []
    values: dict[str, set[str]] = {
        "vendors": set(),
        "products": set(),
        "projects": set(),
        "taxes": set(),
        "deliver_to": set(),
        "uoms": set(),
    }

    client = OdooClient()
    if not client.is_configured:
        warnings.append("Live Odoo validation unavailable (missing Odoo credentials).")
        out = {k: [] for k in values.keys()}
        _cache_set("live", {"values": out, "warnings": warnings})
        return out, warnings

    try:
        vendors = client._execute(  # noqa: SLF001 - internal helper is already used in project
            "res.partner",
            "search_read",
            domain=[["supplier_rank", ">", 0], ["active", "=", True]],
            fields=["name"],
            limit=2000,
        )
        _add_values(values, "vendors", [v.get("name", "") for v in vendors if isinstance(v, dict)])

        products = client._execute(
            "product.product",
            "search_read",
            domain=[["active", "=", True]],
            fields=["name"],
            limit=3000,
        )
        _add_values(values, "products", [p.get("name", "") for p in products if isinstance(p, dict)])

        projects = client._execute(
            "account.analytic.account",
            "search_read",
            domain=[["active", "=", True]],
            fields=["name"],
            limit=2000,
        )
        _add_values(values, "projects", [p.get("name", "") for p in projects if isinstance(p, dict)])

        taxes = client._execute(
            "account.tax",
            "search_read",
            domain=[["active", "=", True], ["type_tax_use", "in", ["purchase", "none"]]],
            fields=["name"],
            limit=2000,
        )
        _add_values(values, "taxes", [t.get("name", "") for t in taxes if isinstance(t, dict)])

        deliver_ops = client._execute(
            "stock.picking.type",
            "search_read",
            domain=[["code", "=", "incoming"]],
            fields=["name"],
            limit=500,
        )
        _add_values(values, "deliver_to", [d.get("name", "") for d in deliver_ops if isinstance(d, dict)])

        uoms = client._execute(
            "uom.uom",
            "search_read",
            domain=[["active", "=", True]],
            fields=["name"],
            limit=200,
        )
        _add_values(values, "uoms", [u.get("name", "") for u in uoms if isinstance(u, dict)])
    except Exception as exc:  # pragma: no cover - network/config dependent
        warnings.append(f"Live Odoo lookup failed: {exc}")

    out = {k: _sorted_unique(list(v)) for k, v in values.items()}
    _cache_set("live", {"values": out, "warnings": warnings})
    return out, warnings


def load_lookup_snapshot(
    validation_mode: str = "hybrid",
    *,
    force_live: bool = False,
    force_refresh: bool = False,
) -> dict[str, Any]:
    mode = _norm_l(validation_mode) or "hybrid"
    bq_values = _fetch_bq_lookups(force_refresh=force_refresh)
    live_values, live_warnings = (
        _fetch_live_odoo_lookups(force_refresh=(force_refresh or force_live))
        if mode in {"hybrid", "live_only"}
        else ({}, [])
    )

    if mode == "live_only":
        merged = {k: live_values.get(k, []) for k in bq_values.keys()}
    elif mode == "bq_only":
        merged = bq_values
    else:
        merged = {}
        for key in bq_values.keys():
            merged[key] = _sorted_unique((bq_values.get(key, []) or []) + (live_values.get(key, []) or []))

    for key in ("taxes", "deliver_to"):
        if not merged.get(key):
            merged[key] = bq_values.get(key, [])
    merged["projects"] = _sorted_unique(_clean_project_values(merged.get("projects", []) or []))

    return {
        "mode": mode,
        "values": merged,
        "warnings": live_warnings,
        "force_live": bool(force_live),
    }


def _strip_trailing_punct(s: str) -> str:
    return re.sub(r"[.,;:!?]+$", "", s).strip()


def _canonicalize(
    value: str,
    options: list[str],
) -> tuple[str, str | None, list[str]]:
    """
    Return canonical value, error message, and candidate list.
    """
    raw = _norm(value)
    if not raw:
        return "", None, []
    if not options:
        return raw, None, []

    lower_map: dict[str, list[str]] = {}
    for opt in options:
        lower_map.setdefault(opt.lower(), []).append(opt)

    exact = lower_map.get(raw.lower(), [])
    if len(exact) == 1:
        return exact[0], None, []
    if len(exact) > 1:
        return raw, f"Ambiguous match for '{raw}'.", exact[:10]

    # Retry after stripping trailing punctuation (e.g. "Precitec, Inc." vs "Precitec, Inc").
    stripped = _strip_trailing_punct(raw)
    stripped_map: dict[str, list[str]] = {}
    for opt in options:
        stripped_map.setdefault(_strip_trailing_punct(opt).lower(), []).append(opt)

    stripped_exact = stripped_map.get(stripped.lower(), [])
    if len(stripped_exact) == 1:
        return stripped_exact[0], None, []

    contains = [opt for opt in options if raw.lower() in opt.lower()]
    if len(contains) == 1:
        return contains[0], None, [contains[0]]
    if len(contains) > 1:
        return raw, f"Multiple matches for '{raw}'.", contains[:10]

    close = difflib.get_close_matches(raw, options, n=5, cutoff=0.55)
    return raw, f"No match found for '{raw}'.", close


def validate_and_canonicalize_rfq(
    draft: dict[str, Any],
    *,
    validation_mode: str = "hybrid",
) -> dict[str, Any]:
    data = deepcopy(draft)
    lookup = load_lookup_snapshot(validation_mode=validation_mode)
    values = lookup.get("values", {})
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = [{"field": "lookup", "message": w} for w in lookup.get("warnings", [])]

    header = data.setdefault("header", {})
    lines = data.setdefault("lines", [])

    required_header_fields = {
        "vendor": values.get("vendors", []),
        "deliver_to": values.get("deliver_to", []),
    }
    for field, options in required_header_fields.items():
        canonical, err, candidates = _canonicalize(_norm(header.get(field, "")), options)
        if canonical:
            header[field] = canonical
        if not _norm(header.get(field, "")):
            errors.append({"field": field, "message": f"Missing required header field '{field}'."})
        elif err:
            errors.append({"field": field, "message": err, "candidates": candidates})

    if not isinstance(lines, list) or not lines:
        errors.append({"field": "lines", "message": "At least one RFQ line is required."})
        lines = []
        data["lines"] = lines

    for idx, line in enumerate(lines):
        if not isinstance(line, dict):
            errors.append({"field": "line", "row": idx, "message": "Invalid line object."})
            continue

        display_type = _norm_l(line.get("display_type", ""))
        is_note_line = display_type in {"line_note", "note"}
        if is_note_line:
            line["display_type"] = "line_note"
            line["product"] = ""
            line["project"] = ""
            line["quantity"] = 0.0
            line["uom"] = ""
            line["unit_price"] = 0.0
            line["taxes"] = []
            if not _norm(line.get("description", "")):
                errors.append({"field": "Order Lines / Description", "row": idx, "message": "Note line requires description."})
            continue

        line["project"] = _norm(line.get("project", "")) or _norm(header.get("project", ""))
        product, product_err, product_candidates = _canonicalize(
            _norm(line.get("product", "")),
            values.get("products", []),
        )
        if product:
            line["product"] = product
        if not _norm(line.get("product", "")):
            errors.append({"field": "Order Lines / Product", "row": idx, "message": "Product is required."})
        elif product_err:
            errors.append(
                {"field": "Order Lines / Product", "row": idx, "message": product_err, "candidates": product_candidates}
            )

        line_project_raw = _norm(line.get("project", ""))
        line_project, line_project_err, line_project_candidates = _canonicalize(
            line_project_raw,
            values.get("projects", []),
        )
        if line_project:
            line["project"] = line_project
        if line_project_err:
            allow_new_project = (
                line_project_raw
                and line_project_err.startswith("No match found")
                and _looks_like_project_code(line_project_raw)
                and not _is_address_like_project(line_project_raw)
            )
            if allow_new_project:
                line["project"] = line_project_raw
                warnings.append(
                    {
                        "field": "Order Lines / Project",
                        "row": idx,
                        "message": f"Project '{line_project_raw}' not in lookup snapshot; keeping as potential new project.",
                        "candidates": line_project_candidates,
                    }
                )
            else:
                errors.append(
                    {
                        "field": "Order Lines / Project",
                        "row": idx,
                        "message": line_project_err,
                        "candidates": line_project_candidates,
                    }
                )

        qty = float(line.get("quantity", 0) or 0)
        unit_price = float(line.get("unit_price", 0) or 0)
        if qty <= 0:
            errors.append({"field": "Order Lines / Quantity", "row": idx, "message": "Quantity must be > 0."})
        if unit_price < 0:
            errors.append({"field": "Order Lines / Unit Price", "row": idx, "message": "Unit price must be >= 0."})
        line["quantity"] = qty
        line["unit_price"] = unit_price

        uom = _norm(line.get("uom", "Unit")) or "Unit"
        uom_canonical, uom_err, uom_candidates = _canonicalize(uom, values.get("uoms", []) or ["Unit"])
        line["uom"] = uom_canonical or "Unit"
        if uom_err and values.get("uoms"):
            warnings.append(
                {"field": "Order Lines / Unit of Measure", "row": idx, "message": uom_err, "candidates": uom_candidates}
            )

        taxes_raw = line.get("taxes", [])
        if isinstance(taxes_raw, str):
            taxes_raw = [taxes_raw]
        taxes_clean = [_norm(t) for t in taxes_raw if _norm(t)]
        if not taxes_clean and values.get("taxes"):
            taxes_clean = [values["taxes"][0]]
            warnings.append(
                {"field": "Order Lines / Taxes", "row": idx, "message": "No tax provided. Defaulted to first known purchase tax."}
            )

        canonical_taxes: list[str] = []
        for tax in taxes_clean:
            tax_canonical, tax_err, tax_candidates = _canonicalize(tax, values.get("taxes", []))
            if tax_canonical:
                canonical_taxes.append(tax_canonical)
            if tax_err:
                errors.append(
                    {"field": "Order Lines / Taxes", "row": idx, "message": tax_err, "candidates": tax_candidates}
                )
        line["taxes"] = _sorted_unique(canonical_taxes)

    # Header project is optional when all line projects are explicitly set.
    header_project = _norm(header.get("project", ""))
    if header_project:
        canon_header_project, hp_err, hp_candidates = _canonicalize(header_project, values.get("projects", []))
        if canon_header_project:
            header["project"] = canon_header_project
        if hp_err:
            allow_new_header_project = (
                hp_err.startswith("No match found")
                and _looks_like_project_code(header_project)
                and not _is_address_like_project(header_project)
            )
            if allow_new_header_project:
                header["project"] = header_project
                warnings.append(
                    {
                        "field": "project",
                        "message": f"Header project '{header_project}' not in lookup snapshot; keeping as potential new project.",
                        "candidates": hp_candidates,
                    }
                )
            else:
                warnings.append({"field": "project", "message": hp_err, "candidates": hp_candidates})
    else:
        missing_line_project_rows = [
            idx for idx, line in enumerate(lines)
            if isinstance(line, dict)
            and _norm_l(line.get("display_type", "")) not in {"line_note", "note"}
            and not _norm(line.get("project", ""))
        ]
        if missing_line_project_rows:
            errors.append(
                {
                    "field": "project",
                    "message": "Header project is blank and some line projects are missing.",
                    "rows": missing_line_project_rows[:20],
                }
            )
        else:
            warnings.append(
                {
                    "field": "project",
                    "message": "Header project is blank. This is allowed because line-level projects are populated.",
                }
            )

    return {
        "draft": data,
        "errors": errors,
        "warnings": warnings,
        "blocking_error_count": len(errors),
        "lookup_snapshot": lookup,
    }
