"""
Utilities for importing station forecast values from Google Sheets.

This module expects a Google OAuth access token for the current user
and reads data from the Google Sheets API.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import requests as http_requests

_STATION_PATTERN = re.compile(r"^BASE\d+-(MOD|CELL|INV)\d+.*$")


class SheetImportError(RuntimeError):
    """Raised when a sheet cannot be parsed or imported safely."""


@dataclass(frozen=True)
class SheetRef:
    """A parsed Google Sheet URL reference."""

    spreadsheet_id: str
    gid: int | None


def parse_sheet_ref(url: str) -> SheetRef:
    """Parse spreadsheet ID and optional gid from a Google Sheets URL."""
    parsed = urlparse((url or "").strip())
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", parsed.path)
    if not match:
        raise SheetImportError("Invalid Google Sheets URL: missing spreadsheet ID.")
    qs = parse_qs(parsed.query)
    gid_raw = qs.get("gid", [None])[0]
    gid = int(gid_raw) if gid_raw and str(gid_raw).isdigit() else None
    return SheetRef(spreadsheet_id=match.group(1), gid=gid)


def _api_get_json(url: str, access_token: str) -> dict[str, Any]:
    response = http_requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    if response.status_code >= 400:
        detail = response.text[:300].replace("\n", " ")
        raise SheetImportError(f"Google Sheets API error ({response.status_code}): {detail}")
    payload = response.json()
    if not isinstance(payload, dict):
        raise SheetImportError("Unexpected Google Sheets API response format.")
    return payload


def _resolve_sheet_title(
    access_token: str,
    spreadsheet_id: str,
    gid: int | None,
) -> str:
    meta_url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
        "?fields=sheets(properties(sheetId,title))"
    )
    payload = _api_get_json(meta_url, access_token)
    sheets = payload.get("sheets", [])
    if not isinstance(sheets, list) or not sheets:
        raise SheetImportError("Spreadsheet has no visible worksheets.")

    if gid is None:
        props = sheets[0].get("properties", {}) if isinstance(sheets[0], dict) else {}
        title = str(props.get("title", "")).strip()
        if not title:
            raise SheetImportError("Unable to resolve worksheet title.")
        return title

    for item in sheets:
        if not isinstance(item, dict):
            continue
        props = item.get("properties", {})
        if not isinstance(props, dict):
            continue
        if int(props.get("sheetId", -1)) == gid:
            title = str(props.get("title", "")).strip()
            if title:
                return title
    raise SheetImportError(f"Worksheet gid {gid} not found in spreadsheet.")


def read_sheet_values(spreadsheet_id: str, gid: int | None, access_token: str) -> list[list[Any]]:
    """Read all tabular values from the worksheet referenced by gid."""
    title = _resolve_sheet_title(access_token, spreadsheet_id, gid)
    escaped_title = title.replace("'", "''")
    rng = quote(f"'{escaped_title}'!A:ZZ", safe="")
    values_url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{rng}"
        "?valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING"
    )
    payload = _api_get_json(values_url, access_token)
    values = payload.get("values", [])
    if not isinstance(values, list) or not values:
        raise SheetImportError("Worksheet is empty.")
    return values


def _normalize_header_name(name: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name or "").strip().lower()).strip("_")


def _to_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("$", "").replace(",", "")
    text = re.sub(r"\s+", "", text)
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    try:
        return float(text)
    except ValueError:
        return None


def _find_station_column(rows: list[list[Any]], headers: list[str]) -> int:
    preferred = {"station_id", "station", "stationid", "station_code"}
    for idx, h in enumerate(headers):
        compact = h.replace("_", "")
        if h in preferred or compact in preferred:
            return idx

    sample_rows = rows[1: min(len(rows), 51)]
    best_idx = -1
    best_hits = -1
    for idx in range(len(headers)):
        hits = 0
        for row in sample_rows:
            cell = row[idx] if idx < len(row) else ""
            sid = str(cell or "").strip().upper()
            if _STATION_PATTERN.match(sid):
                hits += 1
        if hits > best_hits:
            best_hits = hits
            best_idx = idx

    if best_idx < 0 or best_hits <= 0:
        raise SheetImportError("Could not detect a station ID column.")
    return best_idx


def _find_value_column(rows: list[list[Any]], headers: list[str], station_idx: int) -> int:
    preferred_tokens = (
        "forecasted_cost",
        "forecast",
        "cost",
        "total_cost",
        "value",
        "budget",
    )
    for idx, h in enumerate(headers):
        if idx == station_idx:
            continue
        if any(tok in h for tok in preferred_tokens):
            return idx

    sample_rows = rows[1: min(len(rows), 51)]
    best_idx = -1
    best_hits = -1
    for idx in range(len(headers)):
        if idx == station_idx:
            continue
        hits = 0
        for row in sample_rows:
            cell = row[idx] if idx < len(row) else ""
            if _to_float(cell) is not None:
                hits += 1
        if hits > best_hits:
            best_hits = hits
            best_idx = idx

    if best_idx < 0 or best_hits <= 0:
        raise SheetImportError("Could not detect a numeric forecast/cost column.")
    return best_idx


def normalize_forecast_rows(rows: list[list[Any]]) -> dict[str, Any]:
    """
    Normalize sheet rows into station forecast updates.

    Returns:
      {
        "updates": dict[str, float],
        "diagnostics": {...}
      }
    """
    if not rows:
        raise SheetImportError("No rows found in worksheet.")
    header_row = rows[0]
    headers = [_normalize_header_name(v) for v in header_row]
    if not headers:
        raise SheetImportError("Worksheet header row is empty.")

    station_idx = _find_station_column(rows, headers)
    value_idx = _find_value_column(rows, headers, station_idx)

    updates: dict[str, float] = {}
    invalid_station = 0
    invalid_value = 0
    duplicate_station = 0

    for row in rows[1:]:
        sid_raw = row[station_idx] if station_idx < len(row) else ""
        sid = str(sid_raw or "").strip().upper()
        if not _STATION_PATTERN.match(sid):
            invalid_station += 1
            continue

        val_raw = row[value_idx] if value_idx < len(row) else ""
        val = _to_float(val_raw)
        if val is None:
            invalid_value += 1
            continue
        if sid in updates:
            duplicate_station += 1
        updates[sid] = float(val)

    if not updates:
        raise SheetImportError(
            "No valid station forecast rows were found after parsing the worksheet."
        )

    diagnostics = {
        "row_count": max(0, len(rows) - 1),
        "valid_rows": len(updates),
        "invalid_station_rows": invalid_station,
        "invalid_value_rows": invalid_value,
        "duplicate_station_rows": duplicate_station,
        "station_column": headers[station_idx] if station_idx < len(headers) else "",
        "value_column": headers[value_idx] if value_idx < len(headers) else "",
    }
    return {"updates": updates, "diagnostics": diagnostics}


def import_forecast_updates(sheet_url: str, access_token: str) -> dict[str, Any]:
    """Import station forecast updates from a Google Sheet URL."""
    if not access_token:
        raise SheetImportError("Missing Google OAuth access token for Sheets API.")
    ref = parse_sheet_ref(sheet_url)
    rows = read_sheet_values(ref.spreadsheet_id, ref.gid, access_token=access_token)
    parsed = normalize_forecast_rows(rows)
    return {
        "spreadsheet_id": ref.spreadsheet_id,
        "gid": ref.gid,
        "updates": parsed["updates"],
        "diagnostics": parsed["diagnostics"],
    }
