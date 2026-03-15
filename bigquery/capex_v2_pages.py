"""
V2 dashboard pages -- additional routes registered on the main Flask app
when APP_MODE=dashboard_v2.

Adds:
  - /api/v2/classification-reviews   (Phase 2: LLM review queue)
  - /api/v2/classification-feedback   (Phase 2: feedback submission)
  - /api/v2/run-classification-review (Phase 2: manual LLM review trigger)
  - /api/v2/payments                  (Phase 4: payment milestone data)
  - /api/v2/payment-templates         (Phase 5: payment pattern templates)
  - /api/v2/cashflow                  (Phase 6: cashflow projections)

All existing routes remain untouched.
"""
from __future__ import annotations

import os
from pathlib import Path
import json
from datetime import datetime, timezone
import uuid
import threading

import pandas as pd
from flask import Flask, jsonify, request

from access_control import current_user_email, get_access_context, load_settings_with_access_defaults
from auth import get_google_access_token
import storage_backend as store


def register_v2_routes(app: Flask) -> None:
    """Register v2 API routes on the given Flask app."""
    refresh_lock = threading.Lock()
    def _int_env(name: str, default: int) -> int:
        try:
            return int(str(os.environ.get(name, str(default)) or str(default)).strip())
        except Exception:
            return default

    refresh_cooldown_sec = _int_env("REFRESH_COOLDOWN_SEC", 120)
    refresh_timeout_sec = _int_env("REFRESH_TIMEOUT_SEC", 300)
    refresh_job_poll_sec = max(2, _int_env("REFRESH_JOB_POLL_SEC", 10))
    refresh_job_max_wait_sec = max(60, _int_env("REFRESH_JOB_MAX_WAIT_SEC", 2100))
    refresh_use_logged_in_oauth = str(
        os.environ.get("REFRESH_USE_LOGGED_IN_OAUTH", "true") or "true"
    ).strip().lower() in {"1", "true", "yes", "y"}
    refresh_execution_mode = str(os.environ.get("REFRESH_EXECUTION_MODE", "subprocess") or "subprocess").strip().lower()
    refresh_job_name = str(os.environ.get("REFRESH_JOB_NAME", "capex-refresh-job") or "capex-refresh-job").strip()
    refresh_job_region = str(
        os.environ.get("REFRESH_JOB_REGION")
        or os.environ.get("REGION")
        or os.environ.get("GOOGLE_CLOUD_REGION")
        or "us-central1"
    ).strip()
    refresh_job_project = str(
        os.environ.get("REFRESH_JOB_PROJECT")
        or os.environ.get("BQ_ANALYTICS_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or ""
    ).strip()
    refresh_state: dict[str, object] = {
        "running": False,
        "last_run_id": "",
        "last_started_at": "",
        "last_finished_at": "",
        "last_status": "never",
        "last_error": "",
        "last_counts": {"new": 0, "updated": 0, "removed": 0},
        "last_mode": "",
        "last_auth_mode": "",
        "last_operation_name": "",
    }

    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _state_snapshot() -> dict[str, object]:
        return {
            "running": bool(refresh_state.get("running", False)),
            "last_run_id": str(refresh_state.get("last_run_id", "") or ""),
            "last_started_at": str(refresh_state.get("last_started_at", "") or ""),
            "last_finished_at": str(refresh_state.get("last_finished_at", "") or ""),
            "last_status": str(refresh_state.get("last_status", "never") or "never"),
            "last_error": str(refresh_state.get("last_error", "") or ""),
            "last_counts": dict(refresh_state.get("last_counts", {}) or {}),
            "last_mode": str(refresh_state.get("last_mode", "") or ""),
            "last_auth_mode": str(refresh_state.get("last_auth_mode", "") or ""),
            "last_operation_name": str(refresh_state.get("last_operation_name", "") or ""),
            "cooldown_sec": refresh_cooldown_sec,
            "execution_mode": refresh_execution_mode,
            "refresh_use_logged_in_oauth": refresh_use_logged_in_oauth,
            "job_name": refresh_job_name,
        }

    def _seconds_since_last_finish() -> float | None:
        finished = str(refresh_state.get("last_finished_at", "") or "")
        if not finished:
            return None
        try:
            ts = datetime.fromisoformat(finished)
            return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())
        except Exception:
            return None

    def _run_cloud_refresh_job() -> str:
        """Trigger Cloud Run Job execution and return operation name."""
        if not refresh_job_project:
            raise RuntimeError("REFRESH_JOB_PROJECT (or BQ_ANALYTICS_PROJECT/GOOGLE_CLOUD_PROJECT) is not configured")
        if not refresh_job_region:
            raise RuntimeError("REFRESH_JOB_REGION is not configured")
        if not refresh_job_name:
            raise RuntimeError("REFRESH_JOB_NAME is not configured")

        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        creds, _ = google.auth.default(scopes=scopes)
        session = AuthorizedSession(creds)
        run_url = (
            f"https://run.googleapis.com/v2/projects/{refresh_job_project}/locations/"
            f"{refresh_job_region}/jobs/{refresh_job_name}:run"
        )
        resp = session.post(run_url, json={}, timeout=30)
        if resp.status_code < 200 or resp.status_code >= 300:
            raise RuntimeError(f"Cloud Run Job trigger failed ({resp.status_code}): {resp.text[:600]}")
        payload = resp.json() if resp.content else {}
        op_name = str(payload.get("name", "")).strip()
        if not op_name:
            raise RuntimeError("Cloud Run Job trigger returned no operation name")
        return op_name

    def _watch_cloud_refresh_job(operation_name: str) -> None:
        """Poll Cloud Run operation state in background until completion."""
        import time
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        started_ts = datetime.now(timezone.utc)
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        creds, _ = google.auth.default(scopes=scopes)
        session = AuthorizedSession(creds)
        op_url = f"https://run.googleapis.com/v2/{operation_name}"

        try:
            while True:
                elapsed = (datetime.now(timezone.utc) - started_ts).total_seconds()
                if elapsed > refresh_job_max_wait_sec:
                    refresh_state["last_status"] = "failed"
                    refresh_state["last_error"] = (
                        f"Refresh job watcher timed out after {refresh_job_max_wait_sec}s "
                        f"(operation={operation_name})"
                    )
                    break

                resp = session.get(op_url, timeout=30)
                if resp.status_code < 200 or resp.status_code >= 300:
                    refresh_state["last_status"] = "failed"
                    refresh_state["last_error"] = (
                        f"Refresh job polling failed ({resp.status_code}): {resp.text[:400]}"
                    )
                    break

                payload = resp.json() if resp.content else {}
                if payload.get("done") is True:
                    err = payload.get("error")
                    if err:
                        code = err.get("code", "unknown")
                        msg = err.get("message", "Cloud Run Job execution failed")
                        refresh_state["last_status"] = "failed"
                        refresh_state["last_error"] = f"Cloud Run Job failed ({code}): {msg}"
                    else:
                        refresh_state["last_status"] = "ok"
                        refresh_state["last_error"] = ""
                    break

                time.sleep(refresh_job_poll_sec)
        except Exception as exc:
            refresh_state["last_status"] = "failed"
            refresh_state["last_error"] = f"Refresh job watcher error: {exc}"
        finally:
            refresh_state["running"] = False
            refresh_state["last_finished_at"] = _now_iso()

    def _requested_lines() -> set[str] | None:
        raw = request.args.get("lines", "")
        if not raw:
            return None
        values = {v.strip() for v in raw.split(",") if v.strip()}
        return values or None

    def _po_line_map() -> dict[str, str]:
        try:
            from payment_patterns import load_po_data
            import re as _re
        except Exception:
            return {}

        po_data = load_po_data(from_bq=True)
        if po_data.empty or "po_number" not in po_data.columns:
            return {}

        out: dict[str, str] = {}
        for _, row in po_data.iterrows():
            po = str(row.get("po_number", "")).strip()
            station = str(row.get("station_id", "")).strip()
            if not po:
                continue
            m = _re.match(r"(BASE\d+)-(MOD\d+|CELL\d+|INV\d+)", station)
            line = f"{m.group(1)}-{m.group(2)}" if m else ""
            if line and po not in out:
                out[po] = line
        return out

    def _filter_timelines_by_lines(timelines: list[dict], selected: set[str] | None) -> list[dict]:
        if not selected:
            return timelines
        po_to_line = _po_line_map()
        filtered: list[dict] = []
        for t in timelines:
            po = str(t.get("po_number", "")).strip()
            line = po_to_line.get(po, "")
            if line in selected:
                t2 = dict(t)
                t2["line"] = line
                filtered.append(t2)
        return filtered

    def _rfq_settings() -> dict[str, object]:
        raw = store.read_json("dashboard_settings.json")
        cfg = raw if isinstance(raw, dict) else {}
        cfg["rfq_validation_mode"] = "bq_only"
        cfg.setdefault("rfq_ai_provider", "gemini")
        return cfg

    def _require_settings_editor():
        user_email = current_user_email()
        settings, changed = load_settings_with_access_defaults(bootstrap_user_email=user_email)
        if changed:
            store.write_json("dashboard_settings.json", settings)
        access = get_access_context(settings, user_email=user_email)
        if access.get("can_edit_settings"):
            return None
        return jsonify({"error": "Settings edit access required", "access": access}), 403

    def _parse_prior_context(raw: str) -> dict:
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _rfq_history_path() -> Path:
        base_dir = Path(__file__).resolve().parent
        data_dir = base_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir / "rfq_history.json"

    def _rfq_quotes_dir() -> Path:
        base_dir = Path(__file__).resolve().parent
        out_dir = base_dir / "data" / "rfq_quotes"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def _load_rfq_history() -> list[dict]:
        path = _rfq_history_path()
        if not path.exists():
            return []
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, list) else []
        except Exception:
            return []

    def _save_rfq_history(rows: list[dict]) -> None:
        path = _rfq_history_path()
        path.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    def _persist_rfq_artifact(
        *,
        vendor: str,
        prompt: str,
        payment_milestones_note: str,
        pdf_filename: str,
        pdf_bytes: bytes | None,
        result: dict,
    ) -> str:
        history = _load_rfq_history()
        entry_id = str(uuid.uuid4())
        saved_pdf = ""
        if pdf_bytes:
            safe_name = (pdf_filename or f"{entry_id}.pdf").replace("/", "_").replace("\\", "_")
            saved_path = _rfq_quotes_dir() / f"{entry_id}_{safe_name}"
            saved_path.write_bytes(pdf_bytes)
            saved_pdf = str(saved_path)

        entry = {
            "id": entry_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "vendor": vendor,
            "prompt": prompt,
            "payment_milestones_note": payment_milestones_note,
            "pdf_filename": pdf_filename,
            "saved_pdf_path": saved_pdf,
            "csv_filename": result.get("csv_filename", ""),
            "csv_text": result.get("csv_text", ""),
            "draft": result.get("draft", {}),
            "preview": result.get("preview", {}),
            "validation": result.get("validation", {}),
            "meta": result.get("meta", {}),
            "revision_context": result.get("revision_context", {}),
            "provider": result.get("provider", ""),
        }
        history.insert(0, entry)
        # Keep recent history bounded for local storage.
        history = history[:200]
        _save_rfq_history(history)
        return entry_id

    # ------------------------------------------------------------------
    # Phase 2: Classification Review Queue
    # ------------------------------------------------------------------

    @app.route("/api/v2/classification-reviews")
    def v2_classification_reviews():
        """Return pending classification disagreements for human review."""
        rows = []
        try:
            import bq_dataset
            df = bq_dataset.read_table(
                "classification_reviews",
                where="human_decision IS NULL OR TRIM(CAST(human_decision AS STRING)) = ''",
            )
            if not df.empty:
                for col in df.columns:
                    dtype = df[col].dtype
                    if hasattr(dtype, "name") and dtype.name in ("Int8","Int16","Int32","Int64","UInt8","UInt16","UInt32","UInt64"):
                        df[col] = df[col].astype("float64").fillna(0)
                    elif pd.api.types.is_float_dtype(dtype):
                        df[col] = df[col].fillna(0)
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        df[col] = df[col].astype("object").where(df[col].notna(), None)
                        df[col] = df[col].apply(lambda v: v.isoformat() if hasattr(v, "isoformat") else "")
                    elif dtype == "object":
                        df[col] = df[col].fillna("")
                rows = df.to_dict(orient="records")
        except Exception:
            pass

        if not rows:
            local_path = Path(store.local_data_dir()) / "classification_reviews.json"
            if local_path.exists():
                try:
                    raw = json.loads(local_path.read_text(encoding="utf-8"))
                    if isinstance(raw, list):
                        rows = [r for r in raw if not r.get("human_decision")]
                except Exception:
                    pass

        return jsonify({"reviews": rows, "count": len(rows)})

    @app.route("/api/v2/classification-feedback", methods=["POST"])
    def v2_submit_feedback():
        """Submit a human decision on a classification disagreement."""
        try:
            body = request.get_json(force=True)
            review_id = body.get("review_id", "")
            decision = body.get("decision", "")
            final_station = body.get("final_station_id", "")
            final_subcat = body.get("final_subcategory", "")
            reviewed_by = body.get("reviewed_by", "unknown")

            if not review_id or not decision:
                return jsonify({"error": "review_id and decision required"}), 400

            feedback_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc).isoformat()

            entry = {
                "feedback_id": feedback_id,
                "line_id": body.get("line_id", ""),
                "vendor_name": body.get("vendor_name", ""),
                "item_description": body.get("item_description", ""),
                "price_subtotal": float(body.get("price_subtotal", 0)),
                "final_station_id": final_station,
                "final_subcategory": final_subcat,
                "source": decision,
                "created_by": reviewed_by,
                "created_at": now,
            }

            feedback_path = Path(store.local_data_dir()) / "classification_feedback.json"
            existing = []
            if feedback_path.exists():
                try:
                    existing = json.loads(feedback_path.read_text(encoding="utf-8"))
                except Exception:
                    existing = []
            existing.append(entry)
            feedback_path.write_text(json.dumps(existing, indent=2, default=str), encoding="utf-8")

            reviews_path = Path(store.local_data_dir()) / "classification_reviews.json"
            if reviews_path.exists():
                try:
                    reviews = json.loads(reviews_path.read_text(encoding="utf-8"))
                    for rv in reviews:
                        if rv.get("review_id") == review_id:
                            rv["human_decision"] = decision
                            rv["reviewed_by"] = reviewed_by
                            rv["reviewed_at"] = now
                    reviews_path.write_text(json.dumps(reviews, indent=2, default=str), encoding="utf-8")
                except Exception:
                    pass

            try:
                import bq_dataset
                bq_dataset.write_table("classification_feedback", pd.DataFrame([entry]), write_disposition="WRITE_APPEND")
            except Exception:
                pass

            return jsonify({"status": "ok", "feedback_id": feedback_id})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/api/v2/run-classification-review", methods=["POST"])
    def v2_trigger_review():
        """Manually trigger an LLM classification review run."""
        denied = _require_settings_editor()
        if denied:
            return denied
        try:
            from classify_agent import run_review
            result = run_review()
            return jsonify({"status": "ok", **result})
        except ImportError:
            return jsonify({"error": "classify_agent module not yet available"}), 501
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    # ------------------------------------------------------------------
    # Phase 4: Payment Milestones
    # ------------------------------------------------------------------

    @app.route("/api/v2/payments")
    def v2_payments():
        """Return payment records with milestone data."""
        try:
            import bq_dataset
            df = bq_dataset.read_table("payments")
            rows = df.to_dict(orient="records") if not df.empty else []
            return jsonify({"payments": rows, "count": len(rows)})
        except Exception:
            df = store.read_csv("payment_details.csv")
            rows = df.to_dict(orient="records") if not df.empty else []
            return jsonify({"payments": rows, "count": len(rows)})

    @app.route("/api/v2/payment-milestones")
    def v2_payment_milestones():
        """Return payment milestones (expected + actual)."""
        try:
            import bq_dataset
            df = bq_dataset.read_table("payment_milestones")
            rows = df.to_dict(orient="records") if not df.empty else []
            return jsonify({"milestones": rows, "count": len(rows)})
        except Exception as exc:
            return jsonify({"milestones": [], "count": 0, "error": str(exc)})

    @app.route("/api/v2/po-timelines")
    def v2_po_timelines():
        """Return per-PO payment timelines with milestone markers."""
        try:
            from payment_patterns import load_payment_data, load_po_data, build_po_timelines
            payments = load_payment_data(from_bq=True)
            po_data = load_po_data(from_bq=True)
            timelines = build_po_timelines(payments, po_data=po_data)
            selected_lines = _requested_lines()
            timelines = _filter_timelines_by_lines(timelines, selected_lines)
            return jsonify({"timelines": timelines, "count": len(timelines)})
        except Exception as exc:
            return jsonify({"timelines": [], "count": 0, "error": str(exc)})

    @app.route("/api/v2/vendor-profiles")
    def v2_vendor_profiles():
        """Return vendor payment profiles (avg cycle, deposit %, etc.)."""
        try:
            from payment_patterns import load_payment_data, load_po_data, build_po_timelines, build_vendor_profiles
            payments = load_payment_data(from_bq=True)
            po_data = load_po_data(from_bq=True)
            timelines = build_po_timelines(payments, po_data=po_data)
            selected_lines = _requested_lines()
            timelines = _filter_timelines_by_lines(timelines, selected_lines)
            profiles = build_vendor_profiles(timelines)
            return jsonify({"profiles": profiles, "count": len(profiles)})
        except Exception as exc:
            return jsonify({"profiles": [], "count": 0, "error": str(exc)})

    # ------------------------------------------------------------------
    # Phase 5: Payment Templates
    # ------------------------------------------------------------------

    @app.route("/api/v2/po-list")
    def v2_po_list():
        """Return distinct POs with total amounts for the template PO picker."""
        df = store.read_csv("capex_clean.csv")
        if df.empty or "po_number" not in df.columns:
            return jsonify({"pos": []})
        df["price_subtotal"] = pd.to_numeric(df.get("price_subtotal", 0), errors="coerce").fillna(0)
        grouped = df.groupby("po_number").agg(
            vendor_name=("vendor_name", "first"),
            total_amount=("price_subtotal", "sum"),
            date_order=("date_order", "first"),
            station_id=("station_id", "first"),
            created_by_name=("created_by_name", "first"),
            project_name=("project_name", "first"),
        ).reset_index()
        grouped = grouped.sort_values("total_amount", ascending=False)
        return jsonify({"pos": grouped.to_dict(orient="records")})

    @app.route("/api/v2/payment-templates")
    def v2_payment_templates():
        """Return all payment templates (local JSON is the source of truth)."""
        raw = store.read_json("payment_templates.json")
        templates = raw if isinstance(raw, list) else []
        return jsonify({"templates": templates, "count": len(templates)})

    @app.route("/api/v2/payment-templates", methods=["POST"])
    def v2_save_template():
        """Create or update a payment template linked to a specific PO."""
        try:
            body = request.get_json(force=True)
            template_id = body.get("template_id") or str(uuid.uuid4())
            now = datetime.now(timezone.utc).isoformat()

            existing = store.read_json("payment_templates.json")
            if not isinstance(existing, list):
                existing = []

            incoming_po = str(body.get("po_number", "")).strip()
            if incoming_po:
                dup = next(
                    (
                        t for t in existing
                        if str(t.get("po_number", "")).strip() == incoming_po
                        and str(t.get("template_id", "")).strip() != str(template_id).strip()
                    ),
                    None,
                )
                if dup:
                    return jsonify({
                        "error": f"Template already exists for PO {incoming_po}. Edit the existing one instead.",
                        "template_id": dup.get("template_id", ""),
                    }), 409

            old = next((t for t in existing if t.get("template_id") == template_id), {})

            template = {
                **old,
                "template_id": template_id,
                "po_number": body.get("po_number", old.get("po_number", "")),
                "vendor_name": body.get("vendor_name", old.get("vendor_name", "")),
                "total_amount": float(body.get("total_amount", old.get("total_amount", 0))),
                "name": body.get("name", old.get("name", "")),
                "milestones": body.get("milestones", old.get("milestones", [])),
                "updated_at": now,
            }
            if "created_at" not in template:
                template["created_at"] = now

            existing = [t for t in existing if t.get("template_id") != template_id]
            existing.append(template)
            store.write_json("payment_templates.json", existing)

            try:
                import bq_dataset
                bq_dataset.write_table("payment_templates", pd.DataFrame([{
                    "template_id": template_id,
                    "name": template["name"],
                    "description": f"PO: {template['po_number']}",
                    "milestones_json": json.dumps(template["milestones"]),
                    "vendor_name": template["vendor_name"],
                    "line_prefix": "",
                    "created_by": "dashboard",
                    "created_at": now,
                    "updated_at": now,
                }]), write_disposition="WRITE_APPEND")
            except Exception:
                pass

            return jsonify({"status": "ok", "template_id": template_id})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/api/v2/payment-templates/<template_id>", methods=["DELETE"])
    def v2_delete_template(template_id: str):
        """Delete a payment template by ID."""
        try:
            existing = store.read_json("payment_templates.json")
            if not isinstance(existing, list):
                return jsonify({"error": "No templates found"}), 404

            before = len(existing)
            existing = [t for t in existing if t.get("template_id") != template_id]
            if len(existing) == before:
                return jsonify({"error": "Template not found"}), 404

            store.write_json("payment_templates.json", existing)
            return jsonify({"status": "ok", "remaining": len(existing)})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    # ------------------------------------------------------------------
    # Phase 6: Cashflow Projections
    # ------------------------------------------------------------------

    @app.route("/api/v2/cashflow")
    def v2_cashflow():
        """Return cashflow projections with monthly/cumulative/weekly breakdowns."""
        try:
            from cashflow import build_projections, monthly_cashflow, cumulative_cashflow, weekly_detail
            shift_days = request.args.get("shift_days", 0, type=int)
            selected_lines = _requested_lines()

            projections = build_projections(from_bq=True)
            if projections.empty:
                return jsonify({"monthly": [], "cumulative": [], "weekly": [], "total_rows": 0})

            if selected_lines is not None:
                if "__none__" in selected_lines:
                    projections = projections.iloc[0:0]
                elif "line" in projections.columns:
                    projections = projections[projections["line"].astype(str).isin(selected_lines)]

            if shift_days:
                from cashflow import apply_scenario_shift
                projections = apply_scenario_shift(projections, shift_days=shift_days)

            monthly = monthly_cashflow(projections)
            cumul = cumulative_cashflow(monthly)
            weekly = weekly_detail(projections)

            return jsonify({
                "monthly": monthly,
                "cumulative": cumul,
                "weekly": weekly,
                "total_rows": len(projections),
                "actuals": len(projections[projections["source"] == "historical"]),
                "projected": len(projections[projections["source"] == "projected"]),
                "shift_days": shift_days,
            })
        except Exception as exc:
            return jsonify({"monthly": [], "cumulative": [], "weekly": [], "error": str(exc)})

    # ------------------------------------------------------------------
    # Data Management: Refresh + Ramp Upload + Manual PO redirect
    # ------------------------------------------------------------------

    @app.route("/api/v2/refresh-data", methods=["POST"])
    def v2_refresh_data():
        """Run an incremental pipeline refresh from the dashboard.

        Includes server-side protection against rapid repeat clicks by:
        - rejecting while a refresh is already in flight
        - enforcing a configurable cooldown after completion
        """
        import subprocess
        import sys

        denied = _require_settings_editor()
        if denied:
            return denied

        body = request.get_json(silent=True) or {}
        force = str(body.get("force", "")).strip().lower() in {"1", "true", "yes", "y"}

        if refresh_state.get("running"):
            return jsonify({
                "error": "Refresh already in progress",
                "status": _state_snapshot(),
            }), 409

        seconds_since_finish = _seconds_since_last_finish()
        if (not force) and seconds_since_finish is not None and seconds_since_finish < refresh_cooldown_sec:
            wait_sec = int(refresh_cooldown_sec - seconds_since_finish)
            return jsonify({
                "error": f"Refresh cooldown active. Try again in {wait_sec}s.",
                "retry_after_sec": wait_sec,
                "status": _state_snapshot(),
            }), 429

        if not refresh_lock.acquire(blocking=False):
            return jsonify({
                "error": "Refresh lock busy. Another request is starting a refresh.",
                "status": _state_snapshot(),
            }), 409

        run_id = uuid.uuid4().hex
        user_access_token = ""
        if refresh_use_logged_in_oauth:
            try:
                user_access_token = str(get_google_access_token() or "").strip()
            except Exception:
                user_access_token = ""

        # If we have a signed-in user token, force subprocess mode so Odoo pulls
        # execute under that user context instead of the Cloud Run Job service account.
        effective_mode = "subprocess" if user_access_token else refresh_execution_mode

        refresh_state["running"] = True
        refresh_state["last_run_id"] = run_id
        refresh_state["last_started_at"] = _now_iso()
        refresh_state["last_error"] = ""
        refresh_state["last_mode"] = effective_mode
        refresh_state["last_auth_mode"] = "user_oauth" if user_access_token else "service_account"
        refresh_state["last_operation_name"] = ""

        base_dir = Path(__file__).resolve().parent
        pipeline_path = base_dir / "capex_pipeline.py"
        venv_win = base_dir / "venv" / "Scripts" / "python.exe"
        venv_unix = base_dir / "venv" / "bin" / "python"
        venv_python = venv_win if venv_win.exists() else venv_unix
        python_exe = str(venv_python) if venv_python.exists() else sys.executable
        try:
            if effective_mode == "job":
                operation_name = _run_cloud_refresh_job()
                refresh_state["last_status"] = "running"
                refresh_state["last_operation_name"] = operation_name

                watcher = threading.Thread(
                    target=_watch_cloud_refresh_job,
                    args=(operation_name,),
                    daemon=True,
                )
                watcher.start()
                if refresh_lock.locked():
                    refresh_lock.release()
                return jsonify({
                    "status": "accepted",
                    "run_id": run_id,
                    "mode": "job",
                    "operation_name": operation_name,
                    "refresh_status": _state_snapshot(),
                }), 202

            result = subprocess.run(
                [python_exe, "-u", str(pipeline_path), "--incremental"],
                capture_output=True, text=True, timeout=refresh_timeout_sec,
                env={
                    **os.environ,
                    "GOOGLE_OAUTH_ACCESS_TOKEN": user_access_token,
                },
                cwd=str(base_dir),
            )
            output = result.stdout + result.stderr

            new = updated = removed = 0
            for line in output.splitlines():
                if "Incremental sync:" in line:
                    import re
                    m = re.search(r"(\d+) new.*?(\d+) updated.*?(\d+) removed", line)
                    if m:
                        new, updated, removed = int(m.group(1)), int(m.group(2)), int(m.group(3))

            if result.returncode != 0:
                last_lines = "\n".join(output.strip().splitlines()[-5:])
                refresh_state["last_status"] = "failed"
                refresh_state["last_error"] = f"Pipeline failed (exit {result.returncode}): {last_lines}"
                refresh_state["last_counts"] = {"new": new, "updated": updated, "removed": removed}
                return jsonify({
                    "error": str(refresh_state["last_error"]),
                    "status": _state_snapshot(),
                }), 500

            refresh_state["last_status"] = "ok"
            refresh_state["last_counts"] = {"new": new, "updated": updated, "removed": removed}
            return jsonify({
                "status": "ok",
                "run_id": run_id,
                "mode": "subprocess",
                "auth_mode": "user_oauth" if user_access_token else "service_account",
                "new": new,
                "updated": updated,
                "removed": removed,
                "refresh_status": _state_snapshot(),
            })
        except subprocess.TimeoutExpired:
            refresh_state["last_status"] = "failed"
            refresh_state["last_error"] = f"Pipeline timed out after {refresh_timeout_sec} seconds"
            return jsonify({"error": str(refresh_state["last_error"]), "status": _state_snapshot()}), 500
        except Exception as exc:
            refresh_state["last_status"] = "failed"
            refresh_state["last_error"] = str(exc)
            return jsonify({"error": str(exc), "status": _state_snapshot()}), 500
        finally:
            if effective_mode != "job" or refresh_state.get("last_status") == "failed":
                refresh_state["running"] = False
                refresh_state["last_finished_at"] = _now_iso()
            if refresh_lock.locked():
                refresh_lock.release()

    @app.route("/api/v2/refresh-status")
    def v2_refresh_status():
        """Return current refresh lock/status metadata."""
        return jsonify(_state_snapshot())

    @app.route("/api/v2/upload-ramp-csv", methods=["POST"])
    def v2_upload_ramp_csv():
        """Upload a Ramp CSV export, normalize it, and append new rows to capex_clean.csv."""
        import io
        from po_export_utils import load_and_normalize_ramp

        denied = _require_settings_editor()
        if denied:
            return denied

        if "file" not in request.files:
            return jsonify({"error": "No file uploaded"}), 400

        file = request.files["file"]
        if not file.filename or not file.filename.lower().endswith(".csv"):
            return jsonify({"error": "File must be a .csv"}), 400

        try:
            import tempfile

            tmp = Path(tempfile.mktemp(suffix=".csv"))
            file.save(str(tmp))

            ramp_new = load_and_normalize_ramp(tmp)
            tmp.unlink(missing_ok=True)

            if ramp_new.empty:
                return jsonify({"error": "No CAPEX rows found in uploaded CSV (check accounting categories)"}), 400

            existing = store.read_csv("capex_clean.csv")
            if existing.empty or "line_id" not in existing.columns:
                for col in existing.columns:
                    if col not in ramp_new.columns:
                        ramp_new[col] = ""
                store.write_csv("capex_clean.csv", ramp_new)
                return jsonify({"status": "ok", "new_rows": len(ramp_new), "skipped": 0, "total": len(ramp_new)})

            existing_ids = set(existing["line_id"].astype(str).str.strip())
            ramp_new["line_id"] = ramp_new["line_id"].astype(str).str.strip()
            new_mask = ~ramp_new["line_id"].isin(existing_ids)
            new_rows = ramp_new[new_mask].copy()
            skipped = len(ramp_new) - len(new_rows)

            if new_rows.empty:
                return jsonify({"status": "ok", "new_rows": 0, "skipped": skipped, "total": len(existing)})

            for col in existing.columns:
                if col not in new_rows.columns:
                    new_rows[col] = ""
            for col in new_rows.columns:
                if col not in existing.columns:
                    existing[col] = ""

            combined = pd.concat([existing, new_rows[existing.columns]], ignore_index=True)
            store.write_csv("capex_clean.csv", combined)

            return jsonify({
                "status": "ok",
                "new_rows": len(new_rows),
                "skipped": skipped,
                "total": len(combined),
            })
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/api/v2/ai-rfq/lookups")
    def v2_ai_rfq_lookups():
        """Return RFQ lookup values for vendor/product/project/tax/deliver-to validation."""
        try:
            from rfq_odoo_validation import load_lookup_snapshot

            settings = _rfq_settings()
            mode = str(request.args.get("mode") or settings.get("rfq_validation_mode") or "hybrid")
            force_live = str(request.args.get("force_live", "")).strip().lower() in {"1", "true", "yes", "y"}
            force_refresh = str(request.args.get("force_refresh", "")).strip().lower() in {"1", "true", "yes", "y"}
            payload = load_lookup_snapshot(
                validation_mode=mode,
                force_live=force_live,
                force_refresh=force_refresh,
            )
            return jsonify({"status": "ok", **payload})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    def _handle_ai_rfq_request(*, require_file: bool) -> tuple[dict, int]:
        from rfq_ai_service import generate_rfq_payload

        settings = _rfq_settings()
        vendor = str(request.form.get("vendor") or "").strip()
        user_prompt = str(request.form.get("prompt") or "").strip()
        payment_milestones_note = str(request.form.get("payment_milestones_note") or "").strip()
        user_deliver_to = str(request.form.get("deliver_to") or "").strip()
        user_header_project = str(request.form.get("header_project") or "").strip()
        prior_context = _parse_prior_context(str(request.form.get("prior_context") or ""))
        file = request.files.get("file")

        if not vendor:
            vendor = str(prior_context.get("vendor") or "").strip()
        if not vendor:
            return {"error": "vendor is required"}, 400

        if user_deliver_to:
            settings["_user_deliver_to"] = user_deliver_to
        if user_header_project:
            settings["_user_header_project"] = user_header_project

        pdf_bytes: bytes | None = None
        pdf_filename = ""
        if file:
            pdf_filename = str(file.filename or "").strip()
            if pdf_filename and not pdf_filename.lower().endswith(".pdf"):
                return {"error": "Quote file must be a PDF."}, 400
            pdf_bytes = file.read()
            if pdf_bytes and len(pdf_bytes) > 20 * 1024 * 1024:
                return {"error": "PDF file is too large (max 20MB)."}, 400
        elif require_file and not prior_context.get("quote_text"):
            return {"error": "A quote PDF is required for initial generation."}, 400

        result = generate_rfq_payload(
            vendor=vendor,
            user_prompt=user_prompt,
            payment_milestones_note=payment_milestones_note,
            pdf_bytes=pdf_bytes,
            pdf_filename=pdf_filename,
            prior_context=prior_context,
            settings=settings,
        )
        try:
            history_id = _persist_rfq_artifact(
                vendor=vendor,
                prompt=user_prompt,
                payment_milestones_note=payment_milestones_note,
                pdf_filename=pdf_filename,
                pdf_bytes=pdf_bytes,
                result=result,
            )
            result["history_id"] = history_id
        except Exception as exc:
            result.setdefault("validation", {}).setdefault("warnings", []).append(
                {"field": "history", "message": f"RFQ history save failed: {exc}"}
            )
        return result, 200

    @app.route("/api/v2/ai-rfq/history")
    def v2_ai_rfq_history():
        try:
            history = _load_rfq_history()
            out = []
            for row in history:
                if not isinstance(row, dict):
                    continue
                val = row.get("validation", {}) if isinstance(row.get("validation"), dict) else {}
                out.append(
                    {
                        "id": str(row.get("id", "")),
                        "created_at": str(row.get("created_at", "")),
                        "vendor": str(row.get("vendor", "")),
                        "pdf_filename": str(row.get("pdf_filename", "")),
                        "csv_filename": str(row.get("csv_filename", "")),
                        "provider": str(row.get("provider", "")),
                        "blocking_error_count": int(val.get("blocking_error_count", 0) or 0),
                        "warning_count": len(val.get("warnings", []) if isinstance(val.get("warnings"), list) else []),
                    }
                )
            return jsonify({"status": "ok", "items": out, "count": len(out)})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/api/v2/ai-rfq/history/<entry_id>")
    def v2_ai_rfq_history_entry(entry_id: str):
        try:
            history = _load_rfq_history()
            for row in history:
                if isinstance(row, dict) and str(row.get("id", "")) == str(entry_id):
                    return jsonify({"status": "ok", "entry": row})
            return jsonify({"error": "history entry not found"}), 404
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/api/v2/ai-rfq/generate", methods=["POST"])
    def v2_ai_rfq_generate():
        """Generate RFQ draft + CSV + preview from quote input."""
        try:
            payload, status = _handle_ai_rfq_request(require_file=True)
            return jsonify(payload), status
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/api/v2/ai-rfq/regenerate", methods=["POST"])
    def v2_ai_rfq_regenerate():
        """Regenerate RFQ draft with additional user context and prior draft memory."""
        try:
            payload, status = _handle_ai_rfq_request(require_file=False)
            return jsonify(payload), status
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/api/v2/generate-milestones", methods=["POST"])
    def v2_generate_milestones():
        """Generate AI milestone templates for major CAPEX POs."""
        import subprocess
        import sys

        denied = _require_settings_editor()
        if denied:
            return denied

        base_dir = Path(__file__).resolve().parent
        pipeline_path = base_dir / "classify_agent.py"
        venv_win = base_dir / "venv" / "Scripts" / "python.exe"
        venv_unix = base_dir / "venv" / "bin" / "python"
        venv_python = venv_win if venv_win.exists() else venv_unix
        python_exe = str(venv_python) if venv_python.exists() else sys.executable
        try:
            result = subprocess.run(
                [python_exe, "-u", str(pipeline_path), "--generate-milestones", "--provider", "gemini"],
                capture_output=True, text=True, timeout=600,
                cwd=str(base_dir),
            )
            output = result.stdout + result.stderr

            generated = saved = 0
            for line in output.splitlines():
                if "Templates generated:" in line:
                    import re
                    m = re.search(r"(\d+)", line)
                    if m:
                        generated = int(m.group(1))
                if "Saved" in line and "new draft" in line:
                    import re
                    m = re.search(r"Saved (\d+)", line)
                    if m:
                        saved = int(m.group(1))

            if result.returncode != 0:
                last_lines = "\n".join(output.strip().splitlines()[-5:])
                return jsonify({"error": f"Generation failed (exit {result.returncode}): {last_lines}"})

            return jsonify({"status": "ok", "generated": generated, "saved": saved})
        except subprocess.TimeoutExpired:
            return jsonify({"error": "Generation timed out after 10 minutes"}), 500
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/api/v2/manual-po-redirect")
    def v2_manual_po_redirect():
        """Redirect to the station review app's Manual PO tab."""
        review_url = os.environ.get("REVIEW_APP_URL", "http://localhost:5051")
        return f"""<html><head><meta http-equiv="refresh" content="0;url={review_url}#manual_po">
        </head><body><p>Redirecting to <a href="{review_url}#manual_po">Manual PO Entry</a>...</p></body></html>"""

    # ------------------------------------------------------------------
    # Cashflow drill-down
    # ------------------------------------------------------------------

    @app.route("/api/v2/cashflow-drilldown")
    def v2_cashflow_drilldown():
        """Return line-level cashflow items for a given month."""
        try:
            from cashflow import build_projections, apply_scenario_shift
            import pandas as pd
            month = request.args.get("month", "")
            source = request.args.get("source", "all")
            shift_days = request.args.get("shift_days", 0, type=int)
            selected_lines = _requested_lines()

            # Keep data source consistent with /api/v2/cashflow so chart bars and drill rows match.
            projections = build_projections(from_bq=True)
            if projections.empty:
                return jsonify({"items": []})

            if selected_lines is not None:
                if "__none__" in selected_lines:
                    projections = projections.iloc[0:0]
                elif "line" in projections.columns:
                    projections = projections[projections["line"].astype(str).isin(selected_lines)]

            if shift_days:
                projections = apply_scenario_shift(projections, shift_days=shift_days)

            projections["_month"] = projections["expected_date"].dt.to_period("M").astype(str)
            # Plotly may send month labels as YYYY-MM or YYYY-MM-01; normalize both to YYYY-MM.
            month_norm = str(pd.to_datetime(month, errors="coerce").to_period("M")) if month else ""
            if month_norm == "NaT":
                month_norm = month[:7] if len(month) >= 7 else month

            filtered = projections[projections["_month"] == month_norm]
            if source and source != "all":
                filtered = filtered[filtered["source"] == source]

            items = []
            for _, row in filtered.iterrows():
                items.append({
                    "po_number": str(row.get("po_number", "")),
                    "vendor_name": str(row.get("vendor_name", "")),
                    "milestone_label": str(row.get("milestone_label", "")),
                    "expected_date": str(row.get("expected_date", ""))[:10],
                    "expected_amount": float(row.get("expected_amount", 0)),
                    "source": str(row.get("source", "")),
                    "record_type": str(row.get("record_type", "")),
                    "station_id": str(row.get("station_id", "")),
                    "line": str(row.get("line", "")),
                })
            items.sort(key=lambda x: x["expected_amount"], reverse=True)
            return jsonify({"items": items, "month": month_norm or month, "source": source})
        except Exception as exc:
            return jsonify({"items": [], "error": str(exc)})

    # ------------------------------------------------------------------
    # V2 health / info endpoint
    # ------------------------------------------------------------------

    @app.route("/api/v2/info")
    def v2_info():
        """Return v2 feature status and data availability."""
        bq_status = "unknown"
        try:
            import bq_dataset
            bq_dataset.ensure_dataset()
            bq_status = "connected"
        except Exception as exc:
            bq_status = f"error: {exc}"

        return jsonify({
            "v2_enabled": True,
            "app_mode": os.environ.get("APP_MODE", "dashboard"),
            "bq_dataset": bq_status,
            "features": {
                "classification_review": True,
                "payment_milestones": True,
                "payment_templates": True,
                "cashflow_projections": True,
                "vendor_profiles": True,
                "po_timelines": True,
                "ai_rfq_gen": True,
            },
            "endpoints": [
                "/api/v2/classification-reviews",
                "/api/v2/classification-feedback",
                "/api/v2/run-classification-review",
                "/api/v2/payments",
                "/api/v2/payment-milestones",
                "/api/v2/po-timelines",
                "/api/v2/vendor-profiles",
                "/api/v2/payment-templates",
                "/api/v2/cashflow",
                "/api/v2/ai-rfq/lookups",
                "/api/v2/ai-rfq/history",
                "/api/v2/ai-rfq/history/<id>",
                "/api/v2/ai-rfq/generate",
                "/api/v2/ai-rfq/regenerate",
            ],
        })
