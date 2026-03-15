"""
Cloud Run Job entrypoint for incremental CAPEX data refresh.

This wrapper runs `capex_pipeline.py --incremental`, emits structured logs,
and exits non-zero on failure so Cloud Scheduler / Monitoring can alert.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log(event: str, **fields: Any) -> None:
    payload = {"event": event, "ts": _utc_now(), **fields}
    print(json.dumps(payload, ensure_ascii=True), flush=True)


def _parse_incremental_counts(output: str) -> tuple[int, int, int]:
    for line in output.splitlines():
        if "Incremental sync:" not in line:
            continue
        match = re.search(r"(\d+)\s+new.*?(\d+)\s+updated.*?(\d+)\s+removed", line)
        if match:
            return int(match.group(1)), int(match.group(2)), int(match.group(3))
    return 0, 0, 0


def main() -> int:
    run_id = f"refresh-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    timeout_sec = int(os.environ.get("REFRESH_TIMEOUT_SEC", "1800") or 1800)
    base_dir = Path(__file__).resolve().parent
    pipeline_path = base_dir / "capex_pipeline.py"

    _log(
        "refresh_job_started",
        run_id=run_id,
        timeout_sec=timeout_sec,
        pipeline_path=str(pipeline_path),
    )

    try:
        result = subprocess.run(
            [sys.executable, "-u", str(pipeline_path), "--incremental"],
            cwd=str(base_dir),
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired:
        _log("refresh_job_failed", run_id=run_id, reason="timeout", timeout_sec=timeout_sec)
        return 1
    except Exception as exc:  # pragma: no cover - defensive
        _log("refresh_job_failed", run_id=run_id, reason="exception", error=str(exc))
        return 1

    output = (result.stdout or "") + (result.stderr or "")
    new_rows, updated_rows, removed_rows = _parse_incremental_counts(output)

    if result.returncode != 0:
        tail_lines = output.strip().splitlines()[-20:]
        _log(
            "refresh_job_failed",
            run_id=run_id,
            exit_code=result.returncode,
            new_rows=new_rows,
            updated_rows=updated_rows,
            removed_rows=removed_rows,
            output_tail="\n".join(tail_lines),
        )
        return result.returncode or 1

    _log(
        "refresh_job_succeeded",
        run_id=run_id,
        exit_code=0,
        new_rows=new_rows,
        updated_rows=updated_rows,
        removed_rows=removed_rows,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
