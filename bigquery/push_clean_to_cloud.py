"""
Push clean data from local data/ to a GCS bucket.

Use after running the pipeline locally to upload capex_clean.csv,
capex_by_station.csv, spares_catalog.csv, bf1_stations.json, and
station_overrides.json so the Cloud Run dashboard/review apps use the latest data.

Usage:
  python push_clean_to_cloud.py
  python push_clean_to_cloud.py --gcs-bucket capex-pipeline-data
  python push_clean_to_cloud.py --gcs-bucket capex-pipeline-data --major-update

This script uses gcloud CLI auth and storage commands.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import storage_backend as store

VERSION_FILE = "data_version.json"
BACKUP_PREFIX = "backups"


def _resolve_gcloud_cmd() -> str:
    for cand in ("gcloud", "gcloud.cmd", "gcloud.exe"):
        path = shutil.which(cand)
        if path:
            return path
    raise RuntimeError("gcloud CLI not found in PATH")


def _run(cmd: list[str], *, allow_failure: bool = False) -> tuple[int, str, str]:
    if cmd and cmd[0] == "gcloud":
        cmd = [_resolve_gcloud_cmd(), *cmd[1:]]
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )
    if not allow_failure and proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr.strip()}")
    return proc.returncode, proc.stdout, proc.stderr


def _bucket_root_objects(bucket: str, project: str) -> list[str]:
    code, out, _ = _run(
        ["gcloud", "storage", "ls", f"gs://{bucket}/", f"--project={project}"],
        allow_failure=True,
    )
    if code != 0:
        return []
    objects: list[str] = []
    for line in out.splitlines():
        uri = line.strip()
        if not uri.startswith(f"gs://{bucket}/"):
            continue
        name = uri[len(f"gs://{bucket}/"):]
        if not name or "/" in name:
            continue
        objects.append(uri)
    return sorted(set(objects))


def _load_current_version(bucket: str, project: str) -> int:
    code, out, _ = _run(
        ["gcloud", "storage", "cat", f"gs://{bucket}/{VERSION_FILE}", f"--project={project}"],
        allow_failure=True,
    )
    if code != 0:
        return 0
    try:
        payload = json.loads(out)
        return int(payload.get("version_number", 0))
    except Exception:
        return 0


def _backup_bucket_before_push(bucket: str, project: str) -> dict[str, object]:
    root_objects = _bucket_root_objects(bucket, project)
    current_version = _load_current_version(bucket, project)
    next_version = current_version + 1
    version_label = f"v{next_version:04d}"
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_prefix_uri = f"gs://{bucket}/{BACKUP_PREFIX}/{version_label}_{ts}"

    copied_names: list[str] = []
    for src in root_objects:
        name = src[len(f"gs://{bucket}/"):]
        # Skip version metadata objects to keep backup payload focused on data snapshots.
        if name == VERSION_FILE:
            continue
        if name.startswith(f"{BACKUP_PREFIX}/"):
            continue
        dest = f"{backup_prefix_uri}/{name}"
        _run(["gcloud", "storage", "cp", src, dest, f"--project={project}"])
        copied_names.append(name)

    manifest = {
        "version": version_label,
        "version_number": next_version,
        "timestamp_utc": ts,
        "bucket": bucket,
        "backup_prefix": backup_prefix_uri,
        "copied_objects": copied_names,
        "copied_count": len(copied_names),
    }

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as fp:
        json.dump(manifest, fp, indent=2)
        manifest_local = fp.name
    try:
        _run(
            [
                "gcloud",
                "storage",
                "cp",
                manifest_local,
                f"{backup_prefix_uri}/manifest.json",
                f"--project={project}",
            ]
        )
        _run(
            [
                "gcloud",
                "storage",
                "cp",
                manifest_local,
                f"gs://{bucket}/{VERSION_FILE}",
                f"--project={project}",
            ]
        )
    finally:
        Path(manifest_local).unlink(missing_ok=True)

    return manifest


def _push_local_clean_files(bucket: str, project: str) -> list[str]:
    data_dir = store.local_data_dir()
    uploaded: list[str] = []
    for name in store.CLEAN_DATA_FILES:
        src = data_dir / name
        if not src.exists():
            continue
        dest = f"gs://{bucket}/{name}"
        _run(["gcloud", "storage", "cp", str(src), dest, f"--project={project}"])
        uploaded.append(dest)
    return uploaded


def main() -> None:
    parser = argparse.ArgumentParser(description="Push clean data from data/ to GCS")
    parser.add_argument(
        "--gcs-bucket",
        type=str,
        default=os.environ.get("GCS_BUCKET", ""),
        help="GCS bucket name (default: GCS_BUCKET env)",
    )
    parser.add_argument(
        "--project",
        type=str,
        default=os.environ.get("GOOGLE_CLOUD_PROJECT", "mfg-eng-19197"),
        help="GCP project for gcloud commands (default: mfg-eng-19197)",
    )
    parser.add_argument(
        "--major-update",
        action="store_true",
        help="Create a versioned backup in the bucket before uploading new data.",
    )
    args = parser.parse_args()
    bucket = (args.gcs_bucket or os.environ.get("GCS_BUCKET", "")).strip()
    if not bucket:
        print("ERROR: Set GCS_BUCKET or pass --gcs-bucket", file=sys.stderr)
        sys.exit(1)
    project = (args.project or "").strip()
    if not project:
        print("ERROR: Set --project or GOOGLE_CLOUD_PROJECT", file=sys.stderr)
        sys.exit(1)

    try:
        if args.major_update:
            backup_info = _backup_bucket_before_push(bucket, project)
            print(
                f"Backup complete: {backup_info['version']} "
                f"({backup_info['copied_count']} objects) -> {backup_info['backup_prefix']}"
            )
        uris = _push_local_clean_files(bucket, project)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    if not uris:
        print("No clean data files found in data/. Run the pipeline first.")
        sys.exit(0)
    for u in uris:
        print(u)
    print(f"\nPushed {len(uris)} file(s) to gs://{bucket}/")


if __name__ == "__main__":
    main()
