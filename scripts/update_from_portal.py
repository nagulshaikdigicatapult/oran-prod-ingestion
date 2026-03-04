#!/usr/bin/env python3
"""
Update pipeline from latest portal snapshot (Option A: track present_on_portal).


Usage:
  python scripts/update_from_portal.py manifest_links_all.json

What it does:
  1) Saves snapshot under manifests/snapshots/YYYY-MM-DD/
  2) Writes manifests/raw/manifest.latest.json
  3) Computes NEW IDs (portal - inventory) and MISSING IDs (inventory - portal)
  4) Writes:
     - inventory/download_inventory.delta.json
     - reports/portal_diff.latest.json
     - reports/portal_status.latest.json (per-ID present_on_portal)
  5) Updates lockfile append-only for delta IDs (calls scripts/03_update_lockfile_from_delta.py)
  6) Downloads + validates + extracts delta only (temporarily swaps inventory to delta)
  7) Restores full inventory
  8) Regenerates catalog and injects present_on_portal into catalog JSON and CSV
"""

from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Set

SKIP_CATALOG = (os.getenv("ORAN_SKIP_CATALOG", "0") == "1")




REPO_ROOT = Path(".")
MANIFESTS_DIR = REPO_ROOT / "manifests"
INVENTORY_DIR = REPO_ROOT / "inventory"
REPORTS_DIR = REPO_ROOT / "reports"
SCRIPTS_DIR = REPO_ROOT / "scripts"

LOCKFILE = INVENTORY_DIR / "id_filename_map.json"
FULL_INV = INVENTORY_DIR / "download_inventory.full.json"
DELTA_INV = INVENTORY_DIR / "download_inventory.delta.json"

CATALOG_JSON = INVENTORY_DIR / "catalog.latest.json"
CATALOG_CSV = INVENTORY_DIR / "catalog.latest.csv"

LATEST_MANIFEST = MANIFESTS_DIR / "raw" / "manifest.latest.json"


def run(cmd: List[str]) -> None:
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if r.returncode != 0:
        print(r.stdout)
        raise SystemExit(f"Command failed ({r.returncode}): {' '.join(cmd)}")
    if r.stdout.strip():
        print(r.stdout.rstrip())


def read_json(path: Path):
    return json.loads(path.read_text())


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2) + "\n")

def _stable_obj(obj):
    """Return a stable version of report content (ignore generated_at_utc)."""
    if isinstance(obj, dict):
        return {k: _stable_obj(v) for k, v in obj.items() if k != "generated_at_utc"}
    if isinstance(obj, list):
        return [_stable_obj(x) for x in obj]
    return obj


def write_json_if_changed(path: Path, obj) -> bool:
    """Write JSON only if meaningful content changed. Returns True if written."""
    new_stable = _stable_obj(obj)

    if path.exists():
        try:
            old = json.loads(path.read_text())
            old_stable = _stable_obj(old)
            if old_stable == new_stable:
                # Do not rewrite file (prevents PR noise)
                return False
        except Exception:
            # If file is corrupted/unreadable, rewrite it
            pass

    write_json(path, obj)
    return True


def ids_from_full_inventory(full_inv: dict) -> Set[str]:
    return {str(x["id"]) for x in full_inv.get("items", [])}


def ids_from_portal_list(portal: list) -> Set[str]:
    return {str(x.get("id")) for x in portal if x.get("id") is not None}


def build_delta_inventory(portal: list, new_ids: Set[str]) -> dict:
    items = [x for x in portal if str(x.get("id")) in new_ids]
    # stable sort by numeric id
    items.sort(key=lambda x: int(str(x.get("id"))))
    return {"items": items}


def write_portal_status(all_ids: Set[str], portal_ids: Set[str]) -> dict:
    # per-id status
    status = []
    for sid in sorted(all_ids, key=lambda s: int(s)):
        status.append({
            "id": sid,
            "present_on_portal": sid in portal_ids
        })
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(status),
        "items": status
    }


def inject_present_on_portal_to_catalog(portal_ids: Set[str]) -> None:
    if SKIP_CATALOG:
        print("Skipping catalog regeneration/injection (ORAN_SKIP_CATALOG=1)")
        return 0

    """
    Adds present_on_portal boolean to catalog.latest.json + catalog.latest.csv.

    Assumptions:
      - catalog.latest.json contains items with an id field (id or portal_id)
      - catalog.latest.csv contains a column named id or portal_id
    """
    if CATALOG_JSON.exists():
        cat = read_json(CATALOG_JSON)

        # Determine item list
        if isinstance(cat, dict) and "items" in cat and isinstance(cat["items"], list):
            items = cat["items"]
        elif isinstance(cat, list):
            items = cat
            cat = {"items": items, "count": len(items)}
        else:
            # unknown format, skip
            print("WARN: Unknown catalog.latest.json format; skipping JSON injection")
            return

        changed = 0
        for it in items:
            # Try multiple possible keys
            sid = None
            for k in ("id", "portal_id", "download_id"):
                if k in it and it[k] is not None:
                    sid = str(it[k])
                    break
            if not sid:
                continue
            it["present_on_portal"] = (sid in portal_ids)
            changed += 1

        cat["count"] = len(items)
        write_json(CATALOG_JSON, cat)
        print(f"catalog_json_present_on_portal_updated={changed}")

    if CATALOG_CSV.exists():
        # Read CSV and rewrite with present_on_portal column
        rows = []
        with CATALOG_CSV.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            for row in reader:
                rows.append(row)

        # detect id column
        id_col = "id" if (rows and "id" in rows[0]) else ("portal_id" if (rows and "portal_id" in rows[0]) else None)
        if not id_col:
            print("WARN: catalog.latest.csv has no id/portal_id column; skipping CSV injection")
            return

        if "present_on_portal" not in fieldnames:
            fieldnames.append("present_on_portal")

        for row in rows:
            sid = str(row.get(id_col, "")).strip()
            row["present_on_portal"] = "true" if sid in portal_ids else "false"

        tmp = CATALOG_CSV.with_suffix(".csv.tmp")
        with tmp.open("w", newline="\n") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        tmp.replace(CATALOG_CSV)
        print(f"catalog_csv_present_on_portal_updated={len(rows)}")


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/update_from_portal.py manifest_links_all.json")
        return 2

    portal_path = Path(sys.argv[1])
    if not portal_path.exists():
        print(f"ERROR: portal manifest not found: {portal_path}")
        return 2

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Read portal snapshot
    portal = read_json(portal_path)
    if not isinstance(portal, list):
        print("ERROR: portal JSON must be a list of {id, download_url}")
        return 2

    portal_ids = ids_from_portal_list(portal)

    # 2) Snapshot + latest
    snap_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap_dir = MANIFESTS_DIR / "snapshots" / snap_date
    snap_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(portal_path, snap_dir / portal_path.name)
    # If user passes manifests/raw/manifest.latest.json itself, avoid SameFileError
    if portal_path.resolve() != LATEST_MANIFEST.resolve():
        shutil.copy2(portal_path, LATEST_MANIFEST)

    # 3) Read current full inventory
    if not FULL_INV.exists():
        print(f"ERROR: missing {FULL_INV}")
        return 2
    full_inv = read_json(FULL_INV)
    inv_ids = ids_from_full_inventory(full_inv)

    # 4) Compute diffs
    new_ids = portal_ids - inv_ids
    missing_ids = inv_ids - portal_ids

    diff_report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "portal_count": len(portal_ids),
        "inventory_count": len(inv_ids),
        "new_ids_count": len(new_ids),
        "missing_ids_count": len(missing_ids),
        "new_ids": sorted(list(new_ids), key=lambda s: int(s))[:5000],
        "missing_ids": sorted(list(missing_ids), key=lambda s: int(s))[:5000],
        "snapshot_path": str(snap_dir / portal_path.name),
        "latest_manifest_path": str(LATEST_MANIFEST),
        "policy": "Option A: keep archive; mark present_on_portal per ID; append-only lockfile"
    }
    write_json_if_changed(REPORTS_DIR / "portal_diff.latest.json", diff_report)

    # 5) Write per-ID portal status for the entire archive
    all_ids = set(inv_ids) | set(new_ids)  # after update, archive will include new_ids
    status = write_portal_status(all_ids, portal_ids)
    write_json_if_changed(REPORTS_DIR / "portal_status.latest.json", status)

    if SKIP_CATALOG:
        print("Skipping catalog regeneration/injection (ORAN_SKIP_CATALOG=1)")
        return 0


    # 6) Build delta inventory
    delta = build_delta_inventory(portal, new_ids)
    write_json(DELTA_INV, delta)

    print("update_from_portal_summary:")
    print(f"  portal_count={len(portal_ids)}")
    print(f"  inventory_count_before={len(inv_ids)}")
    print(f"  new_ids_count={len(new_ids)}")
    print(f"  missing_ids_count={len(missing_ids)}")
    print(f"  delta_inventory_items={len(delta.get('items', []))}")

    if len(new_ids) == 0:
        print("No new IDs to ingest.")
    if SKIP_CATALOG:
        print("Skipping catalog regeneration/injection (ORAN_SKIP_CATALOG=1)")
        # Still regenerate catalog and inject portal status
        run([sys.executable, "scripts/10_generate_catalog_from_inventory.py"])
        inject_present_on_portal_to_catalog(portal_ids)
        return 0

    # 7) Update lockfile append-only from delta
    run([sys.executable, "scripts/03_update_lockfile_from_delta.py"])

    # 8) Download delta only (safe batch mode):
    #    Temporarily swap FULL_INV with DELTA_INV and run the pipeline,
    #    then restore FULL_INV.
    backup_full = FULL_INV.with_suffix(".full.backup.json")
    if backup_full.exists():
        backup_full.unlink()

    shutil.copy2(FULL_INV, backup_full)
    shutil.copy2(DELTA_INV, FULL_INV)

    try:
        run([sys.executable, "scripts/09_full_run_pipeline_v2.py"])
    finally:
        # restore full inventory
        shutil.copy2(backup_full, FULL_INV)
        backup_full.unlink(missing_ok=True)

    # 9) Regenerate catalog and inject present_on_portal
    run([sys.executable, "scripts/10_generate_catalog_from_inventory.py"])
    inject_present_on_portal_to_catalog(portal_ids)

    # Optional: refresh title view if you use it regularly
    if (SCRIPTS_DIR / "12_create_title_view.py").exists():
        run([sys.executable, "scripts/12_create_title_view.py"])

    print("Done. Reports written:")
    print(f"  {REPORTS_DIR / 'portal_diff.latest.json'}")
    print(f"  {REPORTS_DIR / 'portal_status.latest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
