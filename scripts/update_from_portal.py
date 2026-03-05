#!/usr/bin/env python3
"""
Update pipeline from latest portal snapshot (Option A: keep archive; track present_on_portal).

Usage:
  python scripts/update_from_portal.py <portal_manifest.json>

Environment:
  ORAN_SKIP_CATALOG=1  -> "monitor mode" (CI-safe): write diff/status/delta then exit (no downloads, no catalog)

What it does (ingest mode):
  1) Saves snapshot under manifests/snapshots/YYYY-MM-DD/
  2) Writes manifests/raw/manifest.latest.json (unless same file)
  3) Computes NEW IDs (portal - inventory) and MISSING IDs (inventory - portal)
  4) Writes:
     - inventory/download_inventory.delta.json
     - reports/portal_diff.latest.json
     - reports/portal_status.latest.json
  5) Updates lockfile append-only for delta IDs (scripts/03_update_lockfile_from_delta.py)
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
from typing import List, Set, Tuple

REPO_ROOT = Path(".")
MANIFESTS_DIR = REPO_ROOT / "manifests"
INVENTORY_DIR = REPO_ROOT / "inventory"
REPORTS_DIR = REPO_ROOT / "reports"
SCRIPTS_DIR = REPO_ROOT / "scripts"

FULL_INV = INVENTORY_DIR / "download_inventory.full.json"
DELTA_INV = INVENTORY_DIR / "download_inventory.delta.json"

CATALOG_JSON = INVENTORY_DIR / "catalog.latest.json"
CATALOG_CSV = INVENTORY_DIR / "catalog.latest.csv"

LATEST_MANIFEST = MANIFESTS_DIR / "raw" / "manifest.latest.json"

SKIP_CATALOG = os.getenv("ORAN_SKIP_CATALOG", "0") == "1"


def run(cmd: List[str]) -> None:
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if r.returncode != 0:
        print(r.stdout)
        raise SystemExit(f"Command failed ({r.returncode}): {' '.join(cmd)}")
    if r.stdout.strip():
        print(r.stdout.rstrip())


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")


def _stable_obj(obj):
    """Return stable report content ignoring generated_at_utc (prevents PR noise)."""
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
            old = json.loads(path.read_text(encoding="utf-8"))
            if _stable_obj(old) == new_stable:
                return False
        except Exception:
            pass
    write_json(path, obj)
    return True


def ids_from_full_inventory(full_inv: dict) -> Set[str]:
    return {str(x["id"]) for x in full_inv.get("items", []) if "id" in x}


def ids_from_portal_list(portal: list) -> Set[str]:
    return {str(x.get("id")) for x in portal if x.get("id") is not None}


def build_delta_inventory(portal: list, new_ids: Set[str]) -> dict:
    items = [x for x in portal if str(x.get("id")) in new_ids]
    items.sort(key=lambda x: int(str(x.get("id"))))
    return {"items": items}


def write_portal_status(all_ids: Set[str], portal_ids: Set[str]) -> dict:
    items = [{"id": sid, "present_on_portal": sid in portal_ids} for sid in sorted(all_ids, key=lambda s: int(s))]
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "items": items,
    }


def _detect_id_col(rows: List[dict]) -> str | None:
    if not rows:
        return None
    if "id" in rows[0]:
        return "id"
    if "portal_id" in rows[0]:
        return "portal_id"
    return None


def inject_present_on_portal_to_catalog(portal_ids: Set[str]) -> Tuple[int, int]:
    """
    Adds present_on_portal to:
      - inventory/catalog.latest.json (boolean)
      - inventory/catalog.latest.csv  ("true"/"false")

    Returns: (json_updated_count, csv_rows_written)
    """
    json_updated = 0
    csv_written = 0

    # JSON
    if CATALOG_JSON.exists():
        cat = read_json(CATALOG_JSON)
        if isinstance(cat, dict) and isinstance(cat.get("items"), list):
            items = cat["items"]
        elif isinstance(cat, list):
            items = cat
            cat = {"items": items}
        else:
            print("WARN: Unknown catalog.latest.json format; skipping JSON injection")
            items = None

        if items is not None:
            for it in items:
                sid = None
                for k in ("id", "portal_id", "download_id"):
                    if k in it and it[k] is not None:
                        sid = str(it[k]).strip()
                        break
                if not sid:
                    continue
                it["present_on_portal"] = sid in portal_ids
                json_updated += 1
            cat["count"] = len(items)
            write_json(CATALOG_JSON, cat)
            print(f"catalog_json_present_on_portal_updated={json_updated}")

    # CSV
    if CATALOG_CSV.exists():
        rows: List[dict] = []
        with CATALOG_CSV.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            for row in reader:
                rows.append(row)

        id_col = _detect_id_col(rows)
        if not id_col:
            print("WARN: catalog.latest.csv has no id/portal_id column; skipping CSV injection")
            return (json_updated, 0)

        if "present_on_portal" not in fieldnames:
            fieldnames.append("present_on_portal")

        for row in rows:
            sid = str(row.get(id_col, "")).strip()
            row["present_on_portal"] = "true" if sid in portal_ids else "false"

        tmp = CATALOG_CSV.with_suffix(".csv.tmp")
        with tmp.open("w", newline="\n", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)

        tmp.replace(CATALOG_CSV)
        csv_written = len(rows)
        print(f"catalog_csv_present_on_portal_updated={csv_written}")

    return (json_updated, csv_written)


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/update_from_portal.py <portal_manifest.json>")
        return 2

    portal_path = Path(sys.argv[1])
    if not portal_path.exists():
        print(f"ERROR: portal manifest not found: {portal_path}")
        return 2

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    MANIFESTS_DIR.mkdir(parents=True, exist_ok=True)
    INVENTORY_DIR.mkdir(parents=True, exist_ok=True)

    portal = read_json(portal_path)
    if not isinstance(portal, list):
        print("ERROR: portal JSON must be a list of objects with at least {id, download_url}")
        return 2

    portal_ids = ids_from_portal_list(portal)

    # Snapshot + latest
    snap_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snap_dir = MANIFESTS_DIR / "snapshots" / snap_date
    snap_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(portal_path, snap_dir / portal_path.name)

    if portal_path.resolve() != LATEST_MANIFEST.resolve():
        shutil.copy2(portal_path, LATEST_MANIFEST)

    # Inventory
    if not FULL_INV.exists():
        print(f"ERROR: missing {FULL_INV}")
        return 2
    full_inv = read_json(FULL_INV)
    inv_ids = ids_from_full_inventory(full_inv)

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
        "policy": "Option A: keep archive; mark present_on_portal per ID; append-only lockfile",
    }
    write_json_if_changed(REPORTS_DIR / "portal_diff.latest.json", diff_report)
    if SKIP_CATALOG:
        print("Skipping catalog/download steps (ORAN_SKIP_CATALOG=1)")
        return 0
    all_ids = set(inv_ids) | set(new_ids)
    status = write_portal_status(all_ids, portal_ids)
    write_json_if_changed(REPORTS_DIR / "portal_status.latest.json", status)

    # Delta inventory is useful even in monitor mode
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
        # Still keep catalog status fresh on ingest runs
        run([sys.executable, "scripts/10_generate_catalog_from_inventory.py"])
        inject_present_on_portal_to_catalog(portal_ids)
        return 0

    # Update lockfile append-only from delta (expects DELTA_INV)
    run([sys.executable, "scripts/03_update_lockfile_from_delta.py"])

    # Download delta only: swap FULL_INV <-> DELTA_INV temporarily
    backup_full = FULL_INV.with_suffix(".full.backup.json")
    if backup_full.exists():
        backup_full.unlink()
    shutil.copy2(FULL_INV, backup_full)
    shutil.copy2(DELTA_INV, FULL_INV)

    try:
        run([sys.executable, "scripts/09_full_run_pipeline_v2.py"])
    finally:
        shutil.copy2(backup_full, FULL_INV)
        backup_full.unlink(missing_ok=True)

    run([sys.executable, "scripts/10_generate_catalog_from_inventory.py"])
    inject_present_on_portal_to_catalog(portal_ids)

    if (SCRIPTS_DIR / "12_create_title_view.py").exists():
        run([sys.executable, "scripts/12_create_title_view.py"])

    print("Done. Reports written:")
    print(f"  {REPORTS_DIR / 'portal_diff.latest.json'}")
    print(f"  {REPORTS_DIR / 'portal_status.latest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
