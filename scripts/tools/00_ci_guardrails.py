#!/usr/bin/env python3
"""
CI guardrails (no network):
- Ensure portal manifest is "rich" (contains row_text)
- Ensure normalized manifest has metadata for most portal-visible rows
- Ensure catalog has metadata + present_on_portal column
"""

import csv
import json
from pathlib import Path

RAW = Path("manifests/raw/manifest.latest.json")
NORM = Path("manifests/processed/normalized_manifest.json")
CAT_CSV = Path("inventory/catalog.latest.csv")


def die(msg: str) -> None:
    raise SystemExit(f"CI guardrail failed: {msg}")


def main() -> None:
    # 1) Raw manifest must be rich
    if not RAW.exists():
        die(f"missing {RAW}")

    raw = json.loads(RAW.read_text())
    if not isinstance(raw, list) or not raw:
        die("manifest.latest.json must be a non-empty JSON list")

    sample = raw[0]
    if not isinstance(sample, dict):
        die("manifest.latest.json entries must be objects")

    required_raw_keys = {"id", "download_url", "row_text"}
    if not required_raw_keys.issubset(sample.keys()):
        die(f"manifest.latest.json must include keys {required_raw_keys}, got {set(sample.keys())}")

    # 2) Normalized must exist and contain metadata
    if not NORM.exists():
        die(f"missing {NORM}")

    norm = json.loads(NORM.read_text())
    if not isinstance(norm, list) or not norm:
        die("normalized_manifest.json must be a non-empty JSON list")

    # Count how many have display_title non-null
    titled = sum(1 for r in norm if isinstance(r, dict) and (r.get("display_title") or "").strip())
    # Expect most portal-visible rows to have titles; allow some missing due to portal formatting
    if titled < 50:
        die(f"normalized_manifest.json too few titles: {titled}")

    # 3) Catalog CSV must have present_on_portal and some titles
    if not CAT_CSV.exists():
        die(f"missing {CAT_CSV}")

    with CAT_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            die("catalog.latest.csv has no header")

        if "present_on_portal" not in reader.fieldnames:
            die("catalog.latest.csv missing column: present_on_portal")

        rows = list(reader)
        if not rows:
            die("catalog.latest.csv has no rows")

        csv_titled = sum(1 for row in rows if (row.get("display_title") or "").strip())
        if csv_titled < 50:
            die(f"catalog.latest.csv too few titles: {csv_titled}")

    print("CI guardrails OK")


if __name__ == "__main__":
    main()
