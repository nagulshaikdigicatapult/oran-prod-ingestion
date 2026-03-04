#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

IN_MANIFEST = Path("manifests/processed/normalized_manifest.json")

# IMPORTANT:
# download_inventory.full.json is the ARCHIVE inventory (source of truth for what we keep).
# This script builds an inventory *from metadata* and MUST NOT overwrite the archive by default.
DEFAULT_OUT = Path("inventory/download_inventory.from_metadata.json")


def build_items(data: list) -> list:
    items = []
    for rec in data:
        item = {
            "id": str(rec.get("id", "")).strip(),
            "download_url": str(rec.get("download_url", "")).strip(),
            # control flags
            "status": "planned",
            "enabled": True,
            # provenance + portal metadata (human-friendly)
            "row_text": rec.get("row_text"),
            "display_title": rec.get("display_title"),
            "doc_code": rec.get("doc_code"),
            "month_year": rec.get("month_year"),
            "doc_kind": rec.get("doc_kind"),
            "release": rec.get("release"),
        }
        items.append(item)
    return items


def main() -> None:
    ap = argparse.ArgumentParser(description="Build an inventory JSON from normalized manifest metadata.")
    ap.add_argument(
        "--out",
        default=str(DEFAULT_OUT),
        help="Output path for generated inventory (default: inventory/download_inventory.from_metadata.json)",
    )
    ap.add_argument(
        "--allow-overwrite-archive",
        action="store_true",
        help="Allow writing directly to inventory/download_inventory.full.json (NOT recommended).",
    )
    args = ap.parse_args()

    out_path = Path(args.out)

    # Protect archive inventory by default
    archive_path = Path("inventory/download_inventory.full.json")
    if out_path.resolve() == archive_path.resolve() and not args.allow_overwrite_archive:
        raise SystemExit(
            "Refusing to overwrite inventory/download_inventory.full.json.\n"
            "That file is the archive inventory source of truth.\n"
            "Use --out inventory/download_inventory.from_metadata.json (default)\n"
            "or pass --allow-overwrite-archive if you REALLY intend this."
        )

    data = json.loads(IN_MANIFEST.read_text())
    if not isinstance(data, list):
        raise SystemExit("normalized_manifest.json must be a JSON array")

    generated_at = datetime.utcnow().isoformat() + "Z"
    items = build_items(data)

    out = {
        "generated_at": generated_at,
        "source": str(IN_MANIFEST),
        "count": len(items),
        "items": items,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2) + "\n")
    print(f"Wrote inventory: {out_path} (items={len(items)})")


if __name__ == "__main__":
    main()
