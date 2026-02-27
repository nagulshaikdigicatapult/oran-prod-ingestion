#!/usr/bin/env python3
import json
from datetime import datetime
from pathlib import Path

IN_MANIFEST = Path("manifests/processed/normalized_manifest.json")
OUT_FULL = Path("inventory/download_inventory.full.json")

def main():
    data = json.loads(IN_MANIFEST.read_text())
    if not isinstance(data, list):
        raise SystemExit("normalized_manifest.json must be a JSON array")

    generated_at = datetime.utcnow().isoformat() + "Z"
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

    out = {
        "generated_at": generated_at,
        "source": str(IN_MANIFEST),
        "count": len(items),
        "items": items,
    }

    OUT_FULL.parent.mkdir(parents=True, exist_ok=True)
    OUT_FULL.write_text(json.dumps(out, indent=2))
    print(f"Wrote inventory: {OUT_FULL} (items={len(items)})")

if __name__ == "__main__":
    main()
