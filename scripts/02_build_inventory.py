#!/usr/bin/env python3

import json
from pathlib import Path
from datetime import datetime

SRC = Path("manifests/processed/normalized_manifest.json")
OUT = Path("inventory/download_inventory.json")

def main():
    items = json.loads(SRC.read_text())

    inv = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source_manifest": str(SRC),
        "count": len(items),
        "items": []
    }

    for it in items:
        inv["items"].append({
            "id": it["id"],
            "download_url": it["download_url"],
            "title_hint": it.get("row_text", "").strip(),
            "enabled": True,
            "status": "planned",   # planned|downloaded|failed|skipped
            "http": {},            # will be filled later (headers, status_code)
            "file": {}             # will be filled later (path, size, sha256, pages)
        })

    OUT.write_text(json.dumps(inv, indent=2))
    print(f"Wrote inventory: {OUT} (items={len(items)})")

if __name__ == "__main__":
    main()
