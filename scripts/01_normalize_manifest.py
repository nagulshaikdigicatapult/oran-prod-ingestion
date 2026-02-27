#!/usr/bin/env python3

import json
from pathlib import Path
from datetime import datetime
from rich import print

RAW_MANIFEST = Path("manifests/raw/manifest.json")
OUTPUT_FILE = Path("manifests/processed/normalized_manifest.json")

def normalize():
    data = json.loads(RAW_MANIFEST.read_text())

    normalized = []
    for item in data:
        normalized.append({
            "id": item["id"],
            "download_url": item["download_url"],
            "row_text": item["row_text"],
            "ingested_at": datetime.utcnow().isoformat() + "Z"
        })

    OUTPUT_FILE.write_text(json.dumps(normalized, indent=2))
    print(f"[green]Normalized manifest written to {OUTPUT_FILE}[/green]")
    print(f"[cyan]Total entries:[/cyan] {len(normalized)}")

if __name__ == "__main__":
    normalize()
