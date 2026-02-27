#!/usr/bin/env python3

import json
import hashlib
import subprocess
from pathlib import Path
import requests

INV_PATH = Path("inventory/download_inventory.json")
DL_DIR = Path("downloads")
DL_DIR.mkdir(parents=True, exist_ok=True)

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def pdf_pages(path: Path) -> int:
    # pdfinfo output contains: Pages: <n>
    out = subprocess.check_output(["pdfinfo", str(path)], text=True, stderr=subprocess.STDOUT)
    for line in out.splitlines():
        if line.startswith("Pages:"):
            return int(line.split(":", 1)[1].strip())
    raise RuntimeError("Could not parse Pages from pdfinfo output")

def main():
    inv = json.loads(INV_PATH.read_text())
    session = requests.Session()
    session.headers.update({"User-Agent": "oran-prod-ingestion/1.0"})

    for item in inv["items"]:
        if not item.get("enabled", False):
            continue
        if item.get("status") != "http_ok_pdf":
            continue

        file_name = f"o-ran_{item['id']}.pdf"
        final_path = DL_DIR / file_name
        tmp_path = DL_DIR / f".{file_name}.part"

        try:
            with session.get(item["download_url"], stream=True, timeout=120) as r:
                r.raise_for_status()

                # Write atomically
                with tmp_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            tmp_path.replace(final_path)

            sha = sha256_file(final_path)
            pages = pdf_pages(final_path)
            size = final_path.stat().st_size

            item["file"] = {
                "path": str(final_path),
                "bytes": size,
                "sha256": sha,
                "pages": pages,
            }
            item["status"] = "downloaded"

        except Exception as e:
            item["status"] = "download_failed"
            item["file"] = {"error": str(e)}

            # cleanup partial
            if tmp_path.exists():
                tmp_path.unlink()

    INV_PATH.write_text(json.dumps(inv, indent=2))
    print("Download+validate completed")

if __name__ == "__main__":
    main()
