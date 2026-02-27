#!/usr/bin/env python3

import json
import requests
from pathlib import Path

INV_PATH = Path("inventory/download_inventory.json")

def is_pdf_magic(b: bytes) -> bool:
    return b.startswith(b"%PDF-")

def main():
    inv = json.loads(INV_PATH.read_text())

    session = requests.Session()
    session.headers.update({
        "User-Agent": "oran-prod-ingestion/1.0"
    })

    for item in inv["items"]:
        if not item.get("enabled", False):
            continue
        if item.get("status") not in ("planned", "http_ok", "http_error", "http_exception"):
            # allow rerun if needed, but don't overwrite downloaded states later
            pass

        try:
            with session.get(item["download_url"], allow_redirects=True, timeout=30, stream=True) as r:
                first = r.raw.read(4096)  # sniff only, do NOT download whole file

                item["http"] = {
                    "final_url": str(r.url),
                    "status_code": r.status_code,
                    "content_type": r.headers.get("Content-Type"),
                    "content_length": r.headers.get("Content-Length"),
                    "content_disposition": r.headers.get("Content-Disposition"),
                    "pdf_magic": is_pdf_magic(first),
                }

                if r.status_code == 200 and item["http"]["pdf_magic"]:
                    item["status"] = "http_ok_pdf"
                else:
                    item["status"] = "http_not_pdf"

        except Exception as e:
            item["status"] = "http_exception"
            item["http"] = {"error": str(e)}

    INV_PATH.write_text(json.dumps(inv, indent=2))
    print("HTTP preflight (GET+sniff) completed")

if __name__ == "__main__":
    main()
