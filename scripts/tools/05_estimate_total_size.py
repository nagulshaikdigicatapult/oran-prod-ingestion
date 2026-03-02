#!/usr/bin/env python3

import json
from collections import defaultdict
from pathlib import Path

import requests

SRC = Path("inventory/download_inventory.full.json")
OUT = Path("reports/size_estimate_162.json")
OUT.parent.mkdir(parents=True, exist_ok=True)


def is_pdf_magic(b: bytes) -> bool:
    return b.startswith(b"%PDF-")


def main():
    inv = json.loads(SRC.read_text())

    s = requests.Session()
    s.headers.update({"User-Agent": "oran-prod-ingestion/1.0"})

    by_type = defaultdict(lambda: {"count": 0, "bytes_known": 0, "bytes_unknown": 0})
    items_out = []

    for item in inv["items"]:
        url = item["download_url"]
        try:
            with s.get(url, allow_redirects=True, timeout=30, stream=True) as r:
                first = r.raw.read(4096)
                ctype = (r.headers.get("Content-Type") or "unknown").split(";")[0].strip()
                clen = r.headers.get("Content-Length")
                clen_int = int(clen) if (clen and clen.isdigit()) else None

                pdf_magic = is_pdf_magic(first)

                # trust magic over header only for PDF detection
                inferred = "application/pdf" if pdf_magic else ctype

                rec = {
                    "id": item["id"],
                    "status_code": r.status_code,
                    "content_type": ctype,
                    "inferred_type": inferred,
                    "content_length": clen_int,
                    "final_url": str(r.url),
                    "pdf_magic": pdf_magic,
                }
                items_out.append(rec)

                by_type[inferred]["count"] += 1
                if clen_int is None:
                    by_type[inferred]["bytes_unknown"] += 1
                else:
                    by_type[inferred]["bytes_known"] += clen_int

        except Exception as e:
            items_out.append(
                {
                    "id": item["id"],
                    "error": str(e),
                }
            )
            by_type["errors"]["count"] += 1

    report = {
        "source": str(SRC),
        "total_items": len(inv["items"]),
        "summary_by_type": by_type,
        "items": items_out,
    }

    # convert defaultdict to normal dict for JSON
    report["summary_by_type"] = {k: v for k, v in report["summary_by_type"].items()}

    OUT.write_text(json.dumps(report, indent=2))
    print(f"Wrote report: {OUT}")


if __name__ == "__main__":
    main()
