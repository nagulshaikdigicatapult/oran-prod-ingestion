#!/usr/bin/env python3
import json
import os
import re
import time
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Tuple, Dict, Any, Optional

import requests
from requests.exceptions import RequestException

DL_DIR = Path("downloads")
REPORT_DIR = Path("reports")
INV_PATH = Path("inventory/download_inventory.full.json")  # default; you can swap to a runset

BATCH_SIZE = int(os.environ.get("ORAN_BATCH_SIZE", "1"))
SLEEP_BETWEEN = float(os.environ.get("ORAN_SLEEP_BETWEEN", "0.4"))
MAX_RETRIES = int(os.environ.get("ORAN_MAX_RETRIES", "3"))
BACKOFF_BASE = float(os.environ.get("ORAN_BACKOFF_BASE", "2.0"))

DL_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

def sanitize(name: str) -> str:
    # keep it safe for filesystem
    name = name.replace("\x00", "")
    name = re.sub(r"[\/\\]", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

def parse_content_disposition(cd: Optional[str]) -> Optional[str]:
    if not cd:
        return None
    # filename*=UTF-8''...
    m = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        return requests.utils.unquote(m.group(1)).strip().strip('"')
    # filename=...
    m = re.search(r"filename\s*=\s*([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip().strip('"')
    return None

def unique_target_name(filename: str, id_: str) -> str:
    # Only used when there's a real collision
    base, ext = os.path.splitext(filename)
    return f"{base}__id-{id_}{ext}"

def sha256_file(path: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def sniff_type_and_length(session: requests.Session, url: str) -> Tuple[str, Optional[int], Optional[str]]:
    """
    Do a GET range 0-0 to fetch headers and sniff type reliably.
    Returns: (inferred_type, content_length, content_disposition)
    """
    with session.get(url, stream=True, timeout=60, headers={"Range": "bytes=0-0"}) as r:
        r.raise_for_status()
        ctype = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        clen = r.headers.get("Content-Length")
        cd = r.headers.get("Content-Disposition")
        content_length = int(clen) if (clen and clen.isdigit()) else None
        return (ctype or "application/octet-stream", content_length, cd)

def validate_pdf(path: Path) -> Tuple[bool, Dict[str, Any]]:
    # use pdfinfo
    import subprocess
    p = subprocess.run(["pdfinfo", str(path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        return False, {"pages": None}
    m = re.search(r"^Pages:\s+(\d+)\s*$", p.stdout, flags=re.MULTILINE)
    pages = int(m.group(1)) if m else None
    return True, {"pages": pages}

def validate_zipcontainer(path: Path) -> Tuple[bool, Dict[str, Any]]:
    # unzip -t
    import subprocess
    p = subprocess.run(["unzip", "-t", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ok = (p.returncode == 0)
    return ok, {"unzip_test_ok": ok}

def validate(path: Path, inferred_type: str) -> Tuple[bool, Dict[str, Any]]:
    inferred_type = (inferred_type or "").lower()
    ext = path.suffix.lower()

    if inferred_type == "application/pdf" or ext == ".pdf":
        return validate_pdf(path)

    if inferred_type in {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/x-zip-compressed",
        "application/zip",
    } or ext in {".docx", ".xlsx", ".zip"}:
        return validate_zipcontainer(path)

    # fallback: at least ensure file exists and is non-empty
    return (path.exists() and path.stat().st_size > 0), {}

def fallback_ext(inferred: str) -> str:
    return {
        "application/pdf": "pdf",
        "application/x-zip-compressed": "zip",
        "application/zip": "zip",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    }.get(inferred, "bin")

def download_one(session: requests.Session, item: Dict[str, Any]) -> Dict[str, Any]:
    id_ = str(item["id"])
    url = item["download_url"]

    inferred, content_len, cd = sniff_type_and_length(session, url)
    orig = parse_content_disposition(cd)
    filename = sanitize(orig) if orig else None
    if not filename:
        filename = f"o-ran_{id_}.{fallback_ext(inferred)}"

    # ✅ idempotency fix: check canonical name first
    canonical_path = DL_DIR / filename
    if canonical_path.exists():
        return {
            "id": id_,
            "skipped": True,
            "path": str(canonical_path),
            "type": inferred,
            "reason": "already_exists",
            "content_disposition": cd,
            "content_length_header": content_len,
            **{k: item.get(k) for k in ["display_title","doc_code","month_year","doc_kind","release","row_text"]},
        }

    # handle true collision only
    final_name = filename
    final_path = DL_DIR / final_name
    if final_path.exists():
        final_name = unique_target_name(filename, id_)
        final_path = DL_DIR / final_name

    tmp_path = DL_DIR / f".{final_name}.part"

    # full download (atomic)
    with session.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with tmp_path.open("wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
    tmp_path.replace(final_path)

    ok, vmeta = validate(final_path, inferred)

    return {
        "id": id_,
        "skipped": False,
        "type": inferred,
        "path": str(final_path),
        "bytes": final_path.stat().st_size,
        "sha256": sha256_file(final_path),
        "content_length_header": content_len,
        "content_disposition": cd,
        "validation_ok": ok,
        **vmeta,
        **{k: item.get(k) for k in ["display_title","doc_code","month_year","doc_kind","release","row_text"]},
    }

def main():
    inv = json.loads(INV_PATH.read_text())
    items = inv["items"]

    session = requests.Session()
    session.headers.update({"User-Agent": "oran-prod-ingestion/2.0"})

    results = []
    summary = defaultdict(int)
    total_bytes = 0

    for i in range(0, len(items), BATCH_SIZE):
        batch = items[i:i+BATCH_SIZE]
        for item in batch:
            if not item.get("enabled", True):
                summary["disabled"] += 1
                continue

            attempt = 0
            while attempt < MAX_RETRIES:
                try:
                    res = download_one(session, item)
                    results.append(res)

                    if res.get("skipped"):
                        summary["skipped"] += 1
                    else:
                        if res.get("validation_ok"):
                            summary["downloaded_ok"] += 1
                        else:
                            summary["downloaded_but_invalid"] += 1
                        total_bytes += int(res.get("bytes") or 0)

                    summary[f"type::{res.get('type','unknown')}"] += 1
                    break

                except RequestException as e:
                    attempt += 1
                    if attempt >= MAX_RETRIES:
                        results.append({"id": str(item["id"]), "error": str(e)})
                        summary["failed"] += 1
                    else:
                        time.sleep(BACKOFF_BASE ** attempt)

            time.sleep(SLEEP_BETWEEN)

    report = {
        "source_inventory": str(INV_PATH),
        "total_items": len(items),
        "summary": dict(summary),
        "total_bytes_downloaded": total_bytes,
        "results": results,
    }

    out = REPORT_DIR / "full_run_report_v2.json"
    out.write_text(json.dumps(report, indent=2))
    print(f"Wrote report: {out}")

if __name__ == "__main__":
    main()
