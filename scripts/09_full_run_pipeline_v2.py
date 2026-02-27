#!/usr/bin/env python3

import json
import hashlib
import re
import time
import subprocess
from pathlib import Path
from urllib.parse import unquote
from collections import defaultdict

import requests

INV_PATH = Path("inventory/download_inventory.full.json")
DL_DIR = Path("downloads")
REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)
DL_DIR.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = 20
SLEEP_BETWEEN = 2          # polite throttling
MAX_RETRIES = 3
BACKOFF_BASE = 2           # exponential backoff: 2,4,8 sec

SAFE_RE = re.compile(r"[^A-Za-z0-9._()\- ]+")

def sanitize(name: str) -> str:
    name = name.strip().replace("\n", " ")
    name = name.replace("/", "_").replace("\\", "_")
    name = SAFE_RE.sub("_", name)
    name = re.sub(r"[ _]{2,}", "_", name)
    # remove space before extension if any
    name = re.sub(r"\s+(\.[A-Za-z0-9]{2,6})$", r"\1", name)
    return name[:180] if len(name) > 180 else name

def parse_content_disposition(cd: str | None) -> str | None:
    if not cd:
        return None
    m = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        return unquote(m.group(1))
    m = re.search(r"filename\s*=\s*\"?([^\";]+)\"?", cd, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def run_cmd(cmd):
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
        return 0, out
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output

def infer_type(first: bytes, header_type: str | None) -> str:
    ht = (header_type or "unknown").split(";")[0].strip()
    if first.startswith(b"%PDF-"):
        return "application/pdf"
    if first.startswith(b"PK\x03\x04"):
        return ht if ht != "unknown" else "application/zip"
    return ht

def validate(path: Path, inferred: str):
    if inferred == "application/pdf":
        rc, out = run_cmd(["pdfinfo", str(path)])
        if rc != 0:
            return False, {"validation": "pdfinfo_fail", "detail": out.strip()}
        pages = None
        for line in out.splitlines():
            if line.startswith("Pages:"):
                pages = int(line.split(":", 1)[1].strip())
                break
        return True, {"validation": "pdf_ok", "pages": pages}
    elif (
        "zip" in inferred
        or "wordprocessingml.document" in inferred
        or "spreadsheetml.sheet" in inferred
    ):
        rc, out = run_cmd(["unzip", "-t", str(path)])
        return (rc == 0), {
            "validation": "zipcontainer_ok" if rc == 0 else "zipcontainer_fail",
            "detail": out.strip() if rc != 0 else None,
        }
    else:
        return True, {"validation": "no_rule"}

def unique_target_name(desired: str, id_: str) -> str:
    """Avoid collisions by appending __id-<id> before extension."""
    target = DL_DIR / desired
    if not target.exists():
        return desired
    stem = target.stem
    suf = target.suffix
    return f"{stem}__id-{id_}{suf}"

def download_one(session: requests.Session, id_: str, url: str):
    # Use GET+sniff to infer type and get Content-Disposition filename
    with session.get(url, allow_redirects=True, timeout=30, stream=True, headers={"Range": "bytes=0-4095"}) as r:
        first = r.raw.read(4096)
        inferred = infer_type(first, r.headers.get("Content-Type"))
        cd = r.headers.get("Content-Disposition")
        orig = parse_content_disposition(cd)
        content_len = r.headers.get("Content-Length")

    filename = sanitize(orig) if orig else None
    if not filename:
        # fallback deterministic name
        ext = {
            "application/pdf": "pdf",
            "application/x-zip-compressed": "zip",
            "application/zip": "zip",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
        }.get(inferred, "bin")
        filename = f"o-ran_{id_}.{ext}"

    canonical_path = DL_DIR / filename
    # Idempotency: if canonical (original) filename already exists, skip
    if canonical_path.exists():
        return {"id": id_, "skipped": True, "path": str(canonical_path), "type": inferred, "reason": "already_exists"}

    filename = unique_target_name(filename, id_)
    final_path = DL_DIR / filename
    tmp_path = DL_DIR / f".{filename}.part"
    # Full download
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
        "content_length_header": int(content_len) if (content_len and content_len.isdigit()) else None,
        "content_disposition": cd,
        "validation_ok": ok,
        **vmeta,
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
            id_ = str(item["id"])
            url = item["download_url"]

            attempt = 0
            while attempt < MAX_RETRIES:
                try:
                    res = download_one(session, id_, url)
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

                except Exception as e:
                    attempt += 1
                    if attempt >= MAX_RETRIES:
                        results.append({"id": id_, "error": str(e)})
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
