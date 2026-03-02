#!/usr/bin/env python3
"""
Full ingestion pipeline v2 (hardened)

Reads:  inventory/download_inventory.full.json
Writes: downloads/* (artifacts)
        reports/full_run_report_v2.json (run report)

Key properties:
- Idempotent: will skip already-downloaded files based on a stable filename per ID
- Safe: atomic downloads using .part then rename
- Validates:
  - PDFs via pdfinfo
  - DOCX/XLSX/ZIP via unzip -t (zip container test)
- Throttling & retries to be gentle to the portal
- Filename normalization to prevent duplicates like: "something-v04.00 .pdf"
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

# -----------------------------
# Paths
# -----------------------------
ROOT = Path(__file__).resolve().parents[1]
INV_PATH = ROOT / "inventory" / "download_inventory.full.json"
MAP_PATH = ROOT / "inventory" / "id_filename_map.json"  # optional (recommended)
DL_DIR = ROOT / "downloads"
REPORT_DIR = ROOT / "reports"

DL_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Tuning
# -----------------------------
BATCH_SIZE = int(os.getenv("ORAN_BATCH_SIZE", "25"))
SLEEP_BETWEEN = float(os.getenv("ORAN_SLEEP_BETWEEN", "0.4"))
MAX_RETRIES = int(os.getenv("ORAN_MAX_RETRIES", "3"))
BACKOFF_BASE = float(os.getenv("ORAN_BACKOFF_BASE", "2.0"))
TIMEOUT = int(os.getenv("ORAN_TIMEOUT", "300"))

USER_AGENT = os.getenv("ORAN_USER_AGENT", "oran-prod-ingestion/2.0")

# -----------------------------
# External tools (resolve full path)
# -----------------------------
PDFINFO_BIN = shutil.which("pdfinfo")
UNZIP_BIN = shutil.which("unzip")


def require_bins() -> None:
    missing = []
    if not PDFINFO_BIN:
        missing.append("pdfinfo (poppler-utils)")
    if not UNZIP_BIN:
        missing.append("unzip")
    if missing:
        raise RuntimeError(
            "Missing required system tools: "
            + ", ".join(missing)
            + ". Install with: sudo apt install -y poppler-utils unzip"
        )


# -----------------------------
# Helpers
# -----------------------------
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_filename(name: str) -> str:
    """
    Normalize filenames from Content-Disposition to ensure stable idempotency and avoid duplicates.

    Fixes your incident class:
      "O-RAN.WG1.OAM-Architecture-v04.00 .pdf" -> "O-RAN.WG1.OAM-Architecture-v04.00.pdf"
    """
    name = " ".join(name.split()).strip()  # collapse whitespace + trim ends
    name = re.sub(r"\s+\.(pdf|docx|xlsx|zip)$", r".\1", name, flags=re.IGNORECASE)
    return name


def safe_filename(name: str) -> str:
    """
    Make a filename safe-ish for common filesystems while keeping meaning.
    Keep letters, numbers, spaces, dots, underscores, hyphens, parentheses, and ampersand.
    Replace other characters with underscore.
    """
    name = normalize_filename(name)
    return re.sub(r"[^A-Za-z0-9.\- _()&]", "_", name)


def parse_content_disposition(cd: str) -> Optional[str]:
    """
    Extract filename from Content-Disposition.
    Prefers filename*=UTF-8''... then filename=...
    """
    if not cd:
        return None

    # filename*=UTF-8''...
    m = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        return requests.utils.unquote(m.group(1)).strip()

    # filename="..."
    m = re.search(r'filename\s*=\s*"([^"]+)"', cd, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # filename=...
    m = re.search(r"filename\s*=\s*([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip().strip('"')

    return None


def sniff_type(headers: Dict[str, str], first_bytes: bytes) -> str:
    """
    Infer type from Content-Type and magic bytes.
    Returns a normalized content type string.
    """
    ct = (headers.get("Content-Type") or "").split(";")[0].strip().lower()

    # magic checks
    if first_bytes.startswith(b"%PDF"):
        return "application/pdf"

    # ZIP magic (DOCX/XLSX and zips)
    if first_bytes.startswith(b"PK\x03\x04") or first_bytes.startswith(b"PK\x05\x06") or first_bytes.startswith(
        b"PK\x07\x08"
    ):
        # If server already says docx/xlsx keep it; else treat as zip
        if ct in (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/x-zip-compressed",
            "application/zip",
        ):
            return ct if ct else "application/zip"
        return "application/zip"

    # fallback to ct
    return ct or "application/octet-stream"


def infer_extension(inferred_type: str, filename_hint: Optional[str]) -> str:
    """
    Determine extension based on inferred type or filename hint.
    """
    if filename_hint:
        suffix = Path(filename_hint).suffix.lower()
        if suffix in (".pdf", ".docx", ".xlsx", ".zip"):
            return suffix.lstrip(".")

    mapping = {
        "application/pdf": "pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
        "application/x-zip-compressed": "zip",
        "application/zip": "zip",
    }
    return mapping.get(inferred_type, "bin")


def validate_pdf(path: Path) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate a PDF using pdfinfo.
    """
    # Bandit may warn about subprocess usage; we use absolute binary path and no shell, safe args.
    import subprocess  # nosec B404 (intentional; controlled args)

    assert PDFINFO_BIN is not None
    p = subprocess.run(  # nosec B603 (no shell, controlled args)
        [PDFINFO_BIN, str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if p.returncode != 0:
        return False, {"pdfinfo_ok": False, "pdfinfo_err": (p.stderr or p.stdout).strip()[:500]}

    pages = None
    title = None
    for line in p.stdout.splitlines():
        if line.startswith("Pages:"):
            try:
                pages = int(line.split(":", 1)[1].strip())
            except ValueError:
                pages = None
        if line.startswith("Title:"):
            title = line.split(":", 1)[1].strip() or None

    meta: Dict[str, Any] = {"pdfinfo_ok": True, "pages": pages}
    if title:
        meta["pdf_title"] = title
    return True, meta


def validate_zip_container(path: Path) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate ZIP container using unzip -t (works for .zip, .docx, .xlsx).
    """
    import subprocess  # nosec B404 (intentional; controlled args)

    assert UNZIP_BIN is not None
    p = subprocess.run(  # nosec B603 (no shell, controlled args)
        [UNZIP_BIN, "-t", str(path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    ok = p.returncode == 0
    return ok, {"unzip_test_ok": ok}


def validate(path: Path, inferred_type: str) -> Tuple[bool, Dict[str, Any]]:
    if inferred_type == "application/pdf":
        return validate_pdf(path)

    if inferred_type in (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/x-zip-compressed",
        "application/zip",
    ):
        return validate_zip_container(path)

    # Unknown types: just record file(1)-level info outside this script (optional)
    return True, {}


def load_id_filename_map() -> Dict[str, str]:
    """
    Optional: stable ID -> filename mapping (preferred for idempotency).
    """
    if not MAP_PATH.exists():
        return {}
    data = json.loads(MAP_PATH.read_text())
    mapping = data.get("mapping") or {}
    # normalize + safe sanitize to keep stable comparisons
    out: Dict[str, str] = {}
    for k, v in mapping.items():
        out[str(k)] = safe_filename(str(v))
    return out


def download_one(session: requests.Session, id_: str, url: str, expected_name: Optional[str]) -> Dict[str, Any]:
    """
    Download one artifact idempotently.

    Idempotency rule:
    - If expected_name exists in downloads/, skip.
    - Else derive filename from Content-Disposition + normalize, then check that final.
    """
    # 1) If we have a stable expected filename for this id, use it.
    if expected_name:
        expected_path = DL_DIR / expected_name
        if expected_path.exists():
            return {"id": id_, "skipped": True, "path": str(expected_path), "reason": "already_exists", "type": None}

    # 2) Fetch a small range to get headers + sniff type
    with session.get(url, stream=True, timeout=TIMEOUT, headers={"Range": "bytes=0-4095"}) as r:
        r.raise_for_status()
        first_bytes = b""
        for chunk in r.iter_content(4096):
            if chunk:
                first_bytes += chunk
                break

        inferred = sniff_type(r.headers, first_bytes)
        cd = r.headers.get("Content-Disposition", "") or ""
        filename_cd = parse_content_disposition(cd)

    ext = infer_extension(inferred, filename_cd or expected_name)

    # 3) Decide filename
    if expected_name:
        filename = expected_name
    elif filename_cd:
        filename = safe_filename(filename_cd)
    else:
        filename = safe_filename(f"o-ran_{id_}.{ext}")

    # Ensure extension matches inference if missing
    if Path(filename).suffix == "":
        filename = f"{filename}.{ext}"

    # Final normalization (critical for your duplicate bug class)
    filename = safe_filename(filename)

    final_path = DL_DIR / filename
    tmp_path = DL_DIR / f".{filename}.part"

    # Idempotency: if final path exists already, skip
    if final_path.exists():
        return {"id": id_, "skipped": True, "path": str(final_path), "reason": "already_exists", "type": inferred}

    # 4) Full download (atomic)
    with session.get(url, stream=True, timeout=TIMEOUT) as r:
        r.raise_for_status()
        cd_full = r.headers.get("Content-Disposition", "") or ""
        content_len = r.headers.get("Content-Length")

        # If we did not have a stable expected_name, prefer the full response CD (sometimes differs)
        if not expected_name:
            filename_full = parse_content_disposition(cd_full)
            if filename_full:
                filename2 = safe_filename(filename_full)
                # re-apply normalization to avoid " .pdf"
                filename2 = safe_filename(filename2)

                # If this differs, switch target (but keep idempotent checks)
                if filename2 != filename:
                    filename = filename2
                    final_path = DL_DIR / filename
                    tmp_path = DL_DIR / f".{filename}.part"
                    if final_path.exists():
                        return {
                            "id": id_,
                            "skipped": True,
                            "path": str(final_path),
                            "reason": "already_exists",
                            "type": inferred,
                        }

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
        "content_length_header": int(content_len) if (content_len and str(content_len).isdigit()) else None,
        "content_disposition": parse_content_disposition(cd_full) or parse_content_disposition(cd) or None,
        "validation_ok": ok,
        **vmeta,
    }


def main() -> None:
    require_bins()

    inv = json.loads(INV_PATH.read_text())
    items = inv.get("items", inv)  # allow either {items:[...]} or list

    id_map = load_id_filename_map()

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    results: list[Dict[str, Any]] = []
    summary = defaultdict(int)
    total_bytes = 0

    for i in range(0, len(items), BATCH_SIZE):
        batch = items[i : i + BATCH_SIZE]
        for item in batch:
            id_ = str(item["id"])
            url = item["download_url"]
            expected_name = id_map.get(id_)

            attempt = 0
            while attempt < MAX_RETRIES:
                try:
                    res = download_one(session, id_, url, expected_name)
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
                        time.sleep(BACKOFF_BASE**attempt)

            time.sleep(SLEEP_BETWEEN)

    report = {
        "source_inventory": str(INV_PATH),
        "id_filename_map": str(MAP_PATH) if MAP_PATH.exists() else None,
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
