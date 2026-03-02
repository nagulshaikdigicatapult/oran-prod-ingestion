#!/usr/bin/env python3
"""
Full ingestion pipeline (download + validate + safe ZIP extraction)

Reads:  inventory/download_inventory.full.json
Writes: downloads/* (artifacts; canonical source of truth)
        extracted_flat/* (full extracted contents of downloads/*.zip, nested zips extracted)
        extracted_docs/* (only .pdf/.docx/.xlsx copied from extracted_flat)
        reports/full_run_report.json

Key properties:
- Idempotent downloads: stable filename per ID (via inventory/id_filename_map.json if present)
- Safe downloads: atomic downloads using .part then rename
- Validates:
  - PDFs via pdfinfo
  - DOCX/XLSX/ZIP via unzip -t (zip container test)
- ZIP extraction:
  - Extract each downloads/*.zip into extracted_flat/<id>__<zip_stem>/
  - Recursively extract nested *.zip inside extracted_flat (zip-slip + limits)
  - Delete nested zips after extraction so: find extracted_flat -name "*.zip" => 0
- Docs view:
  - Copy only *.pdf/*.docx/*.xlsx from extracted_flat into extracted_docs (same relative layout)

Env flags:
- ORAN_EXTRACT_ZIPS=1|0 (default 1)
- ORAN_DELETE_NESTED_ZIPS=1|0 (default 1)
- ORAN_REBUILD_EXTRACTED=1|0 (default 0)  # wipe extracted_flat/docs and rebuild
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass
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
EXTRACTED_FLAT_DIR = ROOT / "extracted_flat"
EXTRACTED_DOCS_DIR = ROOT / "extracted_docs"

REPORT_DIR = ROOT / "reports"
REPORT_PATH = REPORT_DIR / "full_run_report.json"

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

USER_AGENT = os.getenv("ORAN_USER_AGENT", "oran-prod-ingestion/2.x")

ENABLE_ZIP_EXTRACTION = os.getenv("ORAN_EXTRACT_ZIPS", "1") == "1"
DELETE_NESTED_ZIPS = os.getenv("ORAN_DELETE_NESTED_ZIPS", "1") == "1"
REBUILD_EXTRACTED = os.getenv("ORAN_REBUILD_EXTRACTED", "0") == "1"

# -----------------------------
# External tools
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
    name = " ".join(name.split()).strip()
    name = re.sub(r"\s+\.(pdf|docx|xlsx|zip)$", r".\1", name, flags=re.IGNORECASE)
    return name


def safe_filename(name: str) -> str:
    name = normalize_filename(name)
    return re.sub(r"[^A-Za-z0-9.\- _()&]", "_", name)


def parse_content_disposition(cd: str) -> Optional[str]:
    if not cd:
        return None

    m = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        return requests.utils.unquote(m.group(1)).strip()

    m = re.search(r'filename\s*=\s*"([^"]+)"', cd, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    m = re.search(r"filename\s*=\s*([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip().strip('"')

    return None


def sniff_type(headers: Dict[str, str], first_bytes: bytes) -> str:
    ct = (headers.get("Content-Type") or "").split(";")[0].strip().lower()

    if first_bytes.startswith(b"%PDF"):
        return "application/pdf"

    if (
        first_bytes.startswith(b"PK\x03\x04")
        or first_bytes.startswith(b"PK\x05\x06")
        or first_bytes.startswith(b"PK\x07\x08")
    ):
        if ct in (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/x-zip-compressed",
            "application/zip",
        ):
            return ct if ct else "application/zip"
        return "application/zip"

    return ct or "application/octet-stream"


def infer_extension(inferred_type: str, filename_hint: Optional[str]) -> str:
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
    import subprocess  # nosec B404

    assert PDFINFO_BIN is not None
    p = subprocess.run(  # nosec B603
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
    import subprocess  # nosec B404

    assert UNZIP_BIN is not None
    p = subprocess.run(  # nosec B603
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

    return True, {}


def load_id_filename_map() -> Dict[str, str]:
    if not MAP_PATH.exists():
        return {}
    data = json.loads(MAP_PATH.read_text())
    mapping = data.get("mapping") or {}
    out: Dict[str, str] = {}
    for k, v in mapping.items():
        out[str(k)] = safe_filename(str(v))
    return out


# -----------------------------
# Safe ZIP extraction (zip-slip + limits + nested)
# -----------------------------
@dataclass(frozen=True)
class ZipSafetyLimits:
    max_depth: int = 6
    max_total_uncompressed_bytes: int = 2_000_000_000  # 2GB
    max_files: int = 50_000
    max_member_size_bytes: int = 500_000_000  # 500MB
    max_compression_ratio: float = 200.0


DOC_EXTS = {".pdf", ".docx", ".xlsx"}


def _is_within_directory(base_dir: Path, target: Path) -> bool:
    base = base_dir.resolve()
    tgt = target.resolve()
    try:
        tgt.relative_to(base)
        return True
    except ValueError:
        return False


def _zipinfo_is_symlink(zi: zipfile.ZipInfo) -> bool:
    return ((zi.external_attr >> 16) & 0o170000) == 0o120000


def safe_extract_zip(zip_path: Path, dest_dir: Path, limits: ZipSafetyLimits) -> Dict[str, Any]:
    if not zip_path.is_file():
        return {"extracted_files": 0, "extracted_docs": 0}

    dest_dir.mkdir(parents=True, exist_ok=True)

    extracted_files = 0
    extracted_docs = 0
    total_uncompressed = 0

    with zipfile.ZipFile(zip_path) as zf:
        infos = zf.infolist()

        if len(infos) > limits.max_files:
            raise ValueError(f"Zip {zip_path} too many entries: {len(infos)} > {limits.max_files}")

        for zi in infos:
            if zi.is_dir():
                continue

            # Ignore common OS junk if present in vendor zips
            if Path(zi.filename).name in {".DS_Store", "Thumbs.db"}:
                continue
            if zi.filename.startswith("__MACOSX/") or Path(zi.filename).name.startswith("._"):
                continue

            if _zipinfo_is_symlink(zi):
                raise ValueError(f"Zip {zip_path} contains symlink entry: {zi.filename}")

            if zi.file_size > limits.max_member_size_bytes:
                raise ValueError(f"Zip {zip_path} member too large: {zi.filename} size={zi.file_size}")

            total_uncompressed += zi.file_size
            if total_uncompressed > limits.max_total_uncompressed_bytes:
                raise ValueError(f"Zip {zip_path} total uncompressed too large: {total_uncompressed}")

            if zi.file_size > 0:
                if zi.compress_size == 0:
                    raise ValueError(f"Zip {zip_path} suspicious compress_size=0 for: {zi.filename}")
                ratio = zi.file_size / max(1, zi.compress_size)
                if ratio > limits.max_compression_ratio:
                    raise ValueError(f"Zip {zip_path} compression ratio too high for {zi.filename}: {ratio:.2f}")

            member_rel = Path(zi.filename)
            if member_rel.is_absolute():
                raise ValueError(f"Zip {zip_path} has absolute path entry: {zi.filename}")

            out_path = dest_dir / member_rel
            if not _is_within_directory(dest_dir, out_path):
                raise ValueError(f"Zip-slip detected in {zip_path}: {zi.filename}")

            out_path.parent.mkdir(parents=True, exist_ok=True)

            with zf.open(zi, "r") as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

            extracted_files += 1
            if out_path.suffix.lower() in DOC_EXTS:
                extracted_docs += 1

    return {"extracted_files": extracted_files, "extracted_docs": extracted_docs}


def recursively_extract_nested_zips(root_dir: Path, limits: ZipSafetyLimits, delete_nested: bool) -> int:
    root_dir = root_dir.resolve()
    if not root_dir.exists():
        return 0

    extracted_count = 0

    for _depth in range(limits.max_depth):
        zips = sorted(root_dir.rglob("*.zip"))
        if not zips:
            break

        for zp in zips:
            dest = zp.parent
            safe_extract_zip(zp, dest, limits)
            extracted_count += 1
            if delete_nested:
                zp.unlink()

    remaining = list(root_dir.rglob("*.zip"))
    if remaining:
        sample = "\n".join(str(p) for p in remaining[:10])
        raise RuntimeError(f"Nested zip extraction hit max_depth but zips remain. Sample:\n{sample}")

    return extracted_count


def copy_docs_view(from_dir: Path, docs_dir: Path) -> int:
    if not from_dir.exists():
        return 0
    docs_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    for p in from_dir.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in DOC_EXTS:
            continue
        rel = p.relative_to(from_dir)
        dst = docs_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, dst)
        copied += 1
    return copied


# -----------------------------
# Download one
# -----------------------------
def download_one(session: requests.Session, id_: str, url: str, expected_name: Optional[str]) -> Dict[str, Any]:
    if expected_name:
        expected_path = DL_DIR / expected_name
        if expected_path.exists():
            return {"id": id_, "skipped": True, "path": str(expected_path), "reason": "already_exists", "type": None}

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

    if expected_name:
        filename = expected_name
    elif filename_cd:
        filename = safe_filename(filename_cd)
    else:
        filename = safe_filename(f"o-ran_{id_}.{ext}")

    if Path(filename).suffix == "":
        filename = f"{filename}.{ext}"

    filename = safe_filename(filename)

    final_path = DL_DIR / filename
    tmp_path = DL_DIR / f".{filename}.part"

    if final_path.exists():
        return {"id": id_, "skipped": True, "path": str(final_path), "reason": "already_exists", "type": inferred}

    with session.get(url, stream=True, timeout=TIMEOUT) as r:
        r.raise_for_status()
        cd_full = r.headers.get("Content-Disposition", "") or ""
        content_len = r.headers.get("Content-Length")

        if not expected_name:
            filename_full = parse_content_disposition(cd_full)
            if filename_full:
                filename2 = safe_filename(filename_full)
                filename2 = safe_filename(filename2)

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
    items = inv.get("items", inv)

    id_map = load_id_filename_map()

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    results: list[Dict[str, Any]] = []
    summary = defaultdict(int)
    total_bytes = 0

    # Derived outputs can be rebuilt safely
    if REBUILD_EXTRACTED:
        shutil.rmtree(EXTRACTED_FLAT_DIR, ignore_errors=True)
        shutil.rmtree(EXTRACTED_DOCS_DIR, ignore_errors=True)

    # ----------------- Download stage -----------------
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

                    summary[f"type::{res.get('type', 'unknown')}"] += 1
                    break

                except Exception as e:
                    attempt += 1
                    if attempt >= MAX_RETRIES:
                        results.append({"id": id_, "error": str(e)})
                        summary["failed"] += 1
                    else:
                        time.sleep(BACKOFF_BASE**attempt)

            time.sleep(SLEEP_BETWEEN)

    # ----------------- ZIP extraction stage -----------------
    zip_extract_report: list[Dict[str, Any]] = []
    nested_total = 0
    docs_copied_total = 0

    if ENABLE_ZIP_EXTRACTION:
        limits = ZipSafetyLimits()

        EXTRACTED_FLAT_DIR.mkdir(parents=True, exist_ok=True)
        EXTRACTED_DOCS_DIR.mkdir(parents=True, exist_ok=True)

        # Map ID -> downloaded path from results
        id_to_path: Dict[str, Path] = {}
        for r in results:
            if r.get("error"):
                continue
            if r.get("id") and r.get("path"):
                id_to_path[str(r["id"])] = Path(r["path"])

        for id_, dl_path in sorted(id_to_path.items(), key=lambda x: int(x[0]) if x[0].isdigit() else x[0]):
            if dl_path.suffix.lower() != ".zip":
                continue
            if not dl_path.exists():
                continue

            dest = EXTRACTED_FLAT_DIR / f"{id_}__{dl_path.stem}"

            # Idempotent: if already extracted and not rebuilding, skip
            if dest.exists() and any(dest.iterdir()) and not REBUILD_EXTRACTED:
                zip_extract_report.append(
                    {"id": id_, "zip": str(dl_path), "dest": str(dest), "skipped": True, "reason": "already_extracted"}
                )
                continue

            if dest.exists():
                shutil.rmtree(dest, ignore_errors=True)
            dest.mkdir(parents=True, exist_ok=True)

            try:
                stats = safe_extract_zip(dl_path, dest, limits)
                nested = recursively_extract_nested_zips(dest, limits, delete_nested=DELETE_NESTED_ZIPS)
                nested_total += nested

                zip_extract_report.append(
                    {
                        "id": id_,
                        "zip": str(dl_path),
                        "dest": str(dest),
                        "skipped": False,
                        "extracted_files": stats["extracted_files"],
                        "extracted_docs_found": stats["extracted_docs"],
                        "nested_zips_extracted": nested,
                    }
                )
                summary["zip_extracted"] += 1
                summary["nested_zips_extracted"] += nested

            except Exception as e:
                zip_extract_report.append({"id": id_, "zip": str(dl_path), "dest": str(dest), "error": str(e)})
                summary["zip_extract_failed"] += 1

        # Refresh docs view (copy from flat)
        if EXTRACTED_DOCS_DIR.exists() and any(EXTRACTED_DOCS_DIR.iterdir()):
            # keep simple: wipe docs view and rebuild from flat each run
            shutil.rmtree(EXTRACTED_DOCS_DIR, ignore_errors=True)
            EXTRACTED_DOCS_DIR.mkdir(parents=True, exist_ok=True)

        docs_copied_total = copy_docs_view(EXTRACTED_FLAT_DIR, EXTRACTED_DOCS_DIR)
        summary["docs_copied_from_zips"] = docs_copied_total

        remaining_nested = len(list(EXTRACTED_FLAT_DIR.rglob("*.zip")))
        summary["remaining_nested_zips_in_extracted_flat"] = remaining_nested

    report = {
        "source_inventory": str(INV_PATH),
        "id_filename_map": str(MAP_PATH) if MAP_PATH.exists() else None,
        "total_items": len(items),
        "summary": dict(summary),
        "total_bytes_downloaded": total_bytes,
        "zip_extraction": {
            "enabled": ENABLE_ZIP_EXTRACTION,
            "extracted_flat_dir": str(EXTRACTED_FLAT_DIR),
            "extracted_docs_dir": str(EXTRACTED_DOCS_DIR),
            "delete_nested_zips": DELETE_NESTED_ZIPS,
            "rebuild_extracted": REBUILD_EXTRACTED,
            "nested_zips_extracted_total": nested_total,
            "docs_copied_total": docs_copied_total,
        },
        "zip_extract_report": zip_extract_report,
        "results": results,
    }

    REPORT_PATH.write_text(json.dumps(report, indent=2))
    print(f"Wrote report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
