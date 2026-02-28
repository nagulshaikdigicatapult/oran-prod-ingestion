#!/usr/bin/env python3

import hashlib
import json
import subprocess
from pathlib import Path

import requests

INV_PATH = Path("inventory/runset-10-anytype.json")
DL_DIR = Path("downloads")
DL_DIR.mkdir(parents=True, exist_ok=True)

EXT_MAP = {
    "application/pdf": "pdf",
    "application/x-zip-compressed": "zip",
    "application/zip": "zip",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
}

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def run_cmd(cmd: list[str]) -> tuple[int, str]:
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
        return 0, out
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output

def pdf_pages(path: Path) -> int:
    rc, out = run_cmd(["pdfinfo", str(path)])
    if rc != 0:
        raise RuntimeError(out.strip())
    for line in out.splitlines():
        if line.startswith("Pages:"):
            return int(line.split(":", 1)[1].strip())
    raise RuntimeError("Pages not found in pdfinfo output")

def main():
    inv = json.loads(INV_PATH.read_text())
    s = requests.Session()
    s.headers.update({"User-Agent": "oran-prod-ingestion/1.0"})

    for item in inv["items"]:
        if item.get("status") != "http_ok":
            continue

        inferred = (item.get("http", {}) or {}).get("inferred_type") or "unknown"
        ext = EXT_MAP.get(inferred, "bin")

        file_name = f"o-ran_{item['id']}.{ext}"
        final_path = DL_DIR / file_name
        tmp_path = DL_DIR / f".{file_name}.part"

        try:
            with s.get(item["download_url"], stream=True, timeout=300) as r:
                r.raise_for_status()
                with tmp_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            tmp_path.replace(final_path)

            meta = {
                "path": str(final_path),
                "bytes": final_path.stat().st_size,
                "sha256": sha256_file(final_path),
                "type": inferred,
            }

            # Validation
            if ext == "pdf":
                meta["pages"] = pdf_pages(final_path)
                item["status"] = "downloaded_pdf"
            elif ext in ("zip", "docx", "xlsx"):
                rc, out = run_cmd(["unzip", "-t", str(final_path)])
                meta["unzip_test_rc"] = rc
                meta["unzip_test_ok"] = (rc == 0)
                # "file" is useful for quick sanity
                _, fout = run_cmd(["file", "-b", str(final_path)])
                meta["file_magic"] = fout.strip()
                item["status"] = "downloaded_ok" if rc == 0 else "downloaded_but_invalid_zipcontainer"
            else:
                _, fout = run_cmd(["file", "-b", str(final_path)])
                meta["file_magic"] = fout.strip()
                item["status"] = "downloaded_untyped"

            item["file"] = meta

        except Exception as e:
            item["status"] = "download_failed"
            item["file"] = {"error": str(e)}
            if tmp_path.exists():
                tmp_path.unlink()

    INV_PATH.write_text(json.dumps(inv, indent=2))
    print("anytype download+validate done")

if __name__ == "__main__":
    main()
