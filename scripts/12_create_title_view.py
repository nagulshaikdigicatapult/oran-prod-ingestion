#!/usr/bin/env python3
"""
Create a human-friendly "title view" of downloaded artifacts.

- Keeps canonical downloads in: downloads/  (official filenames from Content-Disposition)
- Creates symlinks in:         downloads_by_title/
  named using sanitized portal display_title + __id-<id> + original extension.

This avoids breaking idempotency and avoids title-based rename churn.
"""

import json
import os
import re
from pathlib import Path

INV_PATH = Path("inventory/download_inventory.full.json")
MAP_PATH = Path("inventory/id_filename_map.json")

CANON_DIR = Path("downloads")
TITLE_DIR = Path("downloads_by_title")

MAX_NAME = 180  # keep below common filesystem limits (255)


def sanitize_title(s: str) -> str:
    """
    Turn portal display_title into a filesystem-safe, human-friendly stem.

    Heuristics:
    - remove portal noise words (DOWNLOAD etc.)
    - strip embedded doc_code-like fragments (O-RAN.WG*, O-RAN-WG*, R00x-vYY.YY)
    - keep readability, avoid very long names
    """
    s = (s or "").strip()
    if not s:
        return "untitled"

    # Normalize whitespace early
    s = re.sub(r"\s+", " ", s).strip()

    # Remove obvious portal noise words (case-insensitive, whole words)
    noise_words = [
    "download",          # portal button text
    "technical report",  # sometimes redundant
    "recommendation",    # sometimes redundant
    "working group",
    "work group",
]
    for w in noise_words:
        s = re.sub(rf"\b{re.escape(w)}\b", "", s, flags=re.IGNORECASE).strip()
        s = re.sub(r"\s+", " ", s).strip()

    # Remove embedded doc-code-ish fragments that sometimes sneak into "titles"
    # Examples:
    #   O-RAN.WG1.TR.Use-Cases-Analysis-Report-R005-v19.00
    #   O-RAN-WG6.AppLCM-Deployment-R003-v02.00
    s = re.sub(r"\bO-RAN[.\-][A-Za-z0-9.\-_/]+?R\d{3}[-_]v\d{1,3}\.\d{1,3}\b", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\bO-RAN[.\-]WG\d{1,2}[.\-][A-Za-z0-9.\-_/]+\b", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\bR\d{3}[-_]?v\d{1,3}\.\d{1,3}\b", "", s, flags=re.IGNORECASE).strip()

    # Clean multiple spaces created by removals
    s = re.sub(r"\s+", " ", s).strip()

    # Replace filesystem-hostile characters with underscores
    s = re.sub(r"[\\/:*?\"<>|]+", "_", s)

    # Remove leftover weird punctuation at ends
    s = s.strip("._- ").rstrip(". ").strip()

    # Convert spaces to underscores for readability in terminals
    s = re.sub(r"\s+", "_", s)

    # Collapse repeated underscores
    s = re.sub(r"_+", "_", s)

    # Hard trim
    if len(s) > MAX_NAME:
        s = s[:MAX_NAME].rstrip("_")

    return s or "untitled"


def safe_symlink(src: Path, dst: Path) -> str:
    """
    Create/replace a symlink dst -> src.
    Returns status: created | updated | exists | failed
    """
    try:
        if dst.exists() or dst.is_symlink():
            # If already correct, keep
            try:
                if dst.is_symlink() and dst.resolve() == src.resolve():
                    return "exists"
            except Exception:
                pass
            dst.unlink()
            dst.symlink_to(src)
            return "updated"
        dst.symlink_to(src)
        return "created"
    except OSError as e:
        return f"failed: {e}"


def main():
    if not INV_PATH.exists():
        raise SystemExit(f"Missing {INV_PATH}. Run scripts/02_build_inventory.py first.")
    if not MAP_PATH.exists():
        raise SystemExit(f"Missing {MAP_PATH}. Generate inventory/id_filename_map.json first.")
    if not CANON_DIR.exists():
        raise SystemExit(f"Missing {CANON_DIR}. Run scripts/09_full_run_pipeline_v2.py first.")

    TITLE_DIR.mkdir(parents=True, exist_ok=True)

    inv = json.loads(INV_PATH.read_text())
    mp = json.loads(MAP_PATH.read_text())
    id_to_filename = mp.get("mapping", {})

    created = updated = exists = missing_src = missing_map = 0
    failed = []

    for it in inv["items"]:
        id_ = str(it["id"])
        display_title = it.get("display_title") or it.get("doc_code") or "untitled"

        canon_name = id_to_filename.get(id_)
        if not canon_name:
            missing_map += 1
            continue

        src = CANON_DIR / canon_name
        if not src.exists():
            missing_src += 1
            continue

        ext = src.suffix.lower() or ""
        title = sanitize_title(display_title)

        # Always include id to guarantee uniqueness
        link_name = f"{title}__id-{id_}{ext}"
        dst = TITLE_DIR / link_name

        status = safe_symlink(src, dst)
        if status == "created":
            created += 1
        elif status == "updated":
            updated += 1
        elif status == "exists":
            exists += 1
        else:
            failed.append({"id": id_, "dst": str(dst), "src": str(src), "error": status})

    summary = {
        "created": created,
        "updated": updated,
        "exists": exists,
        "missing_map": missing_map,
        "missing_source_file": missing_src,
        "failed": len(failed),
        "title_dir": str(TITLE_DIR),
        "canonical_dir": str(CANON_DIR),
    }

    print("Title view summary:", json.dumps(summary, indent=2))
    if failed:
        print("First 5 failures:", json.dumps(failed[:5], indent=2))


if __name__ == "__main__":
    main()
