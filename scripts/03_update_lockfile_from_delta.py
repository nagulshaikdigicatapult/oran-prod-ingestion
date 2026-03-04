#!/usr/bin/env python3
"""
Append-only lockfile updater (Bandit-safe).

- Reads inventory/download_inventory.delta.json (expects {"items":[{id, download_url, ...}, ...]})
- For each item, fetches headers (HEAD then fallback to GET with Range) to extract filename
- Appends mapping[id] = filename into inventory/id_filename_map.json
- NEVER overwrites existing IDs
- Updates top-level "count"

Security hardening:
- Enforces HTTPS-only URLs
- Enforces hostname allowlist (prevents SSRF + unsafe schemes like file://)
- Uses urllib.request.urlopen with # nosec B310 AFTER strict URL validation
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Optional, Tuple

LOCKFILE_PATH = Path("inventory/id_filename_map.json")
DELTA_INV_PATH = Path("inventory/download_inventory.delta.json")

# Tunables (keep conservative to avoid portal blocks)
TIMEOUT_SECS = 30
SLEEP_BETWEEN_REQ_SECS = 0.4
USER_AGENT = "oran-prod-ingestion/lockfile-updater"

# ---- Security policy ----
ALLOWED_SCHEMES = {"https"}

# Default allowlist (EDIT this to your portal hosts)
_DEFAULT_ALLOWED_HOSTS = {
    # Replace these with actual hosts used in your download_url values
    # Example placeholders:
    "www.o-ran.org",
    "o-ran.org",
}

# Optional: override allowlist from env (comma-separated)
# e.g. export ORAN_ALLOWED_HOSTS="portal.example.com,downloads.example.com"
_env_hosts = os.getenv("ORAN_ALLOWED_HOSTS", "").strip()
if _env_hosts:
    ALLOWED_HOSTS = {h.strip() for h in _env_hosts.split(",") if h.strip()}
else:
    ALLOWED_HOSTS = _DEFAULT_ALLOWED_HOSTS

# Content-Disposition filename parsing
FILENAME_RE = re.compile(r'filename\*?=(?P<val>[^;]+)', re.IGNORECASE)


def _sanitize_filename(name: str) -> str:
    name = name.strip().strip('"').strip("'")
    # Defense in depth: remove path parts if any
    name = name.split("/")[-1].split("\\")[-1]
    return name


def _parse_content_disposition(cd: str) -> Optional[str]:
    """
    Supports:
      - filename="x"
      - filename*=UTF-8''x
    """
    if not cd:
        return None

    m = FILENAME_RE.search(cd)
    if not m:
        return None

    val = m.group("val").strip()

    # filename*=UTF-8''...
    if val.lower().startswith("utf-8''"):
        enc = val[7:]
        try:
            return _sanitize_filename(urllib.parse.unquote(enc))
        except Exception:
            return _sanitize_filename(enc)

    return _sanitize_filename(val)


def _validate_url(url: str) -> None:
    """
    Enforce strict URL policy:
      - https only
      - allowlisted hostname only
      - no empty hostname
    """
    parsed = urllib.parse.urlparse(url)

    if parsed.scheme not in ALLOWED_SCHEMES:
        raise ValueError(f"Blocked non-HTTPS URL: {url}")

    if not parsed.hostname:
        raise ValueError(f"URL missing hostname: {url}")

    # If allowlist is empty, treat as misconfiguration (fail closed)
    if not ALLOWED_HOSTS:
        raise ValueError("ALLOWED_HOSTS is empty - refusing to fetch any URLs")

    if parsed.hostname not in ALLOWED_HOSTS:
        raise ValueError(f"Blocked host not in allowlist: {parsed.hostname}")


def _request_headers(url: str, method: str = "HEAD") -> Dict[str, str]:
    """
    Fetch response headers safely.
    HEAD first. If server rejects, caller may fallback to GET Range.

    Bandit B310:
      urllib.request.urlopen is flagged because it can open file://, etc.
      We fully mitigate by enforcing https + allowlisted hostname in _validate_url().
    """
    _validate_url(url)

    req = urllib.request.Request(url, method=method)
    req.add_header("User-Agent", USER_AGENT)

    if method == "GET":
        # minimal transfer (avoid large downloads)
        req.add_header("Range", "bytes=0-0")

    # URL is validated (https + allowlisted host). Safe by design.
    with urllib.request.urlopen(req, timeout=TIMEOUT_SECS) as resp:  # nosec B310
        return {k: v for k, v in resp.headers.items()}


def _derive_filename(item: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (filename, reason_if_missing)
    """
    url = item.get("download_url")
    if not url:
        return None, "missing download_url"

    # Try HEAD first
    try:
        hdrs = _request_headers(url, method="HEAD")
        cd = hdrs.get("Content-Disposition") or hdrs.get("content-disposition")
        fn = _parse_content_disposition(cd or "")
        if fn:
            return fn, None
    except Exception as e:
        head_err = f"HEAD failed: {e}"
    else:
        head_err = "HEAD ok but no filename"

    # Fallback: GET Range
    try:
        hdrs = _request_headers(url, method="GET")
        cd = hdrs.get("Content-Disposition") or hdrs.get("content-disposition")
        fn = _parse_content_disposition(cd or "")
        if fn:
            return fn, None
        return None, f"{head_err}; GET(no filename)"
    except Exception as e:
        return None, f"{head_err}; GET failed: {e}"


def main() -> int:
    if not LOCKFILE_PATH.exists():
        print(f"ERROR: lockfile missing: {LOCKFILE_PATH}", file=sys.stderr)
        return 2
    if not DELTA_INV_PATH.exists():
        print(f"ERROR: delta inventory missing: {DELTA_INV_PATH}", file=sys.stderr)
        return 2

    lock = json.loads(LOCKFILE_PATH.read_text())
    mapping: Dict[str, str] = lock.get("mapping", {})
    if not isinstance(mapping, dict):
        print("ERROR: lockfile mapping is not a dict", file=sys.stderr)
        return 2

    delta = json.loads(DELTA_INV_PATH.read_text())
    items = delta.get("items", [])
    if not isinstance(items, list):
        print("ERROR: delta inventory 'items' is not a list", file=sys.stderr)
        return 2

    added = 0
    skipped_existing = 0
    failed = 0
    failures = []

    # Deterministic order by numeric id
    def _id_key(x: dict) -> int:
        return int(str(x.get("id", "0")))

    for item in sorted(items, key=_id_key):
        sid = str(item.get("id"))
        if not sid or sid == "None":
            failed += 1
            failures.append({"id": sid, "reason": "missing id"})
            continue

        if sid in mapping:
            skipped_existing += 1
            continue

        filename, reason = _derive_filename(item)
        if not filename:
            failed += 1
            failures.append({"id": sid, "reason": reason or "unknown"})
        else:
            mapping[sid] = filename
            added += 1

        time.sleep(SLEEP_BETWEEN_REQ_SECS)

    # Update count and write back
    lock["mapping"] = mapping
    lock["count"] = len(mapping)

    LOCKFILE_PATH.write_text(json.dumps(lock, indent=2) + "\n")

    print("lockfile_update_summary:")
    print(f"  added={added}")
    print(f"  skipped_existing={skipped_existing}")
    print(f"  failed={failed}")
    print(f"  new_lockfile_count={lock['count']}")

    if failures:
        Path("reports").mkdir(parents=True, exist_ok=True)
        Path("reports/lockfile_update_failures.json").write_text(
            json.dumps(failures, indent=2) + "\n"
        )
        print("  failures_report=reports/lockfile_update_failures.json")

    # Non-zero for CI if failures exist
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
