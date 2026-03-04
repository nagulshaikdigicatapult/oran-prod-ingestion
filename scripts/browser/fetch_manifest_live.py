#!/usr/bin/env python3
"""
Fetch the live O-RAN portal manifest using Playwright.

Output: manifests/raw/manifest.live.json
Schema: [{id, download_url, row_text, display_title}]
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from playwright.sync_api import sync_playwright

OUT = Path("manifests/raw/manifest.live.json")
PORTAL_URL = "https://specifications.o-ran.org/specifications"


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(PORTAL_URL, wait_until="networkidle")

        # Scroll until stable
        seen: Dict[str, Dict] = {}
        stable_rounds = 0
        last_count = 0

        for _ in range(200):  # hard cap safety
            # Collect download links currently visible
            anchors = page.query_selector_all('a[href*="download?id="]')
            for a in anchors:
                href = a.get_attribute("href") or ""
                if "download?id=" not in href:
                    continue
                # Build absolute URL
                url = href if href.startswith("http") else f"https://specifications.o-ran.org{href}"
                # Extract id
                try:
                    id_ = url.split("download?id=")[1].split("&")[0].strip()
                except Exception:
                    continue
                if not id_:
                    continue

                # Try to capture row text by walking up to a row container
                row_text = None
                try:
                    row = a.evaluate_handle("el => el.closest('tr') || el.closest('div')")
                    row_text = row.evaluate("r => r ? r.innerText : null")
                except Exception:
                    row_text = None

                if id_ not in seen:
                    seen[id_] = {"id": str(id_), "download_url": url, "row_text": row_text}

            count = len(seen)
            if count == last_count:
                stable_rounds += 1
            else:
                stable_rounds = 0
            last_count = count

            if stable_rounds >= 5:
                break

            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(800)

        browser.close()

    out: List[Dict] = list(seen.values())
    out.sort(key=lambda x: int(x["id"]))

    OUT.write_text(json.dumps(out, indent=2) + "\n")
    print(f"Wrote {OUT} items={len(out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
