# Browser scripts (data provenance)

These scripts are run manually in a browser (DevTools Console) to collect source data
from the O-RAN specifications portal.

## collect_manifest_links.js

**Purpose:** Scrolls the listing page, collects unique `download?id=<id>` links, and downloads
a JSON file of `{ id, download_url }` records.

**How to run:**
1. Open the O-RAN specs listing page in a browser.
2. Open DevTools → Console.
3. Paste the script contents and run.
4. It will auto-scroll until stable, then download: `manifest_links_all.json`.

**Notes:**
- The portal may lazy-load rows; the script scrolls until the count stabilizes.
- The output JSON becomes input to the ingestion pipeline (stored under `manifests/raw/`).
