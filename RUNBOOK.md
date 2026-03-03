O-RAN Production Ingestion Pipeline – Operational Runbook

This runbook explains how to set up, execute, validate, and recover the O-RAN ingestion pipeline in a reproducible way.

1. Pipeline Flow
Portal
  ↓
manifests/raw/manifest.json
  ↓
scripts/01_normalize_manifest.py
  ↓
manifests/processed/normalized_manifest.json
  ↓
scripts/02_build_inventory.py
  ↓
inventory/download_inventory.full.json
  ↓
scripts/09_full_run_pipeline_v2.py
  ↓
downloads/ + extracted_flat/ + extracted_docs/ + reports/full_run_report.json
  ↓
scripts/10_generate_catalog_from_inventory.py
  ↓
inventory/catalog.latest.json + inventory/catalog.latest.csv
  ↓
scripts/12_create_title_view.py
  ↓
downloads_by_title/ (symlink view)
2. Fresh VM Setup
2.1 Install system dependencies
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git poppler-utils unzip jq ripgrep

Why:

pdfinfo (from poppler-utils) validates PDFs

unzip -t validates ZIP/DOCX/XLSX containers

jq is used for JSON checks

rg helps debug/search quickly

2.2 Clone repo
git clone git@github.com:CDECatapult/oran-prod-ingestion.git
cd oran-prod-ingestion
2.3 Create and activate Python venv
python3 -m venv .venv
source .venv/bin/activate
python -V
pip -V
2.4 Install Python dependencies
pip install --upgrade pip
pip install -r requirements.txt
3. Run the Pipeline
Step 1 — Normalize manifest
python scripts/01_normalize_manifest.py

Verify:

jq '.[0]' manifests/processed/normalized_manifest.json
Step 2 — Build inventory
python scripts/02_build_inventory.py

Verify counts:

jq '.items|length' inventory/download_inventory.full.json

Expected: 162

Step 3 — Download + validate + extract
python scripts/09_full_run_pipeline_v2.py

Check run summary:

jq '.summary' reports/full_run_report.json

Expected (healthy):

downloaded_ok: 162 on a fresh run, or skipped: 162 if already downloaded

failed: 0

downloaded_but_invalid: 0

remaining_nested_zips_in_extracted_flat: 0

4. Validation and Proof Checks
4.1 Control plane checks

Inventory + lockfile exist and match:

jq '.items|length' inventory/download_inventory.full.json
jq '.mapping|length' inventory/id_filename_map.json

Expected:

162

162

4.2 Data plane checks

Files count and no partial downloads:

find downloads -type f | wc -l
find downloads -type f -name "*.part" | wc -l

Expected:

162

0

4.3 Validate PDFs
find downloads -type f -name "*.pdf" -print0 | xargs -0 -I{} pdfinfo "{}" >/dev/null
echo "pdfinfo OK ✅"
4.4 Validate ZIP/DOCX/XLSX containers
python - <<'PY'
import subprocess
from pathlib import Path

bad=[]
for ext in (".zip",".docx",".xlsx"):
    for p in Path("downloads").glob(f"*{ext}"):
        r=subprocess.run(["unzip","-t",str(p)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if r.returncode!=0:
            bad.append(str(p))
print("bad_zip_containers=", len(bad))
if bad:
    print("\n".join(bad[:20]))
PY

Expected: bad_zip_containers= 0

4.5 ZIP extraction sanity

No nested zips remaining in extracted views:

find extracted_flat -type f -name "*.zip" | wc -l
find extracted_docs -type f -name "*.zip" | wc -l

Expected:

0

0

Docs parity (docs in flat == docs in docs view):

test "$(find extracted_flat -type f \( -iname "*.pdf" -o -iname "*.docx" -o -iname "*.xlsx" \) | wc -l)" \
  -eq "$(find extracted_docs -type f \( -iname "*.pdf" -o -iname "*.docx" -o -iname "*.xlsx" \) | wc -l)" \
  && echo "docs parity ✅"
5. Generate Catalog
python scripts/10_generate_catalog_from_inventory.py

Verify:

jq '.count' inventory/catalog.latest.json

Expected: 162

6. Create Title View
python scripts/12_create_title_view.py
ls -lah downloads_by_title | head -n 20
7. Recovery

If corruption is detected:

Delete the corrupted file from downloads/

Re-run the pipeline:

python scripts/09_full_run_pipeline_v2.py

Idempotency ensures only missing files are re-downloaded.

8. Notes on inventory/id_filename_map.json (Lockfile)

This file is the “lockfile” mapping: portal_id → official_filename.

It keeps filenames deterministic and prevents drift (whitespace / extension case changes).

09_full_run_pipeline_v2.py, 10_generate_catalog_from_inventory.py, and 12_create_title_view.py all use it.
