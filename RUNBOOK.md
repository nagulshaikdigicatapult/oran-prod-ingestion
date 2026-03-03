# O-RAN Production Ingestion Pipeline – Operational Runbook

This runbook describes how to set up, execute, validate, and recover the ingestion pipeline in a reproducible and secure manner.

---

# 1. Fresh VM Setup

## 1.1 Install System Dependencies

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git poppler-utils unzip

Required tools:

pdfinfo → PDF validation

unzip -t → ZIP/DOCX/XLSX validation

1.2 Create Virtual Environment
python3 -m venv .venv
source .venv/bin/activate

Verify environment:

python -V
pip -V
1.3 Install Minimal Python Dependencies
pip install --upgrade pip
pip install requests python-dateutil rich

Verify installed versions:

pip freeze | egrep '^(pip|requests|python-dateutil|rich)=' || true
1.4 Lock Dependencies (Reproducible Build)
pip freeze > requirements.txt
wc -l requirements.txt
head -n 20 requirements.txt
2. Execute Pipeline
2.1 Normalize Manifest
python scripts/01_normalize_manifest.py

Verify:

jq '.[0]' manifests/processed/normalized_manifest.json
2.2 Build Inventory
python scripts/02_build_inventory.py

Verify:

jq '.items | length' inventory/download_inventory.full.json

Expected output:

162
2.3 Download, Validate, and Extract
python scripts/09_full_run_pipeline_v2.py

Check summary:

jq '.summary' reports/full_run_report.json

Expected:

downloaded_ok: 162

failed: 0

downloaded_but_invalid: 0

3. Validation Checks
3.1 Verify Total File Count
find downloads -type f | wc -l

Expected:

162
3.2 Ensure No Partial Files
find downloads -type f -name "*.part"

Expected: no output

3.3 Validate All PDFs
find downloads -name "*.pdf" -print0 | \
xargs -0 -I{} pdfinfo "{}" > /dev/null

Expected: no errors

3.4 Validate ZIP / DOCX / XLSX Containers
find downloads \( -name "*.docx" -o -name "*.xlsx" -o -name "*.zip" \) -print0 | \
xargs -0 -I{} unzip -t "{}" > /dev/null

Expected: no errors

3.5 Ensure No Nested ZIPs Remain
find extracted_flat -name "*.zip"

Expected: no output

4. Generate Catalog
python scripts/10_generate_catalog_from_inventory.py

Verify:

jq '.count' inventory/catalog.latest.json

Expected:

162
5. Create Human-Readable Title View
python scripts/12_create_title_view.py

Verify symlink:

readlink downloads_by_title/<file>
6. Recovery Procedure

If a file is corrupted:

Delete the corrupted file from downloads/

Re-run:

python scripts/09_full_run_pipeline_v2.py

Only missing files will re-download.

7. Health Checklist

System is healthy if:

162 files exist

No .part files

No nested ZIPs

All PDF validations pass

All ZIP container checks pass

Catalog count = 162

CI pipeline passes

8. Data Classification

Source: O-RAN public portal

Formats: PDF, DOCX, XLSX, ZIP

Integrity: SHA256 verified

Reproducible: Yes

CI Guarded: Yes
