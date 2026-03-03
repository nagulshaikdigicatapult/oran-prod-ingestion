# O-RAN Production Ingestion Pipeline – Operational Runbook

This document describes how to operate, validate, and troubleshoot the ingestion pipeline.

---

## 1. Purpose

This runbook explains:

- How to execute the pipeline
- How to validate artifacts
- How to verify integrity
- How to regenerate catalogs
- How to recover from corruption

---

## 2. Execution Flow

```text
Portal
  ↓
Raw Manifest
  ↓
Normalize
  ↓
Inventory
  ↓
Download + Validate
  ↓
Extract ZIPs
  ↓
Generate Catalog
  ↓
Create Title View
3. Environment Setup (Fresh VM)
3.1 Install System Dependencies
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git poppler-utils unzip

Required for:

pdfinfo (PDF validation)

unzip -t (ZIP/DOCX/XLSX validation)

3.2 Create Virtual Environment
python3 -m venv .venv
source .venv/bin/activate

Verify environment:

python -V
pip -V
3.3 Install Minimal Python Dependencies (Pin-Friendly)

We start lean:

requests → HTTP downloads

python-dateutil → timestamp handling

rich → readable CLI logs

pip install --upgrade pip
pip install requests python-dateutil rich

Verify installed versions:

pip freeze | egrep '^(pip|requests|python-dateutil|rich)=' || true
3.4 Lock Dependencies for Reproducible Builds
pip freeze > requirements.txt
wc -l requirements.txt
head -n 20 requirements.txt

This ensures reproducible installations on other systems.

4. Step-by-Step Execution
Step 1 – Normalize Manifest
python scripts/01_normalize_manifest.py

Verify:

jq '.[0]' manifests/processed/normalized_manifest.json
Step 2 – Build Inventory
python scripts/02_build_inventory.py

Verify:

jq '.items | length' inventory/download_inventory.full.json

Expected:

162
Step 3 – Execute Download Pipeline
python scripts/09_full_run_pipeline_v2.py

Check result:

jq '.summary' reports/full_run_report.json

Expected:

downloaded_ok: 162

failed: 0

downloaded_but_invalid: 0

5. Validation Checks
Total Files
find downloads -type f | wc -l

Expected:

162
No Partial Files
find downloads -type f -name "*.part"

Expected: no output

Validate PDFs
find downloads -name "*.pdf" -print0 | \
xargs -0 -I{} pdfinfo "{}" > /dev/null

Expected: no errors

Validate ZIP / DOCX / XLSX Containers
find downloads \( -name "*.docx" -o -name "*.xlsx" -o -name "*.zip" \) -print0 | \
xargs -0 -I{} unzip -t "{}" > /dev/null

Expected: no errors

Ensure No Nested ZIPs Remain
find extracted_flat -name "*.zip"

Expected: no output

6. Generate Catalog
python scripts/10_generate_catalog_from_inventory.py

Verify:

jq '.count' inventory/catalog.latest.json

Expected:

162
7. Create Title View
python scripts/12_create_title_view.py

Verify symlink:

readlink downloads_by_title/<file>
8. Recovery Procedure

If corruption is detected:

Delete corrupted file from downloads/

Re-run:

python scripts/09_full_run_pipeline_v2.py

Only missing files will re-download.

9. Health Checklist

System is healthy if:

162 files exist

No .part files

No nested ZIPs

All PDF validations pass

All ZIP container checks pass

Catalog count = 162

CI passes

10. Data Classification

Source: O-RAN public portal

Formats: PDF, DOCX, XLSX, ZIP

Integrity: SHA256 verified

Reproducible: Yes

CI Guarded: Yes
