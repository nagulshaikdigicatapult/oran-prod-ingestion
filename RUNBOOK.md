O-RAN Production Ingestion Pipeline – Operational Runbook
1️⃣ Purpose

This runbook describes:

How to run the O-RAN ingestion pipeline

How to validate artifacts

How to troubleshoot issues

How to regenerate catalog indexes

How to verify integrity

How to safely transfer artifacts

2️⃣ System Overview (Flow)
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
downloads/ + reports/
   ↓
scripts/10_generate_catalog_from_inventory.py
   ↓
inventory/catalog.latest.csv + json
   ↓
scripts/12_create_title_view.py
   ↓
downloads_by_title/ (symlink view)
3️⃣ Environment Setup

Activate virtual environment:

source .venv/bin/activate

Install dependencies (if fresh system):

pip install -r requirements.txt

System packages required:

sudo apt install -y poppler-utils unzip

Why:

pdfinfo validates PDFs

unzip -t validates DOCX/XLSX/ZIP

4️⃣ Step-by-Step Operational Execution
🔹 Step 1 – Normalize Manifest

Parses portal row_text into structured metadata.

python scripts/01_normalize_manifest.py

Validate output:

jq '.[0]' manifests/processed/normalized_manifest.json

Healthy result:

display_title populated

doc_code extracted

month_year extracted

🔹 Step 2 – Build Inventory

Builds control layer for idempotent downloads.

python scripts/02_build_inventory.py

Verify:

jq '.items | length' inventory/download_inventory.full.json

Expected:

162
🔹 Step 3 – Execute Download Pipeline

Main ingestion engine.

python scripts/09_full_run_pipeline_v2.py

Check summary:

jq '.summary' reports/full_run_report_v2.json

Healthy output example:

{
  "downloaded_ok": 162,
  "type::application/pdf": 90,
  "type::application/vnd.openxmlformats-officedocument.wordprocessingml.document": 61,
  "type::application/x-zip-compressed": 10,
  "type::application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": 1
}
5️⃣ Validation & Integrity Checks
🔹 Check Total File Count
find downloads -type f | wc -l

Expected:

162
🔹 Check Disk Usage
du -sh downloads/

Expected:
~400–450MB

🔹 Ensure No Partial Files
find downloads -type f -name "*.part"

Expected:
(no output)

🔹 Validate All PDFs
find downloads -type f -name "*.pdf" -print0 | \
xargs -0 -I{} pdfinfo "{}" > /dev/null

Expected:
(no errors)

🔹 Validate DOCX/XLSX/ZIP
find downloads -type f \( -name "*.docx" -o -name "*.xlsx" -o -name "*.zip" \) -print0 | \
xargs -0 -I{} unzip -t "{}" > /dev/null

Expected:
(no errors)

🔹 Integrity Sweep (Full Dataset)
python scripts/11_verify_integrity_sweep.py

Expected:

pdf_ok: 90
zip_ok: 72
pdf_fail: 0
zip_fail: 0
6️⃣ Generate Human-Friendly Catalog
🔹 Generate JSON + CSV Index
python scripts/10_generate_catalog_from_inventory.py

Verify:

jq '.count' inventory/catalog.latest.json

Expected:

162

Preview CSV:

column -s, -t < inventory/catalog.latest.csv | less -S
7️⃣ Create Title-Based View (Symlink Layer)

Creates readable filenames without modifying canonical files.

python scripts/12_create_title_view.py

Verify:

ls -lah downloads_by_title | head -n 20

Verify symlink target:

readlink downloads_by_title/<filename>

Expected:
Points to downloads/<official_filename>

8️⃣ Open Files in GUI (Server)

PDF:

xdg-open downloads/<file>.pdf

CSV:

libreoffice --calc inventory/catalog.latest.csv
9️⃣ Transfer Files to Local Machine

Recommended method:

rsync -avz --progress \
-e "ssh -J sonic@<jump-host>" \
terraform@<vm-ip>:/home/terraform/oran-prod-ingestion/downloads/ \
./downloads_local/

Verify locally:

find downloads_local -type f | wc -l
🔟 SHA256 Verification

Generate hash list (server):

jq -r '.items[] | "\(.sha256)  \(.filename)"' inventory/catalog.latest.json > server_hashes.txt

On local:

shasum -a 256 downloads_local/* > local_hashes.txt
diff server_hashes.txt local_hashes.txt

Expected:
(no differences)

CI Validation

CI automatically checks:

Python compilation

Ruff lint

Bandit security scan

pip-audit

Artifact guardrails

Manual local check:

ruff check .
bandit -q -r scripts -ll
pip-audit
Troubleshooting Guide
❌ CI fails on Ruff

Fix automatically:

ruff check . --fix
❌ PDF validation fails

Check file type:

file downloads/<file>

Re-download single item:

python scripts/09_full_run_pipeline_v2.py
❌ Missing files in catalog

Regenerate:

python scripts/10_generate_catalog_from_inventory.py
Health Checklist (Production Ready)

System is healthy if:

162 files exist

No .part files

All pdfinfo checks pass

All unzip tests pass

Catalog count = 162

CI passing

SHA256 matches

Recovery Procedure

If corruption detected:

Delete corrupted file

Re-run pipeline:

python scripts/09_full_run_pipeline_v2.py

Idempotency ensures only missing files are re-downloaded.

Versioning

Stable milestone:

git tag v1.0
git push --tags
Data Classification

Source: O-RAN public portal

Format: PDF, DOCX, XLSX, ZIP

Integrity: SHA256 verified

Reproducible: Yes

CI Guarded: Yes

Final Status

This ingestion system is:

Idempotent

Integrity verified

Reproducible

Catalog indexed

CI protected

Human readable

Operationally documented
