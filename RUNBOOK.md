# O-RAN Production Ingestion Pipeline – Operational Runbook

This runbook describes how to set up, execute, validate, and recover the O-RAN ingestion pipeline in a reproducible and production-grade manner.

---

# 1. Pipeline Flow

```
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
```

---

# 2. Fresh VM Setup

## 2.1 Install System Dependencies

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git poppler-utils unzip jq ripgrep
```

### Why These Packages

- `pdfinfo` (poppler-utils) → validates PDFs  
- `unzip -t` → validates ZIP/DOCX/XLSX containers  
- `jq` → JSON verification  
- `rg` (ripgrep) → fast debugging/search  

---

## 2.2 Clone Repository

```bash
git clone git@github.com:CDECatapult/oran-prod-ingestion.git
cd oran-prod-ingestion
```

---

## 2.3 Create and Activate Python Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -V
pip -V
```

---

## 2.4 Install Python Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

# 3. Run the Pipeline

## Step 1 — Normalize Manifest

```bash
python scripts/01_normalize_manifest.py
```

### Verify

```bash
jq '.[0]' manifests/processed/normalized_manifest.json
```

---

## Step 2 — Build Inventory

```bash
python scripts/02_build_inventory.py
```

### Verify Count

```bash
jq '.items | length' inventory/download_inventory.full.json
```

Expected:

```
162
```

---

## Step 3 — Download + Validate + Extract

```bash
python scripts/09_full_run_pipeline_v2.py
```

### Check Run Summary

```bash
jq '.summary' reports/full_run_report.json
```

Expected (Healthy Run):

- `downloaded_ok: 162` (fresh run)
- or `skipped: 162` (idempotent run)
- `failed: 0`
- `downloaded_but_invalid: 0`
- `remaining_nested_zips_in_extracted_flat: 0`

---

# 4. Validation & Proof Checks

---

## 4.1 Control Plane Checks

Inventory + Lockfile must match.

```bash
jq '.items | length' inventory/download_inventory.full.json
jq '.mapping | length' inventory/id_filename_map.json
```

Expected:

```
162
162
```

---

## 4.2 Data Plane Checks

### File Count

```bash
find downloads -type f | wc -l
```

Expected:

```
162
```

### No Partial Downloads

```bash
find downloads -type f -name "*.part" | wc -l
```

Expected:

```
0
```

---

## 4.3 Validate PDFs

```bash
find downloads -type f -name "*.pdf" -print0 | xargs -0 -I{} pdfinfo "{}" >/dev/null
echo "pdfinfo OK ✅"
```

---

## 4.4 Validate ZIP / DOCX / XLSX Containers

```bash
python - <<'PY'
import subprocess
from pathlib import Path

bad = []
for ext in (".zip", ".docx", ".xlsx"):
    for p in Path("downloads").glob(f"*{ext}"):
        r = subprocess.run(["unzip", "-t", str(p)],
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
        if r.returncode != 0:
            bad.append(str(p))

print("bad_zip_containers=", len(bad))
if bad:
    print("\n".join(bad[:20]))
PY
```

Expected:

```
bad_zip_containers= 0
```

---

## 4.5 ZIP Extraction Sanity

### No Nested ZIPs Remaining

```bash
find extracted_flat -type f -name "*.zip" | wc -l
find extracted_docs -type f -name "*.zip" | wc -l
```

Expected:

```
0
0
```

---

### Docs Parity Check

```bash
test "$(find extracted_flat -type f \( -iname "*.pdf" -o -iname "*.docx" -o -iname "*.xlsx" \) | wc -l)" \
  -eq "$(find extracted_docs -type f \( -iname "*.pdf" -o -iname "*.docx" -o -iname "*.xlsx" \) | wc -l)" \
  && echo "docs parity ✅"
```

---

# 5. Generate Catalog

```bash
python scripts/10_generate_catalog_from_inventory.py
```

### Verify

```bash
jq '.count' inventory/catalog.latest.json
```

Expected:

```
162
```

---

# 6. Create Title View

```bash
python scripts/12_create_title_view.py
ls -lah downloads_by_title | head -n 20
```

---

# 7. Recovery Procedure

If corruption is detected:

1. Delete the corrupted file from `downloads/`
2. Re-run the pipeline

```bash
python scripts/09_full_run_pipeline_v2.py
```

Idempotency ensures only missing or invalid files are re-downloaded.

---

# 8. Lockfile Design — inventory/id_filename_map.json

This file acts as a deterministic lockfile:

- Maps `portal_id → official_filename`
- Prevents filename drift
- Prevents extension case changes
- Ensures reproducibility

The following scripts depend on it:

- `09_full_run_pipeline_v2.py`
- `10_generate_catalog_from_inventory.py`
- `12_create_title_view.py`

---
