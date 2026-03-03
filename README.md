# O-RAN Production Ingestion Pipeline

Production-grade ingestion system for downloading, validating, extracting, and cataloging O-RAN specifications from the official portal.

This system is:

- Deterministic
- Idempotent
- Integrity-verified
- Lockfile-enforced
- Reproducible from control plane only

---

# 1. Design Goals

This pipeline was engineered to guarantee:

- Deterministic artifact naming
- Reproducible rebuilds
- Zero filename drift
- Safe recursive ZIP extraction
- Full integrity validation
- Control-plane / data-plane separation
- Idempotent re-runs
- Audit-ready reporting

The data plane can always be rebuilt from the control plane.

---

# 2. High-Level Architecture

```text
Portal Listing
  ↓
manifests/raw/manifest.json
  ↓
01_normalize_manifest.py
  ↓
manifests/processed/normalized_manifest.json
  ↓
02_build_inventory.py
  ↓
inventory/download_inventory.full.json
  ↓
09_full_run_pipeline_v2.py
  ↓
downloads/
  ↓
ZIP extraction
  ↓
extracted_flat/ + extracted_docs/
  ↓
10_generate_catalog_from_inventory.py
  ↓
inventory/catalog.latest.json + .csv
  ↓
12_create_title_view.py
  ↓
downloads_by_title/
```

---

# 3. Repository Structure

```
manifests/
├── raw/                 # Browser-extracted manifest (control plane)
└── processed/           # Normalized structured metadata (generated)

inventory/
├── id_filename_map.json         # Deterministic lockfile (ID → filename)
├── download_inventory.full.json # Generated
└── catalog.latest.json/.csv     # Generated

downloads/               # Canonical artifacts (source of truth)
extracted_flat/          # Full ZIP extraction view
extracted_docs/          # Docs-only extracted view
downloads_by_title/      # Human-readable symlink view
reports/                 # Execution reports + summaries

scripts/
├── 01_normalize_manifest.py
├── 02_build_inventory.py
├── 09_full_run_pipeline_v2.py
├── 10_generate_catalog_from_inventory.py
├── 12_create_title_view.py
└── tools/               # Utility helpers
```

---

# 4. Control Plane vs Data Plane

## Control Plane (Committed to Git)

- Raw manifest
- Deterministic lockfile (`id_filename_map.json`)
- Pipeline scripts
- Documentation
- Requirements

## Data Plane (Generated Artifacts)

- `downloads/`
- `extracted_*` views
- `reports/`
- `catalog.latest.*`

The data plane can always be regenerated from the control plane.

This separation prevents drift and ensures reproducibility.

---

# 5. Fresh Setup (New VM)

## 5.1 Install System Dependencies

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git poppler-utils unzip jq ripgrep
```

Required for:

- `pdfinfo` validation
- `unzip -t` validation
- JSON verification
- Fast debugging

---

## 5.2 Clone Repository

```bash
git clone git@github.com:CDECatapult/oran-prod-ingestion.git
cd oran-prod-ingestion
```

---

## 5.3 Setup Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

---

# 6. Run Full Pipeline

```bash
python scripts/01_normalize_manifest.py
python scripts/02_build_inventory.py
python scripts/09_full_run_pipeline_v2.py
python scripts/10_generate_catalog_from_inventory.py
python scripts/12_create_title_view.py
```

---

# 7. Idempotency Model

Safe to re-run at any time:

```bash
python scripts/09_full_run_pipeline_v2.py
```

Behavior:

- Existing valid files are skipped
- Corrupted files are re-downloaded
- Partial `.part` files are cleaned
- Lockfile ensures stable filenames

---

# 8. Validation & Health Checks

## 8.1 Execution Summary

```bash
jq '.summary' reports/full_run_report.json
```

Healthy state:

- downloaded_ok: 162
- failed: 0
- downloaded_but_invalid: 0
- remaining_nested_zips_in_extracted_flat: 0

---

## 8.2 Data Plane Checks

```bash
find downloads -type f | wc -l
find downloads -type f -name "*.part" | wc -l
find extracted_flat -type f -name "*.zip" | wc -l
```

Expected:

```
162
0
0
```

---

# 9. Lockfile Model

File:

```
inventory/id_filename_map.json
```

Purpose:

- Maps `portal_id → official_filename`
- Derived from HTTP `Content-Disposition`
- Prevents filename drift
- Prevents whitespace changes
- Prevents extension case drift
- Ensures catalog stability

All major pipeline steps depend on this lockfile.

---

# 10. Security Characteristics

The system implements:

- Atomic downloads (`.part → rename`)
- Retry with exponential backoff
- Zip-slip protection
- Compression ratio limits
- Maximum file size guardrails
- Recursive extraction safety
- SHA256 integrity hashing
- Deterministic filename enforcement

This prevents:

- Path traversal attacks
- Zip bombs
- Partial file corruption
- Filename drift
- Silent corruption

---

# 11. Drift Protection Model

Drift risks mitigated:

| Drift Type | Mitigation |
|------------|------------|
| Filename drift | Lockfile enforcement |
| Extension case drift | Canonical rename |
| Partial downloads | Atomic rename |
| Nested zip recursion | Controlled recursive extraction |
| Catalog mismatch | Regenerated from inventory |
| Missing artifact | Idempotent re-run |

---

# 12. Operational Guarantees

- Reproducible on fresh VM
- Fully rebuildable from Git
- Zero manual renaming
- Zero manual catalog editing
- Deterministic outputs
- Safe to run in CI

---

# 13. CI/CD Integration (Recommended)

In CI:

```bash
python scripts/01_normalize_manifest.py
python scripts/02_build_inventory.py
python scripts/09_full_run_pipeline_v2.py
jq '.summary.failed == 0' reports/full_run_report.json
```

Fail pipeline if:

- Any failed downloads
- Any invalid artifacts
- Nested ZIP remains

---

# 14. Recovery Model

If corruption is detected:

1. Delete corrupted file from `downloads/`
2. Re-run:

```bash
python scripts/09_full_run_pipeline_v2.py
```

Idempotency ensures only missing/invalid files are fetched.

---

# 15. Version

Current version: `v1.0`

Includes:

- 162 validated artifacts
- Deterministic lockfile
- Recursive extraction
- SHA256 catalog
- Idempotent pipeline model
- Security hardened extraction

---

# 16. Engineering Standard

This repository follows:

- Deterministic artifact management
- Control-plane / data-plane separation
- Infrastructure-style reproducibility
- Git-driven rebuild model
- Production-grade validation standards

---


