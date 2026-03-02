# O-RAN Production Ingestion Pipeline

Production-grade ingestion system for downloading, validating, extracting, and cataloging O-RAN specifications from the official portal.

This pipeline is deterministic, idempotent, and integrity-verified.

---

## Overview

The system performs:

- Portal manifest ingestion
- Metadata normalization (title, doc_code, release, date, type)
- Controlled idempotent downloads
- PDF validation (`pdfinfo`)
- ZIP/DOCX/XLSX validation (`unzip -t`)
- Recursive ZIP extraction (zip-slip protected)
- SHA256 checksum generation
- Deterministic ID → filename lockfile enforcement
- JSON + CSV catalog generation
- Title-based symlink view creation

---

## Architecture


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


---

## Repository Structure

```text
manifests/
├── raw/                 # Browser-extracted manifest
└── processed/           # Normalized structured metadata

inventory/
├── id_filename_map.json # Deterministic lockfile (ID → filename)
├── download_inventory.full.json (generated)
└── catalog.latest.json/.csv (generated)

downloads/               # Canonical artifacts (source of truth)
extracted_flat/          # Full ZIP extraction view
extracted_docs/          # Docs-only extracted view
downloads_by_title/      # Human-readable symlink view
reports/                 # Execution reports

scripts/
├── 01_normalize_manifest.py
├── 02_build_inventory.py
├── 09_full_run_pipeline_v2.py
├── 10_generate_catalog_from_inventory.py
├── 12_create_title_view.py
└── tools/               # Helper / utility scripts
Control Plane vs Data Plane
Control Plane (Committed)

Raw manifest

ID → filename lockfile

Pipeline scripts

Documentation

Data Plane (Generated)

downloads/

extracted views

reports/

catalog snapshots

The system can fully rebuild the data plane from the control plane.

Fresh Setup (New VM)
1. Install system dependencies
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git poppler-utils unzip
2. Setup virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
Run Full Pipeline
python scripts/01_normalize_manifest.py
python scripts/02_build_inventory.py
python scripts/09_full_run_pipeline_v2.py
python scripts/10_generate_catalog_from_inventory.py
python scripts/12_create_title_view.py
Idempotency

Safe to re-run anytime:

python scripts/09_full_run_pipeline_v2.py

Already downloaded files are skipped.

Validation

Check summary:

jq '.summary' reports/full_run_report.json

Basic health checks:

find downloads -type f | wc -l
find downloads -type f -name "*.part"
find extracted_flat -type f -name "*.zip"

Expected:

162 files

0 partial files

0 nested ZIPs

Lockfile Explanation

inventory/id_filename_map.json

Maps:

portal_id → official_filename

Generated from HTTP Content-Disposition headers.

Ensures:

Deterministic filenames

No whitespace drift

No extension case drift

Stable catalog generation

Security Characteristics

Atomic downloads (.part → rename)

Retry with exponential backoff

Zip-slip protection

Compression ratio limits

Max file size limits

SHA256 integrity hashing

Version

Tagged version: v1.0

Includes:

162 validated artifacts

Deterministic lockfile

Recursive extraction

SHA256 catalog

CI validation
