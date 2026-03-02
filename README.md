# O-RAN Production Ingestion Pipeline

This repository provides a production-grade ingestion pipeline for O-RAN specifications from the official portal.

It performs:

- Portal link collection (browser extraction)
- Metadata normalization (title, doc_code, release, date, type)
- Controlled download with idempotency
- Integrity validation (PDF + ZIP containers)
- Safe ZIP extraction (recursive, zip-slip protected)
- SHA256 checksum generation
- Authoritative ID → filename mapping (lockfile)
- Human-readable catalog generation (JSON + CSV)
- Title-based symlink view generation

---

## Architecture

Portal Listing  
→ Raw Manifest  
→ Normalized Manifest (structured metadata)  
→ Inventory (control plane)  
→ Downloader + Validator (data plane)  
→ downloads/  
→ ZIP extraction (recursive + safe)  
→ extracted_flat/ + extracted_docs/  
→ Catalog (JSON + CSV)  
→ Title-based symlink view  

---

## Repository Model

### 🔐 Control Plane (Committed to Git)

These define deterministic behavior:

- `manifests/raw/manifest.json`
- `inventory/id_filename_map.json`
- `scripts/`
- Documentation

### 📦 Data Plane (Generated – Not Committed)

These are reproducible artifacts:

- `downloads/`
- `extracted_flat/`
- `extracted_docs/`
- `downloads_by_title/`
- `reports/`
- `inventory/download_inventory.full.json`
- `manifests/processed/normalized_manifest.json`

---

## Directory Structure


manifests/raw/ # Browser-extracted manifest
manifests/processed/ # Normalized manifest (generated)
inventory/ # Inventory + mapping + catalog
downloads/ # Downloaded artifacts (source of truth)
extracted_flat/ # Full ZIP extraction view
extracted_docs/ # Docs-only extracted view
downloads_by_title/ # Symlink view (human-friendly)
reports/ # Run reports
scripts/ # Core pipeline scripts
scripts/tools/ # Utility / helper scripts


---

## Key Artifacts

### 1️⃣ Downloads (Data Plane)

`downloads/`

Contains official files exactly as delivered by the O-RAN portal.

This is the canonical source of truth for artifacts.

---

### 2️⃣ Inventory (Control Plane)

`inventory/download_inventory.full.json`

Structured metadata and execution control.

Generated from normalized manifest.

---

### 3️⃣ ID → Filename Lockfile

`inventory/id_filename_map.json`

Authoritative mapping:


portal_id → official_filename


Generated earlier by reading HTTP `Content-Disposition` headers from the O-RAN portal.

This file ensures:
- Deterministic filenames
- Idempotent re-runs
- Catalog consistency
- No whitespace or extension drift

Treated as a lockfile.

---

### 4️⃣ Extracted Views

#### extracted_flat/

Full extraction of all ZIP artifacts.

- Nested ZIPs recursively extracted
- Nested ZIP files removed
- Zip-slip protected

Invariant:

find extracted_flat -name "*.zip"

Should return zero.

---

#### extracted_docs/

Only `.pdf`, `.docx`, `.xlsx` copied from ZIP contents.

Used for document-only browsing.

---

### 5️⃣ Final Catalog (For Team Use)

#### JSON View

`inventory/catalog.latest.json`

Includes:
- id
- display_title
- doc_code
- release
- month_year
- filename
- bytes
- sha256

#### CSV View

`inventory/catalog.latest.csv`

Human-readable index of all specifications.

---

## Fresh VM Setup

### Install system dependencies


sudo apt update
sudo apt install -y python3 python3-venv python3-pip git poppler-utils unzip


Required:
- `pdfinfo` (PDF validation)
- `unzip` (ZIP/DOCX/XLSX validation)

---

### Setup Python environment


python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


---

## Running the Pipeline


python scripts/01_normalize_manifest.py
python scripts/02_build_inventory.py
python scripts/09_full_run_pipeline_v2.py
python scripts/10_generate_catalog_from_inventory.py
python scripts/12_create_title_view.py


---

## Re-running the Pipeline

Idempotent and safe to re-run:


python scripts/09_full_run_pipeline_v2.py


Already downloaded files are skipped.

---

## Integrity Verification

### Container validation


python scripts/tools/11_verify_integrity_sweep.py


Verifies:
- PDF structure
- ZIP container validity
- No corrupted files

### Basic health checks


find downloads -type f | wc -l
find downloads -type f -name ".part"
find extracted_flat -type f -name ".zip"


Expected:
- 162 files
- No `.part` files
- No nested ZIPs remaining

---

## Security Characteristics

- Atomic downloads (.part → rename)
- Retry + exponential backoff
- Zip-slip protection
- Compression ratio limits
- Max file size limits
- SHA256 hashing

---

## Version

Current tagged version:

`v1.0`

Includes:
- 162 portal artifacts
- Deterministic filename lockfile
- Recursive ZIP extraction
- Full integrity validation
- SHA256 checksums
- Catalog export

---

## Production Status

This ingestion pipeline is:

- Deterministic
- Idempotent
- Integrity validated
- Zip-slip protected
- CI guarded
- Operationally documented
