# O-RAN Production Ingestion Pipeline

This repository provides a production-grade ingestion pipeline for O-RAN specifications from the official portal.

It performs:

- Portal link collection (browser extraction)
- Metadata normalization (title, doc_code, release, date, type)
- Controlled download with idempotency
- Integrity validation (PDF + ZIP containers)
- SHA256 checksum generation
- Authoritative ID → filename mapping
- Human-readable catalog generation (JSON + CSV)

---

## Architecture

Portal Listing  
→ Raw Manifest  
→ Normalized Manifest (structured metadata)  
→ Inventory (control plane)  
→ Downloader (data plane)  
→ downloads/  
→ Catalog (JSON + CSV)

---

## Directory Structure


manifests/raw/ # Browser-extracted manifest
manifests/processed/ # Normalized manifest with structured metadata
inventory/ # Inventory + mapping + catalog
downloads/ # Downloaded artifacts (source of truth)
reports/ # Run reports
scripts/ # All pipeline automation scripts


---

## Key Artifacts

### 1. Downloads (Data Plane)

downloads/

Contains official files exactly as delivered by the O-RAN portal.

---

### 2. Inventory (Control Plane)

inventory/download_inventory.full.json

Structured metadata and execution control.

---

### 3. ID → Filename Map

inventory/id_filename_map.json

Authoritative mapping extracted from HTTP headers.

---

### 4. Final Catalog (For Team Use)

#### JSON View

inventory/catalog.latest.json


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

inventory/catalog.latest.csv


Human-readable index of all specifications.

---

## Re-running the Pipeline

Idempotent. Safe to re-run.


python scripts/09_full_run_pipeline_v2.py


Already downloaded files are skipped.

---

## Integrity Verification


python scripts/11_verify_integrity_sweep.py


Verifies:
- PDF structure
- ZIP container validity
- No corrupted files

---

## Version

Current tagged version:


v1.0


Includes:
- 162 files
- Portal title metadata
- Full integrity validation
- SHA256 checksums
- Catalog export

