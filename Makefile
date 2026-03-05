# Makefile — ORAN ingestion pipeline (prod-ish guardrails)
# Usage:
#   make setup
#   make monitor
#   make ingest
#
# Notes:
# - monitor uses live portal snapshot and exits early (ORAN_SKIP_CATALOG=1)
# - ingest uses live portal snapshot and performs delta ingest if new IDs exist

PY ?= python
PIP ?= pip

LIVE_MANIFEST := manifests/raw/manifest.live.json

.PHONY: help setup guardrails fetch normalize build-inventory catalog portal-diff monitor ingest clean-live

help:
	@echo "Targets:"
	@echo "  make setup        - install Python deps (+ playwright browser deps)"
	@echo "  make monitor      - fetch live portal + normalize + diff/status only (no downloads)"
	@echo "  make ingest       - fetch live portal + normalize + build inventory + delta ingest"
	@echo "  make fetch        - fetch live portal manifest to $(LIVE_MANIFEST)"
	@echo "  make normalize    - normalize from live manifest"
	@echo "  make build-inventory - build inventory/download_inventory.full.json from normalized manifest"
	@echo "  make catalog      - regenerate inventory/catalog.latest.* from inventory + lockfile"
	@echo "  make guardrails   - CI guardrails locally"
	@echo "  make clean-live   - remove runtime live manifest"

setup:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	# optional but useful locally (CI already installs these)
	$(PIP) install ruff bandit pip-audit
	# needed for fetch_manifest_live.py
	$(PY) -m playwright install --with-deps chromium

guardrails:
	$(PY) scripts/tools/00_ci_guardrails.py
	ruff check .
	bandit -q -r scripts -ll

fetch:
	$(PY) scripts/browser/fetch_manifest_live.py
	@test -f $(LIVE_MANIFEST)
	@echo "OK: wrote $(LIVE_MANIFEST)"

normalize: fetch
	$(PY) scripts/01_normalize_manifest.py --in $(LIVE_MANIFEST)

build-inventory: normalize
	$(PY) scripts/02_build_inventory.py

catalog:
	$(PY) scripts/10_generate_catalog_from_inventory.py

portal-diff: fetch
	# CI-safe mode: write diff/status/delta then exit (no downloads/catalog)
	ORAN_SKIP_CATALOG=1 $(PY) scripts/update_from_portal.py $(LIVE_MANIFEST)

monitor: guardrails portal-diff
	@echo "DONE: monitor artifacts updated:"
	@echo "  - $(LIVE_MANIFEST)"
	@echo "  - manifests/processed/normalized_manifest.json"
	@echo "  - reports/portal_diff.latest.json"
	@echo "  - reports/portal_status.latest.json"

ingest: guardrails build-inventory
	# Full mode: if new IDs exist -> update lockfile -> download delta -> regenerate catalog
	$(PY) scripts/update_from_portal.py $(LIVE_MANIFEST)
	@echo "DONE: ingest finished. Check:"
	@echo "  - downloads/ (new files if any)"
	@echo "  - inventory/id_filename_map.json (append-only lockfile)"
	@echo "  - inventory/catalog.latest.*"

clean-live:
	rm -f $(LIVE_MANIFEST)
