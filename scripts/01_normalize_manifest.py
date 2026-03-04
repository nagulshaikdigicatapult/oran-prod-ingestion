#!/usr/bin/env python3
import json
import re
from datetime import datetime
from pathlib import Path

RAW = Path("manifests/raw/manifest.latest.json")
OUT = Path("manifests/processed/normalized_manifest.json")

MONTHS = r"(January|February|March|April|May|June|July|August|September|October|November|December)"


def parse_row_text(row_text: str) -> dict:
    """
    Parse portal row_text into structured metadata.

    Example:
      "O-RAN Use Cases Analysis Report 19.0 O-RAN.WG1.TR.Use-Cases-Analysis-Report-R005-v19.00 February 2026 Technical Report R005 DOWNLOAD"
    """
    if not row_text:
        return {
            "display_title": None,
            "doc_code": None,
            "month_year": None,
            "doc_kind": None,
            "release": None,
        }

    s = " ".join(str(row_text).split())
    s = re.sub(r"\bDOWNLOAD\b", "", s).strip()

    # doc_code usually begins with O-RAN. and ends with -vXX.XX
    doc_code = None
    m = re.search(r"\b(O-RAN\.[A-Za-z0-9\.\-]+-v\d+\.\d+)\b", s)
    if m:
        doc_code = m.group(1)

    # month_year e.g. "February 2026"
    month_year = None
    m = re.search(rf"\b{MONTHS}\s+\d{{4}}\b", s)
    if m:
        month_year = m.group(0)

    # release e.g. R005
    release = None
    m = re.search(r"\bR\d{3}\b", s)
    if m:
        release = m.group(0)

    # doc kind (best effort)
    doc_kind = None
    for kind in ["Technical Specification", "Technical Report", "White Paper"]:
        if kind in s:
            doc_kind = kind
            break

    # display_title is usually text before doc_code
    display_title = None
    if doc_code and doc_code in s:
        display_title = s.split(doc_code, 1)[0].strip()
    else:
        # fallback: take up to month_year if present
        tmp = s
        if month_year and month_year in tmp:
            tmp = tmp.split(month_year, 1)[0].strip()
        if release:
            tmp = re.sub(rf"\b{re.escape(release)}\b", "", tmp).strip()
        display_title = tmp or None

    if display_title:
        display_title = re.sub(r"\s{2,}", " ", display_title).strip()
        if doc_code and display_title == doc_code:
            display_title = None

    return {
        "display_title": display_title,
        "doc_code": doc_code,
        "month_year": month_year,
        "doc_kind": doc_kind,
        "release": release,
    }


def main():
    data = json.loads(RAW.read_text())
    if not isinstance(data, list):
        raise SystemExit("Raw manifest must be a JSON array")

    ingested_at = datetime.utcnow().isoformat() + "Z"

    out = []
    for item in data:
        id_ = str(item.get("id", "")).strip()
        url = str(item.get("download_url", "")).strip()
        row_text = item.get("row_text", "")

        meta = parse_row_text(row_text)

        out.append(
            {
                "id": id_,
                "download_url": url,
                "row_text": row_text,
                **meta,
                "ingested_at": ingested_at,
            }
        )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2))

    print(f"Normalized manifest written to {OUT}")
    print(f"Total entries: {len(out)}")


if __name__ == "__main__":
    main()
