#!/usr/bin/env python3
import csv
import json
from pathlib import Path

INV = Path("inventory/download_inventory.full.json")
MAP = Path("inventory/id_filename_map.json")
DL = Path("downloads")

NORM = Path("manifests/processed/normalized_manifest.json")

OUT_JSON = Path("inventory/catalog.latest.json")
OUT_CSV = Path("inventory/catalog.latest.csv")


def sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_meta_by_id() -> dict[str, dict]:
    """
    Best-effort metadata enrichment from normalized portal manifest.
    """
    if not NORM.exists():
        return {}

    data = json.loads(NORM.read_text())
    if not isinstance(data, list):
        return {}

    meta_by_id: dict[str, dict] = {}
    for rec in data:
        rid = str(rec.get("id", "")).strip()
        if not rid:
            continue
        meta_by_id[rid] = {
            "display_title": rec.get("display_title"),
            "doc_code": rec.get("doc_code"),
            "doc_kind": rec.get("doc_kind"),
            "release": rec.get("release"),
            "month_year": rec.get("month_year"),
        }
    return meta_by_id


def main() -> None:
    inv = json.loads(INV.read_text())
    items = inv.get("items", [])
    if not isinstance(items, list):
        raise SystemExit("inventory/download_inventory.full.json must contain items: []")

    mp = json.loads(MAP.read_text())
    id_to_filename = mp.get("mapping", {})
    if not isinstance(id_to_filename, dict):
        raise SystemExit("inventory/id_filename_map.json must contain mapping: {}")

    meta_by_id = load_meta_by_id()

    out_items: list[dict] = []
    missing_files: list[dict] = []

    for it in items:
        id_ = str(it.get("id", "")).strip()
        if not id_:
            continue

        filename = id_to_filename.get(id_)
        if not filename:
            missing_files.append({"id": id_, "reason": "missing_filename_in_map"})
            continue

        path = DL / filename
        if not path.exists():
            missing_files.append(
                {"id": id_, "filename": filename, "reason": "file_not_found_in_downloads"}
            )
            continue

        # Prefer inventory metadata if present; otherwise fallback to normalized manifest
        inv_meta = {
            "display_title": it.get("display_title"),
            "doc_code": it.get("doc_code"),
            "doc_kind": it.get("doc_kind"),
            "release": it.get("release"),
            "month_year": it.get("month_year"),
        }
        fb = meta_by_id.get(id_, {})
        meta = {k: (inv_meta.get(k) or fb.get(k)) for k in inv_meta.keys()}

        out_items.append(
            {
                "id": id_,
                "display_title": meta.get("display_title"),
                "doc_code": meta.get("doc_code"),
                "doc_kind": meta.get("doc_kind"),
                "release": meta.get("release"),
                "month_year": meta.get("month_year"),
                "filename": filename,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
                "relpath": f"downloads/{filename}",
            }
        )

    out_items = sorted(out_items, key=lambda x: int(x["id"]))

    out = {
        "source_inventory": str(INV),
        "source_id_filename_map": str(MAP),
        "source_normalized_manifest": str(NORM) if NORM.exists() else None,
        "count": len(out_items),
        "missing_files_count": len(missing_files),
        "missing_files_sample": missing_files[:10],
        "items": out_items,
    }

    OUT_JSON.write_text(json.dumps(out, indent=2))

    with OUT_CSV.open("w", newline="\n", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "display_title",
                "doc_code",
                "doc_kind",
                "release",
                "month_year",
                "filename",
                "bytes",
                "sha256",
                "relpath",
                "present_on_portal",
            ],
            lineterminator="\n",
        )
        w.writeheader()
        w.writerows(out_items)

    print(
        f"Wrote {OUT_JSON} and {OUT_CSV} (count={len(out_items)}, missing_files={len(missing_files)})"
    )


if __name__ == "__main__":
    main()
