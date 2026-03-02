#!/usr/bin/env python3
import csv
import json
from pathlib import Path

INV = Path("inventory/download_inventory.full.json")
MAP = Path("inventory/id_filename_map.json")
DL = Path("downloads")

OUT_JSON = Path("inventory/catalog.latest.json")
OUT_CSV = Path("inventory/catalog.latest.csv")


def sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    inv = json.loads(INV.read_text())
    items = inv["items"]

    mp = json.loads(MAP.read_text())
    id_to_filename = mp["mapping"]

    out_items = []
    missing_files = []

    for it in items:
        id_ = str(it["id"])
        filename = id_to_filename.get(id_)
        if not filename:
            # Should not happen since mapping count=162, but keep safe
            missing_files.append({"id": id_, "reason": "missing_filename_in_map"})
            continue

        path = DL / filename
        if not path.exists():
            missing_files.append(
                {"id": id_, "filename": filename, "reason": "file_not_found_in_downloads"}
            )
            continue

        out_items.append(
            {
                # identity
                "id": id_,
                "filename": filename,
                "relpath": f"downloads/{filename}",
                # portal human metadata
                "display_title": it.get("display_title"),
                "doc_code": it.get("doc_code"),
                "doc_kind": it.get("doc_kind"),
                "release": it.get("release"),
                "month_year": it.get("month_year"),
                # integrity + size
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )

    out_items = sorted(out_items, key=lambda x: int(x["id"]))

    out = {
        "source_inventory": str(INV),
        "source_id_filename_map": str(MAP),
        "count": len(out_items),
        "missing_files_count": len(missing_files),
        "missing_files_sample": missing_files[:10],
        "items": out_items,
    }

    OUT_JSON.write_text(json.dumps(out, indent=2))

    with OUT_CSV.open("w", newline="") as f:
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
            ],
        )
        w.writeheader()
        w.writerows(out_items)

    print(
        f"Wrote {OUT_JSON} and {OUT_CSV} (count={len(out_items)}, missing_files={len(missing_files)})"
    )


if __name__ == "__main__":
    main()
