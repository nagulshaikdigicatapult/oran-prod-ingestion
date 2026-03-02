#!/usr/bin/env python3

import json
import re
from pathlib import Path
from urllib.parse import unquote

import requests

DL_DIR = Path("downloads")
OUT = Path("reports/rename_map.json")
OUT.parent.mkdir(parents=True, exist_ok=True)

# Acceptable safe filename chars
SAFE_RE = re.compile(r"[^A-Za-z0-9._()\- ]+")


def sanitize(name: str) -> str:
    name = name.strip().replace("\n", " ")
    name = name.replace("/", "_").replace("\\", "_")
    name = SAFE_RE.sub("_", name)
    # collapse spaces/underscores a bit
    name = re.sub(r"[ _]{2,}", "_", name)
    return name[:180] if len(name) > 180 else name


def parse_content_disposition(cd: str | None) -> str | None:
    if not cd:
        return None

    # Prefer filename*=UTF-8''...
    m = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        return unquote(m.group(1))

    m = re.search(r"filename\s*=\s*\"?([^\";]+)\"?", cd, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return None


def main():
    s = requests.Session()
    s.headers.update({"User-Agent": "oran-prod-ingestion/1.0"})

    renamed = []
    existing = [
        p
        for p in DL_DIR.iterdir()
        if p.is_file() and not p.name.startswith(".") and not p.name.endswith(".part")
    ]

    for p in sorted(existing):
        # Expecting our current naming: o-ran_<id>.<ext>
        m = re.match(r"o-ran_(\d+)\.(\w+)$", p.name)
        if not m:
            continue
        id_ = m.group(1)
        url = f"https://specifications.o-ran.org/download?id={id_}"

        try:
            # Range GET to force Content-Disposition without downloading file
            r = s.get(
                url, stream=True, timeout=30, headers={"Range": "bytes=0-0"}, allow_redirects=True
            )
            cd = r.headers.get("Content-Disposition")
            orig = parse_content_disposition(cd)

            if not orig:
                renamed.append(
                    {"id": id_, "old": p.name, "new": None, "note": "no_content_disposition"}
                )
                continue

            new_name = sanitize(orig)
            target = DL_DIR / new_name

            # Collision handling
            if target.exists() and target.resolve() != p.resolve():
                stem = target.stem
                suffix = target.suffix
                target = DL_DIR / f"{stem}__id-{id_}{suffix}"

            p.rename(target)
            renamed.append(
                {"id": id_, "old": p.name, "new": target.name, "content_disposition": cd}
            )

        except Exception as e:
            renamed.append({"id": id_, "old": p.name, "new": None, "error": str(e)})

    OUT.write_text(json.dumps(renamed, indent=2))
    print(f"done: renamed={sum(1 for x in renamed if x.get('new'))}, report={OUT}")


if __name__ == "__main__":
    main()
