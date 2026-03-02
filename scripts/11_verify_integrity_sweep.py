#!/usr/bin/env python3
import subprocess
from pathlib import Path

DL = Path("downloads")


def main():
    results = {"pdf_ok": 0, "pdf_fail": 0, "zip_ok": 0, "zip_fail": 0, "other": 0}
    errors = []

    for f in DL.iterdir():
        if not f.is_file():
            continue
        ext = f.suffix.lower()

        if ext == ".pdf":
            r = subprocess.run(
                ["pdfinfo", str(f)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            if r.returncode == 0:
                results["pdf_ok"] += 1
            else:
                results["pdf_fail"] += 1
                errors.append(f.name)

        elif ext in [".docx", ".xlsx", ".zip"]:
            r = subprocess.run(
                ["unzip", "-t", str(f)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            if r.returncode == 0:
                results["zip_ok"] += 1
            else:
                results["zip_fail"] += 1
                errors.append(f.name)
        else:
            results["other"] += 1

    print("Integrity summary:", results)
    if errors:
        print("Failures (first 20):", errors[:20])


if __name__ == "__main__":
    main()
