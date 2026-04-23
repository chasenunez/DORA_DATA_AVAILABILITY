#!/usr/bin/env python3
"""
Merge Phase 1 (default mode) and Phase 2 (deep-search retry) CSV outputs into a
single final results CSV.

Rule: for each DOI, prefer the Phase 2 row if and only if it successfully
resolved a DAS (has_das == 1); otherwise keep the Phase 1 row.
"""
import argparse
import csv
import sys
from pathlib import Path

def load(path: Path) -> dict:
    rows = {}
    with path.open("r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            doi = (row.get("doi") or "").strip()
            if doi:
                rows[doi] = row
    return rows

def merge(phase1: Path, phase2: Path, out: Path) -> None:
    p1 = load(phase1)
    p2 = load(phase2)

    # Fieldnames from phase1 (canonical)
    with phase1.open("r", encoding="utf-8") as f:
        fieldnames = next(csv.reader(f))

    merged = []
    upgraded = 0
    for doi, row in p1.items():
        r2 = p2.get(doi)
        if r2 and (r2.get("has_das") or "").strip() == "1":
            merged.append(r2)
            upgraded += 1
        else:
            merged.append(row)

    # Include any DOIs that were in phase2 but not phase1 (shouldn't normally happen)
    for doi, row in p2.items():
        if doi not in p1:
            merged.append(row)

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in merged:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    total = len(merged)
    has_das = sum(1 for r in merged if (r.get("has_das") or "").strip() == "1")
    print(f"merged {total} rows -> {out}")
    print(f"  phase2 upgraded:  {upgraded}")
    print(f"  has_das=1 final:  {has_das} ({has_das/total*100:.1f}%)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("phase1", type=Path)
    ap.add_argument("phase2", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    merge(args.phase1, args.phase2, args.out)

if __name__ == "__main__":
    main()
