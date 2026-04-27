#!/usr/bin/env python3
"""
Generate a hand-review sheet for spot-checking extractor quality.

Samples N rows where has_das = 1, *stratified by source tier* so the sample
covers both high-confidence Europe PMC XML hits and lower-confidence PDF /
Crossref-TDM / OpenAlex hits in proportion. Writes:

  - DATA/spot_check.md   — a human-readable Markdown sheet with the DAS text,
                           extracted identifiers, and a verdict checkbox.
  - DATA/spot_check.csv  — same sample as a CSV with empty `verdict` and
                           `notes` columns. Fill these in and we can compute
                           extractor precision afterwards.

The seed is fixed so re-running produces the same sample (idempotent review).

Usage:
    python3 scripts/spot_check.py
    python3 scripts/spot_check.py --n 50 --seed 42
"""

import argparse
import csv
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import quote


def load(path: Path) -> list:
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def stratified_sample(rows, n: int, seed: int) -> list:
    """Sample n rows from has_das=1, stratified by source so all extraction
    paths are represented in proportion to their share of the total hits."""
    pool = [r for r in rows if (r.get("has_das") or "") == "1"]
    by_src = defaultdict(list)
    for r in pool:
        by_src[r.get("source", "?")].append(r)

    rng = random.Random(seed)
    total = len(pool)
    sampled = []
    # Allocate per source proportionally, with a minimum of 1 if that source
    # contributed any hits at all (so rare sources still show up for review).
    for src, items in sorted(by_src.items()):
        share = max(1, round(len(items) / total * n))
        rng.shuffle(items)
        sampled.extend(items[:share])

    rng.shuffle(sampled)
    return sampled[:n]


def parse_ids(s: str) -> list:
    if not s:
        return []
    try:
        return json.loads(s)
    except Exception:
        return []


def render_md(samples, out_md: Path) -> None:
    lines = []
    lines.append("# DAS Extractor Spot Check\n")
    lines.append(
        f"_{len(samples)} randomly sampled rows where `has_das = 1`, "
        f"stratified by source tier._\n\n"
    )
    lines.append(
        "For each entry, open the DOI, find the data availability statement "
        "in the published paper, and compare it to the extracted text below. "
        "Tick one of the verdicts and (optionally) leave a short note. The "
        "same sample is also written to `DATA/spot_check.csv` for tabulation.\n\n"
    )
    lines.append("**Verdict legend.**\n")
    lines.append("- `OK` — the extracted DAS matches the paper's actual statement.\n")
    lines.append("- `partial` — captured the right region but truncated, missing the tail, or grabbed an extra paragraph.\n")
    lines.append("- `wrong section` — the text is from a different part of the paper (e.g., methods, abstract).\n")
    lines.append("- `paper has no DAS` — false positive: there is no DAS in the actual paper.\n")
    lines.append("\n---\n\n")

    # Per-source distribution mini-table
    src_counts = Counter(r.get("source", "?") for r in samples)
    lines.append("**Sample composition by source tier:**\n\n")
    lines.append("| source | n |\n|---|---:|\n")
    for s, c in src_counts.most_common():
        lines.append(f"| `{s}` | {c} |\n")
    lines.append("\n---\n\n")

    for i, r in enumerate(samples, 1):
        doi = r.get("doi", "")
        das = (r.get("das_text") or "").strip()
        src = r.get("source", "")
        conf = r.get("confidence", "")
        ids = parse_ids(r.get("data_identifiers", ""))
        year = r.get("year", "")
        journal = r.get("journal", "")
        publisher = r.get("publisher", "")

        lines.append(f"## {i}. `{doi}`\n\n")
        lines.append(f"- **DOI link:** <https://doi.org/{quote(doi, safe='/')}>\n")
        lines.append(f"- **Source tier:** `{src}`  ·  **confidence:** `{conf}`\n")
        if year or journal:
            meta = []
            if year:    meta.append(year)
            if journal: meta.append(f"_{journal}_")
            if publisher: meta.append(publisher)
            lines.append(f"- **Bibliographic:** {', '.join(meta)}\n")

        lines.append("\n**Extracted DAS:**\n\n")
        if das:
            # Quote-block, but escape pipes that would break Markdown tables
            for line in das.splitlines() or [das]:
                lines.append(f"> {line}\n")
        else:
            lines.append("> _(empty)_\n")
        lines.append("\n")

        if ids:
            lines.append("**Extracted identifiers:**\n\n")
            for it in ids:
                t = it.get("type", "?")
                v = it.get("value", "")
                lines.append(f"- `{t}` — {v}\n")
            lines.append("\n")
        else:
            lines.append("**Extracted identifiers:** _none_\n\n")

        lines.append("**Verdict:**  ☐ OK   ☐ partial   ☐ wrong section   ☐ paper has no DAS\n\n")
        lines.append("**Notes:** _________________________________________________\n\n")
        lines.append("---\n\n")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    with out_md.open("w", encoding="utf-8") as f:
        f.writelines(lines)


def render_csv(samples, out_csv: Path) -> None:
    fields = ["index", "doi", "year", "journal", "publisher",
              "source", "confidence",
              "n_identifiers", "das_chars",
              "verdict", "notes",
              "das_text", "data_identifiers"]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for i, r in enumerate(samples, 1):
            ids = parse_ids(r.get("data_identifiers", ""))
            w.writerow({
                "index":          i,
                "doi":            r.get("doi", ""),
                "year":           r.get("year", ""),
                "journal":        r.get("journal", ""),
                "publisher":      r.get("publisher", ""),
                "source":         r.get("source", ""),
                "confidence":     r.get("confidence", ""),
                "n_identifiers":  len(ids),
                "das_chars":      len(r.get("das_text") or ""),
                "verdict":        "",   # to be filled in by reviewer
                "notes":          "",   # to be filled in by reviewer
                "das_text":       r.get("das_text", ""),
                "data_identifiers": r.get("data_identifiers", ""),
            })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", type=Path,
                    default=Path("DATA/das_results_enriched.csv"),
                    help="enriched results CSV (falls back to final.csv)")
    ap.add_argument("--out-md", type=Path, default=Path("DATA/spot_check.md"))
    ap.add_argument("--out-csv", type=Path, default=Path("DATA/spot_check.csv"))
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    if not args.inp.exists():
        fallback = Path("DATA/das_results_final.csv")
        if fallback.exists():
            print(f"note: {args.inp} not found, falling back to {fallback}")
            args.inp = fallback
        else:
            print(f"error: input file {args.inp} not found", file=sys.stderr)
            sys.exit(1)

    rows = load(args.inp)
    print(f"loaded {len(rows):,} rows from {args.inp}")

    samples = stratified_sample(rows, args.n, args.seed)
    print(f"sampled {len(samples)} has_das=1 rows (seed={args.seed})")

    render_md(samples, args.out_md)
    render_csv(samples, args.out_csv)
    print(f"wrote {args.out_md}")
    print(f"wrote {args.out_csv}")
    print()
    print("Open the markdown file, work through the entries, then either")
    print("  - tick the boxes in the .md and add notes, or")
    print("  - fill the `verdict` and `notes` columns in the .csv for tabulation.")


if __name__ == "__main__":
    main()
