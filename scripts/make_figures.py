#!/usr/bin/env python3
"""
Generate summary figures for the DORA Data Availability study.

Reads the merged final results CSV and produces a small set of charts
showing the relative make-up of data availability statements across the
corpus. Figures land in figures/ as 300-dpi PNGs.

Usage:
    python3 scripts/make_figures.py
    python3 scripts/make_figures.py --in DATA/das_results_final.csv --out figures/
"""

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import rcParams

# Style: restrained, sans-serif, no chart-junk. Single accent palette.

PALETTE = {
    "primary":  "#2E5077",   # deep blue   - "yes / found"
    "secondary":"#4DA1A9",   # teal        - "partial / structured"
    "tertiary": "#79D7BE",   # mint        - "second-source"
    "muted":    "#D6D6D6",   # light grey  - "unknown / missing"
    "warm":     "#F6AE2D",   # gold        - emphasis / concrete
    "warn":     "#E94B3C",   # warm red    - errors only
    "ink":      "#222222",   # text
}

def apply_style():
    rcParams.update({
        "font.family":       "sans-serif",
        "font.sans-serif":   ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
        "font.size":         11,
        "axes.titlesize":    13,
        "axes.titleweight":  "semibold",
        "axes.labelsize":    11,
        "axes.edgecolor":    PALETTE["ink"],
        "axes.labelcolor":   PALETTE["ink"],
        "xtick.color":       PALETTE["ink"],
        "ytick.color":       PALETTE["ink"],
        "axes.spines.top":   False,
        "axes.spines.right": False,
        "axes.grid":         False,
        "figure.dpi":        120,
        "savefig.dpi":       300,
        "savefig.bbox":      "tight",
        "savefig.facecolor": "white",
    })

# Publisher mapping (DOI prefix -> publisher name). Anything unmapped is
# bucketed as "Other" for top-N charts.

PUBLISHER_MAP = {
    "10.1016": "Elsevier",
    "10.1002": "Wiley",
    "10.1111": "Wiley",
    "10.1029": "AGU / Wiley",
    "10.1007": "Springer Nature",
    "10.1038": "Springer Nature",
    "10.1186": "Springer Nature (BMC)",
    "10.1140": "Springer Nature (EPJ)",
    "10.1021": "ACS",
    "10.1039": "RSC",
    "10.1103": "APS",
    "10.1063": "AIP",
    "10.1088": "IOP",
    "10.1109": "IEEE",
    "10.1080": "Taylor & Francis",
    "10.1126": "AAAS / Science",
    "10.1073": "PNAS",
    "10.1093": "Oxford UP",
    "10.1098": "Royal Society",
    "10.1107": "IUCr",
    "10.1117": "SPIE",
    "10.1371": "PLOS",
    "10.3389": "Frontiers",
    "10.3390": "MDPI",
    "10.5194": "Copernicus",
    "10.7717": "PeerJ",
    "10.7554": "eLife",
    "10.1155": "Hindawi",
    "10.1101": "Cold Spring Harbor",
    "10.1128": "ASM",
    "10.1530": "Bioscientifica",
    "10.1167": "ARVO",
    "10.5169": "E-Periodica",
    "10.3929": "ETH Zürich",
    "10.48550": "arXiv",
}

def publisher_of(doi: str) -> str:
    if not doi:
        return "Unknown"
    prefix = doi.split("/", 1)[0]
    return PUBLISHER_MAP.get(prefix, "Other")

# Load and aggregate

def load(path: Path) -> list:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows

def parse_ids(s: str) -> list:
    if not s:
        return []
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return []

# Helpers

def _annotate_pie(ax, total, title, subtitle=None):
    ax.set_title(title, pad=14)
    if subtitle:
        ax.text(0, -1.30, subtitle, ha="center", va="top",
                fontsize=10, color="#555555", transform=ax.transData)
    ax.text(0, 0, f"{total:,}\nDOIs", ha="center", va="center",
            fontsize=14, fontweight="semibold", color=PALETTE["ink"])

def _autopct(values):
    total = sum(values)
    def fmt(p):
        n = int(round(p * total / 100.0))
        return f"{p:.1f}%\n({n:,})"
    return fmt

# Figure 1: top-level outcomes (donut)

def fig_outcomes(rows, out_path: Path):
    has_das       = sum(1 for r in rows if r["has_das"] == "1")
    epmc_no_das   = sum(1 for r in rows if r["source"] == "europepmc_miss")
    not_reachable = sum(1 for r in rows if r["source"] == "none")
    errors        = sum(1 for r in rows if r["source"] == "error")
    total = len(rows)

    labels = [
        "DAS recovered",
        "Indexed in Europe PMC,\nno DAS in paper",
        "Not reachable in default mode\n(paywalled / not indexed)",
        "Errors",
    ]
    values = [has_das, epmc_no_das, not_reachable, errors]
    colors = [PALETTE["primary"], PALETTE["secondary"],
              PALETTE["muted"],   PALETTE["warn"]]

    fig, ax = plt.subplots(figsize=(8.0, 6.4))
    wedges, _texts, autotexts = ax.pie(
        values,
        labels=labels,
        colors=colors,
        startangle=90,
        wedgeprops=dict(width=0.42, edgecolor="white", linewidth=2),
        autopct=_autopct(values),
        pctdistance=0.78,
        textprops=dict(fontsize=10),
    )
    for at in autotexts:
        at.set_color("white")
        at.set_fontsize(9)
    # Errors slice is tiny; nudge label off the donut
    autotexts[-1].set_color(PALETTE["ink"])

    _annotate_pie(ax, total,
                  "Outcomes for every DOI in the DORA Eawag corpus",
                  "Each DOI is classified by the deepest extraction stage that produced an answer.")
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  wrote {out_path}")

# Figure 2: where the DAS came from (horizontal bar)

SOURCE_LABELS = {
    "europepmc_xml":         "Europe PMC (structured XML)",
    "unpaywall_pdf":         "Unpaywall PDF",
    "unpaywall_html":        "Unpaywall HTML",
    "crossref_tdm":          "Crossref TDM link",
    "openalex":              "OpenAlex OA URL",
    "publisher_xml":         "Publisher JATS XML",
    "landing_html":          "Landing page HTML",
    "landing_pdf":           "Landing page PDF",
    "landing_fulltext_html": "Landing fulltext HTML",
    "landing_xml":           "Landing JATS XML",
    "europepmc_metadata":    "Europe PMC metadata",
    "europepmc_flag":        "Europe PMC flag only",
}

# The first three are Phase 1 / default mode; the rest are Phase 2 deep
TIER_OF_SOURCE = {
    "europepmc_xml":         "Phase 1: Europe PMC",
    "europepmc_metadata":    "Phase 1: Europe PMC",
    "europepmc_flag":        "Phase 1: Europe PMC",
    "unpaywall_html":        "Phase 1: Unpaywall",
    "unpaywall_pdf":         "Phase 1: Unpaywall",
    "crossref_tdm":          "Phase 2: Deep search",
    "openalex":              "Phase 2: Deep search",
    "publisher_xml":         "Phase 2: Deep search",
    "landing_html":          "Phase 2: Deep search",
    "landing_pdf":           "Phase 2: Deep search",
    "landing_fulltext_html": "Phase 2: Deep search",
    "landing_xml":           "Phase 2: Deep search",
}
TIER_COLOR = {
    "Phase 1: Europe PMC":   PALETTE["primary"],
    "Phase 1: Unpaywall":    PALETTE["secondary"],
    "Phase 2: Deep search":  PALETTE["warm"],
}

def fig_sources(rows, out_path: Path):
    counts = Counter(r["source"] for r in rows if r["has_das"] == "1")
    items = sorted(counts.items(), key=lambda kv: kv[1])  # ascending for nice bar order
    items = [(SOURCE_LABELS.get(s, s), n, TIER_OF_SOURCE.get(s, "Other")) for s, n in items]

    labels = [x[0] for x in items]
    values = [x[1] for x in items]
    colors = [TIER_COLOR.get(x[2], PALETTE["muted"]) for x in items]

    fig, ax = plt.subplots(figsize=(8.5, 5.0))
    bars = ax.barh(labels, values, color=colors, edgecolor="white", linewidth=0.6)
    total = sum(values)
    for bar, v in zip(bars, values):
        ax.text(bar.get_width() + total * 0.005, bar.get_y() + bar.get_height() / 2,
                f"{v:,}  ({v/total*100:.1f}%)",
                va="center", ha="left", fontsize=9, color=PALETTE["ink"])

    ax.set_xlim(0, max(values) * 1.18)
    ax.set_xlabel("Papers with a recovered DAS")
    ax.set_title(f"How we found the {total:,} data availability statements")

    # Legend mapping colors to tiers
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in TIER_COLOR.values()]
    ax.legend(handles, list(TIER_COLOR.keys()), loc="lower right", frameon=False)

    fig.savefig(out_path)
    plt.close(fig)
    print(f"  wrote {out_path}")

# Figure 3: DAS rate by publisher (stacked bar, top-N by paper count)

def fig_by_publisher(rows, out_path: Path, top_n: int = 12, min_papers: int = 100):
    by_pub = Counter()
    das_by_pub = Counter()
    for r in rows:
        p = publisher_of(r["doi"])
        by_pub[p] += 1
        if r["has_das"] == "1":
            das_by_pub[p] += 1

    # Take the top_n publishers by total paper count, but require min_papers
    candidates = [(p, n) for p, n in by_pub.items() if n >= min_papers and p != "Unknown"]
    candidates.sort(key=lambda kv: -kv[1])
    selected = candidates[:top_n]

    publishers = [p for p, _ in selected]
    totals     = [by_pub[p] for p in publishers]
    yes        = [das_by_pub[p] for p in publishers]
    no         = [t - y for t, y in zip(totals, yes)]
    rates      = [y / t for y, t in zip(yes, totals)]

    # Sort descending by DAS rate so leaders sit at the top
    order = sorted(range(len(publishers)), key=lambda i: rates[i])
    publishers = [publishers[i] for i in order]
    totals     = [totals[i] for i in order]
    yes        = [yes[i] for i in order]
    no         = [no[i] for i in order]
    rates      = [rates[i] for i in order]

    fig, ax = plt.subplots(figsize=(9.0, 6.5))
    bars_yes = ax.barh(publishers, yes, color=PALETTE["primary"],
                       label="Has DAS", edgecolor="white", linewidth=0.6)
    bars_no  = ax.barh(publishers, no, left=yes, color=PALETTE["muted"],
                       label="No DAS found", edgecolor="white", linewidth=0.6)

    for i, (t, r) in enumerate(zip(totals, rates)):
        ax.text(t + max(totals) * 0.01, i,
                f"{r*100:>4.1f}%   (n={t:,})",
                va="center", ha="left", fontsize=9, color=PALETTE["ink"])

    ax.set_xlim(0, max(totals) * 1.22)
    ax.set_xlabel("Papers in the DORA corpus")
    ax.set_title(f"DAS recovery rate by publisher — top {len(publishers)} (≥ {min_papers} papers)")
    ax.legend(loc="lower right", frameon=False)

    fig.savefig(out_path)
    plt.close(fig)
    print(f"  wrote {out_path}")

# Figure 4: among DAS papers, concrete vs vague (donut)

# Words that flag a "vague" or "non-actionable" DAS, when no identifiers found
VAGUE_PATTERNS = re.compile(
    r"\b(available\s+on\s+(?:reasonable\s+)?request"
    r"|upon\s+(?:reasonable\s+)?request"
    r"|from\s+the\s+corresponding\s+author"
    r"|all\s+(?:relevant\s+)?data\s+are\s+within\s+the"
    r"|in\s+the\s+(?:article|paper|manuscript)"
    r"|supplement(?:ary|al)\s+(?:material|information|file))",
    re.IGNORECASE,
)

def fig_specificity(rows, out_path: Path):
    das_rows = [r for r in rows if r["has_das"] == "1"]
    total = len(das_rows)

    has_id = 0
    vague  = 0
    other  = 0
    for r in das_rows:
        ids = parse_ids(r["data_identifiers"])
        if ids:
            has_id += 1
        elif VAGUE_PATTERNS.search(r["das_text"] or ""):
            vague += 1
        else:
            other += 1

    labels = [
        f"Links to a concrete dataset\n(DOI / repository URL / accession)",
        f"Vague: \"on request\" or\n\"in the article / supplement\"",
        f"Other narrative text\n(no identifier extracted)",
    ]
    values = [has_id, vague, other]
    colors = [PALETTE["warm"], PALETTE["muted"], PALETTE["secondary"]]

    fig, ax = plt.subplots(figsize=(8.0, 6.4))
    wedges, _t, autotexts = ax.pie(
        values, labels=labels, colors=colors, startangle=90,
        wedgeprops=dict(width=0.42, edgecolor="white", linewidth=2),
        autopct=_autopct(values), pctdistance=0.78,
        textprops=dict(fontsize=10),
    )
    for at in autotexts:
        at.set_color("white"); at.set_fontsize(9)

    _annotate_pie(ax, total,
                  "What do the recovered statements actually say?",
                  "Among papers with a recovered DAS — does the statement point to retrievable data?")
    fig.savefig(out_path)
    plt.close(fig)
    print(f"  wrote {out_path}")

# Figure 5: types of dataset identifiers found (horizontal bar)

# Group repository URLs into a coarser bucket name
URL_HOST_BUCKET = [
    (re.compile(r"zenodo\.org",      re.I), "Zenodo"),
    (re.compile(r"figshare\.com",    re.I), "Figshare"),
    (re.compile(r"datadryad\.org|dryad\.", re.I), "Dryad"),
    (re.compile(r"osf\.io|openscienceframework", re.I), "OSF"),
    (re.compile(r"pangaea\.de",      re.I), "PANGAEA"),
    (re.compile(r"github\.com",      re.I), "GitHub"),
    (re.compile(r"gitlab\.",         re.I), "GitLab"),
    (re.compile(r"ncbi\.nlm\.nih\.gov", re.I), "NCBI"),
    (re.compile(r"ebi\.ac\.uk",      re.I), "EBI"),
    (re.compile(r"data\.mendeley\.com", re.I), "Mendeley Data"),
    (re.compile(r"dataverse",        re.I), "Dataverse"),
    (re.compile(r"kaggle\.com",      re.I), "Kaggle"),
]

def bucket_identifier(item: dict) -> str:
    t = item.get("type", "")
    v = item.get("value", "")
    if t == "URL":
        for rx, name in URL_HOST_BUCKET:
            if rx.search(v):
                return name
        return "Other URL"
    if t == "DOI":
        # Some DOIs are themselves Zenodo / Dryad / Figshare
        if v.startswith("10.5281/zenodo"): return "Zenodo"
        if v.startswith("10.5061/dryad"):  return "Dryad"
        if v.startswith("10.6084/m9.figshare"): return "Figshare"
        if v.startswith("10.17605/osf"):   return "OSF"
        if v.startswith("10.1594/PANGAEA"): return "PANGAEA"
        return "Dataset DOI (other)"
    return t  # accession types pass through

def fig_identifier_types(rows, out_path: Path, top_n: int = 12):
    counts = Counter()
    papers_with_any = 0
    for r in rows:
        if r["has_das"] != "1":
            continue
        ids = parse_ids(r["data_identifiers"])
        if not ids:
            continue
        papers_with_any += 1
        seen_in_row = set()
        for item in ids:
            b = bucket_identifier(item)
            if b not in seen_in_row:
                counts[b] += 1
                seen_in_row.add(b)

    items = counts.most_common(top_n)
    labels = [k for k, _ in items][::-1]
    values = [v for _, v in items][::-1]

    fig, ax = plt.subplots(figsize=(8.5, 5.5))
    bars = ax.barh(labels, values, color=PALETTE["warm"],
                   edgecolor="white", linewidth=0.6)
    for bar, v in zip(bars, values):
        ax.text(bar.get_width() + max(values) * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{v:,}", va="center", ha="left", fontsize=9, color=PALETTE["ink"])

    ax.set_xlim(0, max(values) * 1.18)
    ax.set_xlabel("Number of papers referencing this destination at least once")
    ax.set_title(
        f"Where the data actually lives\n"
        f"(among the {papers_with_any:,} papers whose DAS contains a usable identifier)"
    )

    fig.savefig(out_path)
    plt.close(fig)
    print(f"  wrote {out_path}")

# Driver

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", type=Path,
                    default=Path("DATA/das_results_final.csv"),
                    help="merged final results CSV")
    ap.add_argument("--out", type=Path, default=Path("figures"),
                    help="output directory for PNGs")
    args = ap.parse_args()

    apply_style()
    args.out.mkdir(parents=True, exist_ok=True)
    rows = load(args.inp)
    print(f"loaded {len(rows):,} rows from {args.inp}")

    fig_outcomes(rows,        args.out / "fig01_outcomes.png")
    fig_sources(rows,         args.out / "fig02_sources.png")
    fig_by_publisher(rows,    args.out / "fig03_by_publisher.png")
    fig_specificity(rows,     args.out / "fig04_specificity.png")
    fig_identifier_types(rows,args.out / "fig05_identifier_types.png")

    print(f"\nfigures written to {args.out}/")

if __name__ == "__main__":
    main()
