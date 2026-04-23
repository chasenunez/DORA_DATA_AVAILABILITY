# DORA Data Availability

A pipeline for reading scientific publications at scale and asking hoe many of the publications in DORA tell us where their data are.

## Why this exists

Whether a paper has a DAS, and what that statement actually says, is one of
the cleanest signals we have for how open a piece of research really is. It
tells us whether the underlying evidence is inspectable, reusable, and
reproducible. 

This repository is the tooling behind a study of those statements across the
publication output of the **four research institutes of the ETH Domain** —

- **Empa** — Swiss Federal Laboratories for Materials Science and Technology
- **Eawag** — Swiss Federal Institute of Aquatic Science and Technology
- **PSI** — Paul Scherrer Institute
- **WSL** — Swiss Federal Institute for Forest, Snow and Landscape Research

These four institutes share a common publication repository ([DORA](https://www.dora.lib4ri.ch)). 

Given a list of DOIs for each institute's publications, the pipeline retrieves, as
faithfully as possible, each paper's data availability statement — and, when
the statement itself references datasets (by DOI, repository URL, or
accession number), extracts those identifiers too. The output is a
structured CSV that can be aggregated by institute, by year, by publisher,
or by discipline for comparative analysis.

## What it does

For each DOI, the pipeline tries in order:

1. **Europe PMC** — a free, structured source that has already parsed data
   availability sections for a large fraction of the biomedical and
   life-sciences literature. For open-access papers it also returns the full
   JATS XML, which we parse for the DAS directly.
2. **Unpaywall + open-access full text** — for papers not covered by Europe
   PMC, we ask Unpaywall for a legal open-access copy, then extract the DAS
   from the HTML or PDF.
3. **Deep search (optional)** — a set of publisher-specific adapters
   (Crossref TDM links, OpenAlex, PLOS/PeerJ JATS endpoints, landing-page
   `citation_*` meta-tag parsing). Off by default because it is slower and
   more fragile. Turned on with `--deep`.

When a DAS is found, its full text is captured verbatim. The text is then
scanned for dataset identifiers: DOIs, repository URLs (Zenodo, Dryad,
Figshare, OSF, Pangaea, GitHub, …), and common life-science accession
codes (GEO, SRA, BioProject, ArrayExpress, PDB, and so on).

For each DOI, the final record answers three questions:

- Is there a data availability statement? (yes / no / unknown)
- If yes, what does it say, word for word?
- If the statement points to specific datasets, what are their identifiers?

## Repository layout

```
DATA/
  DORA_DOIs.txt              # input: pipe-delimited PID|DOI (one institute at a time)
  das_cache.sqlite           # HTTP response cache (makes runs resumable)
  das_results_pass1.csv      # phase 1 output (default mode)
  das_results_pass2.csv      # phase 2 output (deep mode, retry on unresolved)
  das_results_final.csv      # merged final output
scripts/
  das_collect.py             # main collector
  merge_results.py           # merge phase 1 and phase 2 results
```

## Usage

### Setup

Python 3.9+. Install the dependencies:

```bash
python3 -m pip install httpx beautifulsoup4 lxml pypdf cryptography
```

Open `scripts/das_collect.py` and make sure the config block near the top
matches what you want — in particular:

- `EMAIL` — your contact email (Crossref and Unpaywall ask for this; they
  use it to warn about misbehaving clients rather than silently block them).
- `INPUT_FILE` — path to your DOI list.
- `DEEP_SEARCH` — leave `False` for the first pass.

### Input format

A pipe-delimited text file with a header row, one DOI per line:

```
PID|mods_identifier_doi_mt
eawag:10291|10.5169/seals-53948
eawag:10292|10.5169/seals-83657
eawag:10341|10.14512/gaia.9.1.3
...
```

The `PID` (publication ID) is passed through untouched; it is useful for
rejoining results to the institute's own records.

### Run it

**Pilot** — 200 DOIs stratified by publisher, for a sanity check:

```bash
python3 scripts/das_collect.py
```

**Full run, default (fast) mode**:

```bash
nohup python3 scripts/das_collect.py --all \
    --out DATA/das_results_pass1.csv \
    > DATA/pass1_run.log 2>&1 &
```

**Deep retry** over only the unresolved DOIs from Phase 1:

```bash
python3 scripts/das_collect.py --retry-unknowns DATA/das_results_pass1.csv \
    --deep --out DATA/das_results_pass2.csv
```

**Merge** Phase 1 and Phase 2 into a single final CSV. For each DOI, Phase 2
is preferred only when it successfully resolved a DAS:

```bash
python3 scripts/merge_results.py \
    DATA/das_results_pass1.csv \
    DATA/das_results_pass2.csv \
    --out DATA/das_results_final.csv
```

### Monitoring a long run

Runs take hours on a corpus the size of an institute's full publication list.
A few quick progress checks:

```bash
# count of DOIs processed so far
python3 -c "import sqlite3; c=sqlite3.connect('DATA/das_cache.sqlite'); \
  n=c.execute(\"SELECT COUNT(*) FROM http_cache WHERE tier='epmc' AND key LIKE 'search|%'\").fetchone()[0]; \
  print(f'{n} processed')"

# is the process alive?
ps -p <PID> -o pid,etime,stat,rss,pcpu
```

If the process dies, you can just re-run the same command. Every HTTP response is
cached in `DATA/das_cache.sqlite`, so previously-fetched DOIs resolve in
milliseconds and only new work is done.

## Output schema

Each row of the final CSV corresponds to one input DOI.

| column | meaning |
|---|---|
| `doi` | the DOI, as given in the input |
| `has_das` | `1` if a data availability statement was found, `0` if we are confident there isn't one, empty if unknown (e.g. error, paper not reachable) |
| `das_text` | the full verbatim text of the statement, if found |
| `data_identifiers` | JSON list of `{type, value}` pairs extracted from the statement — DOIs, repository URLs, accession numbers |
| `source` | which tier resolved this row (`europepmc_xml`, `unpaywall_html`, `unpaywall_pdf`, `crossref_tdm`, `openalex`, `publisher_xml`, `landing_html`, …) |
| `confidence` | rough qualitative flag: `high` (structured XML), `medium` (HTML/PDF scrape), `low` (flag only, no text), `none` |
| `retrieved_at` | ISO-8601 timestamp |
| `notes` | free-text diagnostic — the URL used, why something failed, etc. |

## Comparative analysis across the 4RI's

The pipeline itself is institute-agnostic; the comparison is a post-hoc
analysis over the output CSVs. A typical workflow:

1. Run the pipeline once per institute, each with its own DOI list. Keep
   the cache shared (`DATA/das_cache.sqlite`) so that DOIs appearing in
   multiple corpora are fetched only once.
2. Tag each output CSV with the institute name and concatenate.
3. Aggregate: by institute, by publication year, by publisher, by
   discipline (which you can derive from the journal via Crossref if
   needed).


Some things i found out along the way:

- **Not every paper can be read.** A substantial fraction of the 4RI
  output sits in paywalled journals where the full text is not legally
  accessible without an institutional subscription. For those papers, the
  best we can do is record `unknown` and be transparent about it. Default
  mode is deliberately conservative here.
- **"No DAS found" is not the same as "no data shared."** Some disciplines
  have a long tradition of depositing data in community archives without
  ever writing a dedicated DAS (crystallography at the CCDC, for example).
  A zero in `has_das` smeans "no statement we could find in the
  article"
- **The tool is heuristic.** Data availability sections are written by
  humans, formatted by publishers, and rendered by websites in wildly
  inconsistent ways. The extractor is a compromise between recall and
  precision.