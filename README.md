# DORA Data Availability

A pipeline for reading scientific publications at scale and asking how many of the publications in [DORA](https://www.dora.lib4ri.ch/) tell us where their data are.

## Why this exists

Whether a paper has a data availability statement (DAS), and what that statement actually says, is one of
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

**Pilot** — 200 DOIs stratified by publisher, for a sanity check.
Writes to `DATA/das_pilot_results.csv` by default:

```bash
python3 scripts/das_collect.py
```

**Full run, default (fast) mode**. Use `nohup … &` so the run survives
shell disconnection, and `python3 -u` so progress lines stream live to
the log:

```bash
nohup python3 -u scripts/das_collect.py --all \
    --out DATA/das_results_pass1.csv \
    > DATA/pass1_run.log 2>&1 &
```

On the Eawag corpus (~80k DOIs) this takes a few hours. Throughput
depends mostly on PDF download latency.

**Deep retry** over only the unresolved DOIs from Phase 1:

```bash
nohup python3 -u scripts/das_collect.py \
    --retry-unknowns DATA/das_results_pass1.csv \
    --deep --out DATA/das_results_pass2.csv \
    > DATA/pass2_run.log 2>&1 &
```

Deep mode is meaningfully slower (extra adapters per DOI). Expect roughly
half the throughput of Phase 1.

**Merge** Phase 1 and Phase 2 into a single final CSV. For each DOI, Phase 2
is preferred only when it successfully resolved a DAS:

```bash
python3 scripts/merge_results.py \
    DATA/das_results_pass1.csv \
    DATA/das_results_pass2.csv \
    --out DATA/das_results_final.csv
```

**Re-running on a subset.** If you ever need to re-process a specific
subset of DOIs (for example, to recover a list of rows lost to a writer
crash), pass `--input` to point at an alternate DOI list in the same
pipe-delimited format as `DORA_DOIs.txt`:

```bash
python3 scripts/das_collect.py --all \
    --input DATA/missing_dois.txt \
    --out DATA/das_results_recovery.csv
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

**Important: only run one collector process at a time against a given
output CSV.** Two processes writing to the same CSV will produce a
corrupted file with NUL bytes interleaved through the rows. If you ever
suspect a crash, check first with `ps aux | grep das_collect` before
starting another.

**Cache size.** The Unpaywall PDF cache can grow very large during a full
run (tens of GB) because every fetched PDF is stored as a BLOB so that
re-runs do not have to re-download. Once a phase completes, you can
reclaim that space by stripping the large bodies and vacuuming:

```bash
python3 -c "
import sqlite3
c = sqlite3.connect('DATA/das_cache.sqlite')
c.execute(\"UPDATE http_cache SET body=x'' WHERE tier='unpaywall' AND length(body) > 100000\")
c.commit()
c.execute('VACUUM')"
```

This preserves the row metadata (so future runs still skip the URL) but
drops the bytes themselves.

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


## Some things I found out along the way

- **Not every paper can be read.** A substantial fraction of the 4RI
  output sits in paywalled journals where the full text is not legally
  accessible without an institutional subscription. For those papers, the
  best we can do is record `unknown` and be transparent about it. Default
  mode is deliberately conservative here.
- **"No DAS found" is not the same as "no data shared."** Some disciplines
  have a long tradition of depositing data in community archives without
  ever writing a dedicated DAS (crystallography at the CCDC, for example).
  A zero in `has_das` means "no statement we could find in the
  article."
- **The tool is heuristic.** Data availability sections are written by
  humans, formatted by publishers, and rendered by websites in wildly
  inconsistent ways. The extractor is a compromise between recall and
  precision.

## Results from the first full run (Eawag, ~80k DOIs)

The pipeline was first exercised against the full Eawag publication list
(79,610 unique DOIs after deduplication). Phase 1 ran in default mode;
Phase 2 then re-tried the unresolved DOIs with `--deep` and the merged
output is in `DATA/das_results_final.csv`.

**Headline:** **9,772 papers (12.27%)** carry a recoverable data
availability statement. Of those, **3,175 statements** explicitly point
to one or more datasets via DOI, repository URL, or accession number.

**Where the recovered statements came from:**

| source | rows | notes |
|---|---:|---|
| `europepmc_xml` | 5,257 | structured JATS XML — highest confidence |
| `unpaywall_pdf` | 2,051 | open-access PDF, parsed with pypdf |
| `unpaywall_html` | 1,168 | open-access HTML landing page |
| `crossref_tdm` | 1,082 | full text via Crossref text-mining links |
| `openalex` | 185 | additional OA URLs from OpenAlex |
| `landing_html` / `landing_pdf` / `landing_fulltext_html` | 29 | parsed from publisher landing pages |
| **total `has_das = 1`** | **9,772** | **12.27% of the corpus** |

**What didn't yield a statement:**

| outcome | rows | meaning |
|---|---:|---|
| `none` | 48,008 | not findable — paper not indexed by Europe PMC and no usable open-access copy from Unpaywall |
| `europepmc_miss` | 21,688 | Europe PMC has the paper but no DAS appears in its full-text XML (often a pre-2016 article) |
| `error` | 142 | network or parsing failure |

A few takeaways:

- The Phase 2 deep search added 1,331 papers on top of Phase 1 — small
  in absolute terms, but noteworthy because it reaches papers that no
  conventional source had indexed. Crossref TDM links were the single
  biggest source of Phase 2 hits.
- About 60% of the corpus is in journals Europe PMC does not index —
  largely physical sciences (chemistry, materials, hydrology, physics).
  These are not life-sciences papers, so the absence is structural,
  not a coverage failure.
- Around a quarter of the corpus pre-dates the era when data
  availability statements were common. A `has_das = 0` from a 2008
  paper means something different than a `has_das = 0` from a 2024 one.
  Year-stratified analysis is recommended.