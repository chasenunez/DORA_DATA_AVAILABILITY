#!/usr/bin/env python3
"""
Data Availability Statement (DAS) collector for a list of DOIs.

Tiered strategy:
  Tier 1: Europe PMC search + JATS full-text XML
  Tier 2: Unpaywall -> open-access HTML/XML parsing
  Tier 3: Publisher-specific scrapers (only when DEEP_SEARCH=True)

Output: CSV with columns:
  doi, has_das, das_text, data_identifiers, source, confidence, retrieved_at, notes

Resumable: all HTTP responses are cached in a SQLite DB keyed by (tier, doi).
"""

import argparse
import asyncio
import csv
import json
import random
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import io

import httpx
from bs4 import BeautifulSoup
from lxml import etree

try:
    from pypdf import PdfReader
    _HAS_PDF = True
except ImportError:
    _HAS_PDF = False

# =============================== CONFIG ===============================
EMAIL = "chase.nunez@lib4ri.ch"

INPUT_FILE  = Path("DATA/DORA_DOIs.txt")
OUTPUT_CSV  = Path("DATA/das_pilot_results.csv")
CACHE_DB    = Path("DATA/das_cache.sqlite")

# Toggle Tier 3 publisher-specific scraping. Leave False for fast runs.
DEEP_SEARCH = False

# Pilot: sample a stratified subset. Set to None to process every DOI.
SAMPLE_SIZE = 200
SAMPLE_SEED = 42

CONCURRENCY      = 8       # global in-flight requests
REQUEST_TIMEOUT  = 30
RETRIES          = 2
USER_AGENT       = f"DORA-DAS-Collector/0.1 (mailto:{EMAIL})"
# ======================================================================

EUROPEPMC_SEARCH = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
EUROPEPMC_FULLTEXT = "https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
UNPAYWALL = "https://api.unpaywall.org/v2/{doi}"

# ------------------------------ regexes -------------------------------

# Case-insensitive DOI match. We strip trailing punctuation.
DOI_IN_TEXT_RE = re.compile(r'10\.\d{4,9}/[-._;()/:A-Za-z0-9]+', re.IGNORECASE)

REPO_URL_RE = re.compile(
    r'https?://(?:www\.)?'
    r'(?:zenodo\.org|figshare\.com|datadryad\.org|dryad\.\S+|osf\.io|pangaea\.de|'
    r'dataverse\.[A-Za-z0-9.\-]+|github\.com|gitlab\.[A-Za-z0-9.\-]+|'
    r'ncbi\.nlm\.nih\.gov|ebi\.ac\.uk|kaggle\.com|data\.mendeley\.com|'
    r'openscienceframework\.org|bco-dmo\.org|seanoe\.org|geo-leo\.de)'
    r'/\S+',
    re.IGNORECASE,
)

# Accession patterns with contextual anchors to avoid false positives.
ACCESSION_PATTERNS = [
    ("GEO",         re.compile(r'\bGSE\d{3,}\b')),
    ("SRA",         re.compile(r'\b(?:SR[RPXS])\d{5,}\b')),
    ("BioProject",  re.compile(r'\bPRJ(?:NA|EB|DB)\d+\b')),
    ("BioSample",   re.compile(r'\bSAM[NED]\d+\b')),
    ("ArrayExpress",re.compile(r'\bE-[A-Z]{4}-\d+\b')),
    ("EGA",         re.compile(r'\bEGA[SDN]\d+\b')),
    ("ENA_Run",     re.compile(r'\bERR\d{5,}\b')),
    ("dbGaP",       re.compile(r'\bphs\d{6}(?:\.v\d+\.p\d+)?\b')),
]

# Headings / section types that typically carry DAS content.
DAS_HEADING_RE = re.compile(
    r'(data\s*(?:and\s*code\s*)?availability'
    r'|data\s*accessibility'
    r'|data\s*sharing\s*statement'
    r'|availability\s*of\s*(?:data|materials?)'
    r'|code\s*and\s*data\s*availability'
    r'|data\s*and\s*materials?\s*availability'
    r'|accessibility\s*of\s*data)',
    re.IGNORECASE,
)

JATS_DAS_SEC_TYPES = {
    "data-availability", "data-availability-statement",
    "availability-of-data", "availability",
    "associated-data",               # Europe PMC-injected container
    "data-access", "data-sharing",
}
JATS_DAS_NOTES_TYPES = {"data-availability", "data-access", "data-sharing"}

# --------------------------- data structures --------------------------

@dataclass
class Result:
    doi: str
    has_das: Optional[bool] = None
    das_text: str = ""
    data_identifiers: list = field(default_factory=list)
    source: str = ""           # europepmc_xml, unpaywall_html, publisher_xxx, none
    confidence: str = ""       # high | medium | low | none
    retrieved_at: str = ""
    notes: str = ""

# ----------------------------- cache layer ----------------------------

def init_cache(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS http_cache (
            tier TEXT NOT NULL,
            key  TEXT NOT NULL,
            status INTEGER,
            content_type TEXT,
            body BLOB,
            fetched_at TEXT,
            PRIMARY KEY (tier, key)
        )
    """)
    conn.commit()
    return conn

def cache_get(conn, tier: str, key: str):
    row = conn.execute(
        "SELECT status, content_type, body FROM http_cache WHERE tier=? AND key=?",
        (tier, key),
    ).fetchone()
    return row

def cache_put(conn, tier: str, key: str, status: int, content_type: str, body: bytes):
    conn.execute(
        "INSERT OR REPLACE INTO http_cache(tier,key,status,content_type,body,fetched_at) VALUES (?,?,?,?,?,?)",
        (tier, key, status, content_type, body, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()

# ---------------------------- HTTP helpers ----------------------------

async def fetch(client: httpx.AsyncClient, url: str, *, params=None, headers=None, accept=None) -> httpx.Response:
    hdrs = {"User-Agent": USER_AGENT}
    if accept:
        hdrs["Accept"] = accept
    if headers:
        hdrs.update(headers)
    last_exc = None
    for attempt in range(RETRIES + 1):
        try:
            r = await client.get(url, params=params, headers=hdrs, timeout=REQUEST_TIMEOUT,
                                 follow_redirects=True)
            if r.status_code in (429, 503):
                wait = 2 ** attempt
                await asyncio.sleep(wait)
                continue
            return r
        except (httpx.RequestError, httpx.ReadTimeout) as e:
            last_exc = e
            await asyncio.sleep(2 ** attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError("unreachable")

# ------------------------- Tier 1: Europe PMC -------------------------

async def tier1_europepmc(client: httpx.AsyncClient, conn, doi: str) -> Optional[Result]:
    """Look up DOI in Europe PMC; fetch fullTextXML if OA; extract DAS from JATS."""
    # Step 1: search by DOI
    cache_key = f"search|{doi}"
    cached = cache_get(conn, "epmc", cache_key)
    if cached:
        status, _, body = cached
        data = json.loads(body) if status == 200 and body else None
    else:
        params = {"query": f'DOI:"{doi}"', "resultType": "core", "format": "json", "pageSize": 1}
        r = await fetch(client, EUROPEPMC_SEARCH, params=params)
        data = r.json() if r.status_code == 200 else None
        cache_put(conn, "epmc", cache_key, r.status_code,
                  "application/json", json.dumps(data).encode() if data else b"")

    if not data:
        return None
    hits = data.get("resultList", {}).get("result", [])
    if not hits:
        return None
    hit = hits[0]

    # Many core records now carry a 'dataAvailability' field directly.
    das_inline = (hit.get("dataAvailability") or "").strip()
    has_das_flag = (hit.get("hasDataAvailabilityStatement") == "Y")

    pmcid = hit.get("pmcid")
    src_id = pmcid or hit.get("id")
    src_db = "PMC" if pmcid else hit.get("source", "MED")

    das_text = das_inline
    source_tag = "europepmc_metadata" if das_inline else ""

    # Step 2: if OA (has PMCID), fetch full text XML for better coverage.
    if pmcid and hit.get("isOpenAccess") == "Y":
        ft_key = f"fullTextXML|{pmcid}"
        cached_ft = cache_get(conn, "epmc", ft_key)
        if cached_ft:
            status, ctype, body = cached_ft
            xml_bytes = body if status == 200 else None
        else:
            url = EUROPEPMC_FULLTEXT.format(pmcid=pmcid)
            r = await fetch(client, url, accept="application/xml")
            xml_bytes = r.content if r.status_code == 200 else None
            cache_put(conn, "epmc", ft_key, r.status_code,
                      r.headers.get("content-type", ""), xml_bytes or b"")

        if xml_bytes:
            xml_das = extract_das_from_jats(xml_bytes)
            if xml_das:
                das_text = xml_das
                source_tag = "europepmc_xml"

    if not das_text and not has_das_flag:
        # Europe PMC knows the paper but no DAS signal.
        return Result(
            doi=doi, has_das=False, source="europepmc_miss",
            confidence="medium",
            retrieved_at=datetime.now(timezone.utc).isoformat(),
            notes=f"epmc indexed ({src_db}:{src_id}) with no DAS field",
        )

    if not das_text and has_das_flag:
        return Result(
            doi=doi, has_das=True, source="europepmc_flag",
            confidence="low",
            retrieved_at=datetime.now(timezone.utc).isoformat(),
            notes="hasDataAvailabilityStatement=Y but no text exposed",
        )

    return Result(
        doi=doi,
        has_das=True,
        das_text=das_text,
        data_identifiers=extract_identifiers(das_text, exclude_doi=doi),
        source=source_tag or "europepmc",
        confidence="high" if source_tag == "europepmc_xml" else "medium",
        retrieved_at=datetime.now(timezone.utc).isoformat(),
    )

def _localname(node) -> str:
    """Return the element localname, or '' for non-element nodes (comments, PIs)."""
    tag = getattr(node, "tag", None)
    if not isinstance(tag, str):
        return ""
    if tag.startswith("{"):
        return tag.split("}", 1)[1]
    return tag

def extract_das_from_jats(xml_bytes: bytes) -> str:
    """Parse JATS XML for a data-availability section/notes element."""
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        return ""

    elements = [e for e in root.iter() if isinstance(e.tag, str)]

    # 1) <sec sec-type="data-availability"> (and variants) or title-based
    for sec in elements:
        if _localname(sec) != "sec":
            continue
        sec_type = (sec.get("sec-type") or "").lower()
        if sec_type in JATS_DAS_SEC_TYPES:
            return _text_of(sec)
        title_el = next((c for c in sec if _localname(c) == "title"), None)
        if title_el is not None and (title_el.text or ""):
            if DAS_HEADING_RE.search(title_el.text):
                return _text_of(sec)

    # 2) <notes notes-type=...> or <notes> with matching <title>
    for notes in elements:
        if _localname(notes) != "notes":
            continue
        ntype = (notes.get("notes-type") or "").lower()
        if ntype in JATS_DAS_NOTES_TYPES:
            return _text_of(notes)
        title_el = next((c for c in notes if _localname(c) == "title"), None)
        if title_el is not None and (title_el.text or "") and DAS_HEADING_RE.search(title_el.text):
            return _text_of(notes)

    # 3) <custom-meta> with meta-name matching DAS heading
    for cm in elements:
        if _localname(cm) != "custom-meta":
            continue
        name_el = next((c for c in cm if _localname(c) == "meta-name"), None)
        value_el = next((c for c in cm if _localname(c) == "meta-value"), None)
        if name_el is not None and name_el.text and DAS_HEADING_RE.search(name_el.text):
            if value_el is not None:
                return _text_of(value_el)

    # 4) Footnote with fn-type hinting data availability
    for fn in elements:
        if _localname(fn) != "fn":
            continue
        ft = (fn.get("fn-type") or "").lower()
        if "data" in ft and ("availab" in ft or "access" in ft or "shar" in ft):
            return _text_of(fn)

    return ""

def _text_of(el) -> str:
    """Flatten element text content, skipping <title> (heading duplicate) and non-element nodes."""
    pieces = []
    for node in el.iter():
        if not isinstance(node.tag, str):
            continue
        if _localname(node) == "title":
            continue
        if node.text:
            pieces.append(node.text)
        if node.tail:
            pieces.append(node.tail)
    txt = " ".join(pieces)
    return re.sub(r'\s+', ' ', txt).strip()

# -------------------------- Tier 2: Unpaywall -------------------------

async def tier2_unpaywall_html(client: httpx.AsyncClient, conn, doi: str) -> Optional[Result]:
    cache_key = f"unpaywall|{doi}"
    cached = cache_get(conn, "unpaywall", cache_key)
    if cached:
        status, _, body = cached
        data = json.loads(body) if status == 200 and body else None
    else:
        r = await fetch(client, UNPAYWALL.format(doi=quote(doi, safe="/")),
                        params={"email": EMAIL})
        data = r.json() if r.status_code == 200 else None
        cache_put(conn, "unpaywall", cache_key, r.status_code,
                  "application/json", json.dumps(data).encode() if data else b"")
    if not data:
        return None

    best = data.get("best_oa_location") or {}
    # Build ordered, de-duplicated candidate list. HTML landing pages first,
    # then PDF URLs (parsed in-process if pypdf is available).
    html_candidates, pdf_candidates = [], []
    def _add(url, is_pdf):
        if not url: return
        tgt = pdf_candidates if is_pdf else html_candidates
        if url not in tgt:
            tgt.append(url)
    _add(best.get("url_for_landing_page"), False)
    _add(best.get("url"), False)
    _add(best.get("url_for_pdf"), True)
    for loc in data.get("oa_locations") or []:
        _add(loc.get("url_for_landing_page"), False)
        _add(loc.get("url"), False)
        _add(loc.get("url_for_pdf"), True)

    # Try HTML first, cap attempts for politeness
    for url in html_candidates[:3]:
        result = await _fetch_and_extract(client, conn, doi, url, expect_pdf=False)
        if result:
            return result
    # Then PDF
    for url in pdf_candidates[:2]:
        result = await _fetch_and_extract(client, conn, doi, url, expect_pdf=True)
        if result:
            return result
    return None

async def _fetch_and_extract(client, conn, doi: str, url: str, *, expect_pdf: bool) -> Optional[Result]:
    cache_key = f"{'pdf' if expect_pdf else 'html'}|{url}"
    cached = cache_get(conn, "unpaywall", cache_key)
    if cached:
        status, ctype, body = cached
    else:
        try:
            accept = "application/pdf" if expect_pdf else "text/html,application/xml;q=0.9"
            r = await fetch(client, url, accept=accept)
        except Exception:
            cache_put(conn, "unpaywall", cache_key, 0, "", b"")
            return None
        status, ctype, body = r.status_code, r.headers.get("content-type", ""), r.content
        cache_put(conn, "unpaywall", cache_key, status, ctype, body)

    if status != 200 or not body:
        return None

    ctype_l = (ctype or "").lower()
    is_pdf = "pdf" in ctype_l or body[:4] == b"%PDF"

    if is_pdf:
        if not _HAS_PDF:
            return None
        das_text = extract_das_from_pdf(body)
        source_tag = "unpaywall_pdf"
    else:
        das_text = extract_das_from_html(body)
        source_tag = "unpaywall_html"

    if das_text:
        return Result(
            doi=doi, has_das=True, das_text=das_text,
            data_identifiers=extract_identifiers(das_text, exclude_doi=doi),
            source=source_tag,
            confidence="medium",
            retrieved_at=datetime.now(timezone.utc).isoformat(),
            notes=f"from {url}",
        )
    return None

def extract_das_from_pdf(body: bytes) -> str:
    try:
        reader = PdfReader(io.BytesIO(body))
    except Exception:
        return ""
    # Concatenate all pages' text
    pages = []
    for p in reader.pages:
        try:
            pages.append(p.extract_text() or "")
        except Exception:
            continue
    text = "\n".join(pages)
    return extract_das_from_plaintext(text)

def extract_das_from_plaintext(text: str) -> str:
    """Heuristic: locate a DAS heading; capture ~600 chars following, trimmed
    at the next plausible section heading."""
    if not text:
        return ""
    # Normalize whitespace within each line but keep line breaks as anchors
    lines = [re.sub(r'[ \t]+', ' ', ln).strip() for ln in text.splitlines()]
    joined = "\n".join(lines)
    m = DAS_HEADING_RE.search(joined)
    if not m:
        return ""
    start = m.end()
    window = joined[start:start + 2000]
    # stop at the next ALL-CAPS / Title Case heading or common subsequent section names
    stop = re.search(
        r'\n\s*(?:[A-Z][A-Z ]{3,}\n'             # ALL CAPS heading
        r'|(?:Acknowledg|Fund|Conflict|Reference|Author\s+contrib|'
        r'Supplement|Competing\s+interest|Abbreviations|Ethics)\b)',
        window,
    )
    if stop:
        window = window[:stop.start()]
    # Drop leading colon/period/whitespace
    window = window.lstrip(" :.\u2013\u2014-\n\t")
    return re.sub(r'\s+', ' ', window).strip()

def extract_das_from_html(body: bytes) -> str:
    """Heuristic extraction: find a heading matching DAS and return the text
    until the next heading or section boundary."""
    try:
        soup = BeautifulSoup(body, "lxml")
    except Exception:
        soup = BeautifulSoup(body, "html.parser")

    # Pass 1: look for elements with a DAS-ish id/class
    for el in soup.find_all(attrs={"id": re.compile(r'data.?avail|avail.*data', re.I)}):
        txt = _clean_html_text(el)
        if len(txt) > 40:
            return txt
    for el in soup.find_all(attrs={"class": re.compile(r'data.?avail|avail.*data', re.I)}):
        txt = _clean_html_text(el)
        if len(txt) > 40:
            return txt

    # Pass 2: find a heading that matches DAS, then collect following content
    for tag in soup.find_all(re.compile(r'^h[1-6]$')):
        title = tag.get_text(" ", strip=True)
        if DAS_HEADING_RE.search(title):
            chunks = []
            for sib in tag.next_siblings:
                name = getattr(sib, "name", None)
                if name and re.fullmatch(r'h[1-6]', name):
                    break
                text = (sib.get_text(" ", strip=True) if hasattr(sib, "get_text")
                        else str(sib).strip())
                if text:
                    chunks.append(text)
                if sum(len(c) for c in chunks) > 4000:
                    break
            joined = re.sub(r'\s+', ' ', " ".join(chunks)).strip()
            if len(joined) > 20:
                return joined

    # Pass 3: <section> with role/aria-label hints
    for sec in soup.find_all("section"):
        label = " ".join(filter(None, [sec.get("aria-label", ""), sec.get("data-title", "")]))
        if DAS_HEADING_RE.search(label):
            return _clean_html_text(sec)

    return ""

def _clean_html_text(el) -> str:
    txt = el.get_text(" ", strip=True)
    return re.sub(r'\s+', ' ', txt).strip()

# -------------------- Tier 3: publisher-specific ----------------------
# Off by default; wire in more adapters as needed when DEEP_SEARCH=True.

async def tier3_publisher(client: httpx.AsyncClient, conn, doi: str) -> Optional[Result]:
    if not DEEP_SEARCH:
        return None
    prefix = doi.split("/", 1)[0]
    # Hooks for expansion: PLOS, MDPI, eLife, Frontiers, Springer, Wiley
    # Each adapter: resolve landing page URL, fetch, parse XML/HTML.
    # Deliberately minimal in the pilot.
    return None

# ----------------------- identifier extraction -----------------------

def extract_identifiers(text: str, *, exclude_doi: str = "") -> list:
    if not text:
        return []
    found = []
    seen = set()

    # DOIs
    for m in DOI_IN_TEXT_RE.finditer(text):
        raw = m.group(0).rstrip(').,;:]')
        if raw.lower() == (exclude_doi or "").lower():
            continue
        if raw.lower() in seen:
            continue
        seen.add(raw.lower())
        found.append({"type": "DOI", "value": raw})

    # Repository URLs
    for m in REPO_URL_RE.finditer(text):
        url = m.group(0).rstrip(').,;:]')
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        found.append({"type": "URL", "value": url})

    # Accessions
    for label, rx in ACCESSION_PATTERNS:
        for m in rx.finditer(text):
            v = m.group(0)
            key = f"{label}:{v}".lower()
            if key in seen:
                continue
            seen.add(key)
            found.append({"type": label, "value": v})

    return found

# ------------------------------ runner --------------------------------

def load_dois(path: Path) -> list:
    pairs = []
    with path.open("r", encoding="utf-8") as f:
        header = f.readline()  # discard header
        for line in f:
            line = line.strip()
            if not line:
                continue
            if "|" not in line:
                continue
            pid, doi = line.split("|", 1)
            doi = doi.strip()
            if doi:
                pairs.append((pid.strip(), doi))
    return pairs

def stratified_sample(pairs: list, n: int, seed: int) -> list:
    if n is None or n >= len(pairs):
        return pairs
    # Stratify by DOI registrant prefix to get publisher diversity
    buckets = {}
    for p, d in pairs:
        prefix = d.split("/", 1)[0]
        buckets.setdefault(prefix, []).append((p, d))
    rng = random.Random(seed)
    # Proportional sampling across buckets
    sample = []
    total = len(pairs)
    for prefix, items in buckets.items():
        share = max(1, round(len(items) / total * n))
        rng.shuffle(items)
        sample.extend(items[:share])
    rng.shuffle(sample)
    return sample[:n]

async def process_doi(client, conn, doi: str, sem: asyncio.Semaphore) -> Result:
    async with sem:
        try:
            r = await tier1_europepmc(client, conn, doi)
            if r and r.has_das and r.das_text:
                return r
            # If tier1 missed entirely or was only a flag, try tier2
            r2 = await tier2_unpaywall_html(client, conn, doi)
            if r2 and r2.has_das and r2.das_text:
                return r2
            # Deep mode
            if DEEP_SEARCH:
                r3 = await tier3_publisher(client, conn, doi)
                if r3:
                    return r3
            # Return the best "no/partial" result we have
            if r:
                return r
            return Result(
                doi=doi, has_das=False, source="none",
                confidence="low",
                retrieved_at=datetime.now(timezone.utc).isoformat(),
                notes="not found in epmc or unpaywall",
            )
        except Exception as e:
            return Result(
                doi=doi, has_das=None, source="error",
                retrieved_at=datetime.now(timezone.utc).isoformat(),
                notes=f"{type(e).__name__}: {e}",
            )

async def main_async(args):
    all_pairs = load_dois(INPUT_FILE)
    print(f"[load] {len(all_pairs)} DOIs in {INPUT_FILE}")

    size = args.sample if args.sample is not None else SAMPLE_SIZE
    if args.all:
        size = None
    sample = stratified_sample(all_pairs, size, SAMPLE_SEED)
    print(f"[sample] processing {len(sample)} DOIs (deep_search={DEEP_SEARCH})")

    conn = init_cache(CACHE_DB)
    sem = asyncio.Semaphore(CONCURRENCY)

    limits = httpx.Limits(max_connections=CONCURRENCY * 2,
                          max_keepalive_connections=CONCURRENCY)
    async with httpx.AsyncClient(limits=limits, http2=False) as client:
        tasks = [process_doi(client, conn, doi, sem) for _, doi in sample]
        results = []
        t0 = time.time()
        for i, fut in enumerate(asyncio.as_completed(tasks), 1):
            r = await fut
            results.append(r)
            if i % 20 == 0 or i == len(tasks):
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed else 0
                print(f"  [{i}/{len(tasks)}] {rate:.1f} doi/s  last={r.source} has_das={r.has_das}")

    out_path = args.out or OUTPUT_CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["doi", "has_das", "das_text", "data_identifiers",
                    "source", "confidence", "retrieved_at", "notes"])
        for r in results:
            w.writerow([
                r.doi,
                "" if r.has_das is None else int(r.has_das),
                r.das_text,
                json.dumps(r.data_identifiers, ensure_ascii=False) if r.data_identifiers else "",
                r.source, r.confidence, r.retrieved_at, r.notes,
            ])
    print(f"[write] {out_path}")
    summarize(results)

def summarize(results):
    n = len(results)
    with_das = sum(1 for r in results if r.has_das)
    with_text = sum(1 for r in results if r.has_das and r.das_text)
    errors = sum(1 for r in results if r.source == "error")
    none = sum(1 for r in results if r.source == "none")
    by_source = {}
    for r in results:
        by_source[r.source] = by_source.get(r.source, 0) + 1
    print()
    print("======== PILOT SUMMARY ========")
    print(f"  total:          {n}")
    print(f"  has_das=True:   {with_das}  ({with_das/n*100:.1f}%)")
    print(f"  with full text: {with_text}  ({with_text/n*100:.1f}%)")
    print(f"  not found:      {none}")
    print(f"  errors:         {errors}")
    print("  by source:")
    for s, c in sorted(by_source.items(), key=lambda kv: -kv[1]):
        print(f"    {s:<28} {c}")
    print("================================")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=None,
                    help="override SAMPLE_SIZE from config")
    ap.add_argument("--all", action="store_true",
                    help="process every DOI in the input file")
    ap.add_argument("--out", type=Path, default=None,
                    help="override output CSV path")
    args = ap.parse_args()
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main()
