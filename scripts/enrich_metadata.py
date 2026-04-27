#!/usr/bin/env python3
"""
Enrich the final DAS results CSV with publication metadata for stratified
analysis: publication year, canonical journal name, ISSN, canonical publisher,
and work type. Source: Crossref.

Reuses the existing das_cache.sqlite cache (tier='tier3', key='crossref|<doi>')
that Phase 2 already populated for ~70k of the ~80k DOIs, so a typical run
only fetches the ~10k uncached remainder.

Usage:
    python3 scripts/enrich_metadata.py
    python3 scripts/enrich_metadata.py --in DATA/das_results_final.csv \
        --out DATA/das_results_enriched.csv
"""

import argparse
import asyncio
import csv
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import quote

import httpx

# ============================ CONFIG =================================
EMAIL = "chase.nunez@lib4ri.ch"
CACHE_DB = Path("DATA/das_cache.sqlite")
CROSSREF = "https://api.crossref.org/works/{doi}"
CONCURRENCY = 12
REQUEST_TIMEOUT = 30
RETRIES = 2
USER_AGENT = f"DORA-DAS-Collector/0.1 (mailto:{EMAIL})"
# =====================================================================

ENRICH_FIELDS = ["year", "journal", "issn", "publisher", "work_type"]


def cache_get(conn, key):
    row = conn.execute(
        "SELECT status, body FROM http_cache WHERE tier='tier3' AND key=?",
        (key,),
    ).fetchone()
    return row

def cache_put(conn, key, status, body_bytes):
    conn.execute(
        "INSERT OR REPLACE INTO http_cache(tier,key,status,content_type,body,fetched_at) "
        "VALUES ('tier3',?,?,?,?,?)",
        (key, status, "application/json", body_bytes,
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


async def fetch(client, url):
    for attempt in range(RETRIES + 1):
        try:
            r = await client.get(
                url,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                timeout=REQUEST_TIMEOUT,
                follow_redirects=True,
            )
            if r.status_code in (429, 503):
                await asyncio.sleep(2 ** attempt)
                continue
            return r
        except (httpx.RequestError, httpx.ReadTimeout):
            await asyncio.sleep(2 ** attempt)
    return None


async def crossref_for(client, conn, doi: str) -> Optional[dict]:
    key = f"crossref|{doi}"
    row = cache_get(conn, key)
    if row:
        status, body = row
        if status == 200 and body:
            try:
                return json.loads(body)
            except Exception:
                return None
        return None

    r = await fetch(client, CROSSREF.format(doi=quote(doi, safe="/")))
    if r is None:
        cache_put(conn, key, 0, b"")
        return None
    if r.status_code != 200:
        cache_put(conn, key, r.status_code, b"")
        return None
    try:
        data = r.json()
    except Exception:
        cache_put(conn, key, r.status_code, b"")
        return None
    cache_put(conn, key, 200, json.dumps(data).encode("utf-8"))
    return data


def extract_metadata(crossref_json: Optional[dict]) -> dict:
    """Pull year, journal, ISSN, publisher, work type from a Crossref payload."""
    out = {f: "" for f in ENRICH_FIELDS}
    if not crossref_json:
        return out
    msg = crossref_json.get("message") or {}

    # --- year: prefer issued, fall back to published-print/online/created
    def _year(field_name):
        node = msg.get(field_name)
        if not node:
            return None
        parts = node.get("date-parts") or [[]]
        if parts and parts[0]:
            try:
                return int(parts[0][0])
            except (ValueError, TypeError):
                return None
        return None

    for fname in ("issued", "published-print", "published-online",
                  "published", "created"):
        y = _year(fname)
        if y:
            out["year"] = str(y)
            break

    # --- journal / container title
    titles = msg.get("container-title") or []
    if titles:
        out["journal"] = titles[0].strip()

    # --- ISSN: prefer print, fall back to electronic; join multiple with ;
    issns = []
    for it in (msg.get("issn-type") or []):
        v = (it.get("value") or "").strip()
        if v and v not in issns:
            issns.append(v)
    if not issns:
        for v in (msg.get("ISSN") or []):
            v = (v or "").strip()
            if v and v not in issns:
                issns.append(v)
    out["issn"] = ";".join(issns)

    # --- publisher
    pub = (msg.get("publisher") or "").strip()
    out["publisher"] = pub

    # --- work type
    out["work_type"] = (msg.get("type") or "").strip()

    return out


async def main_async(args):
    conn = sqlite3.connect(CACHE_DB)
    rows = []
    with args.inp.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        in_fields = list(reader.fieldnames or [])
        for r in reader:
            rows.append(r)
    print(f"loaded {len(rows):,} rows from {args.inp}")

    # First pass: cache hits go fast
    sem = asyncio.Semaphore(CONCURRENCY)
    fetched = 0
    cached_hits = 0
    none_hits = 0
    enrich_by_doi: Dict[str, dict] = {}

    async def work(doi):
        nonlocal fetched, cached_hits, none_hits
        async with sem:
            cached = cache_get(conn, f"crossref|{doi}")
            if cached and cached[0] == 200 and cached[1]:
                cached_hits += 1
            elif cached:
                none_hits += 1
            data = await crossref_for(client, conn, doi)
            enrich_by_doi[doi] = extract_metadata(data)
            if not cached:
                fetched += 1

    limits = httpx.Limits(max_connections=CONCURRENCY * 2,
                          max_keepalive_connections=CONCURRENCY)
    async with httpx.AsyncClient(limits=limits) as client:
        t0 = time.time()
        tasks = [work(r["doi"]) for r in rows if r.get("doi")]
        for i, fut in enumerate(asyncio.as_completed(tasks), 1):
            await fut
            if i % 1000 == 0 or i == len(tasks):
                el = time.time() - t0
                rate = i / el if el else 0
                print(f"  [{i:>6}/{len(tasks)}] {rate:.1f}/s  "
                      f"cache_hits={cached_hits} fetched={fetched} miss={none_hits}")

    out_fields = list(in_fields) + [f for f in ENRICH_FIELDS if f not in in_fields]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()
        for r in rows:
            extra = enrich_by_doi.get(r.get("doi", ""), {})
            for k in ENRICH_FIELDS:
                r[k] = extra.get(k, "")
            w.writerow({k: r.get(k, "") for k in out_fields})

    print(f"\nwrote {args.out}")
    summarize(rows)


def summarize(rows):
    n = len(rows)
    with_year = sum(1 for r in rows if r.get("year"))
    with_journal = sum(1 for r in rows if r.get("journal"))
    with_pub = sum(1 for r in rows if r.get("publisher"))
    print("======== ENRICHMENT SUMMARY ========")
    print(f"  total rows:     {n:,}")
    print(f"  with year:      {with_year:,} ({with_year/n*100:.1f}%)")
    print(f"  with journal:   {with_journal:,} ({with_journal/n*100:.1f}%)")
    print(f"  with publisher: {with_pub:,} ({with_pub/n*100:.1f}%)")

    # year distribution
    from collections import Counter
    years = Counter(r.get("year") or "?" for r in rows)
    print("\n  year (top buckets):")
    for y, c in sorted(((y, c) for y, c in years.items() if y != "?"),
                      key=lambda kv: kv[0])[-10:]:
        print(f"    {y}: {c:,}")
    print(f"    (missing year: {years.get('?', 0):,})")

    # has_das by year, last 10 years
    by_year = {}
    for r in rows:
        y = r.get("year")
        if not y or y == "?": continue
        bucket = by_year.setdefault(y, [0, 0])
        bucket[0] += 1
        if r.get("has_das") == "1":
            bucket[1] += 1
    recent = sorted(by_year.items())[-10:]
    print("\n  DAS recovery rate by year (last 10):")
    for y, (tot, das) in recent:
        print(f"    {y}: {das:>4}/{tot:<5} ({das/tot*100:5.1f}%)")
    print("=====================================")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", type=Path,
                    default=Path("DATA/das_results_final.csv"))
    ap.add_argument("--out", type=Path,
                    default=Path("DATA/das_results_enriched.csv"))
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
