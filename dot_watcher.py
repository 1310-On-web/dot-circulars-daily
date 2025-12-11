#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Minimal DoT watcher — writes CSV + JSON to repository root (no data/ folder).
- Scrapes https://dot.gov.in/all-circulars
- Compares against dot_circulars_master.csv in repo root
- Appends new rows to master CSV
- Writes dot_new_entries.json in repo root with metadata for new items
No downloads, no summaries.
"""

from pathlib import Path
from urllib.parse import urljoin
import csv
import json
import os
import sys
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------- CONFIG (repo-root files) ----------
ROOT = Path(__file__).resolve().parent
MASTER_CSV = ROOT / "dot_circulars_master.csv"
JSON_OUT = ROOT / "dot_new_entries.json"

LIST_URL = "https://dot.gov.in/all-circulars"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Connection": "close",
}

# ---------- HTTP session ----------
def build_session():
    s = requests.Session()
    retry = Retry(
        total=6, connect=6, read=6,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=5, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

SESSION = build_session()

# ---------- scraping ----------
def get_soup(url):
    r = SESSION.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def scrape_all_rows():
    """
    Return list of dicts: {'title', 'publish_date', 'pdf_url'}
    """
    soup = get_soup(LIST_URL)
    download_links = soup.select('a:-soup-contains("Download")')
    rows = []
    for a in download_links:
        tr = a.find_parent("tr")
        if not tr:
            continue
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        title = tds[1].get_text(strip=True)
        date_text = tds[-1].get_text(strip=True)
        href = a.get("href", "")
        pdf_url = urljoin(LIST_URL, href) if href else ""
        if not pdf_url:
            continue
        rows.append({"title": title, "publish_date": date_text, "pdf_url": pdf_url})
    return rows

# ---------- CSV management ----------
def ensure_csv_headers():
    mp = Path(MASTER_CSV)
    if not mp.exists() or mp.stat().st_size == 0:
        mp.parent.mkdir(parents=True, exist_ok=True)
        with mp.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(["title", "publish_date", "pdf_url"])
        print(f"Created master CSV with headers at {mp}")

def load_seen_ids():
    ensure_csv_headers()
    seen = set()
    mp = Path(MASTER_CSV)
    with mp.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("pdf_url"):
                seen.add(row["pdf_url"])
    return seen

def append_to_master(new_rows):
    mp = Path(MASTER_CSV)
    with mp.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for r in new_rows:
            w.writerow([r.get("title", ""), r.get("publish_date", ""), r.get("pdf_url", "")])
    print(f"Appended {len(new_rows)} rows to {mp}")

# ---------- JSON writing ----------
def safe_filename_from_url(u: str) -> str:
    return (u.split("/")[-1].split("?")[0]) if u else ""

def write_json(new_rows, out_path=JSON_OUT):
    items = []
    for r in new_rows:
        items.append({
            "name": safe_filename_from_url(r.get("pdf_url", "")),
            "title": r.get("title", ""),
            "publish_date": r.get("publish_date", ""),
            "pdf_url": r.get("pdf_url", "")
        })
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "count": len(items),
        "items": items
    }
    outp = Path(out_path)
    outp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote JSON with {len(items)} entries to {outp}")

# ---------- main ----------
def main():
    try:
        all_rows = scrape_all_rows()
    except Exception as e:
        print("Failed to scrape list page:", e, file=sys.stderr)
        raise SystemExit(1)

    print(f"Scraped rows this run: {len(all_rows)}")
    if not all_rows:
        print("No rows found; exiting.")
        raise SystemExit(0)

    seen = load_seen_ids()
    new_rows = [r for r in all_rows if r["pdf_url"] not in seen]
    print(f"New rows detected: {len(new_rows)}")

    if not new_rows:
        # Ensure JSON_out exists but with zero count (optional)
        write_json([], JSON_OUT)
        print("No new rows. Wrote empty JSON.")
        return

    append_to_master(new_rows)
    write_json(new_rows, JSON_OUT)
    print("Done.")

if __name__ == "__main__":
    main()
