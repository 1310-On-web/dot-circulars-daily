#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Minimal DoT watcher — writes CSV + JSON to repository root (no data/ folder).
Adds a safe 'pdf_filename' for each item and stores it in CSV + JSON.
"""

from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote
import csv
import json
import os
import re
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

# ---------- filename helpers ----------
def filename_from_url(url: str) -> str:
    """
    Try to get a filename from the URL path (last segment). Returns empty string if none.
    """
    if not url:
        return ""
    try:
        p = urlparse(url).path or ""
        name = unquote(p.split("/")[-1] or "")
        return name
    except Exception:
        return ""

def sanitize_name(name: str, max_len: int = 120) -> str:
    """
    Turn arbitrary text into a safe filename body (without extension).
    Keep alnum, underscore and hyphen. Collapse runs of invalid chars into underscore.
    Trim to max_len.
    """
    if not name:
        return ""
    # Normalize whitespace, replace with single space
    s = re.sub(r"\s+", " ", name).strip()
    # Replace non-alnum (allow - and _) with underscore
    s = re.sub(r"[^A-Za-z0-9\-_\. ]+", "", s)
    s = s.replace(" ", "_")
    if len(s) > max_len:
        s = s[:max_len]
    # Remove leading/trailing dots/underscores/hyphens
    s = s.strip("._-")
    return s or "document"

def ensure_unique_name(base_name: str, existing: set[str]) -> str:
    """
    If base_name already in existing, append -1, -2... before the extension.
    existing should contain full filenames (with extension).
    """
    if base_name not in existing:
        return base_name
    stem, dot, ext = base_name.rpartition(".")
    if not dot:
        stem = base_name
        ext = ""
    counter = 1
    while True:
        candidate = f"{stem}-{counter}.{ext}" if ext else f"{stem}-{counter}"
        if candidate not in existing:
            return candidate
        counter += 1

def make_pdf_filename(item: dict, existing_names: set[str]) -> str:
    """
    Given a scraped row (title, publish_date, pdf_url), produce a safe pdf filename.
    Uses URL filename if present; otherwise uses sanitized title + date.
    Ensures uniqueness against existing_names (set).
    Always returns a filename ending with .pdf
    """
    url = item.get("pdf_url", "") or ""
    url_name = filename_from_url(url)
    if url_name and url_name.lower().endswith(".pdf"):
        base = sanitize_name(url_name)
        if not base.lower().endswith(".pdf"):
            base = base + ".pdf"
    else:
        # Build from title + publish_date
        title = item.get("title", "") or "document"
        date_text = item.get("publish_date", "") or ""
        # Try to keep date compact (YYYYMMDD) if possible
        date_compact = re.sub(r"[^\d]", "", date_text)[:8]  # rough
        parts = [title]
        if date_compact:
            parts.append(date_compact)
        suggested = "_".join(parts)
        suggested = sanitize_name(suggested)
        if not suggested.lower().endswith(".pdf"):
            suggested = suggested + ".pdf"
        base = suggested

    # ensure unique
    final = ensure_unique_name(base, existing_names)
    existing_names.add(final)
    return final

# ---------- CSV management ----------
def ensure_csv_headers():
    mp = Path(MASTER_CSV)
    if not mp.exists() or mp.stat().st_size == 0:
        mp.parent.mkdir(parents=True, exist_ok=True)
        with mp.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(["title", "publish_date", "pdf_url", "pdf_filename"])
        print(f"Created master CSV with headers at {mp}")

def load_seen_ids_and_names():
    """
    Returns two sets:
      - seen_urls: pdf_url strings already present in csv
      - seen_names: pdf_filename strings already present in csv (if present)
    Handles older CSVs which might not have pdf_filename column.
    """
    ensure_csv_headers()
    seen_urls = set()
    seen_names = set()
    mp = Path(MASTER_CSV)
    with mp.open("r", encoding="utf-8", newline="") as f:
        dr = csv.DictReader(f)
        for row in dr:
            if row.get("pdf_url"):
                seen_urls.add(row["pdf_url"])
            # If csv has pdf_filename column, collect it
            if row.get("pdf_filename"):
                seen_names.add(row["pdf_filename"])
            else:
                # Try to infer from url if filename absent
                u = row.get("pdf_url", "")
                if u:
                    fn = filename_from_url(u)
                    if fn:
                        seen_names.add(sanitize_name(fn))
    return seen_urls, seen_names

def append_to_master(new_rows_with_names):
    mp = Path(MASTER_CSV)
    # If CSV existed but with older header (no pdf_filename), ensure we append correct columns.
    # We'll always write in order: title, publish_date, pdf_url, pdf_filename
    with mp.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for r in new_rows_with_names:
            w.writerow([r.get("title", ""), r.get("publish_date", ""), r.get("pdf_url", ""), r.get("pdf_filename", "")])
    print(f"Appended {len(new_rows_with_names)} rows to {mp}")

# ---------- JSON writing ----------
def write_json(new_rows_with_names, out_path=JSON_OUT):
    items = []
    for r in new_rows_with_names:
        items.append({
            "pdf_filename": r.get("pdf_filename", ""),
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

    seen_urls, seen_names = load_seen_ids_and_names()
    new_raw = [r for r in all_rows if r["pdf_url"] not in seen_urls]
    print(f"New rows detected: {len(new_raw)}")

    if not new_raw:
        # Optionally write empty JSON (you can change this behaviour)
        write_json([], JSON_OUT)
        print("No new rows. Wrote empty JSON.")
        return

    # Build pdf_filename for each new row, making sure filenames are unique
    new_with_names = []
    # Clone seen_names so we track duplicates across new items too
    existing_names = set(seen_names)
    for r in new_raw:
        fn = make_pdf_filename(r, existing_names)
        r2 = dict(r)
        r2["pdf_filename"] = fn
        new_with_names.append(r2)

    # Append to master CSV and write JSON
    append_to_master(new_with_names)
    write_json(new_with_names, JSON_OUT)
    print("Done.")

if __name__ == "__main__":
    main()
