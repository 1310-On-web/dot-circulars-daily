#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Minimal DoT watcher — writes CSV + JSON to repository root (no data/ folder).
Adds a safe 'pdf_filename' for each item and stores it in CSV + JSON.
Dates are normalized to mm/dd/yyyy. Each row gets a sequential 'id'.
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

# ---------- date helpers ----------
def normalize_date_mmddyyyy(date_text: str) -> str:
    """
    Try to parse a variety of date strings and return as mm/dd/yyyy.
    If parsing fails, return the original string.
    """
    if not date_text:
        return date_text
    candidates = [
        "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%d %b %Y", "%d %B %Y",
        "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%m-%d-%Y"
    ]
    s = date_text.strip()
    # Common fix: sometimes has extra spaces or commas
    s = re.sub(r"[,\u00A0]+", " ", s).strip()
    for fmt in candidates:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%m/%d/%Y")
        except ValueError:
            continue
    # Fallback: try to extract digits and guess dd/mm/yyyy vs yyyy-mm-dd
    digits = re.findall(r"\d+", s)
    # Try YYYY MM DD
    if len(digits) >= 3 and len(digits[0]) == 4:
        try:
            dt = datetime(int(digits[0]), int(digits[1]), int(digits[2]))
            return dt.strftime("%m/%d/%Y")
        except Exception:
            pass
    # Try DD MM YYYY
    if len(digits) >= 3 and len(digits[-1]) == 4:
        try:
            d, m, y = int(digits[0]), int(digits[1]), int(digits[2])
            dt = datetime(y, m, d)
            return dt.strftime("%m/%d/%Y")
        except Exception:
            pass
    return date_text  # give up, keep original

# ---------- scraping ----------
def get_soup(url):
    r = SESSION.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def scrape_all_rows():
    """
    Return list of dicts: {'title', 'publish_date', 'pdf_url'}
    publish_date is normalized to mm/dd/yyyy if possible.
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
        date_us = normalize_date_mmddyyyy(date_text)
        href = a.get("href", "")
        pdf_url = urljoin(LIST_URL, href) if href else ""
        if not pdf_url:
            continue
        rows.append({"title": title, "publish_date": date_us, "pdf_url": pdf_url})
    return rows

# ---------- filename helpers ----------
def filename_from_url(url: str) -> str:
    if not url:
        return ""
    try:
        p = urlparse(url).path or ""
        name = unquote(p.split("/")[-1] or "")
        return name
    except Exception:
        return ""

def sanitize_name(name: str, max_len: int = 120) -> str:
    if not name:
        return ""
    s = re.sub(r"\s+", " ", name).strip()
    s = re.sub(r"[^A-Za-z0-9\-_\. ]+", "", s)
    s = s.replace(" ", "_")
    if len(s) > max_len:
        s = s[:max_len]
    s = s.strip("._-")
    return s or "document"

def ensure_unique_name(base_name: str, existing: set[str]) -> str:
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
    url = item.get("pdf_url", "") or ""
    url_name = filename_from_url(url)
    if url_name and url_name.lower().endswith(".pdf"):
        base = sanitize_name(url_name)
        if not base.lower().endswith(".pdf"):
            base = base + ".pdf"
    else:
        title = item.get("title", "") or "document"
        date_text = item.get("publish_date", "") or ""
        date_compact = ""
        try:
            # if already normalized, parse back to compact; else strip non-digits
            dt = datetime.strptime(date_text, "%m/%d/%Y")
            date_compact = dt.strftime("%Y%m%d")
        except Exception:
            date_compact = re.sub(r"[^\d]", "", date_text)[:8]
        parts = [title]
        if date_compact:
            parts.append(date_compact)
        suggested = "_".join(parts)
        suggested = sanitize_name(suggested)
        if not suggested.lower().endswith(".pdf"):
            suggested = suggested + ".pdf"
        base = suggested

    final = ensure_unique_name(base, existing_names)
    existing_names.add(final)
    return final

# ---------- CSV management ----------
def ensure_csv_headers():
    mp = Path(MASTER_CSV)
    if not mp.exists() or mp.stat().st_size == 0:
        mp.parent.mkdir(parents=True, exist_ok=True)
        with mp.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(["id", "title", "publish_date", "pdf_url", "pdf_filename"])
        print(f"Created master CSV with headers at {mp}")
        return

    # If file exists but older header (missing 'id' or 'pdf_filename'), ensure we at least keep writing new rows
    with mp.open("r", encoding="utf-8", newline="") as f:
        try:
            dr = csv.reader(f)
            header = next(dr, [])
        except Exception:
            header = []
    required = ["id", "title", "publish_date", "pdf_url", "pdf_filename"]
    if header != required:
        # Re-write header if missing columns (non-destructive for existing rows on append)
        # We won't rewrite the file; we'll just ensure our appends have the right order.
        pass

def load_seen_ids_and_names_and_next_id():
    """
    Returns:
      - seen_urls: set(pdf_url)
      - seen_names: set(pdf_filename)
      - next_id: int (max existing id + 1, or row_count+1 if no id column)
    """
    ensure_csv_headers()
    seen_urls = set()
    seen_names = set()
    max_id = 0
    row_count = 0
    mp = Path(MASTER_CSV)
    with mp.open("r", encoding="utf-8", newline="") as f:
        dr = csv.DictReader(f)
        has_id = "id" in (dr.fieldnames or [])
        for row in dr:
            row_count += 1
            if row.get("pdf_url"):
                seen_urls.add(row["pdf_url"])
            if row.get("pdf_filename"):
                seen_names.add(row["pdf_filename"])
            else:
                u = row.get("pdf_url", "")
                if u:
                    fn = filename_from_url(u)
                    if fn:
                        seen_names.add(sanitize_name(fn))
            if has_id:
                try:
                    max_id = max(max_id, int(row.get("id", "0") or 0))
                except Exception:
                    pass
    next_id = (max_id + 1) if max_id > 0 else (row_count + 1)
    return seen_urls, seen_names, next_id

def append_to_master(new_rows_with_names_and_ids):
    mp = Path(MASTER_CSV)
    with mp.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for r in new_rows_with_names_and_ids:
            w.writerow([
                r.get("id", ""),
                r.get("title", ""),
                r.get("publish_date", ""),
                r.get("pdf_url", ""),
                r.get("pdf_filename", "")
            ])
    print(f"Appended {len(new_rows_with_names_and_ids)} rows to {mp}")

# ---------- JSON writing ----------
def write_json(new_rows_with_names_and_ids, out_path=JSON_OUT):
    items = []
    for r in new_rows_with_names_and_ids:
        items.append({
            "id": r.get("id", ""),
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

    seen_urls, seen_names, next_id = load_seen_ids_and_names_and_next_id()
    new_raw = [r for r in all_rows if r["pdf_url"] not in seen_urls]
    print(f"New rows detected: {len(new_raw)}")

    if not new_raw:
        write_json([], JSON_OUT)
        print("No new rows. Wrote empty JSON.")
        return

    # Build pdf_filename + id for each new row
    new_enriched = []
    existing_names = set(seen_names)
    current_id = next_id
    for r in new_raw:
        fn = make_pdf_filename(r, existing_names)
        r2 = dict(r)
        r2["pdf_filename"] = fn
        r2["id"] = current_id
        current_id += 1
        new_enriched.append(r2)

    append_to_master(new_enriched)
    write_json(new_enriched, JSON_OUT)
    print("Done.")

if __name__ == "__main__":
    main()
