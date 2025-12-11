#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DoT Circulars watcher (minimal)
--------------------------------
- Scrapes DoT circulars list page
- Compares against a master CSV to detect new items
- Appends new items to master CSV
- Writes dot_new_entries.json containing metadata for all new items
- Does NOT download PDFs or call OpenAI
--------------------------------
"""

from pathlib import Path
from urllib.parse import urljoin
import csv
import json
import os
import sys

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------- CONFIG ----------
BASE = "https://dot.gov.in"
LIST_URL = "https://dot.gov.in/all-circulars"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Connection": "close",
}

MASTER_CSV = "dot_circulars_master.csv"
JSON_OUT = "dot_new_entries.json"

# ---------- HTTP session ----------
def build_session():
    s = requests.Session()
    retry = Retry(
        total=6, connect=6, read=6,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD", "GET"]),
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
    (mirrors your old logic; returns only rows that have a Download link)
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
    """
    Master CSV will have these columns by default:
    title,publish_date,pdf_url
    If CSV missing or empty, create with headers.
    """
    if not MASTER_CSV.exists() or MASTER_CSV.stat().st_size == 0:
        MASTER_CSV.parent.mkdir(parents=True, exist_ok=True)
        with MASTER_CSV.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(["title", "publish_date", "pdf_url"])
        print("Created master CSV with headers.")

def load_seen_ids():
    ensure_csv_headers()
    seen = set()
    with MASTER_CSV.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("pdf_url"):
                seen.add(row["pdf_url"])
    return seen

def append_to_master(new_rows):
    ensure_csv_headers()
    with MASTER_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for r in new_rows:
            # Keep same column order as header: title, publish_date, pdf_url
            w.writerow([r.get("title", ""), r.get("publish_date", ""), r.get("pdf_url", "")])

# ---------- utility ----------
def safe_pdf_filename(pdf_url: str) -> str:
    tail = pdf_url.split("/")[-1].split("?")[0] or "document.pdf"
    return tail

# ---------- JSON writer ----------
def write_json(new_rows, out_path: Path):
    """
    Writes a JSON array of objects. Each object includes:
      - name: safe filename extracted from pdf_url
      - title
      - publish_date
      - pdf_url
    Also includes a top-level metadata block with count and timestamp.
    """
    items = []
    for r in new_rows:
        name = safe_pdf_filename(r.get("pdf_url", ""))
        items.append({
            "name": name,
            "title": r.get("title", ""),
            "publish_date": r.get("publish_date", ""),
            "pdf_url": r.get("pdf_url", "")
        })

    payload = {
        "generated_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "count": len(items),
        "items": items
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(items)} entries to {out_path}")

# ---------- GH outputs ----------
def set_output(name, value):
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")

# ---------- main ----------
def main():
    try:
        all_rows = scrape_all_rows()
    except Exception as e:
        print("Failed to scrape list page:", e, file=sys.stderr)
        set_output("has_new", "false")
        raise SystemExit(1)

    print(f"Scraped rows this run: {len(all_rows)}")
    if not all_rows:
        set_output("has_new", "false")
        raise SystemExit(0)

    seen = load_seen_ids()
    new_rows = [r for r in all_rows if r["pdf_url"] not in seen]
    print(f"New rows detected: {len(new_rows)}")

    if not new_rows:
        set_output("has_new", "false")
        raise SystemExit(0)

    # Append new rows to master (we do not download summaries etc.)
    append_to_master(new_rows)

    # Create JSON for Power Automate (or any downstream consumer)
    write_json(new_rows, JSON_OUT)

    # GitHub outputs for later steps
    set_output("has_new", "true")
    set_output("new_count", str(len(new_rows)))
    print(f"Appended {len(new_rows)} new rows and wrote JSON to {JSON_OUT}")

if __name__ == "__main__":
    main()

