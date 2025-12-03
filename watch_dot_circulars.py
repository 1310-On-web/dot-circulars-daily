#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DoT Circulars hourly watcher
- Scrapes the All Circulars page
- Compares against persistent CSV: data/dot_circulars_master.csv
- Appends any NEW items
- Writes an email body listing new items
- Exposes GitHub Actions outputs: has_new=true/false

Unique key used for de-duplication: pdf_url
"""

import csv
import os
from pathlib import Path
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

BASE = "https://dot.gov.in"
LIST_URL = "https://dot.gov.in/all-circulars"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Connection": "close",
    "Upgrade-Insecure-Requests": "1",
}

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
MASTER_CSV = DATA_DIR / "dot_circulars_master.csv"
EMAIL_BODY_PATH = Path("email_body.txt")


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


def get_soup(url):
    r = SESSION.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def scrape_all_rows():
    """
    Scrape every row on the listing page that has a 'Download' link.
    Returns list of dicts: {title, publish_date, pdf_url}
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

        title = tds[1].get_text(strip=True)               # ORDER/CIRCULAR NAME
        date_text = tds[-1].get_text(strip=True)          # Dated
        href = a.get("href", "")
        pdf_url = urljoin(LIST_URL, href) if href else ""

        if not pdf_url:
            continue

        rows.append({
            "title": title,
            "publish_date": date_text,
            "pdf_url": pdf_url
        })
    return rows


def load_seen_ids():
    seen = set()
    if MASTER_CSV.exists():
        with MASTER_CSV.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                if row.get("pdf_url"):
                    seen.add(row["pdf_url"])
    return seen


def append_to_master(new_rows):
    # Write header if file doesn't exist OR is empty
    write_header = not MASTER_CSV.exists() or MASTER_CSV.stat().st_size == 0
    with MASTER_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["title", "publish_date", "pdf_url"])
        for r in new_rows:
            w.writerow([r["title"], r["publish_date"], r["pdf_url"]])



def write_email_body(new_rows):
    lines = [
        "New DoT Circulars detected:\n",
    ]
    for i, r in enumerate(new_rows, 1):
        lines.append(f"{i}. {r['title']}\n"
                     f"   Date: {r['publish_date']}\n"
                     f"   PDF:  {r['pdf_url']}\n")
    EMAIL_BODY_PATH.write_text("\n".join(lines), encoding="utf-8")


def set_output(name, value):
    """
    Write a GitHub Actions output.
    """
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")

# ... existing imports and code ...

if __name__ == "__main__":
    all_rows = scrape_all_rows()
    print(f"Scraped rows this run: {len(all_rows)}")

    if not all_rows:
        print("WARNING: Scrape returned zero rows (site layout change?).")
        set_output("has_new", "false")
        raise SystemExit(0)

    seen = load_seen_ids()
    print(f"Already in master CSV: {len(seen)}")

    new_rows = [r for r in all_rows if r["pdf_url"] not in seen]
    print(f"New rows detected: {len(new_rows)}")

    if not new_rows:
        print("No new circulars.")
        set_output("has_new", "false")
        raise SystemExit(0)

    append_to_master(new_rows)
    write_email_body(new_rows)
    set_output("has_new", "true")
    set_output("subject_suffix", f"{len(new_rows)} new")
    print(f"Appended {len(new_rows)} new rows to {MASTER_CSV}")

# if __name__ == "__main__":
#     all_rows = scrape_all_rows()
#     if not all_rows:
#         print("WARNING: Scrape returned zero rows (site layout change?).")
#         set_output("has_new", "false")
#         raise SystemExit(0)

#     seen = load_seen_ids()
#     new_rows = [r for r in all_rows if r["pdf_url"] not in seen]

#     if not new_rows:
#         print("No new circulars.")
#         set_output("has_new", "false")
#         raise SystemExit(0)

#     # Optional: newest first (as they appear on page)
#     append_to_master(new_rows)
#     write_email_body(new_rows)

#     # Expose outputs to the workflow
#     set_output("has_new", "true")
#     # Also expose a tiny one-line subject
#     set_output("subject_suffix", f"{len(new_rows)} new")
#     print(f"Appended {len(new_rows)} new rows to {MASTER_CSV}")
