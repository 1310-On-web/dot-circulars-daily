#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DoT All Circulars (table parser) -> circulars.csv
Columns: title, publish_date, pdf_url

Deps:
  pip install requests beautifulsoup4 lxml
"""

import csv
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

def scrape_table_last_10():
    soup = get_soup(LIST_URL)

    # Find all rows that have a "Download" link (that cell holds the PDF)
    # BeautifulSoup supports the :-soup-contains() pseudo-class (bs4 >= 4.7)
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

        rows.append({
            "title": title,
            "publish_date": date_text,
            "pdf_url": pdf_url
        })

    return rows[:10]

def write_csv(rows, path="circulars.csv"):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["title", "publish_date", "pdf_url"])
        for r in rows:
            w.writerow([r["title"], r["publish_date"], r["pdf_url"]])
    return path

if __name__ == "__main__":
    data = scrape_table_last_10()
    out = write_csv(data, "circulars.csv")
    print(f"Wrote {len(data)} rows to {out}")
