#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DoT Circulars Hourly Watcher + Gemini Summaries
-------------------------------------------------
- Scrapes DoT circulars
- Detects new items
- Downloads PDFs
- Summarizes each new circular using Google Gemini
- Saves summaries and builds email body

Dependencies:
    pip install requests beautifulsoup4 lxml urllib3 PyMuPDF google-generativeai
"""

import csv
import os
import shutil
from pathlib import Path
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import fitz  # PyMuPDF
import google.generativeai as genai


# -------------------- CONFIG --------------------

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
PDF_DIR = DATA_DIR / "pdfs"
SUM_DIR = DATA_DIR / "summaries"
MASTER_CSV = DATA_DIR / "dot_circulars_master.csv"
EMAIL_BODY_PATH = Path("email_body.txt")

DATA_DIR.mkdir(parents=True, exist_ok=True)
PDF_DIR.mkdir(parents=True, exist_ok=True)
SUM_DIR.mkdir(parents=True, exist_ok=True)

# Gemini model setup
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
MODEL_NAME = os.environ.get("GEMINI_MODEL", "Gemini 2.5 Flash-Lite")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)


# -------------------- SESSION BUILDER --------------------

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


# -------------------- SCRAPER CORE --------------------

def get_soup(url):
    r = SESSION.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def scrape_all_rows():
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

        rows.append({
            "title": title,
            "publish_date": date_text,
            "pdf_url": pdf_url
        })
    return rows


# -------------------- CSV MANAGEMENT --------------------

def ensure_csv_headers():
    if not MASTER_CSV.exists() or MASTER_CSV.stat().st_size == 0:
        MASTER_CSV.parent.mkdir(parents=True, exist_ok=True)
        with MASTER_CSV.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(["title", "publish_date", "pdf_url"])
        print("Created master CSV with headers.")


def load_seen_ids():
    ensure_csv_headers()
    seen = set()
    with MASTER_CSV.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if row.get("pdf_url"):
                seen.add(row["pdf_url"])
    return seen


def append_to_master(new_rows):
    ensure_csv_headers()
    with MASTER_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for r in new_rows:
            w.writerow([r["title"], r["publish_date"], r["pdf_url"]])


# -------------------- PDF DOWNLOADER --------------------

def download_pdf(pdf_url, save_dir):
    filename = pdf_url.split("/")[-1].split("?")[0] or "document.pdf"
    dest = save_dir / filename
    if dest.exists():
        print(f"Already downloaded: {dest.name}")
        return dest
    try:
        r = SESSION.get(pdf_url, headers=HEADERS, timeout=60, stream=True)
        r.raise_for_status()
        with open(dest, "wb") as f:
            shutil.copyfileobj(r.raw, f)
        print(f"Downloaded PDF: {dest.name}")
        return dest
    except Exception as e:
        print(f"Failed to download {pdf_url}: {e}")
        return None


# -------------------- TEXT EXTRACTION --------------------

def extract_text(pdf_path, max_chars=80000):
    """Extract text from a PDF using PyMuPDF."""
    text = ""
    with fitz.open(pdf_path) as doc:
        for page in doc:
            text += page.get_text()
            if len(text) >= max_chars:
                break
    return text[:max_chars]


# -------------------- GEMINI SUMMARIZATION --------------------

def summarize_with_gemini(pdf_text):
    """Use Google Gemini to summarize circulars."""
    if not GEMINI_API_KEY:
        print("Gemini API key missing, skipping summarization.")
        return ""

    prompt = f"""
You are summarizing an official Indian government circular. 
Write 5–8 concise bullet points that capture:
- the subject and purpose
- key directives or rules
- effective dates or deadlines
- obligations for stakeholders
- and any notable context.

Keep the tone neutral and formal.
Text to summarize:
{text if len(pdf_text) < 30000 else pdf_text[:30000]}
"""
    try:
        model = genai.GenerativeModel(MODEL_NAME)
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"Gemini summarization failed: {e}")
        return ""


def save_summary(pdf_path, summary):
    if not summary:
        return None
    out = SUM_DIR / (pdf_path.stem + ".summary.txt")
    out.write_text(summary, encoding="utf-8")
    return out


# -------------------- EMAIL BODY --------------------

def write_email_body(new_rows, downloaded_paths, summaries_map):
    lines = ["New DoT Circulars detected:\n"]
    for i, r in enumerate(new_rows, 1):
        pdf_name = r["pdf_url"].split("/")[-1].split("?")[0]
        lines.append(f"{i}. {r['title']}\n"
                     f"   Date: {r['publish_date']}\n"
                     f"   PDF:  {r['pdf_url']}")
        summ = summaries_map.get(pdf_name, "")
        if summ:
            lines.append("   Summary:\n" + "\n".join(f"     {l}" for l in summ.splitlines()))
        lines.append("")
    lines.append("\nDownloaded files:")
    for p in downloaded_paths:
        lines.append(f" - {p.name}")
    EMAIL_BODY_PATH.write_text("\n".join(lines), encoding="utf-8")


# -------------------- MAIN --------------------

def set_output(name, value):
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")


if __name__ == "__main__":
    all_rows = scrape_all_rows()
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

    append_to_master(new_rows)

    downloaded_paths = []
    for r in new_rows:
        p = download_pdf(r["pdf_url"], PDF_DIR)
        if p:
            downloaded_paths.append(p)

    summaries_map = {}
    for p in downloaded_paths:
        text = extract_text(p)
        summary = summarize_with_gemini(text)
        if summary:
            summaries_map[p.name] = summary
            save_summary(p, summary)

    write_email_body(new_rows, downloaded_paths, summaries_map)

    set_output("has_new", "true")
    set_output("subject_suffix", f"{len(new_rows)} new")
    print(f"Appended {len(new_rows)} new rows, downloaded {len(downloaded_paths)} PDFs, summarized {len(summaries_map)}.")
