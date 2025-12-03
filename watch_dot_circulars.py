#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DoT Circulars Hourly Watcher + OpenAI Summaries
-------------------------------------------------
- Scrapes DoT circulars
- Detects new items via master CSV
- Downloads new PDFs into data/pdfs/
- Extracts text and summarizes each new PDF with OpenAI
- Saves summaries in data/summaries/
- Builds email_body.txt for the mail step
-------------------------------------------------
Dependencies:
  pip install -r requirements.txt
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

# OpenAI settings
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

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
            w.writerow([r["title"], r["publish_date"], r["pdf_url"]])

# ---------- PDF download ----------

def safe_pdf_filename(pdf_url: str) -> str:
    tail = pdf_url.split("/")[-1].split("?")[0] or "document.pdf"
    return tail

def download_pdf(pdf_url, save_dir: Path) -> Path | None:
    filename = safe_pdf_filename(pdf_url)
    dest = save_dir / filename
    if dest.exists():
        print(f"Already downloaded: {dest.name}")
        return dest
    try:
        r = SESSION.get(pdf_url, headers=HEADERS, timeout=120, stream=True)
        r.raise_for_status()
        with open(dest, "wb") as f:
            shutil.copyfileobj(r.raw, f)
        print(f"Downloaded PDF: {dest.name}")
        return dest
    except Exception as e:
        print(f"Failed to download {pdf_url}: {e}")
        return None

# ---------- text extraction ----------

def extract_text_from_pdf(pdf_path: Path, max_chars: int = 80_000) -> str:
    text_parts = []
    try:
        with fitz.open(pdf_path) as doc:
            for page in doc:
                text_parts.append(page.get_text())
                if sum(len(x) for x in text_parts) >= max_chars:
                    break
    except Exception as e:
        print(f"Text extraction failed for {pdf_path.name}: {e}")
        return ""
    text = "\n".join(text_parts)
    return text[:max_chars]

def chunk_text(s: str, chunk_size: int = 6000, overlap: int = 400) -> list[str]:
    if not s:
        return []
    chunks = []
    i = 0
    n = len(s)
    while i < n:
        j = min(i + chunk_size, n)
        chunks.append(s[i:j])
        i = j - overlap if j < n else j
        if i < 0:
            i = 0
    return chunks

# ---------- OpenAI summarization ----------

def summarize_with_openai(pdf_text: str) -> str:
    """
    Map-reduce style summary via OpenAI (chat.completions).
    Uses OPENAI_MODEL (default gpt-4o-mini).
    """
    if not OPENAI_API_KEY:
        print("OPENAI_API_KEY missing; skipping summary.")
        return ""

    # lazy import so the script still runs without the package locally
    try:
        from openai import OpenAI
    except Exception as e:
        print(f"OpenAI SDK not available: {e}")
        return ""

    client = OpenAI(api_key=OPENAI_API_KEY)

    blocks = chunk_text(pdf_text, chunk_size=6000, overlap=400)
    if not blocks:
        return ""

    partials = []
    for idx, block in enumerate(blocks, 1):
        prompt = (
            "You are summarizing an official Indian government circular. "
            "Write clear, neutral bullet points focusing on: subject/purpose, key directives, "
            "effective dates/deadlines, compliance obligations, impacted entities, and penalties if any. "
            "Avoid fluff; cite clause/section numbers only if present.\n\n"
            f"TEXT (chunk {idx}/{len(blocks)}):\n{block}"
        )
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            partials.append(resp.choices[0].message.content.strip())
        except Exception as e:
            print(f"OpenAI partial summary failed on chunk {idx}: {e}")

    if not partials:
        return ""

    combined = "\n".join(partials)
    final_prompt = (
        "Combine the bullet points below into a final concise brief of 5–8 bullets. "
        "Keep statutory references concise; ensure any dates or compliance actions are prominent.\n\n"
        f"POINTS:\n{combined}"
    )
    try:
        final = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": final_prompt}],
            temperature=0.2,
        )
        return final.choices[0].message.content.strip()
    except Exception as e:
        print(f"OpenAI final summary failed: {e}")
        return ""

def save_summary(pdf_path: Path, summary: str) -> Path | None:
    if not summary:
        return None
    out = SUM_DIR / (pdf_path.stem + ".summary.txt")
    out.write_text(summary, encoding="utf-8")
    return out

# ---------- email body ----------

def write_email_body(new_rows, downloaded_paths, summaries_map):
    """
    Build the email body including:
    - Title
    - Publish date
    - Original PDF URL
    - AI summary (if available)
    - Saved filename
    """
    lines = ["New DoT Circulars detected:\n"]
    name_to_summary = {name: summ for name, summ in summaries_map.items()}

    for idx, r in enumerate(new_rows, 1):
        pdf_name = r["pdf_url"].split("/")[-1].split("?")[0]
        summary = (name_to_summary.get(pdf_name) or "").strip()

        lines.append(f"{idx}. {r['title']}")
        lines.append(f"   Date: {r['publish_date']}")
        lines.append(f"   PDF (original): {r['pdf_url']}")

        if summary:
            if len(summary) > 2000:
                summary = summary[:2000].rstrip() + " …"
            lines.append("   AI Summary:")
            for line in summary.splitlines():
                lines.append(f"     {line}")
        else:
            lines.append("   AI Summary: (unavailable)")

        lines.append(f"   Saved file: {pdf_name}")
        lines.append("")

    if downloaded_paths:
        lines.append("Downloaded files this run:")
        for p in downloaded_paths:
            lines.append(f" - {p.name}")

    EMAIL_BODY_PATH.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    print(f"Written email body for {len(new_rows)} new circular(s).")

# ---------- GH outputs ----------

def set_output(name, value):
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")

# ---------- main ----------

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
        text = extract_text_from_pdf(p)
        summary = summarize_with_openai(text)
        if summary:
            summaries_map[p.name] = summary
            save_summary(p, summary)

    print("Summaries generated for files:", list(summaries_map.keys()))
    write_email_body(new_rows, downloaded_paths, summaries_map)

    set_output("has_new", "true")
    set_output("subject_suffix", f"{len(new_rows)} new")
    print(
        f"Appended {len(new_rows)} new rows, downloaded {len(downloaded_paths)} PDFs, "
        f"summarized {len(summaries_map)}."
    )
