#!/usr/bin/env python3
"""
peopleadmin_scraper.py

Scrape job postings from PeopleAdmin installations (common at colleges).

Input: a text file with one school domain per line (e.g., unc.edu).
The script tries multiple likely base URL patterns for each domain, then queries:
  /postings/search?page=1&query=<term>

Optional:
  --details     Fetch each posting detail page for salary/dates/etc.
  --store-html  Store raw detail HTML too (can get large)

Outputs JSONL (one posting per line) and a summary to stdout.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Optional, Dict, List, Set, Tuple
from urllib.parse import urljoin, urlencode, urlparse

import requests
from bs4 import BeautifulSoup


# -----------------------------
# Data model
# -----------------------------

@dataclass
class Posting:
    school_input: str
    base_url: str
    query: str
    title: str
    url: str
    location: Optional[str] = None
    department: Optional[str] = None
    posting_id: Optional[str] = None

    # detail fields (best-effort)
    salary: Optional[str] = None
    salary_min: Optional[str] = None
    salary_max: Optional[str] = None
    posted_date: Optional[str] = None
    close_date: Optional[str] = None
    open_until_filled: Optional[bool] = None
    employment_type: Optional[str] = None
    time_limit: Optional[str] = None
    full_time_or_part_time: Optional[str] = None
    special_instructions: Optional[str] = None

    # store whole posting content for later parsing/search
    detail_text: Optional[str] = None
    detail_html: Optional[str] = None  # optional, can be large


# -----------------------------
# Config / constants
# -----------------------------

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PeopleAdminScraper/1.2; +https://example.com/bot-info)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

BASE_PATTERNS = [
    "https://jobs.{domain}",
    "https://careers.{domain}",
    "https://hr.{domain}",
    "https://{domain}",
    # occasionally PeopleAdmin subdomain directly
    "https://{sub}.peopleadmin.com",
    "https://{sub}.peopleadmin.edu",
    "https://{sub}.peopleadmin.net",
]

PEOPLEADMIN_FINGERPRINTS = [
    "peopleadmin",
    "/postings/search",
    "applicant tracking",
    "postings",
]


# -----------------------------
# Text / label helpers
# -----------------------------

def clean_text(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").split()).strip()

def norm_label(s: str) -> str:
    s = clean_text(s).lower()
    s = re.sub(r"[:\s]+$", "", s)
    return s

def extract_visible_text(soup: BeautifulSoup) -> str:
    """
    Extract readable page text, removing scripts/styles/noscript.
    Returns newline-separated text with blank lines collapsed.
    """
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator="\n", strip=True)
    lines = [clean_text(line) for line in text.splitlines()]
    lines = [ln for ln in lines if ln]
    return "\n".join(lines)

def parse_bool(val: str) -> Optional[bool]:
    v = clean_text(val).lower()
    if v in ("yes", "y", "true"):
        return True
    if v in ("no", "n", "false"):
        return False
    if "open until filled" in v:
        return True
    return None


# Map “label text on page” -> Posting field name
LABEL_MAP: Dict[str, str] = {
    # salary-ish
    "salary": "salary",
    "minimum salary": "salary_min",
    "min salary": "salary_min",
    "salary minimum": "salary_min",
    "maximum salary": "salary_max",
    "max salary": "salary_max",
    "salary maximum": "salary_max",
    "hiring range": "salary",
    "salary range": "salary",
    "anticipated hiring range": "salary",
    "pay grade": "salary",
    "compensation": "salary",

    # dates
    "posted date": "posted_date",
    "posting date": "posted_date",
    "date posted": "posted_date",
    "open date": "posted_date",
    "closing date": "close_date",
    "close date": "close_date",
    "application deadline": "close_date",
    "review date": "close_date",

    # misc
    "employment type": "employment_type",
    "position type": "employment_type",
    "time-limited": "time_limit",
    "time limited": "time_limit",
    "full-time/part-time": "full_time_or_part_time",
    "full time or part time": "full_time_or_part_time",
    "special instructions to applicants": "special_instructions",
    "open until filled": "open_until_filled",
}


# -----------------------------
# Salary normalization
# -----------------------------

def extract_salary_numbers(s: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract all $ amounts from a string; return (min_salary, max_salary) as formatted strings.
    Examples:
      "$80,000" -> ("$80,000", "$80,000")
      "$30,000 to $40,000" -> ("$30,000", "$40,000")
      "$22.50/hour - $25.00/hour" -> ("$22", "$25")  (rounded)
    """
    if not s:
        return None, None

    matches = re.findall(r"\$[\d,]+(?:\.\d{1,2})?", s)
    if not matches:
        return None, None

    values: List[float] = []
    for m in matches:
        num = m.replace("$", "").replace(",", "")
        try:
            values.append(float(num))
        except ValueError:
            continue

    if not values:
        return None, None

    mn = min(values)
    mx = max(values)

    return f"${mn:,.0f}", f"${mx:,.0f}"


# -----------------------------
# Misc utility
# -----------------------------

def normalize_domain(s: str) -> str:
    s = s.strip()
    s = re.sub(r"^https?://", "", s, flags=re.I)
    s = s.split("/")[0]
    return s.lower()

def domain_to_sub(domain: str) -> str:
    parts = domain.split(".")
    return parts[0] if parts else domain

def polite_sleep(min_s: float, max_s: float) -> None:
    time.sleep(random.uniform(min_s, max_s))

def is_likely_peopleadmin(html: str, final_url: str) -> bool:
    text = (html or "").lower()
    u = (final_url or "").lower()

    hits = 0
    for f in PEOPLEADMIN_FINGERPRINTS:
        if f in text or f in u:
            hits += 1

    return hits >= 2 or ("/postings" in u and "search" in u)

def build_search_url(base_url: str, query: str, page: int) -> str:
    path = "/postings/search"
    params = {"page": page, "query": query}
    return f"{base_url.rstrip('/')}{path}?{urlencode(params)}"

def fetch(session: requests.Session, url: str, timeout: float) -> Tuple[Optional[requests.Response], Optional[str]]:
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        return resp, None
    except requests.RequestException as e:
        return None, str(e)

def candidate_bases_for_domain(domain: str) -> List[str]:
    d = normalize_domain(domain)
    sub = domain_to_sub(d)
    bases: List[str] = []
    for pat in BASE_PATTERNS:
        bases.append(pat.format(domain=d, sub=sub))

    # de-dupe while preserving order
    seen = set()
    out = []
    for b in bases:
        if b not in seen:
            out.append(b)
            seen.add(b)
    return out


# -----------------------------
# Base discovery
# -----------------------------

def choose_working_base(session: requests.Session, domain: str, query: str, timeout: float) -> Optional[str]:
    """
    Try candidate bases. Return the first base that looks like PeopleAdmin search works.
    Normalizes base to scheme://netloc of the final landing page.
    """
    for base in candidate_bases_for_domain(domain):
        test_url = build_search_url(base, query=query, page=1)
        resp, _err = fetch(session, test_url, timeout=timeout)
        if resp is None:
            continue

        if resp.status_code in (401, 403, 404, 429) or resp.status_code >= 500:
            continue

        html = resp.text or ""
        final_url = str(resp.url)

        if is_likely_peopleadmin(html, final_url):
            parsed = urlparse(final_url)
            return f"{parsed.scheme}://{parsed.netloc}"

        polite_sleep(0.05, 0.2)

    return None


# -----------------------------
# Search page parsing
# -----------------------------

def extract_postings_from_search_html(
    html: str,
    base_url: str,
    school_input: str,
    query: str
) -> List[Posting]:
    soup = BeautifulSoup(html, "html.parser")
    postings: List[Posting] = []

    link_candidates = soup.select('a[href*="/postings/"]')
    seen_urls: Set[str] = set()

    for a in link_candidates:
        href = (a.get("href", "") or "").strip()
        if not href:
            continue

        if "/postings/search" in href:
            continue
        if re.search(r"/postings/(search|create|new)\b", href):
            continue

        m = re.search(r"/postings/(\d+)", href)
        if not m:
            continue

        abs_url = urljoin(base_url, href)
        if abs_url in seen_urls:
            continue
        seen_urls.add(abs_url)

        title = " ".join((a.get_text(" ", strip=True) or "").split())
        if not title or len(title) < 3:
            title = (a.get("aria-label") or a.get("title") or "").strip()

        posting_id = m.group(1)

        location = None
        department = None

        container_text = ""
        parent = a
        for _ in range(5):
            parent = parent.parent if parent else None
            if not parent:
                break
            container_text = " ".join(parent.get_text(" ", strip=True).split())
            if re.search(r"\bLocation\b|\bDepartment\b", container_text, flags=re.I):
                break

        if container_text:
            loc_m = re.search(r"Location[:\s]+(.+?)(?:\s{2,}|Department[:\s]|$)", container_text, flags=re.I)
            dept_m = re.search(r"Department[:\s]+(.+?)(?:\s{2,}|Location[:\s]|$)", container_text, flags=re.I)
            if loc_m:
                location = loc_m.group(1).strip(" -|")
            if dept_m:
                department = dept_m.group(1).strip(" -|")

        postings.append(
            Posting(
                school_input=school_input,
                base_url=base_url,
                query=query,
                title=title or f"Posting {posting_id}",
                url=abs_url,
                location=location,
                department=department,
                posting_id=posting_id,
            )
        )

    return postings


# -----------------------------
# Detail page parsing (label/value heuristics)
# -----------------------------

def extract_kv_from_dt_dd(soup: BeautifulSoup) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for dt in soup.find_all("dt"):
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        k = norm_label(dt.get_text(" ", strip=True))
        v = clean_text(dd.get_text(" ", strip=True))
        if k and v:
            out[k] = v
    return out

def extract_kv_from_tables(soup: BeautifulSoup) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for tr in soup.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        k = norm_label(th.get_text(" ", strip=True))
        v = clean_text(td.get_text(" ", strip=True))
        if k and v:
            out[k] = v
    return out

def extract_kv_from_label_value_blocks(soup: BeautifulSoup) -> Dict[str, str]:
    """
    Heuristic: find elements that look like labels, then grab adjacent/parent value.
    """
    out: Dict[str, str] = {}
    candidates = soup.find_all(["div", "span", "strong", "label"])
    for el in candidates:
        t = clean_text(el.get_text(" ", strip=True))
        if not t or len(t) > 70:
            continue
        k = norm_label(t)
        if k in LABEL_MAP:
            v = ""
            sib = el.find_next_sibling()
            if sib:
                v = clean_text(sib.get_text(" ", strip=True))
            if not v:
                parent = el.parent
                if parent:
                    pt = clean_text(parent.get_text(" ", strip=True))
                    v = clean_text(pt.replace(t, ""))
            if v:
                out[k] = v
    return out

def scrape_detail_page(
    session: requests.Session,
    posting: Posting,
    timeout: float,
    store_html: bool,
) -> Posting:
    resp, _err = fetch(session, posting.url, timeout=timeout)
    if resp is None:
        return posting
    if resp.status_code in (401, 403, 404, 429) or resp.status_code >= 500:
        return posting

    html = resp.text or ""
    soup = BeautifulSoup(html, "html.parser")

    # Store whole page content
    if store_html:
        posting.detail_html = html
    posting.detail_text = extract_visible_text(soup)

    # Gather raw key/value pairs from multiple structures
    kv: Dict[str, str] = {}
    kv.update(extract_kv_from_dt_dd(soup))
    kv.update(extract_kv_from_tables(soup))
    kv.update(extract_kv_from_label_value_blocks(soup))

    # Map into canonical fields
    for raw_k, raw_v in kv.items():
        canon = LABEL_MAP.get(raw_k)
        if not canon:
            continue

        if canon == "open_until_filled":
            b = parse_bool(raw_v)
            if b is not None:
                posting.open_until_filled = b
            continue

        if getattr(posting, canon, None) in (None, ""):
            setattr(posting, canon, raw_v)

    # Last-resort salary range detection from text (if no labeled salary found)
    if not posting.salary and posting.detail_text:
        m = re.search(r"(\$[\d,]+(?:\.\d{1,2})?)\s*(?:-|to)\s*(\$[\d,]+(?:\.\d{1,2})?)",
                      posting.detail_text)
        if m:
            posting.salary = f"{m.group(1)} - {m.group(2)}"

    # Normalize min/max from the salary string (smallest / largest $ amount found)
    if posting.salary:
        mn, mx = extract_salary_numbers(posting.salary)
        if mn and not posting.salary_min:
            posting.salary_min = mn
        if mx and not posting.salary_max:
            posting.salary_max = mx

    return posting


# -----------------------------
# School scrape (search + optional details)
# -----------------------------

def scrape_school(
    session: requests.Session,
    school_input: str,
    domain: str,
    query: str,
    max_pages: int,
    timeout: float,
    min_delay: float,
    max_delay: float,
    details: bool,
    store_html: bool,
) -> Tuple[List[Posting], Optional[str]]:
    base = choose_working_base(session, domain=domain, query=query, timeout=timeout)
    if not base:
        return [], None

    all_postings: List[Posting] = []
    seen_posting_ids: Set[str] = set()

    for page in range(1, max_pages + 1):

        print(school_input, "| Page", page)

        url = build_search_url(base, query=query, page=page)
        resp, _err = fetch(session, url, timeout=timeout)
        if resp is None:
            break

        if resp.status_code in (401, 403, 404, 429) or resp.status_code >= 500:
            break

        html = resp.text or ""
        if not is_likely_peopleadmin(html, str(resp.url)):
            break

        postings = extract_postings_from_search_html(html, base_url=base, school_input=school_input, query=query)

        new_postings: List[Posting] = []
        for p in postings:
            if p.posting_id and p.posting_id in seen_posting_ids:
                continue
            if p.posting_id:
                seen_posting_ids.add(p.posting_id)
            all_postings.append(p)
            new_postings.append(p)

        if not new_postings:
            break

        if details:
            for i in range(len(new_postings)):
                print("Details for", new_postings[i].posting_id)
                polite_sleep(min_delay, max_delay)
                new_postings[i] = scrape_detail_page(
                    session=session,
                    posting=new_postings[i],
                    timeout=timeout,
                    store_html=store_html,
                )

        polite_sleep(min_delay, max_delay)

    return all_postings, base


# -----------------------------
# I/O
# -----------------------------

def read_inputs(path: str) -> List[str]:
    lines: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            lines.append(line)
    return lines


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="Text file with one school domain per line (e.g., unc.edu)")
    ap.add_argument("--query", default="", help="Search query term (default: data)")
    ap.add_argument("--max-pages", type=int, default=5, help="Max search pages to scrape per school")
    ap.add_argument("--timeout", type=float, default=12.0, help="HTTP timeout seconds")
    ap.add_argument("--min-delay", type=float, default=0.4, help="Min seconds between requests")
    ap.add_argument("--max-delay", type=float, default=1.1, help="Max seconds between requests")
    ap.add_argument("--details", action="store_true", help="Fetch each posting detail page for salary/dates/etc.")
    ap.add_argument("--store-html", action="store_true", help="Store raw detail HTML in output (large).")
    ap.add_argument("--out-jsonl", default="peopleadmin_results.jsonl", help="Output JSONL path")
    args = ap.parse_args()

    inputs = read_inputs(args.inputs)
    if not inputs:
        print("No inputs found.", file=sys.stderr)
        return 2

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    total = 0
    schools_with_hits = 0

    with open(args.out_jsonl, "w", encoding="utf-8") as out:
        for raw in inputs:
            domain = normalize_domain(raw)

            postings, base_used = scrape_school(
                session=session,
                school_input=raw,
                domain=domain,
                query=args.query,
                max_pages=args.max_pages,
                timeout=args.timeout,
                min_delay=args.min_delay,
                max_delay=args.max_delay,
                details=args.details,
                store_html=args.store_html,
            )

            if postings:
                schools_with_hits += 1

            for p in postings:
                out.write(json.dumps(asdict(p), ensure_ascii=False) + "\n")

            total += len(postings)
            base_disp = base_used or ""
            print(f"{raw:30s} -> {len(postings):4d} postings  {base_disp}")

    print("\nDone.")
    print(f"Schools checked: {len(inputs)}")
    print(f"Schools w/ hits: {schools_with_hits}")
    print(f"Total postings:  {total}")
    print(f"Wrote: {args.out_jsonl}")
    return 0


if __name__ == "__main__":
    # Keep it friendly in interactive environments
    main()