#!/usr/bin/env python3
"""
peopleadmin_scraper.py

Scrape job postings from PeopleAdmin installations (common at colleges).

Recommended input: a list of school domains like:
  unc.edu
  ncsu.edu
  vt.edu
  osu.edu

The script will try multiple likely base URL patterns for each domain, then query:
  /postings/search?page=1&query=<term>

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
from typing import Iterable, Optional, Dict, List, Set, Tuple
from urllib.parse import urljoin, urlencode, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup


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


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PeopleAdminScraper/1.0; +https://example.com/bot-info)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# A few known host patterns people use
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

# PeopleAdmin-ish fingerprints to validate we landed in the right place
PEOPLEADMIN_FINGERPRINTS = [
    "peopleadmin",               # common in HTML / assets
    "/postings/search",          # canonical path
    "applicant tracking",        # sometimes in footer/text
    "postings",                  # broad, but combined with others helps
]


def normalize_domain(s: str) -> str:
    s = s.strip()
    s = re.sub(r"^https?://", "", s, flags=re.I)
    s = s.split("/")[0]
    return s.lower()


def domain_to_sub(domain: str) -> str:
    """
    Convert a domain like 'unc.edu' -> 'unc'
    'careers.unf.edu' -> 'careers' (not ideal but ok)
    'utk.edu' -> 'utk'
    """
    parts = domain.split(".")
    return parts[0] if parts else domain


def polite_sleep(min_s: float, max_s: float) -> None:
    time.sleep(random.uniform(min_s, max_s))


def is_likely_peopleadmin(html: str, final_url: str) -> bool:
    text = (html or "").lower()
    u = (final_url or "").lower()
    # stronger check: the final URL path includes /postings or the HTML includes multiple fingerprints
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


def choose_working_base(session: requests.Session, school_input: str, domain: str, query: str, timeout: float) -> Optional[str]:
    """
    Try candidate bases. Return the first base that looks like PeopleAdmin search works.
    """
    for base in candidate_bases_for_domain(domain):
        test_url = build_search_url(base, query=query, page=1)
        resp, err = fetch(session, test_url, timeout=timeout)
        if resp is None:
            continue

        # Quick rejects
        if resp.status_code in (401, 403, 429):
            continue
        if resp.status_code >= 500:
            continue
        if resp.status_code == 404:
            continue

        html = resp.text or ""
        final_url = str(resp.url)

        if is_likely_peopleadmin(html, final_url):
            # normalize base to the scheme+netloc we ended up at
            parsed = urlparse(final_url)
            normalized_base = f"{parsed.scheme}://{parsed.netloc}"
            return normalized_base

        polite_sleep(0.05, 0.2)

    return None


def extract_postings_from_search_html(
    html: str,
    base_url: str,
    school_input: str,
    query: str
) -> List[Posting]:
    """
    Extract postings from a PeopleAdmin search result page.

    PeopleAdmin markup varies, so we:
    - collect links that look like posting detail pages
    - derive a title from link text
    - optionally try to find nearby 'Location'/'Department' text if present
    """
    soup = BeautifulSoup(html, "html.parser")
    postings: List[Posting] = []

    # Common detail URL patterns:
    #   /postings/12345
    #   /postings/12345?something=...
    # Sometimes /postings/<id>/something
    link_candidates = soup.select('a[href*="/postings/"]')

    seen_urls: Set[str] = set()
    for a in link_candidates:
        href = a.get("href", "").strip()
        if not href:
            continue

        # filter out links that are obviously not detail pages
        if "/postings/search" in href:
            continue
        if re.search(r"/postings/(search|create|new)\b", href):
            continue

        # Must contain /postings/<number-ish>
        m = re.search(r"/postings/(\d+)", href)
        if not m:
            continue

        abs_url = urljoin(base_url, href)
        if abs_url in seen_urls:
            continue
        seen_urls.add(abs_url)

        title = " ".join((a.get_text(" ", strip=True) or "").split())
        if not title or len(title) < 3:
            # Sometimes the clickable element has nested spans; fallback to aria-label/title attributes
            title = (a.get("aria-label") or a.get("title") or "").strip()

        posting_id = m.group(1)

        # Optional heuristic: check the surrounding container text for "Location" or "Department"
        location = None
        department = None
        container_text = ""
        parent = a
        for _ in range(5):
            if not parent:
                break
            parent = parent.parent
            if not parent:
                break
            container_text = " ".join(parent.get_text(" ", strip=True).split())
            # stop if we have "Location" or "Department" in neighborhood
            if re.search(r"\bLocation\b|\bDepartment\b", container_text, flags=re.I):
                break

        if container_text:
            # Very loose parsing (since formats vary)
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


def scrape_school(
    session: requests.Session,
    school_input: str,
    domain: str,
    query: str,
    max_pages: int,
    timeout: float,
    min_delay: float,
    max_delay: float,
) -> List[Posting]:
    base = choose_working_base(session, school_input=school_input, domain=domain, query=query, timeout=timeout)
    if not base:
        return []

    all_postings: List[Posting] = []
    seen_posting_ids: Set[str] = set()

    for page in range(1, max_pages + 1):
        url = build_search_url(base, query=query, page=page)
        resp, err = fetch(session, url, timeout=timeout)
        if resp is None:
            break

        if resp.status_code in (401, 403, 429):
            break
        if resp.status_code == 404:
            break
        if resp.status_code >= 500:
            break

        html = resp.text or ""
        if not is_likely_peopleadmin(html, str(resp.url)):
            break

        postings = extract_postings_from_search_html(html, base_url=base, school_input=school_input, query=query)

        # stop condition: no postings or no new postings
        new_count = 0
        for p in postings:
            if p.posting_id and p.posting_id in seen_posting_ids:
                continue
            if p.posting_id:
                seen_posting_ids.add(p.posting_id)
            all_postings.append(p)
            new_count += 1

        if new_count == 0:
            break

        polite_sleep(min_delay, max_delay)

    return all_postings


def read_inputs(path: str) -> List[str]:
    lines: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            lines.append(line)
    return lines


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True, help="Text file with one school domain per line (e.g., unc.edu)")
    ap.add_argument("--query", default="data", help="Search query term (default: data)")
    ap.add_argument("--max-pages", type=int, default=5, help="Max search pages to scrape per school")
    ap.add_argument("--timeout", type=float, default=12.0, help="HTTP timeout seconds")
    ap.add_argument("--min-delay", type=float, default=0.4, help="Min seconds between requests")
    ap.add_argument("--max-delay", type=float, default=1.1, help="Max seconds between requests")
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

            postings = scrape_school(
                session=session,
                school_input=raw,
                domain=domain,
                query=args.query,
                max_pages=args.max_pages,
                timeout=args.timeout,
                min_delay=args.min_delay,
                max_delay=args.max_delay,
            )

            if postings:
                schools_with_hits += 1

            for p in postings:
                out.write(json.dumps(asdict(p), ensure_ascii=False) + "\n")

            total += len(postings)
            print(f"{raw:30s} -> {len(postings):4d} postings")

    print("\nDone.")
    print(f"Schools checked: {len(inputs)}")
    print(f"Schools w/ hits: {schools_with_hits}")
    print(f"Total postings:  {total}")
    print(f"Wrote: {args.out_jsonl}")
    return 0


if __name__ == "__main__":
    main()