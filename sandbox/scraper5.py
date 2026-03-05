#!/usr/bin/env python3
"""
> python scraper5.py --inputs schools_test.txt --details --workers 12 --detail-workers 8 --max-pages 5

Scrape job postings from PeopleAdmin installations (common at colleges).

Input: a text file with one school domain per line (e.g., unc.edu).
The script tries multiple likely base URL patterns for each domain, then queries:
  /postings/search?page=1&query=<term>

Optional:
  --details     Fetch each posting detail page for salary/dates/etc.
  --store-html  Store raw detail HTML too (can get large)

Parallelism (fast):
  - Parallelizes schools (global thread pool)
  - If --details is set, parallelizes detail page fetches within each school (local thread pool)

Storage:
  - Batch UPSERTs results into PostgreSQL table `peopleadmin_postings`
  - Uses psycopg2.extras.execute_values for speed
  - Uses your jetTools.cfg postgres settings (same as jetTools.pgQuery)

IMPORTANT:
  Your UNIQUE constraint is (base_url, posting_id).
  If posting_id is NULL, Postgres UNIQUE won't dedupe (NULL != NULL).
  Consider also adding UNIQUE(url) if you want guaranteed dedupe.
"""

from __future__ import annotations

import os
import argparse
import queue
import random
import re
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from threading import Lock, local
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode, urljoin, urlparse

import psycopg2
import requests
from bs4 import BeautifulSoup
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import jetTools  # must contain cfg["postgres"][...] and your pgQuery function


# -----------------------------
# Global stop handling (Ctrl+C)
# -----------------------------

STOP_EVENT = threading.Event()

SIGINT_COUNT = 0
SIGINT_LOCK = threading.Lock()

def _handle_sigint(signum, frame):
    global SIGINT_COUNT
    with SIGINT_LOCK:
        SIGINT_COUNT += 1
        n = SIGINT_COUNT

    STOP_EVENT.set()

    # On the 2nd Ctrl+C, bail out immediately (threads may be stuck in requests)
    if n >= 2:
        os._exit(130)

# Register handler (best-effort; on some Windows setups, behavior can vary)
signal.signal(signal.SIGINT, _handle_sigint)


# -----------------------------
# Data model
# -----------------------------

class DBWriter:
    """
    Background DB writer thread. Supports graceful stop() and fast stop_fast()
    (fast stop drops any queued-but-not-yet-written rows to exit quickly).
    """
    def __init__(self, batch_size: int = 1000, flush_seconds: float = 2.0):
        self.batch_size = max(1, int(batch_size))
        self.flush_seconds = max(0.1, float(flush_seconds))
        self.q: "queue.Queue[List[object] | None]" = queue.Queue(maxsize=50_000)
        self._stop_fast = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.total = 0  # how many rows attempted
        self._lock = threading.Lock()

        cfg = jetTools.cfg
        self.conn = psycopg2.connect(
            dbname=cfg["postgres"]["db"],
            user=cfg["postgres"]["user"],
            password=cfg["postgres"]["password"],
            host=cfg["postgres"]["host"],
            port=cfg["postgres"]["port"],
        )
        self.conn.autocommit = False

    def start(self) -> None:
        self._thread.start()

    def put_rows(self, rows: List[List[object]]) -> None:
        if not rows:
            return
        # If stopping, avoid blocking forever on queue.put().
        for r in rows:
            if STOP_EVENT.is_set() or self._stop_fast.is_set():
                return
            # block a bit; if queue is full and we're stopping soon, bail quickly
            while True:
                try:
                    self.q.put(r, timeout=0.25)
                    break
                except queue.Full:
                    if STOP_EVENT.is_set() or self._stop_fast.is_set():
                        return

    def stop(self) -> None:
        """
        Graceful: flush queued rows then exit.
        """
        try:
            self.q.put(None, timeout=1.0)
        except Exception:
            pass
        self._thread.join()
        try:
            self.conn.close()
        except Exception:
            pass

    def stop_fast(self) -> None:
        """
        Fast: exit quickly; drops queued rows. Useful on Ctrl+C.
        """
        self._stop_fast.set()
        try:
            self.q.put_nowait(None)
        except Exception:
            pass
        self._thread.join(timeout=2.0)
        try:
            self.conn.close()
        except Exception:
            pass

    def _flush(self, cur, buf: List[List[object]]) -> None:
        if not buf:
            return
        execute_values(cur, UPSERT_SQL_VALUES, buf, page_size=min(len(buf), self.batch_size))
        self.conn.commit()
        with self._lock:
            self.total += len(buf)

    def _run(self) -> None:
        buf: List[List[object]] = []
        last_flush = time.time()

        try:
            with self.conn.cursor() as cur:
                while True:
                    if self._stop_fast.is_set() or STOP_EVENT.is_set():
                        # Fast exit: do not flush remaining buffer/queue.
                        break

                    timeout = max(0.0, self.flush_seconds - (time.time() - last_flush))
                    try:
                        item = self.q.get(timeout=timeout)
                    except queue.Empty:
                        # time-based flush
                        self._flush(cur, buf)
                        buf.clear()
                        last_flush = time.time()
                        continue

                    if item is None:
                        # final flush and exit
                        self._flush(cur, buf)
                        buf.clear()
                        break

                    buf.append(item)

                    # size-based flush
                    if len(buf) >= self.batch_size:
                        self._flush(cur, buf)
                        buf.clear()
                        last_flush = time.time()

        except Exception as e:
            with PRINT_LOCK:
                print("DB writer thread error:", repr(e), file=sys.stderr)
            try:
                self.conn.rollback()
            except Exception:
                pass


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
    "User-Agent": "Mozilla/5.0 (compatible; PeopleAdminScraper/1.3; +https://example.com/bot-info)",
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

PRINT_LOCK = Lock()
_TLS = local()


# -----------------------------
# Session / retries
# -----------------------------

def make_session() -> requests.Session:
    """
    Create a requests.Session with retries and decent connection pooling.
    """
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)

    retry = Retry(
        total=4,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=100,
        pool_maxsize=100,
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def get_thread_session() -> requests.Session:
    """
    Thread-local session to avoid sharing a single Session across threads.
    """
    s = getattr(_TLS, "session", None)
    if s is None:
        s = make_session()
        _TLS.session = s
    return s


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


def money_to_numeric(m: Optional[str]) -> Optional[float]:
    """
    Convert strings like "$35,000" or "$22.50" to float.
    Returns None if not parseable.
    """
    if not m:
        return None
    s = clean_text(m)
    mm = re.search(r"\$?\s*([\d,]+(?:\.\d{1,2})?)", s)
    if not mm:
        return None
    raw = mm.group(1).replace(",", "")
    try:
        return float(raw)
    except ValueError:
        return None


def parse_date_mdy(s: Optional[str]) -> Optional[str]:
    """
    Parse dates like '03/03/2026' or '3/3/2026' into ISO 'YYYY-MM-DD' string.
    Return None if not parseable.
    """
    if not s:
        return None
    t = clean_text(s)
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(t, fmt).date().isoformat()
        except ValueError:
            pass
    return None


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
    """
    Interruptible sleep: wakes early if STOP_EVENT is set.
    """
    dur = random.uniform(min_s, max_s)
    STOP_EVENT.wait(dur)


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
    """
    Stop-aware fetch. Won’t start new requests when stopping.
    Uses split timeout (connect, read) for slightly better responsiveness.
    """
    if STOP_EVENT.is_set():
        return None, "stopped"

    try:
        resp = session.get(url, timeout=(min(3.0, timeout), timeout), allow_redirects=True)
        return resp, None
    except requests.RequestException as e:
        return None, str(e)


def candidate_bases_for_domain(domain: str) -> List[str]:
    d = normalize_domain(domain)
    sub = domain_to_sub(d)
    bases: List[str] = []
    for pat in BASE_PATTERNS:
        bases.append(pat.format(domain=d, sub=sub))

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
        if STOP_EVENT.is_set():
            return None

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

        polite_sleep(0.02, 0.10)

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
    if STOP_EVENT.is_set():
        return posting

    resp, _err = fetch(session, posting.url, timeout=timeout)
    if resp is None:
        return posting
    if resp.status_code in (401, 403, 404, 429) or resp.status_code >= 500:
        return posting

    html = resp.text or ""
    soup = BeautifulSoup(html, "html.parser")

    if store_html:
        posting.detail_html = html
    posting.detail_text = extract_visible_text(soup)

    kv: Dict[str, str] = {}
    kv.update(extract_kv_from_dt_dd(soup))
    kv.update(extract_kv_from_tables(soup))
    kv.update(extract_kv_from_label_value_blocks(soup))

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

    if not posting.salary and posting.detail_text:
        m = re.search(
            r"(\$[\d,]+(?:\.\d{1,2})?)\s*(?:-|to)\s*(\$[\d,]+(?:\.\d{1,2})?)",
            posting.detail_text,
        )
        if m:
            posting.salary = f"{m.group(1)} - {m.group(2)}"

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

def _safe_print(*args, **kwargs) -> None:
    with PRINT_LOCK:
        print(*args, **kwargs)


def scrape_school(
    school_input: str,
    domain: str,
    query: str,
    max_pages: int,
    timeout: float,
    min_delay: float,
    max_delay: float,
    details: bool,
    store_html: bool,
    detail_workers: int,
) -> Tuple[List[Posting], Optional[str]]:
    """
    Runs in a worker thread for one school. Uses that thread's session for search/base discovery.
    """
    if STOP_EVENT.is_set():
        return [], None

    session = get_thread_session()

    base = choose_working_base(session, domain=domain, query=query, timeout=timeout)
    if not base or STOP_EVENT.is_set():
        return [], None

    all_postings: List[Posting] = []
    seen_posting_ids: Set[str] = set()

    for page in range(1, max_pages + 1):
        if STOP_EVENT.is_set():
            break

        _safe_print(school_input, "| Page", page)

        url = build_search_url(base, query=query, page=page)
        resp, _err = fetch(session, url, timeout=timeout)
        if STOP_EVENT.is_set():
            break
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

        if details and new_postings and not STOP_EVENT.is_set():
            def detail_worker(p: Posting) -> Posting:
                if STOP_EVENT.is_set():
                    return p
                polite_sleep(min_delay, max_delay)
                if STOP_EVENT.is_set():
                    return p
                s = get_thread_session()
                return scrape_detail_page(
                    session=s,
                    posting=p,
                    timeout=timeout,
                    store_html=store_html,
                )

            dw = max(1, int(detail_workers))
            with ThreadPoolExecutor(max_workers=dw) as ex:
                futs = [ex.submit(detail_worker, p) for p in new_postings]

                detailed_results: List[Posting] = []
                for f in as_completed(futs):
                    if STOP_EVENT.is_set():
                        break
                    try:
                        detailed_results.append(f.result())
                    except Exception as e:
                        _safe_print("Detail worker error:", repr(e))

            detailed_by_id: Dict[str, Posting] = {p.posting_id: p for p in detailed_results if p.posting_id}
            for idx, p in enumerate(new_postings):
                if p.posting_id and p.posting_id in detailed_by_id:
                    new_postings[idx] = detailed_by_id[p.posting_id]

        polite_sleep(min_delay, max_delay)

    return all_postings, base


# -----------------------------
# DB batch upsert
# -----------------------------

UPSERT_SQL_VALUES = """
    INSERT INTO peopleadmin_postings (
        school_input,
        base_url,
        query,
        posting_id,
        url,
        title,
        location,
        department,
        salary,
        salary_min,
        salary_max,
        salary_min_num,
        salary_max_num,
        posted_date,
        close_date,
        open_until_filled,
        employment_type,
        time_limit,
        full_time_or_part_time,
        special_instructions,
        detail_text,
        detail_html
    ) VALUES %s
    ON CONFLICT (base_url, posting_id)
    DO UPDATE SET
        school_input           = EXCLUDED.school_input,
        query                  = EXCLUDED.query,
        url                    = EXCLUDED.url,
        title                  = EXCLUDED.title,
        location               = EXCLUDED.location,
        department             = EXCLUDED.department,
        salary                 = EXCLUDED.salary,
        salary_min             = EXCLUDED.salary_min,
        salary_max             = EXCLUDED.salary_max,
        salary_min_num         = EXCLUDED.salary_min_num,
        salary_max_num         = EXCLUDED.salary_max_num,
        posted_date            = EXCLUDED.posted_date,
        close_date             = EXCLUDED.close_date,
        open_until_filled      = EXCLUDED.open_until_filled,
        employment_type        = EXCLUDED.employment_type,
        time_limit             = EXCLUDED.time_limit,
        full_time_or_part_time = EXCLUDED.full_time_or_part_time,
        special_instructions   = EXCLUDED.special_instructions,
        detail_text            = EXCLUDED.detail_text,
        detail_html            = EXCLUDED.detail_html,
        scraped_at             = now();
"""

DB_COLS = [
    "school_input",
    "base_url",
    "query",
    "posting_id",
    "url",
    "title",
    "location",
    "department",
    "salary",
    "salary_min",
    "salary_max",
    "salary_min_num",
    "salary_max_num",
    "posted_date",
    "close_date",
    "open_until_filled",
    "employment_type",
    "time_limit",
    "full_time_or_part_time",
    "special_instructions",
    "detail_text",
    "detail_html",
]


def posting_to_db_row(p: Posting) -> List[object]:
    posted_iso = parse_date_mdy(p.posted_date)
    close_iso = parse_date_mdy(p.close_date)

    mn_num = money_to_numeric(p.salary_min)
    mx_num = money_to_numeric(p.salary_max)

    row = {
        "school_input": p.school_input,
        "base_url": p.base_url,
        "query": p.query or "",
        "posting_id": p.posting_id,
        "url": p.url,
        "title": p.title,
        "location": p.location,
        "department": p.department,
        "salary": p.salary,
        "salary_min": p.salary_min,
        "salary_max": p.salary_max,
        "salary_min_num": mn_num,
        "salary_max_num": mx_num,
        "posted_date": posted_iso,
        "close_date": close_iso,
        "open_until_filled": p.open_until_filled,
        "employment_type": p.employment_type,
        "time_limit": p.time_limit,
        "full_time_or_part_time": p.full_time_or_part_time,
        "special_instructions": p.special_instructions,
        "detail_text": p.detail_text,
        "detail_html": p.detail_html,
    }
    return [row.get(c) for c in DB_COLS]


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
    ap.add_argument("--query", default="", help='Search query term (default: "")')
    ap.add_argument("--max-pages", type=int, default=5, help="Max search pages to scrape per school")
    ap.add_argument("--timeout", type=float, default=12.0, help="HTTP timeout seconds")
    ap.add_argument("--min-delay", type=float, default=0.4, help="Min seconds between requests")
    ap.add_argument("--max-delay", type=float, default=1.1, help="Max seconds between requests")
    ap.add_argument("--details", action="store_true", help="Fetch each posting detail page for salary/dates/etc.")
    ap.add_argument("--store-html", action="store_true", help="Store raw detail HTML in output (large).")

    ap.add_argument("--workers", type=int, default=16, help="Parallel workers (schools)")
    ap.add_argument("--detail-workers", type=int, default=6, help="Parallel workers (detail pages within a school)")

    ap.add_argument("--db-batch-size", type=int, default=500, help="Batch size for execute_values upserts")

    args = ap.parse_args()

    dbw = DBWriter(batch_size=args.db_batch_size, flush_seconds=2.0)
    dbw.start()

    inputs = read_inputs(args.inputs)
    if not inputs:
        print("No inputs found.", file=sys.stderr)
        return 2

    total = 0
    schools_with_hits = 0

    max_workers = max(1, int(args.workers))

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = []

        for raw in inputs:
            if STOP_EVENT.is_set():
                break
            domain = normalize_domain(raw)
            futures.append(
                ex.submit(
                    scrape_school,
                    raw,
                    domain,
                    args.query,
                    args.max_pages,
                    args.timeout,
                    args.min_delay,
                    args.max_delay,
                    args.details,
                    args.store_html,
                    args.detail_workers,
                )
            )

        try:
            for fut in as_completed(futures):
                if STOP_EVENT.is_set():
                    break

                try:
                    postings, base_used = fut.result()
                except Exception as e:
                    with PRINT_LOCK:
                        print("School worker error:", repr(e), file=sys.stderr)
                    continue

                if STOP_EVENT.is_set():
                    break

                if postings:
                    schools_with_hits += 1

                rows = [posting_to_db_row(p) for p in postings]
                dbw.put_rows(rows)

                total += len(postings)
                base_disp = base_used or ""
                if postings:
                    _safe_print(f"{postings[0].school_input:30s} -> {len(postings):4d} postings  {base_disp}")
                else:
                    _safe_print(f"{'(no postings)':30s} -> {len(postings):4d} postings  {base_disp}")

        except KeyboardInterrupt:
            STOP_EVENT.set()
            with PRINT_LOCK:
                print("\nCtrl+C received - stopping quickly...", file=sys.stderr)

            # Cancel futures not yet started (Py 3.9+).
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                # Older Python: cancel_futures not available
                ex.shutdown(wait=False)

            dbw.stop_fast()
            return 130

    # Normal graceful stop:
    if STOP_EVENT.is_set():
        dbw.stop_fast()
        return 130

    dbw.stop()

    print("\nDone.")
    print(f"Schools checked: {len(inputs)}")
    print(f"Schools w/ hits: {schools_with_hits}")
    print(f"Total postings:  {total}")
    print(f"DB upserts:      {dbw.total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())