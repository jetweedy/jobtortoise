#!/usr/bin/env python3
"""
detect_job_systems.py

Detect likely university job-posting systems by probing ONLY school-specific URLs
and following redirects. This version does NOT assume any vendor homepage or
generic vendor URL belongs to a school.

Input:
  schools.txt   one school domain per line, e.g.
      unc.edu
      uncg.edu
      ncsu.edu

Outputs:
  - job_systems.json
  - job_systems.csv

Usage:
  python detect_job_systems.py
  python detect_job_systems.py --inputs schools.txt --workers 20 --timeout 10

Dependencies:
  pip install requests beautifulsoup4

Notes:
  - This is intentionally conservative.
  - It only classifies a school when evidence comes from:
      1) a school-domain page,
      2) a redirect from a school-domain page to a vendor domain, or
      3) a school-specific vendor hostname/path derived from the school.
  - It avoids generic vendor homepages like apply.interfolio.com with no school-specific context.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


STOP = False


def handle_sigint(sig, frame):
    global STOP
    STOP = True
    print("\nStopping early after current requests finish...", file=sys.stderr)


signal.signal(signal.SIGINT, handle_sigint)


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

COMMON_PATHS = [
    "",
    "/jobs",
    "/job",
    "/jobs/",
    "/employment",
    "/employment/",
    "/careers",
    "/careers/",
    "/human-resources",
    "/human-resources/",
    "/human-resources/careers",
    "/human-resources/employment",
    "/hr",
    "/hr/",
    "/about/jobs",
    "/about/employment",
    "/faculty-staff/jobs",
    "/faculty-staff/employment",
    "/postings/search",
]


@dataclass
class DetectionResult:
    school: str
    system: Optional[str]
    confidence: float
    matched_pattern: Optional[str]
    matched_url: Optional[str]
    source_url: Optional[str]
    notes: Optional[str]


def normalize_school(line: str) -> Optional[str]:
    s = line.strip()
    if not s:
        return None
    s = s.replace("https://", "").replace("http://", "")
    s = s.split("/")[0].strip().lower()
    if s.startswith("www."):
        s = s[4:]
    return s or None


def school_tokens(school: str) -> List[str]:
    """
    uncg.edu -> ['uncg']
    appstate.edu -> ['appstate']
    something.foo.edu -> ['something', 'foo']
    """
    host = school.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    parts = [p for p in host.split(".") if p and p not in {"edu", "org", "com", "net"}]
    return parts


def build_candidate_urls(school: str) -> List[str]:
    """
    Only school-specific candidate URLs.
    Avoid generic vendor URLs.
    """
    tokens = school_tokens(school)
    first = tokens[0] if tokens else school.split(".")[0]

    bases = [
        f"https://{school}",
        f"http://{school}",
        f"https://www.{school}",
        f"http://www.{school}",
        f"https://jobs.{school}",
        f"http://jobs.{school}",
        f"https://employment.{school}",
        f"http://employment.{school}",
        f"https://careers.{school}",
        f"http://careers.{school}",
        f"https://hr.{school}",
        f"http://hr.{school}",
    ]

    # Only school-specific vendor guesses. No generic vendor homepages.
    school_specific_vendor_guesses = [
        f"https://{first}.peopleadmin.com",
        f"https://{first}.peopleadmin.com/postings/search",
        f"https://{first}.wd1.myworkdayjobs.com",
        f"https://{first}.wd5.myworkdayjobs.com",
    ]

    seen = set()
    urls: List[str] = []

    for base in bases:
        for path in COMMON_PATHS:
            full = base.rstrip("/") + path
            if full not in seen:
                seen.add(full)
                urls.append(full)

    for url in school_specific_vendor_guesses:
        if url not in seen:
            seen.add(url)
            urls.append(url)

    return urls


def fetch_url(
    session: requests.Session,
    source_url: str,
    timeout: int,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns: final_url, html, error
    """
    try:
        r = session.get(source_url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        ct = (r.headers.get("Content-Type") or "").lower()

        if r.status_code >= 400:
            return None, None, f"HTTP {r.status_code}"

        if "text/html" not in ct and "application/xhtml" not in ct:
            return r.url, None, f"Non-HTML content-type: {ct}"

        return r.url, r.text[:500000], None
    except requests.RequestException as e:
        return None, None, str(e)


def extract_visible_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return " ".join(soup.stripped_strings).lower()


def host_of(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def is_same_school_domain(host: str, school: str) -> bool:
    """
    True if host is the school domain or subdomain thereof.
    """
    host = (host or "").lower()
    school = school.lower()
    return host == school or host.endswith("." + school)


def school_token_in_text_or_host(school: str, url: str, text: str) -> bool:
    """
    Conservative check for school-specific context on a vendor page.
    """
    tokens = school_tokens(school)
    host = host_of(url)
    combined = (host + " " + text).lower()

    for token in tokens:
        token = token.strip().lower()
        if not token or len(token) < 3:
            continue
        if token in combined:
            return True

    # Also check root label like uncg from uncg.edu
    root = school.split(".")[0].lower()
    if len(root) >= 3 and root in combined:
        return True

    return False


def allowed_vendor_context(school: str, source_url: str, final_url: str, text: str) -> bool:
    """
    We allow vendor classification only when:
      - the final page is still on the school domain, OR
      - the request started from the school domain and redirected to the vendor, OR
      - the final vendor page contains school-specific context (token in host/text), OR
      - the final vendor hostname itself is school-specific (e.g. unc.peopleadmin.com)
    """
    source_host = host_of(source_url)
    final_host = host_of(final_url)

    if is_same_school_domain(final_host, school):
        return True

    # Redirected from school domain to vendor/system page
    if is_same_school_domain(source_host, school):
        return True

    # School-specific subdomain on vendor host
    # e.g. unc.peopleadmin.com
    root = school.split(".")[0].lower()
    if root and final_host.startswith(root + "."):
        return True

    # Vendor page explicitly mentions school token(s)
    if school_token_in_text_or_host(school, final_url, text):
        return True

    return False


def detect_system_from_content(
    school: str,
    source_url: str,
    final_url: str,
    html: str,
) -> Tuple[Optional[str], Optional[str], float, Optional[str]]:
    """
    Conservative ATS detection.
    """
    u = (final_url or "").lower()
    h = html.lower()
    text = extract_visible_text(html)

    source_host = host_of(source_url)
    final_host = host_of(final_url)
    redirected = source_host != final_host

    candidates: List[Tuple[str, str, float, Optional[str]]] = []

    def add(system: str, pattern: str, score: float, note: Optional[str] = None):
        candidates.append((system, pattern, min(score, 0.99), note))

    # -----------------------
    # PeopleAdmin
    # -----------------------
    if (
        "peopleadmin.com" in final_host
        or "/postings/search" in u
        or "/postings/" in u
        or "search postings" in text
        or "all jobs atom feed" in text
    ):
        if allowed_vendor_context(school, source_url, final_url, text):
            score = 0.0
            if "peopleadmin.com" in final_host:
                score += 0.70
            if "/postings/search" in u:
                score += 0.55
            if "/postings/" in u:
                score += 0.15
            if "search postings" in text:
                score += 0.15
            if "all jobs atom feed" in text:
                score += 0.15
            if redirected:
                score += 0.05
            add("PeopleAdmin", "peopleadmin fingerprint", score)

    # -----------------------
    # Workday
    # -----------------------
    if "myworkdayjobs.com" in final_host or "search for jobs" in text or "jobs found" in text:
        if allowed_vendor_context(school, source_url, final_url, text):
            score = 0.0
            if "myworkdayjobs.com" in final_host:
                score += 0.80
            if "search for jobs" in text:
                score += 0.10
            if "jobs found" in text:
                score += 0.10
            if '"externalcareer"' in h or '"jobpostinginfo"' in h:
                score += 0.10
            if redirected:
                score += 0.05
            add("Workday", "workday fingerprint", score)

    # -----------------------
    # PageUp
    # -----------------------
    if (
        "pageuppeople.com" in final_host
        or "/cw/en-us/listing" in u
        or "/en/listing" in u
        or "recent jobs" in text
    ):
        if allowed_vendor_context(school, source_url, final_url, text):
            score = 0.0
            if "pageuppeople.com" in final_host:
                score += 0.75
            if "/cw/en-us/listing" in u or "/en/listing" in u:
                score += 0.15
            if "recent jobs" in text:
                score += 0.10
            if redirected:
                score += 0.05
            add("PageUp", "pageup fingerprint", score)

    # -----------------------
    # Interfolio
    # Require stronger evidence than just generic apply.interfolio.com
    # -----------------------
    interfolio_posting = re.search(r"apply\.interfolio\.com/\d+", u) is not None
    if (
        interfolio_posting
        or "account.interfolio.com" in final_host
        or "faculty search" in text
        or "interfolio" in text
    ):
        if allowed_vendor_context(school, source_url, final_url, text):
            score = 0.0
            if interfolio_posting:
                score += 0.70
            if "account.interfolio.com" in final_host:
                score += 0.40
            if "faculty search" in text:
                score += 0.15
            if "interfolio" in text:
                score += 0.10
            if redirected:
                score += 0.05

            # Penalize generic apply.interfolio.com root page
            if final_host == "apply.interfolio.com" and not interfolio_posting:
                score -= 0.40

            if score >= 0.35:
                add("Interfolio", "interfolio fingerprint", score)

    # -----------------------
    # NEOGOV / NeoEd
    # -----------------------
    if (
        "governmentjobs.com" in final_host
        or "neoed.com" in final_host
        or "schooljobs" in text
        or "governmentjobs" in text
    ):
        if allowed_vendor_context(school, source_url, final_url, text):
            score = 0.0
            if "governmentjobs.com" in final_host:
                score += 0.75
            if "neoed.com" in final_host:
                score += 0.70
            if "schooljobs" in text:
                score += 0.10
            if "governmentjobs" in text:
                score += 0.10
            if redirected:
                score += 0.05
            add("NEOGOV/NeoEd", "neogov fingerprint", score)

    # -----------------------
    # Taleo
    # -----------------------
    if "taleo.net" in final_host or ("oracle" in text and "taleo" in text):
        if allowed_vendor_context(school, source_url, final_url, text):
            score = 0.0
            if "taleo.net" in final_host:
                score += 0.85
            if "oracle" in text and "taleo" in text:
                score += 0.10
            if redirected:
                score += 0.05
            add("Taleo", "taleo fingerprint", score)

    # -----------------------
    # iCIMS
    # -----------------------
    if "icims.com" in final_host or "icims" in text:
        if allowed_vendor_context(school, source_url, final_url, text):
            score = 0.0
            if "icims.com" in final_host:
                score += 0.85
            if "icims" in text:
                score += 0.10
            if redirected:
                score += 0.05
            add("iCIMS", "icims fingerprint", score)

    # -----------------------
    # SmartRecruiters
    # -----------------------
    if "smartrecruiters.com" in final_host or "smartrecruiters" in text:
        if allowed_vendor_context(school, source_url, final_url, text):
            score = 0.0
            if "smartrecruiters.com" in final_host:
                score += 0.85
            if "smartrecruiters" in text:
                score += 0.10
            if redirected:
                score += 0.05
            add("SmartRecruiters", "smartrecruiters fingerprint", score)

    # -----------------------
    # UKG / UltiPro
    # -----------------------
    if "ultipro.com" in final_host or "ukg.com" in final_host or "ultipro" in text or "ukg" in text:
        if allowed_vendor_context(school, source_url, final_url, text):
            score = 0.0
            if "ultipro.com" in final_host or "ukg.com" in final_host:
                score += 0.80
            if "ultipro" in text or "ukg" in text:
                score += 0.10
            if redirected:
                score += 0.05
            add("UKG / UltiPro", "ukg/ultipro fingerprint", score)

    # -----------------------
    # Jobvite
    # -----------------------
    if "jobvite.com" in final_host or "jobvite" in text:
        if allowed_vendor_context(school, source_url, final_url, text):
            score = 0.0
            if "jobvite.com" in final_host:
                score += 0.85
            if "jobvite" in text:
                score += 0.10
            if redirected:
                score += 0.05
            add("Jobvite", "jobvite fingerprint", score)

    if not candidates:
        return None, None, 0.0, None

    candidates.sort(key=lambda x: x[2], reverse=True)
    best_system, best_pattern, best_conf, best_note = candidates[0]

    # Ambiguity note if another system scored nearly as high
    if len(candidates) > 1:
        second_system, second_pattern, second_conf, _ = candidates[1]
        if second_system != best_system and second_conf >= best_conf - 0.08:
            note = f"Also matched {second_system} via {second_pattern}"
            if best_note:
                note = f"{best_note}; {note}"
            best_note = note

    return best_system, best_pattern, round(best_conf, 3), best_note


def scan_school(school: str, timeout: int) -> DetectionResult:
    if STOP:
        return DetectionResult(
            school=school,
            system=None,
            confidence=0.0,
            matched_pattern=None,
            matched_url=None,
            source_url=None,
            notes="Stopped before scan",
        )

    session = requests.Session()
    session.headers.update(HEADERS)

    best: Optional[DetectionResult] = None
    errors: List[str] = []
    checked = 0

    for source_url in build_candidate_urls(school):
        if STOP:
            break

        checked += 1
        final_url, html, err = fetch_url(session, source_url, timeout=timeout)

        if err:
            errors.append(f"{source_url} -> {err}")
            continue

        if not final_url or not html:
            continue

        system, pattern, conf, note = detect_system_from_content(
            school=school,
            source_url=source_url,
            final_url=final_url,
            html=html,
        )

        if system:
            result = DetectionResult(
                school=school,
                system=system,
                confidence=conf,
                matched_pattern=pattern,
                matched_url=final_url,
                source_url=source_url,
                notes=note,
            )

            if best is None or result.confidence > best.confidence:
                best = result

            if result.confidence >= 0.95:
                break

    if best:
        return best

    note = f"No known ATS fingerprint found after checking {checked} URLs"
    if errors:
        note += f"; sample error: {errors[0][:180]}"
    return DetectionResult(
        school=school,
        system=None,
        confidence=0.0,
        matched_pattern=None,
        matched_url=None,
        source_url=None,
        notes=note,
    )


def load_schools(path: str) -> List[str]:
    schools: List[str] = []
    seen = set()

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            school = normalize_school(line)
            if not school:
                continue
            if school not in seen:
                seen.add(school)
                schools.append(school)

    return schools


def write_json(path: str, rows: List[DetectionResult]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in rows], f, indent=2, ensure_ascii=False)


def write_csv(path: str, rows: List[DetectionResult]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "school",
                "system",
                "confidence",
                "matched_pattern",
                "matched_url",
                "source_url",
                "notes",
            ],
        )
        w.writeheader()
        for row in rows:
            w.writerow(asdict(row))


def summarize(results: List[DetectionResult]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for r in results:
        key = r.system or "Unknown"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", default="schools.txt", help="Input file with one school domain per line")
    parser.add_argument("--workers", type=int, default=20, help="Parallel worker count")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout per request")
    parser.add_argument("--json-out", default="job_systems.json", help="JSON output file")
    parser.add_argument("--csv-out", default="job_systems.csv", help="CSV output file")
    args = parser.parse_args()

    schools = load_schools(args.inputs)
    if not schools:
        print("No schools found in input.", file=sys.stderr)
        return 1

    print(f"Loaded {len(schools)} schools", file=sys.stderr)

    results: List[DetectionResult] = []
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(scan_school, school, args.timeout): school
            for school in schools
        }

        for i, fut in enumerate(as_completed(future_map), start=1):
            school = future_map[fut]

            try:
                result = fut.result()
            except Exception as e:
                result = DetectionResult(
                    school=school,
                    system=None,
                    confidence=0.0,
                    matched_pattern=None,
                    matched_url=None,
                    source_url=None,
                    notes=f"Unhandled error: {e}",
                )

            with lock:
                results.append(result)

            label = result.system or "Unknown"
            print(
                f"[{i}/{len(schools)}] {school:<35} -> {label:<18} conf={result.confidence:.3f}",
                file=sys.stderr,
            )

            if STOP:
                break

    results.sort(key=lambda r: r.school)

    write_json(args.json_out, results)
    write_csv(args.csv_out, results)

    counts = summarize(results)
    print("\nSummary:", file=sys.stderr)
    for system, count in counts.items():
        print(f"  {system:<18} {count}", file=sys.stderr)

    print(
        f"\nWrote {args.json_out}\nWrote {args.csv_out}",
        file=sys.stderr,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())