#!/usr/bin/env python3
"""
sandbox5.py

Read schools.txt (one school domain per line, e.g. unc.edu), probe likely
career/job URLs, and classify the institution's job posting system.

Outputs:
  1) job_systems.json
  2) job_systems.csv

Fields:
  - school
  - system
  - confidence
  - matched_pattern
  - matched_url
  - notes

Usage:
  python sandbox5.py
  python sandbox5.py --inputs schools.txt --workers 20 --timeout 12

Dependencies:
  pip install requests beautifulsoup4

This is intentionally heuristic. It aims for:
  - strong URL/domain matches first
  - then page text / HTML fallback
  - sensible confidence scoring
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
from typing import List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


STOP = False


def handle_sigint(sig, frame):
    global STOP
    STOP = True
    print("\nStopping early after current requests finish...", file=sys.stderr)


signal.signal(signal.SIGINT, handle_sigint)


@dataclass
class DetectionResult:
    school: str
    system: Optional[str]
    confidence: float
    matched_pattern: Optional[str]
    matched_url: Optional[str]
    notes: Optional[str]


COMMON_PATHS = [
    "",
    "/jobs",
    "/employment",
    "/careers",
    "/hr",
    "/about/jobs",
    "/about/employment",
    "/faculty-staff/jobs",
    "/human-resources",
    "/human-resources/careers",
    "/jobs/openings",
    "/postings/search",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def normalize_school(line: str) -> Optional[str]:
    s = line.strip()
    if not s:
        return None
    s = s.replace("https://", "").replace("http://", "")
    s = s.split("/")[0].strip().lower()
    if s.startswith("www."):
        s = s[4:]
    return s or None


def build_candidate_urls(school: str) -> List[str]:
    urls = []

    # Direct school-domain possibilities
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
    ]

    seen = set()
    for base in bases:
        if base not in seen:
            urls.append(base)
            seen.add(base)
        for path in COMMON_PATHS:
            full = base.rstrip("/") + path
            if full not in seen:
                urls.append(full)
                seen.add(full)

    # Higher-ed ATS-specific common vendor host patterns
    vendor_guesses = [
        f"https://{school.split('.')[0]}.peopleadmin.com",
        f"https://{school.split('.')[0]}.peopleadmin.com/postings/search",
        f"https://careers.pageuppeople.com",
        f"https://apply.interfolio.com",
    ]
    for v in vendor_guesses:
        if v not in seen:
            urls.append(v)
            seen.add(v)

    return urls


def fetch_url(session: requests.Session, url: str, timeout: int) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns: final_url, html, error
    """
    try:
        r = session.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
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


def score_match(system: str, pattern: str, url: str, text: str, html: str) -> float:
    u = (url or "").lower()
    t = text.lower()
    h = html.lower()

    score = 0.0

    if system == "PeopleAdmin":
        if "peopleadmin.com" in u:
            score += 0.75
        if "/postings/search" in u:
            score += 0.75
        if "search postings" in t:
            score += 0.20
        if "all jobs atom feed" in t:
            score += 0.20
        if "view all open postings below" in t:
            score += 0.15

    elif system == "PageUp":
        if "pageuppeople.com" in u:
            score += 0.80
        if "/cw/en-us/listing" in u or "/en/listing" in u:
            score += 0.60
        if "recent jobs" in t:
            score += 0.20

    elif system == "Workday":
        if "myworkdayjobs.com" in u:
            score += 0.85
        if "search for jobs" in t:
            score += 0.20
        if "jobs found" in t:
            score += 0.15
        if "jump to selected job details" in t:
            score += 0.15
        if '"externalcareer"' in h or '"jobpostinginfo"' in h:
            score += 0.15

    elif system == "Interfolio":
        if "apply.interfolio.com" in u or "account.interfolio.com" in u:
            score += 0.85
        if "faculty search" in t:
            score += 0.15
        if "interfolio" in t:
            score += 0.15

    elif system == "NEOGOV/NeoEd":
        if "governmentjobs.com" in u:
            score += 0.85
        if "neoed.com" in u:
            score += 0.75
        if "schooljobs" in t:
            score += 0.15
        if "governmentjobs" in t:
            score += 0.15

    elif system == "Taleo":
        if "taleo.net" in u:
            score += 0.90
        if "oracle" in t and "taleo" in t:
            score += 0.10

    elif system == "iCIMS":
        if "icims.com" in u:
            score += 0.90
        if "icims" in t:
            score += 0.10

    elif system == "SmartRecruiters":
        if "smartrecruiters.com" in u:
            score += 0.90
        if "smartrecruiters" in t:
            score += 0.10

    elif system == "UKG / UltiPro":
        if "ultipro.com" in u or "ukg.com" in u:
            score += 0.85
        if "ultipro" in t or "ukg" in t:
            score += 0.15

    elif system == "Jobvite":
        if "jobvite.com" in u:
            score += 0.90
        if "jobvite" in t:
            score += 0.10

    return min(score, 0.99)


def detect_system_from_content(final_url: str, html: str) -> Tuple[Optional[str], Optional[str], float, Optional[str]]:
    u = (final_url or "").lower()
    text = extract_visible_text(html)
    h = html.lower()

    checks = [
        ("PeopleAdmin", "peopleadmin.com domain", lambda: "peopleadmin.com" in u),
        ("PeopleAdmin", "/postings/search path", lambda: "/postings/search" in u),
        ("PeopleAdmin", "Search Postings text", lambda: "search postings" in text),
        ("PeopleAdmin", "All Jobs Atom Feed text", lambda: "all jobs atom feed" in text),

        ("PageUp", "pageuppeople.com domain", lambda: "pageuppeople.com" in u),
        ("PageUp", "PageUp listing path", lambda: "/cw/en-us/listing" in u or "/en/listing" in u),
        ("PageUp", "Recent Jobs text", lambda: "recent jobs" in text),

        ("Workday", "myworkdayjobs.com domain", lambda: "myworkdayjobs.com" in u),
        ("Workday", "Search for Jobs text", lambda: "search for jobs" in text),
        ("Workday", "Jobs Found text", lambda: "jobs found" in text),
        ("Workday", "Workday JSON markers", lambda: '"externalcareer"' in h or '"jobpostinginfo"' in h),

        ("Interfolio", "apply.interfolio.com domain", lambda: "apply.interfolio.com" in u),
        ("Interfolio", "account.interfolio.com domain", lambda: "account.interfolio.com" in u),
        ("Interfolio", "Faculty Search text", lambda: "faculty search" in text),

        ("NEOGOV/NeoEd", "governmentjobs.com domain", lambda: "governmentjobs.com" in u),
        ("NEOGOV/NeoEd", "neoed.com domain", lambda: "neoed.com" in u),
        ("NEOGOV/NeoEd", "SchoolJobs text", lambda: "schooljobs" in text),

        ("Taleo", "taleo.net domain", lambda: "taleo.net" in u),
        ("iCIMS", "icims.com domain", lambda: "icims.com" in u),
        ("SmartRecruiters", "smartrecruiters.com domain", lambda: "smartrecruiters.com" in u),
        ("UKG / UltiPro", "ultipro.com domain", lambda: "ultipro.com" in u),
        ("Jobvite", "jobvite.com domain", lambda: "jobvite.com" in u),
    ]

    candidates = []
    for system, pattern, fn in checks:
        try:
            if fn():
                conf = score_match(system, pattern, final_url, text, html)
                candidates.append((system, pattern, conf))
        except Exception:
            pass

    if not candidates:
        return None, None, 0.0, None

    candidates.sort(key=lambda x: x[2], reverse=True)
    best_system, best_pattern, best_conf = candidates[0]

    note = None
    if len(candidates) > 1:
        second = candidates[1]
        if second[2] >= best_conf - 0.10 and second[0] != best_system:
            note = f"Also matched {second[0]} via {second[1]}"

    return best_system, best_pattern, best_conf, note


def scan_school(school: str, timeout: int) -> DetectionResult:
    if STOP:
        return DetectionResult(school, None, 0.0, None, None, "Stopped before scan")

    session = requests.Session()
    session.headers.update(HEADERS)

    best: Optional[DetectionResult] = None
    errors = []
    checked = 0

    for url in build_candidate_urls(school):
        if STOP:
            break

        checked += 1
        final_url, html, err = fetch_url(session, url, timeout=timeout)
        if err:
            errors.append(f"{url} -> {err}")
            continue
        if not final_url or not html:
            continue

        system, pattern, conf, note = detect_system_from_content(final_url, html)
        if system:
            result = DetectionResult(
                school=school,
                system=system,
                confidence=round(conf, 3),
                matched_pattern=pattern,
                matched_url=final_url,
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
        note += f"; sample error: {errors[0][:200]}"
    return DetectionResult(
        school=school,
        system=None,
        confidence=0.0,
        matched_pattern=None,
        matched_url=None,
        notes=note,
    )


def load_schools(path: str) -> List[str]:
    schools = []
    seen = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            school = normalize_school(line)
            if not school:
                continue
            if school not in seen:
                schools.append(school)
                seen.add(school)
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
                "notes",
            ],
        )
        w.writeheader()
        for row in rows:
            w.writerow(asdict(row))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", default="schools.txt", help="Input file with one school domain per line")
    parser.add_argument("--workers", type=int, default=20, help="Parallel worker count")
    parser.add_argument("--timeout", type=int, default=12, help="HTTP timeout per request")
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

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        future_map = {
            ex.submit(scan_school, school, args.timeout): school
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
                    notes=f"Unhandled error: {e}",
                )

            with lock:
                results.append(result)

            label = result.system or "Unknown"
            print(
                f"[{i}/{len(schools)}] {school:<35} -> {label:<18} "
                f"conf={result.confidence:.3f}",
                file=sys.stderr,
            )

            if STOP:
                break

    results.sort(key=lambda r: r.school)
    write_json(args.json_out, results)
    write_csv(args.csv_out, results)

    found = sum(1 for r in results if r.system)
    print(
        f"\nDone. Detected known systems for {found}/{len(results)} schools.\n"
        f"Wrote {args.json_out}\n"
        f"Wrote {args.csv_out}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())