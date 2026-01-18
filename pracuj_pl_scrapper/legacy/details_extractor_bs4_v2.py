#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import threading
import gc
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import cloudscraper
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------------------------
# Configuration
# -------------------------
MAX_WORKERS = 12
REQUEST_TIMEOUT = 10
MAX_RETRIES = 5
GC_INTERVAL = 1000  # call gc.collect after this many processed items
NDJSON_SUFFIX = ".ndjson"  # incremental append file
FINAL_JSON_SUFFIX = ".json"  # final merged array file

# -------------------------
# Thread-local scraper
# -------------------------
_thread_local = threading.local()


def make_scraper():
    """
    Build a configured cloudscraper session.
    This should be called once per thread and reused.
    """
    scraper = cloudscraper.create_scraper(
        browser={'custom': 'firefox'}  # avoid built-in brotli header courtesy in some configs
    )

    # Avoid brotli decoding to reduce memory pressure:
    # keep gzip/deflate only
    scraper.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; scraper/1.0)",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive"
    })

    # Tune connection pool sizes so urllib3 won't grow pools dynamically
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=Retry(
        total=0,  # we implement our own retry/backoff
        connect=0, read=0
    ))
    scraper.mount("https://", adapter)
    scraper.mount("http://", adapter)
    return scraper


def get_scraper():
    """Return a thread-local scraper instance (create if missing)."""
    if not hasattr(_thread_local, "scraper"):
        _thread_local.scraper = make_scraper()
    return _thread_local.scraper


# -------------------------
# Helpers for fetching and parsing
# -------------------------
def get_soup(url, max_retries=MAX_RETRIES):
    """
    Fetch URL using the thread-local scraper. Implements exponential backoff on 429.
    Returns BeautifulSoup on success, or None on failure.
    """
    scraper = get_scraper()
    backoff = 1

    for attempt in range(max_retries):
        try:
            resp = scraper.get(url, timeout=REQUEST_TIMEOUT)
            # If response exists, ensure we close it after getting text to release connection resources
            if resp.status_code == 429:
                # Too many requests -> exponential backoff
                resp.close()
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code != 200:
                resp.close()
                return None

            text = resp.text
            resp.close()

            # Create soup and return
            soup = BeautifulSoup(text, "html.parser")
            return soup

        except Exception:
            # Sleep a little bit before retrying; do not create many objects here
            time.sleep(1 + attempt)
            continue

    return None


RESP_SECTION_MAP = {
    "section-requirements": "requirements",
    "section-technologies": "technologies",
    "requirements": "requirements",
    "technologies-expected-1": "technologies_expected_1",
    "technologies-optional-1": "technologies_optional_1",

    "requirements-expected": "requirements_expected",
    "requirements-expected-1": "requirements_expected_1",
    "requirements-expected-2": "requirements_expected_2",

    "requirements-optional": "requirements_optional",
    "requirements-optional-1": "requirements_optional_1",
    "requirements-optional-2": "requirements_optional_2",

    "section-must-have": "requirements",
    "must-have": "requirements",

    "section-responsibilities": "responsibilities",
    "responsibilities": "responsibilities",
    "responsibilities-1": "responsibilities_1",
    "responsibilities-2": "responsibilities_2",

    "responsibilities-expected": "responsibilities_expected",
    "responsibilities-expected-1": "responsibilities_expected_1",
    "responsibilities-expected-2": "responsibilities_expected_2",

    "responsibilities-optional": "responsibilities_optional",
    "responsibilities-optional-1": "responsibilities_optional_1",
    "responsibilities-optional-2": "responsibilities_optional_2",

    "section-responsibilities-header": "responsibilities_header",
    "section-missions-header": "missions_header",
    "section-requirements-header": "requirements_header",

    "section-offered": "offered",
    "offered": "offered",
    "offered-1": "offered",
}


def extract_classified_list_items(soup):
    """
    Extract list items and group them by section, using the RESP_SECTION_MAP.
    """
    grouped_results = defaultdict(list)

    li_elements = soup.select(
        "li.offer-view_tkzmjn3, "
        "li.offer-view_catru5k, "
        "li[data-test='item-technologies-os'], "
        "li[data-test^='item-technologies']"
    )

    for li in li_elements:
        text = li.get_text(" ", strip=True)
        section = "unknown"

        if li.get("data-test") == "item-technologies-os":
            mask = li.select_one("mask[id]")
            if mask:
                mask_id = mask.get("id", "")
                if mask_id.startswith("gp_system_"):
                    text = mask_id.replace("gp_system_", "")
            section = "technologies_os"
        else:
            parent = li.parent
            # climb the DOM looking for data-scroll-id attribute
            while parent and getattr(parent, "name", None) != "[document]":
                scroll_id = (parent.get("data-scroll-id", "") or "").lower()
                if scroll_id in RESP_SECTION_MAP:
                    section = RESP_SECTION_MAP[scroll_id]
                    break
                parent = parent.parent

        grouped_results[section].append(text)

    return dict(grouped_results)


def collect_job_details(job_meta):
    """
    Given job_meta (dict with 'link', 'title', 'company', 'location', 'date'),
    fetch the job page and extract structured details.
    Returns a dict or None on failure.
    """
    job_url = job_meta.get("link")
    if not job_url:
        return None

    soup = get_soup(job_url)
    if not soup:
        return None

    try:
        def safe_text(selector):
            el = soup.select_one(selector)
            return el.get_text(strip=True) if el else ""

        def safe_text_company(selector):
            el = soup.select_one(selector)
            if el:
                text_node = el.find(string=True, recursive=False)
                return text_node.strip() if text_node else ""
            return ""

        title = safe_text('[data-test="text-positionName"]')
        company_name = safe_text_company('h2[data-scroll-id="employer-name"]')
        location = safe_text('[data-test="sections-benefit-workplaces"] [data-test="offer-badge-title"]')
        salary = safe_text('[data-test="text-earningAmount"]')
        work_type = safe_text('[data-test="sections-benefit-work-schedule"] [data-test="offer-badge-title"]')
        experience = safe_text('[data-test="sections-benefit-employment-type-name"] [data-test="offer-badge-title"]')
        contract_type = safe_text('[data-test="sections-benefit-contracts"] [data-test="offer-badge-title"]')
        operating_mode = safe_text('[data-scroll-id="work-modes"] [data-test="offer-badge-title"]')

        specification = extract_classified_list_items(soup)

        # Parse salary range if present using en-dash '–' or hyphen '-'
        salary_low, salary_high = "", ""
        if salary:
            if "–" in salary:
                salary_low, salary_high = map(str.strip, salary.split("–", 1))
            elif "-" in salary:
                salary_low, salary_high = map(str.strip, salary.split("-", 1))
            else:
                salary_low = salary

        result = {
            "url": job_url,
            "title": title or job_meta.get("title", ""),
            "company_name": company_name or job_meta.get("company", ""),
            "location": location or job_meta.get("location", ""),
            "salary_low": salary_low,
            "salary_high": salary_high,
            "work_type": work_type,
            "experience": experience,
            "contract_type": contract_type,
            "operating_mode": operating_mode,
            "specification": specification,
            "date": job_meta.get("date", ""),

            # keep original references for traceability
            "link_main": job_meta.get("link", ""),
            "title_main": job_meta.get("title", ""),
            "company_main": job_meta.get("company", ""),
            "location_main": job_meta.get("location", ""),
            "date_main": job_meta.get("date", "")
        }
        return result

    finally:
        # free the soup DOM immediately to release memory
        try:
            soup.decompose()
        except Exception:
            pass


# -------------------------
# Incremental NDJSON writer utilities
# -------------------------
def load_existing_urls_from_ndjson_or_json(path):
    """
    Return a set of URLs already present in an existing output file.
    Accepts either NDJSON or JSON array formats.
    """
    existing = set()
    if not os.path.exists(path):
        return existing

    try:
        with open(path, "r", encoding="utf-8") as f:
            # Quick heuristics: if first non-space char is '[' -> regular JSON array
            first_char = f.read(1)
            if not first_char:
                return existing
            f.seek(0)
            if first_char == "[":
                arr = json.load(f)
                for item in arr:
                    url = item.get("url")
                    if url:
                        existing.add(url)
            else:
                # treat as NDJSON - each line is JSON
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        url = obj.get("url")
                        if url:
                            existing.add(url)
                    except Exception:
                        continue
    except Exception:
        # On failure, return empty set: we'll deduplicate as we go
        pass

    return existing


def append_to_ndjson_file(path, obj):
    """
    Append a single JSON object as a line to an NDJSON file.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as fo:
        fo.write(json.dumps(obj, ensure_ascii=False) + "\n")


def convert_ndjson_to_json_array(ndjson_path, json_path):
    """
    Convert NDJSON file (one JSON object per line) into a standard JSON array file.
    This is called at the end of processing for compatibility with previous workflow.
    """
    objects = []
    try:
        with open(ndjson_path, "r", encoding="utf-8") as fr:
            for line in fr:
                line = line.strip()
                if not line:
                    continue
                try:
                    objects.append(json.loads(line))
                except Exception:
                    continue
        # Write final JSON array atomically
        tmp = json_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fw:
            json.dump(objects, fw, ensure_ascii=False, indent=2)
        os.replace(tmp, json_path)
    except Exception:
        # If conversion fails, don't crash the scraper run; user can attempt later
        pass


# -------------------------
# Main collection function (month-level)
# -------------------------
def collect_job_details_from_links(month_key, links_file, output_dir, max_workers=MAX_WORKERS):
    """
    Process a month's links file (JSON array of job_meta dictionaries).
    Writes incremental results to NDJSON and then produces a final JSON array.
    """
    with open(links_file, "r", encoding="utf-8") as f:
        links_data = json.load(f)

    if not links_data:
        print(f"⚠️ No links for {month_key}")
        return

    os.makedirs(output_dir, exist_ok=True)
    ndjson_file = os.path.join(output_dir, f"details_{month_key}{NDJSON_SUFFIX}")
    final_json_file = os.path.join(output_dir, f"details_{month_key}{FINAL_JSON_SUFFIX}")

    # Load existing URLs if output exists; prevents duplicates across runs
    existing_urls = load_existing_urls_from_ndjson_or_json(ndjson_file) | load_existing_urls_from_ndjson_or_json(final_json_file)

    total = len(links_data)
    print(f"📅 Collecting {total} offers for {month_key} (will append to {ndjson_file})...")

    processed = 0
    # Use executor.map so we don't keep many Future objects in memory
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for res in executor.map(collect_job_details, links_data):
            processed += 1

            if res:
                url = res.get("url")
                if url and url not in existing_urls:
                    append_to_ndjson_file(ndjson_file, res)
                    existing_urls.add(url)
                    print(f"[{processed}/{total}] {res.get('title','<no title>')} – {res.get('company_name','')}")
                else:
                    # duplicate or missing url
                    print(f"[{processed}/{total}] skipped (duplicate or bad)")

            else:
                # failed fetch or parse
                print(f"[{processed}/{total}] ❌ fetch/parse failed")

            # conservative garbage collection after many iterations
            if processed % GC_INTERVAL == 0:
                gc.collect()

    # After loop, create final JSON array file (atomic)
    convert_ndjson_to_json_array(ndjson_file, final_json_file)
    print(f"💾 Completed {processed} offers → NDJSON: {ndjson_file}, JSON: {final_json_file}")


# -------------------------
# Utilities
# -------------------------
def split_links_by_month(links_data):
    """Group job links by month (YYYY-MM)."""
    grouped = defaultdict(list)

    for item in links_data:
        date_str = item.get("date", "")
        if not date_str or len(date_str) < 7:
            continue
        try:
            month_key = date_str[:7]  # "YYYY-MM"
            grouped[month_key].append(item)
        except Exception:
            continue

    return grouped


# -------------------------
# CLI entrypoint
# -------------------------
if __name__ == "__main__":
    start_year = 2024
    end_year = 2025
    links_dir = r"/done_merged"
    output_dir = "../job_details_json"
    max_workers = MAX_WORKERS

    for year in range(start_year, end_year + 1):
        fpath = os.path.join(links_dir, f"pracujpl_links_{year}_all_filtered_v2.json")

        if not os.path.exists(fpath):
            print(f"🚫 Missing file: {fpath}")
            continue

        with open(fpath, "r", encoding="utf-8") as fp:
            links_data = json.load(fp)

        grouped = split_links_by_month(links_data)

        for month_key, month_links in grouped.items():
            year_str, month_str = month_key.split("-")
            out_links_file = os.path.join(links_dir, f"pracujpl_details_{year_str}_{month_str}.json")

            # Save month subset just like you did before (keeps the on-disk monthly links)
            with open(out_links_file, "w", encoding="utf-8") as fp:
                json.dump(month_links, fp, ensure_ascii=False, indent=2)

            print(f"📅 Processing {month_key}: {len(month_links)} links")

            # Collect job details for this month
            collect_job_details_from_links(
                month_key,
                out_links_file,
                output_dir,
                max_workers=max_workers
            )

    print("All done.")
