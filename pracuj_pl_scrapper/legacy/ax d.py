import os
import json
import time
import cloudscraper
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
from month import Month
from collections import defaultdict


def make_scraper():
    scraper = cloudscraper.create_scraper()
    scraper.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Connection": "close"
    })
    return scraper


def get_soup(url, max_retries=5):
    scraper = make_scraper()
    try:
        for attempt in range(max_retries):
            try:
                resp = scraper.get(url, timeout=15)
                if resp.status_code == 200:
                    return BeautifulSoup(resp.text, "html.parser")
                elif resp.status_code in [429, 503]:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    print(f"⚠️ Nieoczekiwany status {resp.status_code} dla {url}")
                    return None
            except Exception as e:
                print(f"⚠️ Wyjątek przy pobieraniu {url}: {e}")
                time.sleep(2 + attempt)
        return None
    finally:
        try:
            scraper.close()
        except:
            pass



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
            while parent and parent.name != "[document]":
                scroll_id = parent.get("data-scroll-id", "").lower()
                if scroll_id in RESP_SECTION_MAP:
                    section = RESP_SECTION_MAP[scroll_id]
                    break
                parent = parent.parent

        grouped_results[section].append(text)

    return dict(grouped_results)


def collect_job_details(job_meta):
    job_url = job_meta["link"]
    soup = get_soup(job_url)
    if not soup:
        return None

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

    if "–" in salary:
        salary_low, salary_high = map(str.strip, salary.split("–"))
    else:
        salary_low, salary_high = salary, ""

    return {
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
        "link_main": job_meta.get("link", ""),
        "title_main": job_meta.get("title", ""),
        "company_main": job_meta.get("company", ""),
        "location_main": job_meta.get("location", ""),
        "date_main": job_meta.get("date", "")
    }


def update_json(file_path, new_data):
    existing = []
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except:
            existing = []

    existing_urls = {item["url"] for item in existing}
    merged = existing + [item for item in new_data if item["url"] not in existing_urls]

    tmp = file_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    os.replace(tmp, file_path)


def collect_job_details_from_links(year, links_file, output_dir, max_workers=10):
    with open(links_file, "r", encoding="utf-8") as f:
        links_data = json.load(f)

    if not links_data:
        print(f"⚠️ No links for {year}")
        return

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"details_{year}.json")
    results = []

    total = len(links_data)
    print(f"📅 Collecting {total} offers for {year}...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(collect_job_details, job): job for job in links_data}
        completed = 0
        total = len(futures)

        for future in as_completed(futures):
            job_meta = futures[future]
            completed += 1
            try:
                res = future.result()
                if res:
                    results.append(res)
                    print(f"[{completed}/{total}] {res['title']} – {res['company_name']}")
                else:
                    print(f"[{completed}/{total}] ❌ Błąd: {job_meta.get('link')}")
            except Exception as e:
                print(f"[{completed}/{total}] ❌ Wyjątek przy {job_meta.get('link')}: {e}")

    update_json(output_file, results)
    print(f"💾 Saved {len(results)} offers → {output_file}")


def split_links_by_month(links_data):
    """Group job links by month (YYYY-MM)."""
    grouped = defaultdict(list)
    for item in links_data:
        date_str = item.get("date", "")
        try:
            month_key = date_str[:7]  # "YYYY-MM"
            grouped[month_key].append(item)
        except:
            continue
    return grouped


if __name__ == "__main__":
    start_year = 2020
    end_year = 2025
    links_dir = "/done_merged/"
    output_dir = "../job_details_json"

    for year in range(start_year, end_year + 1):
        f = os.path.join(links_dir, f"pracujpl_links_{year}_all_filtered.json")
        if not os.path.exists(f):
            print(f"🚫 Missing file: {f}")
            continue

        with open(f, "r", encoding="utf-8") as fp:
            links_data = json.load(fp)

        grouped = split_links_by_month(links_data)

        for month_key, month_links in grouped.items():
            year_str, month_str = month_key.split("-")
            out_links_file = os.path.join(
                links_dir, f"pracujpl_details_{year_str}_{month_str}.json"
            )

            with open(out_links_file, "w", encoding="utf-8") as fp:
                json.dump(month_links, fp, ensure_ascii=False, indent=2)

            print(f"📅 Processing {month_key}: {len(month_links)} links")
            collect_job_details_from_links(month_key, out_links_file, output_dir)
            gc.collect()
