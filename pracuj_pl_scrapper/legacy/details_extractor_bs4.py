import os
import json
import time
import cloudscraper
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from month import Month


# ---------- LOCAL SCRAPER PER THREAD ----------
def make_scraper():
    scraper = cloudscraper.create_scraper()
    scraper.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Connection": "close"   # <-- KLUCZOWE: brak keep-alive
    })
    return scraper


# ---------- SAFE get_soup ----------
def get_soup(url, max_retries=5):
    scraper = make_scraper()

    for attempt in range(max_retries):
        try:
            resp = scraper.get(url, timeout=10)

            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue

            if resp.status_code == 200:
                return BeautifulSoup(resp.text, "html.parser")  # stabilny parser

            return None

        except Exception:
            time.sleep(1 + attempt)

    return None


# ---------- JOB DETAILS ----------
def collect_job_details(job_meta):
    job_url = job_meta["link"]
    soup = get_soup(job_url)
    if not soup:
        return None

    def safe_text(selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else ""

    title = safe_text('[data-test="text-positionName"]')
    company_name = safe_text('h2[data-scroll-id="employer-name"]')
    location = safe_text('[data-test="sections-benefit-workplaces"] [data-test="offer-badge-title"]')
    salary = safe_text('[data-test="text-earningAmount"]')
    work_type = safe_text('[data-test="sections-benefit-work-schedule"] [data-test="offer-badge-title"]')
    experience = safe_text('[data-test="sections-benefit-employment-type-name"] [data-test="offer-badge-title"]')
    contract_type = safe_text('[data-test="sections-benefit-contracts"] [data-test="offer-badge-title"]')
    operating_mode = safe_text('[data-scroll-id="work-modes"] [data-test="offer-badge-title"]')

    technologies = [t.get_text(strip=True) for t in soup.select('[data-test="item-technologies-expected"]')]
    technologies_optional = [t.get_text(strip=True) for t in soup.select('[data-test="item-technologies-optional"]')]
    specification = [t.get_text(strip=True) for t in soup.select("li.offer-view_tkzmjn3")]

    all_technologies = technologies + technologies_optional

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
        "technologies": all_technologies,
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
        futures = {executor.submit(collect_job_details, job): job for job in links_data[:20]}

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


if __name__ == "__main__":
    start_year = 2025
    end_year = 2025
    links_dir = "/done_merged/"
    output_dir = "../job_details_json"

    for year in range(start_year, end_year + 1):
        f = os.path.join(links_dir, f"pracujpl_links_{year}_all_filtered.json")
        if os.path.exists(f):
            collect_job_details_from_links(year, f, output_dir)
        else:
            print(f"🚫 Missing file: {f}")
