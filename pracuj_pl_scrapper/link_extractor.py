import os
import time
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import cloudscraper
from bs4 import BeautifulSoup
from month import Month


def collect_links(year, month, path_exist, max_retries=5):
    """Collect job links and metadata for all pages in a given year and month, save to JSON."""
    json_file = f'{path_exist}/pracujpl_links_{year}_{month}.json'
    base_url = "https://archiwum.pracuj.pl"
    scraper = cloudscraper.create_scraper()

    all_offers = []
    page_num = 1

    while True:
        url = f'{base_url}/archive/offers?Year={year}&Month={month}&PageNumber={page_num}'
        retry_count = 0
        while retry_count < max_retries:
            resp = scraper.get(url)
            if resp.status_code == 429:
                wait = 2 ** retry_count
                print(f"429 received for {year}-{Month(month)} page {page_num}, waiting {wait}s before retry")
                time.sleep(wait)
                retry_count += 1
            else:
                break
        else:
            print(f"Too many 429 responses for {year}-{Month(month)} page {page_num}. Skipping page.")
            break

        if resp.status_code != 200:
            print(f"Page {page_num} returned {resp.status_code}, stopping for {year}-{month}.")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        offers = soup.select(".offers_item")

        if not offers:
            print(f"No offers found for year {year}, month {month}, page {page_num}. Stopping.")
            break

        print(f"Collected {len(offers)} offers for {year}-{Month(month)} page {page_num}.")

        for offer in offers:
            link_tag = offer.select_one(".offers_item_link[href]")
            link = urljoin(base_url, link_tag["href"]) if link_tag else ""

            parts = offer.select(".offers_item_link_cnt_part")
            title = parts[0].get_text(strip=True) if len(parts) > 0 else ""
            company = parts[1].get_text(strip=True) if len(parts) > 1 else ""

            loc_tag = offer.select_one(".offers_item_desc_loc")
            location = loc_tag.get_text(strip=True) if loc_tag else ""

            date_tag = offer.select_one(".offers_item_desc_date")
            date = date_tag.get_text(strip=True) if date_tag else ""

            all_offers.append({
                "link": link,
                "title": title,
                "company": company,
                "location": location,
                "date": date
            })

        next_button = soup.select_one(".offers_nav_next")
        if not next_button:
            print(f"No 'Next' button found. Stopping collecting for year {year}, month {Month(month)}.")
            break

        page_num += 1
        time.sleep(random.uniform(1, 3))

    # Save to JSON
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(all_offers, f, ensure_ascii=False, indent=4)


def collect_links_all_years(start_year=2017, end_year=2025, max_workers=12):
    """Run scraping in parallel across all months and years with shared workers."""
    tasks = []
    for year in range(start_year, end_year + 1):
        path_exist = f'C:/Users/Tomasz/PycharmProjects/PythonProject/{year}'
        os.makedirs(path_exist, exist_ok=True)
        for month in range(1, 13):
            tasks.append((year, month, path_exist))

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(collect_links, year, month, path): (year, month)
                   for year, month, path in tasks}

        for future in as_completed(futures):
            year, month = futures[future]
            try:
                future.result()
                print(f"✅ Finished {year}-{Month(month)}")
            except Exception as e:
                print(f"❌ Error for {year}-{month}: {e}")

    elapsed = time.time() - start_time
    print(f"All scraping done in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    collect_links_all_years()
