import os
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import cloudscraper
from bs4 import BeautifulSoup
from month import Month

def collect_links(year, month, path_exist):
    """Collect job links and metadata for all pages in a given year and month, save to JSON."""
    json_file = f'{path_exist}/pracujpl_links_{year}_{month}.json'
    base_url = "https://archiwum.pracuj.pl"
    scraper = cloudscraper.create_scraper()

    all_offers = []
    page_num = 1

    while True:
        url = f'{base_url}/archive/offers?Year={year}&Month={month}&PageNumber={page_num}'
        resp = scraper.get(url)
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

        # Check if there is a "Next" button
        next_button = soup.select_one(".offers_nav_next")
        if not next_button:
            print(f"No 'Next' button found. Stopping collecting for year {year}, month {Month(month)}.")
            break

        page_num += 1
        time.sleep(1)

    # Save to JSON
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(all_offers, f, ensure_ascii=False, indent=4)

def collect_links_for_year_all_months(year):
    path_exist = f'C:/Users/Tomasz/PycharmProjects/PythonProject/{year}'
    os.makedirs(path_exist, exist_ok=True)

    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [
            executor.submit(collect_links, year=year, month=month, path_exist=path_exist)
            for month in range(1, 6)
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Scraping error for year {year}: {str(e)}")

if __name__ == "__main__":
    for year in range(2016, 2026):
        start_time = time.time()
        collect_links_for_year_all_months(year)
    
        elapsed = time.time() - start_time
        print(f"Collected links for all months of year {year} in {elapsed:.2f} seconds")
