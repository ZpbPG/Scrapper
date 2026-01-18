import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from month import Month
from utils import get_driver, quit_drivers

def collect_links(year, month, path_exist):
    """collect job links for all pages in a given year and month until no more pages are available."""

    csv_file = f'{path_exist}/pracujpl_links_{year}_{month}.csv'
    page_num = 1
    driver = get_driver()

    with open(csv_file, 'w', newline='', encoding='cp1250') as csvfile:
        writer = csv.writer(csvfile)

        while True:
            driver.get(f'https://archiwum.pracuj.pl/archive/offers?Year={year}&Month={month}&PageNumber={page_num}')

            try:
                WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@data-test="button-submitCookie"]'))).click()
            except:
                pass

            links = driver.find_elements(By.XPATH, "//*[@class='offers_item_link']")
            print(f"Collecting year {year}, month {str(Month(month))}, page {page_num}")

            for link in links:
                writer.writerow([link.get_attribute('href')])
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//*[@class='offers_nav_next']"))
                )
                page_num += 1
            except:
                print(f"No 'Next' button found. Stopping collecting for year {year}, month {str(Month(month))}.")
                break


def collect_links_for_year_all_months(year):
    """Collect job links for all months of a specified year."""
    path_exist = f'C:/Users/Tomasz/PycharmProjects/PythonProject/{year}'
    if not os.path.exists(path_exist):
        os.makedirs(path_exist)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for month in range(1,13):
            futures.append(executor.submit(collect_links, year=year, month=month, path_exist=path_exist))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Scraping error for year {year}: {str(e)}")


if __name__ == "__main__":
    try:
        year_to_collect = 2024
        start_collect_time = time.time()
        collect_links_for_year_all_months(year_to_collect)
        end_collect_time = time.time()
        collect_elapsed_time = end_collect_time - start_collect_time
        print(f"collectd links for all months of year {year_to_collect} in {collect_elapsed_time:.4f} seconds")
    finally:
        quit_drivers()
