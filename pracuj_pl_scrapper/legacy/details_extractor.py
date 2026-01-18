import csv
import os
from month import Month
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from utils import get_driver, quit_drivers

def collect_job_details(job_url):
    driver = get_driver()
    driver.get(job_url)

    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@data-test="button-submitCookie"]'))).click()
    except:
        pass

    try:
        title = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@data-test="text-positionName"]'))).text
    except:
        title = ""
    try:
        company_name = driver.find_element(By.XPATH, '//h2[@data-scroll-id="employer-name"]').text.replace("O firmie",
                                                                                                           "")
    except:
        company_name = ""
    try:
        location = driver.find_element(By.XPATH,
                                       '//li[@data-test="sections-benefit-workplaces"]//div[@data-test="offer-badge-title"]').text
    except:
        location = ""
    try:
        salary = driver.find_element(By.XPATH, '//*[@data-test="text-earningAmount"]').text
    except:
        salary = ""
    try:
        work_type = driver.find_element(By.XPATH,
                                        '//li[@data-test="sections-benefit-work-schedule"]//div[@data-test="offer-badge-title"]').text
    except:
        work_type = ""
    try:
        experience = driver.find_element(By.XPATH,
                                         '//li[@data-test="sections-benefit-employment-type-name"]//div[@data-test="offer-badge-title"]').text
    except:
        experience = ""
    try:
        contract_type = driver.find_element(By.XPATH,
                                            '//li[@data-test="sections-benefit-contracts"]//div[@data-test="offer-badge-title"]').text
    except:
        contract_type = ""
    try:
        operating_mode = driver.find_element(By.XPATH,
                                             '//li[@data-scroll-id="work-modes"]//div[@data-test="offer-badge-title"]').text
    except:
        operating_mode = ""
    technologies = [j.text.replace('\n', ', ') for j in
                    driver.find_elements(By.XPATH, '//*[@data-test="item-technologies-expected"]')]
    technologies_optional = [j.text.replace('\n', ', ') for j in
                             driver.find_elements(By.XPATH, '//*[@data-test="item-technologies-optional"]')]
    specification = [k.text.replace('\n', ', ') for k in driver.find_elements(By.XPATH, "//*[@class='tkzmjn3']")]
    all_technologies = technologies + technologies_optional
    if salary != "":
        salary_low, salary_high = salary.split("–")
    else:
        salary_low, salary_high = "", ""
    job_details = [
        title,
        company_name,
        location,
        salary_low,
        salary_high,
        work_type,
        experience,
        contract_type,
        operating_mode,
        ', '.join(all_technologies),
        ', '.join(specification)
    ]

    job_details = [detail.encode('cp1250', errors='ignore').decode('cp1250') for detail in job_details]
    return job_details

def collect_job_details_in_parallel(links, month, year, max_workers=10, csv_file_details="default.csv", header=None):
    """collect job links in parallel and save details to a CSV file."""
    page_count = 0
    total_pages = len(links)
    with open(csv_file_details, 'a', newline='', encoding='cp1250') as csvfile:
        writer = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_MINIMAL)

        if header is not None:
            csvfile.seek(0, 2)
            if csvfile.tell() == 0:
                writer.writerow(header)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(collect_job_details, link[0]): link for link in links}
            for future in as_completed(future_to_url):
                link = future_to_url[future]
                try:
                    job_detail = future.result()
                    page_count += 1
                    print(f"Collected {page_count}/{total_pages} ({format(page_count/total_pages, ".2%")}) of {str(Month(month))}/{year}: {link[0]}")
                    writer.writerow(job_detail)

                except Exception as e:
                    print(f"Failed to collect {link[0]}: {str(e)}")

if __name__ == "__main__":
    number_of_pages = 1
    starting_year = 2015
    ending_year = 2024
    header = ['title', 'company_name', 'location', 'salary_low', 'salary_high', 'work_type', 'experience', 'contract_type',
              'operating_mode', 'technologies', 'specification']
    current_directory = os.getcwd()
    path_exist = os.path.join(current_directory, 'done_details')
    if not os.path.exists(path_exist):
        os.makedirs(path_exist)
    absolute_path = os.path.abspath(path_exist)
    print("Saves in:", absolute_path)
    for year in range(starting_year, ending_year+1):
        for month in range(1, 13):
            csv_file = f"done_pozostale/links_{year}_{month}.csv"
            csv_file_details = f'{path_exist}/details_{year}_{month}.csv'
            try:
                with open(csv_file, 'r', newline='') as csvfile:
                    links = list(csv.reader(csvfile))
                    start_collect_time = time.time()
                    collect_job_details_in_parallel(links, month, year, csv_file_details=csv_file_details,
                                                    header=header)
                    end_collect_time = time.time()
                    collect_elapsed_time = end_collect_time - start_collect_time
                    print(
                        f"Collected {len(links)} pages of {str(Month(month))}/{year} in {collect_elapsed_time:.4f} seconds, saved in {csv_file_details}")
            finally:
                quit_drivers()
