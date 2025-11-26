import requests
from bs4 import BeautifulSoup
import os

URLS = [
    "https://ects.pg.edu.pl/pl/courses/17326/subcourses/17329/subjects",
    "https://ects.pg.edu.pl/pl/courses/17326/subcourses/17328/subjects",
    "https://ects.pg.edu.pl/pl/courses/17326/subcourses/17332/subjects",
    "https://ects.pg.edu.pl/pl/courses/17326/subcourses/17331/subjects",
]

BASE = "https://ects.pg.edu.pl"

os.makedirs("karty_przedmiotow", exist_ok=True)

# Zbiór unikalnych linków
unique_pdf_urls = set()

# Zbieranie linków PDF z każdej strony
for url in URLS:
    print(f"Przetwarzam: {url}")
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    links = soup.find_all("a", href=lambda href: href and href.endswith("card.pdf"))

    for link in links:
        href = link["href"]
        pdf_url = href if href.startswith("http") else BASE + href
        unique_pdf_urls.add(pdf_url)

print(f"Znaleziono {len(unique_pdf_urls)} unikalnych plików PDF.\n")

# Pobieranie PDF-ów
for i, pdf_url in enumerate(unique_pdf_urls, 1):
    filename = pdf_url.split("/")[-2] + ".pdf"
    filepath = os.path.join("karty_przedmiotow", filename)

    print(f"Pobieram {i}: {filename}")
    pdf_response = requests.get(pdf_url)
    with open(filepath, "wb") as f:
        f.write(pdf_response.content)

print("Wszystkie pliki zostały zapisane.")