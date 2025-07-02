import requests
from bs4 import BeautifulSoup
import os

# Adres strony z przedmiotami
URL = "https://ects.pg.edu.pl/pl/courses/17326/subjects"
BASE = "https://ects.pg.edu.pl"

# Pobierz stronę
response = requests.get(URL)
response.raise_for_status()
soup = BeautifulSoup(response.text, "html.parser")

# Stwórz folder na pliki
os.makedirs("karty_przedmiotow", exist_ok=True)

# Znajdź wszystkie linki do kart przedmiotu
links = soup.find_all("a", href=lambda href: href and href.endswith("card.pdf"))

print(f"Znaleziono {len(links)} kart przedmiotów.")

for i, link in enumerate(links, 1):
    href = link["href"]
    pdf_url = href if href.startswith("http") else BASE + href
    filename = pdf_url.split("/")[-2] + ".pdf"  # np. 372984.pdf
    filepath = os.path.join("karty_przedmiotow", filename)

    print(f"Pobieranie {i}: {filename}...")
    pdf_response = requests.get(pdf_url)
    with open(filepath, "wb") as f:
        f.write(pdf_response.content)

print("Wszystkie pliki zostały zapisane.")