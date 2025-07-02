import fitz  # PyMuPDF
import re
import json
from pathlib import Path

def extract_text_from_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    return "\n".join([page.get_text() for page in doc])

def extract_section(text, section_title, stop_titles):
    lines = text.splitlines()
    capturing = False
    collected = []

    for line in lines:
        if section_title.lower() in line.lower():
            capturing = True
            continue
        if capturing and any(stop.lower() in line.lower() for stop in stop_titles):
            break
        if capturing:
            collected.append(line.strip())

    return "\n".join(collected).strip()

def parse_course_info(text):
    data = {}

    lines = text.splitlines()

    def safe_search(pattern):
        match = re.search(pattern, text)
        return match.group(1).strip() if match else None

    def search_next_line(keyword):
        for i, line in enumerate(lines):
            if keyword.lower() in line.lower():
                if i + 1 < len(lines):
                    return lines[i + 1].strip()
        return None

    data['nazwa_kod_przedmiotu'] = safe_search(r'Nazwa i kod przedmiotu\s+(.+)')
    data['kierunek'] = safe_search(r'Kierunek studiów\s+(.+)')
    data['rok_akademicki'] = safe_search(r'Rok akademicki realizacji przedmiotu\s+(\d{4}/\d{4})') or search_next_line("Rok akademicki realizacji przedmiotu")
    data['poziom_ksztalcenia'] = safe_search(r'Poziom kształcenia\s+(.+)')
    data['rok'] = safe_search(r'Rok studiów\s+(\d+)')
    data['semestr'] = safe_search(r'Semestr studiów\s+(\d+)')
    data['jezyk'] = safe_search(r'Język wykładowy\s+(.+)')
    data['forma_studiow'] = safe_search(r'Forma studiów\s+(.+)')
    data['jednostka_prowadzaca'] = safe_search(r'Jednostka prowadząca\s+(.+)')

    data['cel_przedmiotu'] = extract_section(
        text, "Cel przedmiotu", ["Efekty uczenia się", "Efekt kierunkowy", "Data wygenerowania"]
    )


    efekty_raw = extract_section(
        text, "Efekty uczenia się", ["Treści przedmiotu", "Wymagania wstępne"]
    )
    efekt_linie = efekty_raw.splitlines()
    efekt_linie_czyste = [
        line for line in efekt_linie
        if not re.match(r'(?i)(efekt kierunkowy|efekt z przedmiotu|sposób weryfikacji|oceny efektu|przedmiotu)', line.strip())
    ]
    data['efekty_uczenia_sie'] = " ".join(efekt_linie_czyste).strip()

    data['tresc_przedmiotu'] =  extract_section(
        text, "Treści przedmiotu", ["Wymagania wstępne", "Sposoby i kryteria"]
    )

    formy_section = extract_section(text, "Formy zajęć", ["Aktywność studenta", "W tym liczba"])

    data['formy_zajec'] = formy_section

    data['wymagania_wstepne'] = extract_section(
        text, "Wymagania wstępne", ["Sposoby i kryteria"]
    )

    data['lista_lektur'] = extract_section(
        text, "Zalecana lista lektur", ["Adresy eZasobów", "Data wygenerowania"]
    )

    data['przykladowe_zagadnienia'] = extract_section(
        text, "Przykładowe zagadnienia", ["Praktyki zawodowe", "Dokument wygenerowany"]
    )

    data['przykladowe_zagadnienia'] = data['przykladowe_zagadnienia'].replace('\n', ' ')
    data['lista_lektur'] = data['lista_lektur'].replace('\n', ' ')
    data['tresc_przedmiotu'] = data['tresc_przedmiotu'].replace('\n', ' ')
    data['efekty_uczenia_sie'] = data['efekty_uczenia_sie'].replace('\n', ' ')
    data['cel_przedmiotu'] = data['cel_przedmiotu'].replace('\n', ' ')

    data['wymagania_wstepne'] = data['wymagania_wstepne'].replace('\n', ' ')
    data['wymagania_wstepne'] = data['wymagania_wstepne'].replace('i dodatkowe', '')

    data['formy_zajec'] = data['formy_zajec'].replace('\n', ' ')

    return data

def save_as_json(data, out_path):
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def process_pdfs_in_folder(folder_path):
    folder = Path(folder_path)
    pdf_files = list(folder.glob("*.pdf"))

    for pdf_file in pdf_files:
        try:
            text = extract_text_from_pdf(pdf_file)
            parsed_data = parse_course_info(text)
            output_file = pdf_file.with_suffix(".json")
            save_as_json(parsed_data, output_file)
            print(f"{output_file.name}")
        except Exception as e:
            print(f"{pdf_file.name}: {e}")

if __name__ == "__main__":
    process_pdfs_in_folder("C:\\Users\\Kacper\\Desktop\\PDFy")
