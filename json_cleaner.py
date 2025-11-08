import json
import unicodedata
import re

POLISH_MAP = str.maketrans({
    "ą": "a", "ć": "c", "ę": "e", "ł": "l", "ń": "n",
    "ó": "o", "ś": "s", "ź": "z", "ż": "z",
    "Ą": "A", "Ć": "C", "Ę": "E", "Ł": "L", "Ń": "N",
    "Ó": "O", "Ś": "S", "Ź": "Z", "Ż": "Z"
})

FIELDS_TO_REMOVE = {
    "kierunek", "poziom_ksztalcenia", "rok_akademicki", "rok",
    "semestr", "jezyk", "forma_studiow", "jednostka_prowadzaca",
    "plik", "formy_zajec"
}

def remove_polish_characters(text):
    if not isinstance(text, str):
        return text
    text = text.translate(POLISH_MAP)
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    return text

def clean_text(text):
    if not isinstance(text, str):
        return text
    text = remove_polish_characters(text)
    text = text.strip()
    text = re.sub(r'Data wygenerowania:.*', '', text)
    text = re.sub(r'Strona\s+\d+\s+z\s+\d+', '', text)
    text = re.sub(r'\[[A-Z0-9_]+\]', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\s+([,;:.])', r'\1', text)
    text = re.sub(r'[,\.;:\-–]+$', '', text)
    text = text.strip()
    if text == "":
        return None
    return text

def clean_object(obj):
    if isinstance(obj, dict):
        obj = {k: v for k, v in obj.items() if k not in FIELDS_TO_REMOVE}
        return {k: clean_object(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_object(el) for el in obj]
    elif isinstance(obj, str):
        return clean_text(obj)
    else:
        return obj

with open("json_karty/wszystkie_karty.json", "r", encoding="utf-8") as f:
    data = json.load(f)

cleaned_data = clean_object(data)

with open("json_karty/clean_wszystkie_karty.json", "w", encoding="utf-8") as f:
    json.dump(cleaned_data, f, ensure_ascii=False, indent=4)

