import json
import re
import spacy
import os

# ładowanie modeli spaCy
nlp_pl = spacy.load("pl_core_news_sm")
nlp_en = spacy.load("en_core_web_sm")

STOP_WORDS = {
    "i", "oraz", "w", "na", "do", "z",
    "or", "and", "the", "for", "with", "of"
}

# tokenizacja i lematyzacja
def get_tokens_spacy(text, lang_model):
    doc = lang_model(text)
    return [
        t.lemma_.lower()
        for t in doc
        if t.is_alpha and not t.is_stop and t.lemma_.lower() not in STOP_WORDS
    ]

# n-gramy
def generate_ngrams(tokens, n_min=1, n_max=5):
    ngrams = []
    for n in range(n_min, n_max + 1):
        ngrams.extend([
            " ".join(tokens[i:i+n])
            for i in range(len(tokens)-n+1)
        ])
    return ngrams

# przetwarzanie tekstu z spaCy
def process_text(text):
    if not isinstance(text, str) or not text.strip():
        return []

    text = re.sub(r"[.,;:!?()\[\]{}\\/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    tokens_pl = get_tokens_spacy(text, nlp_pl)
    tokens_en = get_tokens_spacy(text, nlp_en)

    ngrams = []
    for tokens in (tokens_pl, tokens_en):
        ngrams.extend(generate_ngrams(tokens))

    return ngrams

# rekurencyjne przetwarzanie tylko specification
def process_specification(value):
    if isinstance(value, str):
        return process_text(value)
    elif isinstance(value, list):
        return [process_specification(v) for v in value]
    elif isinstance(value, dict):
        return {k: process_specification(v) for k, v in value.items()}
    else:
        return value  # inne typy pomijamy

# --- PRZETWARZANIE PLIKU --- 
file_path = "job_offers_json/details_2025-11.json"
filename = os.path.basename(file_path)
name_no_ext = filename.replace(".json", "")
output_file = f"job_offers_tokenized/ngrams_{name_no_ext}_specification.json"

print(f"Przetwarzam: {filename}")

with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)

processed = []

for entry in data:
    entry_id = entry.get("url", "unknown")
    spec = entry.get("specification", {})

    processed.append({
        entry_id: process_specification(spec)
    })

    print(f"Przetworzono: {entry_id}")

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(processed, f, ensure_ascii=False, indent=4)

print("\n✔ Zapisano plik:", output_file)
