import json
import re
import spacy
import os
import glob

nlp_pl = spacy.load("pl_core_news_sm")
nlp_en = spacy.load("en_core_web_sm")

STOP_WORDS = {
    "i", "oraz", "w", "na", "do", "z",
    "or", "and", "the", "for", "with", "of"
}

def get_tokens_spacy(text, lang_model):
    doc = lang_model(text)
    return [
        t.lemma_.lower()
        for t in doc
        if t.is_alpha and not t.is_stop and t.lemma_.lower() not in STOP_WORDS
    ]

def generate_ngrams(tokens, n_min=2, n_max=5):
    ngrams = []
    for n in range(n_min, n_max+1):
        ngrams.extend([
            " ".join(tokens[i:i+n])
            for i in range(len(tokens)-n+1)
        ])
    return ngrams

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

def process_any(value):

    if isinstance(value, str):
        return process_text(value)

    elif isinstance(value, list):
        return [process_any(v) for v in value]

    elif isinstance(value, dict):
        return {k: process_any(v) for k, v in value.items()}

    else:
        return value


file_path = "job_offers_json/details_2025-01.json"   # ← ustaw swój plik!
filename = os.path.basename(file_path)
name_no_ext = filename.replace(".json", "")
output_file = f"job_offers_json/ngrams_{name_no_ext}.json"

print(f"Przetwarzam: {filename}")

with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)

processed = []
for entry in data:
    entry_id = entry.get("url", "unknown")
    processed.append({
        entry_id: process_any(entry)
    })

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(processed, f, ensure_ascii=False, indent=4)

