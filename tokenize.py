import json
import re

#KG i KW: może niepotrzebne, może wykluczać jakieś frazy
STOP_WORDS = {"i", "oraz", "w", "na", "do", "z"}

def tokenize(text):
    if not isinstance(text, str):
        return []
    text = text.lower()
    text = re.sub(r'\.', ' ', text)
    text = re.sub(r'\b\d+\b', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    tokens = text.split()
    tokens = [t for t in tokens if t not in STOP_WORDS]
    return tokens

def ngrams(tokens, n):
    return [' '.join(tokens[i:i+n]) for i in range(len(tokens)-n+1)]

def filter_ngrams(ngrams_list):
    filtered = []
    for ng in ngrams_list:
        words = ng.split()
        if not any(w in STOP_WORDS for w in words):
            filtered.append(ng)
    return filtered

def extract_text_fields(obj):
    texts = []
    if isinstance(obj, dict):
        for v in obj.values():
            texts.extend(extract_text_fields(v))
    elif isinstance(obj, list):
        for el in obj:
            texts.extend(extract_text_fields(el))
    elif isinstance(obj, str):
        texts.append(obj)
    return texts

with open("json_karty/clean_wszystkie_karty.json", "r", encoding="utf-8") as f:
    data = json.load(f)

ANALYSIS_FIELDS = ["wymagania_wstepne", "efekty_uczenia_sie", "tresc_przedmiotu",
                   "lista_lektur", "przykladowe_zagadnienia", "cel_przedmiotu"]

ngrams_per_course = {}

for course in data:
    course_id = course.get("kod_przedmiotu", course.get("nazwa_przedmiotu", "unknown"))
    ngrams_per_course[course_id] = {}

    for field in ANALYSIS_FIELDS:
        field_value = course.get(field)
        if isinstance(field_value, str) and field_value.strip():
            tokens = tokenize(field_value)
            field_ngrams = []
            for n in range(2, 6):
                field_ngrams.extend(ngrams(tokens, n))
            field_ngrams = filter_ngrams(field_ngrams)
            ngrams_per_course[course_id][field] = list(set(field_ngrams))

with open("ngrams_per_course_per_field.json", "w", encoding="utf-8") as f:
    json.dump(ngrams_per_course, f, ensure_ascii=False, indent=4)

