import json
import re
import spacy

nlp_pl = spacy.load("pl_core_news_sm")
nlp_en = spacy.load("en_core_web_sm")

STOP_WORDS = {"i", "oraz", "w", "na", "do", "z", "or", "and", "the", "for", "with", "of"}

def get_tokens_spacy(text, lang_model):
    doc = lang_model(text)
    # filtrujemy tokeny: tylko słowa (is_alpha), nie stopwords, i nie przymiotniki (ADJ)
    tokens = [
        t.lemma_.lower()
        for t in doc
        if t.is_alpha
        and not t.is_stop
        and t.lemma_.lower() not in STOP_WORDS
        and t.pos_ != "ADJ"  # pomijamy przymiotniki
    ]
    return tokens


def generate_ngrams(tokens, n_min=1, n_max=5):
    ngrams = []
    for n in range(n_min, n_max+1):
        ngrams.extend([' '.join(tokens[i:i+n]) for i in range(len(tokens)-n+1)])
    return ngrams

def process_field_spacy(text):
    if not text or not isinstance(text, str):
        return []

    text = re.sub(r'[.,;:!?()\[\]{}\\/]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()

    tokens_pl = get_tokens_spacy(text, nlp_pl)
    tokens_en = get_tokens_spacy(text, nlp_en)

    ngrams_list = []
    for tokens in [tokens_pl, tokens_en]:
        ngrams_list.extend(generate_ngrams(tokens))

    return ngrams_list

with open("json_karty/clean_wszystkie_karty.json", "r", encoding="utf-8") as f:
    data = json.load(f)

ANALYSIS_FIELDS = [
    "wymagania_wstepne",
    "efekty_uczenia_sie",
    "tresc_przedmiotu",
    "lista_lektur",
    "przykladowe_zagadnienia",
    "cel_przedmiotu"
]

ngrams_per_course = {}

for course in data:
    course_id = course.get("kod_przedmiotu", course.get("nazwa_przedmiotu", "unknown"))
    ngrams_per_course[course_id] = {}

    for field in ANALYSIS_FIELDS:
        field_value = course.get(field)
        if isinstance(field_value, str) and field_value.strip():
            field_ngrams = process_field_spacy(field_value)
            ngrams_per_course[course_id][field] = field_ngrams

with open("json_karty/ngrams_per_course.json", "w", encoding="utf-8") as f:
    json.dump(ngrams_per_course, f, ensure_ascii=False, indent=4)

