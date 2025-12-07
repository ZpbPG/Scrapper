import json
from sentence_transformers import SentenceTransformer, util

model = SentenceTransformer('all-MiniLM-L6-v2')

# Wczytanie danych
with open("json_karty/ngrams_per_course.json", "r", encoding="utf-8") as f:
    courses = json.load(f)

# Oferty Pracy (OP) - oryginalne dane (bez n-gramów)
with open("job_offers_json/details_2025-11.json", "r", encoding="utf-8") as f:
    jobs = json.load(f)

# Przygotowanie fraz KP (Karty Przedmiotów)
all_course_phrases = []
course_map = []  # (course_id, fraza)

kp_fields = [
    "lista_lektur",
    "tresc_przedmiotu",
    "przykladowe_zagadnienia",
    "cel_przedmiotu",
    "efekty_uczenia_sie"
]

for course_id, fields in courses.items():
    for key in kp_fields:
        if key in fields:
            for value in fields[key]:
                phrase = str(value)
                all_course_phrases.append(phrase)
                course_map.append((course_id, phrase))

# Przygotowanie puli wszystkich fraz OP (Oferty Pracy)
all_op_phrases = []
op_map = []  # (job_id, fraza)

op_fields = [
    "technologies_expected_1",
    "technologies_optional_1",
    "technologies_os",
    "requirements_expected_1",
    "requirements_optional_1",
    "responsibilities_1"
]

for job_entry in jobs:
    job_id = job_entry.get("url")
    spec = job_entry.get("specification", {})

    for key in op_fields:
        if key in spec:
            for value in spec[key]:
                phrase = str(value).strip()
                if phrase:
                    # Dodaj tylko unikalne frazy do listy do embeddingu
                    # Zapisujemy mapowanie do job_id
                    all_op_phrases.append(phrase)
                    op_map.append((job_id, phrase))

# Embeddingi OP
print("Tworzenie embeddingów wszystkich fraz OP...")
op_embeddings = model.encode(
    all_op_phrases,
    convert_to_tensor=True,
    batch_size=64
)
print(f"Gotowe. Wygenerowano {len(op_embeddings)} embeddingów OP.")

# Embeddingi KP
print("Tworzenie embeddingów KP...")
course_embeddings = model.encode(
    all_course_phrases,
    convert_to_tensor=True,
    batch_size=64
)
print(f"Gotowe. Wygenerowano {len(course_embeddings)} embeddingów KP.")

# Porównanie KP ↔ OP
results = {}
threshold = 0.45

for i, course_phrase in enumerate(all_course_phrases):
    kp_embedding = course_embeddings[i].unsqueeze(0)

    # Cosine similarity (KP ↔ Wszystkie OP)
    cos_scores = util.cos_sim(kp_embedding, op_embeddings)

    match_indices = (cos_scores.squeeze(0) > threshold).nonzero(as_tuple=True)[0]

    matches = []
    if len(match_indices) > 0:
        for idx in match_indices:
            job_id, job_phrase = op_map[idx]
            similarity = float(cos_scores[0][idx])

            matches.append({
                "job_id": job_id,
                "job_phrase": job_phrase,
                "similarity": similarity
            })

    # Zapis wyniku do słownika 'results'
    if matches:
        course_id, _ = course_map[i]

        # Sortowanie dopasowań malejąco
        matches = sorted(matches, key=lambda x: x["similarity"], reverse=True)

        if course_id not in results:
            results[course_id] = {}

        # Struktura: course_id -> {course_phrase -> [lista_dopasowań_OP]}
        results[course_id][course_phrase] = matches

# Zapis wyników
output_filename = "kp_to_op/kp_to_op_matches_no_tokenization_2025_11.json"

with open(output_filename, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=4)

print(f"\n✔️ Powiązania KP ↔ OP (bez tokenizacji) zapisane w: {output_filename}")
