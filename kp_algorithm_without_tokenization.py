import json
from sentence_transformers import SentenceTransformer, util
import torch

model = SentenceTransformer('all-MiniLM-L6-v2')

# Wczytanie danych
with open("json_karty/ngrams_per_course.json", "r", encoding="utf-8") as f:
    courses = json.load(f)

with open("job_offers_json/details_2025-11.json", "r", encoding="utf-8") as f:
    jobs = json.load(f)

# Pobieramy pełne frazy z kart przedmiotów (KP)
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

# Embeddingi KP
print("Tworzenie embeddingów kursów...")
course_embeddings = model.encode(
    all_course_phrases,
    convert_to_tensor=True,
    batch_size=64
)
print("Gotowe.")

# Zapis embeddingów do pliku
torch.save({
    "embeddings": course_embeddings,
    "course_map": course_map
}, "op_to_kp/course_embeddings.pt")

# Porównanie ofert pracy ↔ KP
results = {}

op_fields = [
    "technologies_expected_1",
    "technologies_optional_1",
    "technologies_os",
    "requirements_expected_1",
    "requirements_optional_1",
    "responsibilities_1"
]

threshold = 0.6

all_op_phrases = []

for job_entry in jobs: 
    job_id = job_entry.get("url")
    spec = job_entry.get("specification", {})
    op_phrases = []

    # Pobieramy frazy OP
    for key in op_fields:
        if key in spec:
            for value in spec[key]:
                phrase = str(value).strip()
                if phrase:
                    op_phrases.append(phrase)

    if not op_phrases:
        continue

    # Embeddingi OP
    op_embeddings = model.encode(
        op_phrases,
        convert_to_tensor=True,
        batch_size=64
    )

    # Cosine similarity
    cos_scores = util.cos_sim(op_embeddings, course_embeddings)

    job_matches = {}

    for i, op_phrase in enumerate(op_phrases):
        match_indices = (cos_scores[i] > threshold).nonzero(as_tuple=True)[0]

        matches = []
        for idx in match_indices:
            course_id, course_phrase = course_map[idx]
            similarity = float(cos_scores[i][idx])
            matches.append({
                "course_id": course_id,
                "course_phrase": course_phrase,
                "similarity": similarity
            })

        if matches:
            job_matches[op_phrase] = sorted(matches, key=lambda x: x["similarity"], reverse=True)

    if job_matches:
        results[job_id] = job_matches

    all_op_phrases.append(op_phrases)

# Zapis wyników
with open("op_to_kp/op_to_kp_matches_no_tokenization.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=4)

# Zapis OP fraz
with open("op_to_kp/op_phrases.json", "w", encoding="utf-8") as f:
    json.dump(all_op_phrases, f, ensure_ascii=False, indent=2)

print("\nPowiązania OP ↔ KP zapisane.")
