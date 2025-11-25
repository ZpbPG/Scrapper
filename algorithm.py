import json
from sentence_transformers import SentenceTransformer, util
import torch

model = SentenceTransformer('all-MiniLM-L6-v2')

# 1️⃣ Wczytanie danych
with open("json_karty/ngrams_per_course.json", "r", encoding="utf-8") as f:
    courses = json.load(f)

with open("job_offers_json/ngrams_details_2025-11_specification.json", "r", encoding="utf-8") as f:
    jobs = json.load(f)

# 2️⃣ Przygotowanie fraz KP
all_course_phrases = []
course_map = []  # (course_id, fraza)
for course_id, fields in courses.items():
    for key in ["lista_lektur", "tresc_przedmiotu", "przykladowe_zagadnienia", "cel_przedmiotu"]:
        if key in fields:
            for t in fields[key]:
                if isinstance(t, list):
                    phrase = " ".join(t) if all(isinstance(x, str) for x in t) else str(t)
                else:
                    phrase = str(t)
                all_course_phrases.append(phrase)
                course_map.append((course_id, phrase))

# 3️⃣ Embeddingi KP
print("Tworzenie embeddingów KP...")
course_embeddings = model.encode(all_course_phrases, convert_to_tensor=True, batch_size=64)
print("Gotowe.")

# 4️⃣ Porównanie fraz OP ↔ KP
results = {}

for job_entry in jobs:  # jobs to lista słowników
    for job_id, fields in job_entry.items():
        # zbieramy frazy OP
        op_phrases = []
        for key in ["technologies_expected_1", "technologies_optional_1",
                    "technologies_os", "requirements_expected_1",
                    "requirements_optional_1", "responsibilities_1", "offered"]:
            if key in fields:
                for t in fields[key]:
                    if isinstance(t, list):
                        for tt in t:
                            op_phrases.append(" ".join(tt) if isinstance(tt, list) else str(tt))
                    else:
                        op_phrases.append(str(t))

        if not op_phrases:
            continue

        # embeddingi fraz OP
        op_embeddings = model.encode(op_phrases, convert_to_tensor=True, batch_size=64)

        # cosine similarity macierzowo
        cos_scores = util.cos_sim(op_embeddings, course_embeddings)

        job_matches = {}
        threshold = 0.7
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
                job_matches[op_phrase] = matches

        if job_matches:
            results[job_id] = job_matches

# 5️⃣ Zapis wyników
with open("op_to_kp_matches.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=4)

print("✔️ Powiązania fraz OP ↔ KP zapisane.")
