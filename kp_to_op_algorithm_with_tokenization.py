import json
from sentence_transformers import SentenceTransformer, util

# Używamy tego samego modelu do generowania embeddingów
model = SentenceTransformer('all-MiniLM-L6-v2')

# 1️⃣ Wczytanie danych
# Karty Przedmiotów (KP) - stokenizowane n-gramy
with open("json_karty/ngrams_per_course.json", "r", encoding="utf-8") as f:
    courses = json.load(f)

# Oferty Pracy (OP) - stokenizowane n-gramy (Wybierz odpowiedni plik!)
# UWAGA: W przeciwieństwie do op_to_kp, tutaj musimy przetwarzać wszystkie OP naraz lub sumować wyniki.
# Zakładamy, że ten plik zawiera frazy ze WSZYSTKICH ofert pracy do porównania.
with open("job_offers_tokenized/ngrams_details_2025-11_specification.json", "r", encoding="utf-8") as f:
    jobs_data = json.load(f)

# 2️⃣ Przygotowanie fraz OP
all_job_phrases = []
job_map = []  # (job_id, fraza)

# Zbieranie wszystkich fraz OP z listy słowników
for job_entry in jobs_data:  # jobs_data to lista słowników
    for job_id, fields in job_entry.items():
        # Zbieramy frazy OP z tych samych pól, co w oryginalnym skrypcie
        for key in ["technologies_expected_1", "technologies_optional_1",
                    "technologies_os", "requirements_expected_1",
                    "requirements_optional_1", "responsibilities_1", "offered"]:
            if key in fields:
                for t in fields[key]:
                    # Zapewnienie, że t nie jest listą, ale frazą (obsługa zagnieżdżonych list n-gramów)
                    if isinstance(t, list):
                        for tt in t:
                            phrase = " ".join(tt) if isinstance(tt, list) else str(tt)
                            all_job_phrases.append(phrase)
                            job_map.append((job_id, phrase))
                    else:
                        all_job_phrases.append(str(t))
                        job_map.append((job_id, str(t)))

# Usuwanie duplikatów dla efektywności, zachowując mapowanie
unique_job_phrases = []
unique_job_map = []
seen_phrases = set()

for job_id, phrase in job_map:
    if phrase not in seen_phrases:
        seen_phrases.add(phrase)
        unique_job_phrases.append(phrase)
        unique_job_map.append((job_id, phrase))
job_map = unique_job_map

# 3️⃣ Embeddingi OP
print("Tworzenie embeddingów OP...")
job_embeddings = model.encode(unique_job_phrases, convert_to_tensor=True, batch_size=64)
print(f"Gotowe. Wygenerowano {len(job_embeddings)} unikalnych embeddingów OP.")

# 4️⃣ Porównanie fraz KP ↔ OP
results = {}

# Przygotowanie fraz KP i ich mapowanie
all_course_phrases = []
course_map = []  # (course_id, fraza)
for course_id, fields in courses.items():
    for key in ["lista_lektur", "tresc_przedmiotu", "przykladowe_zagadnienia", "cel_przedmiotu", "efekty_uczenia_sie"]:
        if key in fields:
            for t in fields[key]:
                if isinstance(t, list):
                    phrase = " ".join(t) if all(isinstance(x, str) for x in t) else str(t)
                else:
                    phrase = str(t)
                all_course_phrases.append(phrase)
                course_map.append((course_id, phrase))

print("Tworzenie embeddingów KP...")
course_embeddings = model.encode(all_course_phrases, convert_to_tensor=True, batch_size=64)
print(f"Gotowe. Wygenerowano {len(course_embeddings)} embeddingów KP.")

# Główna pętla: iteracja po frazach KP
threshold = 0.7
results = {}  # course_id -> {course_phrase -> [match_list]}

for i, course_phrase in enumerate(all_course_phrases):
    # Wybieramy embedding frazy KP
    kp_embedding = course_embeddings[i].unsqueeze(0)  # Dodanie wymiaru dla util.cos_sim

    # Macierz podobieństwa (KP ↔ OP)
    # cos_scores będzie tensorem o wymiarach [1, liczba_fraz_OP]
    cos_scores = util.cos_sim(kp_embedding, job_embeddings)

    # Wyszukiwanie dopasowań powyżej progu
    # .squeeze(0) redukuje wymiar [1, X] do [X]
    match_indices = (cos_scores.squeeze(0) > threshold).nonzero(as_tuple=True)[0]

    matches = []
    if len(match_indices) > 0:
        for idx in match_indices:
            job_id, job_phrase = job_map[idx]
            similarity = float(cos_scores[0][idx])  # cos_scores[0] bo to jest macierz 1xN

            matches.append({
                "job_id": job_id,
                "job_phrase": job_phrase,
                "similarity": similarity
            })

    # Zapis wyniku do słownika 'results'
    if matches:
        course_id, _ = course_map[i]

        if course_id not in results:
            results[course_id] = {}

        # Zapisujemy dopasowania dla konkretnej frazy KP
        results[course_id][course_phrase] = matches

# 5️⃣ Zapis wyników
# Zapiszemy do nowego pliku w folderze op_to_kp (lub w głównym)
output_filename = "kp_to_op_matches.json"
# UWAGA: Zmień nazwę pliku wynikowego ręcznie, aby była zgodna z plikiem OP, którego użyłeś!

with open(output_filename, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=4)

print(f"✔️ Powiązania fraz KP ↔ OP zapisane w: {output_filename}")
