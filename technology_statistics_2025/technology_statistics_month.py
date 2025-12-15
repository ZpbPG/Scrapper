import json
from collections import defaultdict

# 1. Wczytywanie danych
with open("kp_to_op/kp_to_op_matches_no_tokenization_2025_11.json", "r", encoding="utf-8") as f:
    kp_to_op = json.load(f)

with open("op_to_kp/op_to_kp_matches_no_tokenization_2025_11.json", "r", encoding="utf-8") as f:
    op_to_kp = json.load(f)

# 2. Wyodrębnianie wszystkich technologii
technologies = set()

def is_valid_tech(phrase):
    return len(phrase.split()) <= 2  

# Z kp_to_op
for course_id, sections in kp_to_op.items():
    for section_name, entries in sections.items():
        for entry in entries:
            tech = entry.get("job_phrase")
            if tech and is_valid_tech(tech):
                technologies.add(tech.strip())

# Z op_to_kp
for tech_name, entries in op_to_kp.get("specification", {}).items():
    if is_valid_tech(tech_name):
        technologies.add(tech_name.strip())

technologies = sorted(technologies)

# 3. Liczenie wystąpień technologii w ofertach pracy
in_jobs_count = defaultdict(int)

for course_id, sections in kp_to_op.items():
    for section_name, entries in sections.items():
        for entry in entries:
            tech = entry.get("job_phrase")
            if tech and is_valid_tech(tech):
                in_jobs_count[tech] += 1

# 3. Liczenie wystąpień technologii w kursach 
in_courses_count = defaultdict(int)

for tech_name, entries in op_to_kp.get("specification", {}).items():
    if is_valid_tech(tech_name):
        in_courses_count[tech_name] += len(entries)  

# 5. Tworzenie finalnego jsona
results = {}

for tech in technologies:
    jobs = in_jobs_count.get(tech, 0)
    courses = in_courses_count.get(tech, 0)
    results[tech] = {
        "in_jobs": jobs,
        "in_courses": courses,
        "total": jobs + courses
    }

# 6. Zapis do json
with open("technology_statistics_2025_11.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print("✔️ Zapisano: technology_statistics_2025_11.json")
