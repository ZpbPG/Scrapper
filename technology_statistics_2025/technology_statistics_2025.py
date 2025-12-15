import json
import glob
from collections import defaultdict

def is_valid_tech(phrase):
    return len(phrase.split()) <= 2  

in_jobs_count = defaultdict(int)
in_courses_count = defaultdict(int)  
technologies = set()

# Przetwarzanie wszystkich plików (11 miesięcy)
for month in range(1, 12+1):
    month_str = f"{month:02d}"

    # ===== KP_TO_OP =====
    kp_files = glob.glob(f"kp_to_op/kp_to_op_matches_no_tokenization_2025_{month_str}.json")
    for kp_file in kp_files:
        with open(kp_file, "r", encoding="utf-8") as f:
            kp_to_op = json.load(f)
        for course_id, sections in kp_to_op.items():
            for section_name, entries in sections.items():
                for entry in entries:
                    tech = entry.get("job_phrase")
                    if tech and is_valid_tech(tech):
                        tech_clean = tech.strip()
                        technologies.add(tech_clean)
                        in_jobs_count[tech_clean] += 1  

    # ===== OP_TO_KP =====
    op_files = glob.glob(f"op_to_kp/op_to_kp_matches_no_tokenization_2025_{month_str}.json")
    for op_file in op_files:
        with open(op_file, "r", encoding="utf-8") as f:
            op_to_kp = json.load(f)
        for tech_name, entries in op_to_kp.get("specification", {}).items():
            if is_valid_tech(tech_name):
                tech_clean = tech_name.strip()
                technologies.add(tech_clean)
                in_courses_count[tech_clean] += len(entries)  

# Tworzenie finalnego json
results = {}
for tech in sorted(technologies):
    jobs = in_jobs_count.get(tech, 0)
    courses = in_courses_count.get(tech, 0)
    results[tech] = {
        "in_jobs": jobs,
        "in_courses": courses,
        "total": jobs + courses
    }

# Zapis do json
with open("technology_statistics_2025_total.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print("✔️ Zapisano: technology_statistics_2025_total.json")
