import os
import json

# Input JSON files for 2024
input_files = [
    "done_merged/pracujpl_links_2015_all.json",
    "done_merged/pracujpl_links_2016_all.json",
    "done_merged/pracujpl_links_2017_all.json",
    "done_merged/pracujpl_links_2018_all.json",
    "done_merged/pracujpl_links_2019_all.json",
    "done_merged/pracujpl_links_2020_all.json",
    "done_merged/pracujpl_links_2021_all.json",
    "done_merged/pracujpl_links_2022_all.json",
    "done_merged/pracujpl_links_2023_all.json",
    "done_merged/pracujpl_links_2024_all.json",
    "done_merged/pracujpl_links_2025_all.json",
]

# Keywords to filter for
keywords = [
    # seniority
    'junior', 'młodszy', 'staz', 'staż', 'mid', 'regular', 'senior', 'ekspert',
    'lead', 'team lead', 'tech lead', 'principal',

    # internships
    'intern', 'praktykant', 'praktyki',

    # IT roles general
    'engineer', 'inzynier', 'inżynier', 'developer', 'programista',
    'software', 'fullstack', 'frontend', 'backend',

    # cloud & devops
    'cloud', 'aws', 'azure', 'gcp', 'devops', 'site reliability', 'sre',
    'administrator', 'sysadmin', 'system administrator',

    # cybersecurity
    'cyber', 'security', 'bezpieczeństwo', 'pentester', 'soc', 'analyst',

    # programming languages
    'python', 'java', 'javascript', 'typescript', 'c++', 'c#', 'golang', 'php',
    'kotlin', 'swift', 'rust', 'scala',

    # data & analytics
    'data', 'big data', 'ml', 'machine learning', 'ai', 'sztuczna inteligencja',
    'analyst', 'analityk', 'data scientist', 'data engineer',
    'business intelligence', 'bi developer', 'etl', 'dane', 'danych'

    # testing
    'qa', 'tester', 'test engineer', 'automatyzacja', 'automation',

    # management / product / scrum
    'manager', 'product owner', 'product manager',
    'scrum master', 'project manager', 'pm',

    # UX/UI & design
    'ux', 'ui', 'designer', 'projektant', 'grafik', 'web designer',
]



def filter_json(input_file, output_file, keywords):
    """Filter job offers in a JSON file based on keywords in the title or URL."""
    number_of_rows = 0

    with open(input_file, 'r', encoding='utf-8') as infile:
        data = json.load(infile)

    filtered_offers = []
    for offer in data:
        text_to_check = (offer.get("title", "") + " " + offer.get("link", "")).lower()
        if any(keyword in text_to_check for keyword in keywords):
            filtered_offers.append(offer)
            number_of_rows += 1

    # Save filtered results
    with open(output_file, 'w', encoding='utf-8') as outfile:
        json.dump(filtered_offers, outfile, ensure_ascii=False, indent=4)

    print(f"✅ {input_file} → {output_file}")
    print(f"   Number of relevant job offers: {number_of_rows}")


# Main logic
for file in input_files:
    base_path = 'C:/Users/Tomasz/PycharmProjects/PythonProject'
    if not os.path.exists(base_path):
        os.makedirs(base_path)

    sub_path = os.path.dirname(file)
    output_path = os.path.join(base_path, sub_path)
    os.makedirs(output_path, exist_ok=True)

    output = os.path.basename(file).replace('.json', '_filtered')
    output_file = f'{output_path}/{output}_v2.json'

    filter_json(file, output_file, keywords)
