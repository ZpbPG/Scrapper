import os
import json
from glob import glob

def merge_yearly_files(base_path):
    """Merge all monthly JSON files into one per year."""
    for year_dir in os.listdir(base_path):
        year_path = os.path.join(base_path, year_dir)
        if not os.path.isdir(year_path):
            continue

        output_file = f"C:/Users/Tomasz/PycharmProjects/PythonProject/done_merged/pracujpl_links_{year_dir}_all.json"
        all_offers = []

        json_files = glob(os.path.join(year_path, "pracujpl_links_*.json"))
        json_files = [f for f in json_files if not f.endswith('_all.json')]
        json_files.sort()

        if not json_files:
            print(f"No monthly JSON files found for {year_dir}. Skipping.")
            continue

        for json_file in json_files:
            with open(json_file, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    if isinstance(data, list):
                        all_offers.extend(data)
                    else:
                        print(f"⚠️ File {json_file} does not contain a list.")
                except json.JSONDecodeError:
                    print(f"❌ Could not decode {json_file}")

        # Save merged file
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_offers, f, ensure_ascii=False, indent=4)

        print(f"✅ Merged {len(json_files)} files for {year_dir} → {output_file} ({len(all_offers)} offers total)")

if __name__ == "__main__":
    base_path = r"C:\Users\Tomasz\PycharmProjects\PythonProject\done"
    merge_yearly_files(base_path)
