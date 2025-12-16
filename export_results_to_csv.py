import json
import pandas as pd
import glob
from collections import defaultdict

# --- KONFIGURACJA ŚCIEŻEK ---
MONTH = "11"
YEAR = "2025"
KP_CLEAN_PATH = "json_karty/clean_wszystkie_karty"
JOB_DETAILS_PATH = f"job_offers_json/details_{YEAR}-{MONTH}.json"
KP_TO_OP_PATH = f"kp_to_op/kp_to_op_matches_no_tokenization_{YEAR}_{MONTH}.json"
OP_TO_KP_PATH = f"op_to_kp/op_to_kp_matches_no_tokenization_{YEAR}_{MONTH}.json"
OUTPUT_EXCEL_PATH = f"analytics_report_{YEAR}_{MONTH}.xlsx"
STATS_PATH = f"technology_statistics_2025/technology_statistics_{YEAR}_{MONTH}.json" 


# --- 1. FUNKCJE POMOCNICZE DO MAPOWANIA DANYCH ---

def create_course_name_map():
    """Tworzy mapę kod_przedmiotu -> nazwa_przedmiotu."""
    try:
        with open(f"{KP_CLEAN_PATH}.json", "r", encoding="utf-8") as f:
            courses_data = json.load(f)
        
        return {
            c.get("kod_przedmiotu"): c.get("nazwa_przedmiotu", "Brak nazwy")
            for c in courses_data
        }
    except FileNotFoundError:
        print(f"Błąd: Nie znaleziono pliku {KP_CLEAN_PATH}.json")
        return {}


def create_job_title_map():
    """Tworzy mapę job_id (URL) -> job_title."""
    try:
        with open(JOB_DETAILS_PATH, "r", encoding="utf-8") as f:
            jobs_data = json.load(f)
        
        return {
            job.get("url"): job.get("title", "Brak tytułu")
            for job in jobs_data
        }
    except FileNotFoundError:
        print(f"Błąd: Nie znaleziono pliku detali ofert pracy: {JOB_DETAILS_PATH}")
        return {}


# --- 2. PRZETWARZANIE DOPASOWAŃ SZCZEGÓŁOWYCH ---

def process_kp_to_op(course_map, job_title_map): 
    """Przetwarza i spłaszcza dane KP -> OP oraz dodaje tytuł stanowiska."""
    try:
        with open(KP_TO_OP_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Błąd: Nie znaleziono pliku KP->OP: {KP_TO_OP_PATH}")
        return pd.DataFrame()

    records = []
    for course_id, sections in data.items():
        course_name = course_map.get(course_id, course_id)
        for kp_phrase, matches in sections.items():
            for match in matches:
                job_id = match.get("job_id")
                job_phrase = match.get("job_phrase")
                similarity = match.get("similarity")
                
                records.append({
                    "Course ID": course_id,
                    "Nazwa Przedmiotu": course_name,
                    "Fraza KP": kp_phrase,
                    "ID Oferty Pracy (URL)": job_id,
                    "Tytuł Stanowiska": job_title_map.get(job_id, "Brak tytułu (Sprawdź detale)"),
                    "Fraza OP (Wymaganie)": job_phrase,
                    "Podobieństwo (Cosine Similarity)": similarity,
                })

    return pd.DataFrame(records)


def process_op_to_kp(course_map):
    """Przetwarza i spłaszcza dane OP -> KP."""
    try:
        with open(OP_TO_KP_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Błąd: Nie znaleziono pliku OP->KP: {OP_TO_KP_PATH}")
        return pd.DataFrame()

    records = []
    tech_data = data.get("specification", {})
    for op_phrase, matches in tech_data.items():
        for match in matches:
            course_id = match.get("course_id")
            course_name = course_map.get(course_id, course_id)
            records.append({
                "Fraza OP (Technologia)": op_phrase,
                "Course ID": course_id,
                "Nazwa Przedmiotu": course_name,
                "Fraza KP (Pokrycie)": match.get("course_phrase"),
                "Podobieństwo (Cosine Similarity)": match.get("similarity"),
            })

    return pd.DataFrame(records)


# --- 3. TWORZENIE PODSUMOWAŃ I STATYSTYK ---

def create_course_summary(df_kp_to_op):
    """Tworzy podsumowanie ilościowe dla kursów (z metrykami Szerokość/Gęstość)."""
    if df_kp_to_op.empty:
        return pd.DataFrame()
        
    grouped_titles = df_kp_to_op.groupby(['Course ID', 'Nazwa Przedmiotu']) \
                                .agg({'Tytuł Stanowiska': lambda x: ', '.join(sorted(x.unique()))}) \
                                .reset_index() \
                                .rename(columns={'Tytuł Stanowiska': 'Unikalne Stanowiska (Tytuły)'})

    summary = df_kp_to_op.groupby(['Course ID', 'Nazwa Przedmiotu']) \
                         .agg(
                             **{'Liczba Unikalnych Powiązań Wymaganie-Przedmiot': ('Fraza OP (Wymaganie)', 'nunique')},
                             **{'Całkowita Liczba Powiązań Wymaganie-Przedmiot': ('Podobieństwo (Cosine Similarity)', 'count')},
                             **{'Średnie Podobieństwo Dopasowań': ('Podobieństwo (Cosine Similarity)', 'mean')}
                         ) \
                         .reset_index()

    final_summary = pd.merge(summary, grouped_titles, on=['Course ID', 'Nazwa Przedmiotu'])
    return final_summary.sort_values(by='Liczba Unikalnych Powiązań Wymaganie-Przedmiot', ascending=False)

# Arkusz: Powiązania Wymaganie-Przedmiot
def create_requirement_course_summary(df_kp_to_op):
    """Tworzy podsumowanie na poziomie Wymaganie-Kurs (częstotliwość wystąpień i tytuły)."""
    if df_kp_to_op.empty:
        return pd.DataFrame()

    df_agg = df_kp_to_op[['Course ID', 'Nazwa Przedmiotu', 'Fraza OP (Wymaganie)', 'Tytuł Stanowiska']].copy()
    
    summary = df_agg.groupby(['Course ID', 'Nazwa Przedmiotu', 'Fraza OP (Wymaganie)']) \
                    .agg(
                        **{'Liczba Powiązań w Kursie': ('Fraza OP (Wymaganie)', 'count')},
                        **{'Tytuły Zawodowe (związane z wymaganiem)': ('Tytuł Stanowiska', lambda x: ', '.join(sorted(x.unique())))}
                    ) \
                    .reset_index() \
                    .rename(columns={'Fraza OP (Wymaganie)': 'Nazwa Wymagania/Technologii'})

    return summary.sort_values(by=['Nazwa Przedmiotu', 'Liczba Powiązań w Kursie'], ascending=[True, False])

# NOWA FUNKCJA: Odwrotne grupowanie (Technologia -> Kursy)
def create_technology_course_summary(df_kp_to_op):
    """Tworzy podsumowanie na poziomie Technologia-Kursy."""
    if df_kp_to_op.empty:
        return pd.DataFrame()

    df_agg = df_kp_to_op[['Course ID', 'Nazwa Przedmiotu', 'Fraza OP (Wymaganie)', 'Tytuł Stanowiska']].copy()
    
    summary = df_agg.groupby(['Fraza OP (Wymaganie)', 'Course ID', 'Nazwa Przedmiotu']) \
                    .agg(
                        **{'Liczba Powiązań z Kursem': ('Fraza OP (Wymaganie)', 'count')},
                        **{'Tytuły Zawodowe': ('Tytuł Stanowiska', lambda x: ', '.join(sorted(x.unique())))}
                    ) \
                    .reset_index() \
                    .rename(columns={'Fraza OP (Wymaganie)': 'Nazwa Wymagania/Technologii'})

    return summary.sort_values(by=['Nazwa Wymagania/Technologii', 'Liczba Powiązań z Kursem'], ascending=[True, False])


def process_technology_stats():
    """Wczytuje i przygotowuje statystyki technologii do osobnego arkusza."""
    try:
        with open(STATS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Ostrzeżenie: Nie znaleziono pliku statystyk: {STATS_PATH}. Arkusz Statystyki Technologii będzie pusty.")
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(data, orient='index')
    df.index.name = 'Technologia (Fraza OP)'
    df.columns = ["Liczba wystąpień w dopasowaniach OP->KP (in_jobs)", 
                  "Liczba pokryć w KP (in_courses)", 
                  "Suma wystąpień i pokryć"]
    df = df.reset_index()
    return df.sort_values(by='Suma wystąpień i pokryć', ascending=False)


# --- 4. GŁÓWNA FUNKCJA EKSPORTU ---

if __name__ == "__main__":
    print(f"--- Generowanie Raportu Analitycznego dla {YEAR}_{MONTH} ---")
    
    course_name_map = create_course_name_map()
    job_title_map = create_job_title_map() 

    # 1. Przetwarzanie danych
    df_kp_to_op = process_kp_to_op(course_name_map, job_title_map)
    df_op_to_kp = process_op_to_kp(course_name_map)
    
    # 2. Tworzenie podsumowań
    df_course_summary = create_course_summary(df_kp_to_op)
    df_tech_stats = process_technology_stats() 
    df_req_course_summary = create_requirement_course_summary(df_kp_to_op) 
    df_tech_course_summary = create_technology_course_summary(df_kp_to_op) 

    print("\nTworzenie raportu w Excelu...")
    
    try:
        with pd.ExcelWriter(OUTPUT_EXCEL_PATH, engine='xlsxwriter') as writer:
            
            # Arkusz 1: Podsumowanie Kursów
            if not df_course_summary.empty:
                df_course_summary.to_excel(writer, sheet_name='Podsumowanie Kursów', index=False)
            
            # Arkusz 2: Statystyki Technologii
            if not df_tech_stats.empty:
                df_tech_stats.to_excel(writer, sheet_name='Statystyki Technologii', index=False)
                
            # Arkusz 3: Powiązania Wymaganie-Przedmiot
            if not df_req_course_summary.empty:
                df_req_course_summary.to_excel(writer, sheet_name='Powiązania Wymaganie-Przedmiot', index=False)
            
            # Arkusz 4: Pokrycie Technologii
            if not df_tech_course_summary.empty:
                df_tech_course_summary.to_excel(writer, sheet_name='Pokrycie Technologii', index=False)
                
            # Arkusz 5: Detale KP -> OP
            if not df_kp_to_op.empty:
                df_kp_to_op.to_excel(writer, sheet_name='Detale KP -> OP', index=False)
                
            # Arkusz 6: Detale OP -> KP
            if not df_op_to_kp.empty:
                df_op_to_kp.to_excel(writer, sheet_name='Detale OP -> KP', index=False)
            
            print(f"Raport zapisany do: {OUTPUT_EXCEL_PATH}")

    except Exception as e:
        print(f"Błąd podczas zapisu pliku Excela: {e}")