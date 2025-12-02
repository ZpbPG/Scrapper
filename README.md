## Opis folderów

- **job_offers_json** - Folder przechowujący wszystkie zebrane dane o ofertach pracy.
- **job_offers_tokenized** - Folder przechowujący stokenizowane dane o ofertach pracy.
- **json_karty** - Folder przechowujący wszelkie obrobione dane o kartach przedmiotów.
- **karty_przedmiotow** - Folder przechowujący karty przedmiotów.
- **op_to_kp** - Folder przechowujący jsony z danymi o technologii z ofert pracy w kartach przedmiotów.

## Opis plików

- **details_2025-01.json** - przykładowy plik z danymi o ofertach pracy.
- **ngrams_details_2025-11_specification.json** - przykładowy plik z stokenizowanymi danymi o ofertach pracy.
- **clean_wszystkie_karty.json** - plik z oczyszczonymi danymi z kart przedmiotów.
- **ngrams_per_course.json** - plik ze stokenizowanymi danymi z kart przedmiotów.
- **wszystkie_karty.json** - plik z nie oczyszczonymi danymi z kart przedmiotów.
- **372960.pdf** - przykładowy plik z danymi z karty przedmiotu.
- **course_embeddings.pt** - plik z embedingami wygenerowanymi z oferty pracy podczas tworzenia pliku **op_to_kp_matches_no_tokenization.json**. Przydatny tylko podczas debugowania.
- **op_phrases.json** - plik z frazami wygenerowanymi z oferty pracy podczas tworzenia pliku **op_to_kp_matches_no_tokenization.json**. Przydatny tylko podczas debugowania.
- **op_to_kp_matches_no_tokenization_2025_01.json** - przykładowy plik z pokryciem danych z ofert pracy w kartach przedmiotów.

## Opis skryptów

- **pt_reader.py** - skrypt odczytujący **course_embeddings.pt**. Można dowolnie modyfikować.
- **clean_courses.py** - skrypt oczyszczający dane z **wszystkie_karty.json** i zapisujący je do **clean_wszystkie_karty.json**
- **download_courses.py** - pobiera karty przedmiotów z podanych URLi i zapisuje w folderze **karty_przedmiotów**
- **op_to_kp_algorithm_with_tokenization.py** - wykonuje porównanie pokrycia technologii z ofert pracy na karty przedmiotów. Wykorzystuje dane o ofertach pracy z folderu **job_offers_tokenized**. Wyniki zapisuje w pliku **op_to_kp_matches.json** w folderze głównym projektu. Nazwę pliku wynikowego należy następnie zmienić ręcznie na odpowiednią plikowi z ofert pracy. Plik ofert pracy wybieramy ręcznie poprzez zmianę nazwy w kodzie.
- **op_to_kp_algorithm_without_tokenization.py** - wykonuje porównanie pokrycia technologii z ofert pracy na karty przedmiotów. Wykorzystuje dane o ofertach pracy z folderu **job_offers_json**. Wyniki zapisuje w pliku **op_to_kp_matches_no_tokenization.json** w folderze **op_to_kp**. Plik ofert pracy wybieramy ręcznie poprzez zmianę nazwy w kodzie.Nazwę pliku wynikowego należy zmienić ręcznie na odpowiednią plikowi z ofert pracy.
- **scrap_courses.py** - zczytuje dane z wszystkich kart przedmiotów znajdujących się w folderze **karty_przedmiotow**, i zapisuje je do pliku **wszystkie_karty.json** w folderze **json_karty**.
- **tokenize_courses.py** - tokenizuje dane z pliku **clean_wszystkie_karty.json** w folderze **json_karty** i zapisuje wyniki do pliku **ngrams_per_course.json** w tym samym folderze.
- **tokenize_job_offers.py** - tokenizuje dane z wybranej oferty pracy z folderu **job_offers_json** i zapisuje wynik w folderze **job_offers_tokenized**

## Główne flow

1. Odpalić **download_courses.py**. UWAGA! Karty przedmiotów umieszczone są w repozytorium GIT, możliwe pominięcie kroku.
2. Odpalić **scrap_courses.py**. UWAGA! Plik wynikowy umieszczony jest w repozytorium GIT, możliwe pominięcie kroku.
3. Odpalić **clean_courses.py**. UWAGA! Plik wynikowy umieszczony jest w repozytorium GIT, możliwe pominięcie kroku.
4. Odpalić **tokenize_courses.py**. UWAGA! Plik wynikowy umieszczony jest w repozytorium GIT, możliwe pominięcie kroku.
5. Pobrać oferty pracy ręcznie z dysku google. UWAGA! Oferty pracy umieszczone są w repozytorium GIT, możliwe pominięcie kroku.
6. Odpalić **op_to_kp_algorithm_without_tokenization.py** osobno dla każdego pliku z ofertami pracy
