# books-lakehouse

Řešení úkolu: bronze–silver–gold pipeline nad [Book-Crossing datasetem](https://www.kaggle.com/datasets/arashnic/book-recommendation-dataset)
na Databricks Free Edition. Nad rámec zadání jsem přidal druhý datový
zdroj (Open Library), AI/BI dashboard a Genie agenta — hlavně proto, že
10 % hodnocení v datasetu mířilo na knihy, které v katalogu vůbec nebyly,
a to se ukázalo jako opravitelné.

Pár čísel na úvod: 1 149 780 hodnocení, z toho 62,3 % implicitních.
Katalog má po obohacení 307 914 knih; 36 925 z nich je dohledaných přes
Open Library, čímž se zachránilo 62 % zmíněných „sirotčích" hodnocení.
Na transformační pravidla je 48 unit testů. Celé se to nasazuje jedním
příkazem přes Asset Bundle.

## Architektura

```
Kaggle (3× CSV) ──┐                          silver                 gold (6 views)
                  ├─→ landing ─→ bronze ───→ books ∪ enrichment ──→ v_books, v_authors,
Open Library ─────┘   volume    Auto Loader  ratings + karanténa    žánry, trendy, KPI
(vlastní extraktor)             append-only  users                        │
                                                              dashboard + Genie
```

Bronze je surová append-only kopie obou zdrojů, všechno jako string.
Čištění, validace a spojení zdrojů se děje v silveru — co je vadné celé,
končí v karanténě s uvedeným důvodem; vadné atributy se nulují a řádek
zůstává. Gold má šest views, jedno na zrnitost: tam žijí agregace,
slučování vydání a vážený rating.

Kde číst dál: [architecture.md](docs/architecture.md) shrnuje
nejdůležitější rozhodnutí, [layers.md](docs/layers.md) popisuje vrstvy
do detailu.

## Struktura repa

```
databricks.yml                  Asset Bundle (target dev)
resources/                      job (3 tasky, file arrival trigger), dashboard
src/setup/init_schemas.py       jednorázové založení schémat a volumes
src/bronze/ingest_raw.py        Auto Loader: CSV + JSONL → bronze
src/silver/transform.py         čištění, validace, spojení zdrojů
src/silver/lib/transforms.py    čistá pravidla — testovatelná bez Sparku
src/gold/create_views.py        6 views s komentáři v Unity Catalogu
src/dashboards/                 AI/BI dashboard jako JSON definice
src/genie/                      export konfigurace Genie space
scripts/                        stažení dat, upload, Open Library extraktor
tests/                          pytest nad transforms (48 testů)
docs/                           architektura, vrstvy, deník, revize
notebooks/exploration.sql       profilování bronze vrstvy
```

## Spuštění

```sh
# 1) auth (jednorázově):
databricks auth login --host <workspace-url> --profile personal
# 2) schémata a volumes: spusť src/setup/init_schemas.py ve workspace
# 3) data:
./scripts/download_data.sh              # Kaggle CSV → data/raw
./scripts/upload_to_volume.sh           # → landing volume
python3 scripts/fetch_open_library.py   # enrichment (resumable, ~30 min)
# 4) nasazení a běh:
databricks bundle deploy
databricks bundle run books_pipeline
```

Testy: `pip install -r requirements-dev.txt && pytest`

## Nejdůležitější rozhodnutí

Asi nejzásadnější bylo nechat v datech hodnocení s nulou, kterých je
62 %. Nula podle dokumentace datasetu není známka, ale záznam typu
„uživatel měl knihu v ruce" — pro žebříčky kvality se filtruje, pro
popularitu je to naopak hlavní signál. Ze stejného důvodu jsem nemazal
ani hodnocení knih, které katalog neznal: samotné hodnocení vadné nebylo,
chyběl k němu jen katalogový záznam. Právě tahle množina se později stala
seznamem „co dohledat" pro druhý zdroj.

U žebříčků bylo nutné vyřešit, že medián počtu známek na knihu je 1 —
prostý průměr by vyhrávaly knihy s jedinou desítkou. Používám proto
bayesovský vážený rating (vzorec známý z IMDb Top 250) s m = 25, což
odpovídá 99. percentilu rozdělení; citlivost jsem ověřil pro m = 10/25/50.

Další rozhodnutí a limity dat shrnuje
[docs/architecture.md](docs/architecture.md).
