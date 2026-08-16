# books-lakehouse

Medallion pipeline nad [Book-Crossing datasetem](https://www.kaggle.com/datasets/arashnic/book-recommendation-dataset)
na Databricks Free Edition — obohacená o druhý zdroj (Open Library),
zakončená AI/BI dashboardem a Genie agentem.

**Výsledky v číslech:** 1,15 mil. hodnocení (62,3 % implicitních),
katalog 307 914 knih — z toho 36 925 dohledaných z Open Library, čímž se
zachránilo 62 % „sirotčích" hodnocení. Žebříčky stojí na bayesovském
váženém ratingu (m = 25, ukotveno v rozdělení dat). 48 unit testů,
všechno nasazované Asset Bundlem.

## Architektura

```
Kaggle (3× CSV) ──┐                          silver                 gold (6 views)
                  ├─→ landing ─→ bronze ───→ books ∪ enrichment ──→ v_books, v_authors,
Open Library ─────┘   volume    Auto Loader  ratings + karanténa    žánry, trendy, KPI
(vlastní extraktor)             append-only  users                        │
                                                              dashboard + Genie
```

Bronze drží fakta tak, jak přišla. Silver dělá rozhodnutí — typy,
pravidla, karanténa, spojení zdrojů. Gold dělá interpretace — slučování
vydání, vážené žebříčky, jedno view na zrnitost.

Kde číst dál: [architecture.md](docs/architecture.md) (diagram + decision
log), [layers.md](docs/layers.md) (vrstvy do detailu),
[journal.md](docs/journal.md) (celá cesta den po dni),
[review.md](docs/review.md) (revize řešení v půlce projektu).

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

## Klíčová rozhodnutí ve zkratce

- **Nula není známka.** 62 % hodnocení jsou implicitní interakce — drží
  se s příznakem, kvalita je filtruje, popularita je potřebuje.
- **Mazat co nejméně.** Vadný řádek → karanténa s důvodem; vadný atribut
  → NULL. Sirotčí hodnocení zůstala — a druhý zdroj je pak zachránil.
- **Malé vzorky lžou.** Medián 1 hodnocení na knihu → bayesovský vážený
  rating (vzorec IMDb), m = 25 ≈ 99. percentil rozdělení.
- **Druhý zdroj přitekl stejnou cestou jako první.** JSONL do landing
  zóny, Auto Loader, silver — architektura se nezměnila.

Kompletní decision log s alternativami: [docs/architecture.md](docs/architecture.md)
