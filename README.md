# books-lakehouse

Medallion pipeline nad Kaggle [Book-Crossing datasetem](https://www.kaggle.com/datasets/arashnic/book-recommendation-dataset)
na Databricks Free Edition, obohacená o druhý zdroj (Open Library) a
zakončená AI/BI dashboardem.

**Výsledky v číslech:** 1,15M ratingů (62,3 % implicitních), katalog
307 914 knih (36 925 dohledáno z Open Library — 62 % sirotčích ratingů
zachráněno), žebříčky přes bayesovský vážený rating (m=25 ukotveno
v datech), 47 unit testů, vše nasazované Asset Bundlem.

## Architektura

```
Kaggle CSV ─┐                          ┌─ silver.ratings (is_explicit, karanténa)
            ├→ landing volume          ├─ silver.books (Kaggle ∪ Open Library)
Open Library┘   ↓ Auto Loader          ├─ silver.book_enrichment (žánry, author_key)
(extraktor)   bronze (raw, append) ──→ ├─ silver.users (věk, lokace)
                                       ↓
                              gold: 8 views (vážený rating, KPI, trendy)
                                       ↓
                              AI/BI Dashboard (+ Genie)
```

Job se 3 tasky (bronze_ingest → silver_transform → gold_views) na
serverless, file arrival trigger připraven. Detaily: [docs/layers.md](docs/layers.md),
rozhodnutí a proč: [docs/architecture.md](docs/architecture.md),
celá cesta krok po kroku: [docs/journal.md](docs/journal.md).

## Struktura repa

```
databricks.yml              Asset Bundle (target dev, profil personal)
resources/pipeline.job.yml  job: 3 tasky, parametry, trigger
src/setup/init_schemas.py   jednorázové založení schémat a volumes
src/bronze/ingest_raw.py    Auto Loader: CSV + JSONL -> bronze
src/silver/transform.py     čištění, validace, union zdrojů
src/silver/lib/transforms.py  čistá pravidla (testovatelná bez Sparku)
src/gold/create_views.py    8 views: žebříčky, KPI, trendy, žánry
scripts/                    stažení dat, upload do volume, OL extraktor
tests/                      pytest nad transforms (47 testů)
docs/                       layers, architecture, journal, review
notebooks/exploration.sql   profilování bronze vrstvy
```

## Spuštění

```sh
# 1. auth (jednorázově): databricks auth login --host <workspace> --profile personal
# 2. schémata: spusť src/setup/init_schemas.py ve workspace
# 3. data:
./scripts/download_data.sh          # Kaggle CSV -> data/raw
./scripts/upload_to_volume.sh       # -> landing volume
python3 scripts/fetch_open_library.py   # enrichment (resumable)
# 4. deploy + běh:
databricks bundle deploy
databricks bundle run books_pipeline
```

## Testy

```sh
pip install -r requirements-dev.txt && pytest
```

## Klíčová rozhodnutí (zkráceně)

- **Rating 0 = implicitní feedback** (dle dokumentace datasetu) — flag,
  ne drop; kvalita ho filtruje, popularita potřebuje.
- **Full overwrite silver** — na této škále triviálně správné; škálovací
  cesta (streaming + MERGE) zdokumentována.
- **Vážený rating (IMDb vzorec), m=25** — medián 1 rating/knihu, prostý
  průměr by vynesl knihy s jedinou desítkou.
- **Druhý zdroj přes landing** — Open Library JSONL jde stejnou
  bronze→silver cestou jako CSV; architektura se nezměnila.
- Kompletní decision log s alternativami: [docs/architecture.md](docs/architecture.md)
