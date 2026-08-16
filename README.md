# books-lakehouse

Bronze–silver–gold pipeline nad Book-Crossing datasetem (Kaggle) na
Databricks Free Edition. Klasický job se třemi notebook tasky
(bronze_ingest → silver_transform → gold_views) + Asset Bundle;
vizualizace přes AI/BI Dashboard.

## Struktura

```
databricks.yml              bundle konfigurace (profil personal, target dev)
resources/pipeline.job.yml  job: 3 tasky na serverless, file arrival trigger
src/setup/init_schemas.py   jednorázové založení schémat a volumes
src/bronze/ingest_raw.py    Auto Loader: landing CSV -> bronze Delta tabulky
src/silver/transform.py     čištění + validace, full overwrite (TODO logika)
src/silver/lib/transforms.py  čisté funkce - testovatelné bez Sparku (TODO)
src/gold/create_views.py    views pro dashboard (TODO logika)
src/enrichment/             Open Library obohacení (nice-to-have)
tests/                      pytest nad lib/transforms.py
docs/layers.md              dokumentace vrstev = podklad prezentace
docs/architecture.md        decision log + produkční Azure návrh
```

## Setup (jednorázově)

1. CLI auth (hotovo): profil `personal` → Free Edition workspace.
2. Schémata a volumes: spusť `src/setup/init_schemas.py` ve workspace
   (po `bundle deploy` je v `.bundle/books-lakehouse/dev/files/src/setup/`).
3. Data: `./scripts/download_data.sh` (nebo ručně z Kaggle do `data/raw/`),
   pak `./scripts/upload_to_volume.sh`.

## Deploy + běh

```sh
databricks bundle validate
databricks bundle deploy
databricks bundle run books_pipeline
```

## Testy

```sh
pip install -r requirements-dev.txt
pytest
```

## Plán

- [ ] Večer 1: silver transformace (lib/transforms.py + testy), první běh
      pipeline, čísla o datové špíně do docs/layers.md
- [ ] Večer 2: gold views (vážený rating), AI/BI dashboard, docs + slides
- [ ] Nice-to-have: Open Library enrichment, inkrementální demo (file
      arrival trigger + druhá dávka CSV), karanténní tabulky + DQ metriky,
      GitHub Actions na pytest, Genie space
