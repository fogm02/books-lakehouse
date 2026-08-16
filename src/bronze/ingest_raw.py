# Databricks notebook source
"""Bronze — Auto Loader nad landing volume.

Raw 1:1 kopie zdrojů: všechny sloupce string, žádné čištění. Přidávají se
jen technická metadata (_source_file, _ingested_at) a _rescued_data pro
řádky mimo schéma. Typování a sémantika patří do silveru — když selže tam,
bronze zůstává k dispozici na přehrání.

Parametry (z notebook_task.base_parameters):
    catalog, schema_bronze, landing_base, checkpoint_base
"""

# COMMAND ----------

dbutils.widgets.text("catalog", "books")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("landing_base", "/Volumes/books/landing/raw")
dbutils.widgets.text("checkpoint_base", "/Volumes/books/bronze/checkpoints")

catalog         = dbutils.widgets.get("catalog")
schema_bronze   = dbutils.widgets.get("schema_bronze")
landing_base    = dbutils.widgets.get("landing_base")
checkpoint_base = dbutils.widgets.get("checkpoint_base")

# COMMAND ----------

from pyspark.sql import functions as F

# glob místo přesného jména: další dávka (např. Ratings_2.csv) se zpracuje
# bez změny kódu
SOURCES = {
    "books":   "Books*.csv",
    "ratings": "Ratings*.csv",
    "users":   "Users*.csv",
}


def ingest(name: str, pattern: str) -> None:
    """Jeden Auto Loader stream: landing CSV -> bronze Delta tabulka (append)."""
    target = f"{catalog}.{schema_bronze}.{name}"
    print(f"Spouštím stream {pattern} -> {target}")
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        # bronze drží vše jako string; typování je práce silver vrstvy
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaLocation", f"{checkpoint_base}/{name}_schema")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(f"{landing_base}/{pattern}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    (
        stream.writeStream
        .option("checkpointLocation", f"{checkpoint_base}/{name}")
        .trigger(availableNow=True)
        .toTable(target)
        .awaitTermination()
    )
    print(f"✓ {target}")

# COMMAND ----------

for _name, _pattern in SOURCES.items():
    ingest(_name, _pattern)

# COMMAND ----------

# --- druhý zdroj: Open Library JSONL ---
# Tady typy inferujeme: data generuje scripts/fetch_open_library.py, schéma
# je stabilní a vnořené (authors: array<struct>, subjects: array<string>).

OL_PATH = f"{landing_base}/open_library/"


def _path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


if _path_exists(OL_PATH):
    target = f"{catalog}.{schema_bronze}.open_library"
    print(f"Spouštím stream open_library -> {target}")
    ol_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{checkpoint_base}/open_library_schema")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(OL_PATH)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    (
        ol_stream.writeStream
        .option("checkpointLocation", f"{checkpoint_base}/open_library")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target)
        .awaitTermination()
    )
    print(f"✓ {target}")
else:
    print(f"Open Library landing ({OL_PATH}) zatím neexistuje - přeskakuji.")

print("Bronze ingest dokončen.")
