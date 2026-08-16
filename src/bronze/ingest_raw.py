# Databricks notebook source
"""Bronze ingestion — Auto Loader nad landing volume s CSV z Kaggle.

Tři streamy: Books.csv → books, Ratings.csv → ratings, Users.csv → users.

Bronze = raw 1:1 reprezentace zdroje: všechny sloupce string, žádné čištění,
jen technická metadata (_source_file, _ingested_at) a _rescued_data pro
řádky, které nesedí do schématu. Typování a sémantika patří do silver.

Na rozdíl od práce (parquet + binaryFile/foreachBatch kvůli exploding
schema) tady stačí přímý cloudFiles CSV stream — schéma je stabilní.

Parameters (from notebook_task.base_parameters):
    catalog:         Katalog projektu (např. "books")
    schema_bronze:   Cílové bronze schéma
    landing_base:    Cesta k landing volume se CSV
    checkpoint_base: Cesta pro Auto Loader checkpointy
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

# glob pattern místo přesného jména -> inkrementální demo: další dávku
# nahraješ jako např. Ratings_batch2.csv a Auto Loader ji chytne
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

print("Bronze ingest dokončen.")
