# Databricks notebook source
"""Setup — vytvoření schémat a volumes pro books pipeline.
Spustit JEDNOU ručně před prvním spuštěním pipeline.

Parameters (from dbutils.widgets):
    catalog: Katalog projektu (default: books)
"""

# COMMAND ----------

dbutils.widgets.text("catalog", "books")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

for schema in ["landing", "bronze", "silver", "gold"]:
    sql = f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
    print(f"Running: {sql}")
    spark.sql(sql)
    print(f"✓ {catalog}.{schema} exists")

# COMMAND ----------

# --- Volumes: landing na zdrojové CSV, checkpoints pro Auto Loader ---

for volume in [f"{catalog}.landing.raw", f"{catalog}.bronze.checkpoints"]:
    sql = f"CREATE VOLUME IF NOT EXISTS {volume}"
    print(f"Running: {sql}")
    spark.sql(sql)
    print(f"✓ Volume {volume} exists")

# COMMAND ----------

print("Setup complete — all schemas and volumes created.")
