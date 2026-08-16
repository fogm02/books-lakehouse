# Databricks notebook source
"""Silver — typovaná, vyčištěná a validovaná data. Full overwrite z bronze.

Full overwrite (vzor z praxe): silver vždy odráží aktuální stav bronze,
změna business logiky = deploy kódu a rebuild, bronze zůstává untouched.
Na 1,1M ratingů běží v sekundách, trade-off zanedbatelný. Škálovací cesta
(future improvement, viz decision log): ratings číst z bronze streamingem
s checkpointem a appendovat, dimenze MERGE.

Pravidla čištění = rozhodnutí z explorace (docs/journal.md 15. 8. 2026):
1. rating 0 = implicitní feedback -> flag is_explicit, řádky zůstávají
2. sirotčí ratingy (ISBN mimo books) zůstávají - vyřadí je až join v goldu
3. posunuté řádky books (nečíselný rok) -> karanténa s reason
4. věk mimo 5-100 -> NULL atributu, řádek zůstává
5. location parsovaná pozičně zprava, placeholdery/escape smetí -> NULL

Parameters (from notebook_task.base_parameters):
    catalog, schema_bronze, schema_silver
"""

# COMMAND ----------

dbutils.widgets.text("catalog", "books")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_silver", "silver")

catalog       = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")

# COMMAND ----------

import sys
from pathlib import Path

try:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
except NameError:
    sys.path.insert(0, "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[0])

from pyspark.sql import functions as F, types as T

from lib.transforms import clean_age, clean_year, normalize_author, parse_location

bronze = f"{catalog}.{schema_bronze}"
silver = f"{catalog}.{schema_silver}"

# UDF místo nativních výrazů: vědomý trade-off - stejný kód, který kryjí
# pytest testy, se používá i v pipeline. Na 1,1M řádků je režie UDF
# zanedbatelná; při větší škále přepsat na nativní sloupcové výrazy
# a lib/ nechat jako referenční implementaci.
clean_year_udf = F.udf(clean_year, T.IntegerType())
clean_age_udf = F.udf(clean_age, T.IntegerType())
normalize_author_udf = F.udf(normalize_author, T.StringType())
parse_location_udf = F.udf(
    parse_location,
    T.StructType([
        T.StructField("city", T.StringType()),
        T.StructField("state", T.StringType()),
        T.StructField("country", T.StringType()),
    ]),
)


def overwrite(df, table: str) -> None:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"✓ {table}: {spark.table(table).count():,} řádků")

# COMMAND ----------

# --- ratings ---
# Explorace: hodnoty jen 0-10, karanténa je tu pojistka na budoucí dodávky.

ratings_typed = (
    spark.table(f"{bronze}.ratings")
    .select(
        F.col("`User-ID`").cast("int").alias("user_id"),
        F.upper(F.trim(F.col("ISBN"))).alias("isbn"),
        F.col("`Book-Rating`").cast("int").alias("rating"),
        "_source_file",
        "_ingested_at",
    )
)

ratings_invalid_cond = (
    F.col("user_id").isNull()
    | F.col("isbn").isNull() | (F.col("isbn") == "")
    | F.col("rating").isNull() | ~F.col("rating").between(0, 10)
)

quarantined_ratings = ratings_typed.where(ratings_invalid_cond).withColumn(
    "reason", F.lit("null_key_or_rating_out_of_range")
)
overwrite(quarantined_ratings, f"{silver}.quarantine_ratings")

ratings = (
    ratings_typed.where(~ratings_invalid_cond)
    .withColumn("is_explicit", F.col("rating") > 0)
    .withColumn("_transformed_at", F.current_timestamp())
)
overwrite(ratings, f"{silver}.ratings")

# COMMAND ----------

# --- books ---
# Posunuté řádky: rok vydání vyplněný, ale nečíselný (autor/text ve sloupci
# roku) => celý řádek je nedůvěryhodný -> karanténa. Rok "0" a budoucnost
# jsou formátově validní řádky -> zůstávají, jen rok -> NULL (clean_year).
# Katalog = Kaggle (source='kaggle') + knihy dohledané z Open Library pro
# sirotčí ISBN (source='open_library', jen ty co v Kaggle nejsou).

has_open_library = spark.catalog.tableExists(f"{bronze}.open_library")

books_raw = spark.table(f"{bronze}.books")

shifted_cond = F.col("`Year-Of-Publication`").isNotNull() & F.expr(
    "TRY_CAST(`Year-Of-Publication` AS INT) IS NULL"
)

quarantined_books = books_raw.where(shifted_cond).withColumn(
    "reason", F.lit("shifted_columns_nonnumeric_year")
)
overwrite(quarantined_books, f"{silver}.quarantine_books")

books_kaggle = (
    books_raw.where(~shifted_cond)
    .select(
        F.upper(F.trim(F.col("ISBN"))).alias("isbn"),
        F.col("`Book-Title`").alias("title"),
        normalize_author_udf("`Book-Author`").alias("author"),
        clean_year_udf("`Year-Of-Publication`").alias("year_of_publication"),
        F.col("Publisher").alias("publisher"),
        F.col("`Image-URL-L`").alias("image_url"),
        "_source_file",
        "_ingested_at",
    )
    # explorace: 0 duplicit na raw ISBN, 314 po UPPER/TRIM
    .dropDuplicates(["isbn"])
    .withColumn("source", F.lit("kaggle"))
)

if has_open_library:
    books_recovered = (
        spark.table(f"{bronze}.open_library")
        .where(F.col("found") & (F.col("target") == "orphan") & F.col("title").isNotNull())
        .select(
            F.upper(F.trim(F.col("isbn"))).alias("isbn"),
            F.col("title"),
            # get() místo [0]: ANSI mód hází chybu při indexaci prázdného pole
            normalize_author_udf(F.expr("get(authors, 0).name")).alias("author"),
            clean_year_udf(F.regexp_extract("publish_date", r"(\d{4})", 1)).alias("year_of_publication"),
            F.lit(None).cast("string").alias("publisher"),   # fetch je nestahuje
            F.lit(None).cast("string").alias("image_url"),
            "_source_file",
            "_ingested_at",
        )
        .dropDuplicates(["isbn"])
        # při konfliktu vyhrává Kaggle - dohledáváme jen skutečné sirotky
        .join(books_kaggle.select("isbn"), "isbn", "left_anti")
        .withColumn("source", F.lit("open_library"))
    )
    books_all = books_kaggle.unionByName(books_recovered)
else:
    print("bronze.open_library neexistuje - katalog jen z Kaggle.")
    books_all = books_kaggle

books = books_all.withColumn("_transformed_at", F.current_timestamp())
overwrite(books, f"{silver}.books")

# COMMAND ----------

# --- book_enrichment (Open Library: žánry, author_key, počet stran) ---

if has_open_library:
    enrichment = (
        spark.table(f"{bronze}.open_library")
        .where(F.col("found"))
        .select(
            F.upper(F.trim(F.col("isbn"))).alias("isbn"),
            F.col("target"),
            F.expr("get(authors, 0).key").alias("author_key"),
            F.col("subjects"),
            F.col("number_of_pages").cast("int").alias("pages"),
            "_ingested_at",
        )
        .dropDuplicates(["isbn"])
        .withColumn("_transformed_at", F.current_timestamp())
    )
    overwrite(enrichment, f"{silver}.book_enrichment")

# COMMAND ----------

# --- users ---
# Věk i lokace se NULLují na úrovni atributu - řádek uživatele zůstává
# (je dál použitelný pro joiny), žádná karanténa.

users = (
    spark.table(f"{bronze}.users")
    .select(
        F.col("`User-ID`").cast("int").alias("user_id"),
        clean_age_udf("Age").alias("age"),
        parse_location_udf("Location").alias("loc"),
        "_source_file",
        "_ingested_at",
    )
    .select(
        "user_id",
        "age",
        F.col("loc.city").alias("city"),
        F.col("loc.state").alias("state"),
        F.col("loc.country").alias("country"),
        "_source_file",
        "_ingested_at",
    )
    .withColumn("_transformed_at", F.current_timestamp())
)
overwrite(users, f"{silver}.users")

# COMMAND ----------

# --- DQ shrnutí běhu (čísla do prezentace) ---

for tbl in ["ratings", "books", "users", "quarantine_ratings", "quarantine_books"]:
    print(f"{silver}.{tbl}: {spark.table(f'{silver}.{tbl}').count():,}")

print("Silver transform dokončen.")
