# Databricks notebook source
"""Gold — prezentační vrstva pro dashboard. Čisté DDL (idempotentní).

Views (zdroj: silver, inner join ratings×books vyřadí sirotčí ISBN):
- v_top_books:                 knihy dle VÁŽENÉHO ratingu, vydání sloučená
                               přes (title, author)
- v_top_authors:               autoři dle váženého ratingu
- v_authors_by_year_publisher: průměry per autor × rok vydání × vydavatel

Vážený rating = bayesovský průměr (IMDb vzorec):
    weighted = (v/(v+m))*R + (m/(v+m))*C
m (min_ratings, default 25): ukotveno v datech — medián ratingů na knihu
je 1, p99 = 22; citlivostní analýza m=10/25/50 ukázala stabilní špičku,
posuny jen na pozicích 5-10 (nika vs. mainstream). Viz docs/journal.md.

Limit dat: ratingy nemají timestamp -> "top za období" jde interpretovat
jen přes rok vydání (v_authors_by_year_publisher), ne přes čas hodnocení.

Parameters (from notebook_task.base_parameters):
    catalog, schema_silver, schema_gold, min_ratings
"""

# COMMAND ----------

dbutils.widgets.text("catalog", "books")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_gold", "gold")
dbutils.widgets.text("min_ratings", "25")

catalog       = dbutils.widgets.get("catalog")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold   = dbutils.widgets.get("schema_gold")
m             = int(dbutils.widgets.get("min_ratings"))

silver = f"{catalog}.{schema_silver}"
gold   = f"{catalog}.{schema_gold}"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.v_top_books
COMMENT 'Knihy podle bayesovského váženého ratingu (m={m}). Vydání sloučená přes (title, author) — sloupec editions ukazuje kolik ISBN se spojilo.'
AS
WITH explicit AS (
  SELECT r.rating, b.title, b.author, b.isbn, b.image_url
  FROM {silver}.ratings r
  JOIN {silver}.books b ON r.isbn = b.isbn
  WHERE r.is_explicit
),
g AS (SELECT AVG(rating) AS c FROM explicit),
per_book AS (
  SELECT
    title,
    author,
    COUNT(*)              AS ratings_cnt,
    AVG(rating)           AS avg_rating,
    COUNT(DISTINCT isbn)  AS editions,
    MAX(image_url)        AS image_url
  FROM explicit
  GROUP BY title, author
)
SELECT
  title,
  author,
  editions,
  ratings_cnt,
  ROUND(avg_rating, 2) AS avg_rating,
  ROUND((ratings_cnt / (ratings_cnt + {m})) * avg_rating
      + ({m} / (ratings_cnt + {m})) * g.c, 3) AS weighted_rating,
  image_url
FROM per_book CROSS JOIN g
""")
print(f"✓ {gold}.v_top_books")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.v_top_authors
COMMENT 'Autoři podle bayesovského váženého ratingu (m={m}) přes explicitní hodnocení jejich knih.'
AS
WITH explicit AS (
  SELECT r.rating, b.author
  FROM {silver}.ratings r
  JOIN {silver}.books b ON r.isbn = b.isbn
  WHERE r.is_explicit AND b.author IS NOT NULL
),
g AS (SELECT AVG(rating) AS c FROM explicit),
per_author AS (
  SELECT author, COUNT(*) AS ratings_cnt, AVG(rating) AS avg_rating
  FROM explicit
  GROUP BY author
)
SELECT
  author,
  ratings_cnt,
  ROUND(avg_rating, 2) AS avg_rating,
  ROUND((ratings_cnt / (ratings_cnt + {m})) * avg_rating
      + ({m} / (ratings_cnt + {m})) * g.c, 3) AS weighted_rating
FROM per_author CROSS JOIN g
""")
print(f"✓ {gold}.v_top_authors")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.v_authors_by_year_publisher
COMMENT 'Průměrný rating per autor × rok vydání × vydavatel. Období = rok VYDÁNÍ (ratingy nemají timestamp — limit zdroje).'
AS
SELECT
  b.author,
  b.year_of_publication,
  b.publisher,
  COUNT(*)             AS ratings_cnt,
  ROUND(AVG(r.rating), 2) AS avg_rating
FROM {silver}.ratings r
JOIN {silver}.books b ON r.isbn = b.isbn
WHERE r.is_explicit AND b.author IS NOT NULL
GROUP BY b.author, b.year_of_publication, b.publisher
""")
print(f"✓ {gold}.v_authors_by_year_publisher")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.v_most_popular_books
COMMENT 'Knihy podle počtu čtenářů - všechny interakce vč. implicitních (rating 0 = zalogováno bez známky). Popularita není kvalita: srovnej s v_top_books (Wild Animus story).'
AS
SELECT
  b.title,
  b.author,
  COUNT(*)                                                   AS readers_total,
  SUM(CASE WHEN r.is_explicit THEN 1 ELSE 0 END)             AS readers_explicit,
  ROUND(AVG(CASE WHEN r.is_explicit THEN r.rating END), 2)   AS avg_rating,
  COUNT(DISTINCT r.isbn)                                     AS editions,
  MAX(b.image_url)                                           AS image_url
FROM {silver}.ratings r
JOIN {silver}.books b ON r.isbn = b.isbn
GROUP BY b.title, b.author
""")
print(f"✓ {gold}.v_most_popular_books")

# COMMAND ----------

print("Gold views vytvořeny.")
