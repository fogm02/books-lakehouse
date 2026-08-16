# Databricks notebook source
"""Gold — prezentační vrstva, čisté DDL (idempotentní).

Design: JEDNO VIEW NA GRAIN (zrnitost), ne na use-case. Prezentační řezy
(top-10, řazení, limity) dělají až konzumenti (dashboard datasety, Genie).

| view                        | grain                    |
|-----------------------------|--------------------------|
| v_books                     | kniha (title × author)   |
| v_authors                   | autor                    |
| v_books_by_genre            | kniha × žánr             |
| v_authors_by_year_publisher | autor × rok × vydavatel  |
| v_avg_rating_by_year        | rok vydání               |
| v_kpi_summary               | celek (1 řádek)          |

Vážený rating = bayesovský průměr (IMDb): (v/(v+m))·R + (m/(v+m))·C.
m (min_ratings, default 25) ukotveno v datech: medián ratingů/knihu = 1,
p99 = 22; citlivost m=10/25/50 -> stabilní špička. C = globální průměr
explicitních známek (~7,6).

Limit dat: ratingy nemají timestamp -> "období" = rok VYDÁNÍ (crawl končí
09/2004). Rating 0 = implicitní feedback -> měří popularitu (readers_total),
kvalitu měří jen explicitní známky (ratings_cnt, avg_rating, weighted_rating).

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

# úklid po konsolidaci na grain-based design (8 -> 6 views)
for old in ["v_top_books", "v_most_popular_books", "v_books_by_year", "v_top_authors"]:
    spark.sql(f"DROP VIEW IF EXISTS {gold}.{old}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.v_books
COMMENT 'Grain: kniha (title × author, vydání sloučena). readers_total = všechny interakce vč. implicitních (popularita); ratings_cnt/avg_rating/weighted_rating = jen explicitní známky (kvalita, bayesovský průměr m={m}). year_of_publication = rok prvního vydání.'
AS
WITH joined AS (
  SELECT r.rating, r.is_explicit, b.title, b.author, b.isbn,
         b.year_of_publication, b.image_url
  FROM {silver}.ratings r
  JOIN {silver}.books b ON r.isbn = b.isbn
),
g AS (SELECT AVG(rating) AS c FROM joined WHERE is_explicit),
per_book AS (
  SELECT
    title,
    author,
    MIN(year_of_publication)                            AS year_of_publication,
    COUNT(*)                                            AS readers_total,
    SUM(CASE WHEN is_explicit THEN 1 ELSE 0 END)        AS ratings_cnt,
    AVG(CASE WHEN is_explicit THEN rating END)          AS avg_rating,
    COUNT(DISTINCT isbn)                                AS editions,
    MAX(image_url)                                      AS image_url
  FROM joined
  GROUP BY title, author
)
SELECT
  title, author, year_of_publication, editions, readers_total, ratings_cnt,
  ROUND(avg_rating, 2) AS avg_rating,
  ROUND((ratings_cnt / (ratings_cnt + {m})) * avg_rating
      + ({m} / (ratings_cnt + {m})) * g.c, 3) AS weighted_rating,
  image_url
FROM per_book CROSS JOIN g
""")
print(f"✓ {gold}.v_books")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.v_authors
COMMENT 'Grain: autor. Bayesovský vážený rating (m={m}) přes explicitní známky všech knih autora. Autor = normalizovaný string - identity resolution přes OL author_key je future path.'
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
print(f"✓ {gold}.v_authors")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.v_books_by_genre
COMMENT 'Grain: kniha × žánr (subjects z Open Library, jen obohacená podmnožina). Subjects jsou knihovnické hlavičky s čárkami uvnitř - rozpadají se na atomické tokeny, balast "General" se zahazuje.'
AS
WITH explicit AS (
  SELECT r.rating, b.title, b.author, b.isbn
  FROM {silver}.ratings r
  JOIN {silver}.books b ON r.isbn = b.isbn
  WHERE r.is_explicit
),
g AS (SELECT AVG(rating) AS c FROM explicit),
per AS (
  SELECT INITCAP(TRIM(genre_token)) AS genre, e.title, e.author,
         COUNT(*) AS ratings_cnt, AVG(e.rating) AS avg_rating
  FROM explicit e
  JOIN {silver}.book_enrichment en ON e.isbn = en.isbn
  LATERAL VIEW EXPLODE(en.subjects) s AS subject_raw
  LATERAL VIEW EXPLODE(SPLIT(subject_raw, ',')) t AS genre_token
  WHERE TRIM(genre_token) != ''
    AND LOWER(TRIM(genre_token)) NOT IN ('general', 'etc', 'etc.')
  GROUP BY INITCAP(TRIM(genre_token)), e.title, e.author
)
SELECT genre, title, author, ratings_cnt,
  ROUND(avg_rating, 2) AS avg_rating,
  ROUND((ratings_cnt / (ratings_cnt + {m})) * avg_rating
      + ({m} / (ratings_cnt + {m})) * g.c, 3) AS weighted_rating
FROM per CROSS JOIN g
""")
print(f"✓ {gold}.v_books_by_genre")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.v_authors_by_year_publisher
COMMENT 'Grain: autor × rok vydání × vydavatel. Průměry explicitních známek pro filtrovatelnou analytiku.'
AS
SELECT
  b.author,
  b.year_of_publication,
  b.publisher,
  COUNT(*)                AS ratings_cnt,
  ROUND(AVG(r.rating), 2) AS avg_rating
FROM {silver}.ratings r
JOIN {silver}.books b ON r.isbn = b.isbn
WHERE r.is_explicit AND b.author IS NOT NULL
GROUP BY b.author, b.year_of_publication, b.publisher
""")
print(f"✓ {gold}.v_authors_by_year_publisher")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.v_avg_rating_by_year
COMMENT 'Grain: rok vydání. Průměrná explicitní známka vážená přes hodnocení. Pozor při čtení: starší roky = survivorship bias, crawl končí 09/2004.'
AS
SELECT
  b.year_of_publication,
  COUNT(DISTINCT b.isbn)  AS books_cnt,
  COUNT(*)                AS ratings_cnt,
  ROUND(AVG(r.rating), 2) AS avg_rating
FROM {silver}.ratings r
JOIN {silver}.books b ON r.isbn = b.isbn
WHERE r.is_explicit AND b.year_of_publication IS NOT NULL
GROUP BY b.year_of_publication
""")
print(f"✓ {gold}.v_avg_rating_by_year")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.v_kpi_summary
COMMENT 'Grain: celý dataset (1 řádek). KPI: katalog, interakce, podíl implicitních, globální průměr známek (C ze vzorce), efekt Open Library enrichmentu.'
AS
WITH r AS (
  SELECT rt.is_explicit, rt.rating, b.isbn IS NOT NULL AS has_book, b.source
  FROM {silver}.ratings rt
  LEFT JOIN {silver}.books b ON rt.isbn = b.isbn
)
SELECT
  (SELECT COUNT(*) FROM {silver}.books)                                  AS books_in_catalog,
  (SELECT COUNT(*) FROM {silver}.books WHERE source = 'open_library')    AS books_recovered,
  COUNT(*)                                                               AS interactions_total,
  SUM(CASE WHEN is_explicit THEN 1 ELSE 0 END)                           AS ratings_explicit,
  ROUND(100 * (1 - SUM(CASE WHEN is_explicit THEN 1 ELSE 0 END) / COUNT(*)), 1) AS implicit_pct,
  ROUND(AVG(CASE WHEN is_explicit THEN rating END), 2)                   AS avg_rating_explicit,
  SUM(CASE WHEN has_book AND source = 'open_library' THEN 1 ELSE 0 END)  AS ratings_recovered,
  SUM(CASE WHEN NOT has_book THEN 1 ELSE 0 END)                          AS ratings_orphaned
FROM r
""")
print(f"✓ {gold}.v_kpi_summary")

# COMMAND ----------

print("Gold views vytvořeny (grain-based, 6 views).")
