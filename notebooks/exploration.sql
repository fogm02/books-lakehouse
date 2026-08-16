-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Exploration — profilování bronze vrstvy
-- MAGIC Odpovědi (čísla!) průběžně zapisuj do `docs/layers.md` — jsou to
-- MAGIC podklady pro čistící pravidla v silveru i pro prezentaci.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q1: Distribuce ratingů — kolik je implicitních nul? Jsou tam hodnoty mimo 0–10?

-- COMMAND ----------

SELECT
  `Book-Rating`                                        AS rating,
  COUNT(*)                                             AS cnt,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)     AS pct
FROM books.bronze.ratings
GROUP BY `Book-Rating`
ORDER BY TRY_CAST(`Book-Rating` AS INT)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q2: Sirotčí ratingy — ISBN, která v books neexistují

-- COMMAND ----------

SELECT COUNT(*) AS orphan_ratings
FROM books.bronze.ratings r
LEFT ANTI JOIN books.bronze.books b
  ON UPPER(TRIM(r.ISBN)) = UPPER(TRIM(b.ISBN))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q3: Rok vydání — rozsah, nuly, budoucnost, nečíselné hodnoty (= posunuté řádky)

-- COMMAND ----------

SELECT
  MIN(TRY_CAST(`Year-Of-Publication` AS INT))                                   AS min_year,
  MAX(TRY_CAST(`Year-Of-Publication` AS INT))                                   AS max_year,
  SUM(CASE WHEN `Year-Of-Publication` = '0' THEN 1 ELSE 0 END)                  AS year_zero,
  SUM(CASE WHEN TRY_CAST(`Year-Of-Publication` AS INT) > YEAR(CURRENT_DATE()) THEN 1 ELSE 0 END) AS future,
  SUM(CASE WHEN TRY_CAST(`Year-Of-Publication` AS INT) IS NULL THEN 1 ELSE 0 END) AS non_numeric
FROM books.bronze.books

-- COMMAND ----------

-- posunuté řádky v plné kráse
SELECT * FROM books.bronze.books
WHERE TRY_CAST(`Year-Of-Publication` AS INT) IS NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q4: Duplicitní ISBN — kolik jich je a čím se liší?

-- COMMAND ----------

SELECT ISBN, COUNT(*) AS cnt
FROM books.bronze.books
GROUP BY ISBN
HAVING COUNT(*) > 1
ORDER BY cnt DESC

-- COMMAND ----------

-- detail jednoho duplikátu: doplň ISBN z výsledku výše
-- SELECT * FROM books.bronze.books WHERE ISBN = '...'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q5: Varianty zápisu autora

-- COMMAND ----------

SELECT `Book-Author`, COUNT(*) AS cnt
FROM books.bronze.books
WHERE `Book-Author` ILIKE '%rowling%'
GROUP BY `Book-Author`
ORDER BY cnt DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q6: Age — rozsah, NULL, mimo rozumné meze

-- COMMAND ----------

SELECT
  MIN(TRY_CAST(Age AS INT))                                                    AS min_age,
  MAX(TRY_CAST(Age AS INT))                                                    AS max_age,
  SUM(CASE WHEN TRY_CAST(Age AS INT) IS NULL THEN 1 ELSE 0 END)                AS null_or_nonnumeric,
  SUM(CASE WHEN TRY_CAST(Age AS INT) < 5 OR TRY_CAST(Age AS INT) > 110 THEN 1 ELSE 0 END) AS out_of_range
FROM books.bronze.users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q7: Location — má vždy 3 části "city, state, country"?

-- COMMAND ----------

SELECT SIZE(SPLIT(Location, ',')) AS parts, COUNT(*) AS cnt
FROM books.bronze.users
GROUP BY parts
ORDER BY parts
