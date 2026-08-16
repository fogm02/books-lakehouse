"""Obohacení knih z Open Library API (druhý datový zdroj).

Strategie: NEenrichuj všech 271k ISBN (rate limity), jen top-N knih,
které projdou do gold vrstvy. Výstup ulož jako JSON do landing volume
(subdir open_library/) -> stane se dalším bronze zdrojem a jde stejnou
cestou bronze -> silver jako zbytek dat.

API: https://openlibrary.org/isbn/{isbn}.json
     (jazyk, počet stran, subjects/žánry)

TODO(večer 2 / nice-to-have):
- vytáhni top-N ISBN z gold.top_books (databricks sql query, nebo export)
- requests s retry + rate limit (~1 req/s slušnost)
- zapiš JSONL do /Volumes/books/landing/raw/open_library/
"""
