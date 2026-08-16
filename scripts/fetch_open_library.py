#!/usr/bin/env python3
"""Stáhne metadata knih z Open Library (batch Books API) do JSONL.

Cíle (viz docs/review.md, docs/journal.md):
1. sirotčí ISBN z Ratings.csv (validní formát, >=1 interakce) - dohledání
   knih chybějících v katalogu
2. top N katalogových ISBN podle počtu interakcí - žánry/author_key

Výstup: data/enrichment/open_library.jsonl - jeden řádek na ISBN:
  {"isbn": ..., "found": true/false, "title": ..., "authors": [{"name","key"}],
   "subjects": [...], "number_of_pages": ..., "publish_date": ..., "target": ...}

Rerun je resumable - už stažená ISBN se přeskočí. Rate limit ~1 req/s,
batch 50 ISBN/request (User-Agent s kontaktem dle OL etikety).
"""
import csv
import json
import ssl
import sys
import time
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path

import certifi

# macOS Python bez CA bundle -> explicitní kontext s certifi
SSL_CTX = ssl.create_default_context(cafile=certifi.where())

REPO = Path(__file__).resolve().parent.parent
RAW = REPO / "data" / "raw"
OUT = REPO / "data" / "enrichment" / "open_library.jsonl"
TOP_CATALOG = 2000
BATCH = 50
UA = "books-lakehouse-assignment/0.1 (kontakt: matej.fogy@gmail.com)"
KEYS_FILE = REPO / "data" / "enrichment" / "ol_keys.env"


def _load_auth() -> str | None:
    """Archive.org S3 klíče (gitignored soubor) -> 'LOW access:secret'."""
    if not KEYS_FILE.exists():
        return None
    kv = dict(
        line.strip().split("=", 1)
        for line in KEYS_FILE.read_text().splitlines()
        if "=" in line
    )
    if kv.get("OL_S3_ACCESS") and kv.get("OL_S3_SECRET"):
        return f"LOW {kv['OL_S3_ACCESS']}:{kv['OL_S3_SECRET']}"
    return None


AUTH = _load_auth()
# s přihlášením povoluje OL ~3 req/s, bez něj se držíme ~1 req/s
SLEEP_S = 0.35 if AUTH else 1.1

csv.field_size_limit(10_000_000)


def valid_isbn(s: str) -> bool:
    return len(s) == 10 and all(c.isdigit() or c == "X" for c in s)


def build_target_list() -> list[tuple[str, str]]:
    books = set()
    with open(RAW / "Books.csv", encoding="utf-8", errors="replace", newline="") as f:
        r = csv.reader(f)
        next(r)
        for row in r:
            if row:
                books.add(row[0].strip().upper())

    orphans = Counter()
    catalog = Counter()
    with open(RAW / "Ratings.csv", encoding="utf-8", errors="replace", newline="") as f:
        r = csv.reader(f)
        next(r)
        for row in r:
            if len(row) != 3:
                continue
            isbn = row[1].strip().upper()
            (catalog if isbn in books else orphans)[isbn] += 1

    targets = [(i, "orphan") for i in orphans if valid_isbn(i)]
    targets += [(i, "catalog") for i, _ in catalog.most_common(TOP_CATALOG) if valid_isbn(i)]
    return targets


def fetch_batch(isbns: list[str]) -> dict | None:
    """Vrací dict s výsledky, nebo None při totálním selhání requestu.
    None => batch se NEZAPISUJE, aby ho příští (resumable) běh zopakoval."""
    bibkeys = ",".join(f"ISBN:{i}" for i in isbns)
    url = "https://openlibrary.org/api/books?" + urllib.parse.urlencode(
        {"bibkeys": bibkeys, "format": "json", "jscmd": "data"}
    )
    headers = {"User-Agent": UA}
    if AUTH:
        headers["Authorization"] = AUTH
    req = urllib.request.Request(url, headers=headers)
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=60, context=SSL_CTX) as resp:
                return json.loads(resp.read())
        except Exception as e:
            wait = 5 * (attempt + 1)
            print(f"  retry {attempt + 1} za {wait}s ({e})", flush=True)
            time.sleep(wait)
    return None


def record(isbn: str, target: str, data: dict | None) -> dict:
    if not data:
        return {"isbn": isbn, "target": target, "found": False}
    return {
        "isbn": isbn,
        "target": target,
        "found": True,
        "title": data.get("title"),
        "authors": [
            {"name": a.get("name"), "key": (a.get("url") or "").rstrip("/").rsplit("/authors/", 1)[-1].split("/")[0] or None}
            for a in data.get("authors", [])
        ],
        "subjects": [s.get("name") for s in data.get("subjects", [])][:25],
        "number_of_pages": data.get("number_of_pages"),
        "publish_date": data.get("publish_date"),
    }


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    done = set()
    if OUT.exists():
        with open(OUT, encoding="utf-8") as f:
            for line in f:
                try:
                    done.add(json.loads(line)["isbn"])
                except (json.JSONDecodeError, KeyError):
                    pass

    targets = [(i, t) for i, t in build_target_list() if i not in done]
    print(f"ke stažení: {len(targets):,} ISBN (přeskočeno hotových: {len(done):,})", flush=True)

    found_cnt = 0
    with open(OUT, "a", encoding="utf-8") as out:
        failed_batches = 0
        for n in range(0, len(targets), BATCH):
            chunk = targets[n : n + BATCH]
            result = fetch_batch([i for i, _ in chunk])
            if result is None:
                failed_batches += 1
                if failed_batches >= 5:
                    print("5 batchů po sobě selhalo, končím - rerun naváže.", flush=True)
                    break
                continue
            failed_batches = 0
            for isbn, target in chunk:
                rec = record(isbn, target, result.get(f"ISBN:{isbn}"))
                found_cnt += rec["found"]
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out.flush()
            if (n // BATCH) % 20 == 0:
                print(f"  {n + len(chunk):,}/{len(targets):,} (nalezeno {found_cnt:,})", flush=True)
            time.sleep(SLEEP_S)

    print(f"HOTOVO: {len(targets):,} ISBN, nalezeno {found_cnt:,}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
