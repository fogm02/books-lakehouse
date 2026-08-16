#!/usr/bin/env bash
# Nahraje CSV z data/raw do landing volume ve Free Edition workspace.
# Předpoklad: katalog a volume existují (viz README, krok 3).
set -euo pipefail
cd "$(dirname "$0")/.."

PROFILE=personal
VOLUME=dbfs:/Volumes/books/landing/raw

for f in data/raw/*.csv; do
  echo "-> $f"
  databricks fs cp "$f" "$VOLUME/$(basename "$f")" --profile "$PROFILE" --overwrite
done
