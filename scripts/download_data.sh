#!/usr/bin/env bash
# Stáhne Book-Crossing dataset z Kaggle do data/raw/.
#
# Varianta A (kaggle CLI):
#   pip install kaggle
#   + API token z https://www.kaggle.com/settings -> ~/.kaggle/kaggle.json
#
# Varianta B (ručně): stáhni zip z
#   https://www.kaggle.com/datasets/arashnic/book-recommendation-dataset
#   a rozbal do data/raw/
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p data/raw
kaggle datasets download -d arashnic/book-recommendation-dataset -p data/raw --unzip
ls -lh data/raw
