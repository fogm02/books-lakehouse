# Layers

Co se děje v každé vrstvě pipeline, klíčová designová rozhodnutí a jak
spolu vrstvy komunikují. Čísla platí pro plný běh s Open Library
enrichmentem (16. 8. 2026).

---

## Bronze

### Účel

Raw 1:1 reprezentace zdrojů. Drží historii dodávek a slouží jako single
source of truth — silver lze kdykoli přepočítat z bronze bez návratu ke
zdroji. Žádná business pravidla, jen technická metadata.

### Vstup

Landing volume `/Volumes/books/landing/raw`:

- `Books*.csv`, `Ratings*.csv`, `Users*.csv` — Kaggle Book-Crossing
  (glob patterny → další dávka = další soubor, žádná změna kódu)
- `open_library/*.jsonl` — druhý zdroj (vlastní extraktor nad Open Library
  Books API, `scripts/fetch_open_library.py`; dorazil ve 2 dávkách)

### Transformace

Auto Loader (`cloudFiles`) s `Trigger.availableNow` — inkrementální
zpracování nových souborů, checkpoint per zdroj, spouštěné jobem
(file arrival trigger připraven, defaultně PAUSED). CSV se čte **bez
inference typů** (vše string — typování je práce silveru, string chrání
proti změnám schématu); JSONL s inferencí (strojově generovaný, stabilní
schéma, vnořené struktury). Každý řádek dostane `_source_file`,
`_ingested_at`; nevalidní řádky by zachytil `_rescued_data`.

### Idempotence

Checkpoint per zdroj: smazání checkpointu + tabulky = plné přehrání;
stejný soubor se podruhé nezpracuje.

### Výstup

`books` 271 360 | `ratings` 1 149 780 | `users` 278 859 |
`open_library` 63 457 (append-only, rostou s dávkami)

### Edge cases

- `_rescued_data` = 0 všude — NEznamená čistá data: posunuté řádky
  Books.csv jsou strukturně validní CSV (najde je až profilování).
- Python csv parser a Spark čtou tentýž soubor různě (jiný počet
  posunutých řádků) — escapování uvozovek v Books.csv je nekonzistentní
  a každý parser si s ním poradí po svém.

---

## Silver

### Účel

Typovaná, vyčištěná, validovaná data. Full overwrite z bronze při každém
běhu. Pravidlo pro špínu: **vadný celý řádek → karanténa s reason; vadný
atribut → NULL, řádek zůstává.** Všechna pravidla = čisté funkce
v `lib/transforms.py`, 48 pytest testů.

### Tabulky a transformace

**`ratings` (1 149 780)** — cast typů, ISBN UPPER+TRIM, flag
`is_explicit = rating > 0` (62,3 % ratingů je 0 = implicitní feedback dle
dokumentace datasetu — nezahazujeme, popularita ho potřebuje).
Karanténa `quarantine_ratings`: 0 řádků (pojistka pro budoucí dávky).

**`books` (307 914 = 270 989 Kaggle + 36 925 Open Library)** — union dvou
větví se sloupcem `source`:

- *Kaggle*: posunuté řádky (nečíselný rok = rozbité escapování uvozovek)
  → `quarantine_books` (57). Oprava mojibake (zdroj je dvojitě zakódovaný
  — doloženo v raw bytech), normalizace autora (tečky, iniciály, case;
  „Rowling“ 7 variant → 4), rok vydání mimo 1450–2004 → NULL (dolní mez
  = knihtisk, horní = konec crawlu; obě ověřené v datech — pod rokem 1900
  jsou v katalogu 4 knihy, nad 2004 jen 72 chybných záznamů),
  dedup po normalizaci ISBN (314 duplicit — na raw datech 0).
- *Open Library*: dohledané sirotčí knihy (`target='orphan'`, found,
  s titulem), stejná normalizace autora, rok z `publish_date`;
  anti-join na Kaggle ISBN — **při konfliktu vyhrává Kaggle**.

**`book_enrichment` (38 933)** — žánry (subjects), `author_key` (stabilní
OL ID autora — podklad pro budoucí identity resolution), počet stran.
Odděleně od `books`: je to volitelné obohacení, ne katalog.

**`users` (278 859)** — věk přes double→int (zdroj má „25.0“; mimo 5–100
→ NULL, celkem 112 432 = 40,3 %), Location parsovaná pozičně zprava
(placeholdery a escape smetí → NULL; 4 578 řádků bez země).

### Klíčová rozhodnutí

- **Full overwrite**: na 1,1M řádků sekundy; rebuild po změně logiky
  zadarmo; docs full rebuild malých dimenzí posvěcují. Škálovací cesta:
  ratings streaming z bronze + checkpoint, dimenze MERGE.
- **Sirotčí ratingy zůstávají v silveru** (vadné není hodnocení, ale
  pokrytí katalogu) — vyřadí je až join v goldu. Enrichment jich 62 %
  zachránil: 118 752 → 44 589 (z toho 8 915 má nevalidní ISBN formát =
  nedohledatelné navždy).
- **SCD2 vědomě ne** — statický snapshot, atributy se nemění; u živého
  zdroje MERGE s valid_from/valid_to či AUTO CDC.

### Edge cases

- Referenční integrita users: 100 % (sirotci jen na straně books).

---

## Gold

### Účel

Prezentační vrstva — čisté DDL views (vždy live ze silveru, žádná
duplikace dat). Logika patří sem; dashboard datasety jsou jen tenké řezy
(ORDER/LIMIT/parametry) — limit v goldu by rozbil filtrování a ostatní
konzumenty.

### Views — jedno na zrnitost (grain)

Původní návrh měl osm views pojmenovaných podle panelů dashboardu; při
revizi jsem je zredukoval na šest podle zrnitosti. Prezentační řezy
(řazení, limity, parametry) dělají až konzumenti.

| view | zrnitost | poznámka |
|---|---|---|
| `v_books` | kniha (title × author) | vydání sloučena (17 364 vícevydáňových skupin); `readers_total` = popularita vč. implicitních, `weighted_rating` = kvalita |
| `v_authors` | autor | nejde odvodit z knižního grainu — vážený průměr potřebuje počty na úrovni autora |
| `v_books_by_genre` | kniha × žánr | žánry z enrichmentu (jen obohacená podmnožina); subjects rozpadlé na atomické tokeny |
| `v_authors_by_year_publisher` | autor × rok × vydavatel | pro filtrovatelnou analytiku |
| `v_avg_rating_by_year` | rok vydání | trend známek |
| `v_kpi_summary` | celý dataset | katalog, interakce, % implicitních, globální průměr C, efekt enrichmentu |

Všechna views mají komentáře tabulek i sloupců v Unity Catalogu — čte je
Catalog Explorer i Genie.

### Vážený rating

Bayesovský průměr (IMDb vzorec): `(v/(v+m))·R + (m/(v+m))·C`, kde
C = 7,6 (globální průměr explicitních známek) a **m = 25** — ukotveno
v datech: medián ratingů na knihu je 1, p99 = 22; citlivostní analýza
m=10/25/50 ukázala stabilní špičku, posuny jen na pozicích 5–10
(nika ↔ mainstream). Bez vážení by žebříček ovládly knihy s jedním
ratingem 10 (185 841 hodnocených knih, medián 1 rating).

### Limity a interpretace

- **Ratingy nemají timestamp** → „top za období“ interpretujeme rokem
  vydání (parametry `:od_roku`/`:do_roku`), ne časem hodnocení.
- **Top 1 % uživatelů = 48,3 % ratingů** — data popisují chování malé
  skupiny power-users.
- Slučování přes title: varianty titulu vydání se nesloučí dokonale
  („(Book 1)“ vs „(Paperback)“).

---

