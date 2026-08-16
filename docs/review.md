# Revize řešení v půlce projektu (15. 8. 2026)

Hloubkové profilování dat (lokálně nad surovými CSV) + revize kódu
a designu proti Databricks best practices. Priority: P0 = nutné před
odevzdáním, P1 = rychlé zlepšení, P2 = bonus/future.

> **Stav k 16. 8.:** revize splnila účel — všechna P0 a většina P1 jsou
> vyřešené. „Most popular" pohled vznikl (a při konsolidaci goldu se
> sloučil do `v_books`), dashboard je nasazený jako kód, mojibake (N3)
> opravený v silveru, repo má commit historii, layers.md je dopsaná.
> Otevřené zůstávají: deterministický dedup (K1), `is_valid_isbn` flag
> (N6) a CI — vedené jako future improvements.

---

## I. Nové nálezy z dat

### 🔴 N1: "Most popular" ≠ "best rated" — chybí nám pohled ze zadání (P0)

Zadání nabízí variantu **(a) "10 most popular books"** — popularita =
počet interakcí, ne známka. Podle počtu interakcí (vč. implicitních)
vede úplně jiný žebříček než podle váženého ratingu:

| interakcí | kniha |
|---|---|
| 2 502 | **Wild Animus** (Rich Shapero) |
| 1 295 | The Lovely Bones (Alice Sebold) |
| 883 | The Da Vinci Code (Dan Brown) |

Wild Animus je slavná kuriozita Book-Crossing datasetu — kniha masově
rozdávaná zdarma: nejvíc interakcí, mizerné známky. Perfektní příběh
k vysvětlení rozdílu popularita vs. kvalita v prezentaci.
**Akce: přidat `v_most_popular_books` (počet interakcí, split
explicit/implicit) do goldu.**

### 🟡 N2: Top 1 % uživatelů = 48,3 % všech ratingů (P1 — pojmenovat)

Extrémní koncentrace: user 11676 má 13 602 ratingů, top 5 uživatelů
dohromady ~39k. Pro počty interakcí per kniha to nevadí (každý pár
(user, isbn) je unikátní — viz N4), ale je to bias k pojmenování:
žebříčky odrážejí chování malé skupiny power-users. Future: vážení
per-user, robustnější mediánové agregace.

### 🟡 N3: Mojibake je ZAPEČENÝ VE ZDROJI, ne v naší pipeline (P1 — doložit)

Books.csv je validní UTF-8, ale obsahuje dvojitě zakódované sekvence
přímo v bytech: `Saint-Exup\xc3\x83\xc2\xa9ry` ("ExupÃ©ry"), 1 206×
mojibake "é", 449× "ü". Naše čtení je správně — vada vznikla před
publikací na Kaggle. **Akce: dokumentovat jako source defect; oprava
(ftfy-style fix v silveru) = future improvement.**

### 🟢 N4: Duplicitní hodnocení NEEXISTUJÍ (validace designu)

0 duplicitních (user, isbn) párů v 1,15M ratingů → žádné dvojité
započítání, count interakcí = count distinct uživatelů. Gold agregace
jsou tím pádem korektní bez dodatečného dedupu.

### 🟢 N5: Referenční integrita users je 100% (validace)

Všichni uživatelé z ratings existují v users. Sirotčí problém je jen
na straně books (118 641 ratingů, viz dřívější explorace).

### 🟡 N6: Nevalidní formát ISBN (P1 — levný DQ flag)

117 knih a 11 361 ratingů má ISBN mimo formát 10 znaků [0-9X] (pomlčky,
smetí). Většina sirotčích ratingů s tím souvisí. **Akce: přidat
`is_valid_isbn` flag do silveru — 1 řádek kódu, ukazuje rigor; checksum
validace ISBN-10 = future.**

### 🟢 N7: Slučování vydání je důležitější, než se zdálo (validace)

17 364 skupin (title, author) má víc než jedno ISBN. Rozhodnutí slučovat
vydání v goldu tedy ovlivňuje ~6 % katalogu, ne jen pár top knih.

### ⚪ N8: Drobnosti

- Users.csv: 278 858 unikátních ID vs. 278 859 řádků v bronze (1 řádek
  k prověření), 0 duplicit.
- Books: 1 prázdný autor, 2 prázdní vydavatelé — zanedbatelné.
- Python csv parser přečte všech 271 360 řádků Books bez posunu — Spark
  jich 57 posune: rozdíl v interpretaci escapovaných uvozovek. Dobrá
  ilustrace "CSV není formát, CSV je vyjednávání".

---

## II. Revize kódu

### 🟡 K1: `dropDuplicates(["isbn"])` není deterministický (P1)

Komentář v silveru tvrdí "vyhrává první řádek dle pořadí ingestu" —
dropDuplicates ale žádné pořadí negarantuje. Buď opravit komentář,
nebo (lépe) explicitní okno: row_number() OVER (PARTITION BY isbn
ORDER BY _ingested_at, _source_file) a vzít rn=1.

### 🟡 K2: Title-case komolí jména s částicemi a mojibake (P2)

`normalize_author` s .title(): "Antoine De Saint-Exupã©Ry" (De, ã©Ry).
Kosmetické, dokumentovaný limit; po opravě mojibake (N3) se zlepší.

### 🟢 K3: Zbytek silveru drží vodu

Karanténa s reason ✓, NULLování atributů vs. vyřazení řádku
konzistentní s rozhodnutími ✓, UDF = testovaný kód (41 testů) ✓,
pořadí dedup po normalizaci ✓ (315 dupů by na raw datech nechytil).

### 🟢 K4: Gold formule správně

Dělení v Spark SQL je double (žádná integer division), m parametrizované
přes widget + job parameter, explicit-only filtr ✓, inner join vyřazuje
sirotky dle rozhodnutí ✓.

---

## III. Scorecard vůči best practices (Databricks docs)

| Oblast | Stav |
|---|---|
| Auto Loader, availableNow, file arrival trigger | ✅ přesně dle docs |
| Bronze: stringy, append-only, metadata, rescued | ✅ |
| Schéma per vrstva v jednom katalogu | ✅ (vzor z docs) |
| Silver: typování, dedup, validace, karanténa | ✅ |
| Silver full overwrite | ⚠️ vědomá odchylka, zdokumentováno + škálovací cesta |
| Gold views místo materializace | ⚠️ vědomá odchylka, zdokumentováno |
| Asset Bundles | ✅ |
| Unit testy (pytest) | ✅ 41 testů, lokálně |
| CI (testy na push) | ❌ P2 — GitHub Actions chybí |
| Git historie | ❌ P1 — repo bez jediného commitu! |
| Column comments (Genie/UC discovery) | ⚠️ jen views; silver tabulky bez komentářů (P2) |
| DQ metriky pro dashboard | ⚠️ jen printy v jobu; view `v_dq_summary` (P1) |

---

## IV. Prioritizovaný plán

**P0 — bez toho neodevzdávat**
1. `v_most_popular_books` (N1) — pokrývá variantu (a) ze zadání
2. **AI/BI dashboard** — povinný deliverable, dosud neexistuje
3. Git commity — historie = "journey", teď je repo prázdné

**P1 — levné, vysoký dojem**
4. Deterministický dedup (K1)
5. `is_valid_isbn` flag (N6)
6. `v_dq_summary` view (karanténa + NULL rate) pro DQ panel v dashboardu
7. Doplnit layers.md čísly z profilování; power-user bias (N2) a mojibake
   (N3) do limitations

**P2 — bonus, pokud zbyde čas**
8. Open Library enrichment (author_key, žánry)
9. Mojibake fix v silveru (ftfy)
10. GitHub Actions na pytest
11. Zkouška inkrementálního dema (druhá dávka) před pohovorem
