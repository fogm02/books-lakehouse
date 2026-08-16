# Journal — postup řešení

Chronologický deník práce a rozhodnutí. Zdroj pro finální prezentaci
(zadání: "the journey there and potential future paths are crucial").
Doplňuje `architecture.md` (decision log) a `layers.md` (dokumentace vrstev).

## 14. 8. 2026 — analýza zadání a setup

- Prostudováno zadání + dataset: Kaggle Book-Crossing (books 271k, ratings
  1,1M, users 279k). Všimnout si: ratingy nemají timestamp → "top za období"
  jde jen přes rok vydání (limit dat, pojmenovat v prezentaci).
- **Platforma: Databricks Free Edition** — zdarma, serverless, UC + jobs +
  dashboardy. Azure reálně nezapojovat (setup čas bez přidané hodnoty),
  místo toho slide "produkční nasazení na Azure".
- Oddělení od pracovního účtu: CLI profil `personal`, bundle na něj natvrdo
  připnutý.
- První skeleton: DLT (Lakeflow Declarative Pipelines) + Asset Bundle.

## 15. 8. 2026 — přestavba na ověřený pattern, první běh

- **Rozhodnutí: přestavět z DLT na job + 3 notebook tasky** (bronze_ingest →
  silver_transform → gold_views) po vzoru produkčního projektu, který znám.
  Důvod: u pohovoru obhajuju věci s praktickou zkušeností (checkpointy,
  idempotence, full overwrite trade-off). DLT zůstává jako future path.
- Struktura: jeden katalog `books`, schémata `landing/bronze/silver/gold`
  (schéma per vrstva = vzor přímo z oficiální medallion dokumentace).
- **Research best practices** (Databricks docs) — potvrzeno: Auto Loader jako
  triggered batch s availableNow + file arrival trigger; bronze jako stringy,
  append-only, metadata sloupce; DAB jako doporučené CI/CD. Vědomé odchylky:
  silver full overwrite (docs doporučují inkrement pro fakta; na 1,1M řádků
  je rebuild v sekundách), gold jako views (docs: materializace pro výkon).
- **Zavržené alternativy (talking points!):**
  - Scheduled Kaggle download job — zdroj je statický snapshot, simuloval
    by freshness, která neexistuje. Inkrementálnost se ukazuje dávkami
    v landingu + file arrival triggerem.
  - replaceWhere v silveru — nemá partition klíč, na který by sáhl (ratingy
    nemají periodu); idempotenci řeší checkpoint v bronze.
  - Hybrid (ratings streaming inkrement) — správný směr dle docs, ale
    riziko zamotání (foreachBatch kvůli karanténě, invalidace checkpointu);
    jde do future improvements s konkrétním postupem.
  - SCD2 — atributy dimenzí se ve statickém snapshotu nemění, historie by
    byla prázdná; raw historii drží append-only bronze. U živého zdroje:
    MERGE s valid_from/valid_to, případně AUTO CDC.
- Setup proveden: init_schemas (schémata + volumes), upload 3 CSV do
  `/Volumes/books/landing/raw`, `bundle deploy`, první běh jobu ✅.
- **Bronze ověřen:** books 271 360, ratings 1 149 780, users 278 859 —
  přesně dle Kaggle. `_rescued_data` = 0 všude: NEznamená čistá data —
  posunuté řádky jsou strukturně validní CSV, schovávají se uvnitř sloupců
  a najde je až profilování.
- Vytvořen explorační notebook (7 profilovacích otázek nad bronze).

## 15. 8. 2026 večer — výsledky explorace bronze (ČÍSLA do prezentace)

- **Q1 ratings**: 62,28 % ratingů je 0 = implicitní feedback (716 109
  z 1 149 780). Explicitních 1–10 je 433 671. Hodnoty jen 0–10, nic mimo.
- **Q2 sirotčí ratingy**: 118 641 (~10,3 %) odkazuje na ISBN, které v books
  není. Rozhodnout: nechat ve faktech bez joinu / oddělit / dropnout.
- **Q3 rok vydání**: 4 618× rok 0, 11× budoucnost (max 2050), 57 řádků
  s nečíselným rokem = posunuté sloupce. Příčina posunů: tituly s vnořenými
  uvozovkami escapovanými `\"` (dataset míchá escape styly) — CSV parser
  s default escape nastavením sloupce rozhodí. ~0,02 % dat.
- **Q4 duplicitní ISBN**: ŽÁDNÉ (na raw hodnotách). Ověřit ještě po
  UPPER/TRIM normalizaci (case varianty '...x' vs '...X').
- **Q5 autoři**: "Rowling" má 7 variant zápisu (J. K. / J.K. / Joanne K. /
  J .K. / ROWLING / Rowling J K...). Pro gold žebříčky autorů je normalizace
  nutná; dokonalá kanonizace bez externího zdroje nejde — pojmenovat limit.
- **Q6 age**: hodnoty jsou desetinné stringy ("25.0") → TRY_CAST na INT
  selhává na všem. Lekce: profilovací dotaz musí sedět na formát dat
  (falešných 100 % špíny). Přepočítat přes DOUBLE→INT, pak teprve rozsahy.
- **Q7 location**: 276 694 řádků (99,2 %) má přesně 3 části; 657× 2 části,
  9× 1 část, ~1 500× více než 3 (čárky uvnitř názvů). Pravidlo: happy path
  3 části; rozhodnout fallbacky.

## 15. 8. 2026 — rozhodnutí čistících pravidel (po diskusi)

1. Implicitní nuly: nechat s flagem `is_explicit` (signál pro recommender).
2. Sirotčí ratingy: nechat v silveru, vyřadí je až inner join v goldu —
   vadné není hodnocení, ale pokrytí katalogu. 118k ISBN = kandidáti na
   Open Library enrichment.
3. Posunuté řádky books: karanténa (0,02 %); oprava CSV escape jako future
   improvement (dataset míchá `\"` i `"""` styly, jedno nastavení nestačí).
4. Age: mimo rozsah 5–100 → NULL atributu (řádek zůstává). Pozor: hodnoty
   jsou "25.0" → cast přes DOUBLE→INT.
5. Location: parsování zprava OVĚŘENO na datech — 4 578 řádků s prázdnou
   poslední částí ("portland, ,"), dvoudílné jsou hlavně "city, n/a"
   s escape smetím (`\n/a"` — stejný nepořádek jako v Books.csv), vícedílné
   fungují ("...england, united kingdom"). Pravidlo: split → trim → očistit
   `\`/`"`/`)` → placeholdery ('', n/a, -) NULL → country=poslední,
   state=předposlední, city=zbytek. Perla do prezentace: "paris, alabama,
   gambia, the" → country "the".
- Lekce: exploraci raw CSV dělat lokálně (soubory jsou v data/raw),
  warehouse až na Spark/Delta věci.

## 15. 8. 2026 — implementace čistících funkcí

- `lib/transforms.py`: clean_year, clean_age, parse_location,
  normalize_author — přesně dle 5 rozhodnutí výše. 39 pytest testů zeleně;
  testovací případy = reálné hodnoty z explorace ("25.0", "portland, ,",
  `tel-aviv, \n/a"`, "DK Publishing Inc" v roce vydání...).
- Vědomé limity normalize_author zdokumentované v docstringu (přehozené
  pořadí jména, zkratka vs. plné jméno, částice van/der).

## 15. 8. 2026 — silver v provozu (ČÍSLA)

- Běh pipeline ✅ (bronze → silver → gold za ~2 min na serverless).
- ratings 1 149 780 (karanténa 0 — hodnoty čisté dle explorace),
  books 270 989 (57 posunutých v karanténě s reason, 314 duplicit po
  UPPER/TRIM — na raw datech 0! → dedup se dělá až po normalizaci),
  users 278 859 (řádky zachovány, čistily se atributy).
- Věk NULL: 112 432 (40,3 %) — skutečné číslo po opravě castu přes double.
- Rok vydání NULL: 4 624. Rowling: 7 variant → 4 (3 zbylé = dokumentované
  limity; "Marjorie Rowling" je reálná jiná autorka — proto se neslučuje
  agresivně).

## 15. 8. 2026 — gold vrstva (vážený rating, m ukotveno v datech)

- Volba m=25 podložena: medián ratingů/knihu = 1, p99 = 22 → m≈p99;
  citlivostní analýza m=10/25/50: špička stabilní, pozice 5-10 se posouvají
  nika↔mainstream. Vzorec = bayesovský průměr (IMDb Top 250).
- Nález z citlivostní analýzy: žebříček po ISBN = žebříček VYDÁNÍ (Azkaban
  2× v top 10) → gold agreguje přes (title, author), sloupec editions.
- normalize_author rozšířen: iniciály bez tečky ("J.K Rowling" →
  "J. K. Rowling") — jinak by se vydání nesloučila. 41 testů zeleně.
- v_top_books, v_top_authors, v_authors_by_year_publisher nasazeny; top 10
  věrohodné (Tolkien, Rowling, Harper Lee, Malý princ), Azkaban sloučen
  (3 vydání, 277 ratingů), Two Towers 10 vydání.
- Nové nálezy z výstupu: (1) encoding mojibake "Saint-Exupã©Ry" — zdroj je
  latin-1, čteme UTF-8 → rozhodnout: encoding option v bronze + reingest,
  nebo dokumentovaný limit; (2) HP Stone 2× kvůli variantě titulu
  ("(Book 1)" vs "(Paperback)") — očekávaný, dokumentovaný limit slučování.

## 15. 8. 2026 — komplexní revize (docs/review.md)

- Hloubkové profilování lokálně nad raw CSV + revize kódu a scorecard
  vůči best practices. Klíčové: chybí pohled "most popular" (zadání a) —
  Wild Animus story; top 1 % uživatelů = 48,3 % ratingů; mojibake je
  zapečený ve zdroji (doloženo byty) — pipeline je nevinná; 0 duplicitních
  (user,isbn) párů = gold agregace korektní; 17 364 skupin vydání.
- Plný report s P0/P1/P2 plánem: docs/review.md.

## 16. 8. 2026 — Open Library enrichment v provozu (ČÍSLA)

- Staženo 63 457 ISBN (61 457 sirotčích + top 2 000 katalogu) přes batch
  Books API; s archive.org auth ~3 req/s. Resumable skript
  (scripts/fetch_open_library.py), zdroj = dva JSONL soubory v landingu
  (inkrementální dodávka druhého zdroje - Auto Loader je sebral sám).
- **Katalog narostl o 36 925 dohledaných knih** (source='open_library';
  Kaggle 270 989). Enrichment tabulka: 38 933 ISBN se žánry/author_key
  (36 933 orphan + 2 000 catalog - top katalog měl 100% hit rate).
- **Sirotčí ratingy: 118 752 → 44 589** (zachráněno 62 %; explicitních
  známek se do žebříčků vrátilo ~30 tisíc, zbývá 19 718).
- Zbytek nedohledatelný: 8 915 nevalidních ISBN + ISBN neznámá Open Library.
- Debug poznámky: macOS Python bez CA certifikátů (fix: certifi);
  ANSI mód na serverless -> get(authors, 0) místo authors[0].
- Dashboard staví MF ručně v UI (učení) - 4 gold views + kpi dataset.

## Do prezentace nezapomenout

- Čísla ze špíny dat (doplní se z explorace do layers.md).
- Příběh "_rescued_data=0 ≠ čistá data".
- Vědomá jednoduchost + jasná škálovací cesta > složitost napůl pochopená.
- Demo naživo: druhá dávka ratingů → file arrival trigger → dashboard.
