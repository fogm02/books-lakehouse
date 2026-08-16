# Deník řešení

Chronologický záznam postupu a rozhodnutí. Zadání říká, že cesta k řešení
je důležitější než výsledek — tenhle soubor je ta cesta. Doplňuje
`architecture.md` (souhrn rozhodnutí) a `layers.md` (dokumentace vrstev).

## 14. 8. — zadání, platforma, skeleton

Prostudoval jsem zadání a dataset: Kaggle Book-Crossing, tři CSV (271 tis.
knih, 1,15 mil. hodnocení, 279 tis. uživatelů). Hned první čtení odhalilo
zásadní vlastnost: hodnocení nemají timestamp, takže „top za období" půjde
interpretovat jedině rokem vydání.

Platformu jsem zvolil Databricks Free Edition — je zdarma, serverless
a má Unity Catalog, joby i dashboardy. Azure jsem se rozhodl reálně
nezapojovat: setup by stál večer a nepřidal nic, co neukážu návrhem
produkčního nasazení v dokumentaci. Projekt jsem od začátku oddělil od
pracovního prostředí vlastním CLI profilem, na který je bundle natvrdo
připnutý.

První skeleton jsem postavil nad deklarativními pipelines (DLT), ale
ještě týž den přehodnotil — viz další záznam.

## 15. 8. — přestavba na ověřený vzor, první běh

Rozhodl jsem se přestavět řešení z DLT na klasický job se třemi notebook
tasky (bronze_ingest → silver_transform → gold_views), podle vzoru
produkčního projektu, který znám z praxe. Důvod: chci obhajovat
architekturu, se kterou mám zkušenost — checkpointy, idempotenci,
trade-off full overwrite — a ne technologii nastudovanou přes noc.
Deklarativní pipelines zůstávají jako budoucí cesta.

Strukturu jsem ověřil proti oficiální dokumentaci medallion architektury:
jeden katalog, schéma na vrstvu (`landing/bronze/silver/gold`) je přímo
vzor z docs. Potvrdily se i další volby: Auto Loader jako triggered batch
s `availableNow` a file arrival triggerem, bronze jako append-only stringy
s metadata sloupci, Asset Bundle jako doporučené CI/CD. Vědomě se
odchyluji ve dvou bodech: silver dělám full overwrite (docs doporučují
inkrement pro fakta; na 1,1M řádků je rebuild otázka sekund) a gold jako
obyčejné views (materializace je optimalizace, kterou tahle velikost dat
nepotřebuje).

Alternativy, které jsem zvážil a zavrhl:

- **Plánované stahování z Kaggle** — zdroj je statický snapshot; denní
  download by simuloval čerstvost, která neexistuje.
- **replaceWhere v silveru** — nemá partition klíč, na který by sáhl;
  idempotenci opakovaných běhů řeší checkpoint v bronze.
- **Inkrementální silver (streaming)** — správný směr podle dokumentace,
  ale za cenu složitosti (foreachBatch kvůli karanténě, invalidace
  checkpointu při změně logiky), která se na této velikosti nevrátí.
  Jde do future improvements s konkrétním postupem.
- **SCD2** — atributy dimenzí se ve statickém snapshotu nemění, historie
  by byla prázdná; surovou historii navíc drží append-only bronze.
  U živého zdroje bych sáhl po MERGE s valid_from/valid_to.

Odpoledne proběhl setup (schémata, volumes, upload CSV) a první běh jobu.
Bronze sedí přesně na publikované počty datasetu. Zajímavý detail:
`_rescued_data` je všude prázdný — což neznamená čistá data. Poškozené
řádky Books.csv jsou strukturně validní CSV a schovávají se uvnitř
sloupců; najde je až profilování.

## 15. 8. — profilování bronze

Sedm profilovacích dotazů nad bronze vrstvou, výsledky:

- **62,28 % hodnocení je nula** (716 109 z 1 149 780). Podle dokumentace
  datasetu nula není známka, ale implicitní interakce — uživatel knihu
  zalogoval bez hodnocení. Explicitních známek 1–10 je 433 671.
- **118 641 hodnocení (10,3 %) odkazuje na ISBN, které v katalogu není.**
- Rok vydání: 4 618× hodnota 0, 11× rok v budoucnosti, 57 řádků
  s nečíselným rokem — to jsou ty posunuté sloupce; příčinou je
  nekonzistentní escapování uvozovek v titulech.
- Duplicitní ISBN: na surových hodnotách žádné.
- Autor „Rowling" existuje v sedmi variantách zápisu.
- Věk je uložený jako desetinný string („25.0") — první profilovací dotaz
  s přímým castem na int hlásil 100 % nevalidních hodnot. Falešný poplach
  a dobrá lekce: profilovací dotaz musí sedět na skutečný formát dat.
- Location má u 99,2 % řádků přesně tři části „city, state, country";
  zbytek jsou prázdné části, chybějící stát nebo čárky v názvech.

Lekce z procesu: profilování surových CSV je rychlejší lokálně než přes
warehouse; na Spark má smysl až to, co potřebuje Deltu.

## 15. 8. — pravidla čištění

Pět rozhodnutí, každé s důvodem:

1. **Implicitní nuly zůstávají** s příznakem `is_explicit` — metriky
   kvality je odfiltrují, metriky popularity je potřebují.
2. **Sirotčí hodnocení zůstávají v silveru** — vadné není hodnocení, ale
   pokrytí katalogu. (Tohle rozhodnutí se později ukázalo jako klíčové —
   umožnilo díru v katalogu opravit druhým zdrojem.)
3. **Posunuté řádky do karantény** s důvodem — 0,02 % dat, oprava
   escapování ve zdroji nemá smysl (míchá dva styly).
4. **Věk mimo 5–100 → NULL atributu**, řádek uživatele zůstává.
5. **Location parsovat pozičně zprava** — země je nejspolehlivější část.
   Pravidlo jsem ověřil na datech: 4 578 řádků má prázdnou zemi
   („portland, ,"), dvoudílné hodnoty jsou většinou „city, n/a".

Implementace: čisté funkce v `lib/transforms.py`, testy s reálnými
hodnotami z profilování („25.0", „portland, ,", „DK Publishing Inc"
ve sloupci roku).

## 15. 8. — silver v provozu

První plný běh čištění: ratings 1 149 780 (karanténa prázdná — hodnoty
jsou čisté), books 270 989 (57 posunutých v karanténě, 314 duplicit
odstraněných po normalizaci ISBN — na surových datech nebyly vidět),
users 278 859. Věk je NULL u 112 432 uživatelů (40,3 %), rok vydání
u 4 624 knih. Normalizace autorů sjednotila „Rowling" ze 7 variant na 4 —
zbylé tři jsou dokumentované limity (mj. „Marjorie Rowling" je skutečná
jiná autorka, proto se neslučuje agresivně).

## 15. 8. — gold a vážený rating

Medián hodnocení na knihu je 1 — prostý průměr by žebříček rozbil.
Použil jsem bayesovský vážený průměr (vzorec IMDb Top 250) s m = 25:
hodnota odpovídá 99. percentilu rozdělení (p99 = 22) a citlivostní
analýza m = 10/25/50 ukázala stabilní špičku žebříčku, mění se jen
pozice 5–10.

Citlivostní analýza přinesla i vedlejší nález: žebříček po ISBN je ve
skutečnosti žebříček *vydání* — Azkaban byl v top 10 dvakrát. Gold proto
agreguje přes (title, author) a eviduje počet sloučených vydání. Kvůli
slučování jsem rozšířil normalizaci autorů o iniciály bez tečky
(„J.K Rowling" → „J. K. Rowling").

Ve výstupu se poprvé zviditelnil rozbitý text („Saint-Exupã©Ry") —
poznamenáno k prošetření.

## 15. 8. — revize celého řešení

Hloubkové profilování surových dat + revize kódu proti best practices
(plný report: `review.md`). Hlavní nálezy:

- Chyběl pohled „most popular" — popularita (počet interakcí) je jiná
  metrika než kvalita (známky) a zadání ji explicitně nabízí. Podle počtu
  interakcí vede Wild Animus (2 502 čtenářů) — kniha, která se masově
  rozdávala zdarma a má podprůměrné známky.
- **Top 1 % uživatelů vytvořilo 48,3 % všech hodnocení** — dataset
  popisuje chování malé skupiny power-users.
- Rozbitý text je **zapečený přímo ve zdrojových bytech** (dvojité
  kódování před publikací na Kaggle) — doloženo pohledem do surového
  souboru; čtení v pipeline je korektní.
- 0 duplicitních párů (uživatel, kniha) — agregace nepočítají nic dvakrát.
- 17 364 skupin (title, author) má víc než jedno ISBN — slučování vydání
  ovlivňuje podstatnou část katalogu.

## 16. 8. — druhý zdroj: Open Library

Napsal jsem extraktor nad Open Library Books API (batch dotazy po 50
ISBN, autentizace archive.org klíči ~3 req/s, resumable běh) a stáhl
metadata pro 63 457 ISBN: všechna dohledatelná sirotčí + top 2 000
katalogu kvůli žánrům. Data přitekla jako JSONL do landing zóny — stejnou
cestou jako CSV, ve dvou dávkách, které si Auto Loader sebral sám.

Výsledek: katalog narostl o 36 925 dohledaných knih, sirotčích hodnocení
ubylo z 118 752 na 44 589 (62 %) a do žebříčků se vrátilo ~30 tisíc
explicitních známek. Enrichment tabulka nese žánry a stabilní ID autorů
(`author_key`) pro 38 933 ISBN; top 2 000 katalogu mělo 100% úspěšnost.
Zbytek je nedohledatelný: 8 915 ISBN má nevalidní formát, ostatní Open
Library nezná.

Provozní poznámky: macOS Python bez CA certifikátů (řešení: certifi);
ANSI mód na serverless vyžaduje `get(authors, 0)` místo indexace pole.

## 16. 8. — dashboard jako kód, mez roku, žánry

Dashboard jsem postavil jako JSON definici nasazovanou bundlem. Klíčové
poučení z první (klikané) verze: limity a řazení patří do SQL datasetů,
ne do widgetů — grafy ořezávají na 10 tisíc řádků a filtr aplikovaný po
limitu vrací nesmysly. „Top 10 v období" proto řeší parametry
(`:year_range`, `:genre`), které se vykonají před limitem.

Dvě korekce podložené surovými daty:

- **Horní mez roku vydání 2026 → 2004** (konec crawlu). Nad rokem 2004 je
  jen 72 knih s 205 hodnoceními — vesměs předprodejní metadata a překlepy.
- **Dolní mez 1450 obstála**: pod rokem 1900 jsou v katalogu 4 knihy,
  z toho dvě jsou moderní íránské romány s rokem v perském kalendáři
  (1376 SH ≈ 1997). Sloupec je rok vydání *edice*, ne vzniku díla.

Žánry z Open Library se ukázaly být knihovnické hlavičky s čárkami uvnitř
(„Fiction, Fantasy, General" je jeden řetězec) — rozpadl jsem je na
atomické tokeny a vyhodil balast typu „General". I tak zůstávají
folksonomií: míchají žánry, místa („Middle Earth"), série i jazyky.

## 16. 8. — konsolidace goldu, metadata, Genie

Gold jsem zredukoval z osmi views na šest podle pravidla **jedno view na
zrnitost**: tři knižní views se sloučily do `v_books` (popularita
i kvalita v jednom řádku), `v_top_authors` se přejmenoval na `v_authors`.
Autorský grain z knižního odvodit nejde — vážený průměr potřebuje počty
známek na úrovni autora.

V silveru přibyla oprava dvojitě zakódovaného textu (`fix_mojibake`) —
„Antoine De Saint-Exupéry" je konečně čitelný. Všechna gold views dostala
komentáře tabulek i sloupců v Unity Catalogu.

Nad gold vrstvou vznikl Genie space (6 views + instrukce o sémantice:
nula = implicitní, weighted_rating pro „nejlepší", readers_total pro
„nejčtenější", práh 25 známek). Test otázkou „Which books are most read
but poorly rated?" — agent sám použil práh z instrukcí, správně rozlišil
popularitu od kvality a odpověděl Wild Animus. Konfigurace space je
exportovaná v repu, dashboard má tlačítko Ask Genie.
