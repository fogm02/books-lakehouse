# Deník řešení

Chronologický záznam postupu a rozhodnutí — zadání explicitně říká, že
cesta k řešení je důležitější než výsledek. Poznámka k historii repa:
první dva dny jsem pracoval mimo git (skica v notebooku, experimenty),
repozitář a první commity vznikly až 16. 8. zpětně po logických celcích.

## 14. 8. — zadání, platforma, skeleton

Přečetl jsem zadání a stáhl dataset: Kaggle Book-Crossing, tři CSV
(271 tis. knih, 1,15 mil. hodnocení, 279 tis. uživatelů). První věc,
které jsem si všiml: hodnocení nemají žádné časové razítko. Zadání přitom
nabízí „top knihy za poslední rok / 2 roky" — tohle půjde interpretovat
jedině přes rok vydání a bude to potřeba říct nahlas.

Platforma: Databricks Free Edition. Je zdarma a má všechno, co potřebuju
(Unity Catalog, joby, dashboardy). Zvažoval jsem i Azure trial, ale setup
by spolkl večer a nic navíc bych neukázal — produkční nasazení na Azure
radši popíšu v dokumentaci. Projekt jsem oddělil vlastním CLI profilem,
bundle je na něj připnutý natvrdo.

Večer jsem rozeskicoval první verzi nad deklarativními pipelines (DLT).
Druhý den jsem ji zahodil, viz dál.

## 15. 8. — přestavba, best practices, první běh

DLT verzi jsem nahradil klasickým jobem se třemi notebook tasky
(bronze_ingest → silver_transform → gold_views). Důvod je jednoduchý:
tenhle vzor znám z produkce a dokážu obhájit každý jeho detail —
checkpointy, idempotenci, full overwrite. DLT bych obhajoval z tutoriálu,
a to by bylo poznat. Nechávám ho ve future paths.

Strukturu jsem si ověřil proti dokumentaci: jeden katalog, schéma na
vrstvu je přímo příklad z oficiální stránky o medallion architektuře.
Sedí i Auto Loader jako triggered batch (availableNow + file arrival
trigger) a bundle jako doporučený způsob nasazování. Vědomě se odchyluju
ve dvou věcech. Silver dělám full overwrite, i když docs pro fakta
doporučují inkrement — na 1,1M řádků trvá rebuild pár sekund a nechci
řešit merge edge-casy. A gold nechávám jako obyčejné views bez
materializace, na téhle velikosti je to jedno.

Co jsem zvážil a zavrhl (u každého jsem si chvíli myslel, že to chci):

- Denní stahování z Kaggle jobem. Zdroj je ale mrtvý snapshot z roku
  2004 — stahoval bych pořád totéž.
- replaceWhere v silveru. Vzor, který znám odjinud, jenže tam měla data
  přirozený klíč dávky (období). Ratingy nic takového nemají;
  idempotenci opakovaných běhů řeší checkpoint v bronze.
- Inkrementální silver streamingem. Podle dokumentace správný směr, ale
  karanténa by si vynutila foreachBatch a každá změna logiky invalidaci
  checkpointu. Na této škále se ta složitost nevrátí.
- SCD2. Ve statickém snapshotu se žádný atribut nikdy nezmění, historie
  by zela prázdnotou. Kdyby zdroj žil, dělal bych MERGE
  s valid_from/valid_to.

Odpoledne setup (schémata, volumes, upload CSV) a první běh. Bronze
napočítal přesně publikované počty datasetu, což potěšilo. Zaujalo mě,
že `_rescued_data` zůstal všude prázdný — chvíli jsem to četl jako „data
jsou čistá", pak mi došlo, že poškozené řádky Books.csv jsou pořád
validní CSV, jen mají obsah v špatných sloupcích. Parser je nechytí,
profilování ano.

## 15. 8. — profilování bronze

Sedm dotazů, výsledky stručně:

- 62,28 % hodnocení je nula (716 109 z 1 149 780). Dokumentace datasetu
  říká jasně: nula = implicitní interakce, ne známka. Explicitních
  známek 1–10 zbývá 433 671.
- 118 641 hodnocení (10,3 %) odkazuje na ISBN, které katalog nezná.
- Rok vydání: 4 618× nula, 11× budoucnost, 57 řádků s textem místo roku
  (to jsou ty posunuté sloupce).
- Duplicitní ISBN na surových hodnotách: žádné.
- Autor „Rowling" existuje v sedmi variantách zápisu.
- Věk: první dotaz s castem na int hlásil 100 % nevalidních hodnot.
  Chvíli jsem věřil, že je sloupec celý rozbitý. Ve skutečnosti je věk
  uložený jako „25.0" a rozbitý byl můj dotaz. Dobrá připomínka, že
  profilovací dotaz musí sedět na skutečný formát dat.
- Location: 99,2 % řádků má přesně tři části „city, state, country".

Praktická poznámka: profilování surových CSV jsem nakonec dělal lokálně
v Pythonu, je to rychlejší smyčka než dotazy přes warehouse.

## 15. 8. — pravidla čištění

Rozhodnutí, ke kterým profilování vedlo:

1. Nuly zůstávají, přibude příznak `is_explicit`. Kvalita je filtruje,
   popularita je potřebuje.
2. Sirotčí hodnocení zůstávají taky. Vadné není hodnocení — skutečný
   uživatel ohodnotil skutečnou knihu — vadné je pokrytí katalogu.
   (Za dva dny se ukázalo, že tohle bylo nejdůležitější rozhodnutí
   celého čištění.)
3. Posunuté řádky jdou do karantény s důvodem. Je jich 57, tj. 0,02 %.
4. Věk mimo 5–100 se nuluje, řádek uživatele zůstává.
5. Location parsuju pozičně zprava — země je nejspolehlivější část.
   Ověřeno na datech: 4 578 řádků má prázdnou zemi („portland, ,"),
   dvoudílné hodnoty jsou skoro vždycky „city, n/a".

Implementace jako čisté funkce v `lib/transforms.py`. Testovací případy
jsem bral přímo z profilování — „25.0", „portland, ,", „DK Publishing
Inc" ve sloupci roku.

## 15. 8. — silver v provozu

První plný běh čištění. Ratings 1 149 780, karanténa prázdná (hodnoty
byly čisté už podle profilování, karanténa je pojistka do budoucna).
Books 270 989 — z toho 57 posunutých v karanténě a 314 duplicit
odstraněných po normalizaci ISBN; na surových datech duplicity vidět
nebyly, což je samo o sobě argument, proč dedupovat až po normalizaci.
Users 278 859. Věk vyšel NULL u 112 432 uživatelů (40,3 %), rok vydání
u 4 624 knih. Normalizace autorů stáhla „Rowling" ze 7 variant na 4;
zbylé tři nechávám schválně — „Marjorie Rowling" je skutečná jiná
autorka a agresivnější slučování by začalo škodit.

## 15. 8. — gold a vážený rating

Medián počtu známek na knihu je 1. Jeden. Prostý průměr je tím pádem
nepoužitelný — žebříček by patřil knihám s jedinou desítkou. Vzal jsem
bayesovský vážený průměr (IMDb vzorec) a m = 25: odpovídá to 99.
percentilu rozdělení (p99 = 22) a pro jistotu jsem projel citlivost
m = 10/25/50 — špička žebříčku se nehýbe, přeskupují se pozice 5–10.

Při citlivostní analýze vylezl vedlejší nález: v top 10 byl Azkaban
dvakrát, protože žebříček po ISBN je ve skutečnosti žebříček vydání.
Gold od té doby agreguje přes (title, author) a eviduje počet sloučených
vydání. Kvůli slučování jsem musel rozšířit normalizaci autorů
o iniciály bez tečky, jinak se „J.K Rowling" nesloučila s „J. K. Rowling".

Ve výstupech se poprvé ukázal rozbitý text („Saint-Exupã©Ry").
Poznamenal jsem si to a nechal na později.

## 15. 8. — revize v půlce

Zastavil jsem se a udělal si revizi celého řešení — hloubkové profilování
surových CSV plus průchod kódu proti best practices. Sepsáno v review.md
včetně priorit. Hlavní nálezy: chyběl mi pohled na popularitu (zadání ho
přitom výslovně nabízí a podle počtu interakcí vede Wild Animus, kniha
masově rozdávaná zdarma s podprůměrnými známkami); top 1 % uživatelů
vytvořilo 48,3 % všech hodnocení; rozbité texty jsou dvojitě zakódované
už ve zdrojových bajtech, takže pipeline čte správně; a 17 364 skupin
(title, author) má víc než jedno ISBN, slučování vydání tedy není okrajová
záležitost.

## 16. 8. — druhý zdroj: Open Library

Napsal jsem extraktor nad Open Library Books API. Dávky po 50 ISBN,
autentizace archive.org klíči (~3 dotazy/s), běh jde kdykoli přerušit
a navázat. Stahoval jsem 63 457 ISBN: všechna dohledatelná sirotčí plus
top 2 000 katalogu kvůli žánrům. Stažené JSONL šlo do landing zóny a dál
stejnou cestou jako CSV — dorazilo ve dvou dávkách a Auto Loader si je
sebral bez zásahu, což byl mimochodem první ostrý test inkrementálnosti.

Výsledek předčil očekávání: katalog +36 925 knih, sirotčích hodnocení
ze 118 752 na 44 589, do žebříčků se vrátilo kolem 30 tisíc známek.
Enrichment tabulka navíc nese žánry a stabilní ID autorů (author_key)
pro 38 933 ISBN. Zbytek je nedohledatelný — 8 915 ISBN má rozbitý
formát, ostatní Open Library prostě nezná.

Dvě provozní drobnosti, které sežraly čas: Python na macOS bez CA
certifikátů (spraveno přes certifi) a ANSI mód na serverless, kvůli
kterému indexace prázdného pole hází chybu místo NULL — místo
`authors[0]` je potřeba `get(authors, 0)`.

## 16. 8. — dashboard jako kód, meze roku, žánry

Dashboard jsem nejdřív klikal v UI, hlavně abych nástroj poznal. Finální
verze je ale JSON v repu nasazovaný bundlem — a z té klikané zůstalo
jedno důležité poučení: limity a řazení musí být v SQL datasetu, ne ve
widgetu. Grafy ořezávají na 10 tisíc řádků a filtr položený na už
oříznutý top 10 vrací nesmysly. Interaktivní „top v období" proto řeší
parametry, které se vykonají před LIMIT.

Dvě korekce po ověření na surových datech. Horní mez roku vydání jsem
stáhl z 2026 na 2004 — sběr dat tehdy skončil a nad rokem 2004 je jen
72 knih s 205 hodnoceními, vesměs předprodejní metadata a překlepy.
Dolní mez 1450 obstála, i když jinak, než jsem čekal: pod rokem 1900
jsou v katalogu čtyři knihy a dvě z nich jsou moderní íránské romány
s rokem v perském kalendáři (1376 SH ≈ 1997). Sloupec je zkrátka rok
vydání edice, ne vzniku díla.

Žánry z Open Library se ukázaly být knihovnické hlavičky s čárkami
uvnitř — „Fiction, Fantasy, General" je jeden řetězec. Rozpadl jsem je
na atomické tokeny a vyhodil balast typu „General". I potom je to spíš
folksonomie než taxonomie: mezi „žánry" jsou místa (Middle Earth),
série i španělské a francouzské hlavičky z překladů.

## 16. 8. — konsolidace goldu, metadata, Genie

Gold jsem zredukoval z osmi views na šest. Původní byly pojmenované
podle panelů dashboardu, což se přestalo škálovat v momentě, kdy stejná
data chtěl i Genie agent. Nové pravidlo: jedno view na zrnitost — kniha,
autor, kniha × žánr, autor × rok × vydavatel, rok, celek. Tři knižní
views se sloučily do jednoho `v_books`. Autorské view zůstává zvlášť,
protože vážený průměr na úrovni autora z knižních agregátů spočítat
nejde (potřebuje počty známek per autor).

Do silveru přibyla oprava dvojitého kódování (`fix_mojibake`) — konečně
„Antoine De Saint-Exupéry" místo rozsypaného čaje. Všechna gold views
dostala komentáře tabulek i sloupců v Unity Catalogu.

Nakonec Genie space nad šesti gold views, s instrukcemi o sémantice
(nula = implicitní, weighted_rating pro „nejlepší", readers_total pro
„nejčtenější", doporučený práh 25 známek). Test: na otázku „Which books
are most read but poorly rated?" si agent sám sestavil dotaz s prahem
z instrukcí a odpověděl Wild Animus. Konfigurace space je exportovaná
v repu a dashboard má tlačítko Ask Genie.
