---
marp: true
paginate: true
theme: default
style: |
  section {
    font-family: 'Inter', 'Helvetica Neue', sans-serif;
    font-size: 27px;
    padding: 56px 64px;
    color: #1a2332;
    background: #fcfcfa;
  }
  h1 { font-size: 1.35em; color: #0f1722; line-height: 1.25; }
  h2 { font-size: 1.05em; color: #3a4a5e; font-weight: 600; }
  strong { color: #0b5394; }
  table { font-size: 0.82em; }
  code, pre { font-size: 0.78em; background: #f0f1f3; }
  blockquote { border-left: 4px solid #0b5394; padding-left: 0.8em; color: #3a4a5e; font-style: normal; }
  footer { color: #9aa5b1; font-size: 0.55em; }
---

<!-- _paginate: false -->

# Books Lakehouse

## Bronze–silver–gold pipeline nad Book-Crossing datasetem

Matěj Fogy · srpen 2026

<br>

1,15 mil. hodnocení · 307 914 knih · dva zdroje · Databricks Free Edition

<!--
Úvod, 30 vteřin: co dostali za zadání, co uvidí.
Pak rovnou k věci — první slide je teze celého řešení.
-->

---

# Stavěl jsem pipeline, ne skript

Skript zpracuje dataset jednou. Pipeline počítá s tím, že data přitečou znovu.

- ingest přes **Auto Loader** — nová dávka = nový soubor v landing zóně, žádná změna kódu
- celý projekt definovaný v **gitu**, nasazovaný **Asset Bundlem** — workspace jde kdykoli postavit znovu
- transformační pravidla jako čisté funkce, **48 testů** mimo Databricks

> Vyplatilo se dřív, než jsem čekal: druhý zdroj dorazil ve dvou dávkách
> a pipeline ho zpracovala bez zásahu.

---

# Architektura: medailon se dvěma zdroji

```
Kaggle (CSV) ──┐                      silver                gold (6 views)
               ├─→ landing ─→ bronze ─→ books ∪ enrichment ─→ v_books, v_authors,
Open Library ──┘    volume    (raw,     ratings + karanténa    žánry, trendy, KPI
(můj extraktor)               append)   users                       │
                                                            dashboard + Genie
```

- **bronze** drží fakta tak, jak přišla — append-only, všechno string
- **silver** dělá rozhodnutí — typy, pravidla, karanténa, spojení zdrojů
- **gold** dělá interpretace — slučování vydání, vážené žebříčky

---

# Nejdřív jsem se díval do dat. Vyplatilo se

| nález | rozsah |
|---|---|
| hodnocení „0“ — není známka, ale záznam bez známky | **62,3 %** všech řádků |
| hodnocení knih, které v katalogu neexistují | **118 752** (10,3 %) |
| rok vydání 0 nebo v budoucnosti | 4 629 knih |
| věk uložený jako text `"25.0"` — přímý cast selže na všem | 100 % sloupce |
| řádky rozbité escapováním uvozovek | 57 |
| texty dvojitě zakódované už u vydavatele datasetu | tisíce („ExupÃ©ry“) |

Každý řádek téhle tabulky se stal pravidlem v silver vrstvě — s testem.

---

# Pravidlo pro špínu: mazat co nejméně

**Vadný celý řádek → karanténa s důvodem.** Vadný atribut → NULL, řádek žije dál.

- posunuté řádky: 57 v `quarantine_books`, i s `reason` — nic nezmizelo beze stopy
- věk mimo 5–100 → NULL (řádek uživatele je dál použitelný pro joiny)
- implicitní nuly **zůstávají s příznakem** `is_explicit` — kvalita je filtruje, popularita je potřebuje
- sirotčí hodnocení zůstávají v silveru: vadné není hodnocení, ale pokrytí katalogu

> Díky tomu poslednímu šla díra v katalogu později opravit — další slide.

---

# Druhý zdroj neopravil vzhled, ale díru v datech

10 % hodnocení mířilo na knihy, které katalog neznal. Dohledal jsem je přes Open Library API (vlastní extraktor: batch dotazy, autentizace, resumable).

| | před | po |
|---|---|---|
| knih v katalogu | 270 989 | **307 914** |
| sirotčích hodnocení | 118 752 | **44 589** |
| explicitních známek mimo žebříčky | 49 868 | 19 718 |

Nový zdroj přitekl **stejnou cestou jako ten první** — JSONL do landing zóny,
Auto Loader, silver. Architektura se nezměnila, jen dostala další vstup.

---

# Malé vzorky lžou. Žebříčky vážím bayesovsky

Medián hodnocení na knihu je **1**. Prostý průměr by vynesl nahoru knihy s jedinou desítkou.

```
weighted = (v/(v+m))·R + (m/(v+m))·C          (vzorec IMDb Top 250)

R = průměr knihy   C = 7,6 (globální průměr)   v = počet známek
m = 25  →  99. percentil rozdělení (p99 = 22)
```

Citlivost ověřená pro m = 10/25/50: špička žebříčku stabilní,
mění se jen pozice 5–10 (výklenkové tituly ↔ mainstream).

---

# Popularita není kvalita — a data to umí ukázat

Nejčtenější kniha datasetu vs. vítěz žebříčku kvality:

| | čtenářů | známek | vážený rating |
|---|---|---|---|
| **Wild Animus** (Shapero) | 2 502 | 581 | **4,52** |
| **The Two Towers** (Tolkien) | 260 | 136 | **9,07** |

Wild Animus se počátkem tisíciletí masově rozdával zdarma — rekord ve čtenosti, propadák ve známkách. Jeden řádek dat vypráví celý příběh implicitního feedbacku.

*(tady v demu ukazuji dashboard — KPI, oba žebříčky, parametry období a žánru)*

---

# Gold: jedno view na zrnitost, ne na graf

Začal jsem s osmi views „podle panelů dashboardu“ a při revizi je zredukoval:

| view | zrnitost |
|---|---|
| `v_books` | kniha (vydání sloučena) |
| `v_authors` | autor |
| `v_books_by_genre` | kniha × žánr (z enrichmentu) |
| `v_authors_by_year_publisher` | autor × rok × vydavatel |
| `v_avg_rating_by_year` | rok vydání |
| `v_kpi_summary` | celý dataset |

Logika žije v goldu; dashboard i Genie si berou jen tenké řezy (řazení, limit, parametry). Limit v goldu by rozbil filtrování — top 10 po filtru ≠ filtrovaný top 10.

---

# Co data neumí — a proč to vím

- **hodnocení nemají čas** → „top za období“ interpretuji rokem vydání, ne datem hodnocení
- crawl skončil **v září 2004** → tam končí osy, trendy i horní mez validace roku
- trend známek podle roku nese **survivorship bias** — ze starých ročníků přežily jen klasiky
- **1 % uživatelů vytvořilo 48 % hodnocení** — data popisují chování malé skupiny
- slučování vydání přes název je heuristika — „(Book 1)“ vs. „(Paperback)“ se nesloučí

> Nejmilejší nález: dvě „středověké“ knihy z let 1376–1378 jsou moderní íránské
> romány — rok vydání prosákl v perském kalendáři (1376 SH ≈ 1997).

---

# Kudy dál, kdyby projekt žil

**Škálování** — silver inkrementálně (streaming z bronze + MERGE dimenzí); plný katalog z Open Library dumpů místo API; materializovaná views pro výkon dashboardu.

**Datový model** — `dim_authors` se skutečnou identitou přes `author_key`
(už ho sbírám v enrichmentu); SCD2 až bude zdroj živý — na snapshotu by evidoval historii, která se neděje.

**Provoz** — CI na testy, lakehouse monitoring, alerting na selhání jobu; produkčně Azure: ADLS Gen2, workspace na prostředí, nasazování stejným bundlem přes DevOps.

---

<!-- _paginate: false -->

# Tři věty na závěr

**Bronze drží fakta, silver dělá rozhodnutí, gold dělá interpretace** — a každé rozhodnutí umím doložit číslem, testem nebo záznamem v deníku.

**Druhý zdroj přitekl stejnou cestou jako první** — to je test architektury, ne dekorace.

**Všechno je v gitu a nasazuje se jedním příkazem** — workspace je jen projekce repa.

<br>

repo: `github.com/fogm02/books-lakehouse` · celý postup: `docs/journal.md`
