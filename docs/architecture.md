# Architektura a rozhodnutí

```
Kaggle (3× CSV) ──┐                                  ┌─ ratings  (is_explicit, karanténa)
                  ├─→ landing volume ─→ bronze ────→ ├─ books    (Kaggle ∪ Open Library)
Open Library ─────┘    (soubory)        Auto Loader  ├─ book_enrichment (žánry, author_key)
(vlastní extraktor)                     append-only  └─ users    (věk, lokace)
                                        vše string          │ full overwrite
                                                            ▼
                                          gold: 6 views podle zrnitosti
                                          (vážený rating, KPI, trendy, žánry)
                                                            │
                                        AI/BI dashboard (parametry) · Genie space
```

Job se třemi tasky (bronze_ingest → silver_transform → gold_views) na
serverless compute, spouštěný ručně nebo file arrival triggerem nad
landing zónou. Celý projekt — pipeline, dashboard i konfigurace — je
definovaný v tomto repu a nasazuje se příkazem `databricks bundle deploy`.

## Decision log

Každé rozhodnutí s důvodem a zváženou alternativou. Podrobnosti a čísla
v `journal.md`.

| rozhodnutí | proč | zvážená alternativa |
|---|---|---|
| Databricks Free Edition | zdarma, serverless, UC + joby + dashboardy; produkční Azure návrh je v kapitole níže | Azure trial (čas na setup bez přidané hodnoty pro úkol) |
| job + notebook tasky | vzor ověřený z produkce, plná kontrola | deklarativní pipelines (DLT) — expectations zadarmo, jde do future paths |
| Auto Loader s availableNow | idempotence opakovaných běhů, nová dávka = nový soubor; docs jej pro file ingest doporučují | prostý `spark.read` — jednodušší, ale opakovaný běh duplikuje append-only bronze |
| bronze = stringy, append-only | věrná kopie zdroje; typování může selhat a selhávat má v silveru, kde je bronze k dispozici na přehrání | inference typů při čtení |
| rating 0 = implicitní interakce, flag | definice od autora datasetu; kvalita nuly filtruje, popularita je potřebuje | drop nul — ztráta 62 % dat a metriky popularity |
| vadný řádek → karanténa, vadný atribut → NULL | nic nemizí beze stopy; řádek s jednou vadnou hodnotou je dál použitelný | tiché dropování |
| sirotčí hodnocení zůstávají v silveru | vadné není hodnocení, ale pokrytí katalogu — a to šlo opravit druhým zdrojem | karanténa/drop — zahodila by 10 % dat včetně 50 tis. známek |
| silver = full overwrite | na 1,1M řádků sekundy, triviálně správné, rebuild po změně logiky zadarmo | inkrement (streaming z bronze + MERGE) — správné pro škálu, tady se nevrátí |
| SCD2 ne | statický snapshot — historie by byla prázdná; surovou historii drží bronze | u živého zdroje MERGE s valid_from/valid_to |
| rok vydání validní 1450–2004 | sloupec je rok vydání edice: před knihtiskem není co vydat, po konci crawlu (09/2004) není co hodnotit; obě meze ověřené v datech | obecné meze (např. do současnosti) — pustily by 72 prokazatelně vadných záznamů |
| přirozené klíče, žádná umělá ID | ISBN a user_id jsou stabilní identity; surrogate klíč by nepřidal informaci | plná normalizace — patří do multi-source/SCD scénáře |
| tabulka autorů neexistuje | autor je zatím textový atribut; ID bez identity resolution by předstíralo jistotu | dim_authors přes Open Library author_key (data už sbírám — future path) |
| slučování vydání na díla až v goldu | je to heuristika (title + author) — interpretace patří do gold, ne do base vrstvy | book_id v silveru — chyby heuristiky by byly nevratné pro všechny konzumenty |
| gold = jedno view na zrnitost | 6 views obslouží dashboard, ad-hoc SQL i Genie; prezentační řezy dělají konzumenti | view per graf (původní stav) — duplicity; limity v goldu by rozbily filtrování |
| vážený rating, m = 25 | medián 1 známka/knihu; m ≈ p99 rozdělení, citlivost ověřena pro 10/25/50 | prostý průměr + minimální práh |
| dashboard jako kód (bundle) | verzované, reprodukovatelné; parametry se aplikují před LIMIT | klikaná verze — drift od repa, limity ve widgetech nefungují (ořez na 10 tis. řádků) |
| metadata v Unity Catalogu | komentáře tabulek a sloupců jako jediný zdroj pravdy — čte je katalog i Genie | kopie popisů v Genie space konfiguraci — dvě místa k údržbě |

## Známé limity

- Hodnocení nemají timestamp → „období" znamená rok vydání, ne čas hodnocení.
- Crawl skončil 09/2004 → osy a validace končí tímto rokem.
- Top 1 % uživatelů = 48,3 % hodnocení; trend podle let nese survivorship bias.
- Slučování vydání přes titul nesloučí varianty názvů („(Book 1)" vs. „(Paperback)").
- Žánry existují jen pro obohacenou podmnožinu a jsou folksonomie (místa, série, jazyky).
- 44 589 hodnocení zůstává sirotčích (8 915 má nevalidní ISBN — nedohledatelné z principu).

## Produkční nasazení na Azure

Stejný kód, jiné prostředí: ADLS Gen2 pro landing i tabulky, workspace na
prostředí (dev/test/prod) nasazované týmž bundlem s různými targety,
Entra ID + Unity Catalog pro přístupy, Key Vault na secrets. CI/CD:
PR → testy → deploy do dev → approval → prod. Provoz: lakehouse
monitoring, alerting na selhání jobu a kvalitu dat (karanténní metriky).
