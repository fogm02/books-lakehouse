# Architektura

```
Kaggle (3× CSV) ──┐                                  ┌─ ratings  (is_explicit, karanténa)
                  ├─→ landing volume ─→ bronze ────→ ├─ books    (Kaggle ∪ Open Library)
Open Library ─────┘    (soubory)        Auto Loader  ├─ book_enrichment (žánry, author_key)
(vlastní extraktor)                     append-only  └─ users    (věk, lokace)
                                                            │ full overwrite
                                                            ▼
                                          gold: 6 views podle zrnitosti
                                                            │
                                        AI/BI dashboard · Genie space
```

Job se třemi tasky (bronze → silver → gold) na serverless, spouštěný ručně
nebo file arrival triggerem. Všechno včetně dashboardu je definované
v tomhle repu a nasazuje se přes `databricks bundle deploy`.

## Nejdůležitější rozhodnutí

**Hodnocení s nulou zůstávají.** Je jich 62 % a podle dokumentace datasetu
nejde o známky, ale o záznamy „uživatel měl knihu v ruce". Žebříčky kvality
je filtrují příznakem, popularita na nich stojí.

**Sirotčí hodnocení taky zůstávají.** 10 % hodnocení mířilo na knihy mimo
katalog. Vadné nebylo hodnocení, ale katalog — a přesně tahle množina se
pak stala zadáním pro dohledání z Open Library (62 % se podařilo zachránit).

**S vadnými daty zacházím dvěma způsoby:** celý poškozený řádek jde do
karanténní tabulky i s důvodem, vadná hodnota v jinak dobrém řádku se
nuluje. Nic se nemaže potichu.

**Silver se přepočítává celý.** Na 1,1M řádků pár sekund; změna pravidla
znamená prostě nový běh a bronze zůstává netknutá. Inkrementální zpracování
by tu bylo jen složitost navíc.

**Žebříčky váží bayesovský průměr** (vzorec z IMDb) s m = 25 — medián počtu
známek na knihu je totiž 1 a prostý průměr by vyhrávaly knihy s jedinou
desítkou. Hodnotu m jsem odvodil z 99. percentilu rozdělení a ověřil
citlivostní analýzou.

**Gold má jedno view na zrnitost.** Řazení, limity a parametry si dělají
konzumenti — díky tomu stejných šest views krmí dashboard, ad-hoc SQL
i Genie agenta.


## Limity dat

Hodnocení nemají čas — „období" znamená rok vydání. Sběr dat skončil v září
2004. Top 1 % uživatelů vytvořilo 48 % hodnocení. Slučování vydání přes
titul je heuristika a žánry z Open Library jsou spíš folksonomie než
taxonomie. Podrobněji v `layers.md`.

## Kdyby to mělo běžet v produkci (Azure)

Stejný kód, jiné prostředí: ADLS Gen2, workspace pro dev/test/prod
nasazované týmž bundlem s různými targety, Entra ID + Unity Catalog pro
přístupy, Key Vault na secrets. CI/CD přes PR → testy → dev → approval →
prod, k tomu monitoring a alerting na selhání jobu a kvalitu dat.
