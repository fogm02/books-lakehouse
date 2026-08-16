# Layers

Tato stránka popisuje, co se děje v každé vrstvě pipeline (Bronze, Silver,
Gold), klíčová designová rozhodnutí a jak spolu vrstvy komunikují.

<!-- Vyplňuj průběžně - tenhle dokument je půlka prezentace. U každé vrstvy
     drž strukturu: Účel / Vstup / Transformace / Klíčová rozhodnutí /
     Idempotence / Výstup / Edge cases. Konkrétní ČÍSLA (kolik řádků spadlo
     do karantény a proč) mají větší váhu než popis technologie. -->

---

## Bronze

### Účel

TODO: raw 1:1 reprezentace tří CSV z Kaggle, historie dodávek, rebuild point.

### Vstup

TODO: landing volume, glob patterny, jak funguje inkrementální dodávka.

### Transformace

TODO: Auto Loader (proč), _source_file/_ingested_at, _rescued_data.

### Idempotence

TODO: checkpointy - co se stane po smazání, co po reuploadu souboru.

### Edge cases

TODO

---

## Silver

### Účel

TODO

### Transformace

TODO: per tabulka - typování, čištění, dedup, karanténa. ČÍSLA!

### Klíčová rozhodnutí

TODO: full overwrite vs. incremental (trade-off), karanténa vs. drop,
implicitní ratingy (proč je držíme).

### Edge cases

TODO

---

## Gold

### Účel

TODO

### Transformace

TODO: views vs. tabulky (proč), vážený rating (vzorec + volba m),
interpretace "období" bez timestampů.

### Edge cases

TODO

---

## Limity a future improvements

TODO: chybějící timestampy ratingů, sirotčí ISBN, Open Library enrichment,
deklarativní pipeline (DLT) jako alternativa, CI/CD, monitoring.
