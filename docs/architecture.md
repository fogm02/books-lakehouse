# Architektura

TODO: diagram (excalidraw/draw.io) - landing volume -> Auto Loader -> bronze
-> silver (expectations + karanténa) -> gold (star schema + agregáty)
-> AI/BI dashboard / Genie. Druhá větev: Open Library enrichment.

## Rozhodnutí (decision log)

| Rozhodnutí | Proč | Alternativa |
|---|---|---|
| Databricks Free Edition | zdarma, serverless, UC + jobs + dashboardy | Azure trial (setup čas, $) |
| Job + notebook tasky (imperativní) | pattern ověřený z produkce, plná kontrola | Lakeflow Declarative Pipelines (expectations zadarmo, ale bez praxe) |
| rating 0 = implicitní, oddělený flagem | není to známka; zahodit = ztráta signálu pro recommender | drop nul |
| vážený rating (IMDb vzorec) | AVG žebříček ovládne 1 rating s 10 | prostý AVG + min. práh |
| silver = full overwrite | na 1,1M řádků sekundy; triviálně správné; rebuild po změně logiky zadarmo; docs full rebuild malých dimenzí posvěcují | inkrementální: ratings streaming z bronze + checkpoint, dimenze MERGE (future improvement pro větší škálu — docs ho pro fakta doporučují) |
| gold = obyčejné views | vždy live ze silveru, čisté DDL, na této velikosti dat instantní | materialized views pro pre-agregace (future path pro výkon dashboardu) |
| SCD2 vědomě NE | dataset je statický snapshot — atributy dimenzí se mezi dodávkami nemění, historie by byla prázdná; raw historii drží append-only bronze | u živého zdroje: books/users přes MERGE s valid_from/valid_to, případně AUTO CDC v Lakeflow pipelines |
| TODO ... | | |

## Známé limity dat

- ratingy nemají timestamp -> "top za poslední rok" jde jen přes rok vydání
- ISBN v Ratings, která nejsou v Books (~X % - doplň číslo!)
- TODO: čísla z karantény po prvním běhu

## Produkční nasazení na Azure (slide do prezentace)

Free Edition -> produkce: ADLS Gen2 (landing i tabulky), workspace per
prostředí (dev/test/prod) nasazované stejným bundlem s různými targety,
Entra ID + UC pro přístupy, Key Vault na secrets, Azure DevOps/GitHub
Actions: PR -> testy -> bundle deploy do dev -> approval -> prod.
Monitoring: lakehouse monitoring + alerting na selhání jobu a DQ metriky.
