"""Testy čistých funkcí ze src/silver/lib/transforms.py.

Testovací případy jsou reálné hodnoty nalezené při exploraci bronze
vrstvy (viz docs/journal.md). Spuštění z rootu repa: pytest
"""

import pytest

from src.silver.lib.transforms import (
    clean_age,
    clean_year,
    fix_mojibake,
    normalize_author,
    parse_location,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Saint-ExupÃ©ry", "Saint-Exupéry"),        # reálný případ z Books.csv
        ("FÃ¼r Elise", "Für Elise"),
        ("plain ascii", "plain ascii"),              # beze změny
        ("café", "café"),                            # korektní diakritika beze změny
        (None, None),
    ],
)
def test_fix_mojibake(raw, expected):
    assert fix_mojibake(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2002", 2002),
        (1998, 1998),
        (" 1987 ", 1987),
        ("0", None),                  # 4 618 řádků v Books.csv
        ("2050", None),               # budoucnost (11 řádků)
        ("2026", 2026),               # horní mez včetně
        ("1449", None),               # před knihtiskem
        ("DK Publishing Inc", None),  # posunutý sloupec - autor v roce
        (None, None),
        ("", None),
    ],
)
def test_clean_year(raw, expected):
    assert clean_year(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("25.0", 25),      # reálný formát v Users.csv (desetinný string!)
        ("34", 34),
        (61.0, 61),
        ("5.0", 5),        # dolní mez včetně
        ("100.0", 100),    # horní mez včetně
        ("4.0", None),     # pod rozsahem
        ("101.0", None),
        ("244.0", None),   # reálné maximum v datech
        ("0.0", None),
        ("abc", None),
        ("", None),
        (None, None),
    ],
)
def test_clean_age(raw, expected):
    assert clean_age(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # happy path - 99,2 % řádků
        ("nyc, new york, usa", ("nyc", "new york", "usa")),
        # prázdné části -> None (4 578 řádků typu "portland, ,")
        ("portland, ,", ("portland", None, None)),
        # 2 části s escape smetím (reálný řádek: tel-aviv, \n/a")
        ('tel-aviv, \\n/a"', ("tel-aviv", None, None)),
        # >3 části - parsování zprava, city slepené
        (
            "herne bay, kent, england, united kingdom",
            ("herne bay, kent", "england", "united kingdom"),
        ),
        # placeholdery
        ("n/a, n/a, n/a", (None, None, None)),
        ("cologne, nrw, germany", ("cologne", "nrw", "germany")),
        ("somewhere", ("somewhere", None, None)),
        ("", (None, None, None)),
        (None, (None, None, None)),
    ],
)
def test_parse_location(raw, expected):
    assert parse_location(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # 4 varianty z bronze, které se mají sjednotit
        ("J. K. Rowling", "J. K. Rowling"),
        ("J.K. Rowling", "J. K. Rowling"),
        ("J .K. Rowling", "J. K. Rowling"),
        ("J.K. ROWLING", "J. K. Rowling"),
        ("J.K Rowling", "J. K. Rowling"),   # iniciála bez tečky
        ("O'Brien", "O'Brien"),             # apostrof nesmí dostat tečku
        ("Antoine De Saint-ExupÃ©ry", "Antoine De Saint-Exupéry"),  # mojibake fix
        # vědomé limity - tyhle varianty zůstávají oddělené
        ("Rowling J K", "Rowling J. K."),   # přehozené pořadí neřešíme
        ("Joanne K. Rowling", "Joanne K. Rowling"),
        (None, None),
        ("", None),
    ],
)
def test_normalize_author(raw, expected):
    assert normalize_author(raw) == expected
