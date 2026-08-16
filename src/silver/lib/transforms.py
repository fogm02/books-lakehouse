"""Čisté transformační funkce - žádný Spark, žádný Databricks.

Testovatelné pytestem lokálně i v CI (tests/test_transforms.py).
Pravidla vycházejí z explorace bronze vrstvy, viz docs/journal.md
(15. 8. 2026 - rozhodnutí čistících pravidel).
"""

from __future__ import annotations

import re

# Knihtisk ~1450; horní mez = konec crawlu (září 2004) - dataset po tomto
# datu neexistuje, takže pozdější rok vydání je z definice vadný záznam.
# Ověřeno v raw: jen 72 knih s rokem > 2004 (205 ratingů, 0,018 %).
YEAR_RANGE = (1450, 2004)

# Mimo tento rozsah je věk považován za nevyplněný (v datech max 244).
# NULLuje se atribut, řádek uživatele zůstává - je dál použitelný pro joiny.
AGE_RANGE = (5, 100)

# Hodnoty, které v Location znamenají "nevyplněno".
_PLACEHOLDERS = {"", "n/a", "-", "unknown"}

# Escape smetí z nekonzistentního CSV quotingu (`\n/a"`, `"portland, ,`).
_GARBAGE_CHARS = re.compile(r'[\\")(]')


def clean_year(raw: str | int | None) -> int | None:
    """Rok vydání: 0, nečíselné hodnoty (posunuté sloupce) a roky mimo
    YEAR_RANGE -> None."""
    if raw is None:
        return None
    try:
        year = int(str(raw).strip())
    except ValueError:
        return None
    lo, hi = YEAR_RANGE
    return year if lo <= year <= hi else None


def clean_age(raw: str | int | float | None) -> int | None:
    """Věk mimo AGE_RANGE nebo nečíselný -> None.

    Pozor: zdroj ukládá věk jako desetinný string ("25.0"), proto se
    parsuje přes float - přímý cast na int by selhal na všech řádcích.
    """
    if raw is None:
        return None
    try:
        age = int(float(str(raw).strip()))
    except (ValueError, OverflowError):
        return None
    lo, hi = AGE_RANGE
    return age if lo <= age <= hi else None


def _clean_part(part: str) -> str | None:
    """Jedna část Location: pryč escape smetí, trim, placeholdery -> None."""
    cleaned = _GARBAGE_CHARS.sub("", part).strip().lower()
    return None if cleaned in _PLACEHOLDERS else cleaned


def parse_location(raw: str | None) -> tuple[str | None, str | None, str | None]:
    """Free-text "city, state, country" -> (city, state, country).

    Formát je poziční, parsuje se zprava (země je nejspolehlivější část):
    - 3 části:  city, state, country ("nyc, new york, usa")
    - >3 části: country = poslední, state = předposlední, city = zbytek
                slepený ("herne bay, kent, england, united kingdom")
    - 2 části:  city, country - state chybí ("tel-aviv, n/a")
    - 1 část:   jen city (9 řádků v datech)
    Prázdné části a placeholdery -> None ("portland, ," -> state i country
    None). Zbytkový šum zůstává ("paris, alabama, gambia, the") - gold
    pracuje na granularitě země, dokonalá geokanonizace je mimo scope.
    """
    if raw is None:
        return (None, None, None)
    parts = [_clean_part(p) for p in raw.split(",")]
    if len(parts) == 1:
        return (parts[0], None, None)
    if len(parts) == 2:
        return (parts[0], None, parts[1])
    city_parts = [p for p in parts[:-2] if p is not None]
    city = ", ".join(city_parts) if city_parts else None
    return (city, parts[-2], parts[-1])


def fix_mojibake(raw: str | None) -> str | None:
    """Opraví text dvojitě zakódovaný ve zdroji ("Saint-ExupÃ©ry" -> "Saint-Exupéry").

    Books.csv obsahuje UTF-8 bajty omylem překódované přes latin-1 už od
    vydavatele datasetu (doloženo v raw bytech, viz journal). Oprava je
    inverzní krok: encode latin-1 -> decode utf-8, opakovaně dokud text
    "ozdravuje". Bezpečnost: korektní text (i s diakritikou) projde beze
    změny - encode/decode na něm selže nebo vrátí identitu.
    """
    if raw is None:
        return None
    text = raw
    for _ in range(2):  # zdroj má místy dvojité kódování
        try:
            repaired = text.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            break
        if repaired == text:
            break
        text = repaired
    return text


def normalize_author(raw: str | None) -> str | None:
    """Sjednocení pravopisných variant jména autora.

    Řeší: mezery kolem teček ("J.K." / "J .K." -> "J. K."), iniciály bez
    tečky ("J.K Rowling" -> "J. K. Rowling"), násobné mezery, velikost
    písmen ("ROWLING" -> "Rowling").
    Neřeší (vědomý limit, bez externího zdroje nejde): přehozené pořadí
    ("Rowling J K"), zkratka vs. plné jméno ("Joanne K." vs. "J. K."),
    částice ("van der" -> title-case je zkomolí). Kanonizace přes externí
    ID (Open Library author_key) = future path.
    """
    if raw is None:
        return None
    name = re.sub(r"\s*\.\s*", ". ", fix_mojibake(raw))
    # iniciála bez tečky uprostřed jména: "J. K Rowling" -> "J. K. Rowling"
    name = re.sub(r"\b([A-Za-z])\b(?![.'])", r"\1.", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name.title() or None
