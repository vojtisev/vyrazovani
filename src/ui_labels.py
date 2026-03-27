"""České popisky sloupců a klasifikací pro zobrazení v dashboardu (interní názvy zůstávají v datech)."""

from __future__ import annotations

from typing import Dict

import pandas as pd

KLASIFIKACE_LABELS: Dict[str, str] = {
    "AUTO_KANDIDAT": "Automatický kandidát (nízký výskyt)",
    "RUCNI_POSOUZENI": "Ruční posouzení",
    "CHRANENO_VYJIMKOU": "Chráněno výjimkou",
    "PONECHAT": "Ponechat",
}

# Sloupce v tabulkách → uživatelsky čitelné názvy (bez změny datového modelu).
DISPLAY_COLUMN_LABELS: Dict[str, str] = {
    "title_key": "Interní klíč titulu",
    "TITUL_NAZEV": "Název titulu",
    "SIGN_PREFIX": "Prefix signatury",
    "TITUL_SIGN_FULL": "Signatura",
    "TITUL_KEY": "ID titulu",
    "TITUL_DRUH_DOKUMENTU": "Druh dokumentu",
    "TITUL_JAZYK": "Jazyk",
    "TITUL_ROK_VYDANI": "Rok vydání",
    "okno_let": "Délka sledovaného období (roky)",
    "vypujcky_okno": "Počet výpůjček v období (pro výkon)",
    "vypujcky_5_let": "Počet výpůjček v období (pro výkon)",
    "pocet_operaci_v_obdobi": "Počet všech operací ve zvoleném období",
    "pocet_vypujcek_is_loan": "Počet výpůjček (IS_LOAN)",
    "pocet_vraceni_is_return": "Počet vrácení (IS_RETURN)",
    "pocet_svazku": "Počet svazků na titul",
    "vykon_na_svazek": "Výkon na svazek (výpůjčky / svazek)",
    "datum_posledni_vypujcky": "Datum poslední výpůjčky",
    "roky_od_posledni_vypujcky": "Roky od poslední výpůjčky",
    "percentil_v_signature": "Percentil v rámci prefixu signatury",
    "relativni_pozice_v_signature": "Pořadí / počet titulů ve stejném prefixu",
    "rizikove_skore": "Rizikové skóre",
    "bez_vypujcek": "Bez výpůjček v období",
    "bez_svazku": "Bez svazku v datech",
    "duvod_oznaceni": "Důvod označení",
    "desk_subjects": "Deskriptory (obsah)",
    "och_subjects": "Obsahová charakteristika (OCH)",
    "vyjimka_flag": "Spadá pod výjimku (pravidla)",
    "typ": "Typ případu",
    "opid": "OPID",
    "popis": "Popis operace (OPIDY)",
}

SUMMARY_SIGNATURE_LABELS: Dict[str, str] = {
    "SIGN_PREFIX": "Prefix signatury",
    "AUTO_KANDIDAT": KLASIFIKACE_LABELS["AUTO_KANDIDAT"],
    "RUCNI_POSOUZENI": KLASIFIKACE_LABELS["RUCNI_POSOUZENI"],
    "CHRANENO_VYJIMKOU": KLASIFIKACE_LABELS["CHRANENO_VYJIMKOU"],
    "PONECHAT": KLASIFIKACE_LABELS["PONECHAT"],
    "CELKEM_TITULU": "Celkem titulů",
}

PREFIX_SUMMARY_LABELS: Dict[str, str] = {
    "SIGN_PREFIX": "Prefix signatury",
    "TITULU": "Počet titulů",
}


def label_opid_column(name: str) -> str:
    if name.startswith("pocet_opid_"):
        oid = name.replace("pocet_opid_", "")
        return f"Počet operací (OPID {oid})"
    return name


def dataframe_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Kopie DataFrame s přejmenovanými sloupci a čitelnými hodnotami klasifikace."""
    out = df.copy()
    if "klasifikace" in out.columns:
        out["Klasifikace"] = out["klasifikace"].map(lambda x: KLASIFIKACE_LABELS.get(str(x), str(x)))
        out = out.drop(columns=["klasifikace"])
    rename: Dict[str, str] = {}
    for c in out.columns:
        if c in DISPLAY_COLUMN_LABELS:
            rename[c] = DISPLAY_COLUMN_LABELS[c]
        elif c.startswith("pocet_opid_"):
            rename[c] = label_opid_column(c)
    return out.rename(columns=rename)


def klasifikace_display_name(internal: str) -> str:
    return KLASIFIKACE_LABELS.get(internal, internal)


def summary_signature_for_display(df: pd.DataFrame) -> pd.DataFrame:
    m = {k: v for k, v in SUMMARY_SIGNATURE_LABELS.items() if k in df.columns}
    return df.rename(columns=m)


def prefix_summary_for_display(df: pd.DataFrame) -> pd.DataFrame:
    m = {k: v for k, v in PREFIX_SUMMARY_LABELS.items() if k in df.columns}
    return df.rename(columns=m)


def opid_legend_for_display(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"opid": DISPLAY_COLUMN_LABELS["opid"], "popis": DISPLAY_COLUMN_LABELS["popis"]})
