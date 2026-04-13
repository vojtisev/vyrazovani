"""Streamlit app - analyza vyrazovani z Parquet dat."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import plotly.express as px
import pandas as pd
import streamlit as st

from src.config import DEFAULT_ACTION_TYPES, DEFAULT_EXCEPTION_KEYWORDS, DEFAULT_PARQUET_PATH
from src.paths import (
    find_default_parquet_in_project,
    project_root,
    resolve_parquet_path_for_load,
)
from src.data_processing import (
    aggregate_by_signature,
    classify_titles,
    clean_data,
    compute_title_metrics,
    compute_title_metrics_from_parquet,
    get_distinct_action_types_from_parquet,
    get_opid_legend_from_parquet,
    get_sign_prefix_summary_from_parquet,
    suggest_loan_action_types_from_parquet,
    filter_to_relevant_window,
    identify_exceptions,
    load_and_validate_data,
    is_precomputed_titles_metrics_parquet,
    resolve_parquet_source_to_local_file,
)
from src.export_utils import export_filtered_to_csv_bytes, export_filtered_to_excel_bytes
from src.ui_labels import (
    dataframe_for_display,
    klasifikace_display_name,
    opid_legend_for_display,
    prefix_summary_for_display,
    summary_signature_for_display,
)


st.set_page_config(page_title="Vyřazování – analýza fondu", page_icon="📚", layout="wide")


def _initial_parquet_path_display() -> str:
    """Vychozi hodnota pole: Secrets (Cloud / vlastni), jinak nalezeny soubor v projektu nebo ocekavana cesta."""
    try:
        p = st.secrets.get("PARQUET_PATH_DEFAULT")
        if p and str(p).strip():
            return str(p).strip()
    except Exception:
        pass
    found = find_default_parquet_in_project()
    if found is not None:
        return str(found.resolve())
    return str(project_root() / DEFAULT_PARQUET_PATH)


def _using_secrets_parquet_default() -> bool:
    try:
        p = st.secrets.get("PARQUET_PATH_DEFAULT")
        return bool(p and str(p).strip())
    except Exception:
        return False


def _s3_storage_options_from_secrets() -> Optional[Dict[str, Any]]:
    """Pro nacitani s3://... z AWS klicu ve Streamlit Secrets."""
    try:
        key = st.secrets.get("AWS_ACCESS_KEY_ID")
        secret = st.secrets.get("AWS_SECRET_ACCESS_KEY")
        if not key or not secret:
            return None
        opts: Dict[str, Any] = {"key": str(key), "secret": str(secret)}
        region = st.secrets.get("AWS_DEFAULT_REGION")
        if region:
            opts["client_kwargs"] = {"region_name": str(region)}
        return opts
    except Exception:
        return None


def _path_exists_for_ui(path: str) -> bool:
    p = path.strip()
    if p.lower().startswith(("http://", "https://", "s3://")):
        return True
    return Path(resolve_parquet_path_for_load(p)).exists()


def parse_exception_rules(raw_text: str) -> Dict[str, List[str]]:
    """Parse JSON pravidel vyjimek."""
    try:
        parsed = json.loads(raw_text)
        if isinstance(parsed, dict):
            return {str(k): [str(x) for x in v] for k, v in parsed.items() if isinstance(v, list)}
    except json.JSONDecodeError:
        pass
    return DEFAULT_EXCEPTION_KEYWORDS


def _fmt_int(n: Any) -> str:
    try:
        return f"{int(n):,}".replace(",", " ")
    except Exception:
        return str(n)


def _fmt_float(x: Any, digits: int = 2) -> str:
    try:
        return f"{float(x):.{digits}f}".replace(".", ",")
    except Exception:
        return str(x)


def _subject_terms(series: pd.Series) -> List[str]:
    """Rozseká agregovaný text subjektů na unikátní termíny pro našeptávač."""
    terms: set[str] = set()
    for raw in series.fillna("").astype(str):
        if not raw.strip():
            continue
        for part in re.split(r"\s*[|;,]\s*", raw):
            t = part.strip()
            if len(t) >= 2:
                terms.add(t)
    return sorted(terms, key=lambda x: x.lower())


def _opid_display_label(col_name: str, opid_name_map: Dict[int, str]) -> str:
    """UI label pro sloupce počtů operací podle OPID."""
    if col_name == "pocet_operaci_v_obdobi":
        return "Počet operací v období (celkem)"
    if col_name == "pocet_vypujcek_is_loan":
        return "Počet výpůjček (IS_LOAN)"
    if col_name == "pocet_vraceni_is_return":
        return "Počet vrácení (IS_RETURN)"
    if col_name.startswith("pocet_opid_"):
        raw = col_name.replace("pocet_opid_", "")
        try:
            oid = int(raw)
            fallback = {
                4: "Abs. půjčování (načtení)",
                5: "Abs. vrácení",
                225: "Abs. půjčování (načtení)",
                226: "Abs. vrácení",
                94: "S. prez. půjčování (načtení)",
                95: "Prez. vrácení",
                97: "Prez. půjčování (načtení)",
                98: "Prez. vrácení",
            }
            popis = opid_name_map.get(oid, "").strip() or fallback.get(oid, "")
            if popis:
                return f"{popis} (OPID {oid})"
            return f"OPID {oid}"
        except Exception:
            return col_name
    return col_name


def _build_opid_name_map(df: pd.DataFrame) -> Dict[int, str]:
    """Mapování OPID -> popis, preferenčně z OPIDY_OPID/OPIDY_POPIS."""
    out: Dict[int, str] = {}
    if df is None or df.empty:
        return out

    oid_col = None
    txt_col = None
    cols_upper = {str(c).upper(): c for c in df.columns}
    if "OPIDY_OPID" in cols_upper:
        oid_col = cols_upper["OPIDY_OPID"]
    if "OPIDY_POPIS" in cols_upper:
        txt_col = cols_upper["OPIDY_POPIS"]

    if oid_col is None or txt_col is None:
        for c in df.columns:
            lc = str(c).lower()
            if oid_col is None and ("opid" in lc or lc == "id"):
                oid_col = c
            if txt_col is None and ("popis" in lc or "text" in lc or "name" in lc):
                txt_col = c
    if oid_col is None or txt_col is None:
        return out

    for _, row in df.iterrows():
        try:
            oid = int(row[oid_col])
            raw_txt = row[txt_col]
            if pd.isna(raw_txt):
                continue
            txt = str(raw_txt).strip()
            if not txt or txt.lower() in {"nan", "none", "null"}:
                continue
            if txt:
                out[oid] = txt
        except Exception:
            continue
    return out


def main() -> None:
    """Hlavni UI."""
    st.title("VYŘAZOVÁNÍ – experimentální analýza fondu")
    st.caption("Vstup: Parquet s výpůjčkami. Výstup: návrh kategorie pro další posouzení na pobočkách.")
    with st.expander("Pozice a percentil ve signatuře — co znamenají", expanded=False):
        st.markdown(
            """
            - **Prefix signatury** (`SIGN_PREFIX`): první písmena signatury (typicky 2 znaky), např. z `JD 30652` → `JD`.  
              Tituly se uvnitř něj **řadí podle počtu výpůjček v období** (sestupně).
            - **Pořadí / počet titulů ve stejném prefixu** (`relativni_pozice_v_signature`): např. `42/1200` znamená, že titul je  
              **42. nejpůjčovanější** v daném prefixu mezi **1200** tituly, které ten prefix mají.
            - **Percentil v rámci prefixu signatury** (`percentil_v_signature`): jak „vysoko“ je titul v žebříčku prefixu —  
              **vyšší číslo = relativně půjčovanější** v rámci prefixu (100 % = nejlepší v prefixu).  
              Není to percentil celého fondu, jen **uvnitř stejné skupiny signatur**.
            """
        )
    with st.expander("Chráněné výjimky — proč může být 0 titulů", expanded=False):
        st.markdown(
            """
            Kategorie **Chráněno výjimkou** vzniká, když **text v datech** obsahuje některé z klíčových slov z JSONu v postranním panelu  
            (sloupce `TITUL_DRUH_DOKUMENTU`, `TITUL_SIGN_FULL`, `TITUL_NAZEV` — podle toho, co v Parquetu je).

            Ve výchozím buildu je sloupec **`desk_subjects` v enriched datech prázdný**, takže pravidla zaměřená jen na deskriptory  
            **nemají co matchovat**, dokud do pipeline nedoplníte textové deskriptory nebo neupravíte pravidla (např. názvy, signatura).

            **Doporučená vazba:** upřednostněte klíčová slova v **názvu**, **signatuře** a **druhu dokumentu** — ta jsou v datech běžně vyplněná.
            """
        )

    uploaded = None
    parquet_path = ""

    # Streamlit si drží session_state i po změně Secrets; pokud uživatel kdysi zadal lokální cestu
    # (např. /Users/...), na cloudu pak soubor neexistuje. V tom případě automaticky resetujeme na výchozí.
    default_display = _initial_parquet_path_display()
    if "parquet_path_input" not in st.session_state:
        st.session_state.parquet_path_input = default_display
    else:
        cur = str(st.session_state.parquet_path_input or "").strip()
        if cur and not _path_exists_for_ui(cur) and not _using_secrets_parquet_default():
            st.session_state.parquet_path_input = default_display

    with st.sidebar:
        st.header("Nastavení analýzy")
        if st.button("Resetovat cestu na výchozí dataset", help="Vrátí cestu na soubor vedle `app.py` (nebo na Secrets default)."):
            st.session_state.parquet_path_input = default_display
        if not _using_secrets_parquet_default():
            found = find_default_parquet_in_project()
            if found is not None:
                st.success(f"Data: `{found.name}` (ve složce aplikace)")
            else:
                st.info(
                    f"Do složky s aplikací zkopírujte soubor **{DEFAULT_PARQUET_PATH}** "
                    f"(stejná složka jako `app.py`). "
                    f"Složka aplikace: `{project_root()}`"
                )
        with st.expander("Pokročilé: vlastní cesta k souboru nebo odkaz (většinou nechte výchozí)"):
            parquet_path = st.text_input(
                "Cesta k Parquet / https / s3",
                key="parquet_path_input",
                help="Relativní cesty jsou vždy od složky s aplikací. Pro velké soubory "
                "nepoužívejte síťový disk – zkopírujte Parquet k sobě do této složky.",
            )
        uploaded = st.file_uploader(
            "Nebo nahraj menší Parquet z disku",
            type=["parquet"],
            help="Nahraje se do paměti; u velkých souborů raději soubor ve složce aplikace.",
        )
        years_window = st.number_input("Délka sledovaného období (roky)", min_value=1, max_value=20, value=5)
        bottom_percentile = st.slider(
            "Hranice spodních procent v rámci prefixu signatury",
            1,
            80,
            30,
            help="Pro ruční posouzení: titul musí být v dolních X % výpůjčnosti uvnitř stejného prefixu signatury.",
        )
        low_min, low_max = st.slider(
            "Nízká výpůjčnost (ruční posouzení) — rozsah výpůjček v období",
            0,
            20,
            (1, 2),
            help="Spolu s percentilem výše určuje „ruční posouzení“ u titulů s malým počtem výpůjček.",
        )
        auto_max_loans = st.slider(
            "Automatický kandidát — max. výpůjček v období",
            0,
            50,
            0,
            help="Tituly s počtem výpůjček v období **nejvýše** tolik (a bez výjimky) jdou mezi automatické kandidáty, "
            "pokud na ně pak nesedí pravidla pro ruční posouzení. Hodnota **0** = jen tituly s nulou výpůjček.",
        )
        stale_years = st.slider("Poslední výpůjčka starší než (roky)", 1.0, 10.0, 3.0)
        exceptions_enabled = st.checkbox("Zapnout výjimky", value=True)
        rule_text = st.text_area(
            "Pravidla výjimek (JSON: sloupec → seznam klíčových slov)",
            value=json.dumps(DEFAULT_EXCEPTION_KEYWORDS, ensure_ascii=False, indent=2),
            height=170,
            help="Shoda jako podřetězec v textu sloupce (bez rozlišení velikosti písmen). Sloupec musí v Parquetu existovat.",
        )
        parsed_rules = parse_exception_rules(rule_text)
        st.caption("Pokud JSON není platný, použijí se výchozí pravidla z konfigurace.")

    try:
        if uploaded is not None:
            raw_df, validation = load_and_validate_data(uploaded_bytes=uploaded.getvalue())
            data_label = "nahraný soubor"
            df = clean_data(raw_df)
        else:
            resolved = resolve_parquet_path_for_load(parquet_path)
            # Cloud-safe: pro URL si stahneme Parquet do cache souboru a dal pracujeme nad souborem,
            # ne nad obrovskym pandas DF.
            local_parquet = resolve_parquet_source_to_local_file(
                resolved,
                storage_options=_s3_storage_options_from_secrets(),
            )
            data_label = str(local_parquet)
            df = None
    except Exception as exc:
        st.error(f"Nepodařilo se načíst Parquet data: {exc}")
        if uploaded is None and not _path_exists_for_ui(parquet_path):
            st.info(
                f"Soubor se nepodařilo otevřít. Zkontrolujte, že v projektu je **{DEFAULT_PARQUET_PATH}** "
                f"nebo zadejte úplnou cestu. Složka aplikace: `{project_root()}`"
            )
        return

    if uploaded is not None:
        if validation.warnings:
            for warning in validation.warnings:
                st.warning(warning)
        st.success(f"Načteno {_fmt_int(len(df))} řádků z {data_label}")
    else:
        st.success(f"Zdroj připraven: {data_label}")

    # Výběr akcí (ACTION_TYPE = text z OPIDY v enriched Parquetu):
    # - upload: multiselect z hodnot ve DataFrame,
    # - soubor s událostmi (enriched): DISTINCT z Parquetu + výchozí dle IS_LOAN/ACTION_GROUP,
    # - titles_metrics.parquet: bez filtru (už agregováno při buildu).
    selected_actions: List[str] = list(DEFAULT_ACTION_TYPES)
    if uploaded is not None and df is not None and "ACTION_TYPE" in df.columns:
        action_options = sorted([x for x in df["ACTION_TYPE"].dropna().unique().tolist() if str(x).strip()])
        default_set = {x.lower() for x in DEFAULT_ACTION_TYPES}
        default_actions = [a for a in action_options if str(a).lower().strip() in default_set] or action_options
        selected_actions = st.sidebar.multiselect(
            "Typy operací (ACTION_TYPE) do počtu výpůjček",
            options=action_options if action_options else list(DEFAULT_ACTION_TYPES),
            default=default_actions if default_actions else action_options,
            help="Do metrik se počítají jen vybrané typy operací (řádky v datech).",
        )
    elif uploaded is None:
        p = Path(data_label)
        if is_precomputed_titles_metrics_parquet(p):
            with st.sidebar:
                st.caption(
                    "Předpočítaný soubor: **vypujcky_okno** = počet podle výběru typů operací při buildu; "
                    "sloupce **pocet_opid_***, **pocet_operaci_v_obdobi**, **pocet_vypujcek_is_loan**, "
                    "**pocet_vraceni_is_return** jsou k dispozici po přegenerování `titles_metrics.parquet` "
                    "z aktuálního kódu. Pro interaktivní výběr typů načtěte `vypujcky_enriched.parquet`."
                )
        else:
            action_opts = get_distinct_action_types_from_parquet(p)
            if action_opts:
                suggested = suggest_loan_action_types_from_parquet(p)
                default_sel = suggested if suggested else action_opts
                default_sel = [x for x in default_sel if x in action_opts] or action_opts
                with st.sidebar:
                    selected_actions = st.multiselect(
                        "Typy operací (ACTION_TYPE) do počtu výpůjček",
                        options=action_opts,
                        default=default_sel,
                        key="file_action_types",
                        help="Počítá se jen vybraný typ operace (text z OPIDY). Výchozí = typy označené jako výpůjčka (IS_LOAN / ACTION_GROUP).",
                    )
                if not selected_actions:
                    st.warning("Vyberte alespoň jeden typ operace.")
                    return
            else:
                with st.sidebar:
                    st.caption(
                        "V souboru není sloupec ACTION_TYPE — výpůjčky se určí přes IS_LOAN / ACTION_GROUP v datech."
                    )
                selected_actions = []

    if uploaded is None:
        p = Path(data_label)
        if is_precomputed_titles_metrics_parquet(p):
            metrics_df = pd.read_parquet(p)
            if "pocet_udalosti_okno" in metrics_df.columns and "pocet_operaci_v_obdobi" not in metrics_df.columns:
                metrics_df = metrics_df.rename(columns={"pocet_udalosti_okno": "pocet_operaci_v_obdobi"})
            prefix_summary = (
                metrics_df["SIGN_PREFIX"].fillna("").astype(str).value_counts().rename_axis("SIGN_PREFIX").reset_index(name="TITULU")
            )
            prefix_options = ["(vsechny)"] + prefix_summary["SIGN_PREFIX"].astype(str).tolist() if not prefix_summary.empty else ["(vsechny)"]
            with st.sidebar:
                chosen_prefix = st.selectbox(
                    "Vyber SIGN_PREFIX (doporučeno pro rychlost)",
                    options=prefix_options,
                    index=1 if len(prefix_options) > 1 else 0,
                    help="Aby appka neběhala nad miliony titulů najednou, vyber prefix signatury.",
                )
            st.subheader("Dostupné prefixy signatur (souhrn)")
            st.dataframe(prefix_summary_for_display(prefix_summary), use_container_width=True)
            if chosen_prefix != "(vsechny)":
                metrics_df = metrics_df[metrics_df["SIGN_PREFIX"].astype(str) == chosen_prefix]
            st.caption("Používám předpočítaný dataset po titulech (titles_metrics.parquet).")
        else:
            prefix_summary = get_sign_prefix_summary_from_parquet(p)
            prefix_options = ["(vsechny)"] + prefix_summary["SIGN_PREFIX"].astype(str).tolist() if not prefix_summary.empty else ["(vsechny)"]
            with st.sidebar:
                chosen_prefix = st.selectbox(
                    "Vyber SIGN_PREFIX (doporučeno pro rychlost)",
                    options=prefix_options,
                    index=1 if len(prefix_options) > 1 else 0,
                    help="Aby appka neběhala nad miliony titulů najednou, vyber prefix signatury.",
                )
            st.subheader("Dostupné prefixy signatur (souhrn)")
            st.dataframe(prefix_summary_for_display(prefix_summary), use_container_width=True)

            metrics_df = compute_title_metrics_from_parquet(
                p,
                years_window=int(years_window),
                action_types_for_loans=list(selected_actions),
                sign_prefix=None if chosen_prefix == "(vsechny)" else chosen_prefix,
            )
            st.caption(
                "Metriky počítané cloud-friendly přes DuckDB (bez načtení celého Parquetu do paměti). "
                "Pro stabilitu doporučujeme vybrat konkrétní SIGN_PREFIX."
            )
    else:
        window_all, start_date, end_date = filter_to_relevant_window(
            df, int(years_window), selected_actions, loan_filter=False
        )
        window_df, _, _ = filter_to_relevant_window(
            df, int(years_window), selected_actions, loan_filter=True
        )
        st.caption(
            f"Období analýzy: {start_date.date()} - {end_date.date()} "
            "(konec = nejnovější datum v datasetu, fallback = dnes)."
        )
        metrics_df = compute_title_metrics(df, window_df, window_all_df=window_all)
    # Standardizace názvů: okno let + neutrální sloupec pro počet výpůjček v okně
    try:
        metrics_df["okno_let"] = int(years_window)
    except Exception:
        pass
    if "vypujcky_okno" not in metrics_df.columns and "vypujcky_5_let" in metrics_df.columns:
        metrics_df["vypujcky_okno"] = metrics_df["vypujcky_5_let"]
    metrics_df["vyjimka_flag"] = identify_exceptions(metrics_df, parsed_rules, exceptions_enabled)
    classified_df = classify_titles(
        metrics_df,
        low_min,
        low_max,
        bottom_percentile,
        stale_years,
        auto_max_loans=int(auto_max_loans),
    )

    with st.sidebar:
        with st.expander("Diagnostika zdrojových souborů", expanded=False):
            st.caption(f"Použitý dataset: `{data_label}`")
            needed_cols = ["pocet_svazku", "vykon_na_svazek", "vypujcky_okno", "TITUL_NAZEV", "TITUL_SIGN_FULL"]
            present = [c for c in needed_cols if c in metrics_df.columns]
            missing = [c for c in needed_cols if c not in metrics_df.columns]
            st.caption("Důležité sloupce: " + (", ".join(present) if present else "(žádné)"))
            if missing:
                st.warning("Chybí sloupce: " + ", ".join(missing))

            # Sidecar soubory používané jen pro zobrazení popisů OPID.
            candidates = []
            try:
                candidates.append(Path(data_label).parent / "opidy.parquet")
            except Exception:
                pass
            candidates.append(project_root() / "opidy.parquet")
            found = [p for p in candidates if p.exists()]
            if found:
                st.caption("Nalezeno `opidy.parquet`: " + ", ".join([f"`{p.name}`" for p in found]))
            else:
                st.info("Nenalezeno `opidy.parquet` vedle datasetu ani vedle `app.py` (OPID popisky mohou být jen jako ID).")

    opid_name_map: Dict[int, str] = {}
    if uploaded is None:
        p_leg = Path(data_label)
        if not is_precomputed_titles_metrics_parquet(p_leg):
            leg = get_opid_legend_from_parquet(p_leg)
            if not leg.empty:
                opid_name_map = _build_opid_name_map(leg)
                with st.expander("Legenda OPID (mapování ID operace na popis)", expanded=False):
                    st.dataframe(opid_legend_for_display(leg), use_container_width=True)
        # U precomputed titles_metrics už OPID legenda ve vstupu nebývá.
        # Zkusíme ji najít vedle dat v opidy.parquet.
        if not opid_name_map:
            opidy_candidates = []
            try:
                opidy_candidates.append(Path(data_label).parent / "opidy.parquet")
            except Exception:
                pass
            opidy_candidates.append(project_root() / "opidy.parquet")
            for op_path in opidy_candidates:
                if op_path.exists():
                    try:
                        op_df = pd.read_parquet(op_path)
                        opid_name_map = _build_opid_name_map(op_df)
                        if opid_name_map:
                            break
                    except Exception:
                        continue

    with st.sidebar:
        f_opid_col = "(žádný)"
        f_opid_min = 0
        f_opid_max = 10**12
        all_sign = sorted(classified_df["SIGN_PREFIX"].fillna("").unique().tolist())
        all_lang = sorted(classified_df["TITUL_JAZYK"].fillna("").unique().tolist())
        all_doc = sorted(classified_df["TITUL_DRUH_DOKUMENTU"].fillna("").unique().tolist())
        all_cls = ["AUTO_KANDIDAT", "RUCNI_POSOUZENI", "CHRANENO_VYJIMKOU", "PONECHAT"]
        f_sign = st.multiselect("Filtrovat signatury (prefix, např. JD)", all_sign)
        f_lang = st.multiselect("Filtrovat jazyk", all_lang)
        f_doc = st.multiselect("Filtrovat typ dokumentu", all_doc)
        f_cls = st.multiselect(
            "Filtrovat klasifikaci",
            all_cls,
            default=all_cls,
            format_func=klasifikace_display_name,
        )
        f_name = st.text_input("Vyhledat podle názvu")
        f_desk = ""
        f_och = ""
        f_desk_suggest: List[str] = []
        f_och_suggest: List[str] = []
        with st.expander("Diagnostika subjektů (desk/OCH)", expanded=False):
            if "desk_subjects" in classified_df.columns:
                dser = classified_df["desk_subjects"].fillna("").astype(str).str.strip()
                st.caption(f"Deskriptory vyplněné u titulů: {_fmt_int((dser != '').sum())} / {_fmt_int(len(dser))}")
            else:
                st.caption("Sloupec `desk_subjects` není ve vstupních datech.")
            if "och_subjects" in classified_df.columns:
                oser = classified_df["och_subjects"].fillna("").astype(str).str.strip()
                st.caption(f"OCH vyplněné u titulů: {_fmt_int((oser != '').sum())} / {_fmt_int(len(oser))}")
            else:
                st.caption("Sloupec `och_subjects` není ve vstupních datech.")
        if "desk_subjects" in classified_df.columns:
            f_desk = st.text_input(
                "Vyhledat v deskriptorech (obsah)",
                key="filter_desk",
                help="Podřetězec v agregovaném textu deskriptorů (po joinu z pipeline).",
            )
            desk_terms = _subject_terms(classified_df["desk_subjects"])
            desk_prefix = f_desk.strip().lower()
            desk_opts = [t for t in desk_terms if (not desk_prefix or t.lower().startswith(desk_prefix))][:80]
            f_desk_suggest = st.multiselect(
                "Našeptávač deskriptorů",
                options=desk_opts,
                key="filter_desk_suggest",
                help="Začni psát do pole výše a vyber jednu či více navržených hodnot.",
            )
        if "och_subjects" in classified_df.columns:
            f_och = st.text_input(
                "Vyhledat v obsahové charakteristice (OCH)",
                key="filter_och",
                help="Podřetězec v textech OCH (naučná literatura / obor).",
            )
            och_terms = _subject_terms(classified_df["och_subjects"])
            och_prefix = f_och.strip().lower()
            och_opts = [t for t in och_terms if (not och_prefix or t.lower().startswith(och_prefix))][:80]
            f_och_suggest = st.multiselect(
                "Našeptávač OCH",
                options=och_opts,
                key="filter_och_suggest",
                help="Začni psát do pole výše a vyber jednu či více navržených hodnot.",
            )

        st.divider()
        st.subheader("Svazky × výkon")
        has_copies = "pocet_svazku" in classified_df.columns
        has_perf = "vykon_na_svazek" in classified_df.columns
        if has_copies:
            max_copies_seen = int(pd.to_numeric(classified_df["pocet_svazku"], errors="coerce").fillna(0).max())
            copies_series = pd.to_numeric(classified_df["pocet_svazku"], errors="coerce").fillna(0)
        else:
            max_copies_seen = 0
            copies_series = pd.Series([0])
        if has_perf:
            perf_series = pd.to_numeric(classified_df["vykon_na_svazek"], errors="coerce").fillna(0.0)
        else:
            perf_series = pd.Series([0.0])

        if not (has_copies and has_perf):
            st.info("Svazkové metriky nejsou dostupné (chybí `pocet_svazku` nebo `vykon_na_svazek`).")
            mode = "Vlastní filtry"
            max_copies_ui = 1
            perf_cap = 1.0
        else:
            # Doporučené výchozí prahy z rozdělení dat (posuvné uživatelem).
            max_copies_ui = max(1, int(max_copies_seen))
            perf_cap = float(max(1.0, float(perf_series.max())))
            rec_copies_high = int(max(3, copies_series.quantile(0.90)))
            rec_perf_low = float(perf_series.quantile(0.20))
            rec_copies_low = int(max(1, copies_series.quantile(0.10)))
            rec_perf_high = float(perf_series.quantile(0.80))

            mode = st.radio(
                "Režim doporučení",
                options=[
                    "Vlastní filtry",
                    "Kandidát na odpis (příliš svazků)",
                    "Kandidát na dokup (málo svazků)",
                ],
                index=0,
                key="svazky_mode",
                help="Přepíná přednastavené prahy pro hledání titulů se špatným poměrem svazky × výkon.",
            )

            prev = st.session_state.get("_svazky_mode_prev")
            if prev != mode:
                if mode == "Kandidát na odpis (příliš svazků)":
                    st.session_state["filters_copies_range"] = (rec_copies_high, max_copies_ui)
                    st.session_state["filters_perf_range"] = (0.0, float(max(0.0, rec_perf_low)))
                elif mode == "Kandidát na dokup (málo svazků)":
                    st.session_state["filters_copies_range"] = (0, min(rec_copies_low, max_copies_ui))
                    st.session_state["filters_perf_range"] = (float(max(0.0, rec_perf_high)), perf_cap)
                else:
                    st.session_state["filters_copies_range"] = (0, max_copies_ui)
                    st.session_state["filters_perf_range"] = (0.0, perf_cap)
                st.session_state["_svazky_mode_prev"] = mode

        # Keep filter values separate from widget keys to avoid Streamlit session_state/widget conflicts.
        if "filters_copies_range" not in st.session_state:
            st.session_state["filters_copies_range"] = (0, max(1, int(max_copies_ui)))
        if "filters_perf_range" not in st.session_state:
            st.session_state["filters_perf_range"] = (0.0, float(perf_cap))

        c_min, c_max = st.slider(
            "Počet svazků na titul (rozsah)",
            min_value=0,
            max_value=max(1, int(max_copies_ui)),
            value=st.session_state["filters_copies_range"],
            key="copies_range_widget",
            help="Filtr na počet exemplářů/svazků pro titul.",
        )
        p_min, p_max = st.slider(
            "Výkon na svazek (výpůjčky / svazek)",
            min_value=0.0,
            max_value=float(perf_cap),
            value=st.session_state["filters_perf_range"],
            key="perf_range_widget",
            step=max(0.1, float(perf_cap) / 100.0),
            help="Počet výpůjček v období (pro výkon) dělený počtem svazků — vyšší = lepší využití exemplářů.",
        )

        st.session_state["filters_copies_range"] = (c_min, c_max)
        st.session_state["filters_perf_range"] = (p_min, p_max)

        st.caption(
            "Tip: pro kandidáty na odpis nastav vyšší počet svazků na titul a nižší výkon na svazek; "
            "pro kandidáty na dokup naopak."
        )

        opid_cols = sorted([c for c in classified_df.columns if c.startswith("pocet_opid_")])
        summary_op_cols = [
            c
            for c in ("pocet_operaci_v_obdobi", "pocet_vypujcek_is_loan", "pocet_vraceni_is_return")
            if c in classified_df.columns
        ]
        if opid_cols or summary_op_cols:
            st.divider()
            st.subheader("Počty operací v období a podle OPID")
            st.caption(
                "Počet výpůjček v období (sloupec v tabulce) odpovídá výběru typů operací výše — používá se pro klasifikaci "
                "a výkon na svazek. Sloupce počtu operací v období a podle OPID udávají totéž časové okno jako analýza, "
                "rozpadlé podle typu operace."
            )
            f_opid_col = st.selectbox(
                "Filtrovat podle počtu (sloupec)",
                ["(žádný)"] + summary_op_cols + opid_cols,
                key="filter_opid_col",
                format_func=lambda c: "(žádný)" if c == "(žádný)" else _opid_display_label(c, opid_name_map),
            )
            if f_opid_col != "(žádný)":
                ser_mx = int(pd.to_numeric(classified_df[f_opid_col], errors="coerce").fillna(0).max())
                ser_mx = max(ser_mx, 1)
                cap = min(ser_mx, 500_000)
                opid_label = _opid_display_label(f_opid_col, opid_name_map)
                f_opid_min, f_opid_max = st.slider(
                    f"Rozsah {opid_label} (0–{ser_mx} v datech, posuvník do {cap})",
                    min_value=0,
                    max_value=int(cap),
                    value=(0, int(cap)),
                    key="filter_opid_slider",
                )
                if ser_mx > cap:
                    st.caption(
                        f"Posuvník je omezen na {cap}; pro vyšší hodnoty použijte export a filtr v Excelu."
                    )

    filtered = classified_df.copy()
    if f_sign:
        filtered = filtered[filtered["SIGN_PREFIX"].isin(f_sign)]
    if f_lang:
        filtered = filtered[filtered["TITUL_JAZYK"].isin(f_lang)]
    if f_doc:
        filtered = filtered[filtered["TITUL_DRUH_DOKUMENTU"].isin(f_doc)]
    if f_cls:
        filtered = filtered[filtered["klasifikace"].isin(f_cls)]
    if f_name:
        filtered = filtered[filtered["TITUL_NAZEV"].str.lower().str.contains(f_name.lower(), na=False)]
    desk_terms = [x for x in ([f_desk.strip()] + f_desk_suggest) if str(x).strip()]
    if desk_terms and "desk_subjects" in filtered.columns:
        dser = filtered["desk_subjects"].fillna("").astype(str).str.lower()
        mask = pd.Series(False, index=filtered.index)
        for term in desk_terms:
            mask = mask | dser.str.contains(str(term).lower(), na=False)
        filtered = filtered[mask]
    och_terms = [x for x in ([f_och.strip()] + f_och_suggest) if str(x).strip()]
    if och_terms and "och_subjects" in filtered.columns:
        oser = filtered["och_subjects"].fillna("").astype(str).str.lower()
        mask = pd.Series(False, index=filtered.index)
        for term in och_terms:
            mask = mask | oser.str.contains(str(term).lower(), na=False)
        filtered = filtered[mask]
    if f_opid_col != "(žádný)" and f_opid_col in filtered.columns:
        ser_op = pd.to_numeric(filtered[f_opid_col], errors="coerce").fillna(0)
        filtered = filtered[ser_op.between(int(f_opid_min), int(f_opid_max))]
    if "pocet_svazku" in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered["pocet_svazku"], errors="coerce").fillna(0).between(c_min, c_max)]
    if "vykon_na_svazek" in filtered.columns:
        perf_series = pd.to_numeric(filtered["vykon_na_svazek"], errors="coerce").fillna(0.0)
        filtered = filtered[perf_series.between(p_min, p_max)]

    summary_signature = aggregate_by_signature(filtered)
    signature_focus = st.selectbox(
        "Přehled po signaturách - detail prefixu",
        options=["(vsechny)"] + summary_signature["SIGN_PREFIX"].astype(str).tolist()
        if not summary_signature.empty
        else ["(vsechny)"],
    )
    if signature_focus != "(vsechny)":
        filtered = filtered[filtered["SIGN_PREFIX"].astype(str) == signature_focus]
        summary_signature = aggregate_by_signature(filtered)

    total_titles = int(filtered["title_key"].nunique()) if not filtered.empty else 0
    counts = filtered["klasifikace"].value_counts().to_dict()
    c_auto = int(counts.get("AUTO_KANDIDAT", 0))
    c_manual = int(counts.get("RUCNI_POSOUZENI", 0))
    c_exc = int(counts.get("CHRANENO_VYJIMKOU", 0))
    c_keep = int(counts.get("PONECHAT", 0))
    if not filtered.empty and "pocet_svazku" in filtered.columns:
        total_copies = int(pd.to_numeric(filtered["pocet_svazku"], errors="coerce").fillna(0).sum())
    else:
        total_copies = 0
    if not filtered.empty and "vykon_na_svazek" in filtered.columns:
        avg_perf = float(pd.to_numeric(filtered["vykon_na_svazek"], errors="coerce").fillna(0.0).mean())
    else:
        avg_perf = 0.0

    st.subheader("Souhrn")
    k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
    k1.metric("Titulů celkem", _fmt_int(total_titles))
    k2.metric(klasifikace_display_name("AUTO_KANDIDAT"), _fmt_int(c_auto))
    k3.metric(klasifikace_display_name("RUCNI_POSOUZENI"), _fmt_int(c_manual))
    k4.metric(klasifikace_display_name("CHRANENO_VYJIMKOU"), _fmt_int(c_exc))
    k5.metric(klasifikace_display_name("PONECHAT"), _fmt_int(c_keep))
    k6.metric("Svazků celkem", _fmt_int(total_copies))
    k7.metric("Prům. výkon/svazek", _fmt_float(avg_perf, 2))
    if total_titles > 0:
        st.caption(
            f"Podíly: automatický kandidát {_fmt_float(100*c_auto/total_titles, 1)} % | ruční posouzení "
            f"{_fmt_float(100*c_manual/total_titles, 1)} % | chráněno výjimkou {_fmt_float(100*c_exc/total_titles, 1)} % | "
            f"ponechat {_fmt_float(100*c_keep/total_titles, 1)} %"
        )

    # Top kandidati podle svazku × výkon (jen pokud jsou metriky dostupné)
    if "pocet_svazku" in filtered.columns and "vykon_na_svazek" in filtered.columns:
        st.subheader("Top kandidáti (svazky × výkon)")
        f2 = filtered.copy()
        f2["pocet_svazku"] = pd.to_numeric(f2["pocet_svazku"], errors="coerce").fillna(0).astype(int)
        f2["vykon_na_svazek"] = pd.to_numeric(f2["vykon_na_svazek"], errors="coerce").fillna(0.0)
        eps = 1e-9

        odpis = f2[(f2["pocet_svazku"] >= int(c_min)) & (f2["vykon_na_svazek"] <= float(p_max))].copy()
        odpis["skore_odpis"] = (odpis["pocet_svazku"].clip(lower=1) - 1) / (odpis["vykon_na_svazek"] + eps)
        odpis = odpis.sort_values(["skore_odpis", "pocet_svazku"], ascending=[False, False]).head(50)

        dokup = f2[(f2["pocet_svazku"] <= int(c_max)) & (f2["vykon_na_svazek"] >= float(p_min))].copy()
        dokup["skore_dokup"] = (dokup["vykon_na_svazek"] + eps) / (dokup["pocet_svazku"].clip(lower=1))
        dokup = dokup.sort_values(["skore_dokup", "vykon_na_svazek"], ascending=[False, False]).head(50)

        cA, cB = st.columns(2)
        with cA:
            st.markdown("**Kandidát na odpis (příliš svazků)**")
            cols_odpis = [
                c
                for c in [
                    "TITUL_NAZEV",
                    "SIGN_PREFIX",
                    "TITUL_SIGN_FULL",
                    "pocet_svazku",
                    "vypujcky_okno",
                    "vykon_na_svazek",
                    "klasifikace",
                ]
                if c in odpis.columns
            ]
            st.dataframe(dataframe_for_display(odpis[cols_odpis]), use_container_width=True)
        with cB:
            st.markdown("**Kandidát na dokup (málo svazků)**")
            cols_dokup = [
                c
                for c in [
                    "TITUL_NAZEV",
                    "SIGN_PREFIX",
                    "TITUL_SIGN_FULL",
                    "pocet_svazku",
                    "vypujcky_okno",
                    "vykon_na_svazek",
                    "klasifikace",
                ]
                if c in dokup.columns
            ]
            st.dataframe(dataframe_for_display(dokup[cols_dokup]), use_container_width=True)

        st.subheader("Nejasné případy (vyžaduje kontext)")
        med_copies = int(f2["pocet_svazku"].median()) if not f2.empty else 0
        med_perf = float(f2["vykon_na_svazek"].median()) if not f2.empty else 0.0
        st.caption(
            f"Orientační dělení podle mediánu v aktuálním výběru: "
            f"počet svazků ≈ {_fmt_int(med_copies)}, výkon na svazek ≈ {_fmt_float(med_perf, 2)}. "
            "Typicky sem patří (a) hodně svazků i vysoký výkon nebo (b) málo svazků i nízký výkon."
        )

        unclear_hi = f2[(f2["pocet_svazku"] >= med_copies) & (f2["vykon_na_svazek"] >= med_perf)].copy()
        unclear_hi["typ"] = "hodně svazků + vysoký výkon"
        unclear_hi = unclear_hi.sort_values(["pocet_svazku", "vykon_na_svazek"], ascending=[False, False]).head(25)

        unclear_lo = f2[(f2["pocet_svazku"] <= med_copies) & (f2["vykon_na_svazek"] <= med_perf)].copy()
        unclear_lo["typ"] = "málo svazků + nízký výkon"
        unclear_lo = unclear_lo.sort_values(["vykon_na_svazek", "pocet_svazku"], ascending=[True, True]).head(25)

        unclear = pd.concat([unclear_hi, unclear_lo], ignore_index=True)
        cols_unclear = [
            c
            for c in [
                "typ",
                "TITUL_NAZEV",
                "SIGN_PREFIX",
                "TITUL_SIGN_FULL",
                "pocet_svazku",
                "vypujcky_okno",
                "vykon_na_svazek",
                "klasifikace",
            ]
            if c in unclear.columns
        ]
        st.dataframe(dataframe_for_display(unclear[cols_unclear]), use_container_width=True)

    st.subheader("Tabulka výsledků")
    op_cols_ordered = [
        c
        for c in ("pocet_operaci_v_obdobi", "pocet_vypujcek_is_loan", "pocet_vraceni_is_return")
        if c in filtered.columns
    ]
    op_cols_ordered += sorted([c for c in filtered.columns if c.startswith("pocet_opid_")])
    cols = [
        "TITUL_NAZEV",
        "SIGN_PREFIX",
        "TITUL_SIGN_FULL",
        "TITUL_KEY",
        "TITUL_DRUH_DOKUMENTU",
        "TITUL_JAZYK",
        "TITUL_ROK_VYDANI",
        "okno_let",
        "vypujcky_okno",
        *op_cols_ordered,
        "pocet_svazku",
        "vykon_na_svazek",
        "datum_posledni_vypujcky",
        "roky_od_posledni_vypujcky",
        "percentil_v_signature",
        "relativni_pozice_v_signature",
        "rizikove_skore",
        "bez_vypujcek",
        "bez_svazku",
        "desk_subjects",
        "och_subjects",
        "klasifikace",
        "duvod_oznaceni",
        "vyjimka_flag",
    ]
    cols = [c for c in cols if c in filtered.columns]
    st.dataframe(dataframe_for_display(filtered[cols]), use_container_width=True)

    st.subheader("Souhrn po signaturách")
    st.dataframe(summary_signature_for_display(summary_signature), use_container_width=True)

    st.subheader("Vizualizace")
    c1, c2, c3 = st.columns(3)
    with c1:
        chart_data = filtered["klasifikace"].value_counts().rename_axis("klasifikace").reset_index(name="pocet")
        chart_data["Klasifikace"] = chart_data["klasifikace"].map(klasifikace_display_name)
        st.plotly_chart(
            px.bar(chart_data, x="Klasifikace", y="pocet", title="Počet titulů podle klasifikace"),
            use_container_width=True,
        )
    with c2:
        top_auto = summary_signature.sort_values("AUTO_KANDIDAT", ascending=False).head(10)
        top_auto_d = top_auto.rename(
            columns={"SIGN_PREFIX": "Prefix signatury", "AUTO_KANDIDAT": klasifikace_display_name("AUTO_KANDIDAT")}
        )
        st.plotly_chart(
            px.bar(
                top_auto_d,
                x="Prefix signatury",
                y=klasifikace_display_name("AUTO_KANDIDAT"),
                title="Prefixy s nejvíce automatickými kandidáty",
            ),
            use_container_width=True,
        )
    with c3:
        if "pocet_svazku" in filtered.columns and "vykon_na_svazek" in filtered.columns:
            viz = filtered.copy()
            viz["pocet_svazku"] = pd.to_numeric(viz["pocet_svazku"], errors="coerce").fillna(0)
            viz["vykon_na_svazek"] = pd.to_numeric(viz["vykon_na_svazek"], errors="coerce").fillna(0.0)
            if "klasifikace" in viz.columns:
                viz["Klasifikace"] = viz["klasifikace"].map(klasifikace_display_name)
            st.plotly_chart(
                px.scatter(
                    viz,
                    x="pocet_svazku",
                    y="vykon_na_svazek",
                    color="Klasifikace" if "Klasifikace" in viz.columns else None,
                    labels={
                        "pocet_svazku": "Počet svazků na titul",
                        "vykon_na_svazek": "Výkon na svazek (výpůjčky / svazek)",
                    },
                    hover_data=[c for c in ["TITUL_NAZEV", "SIGN_PREFIX", "TITUL_SIGN_FULL", "vypujcky_okno", "vypujcky_5_let"] if c in viz.columns],
                    title="Svazky × výkon (výpůjčky na svazek)",
                ),
                use_container_width=True,
            )
        else:
            st.plotly_chart(
                px.histogram(filtered, x="vypujcky_5_let", nbins=30, title=f"Distribuce výpůjček ({years_window} let)"),
                use_container_width=True,
            )

    st.subheader("Export")
    st.caption("Do souboru se ukládají technické názvy sloupců (vhodné pro další zpracování). V tabulce výše jsou české popisky.")
    st.download_button(
        "Export aktuálně filtrovaných dat do CSV",
        data=export_filtered_to_csv_bytes(filtered[cols]),
        file_name="vyrazovani_filtrovano.csv",
        mime="text/csv",
    )
    st.download_button(
        "Export aktuálně filtrovaných dat do Excelu",
        data=export_filtered_to_excel_bytes(filtered[cols], summary_signature),
        file_name="vyrazovani_filtrovano.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    main()
