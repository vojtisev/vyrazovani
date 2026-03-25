"""Streamlit app - analyza vyrazovani z Parquet dat."""

from __future__ import annotations

import json
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
    get_sign_prefix_summary_from_parquet,
    filter_to_relevant_window,
    identify_exceptions,
    load_and_validate_data,
    is_precomputed_titles_metrics_parquet,
    resolve_parquet_source_to_local_file,
)
from src.export_utils import export_filtered_to_csv_bytes, export_filtered_to_excel_bytes


st.set_page_config(page_title="VYRAZOVANI - analyza fondu", page_icon="📚", layout="wide")


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


def main() -> None:
    """Hlavni UI."""
    st.title("VYRAZOVANI - lokalni analyza knihovniho fondu")
    st.caption("Vstup: parquet se vsemi vypujckami. Vystup: navrh kategorii pro dalsi posouzeni na pobockach.")

    uploaded = None
    parquet_path = ""

    if "parquet_path_input" not in st.session_state:
        st.session_state.parquet_path_input = _initial_parquet_path_display()

    with st.sidebar:
        st.header("Nastaveni analyzy")
        if not _using_secrets_parquet_default():
            found = find_default_parquet_in_project()
            if found is not None:
                st.success(f"Data: `{found.name}` (ve složce aplikace)")
            else:
                st.info(
                    f"Do složky s aplikací zkopírujte soubor **{DEFAULT_PARQUET_PATH}** "
                    f"(stejná složka jako `run_dashboard.bat`). "
                    f"Složka: `{project_root()}`"
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
        years_window = st.number_input("Delka sledovaneho obdobi (roky)", min_value=1, max_value=20, value=5)
        bottom_percentile = st.slider("Hranice spodnich procent v signature", 1, 80, 30)
        low_min, low_max = st.slider("Nizka vypujcnost (RUCNI_POSOUZENI)", 0, 20, (1, 2))
        stale_years = st.slider("Posledni vypujcka starsi nez (roky)", 1.0, 10.0, 3.0)
        exceptions_enabled = st.checkbox("Zapnout vyjimky", value=True)
        rule_text = st.text_area(
            "Seznam vyjimek (JSON)",
            value=json.dumps(DEFAULT_EXCEPTION_KEYWORDS, ensure_ascii=False, indent=2),
            height=170,
        )
        parsed_rules = parse_exception_rules(rule_text)
        st.caption("Pokud JSON neni validni, pouziji defaultni pravidla.")

    try:
        if uploaded is not None:
            raw_df, validation = load_and_validate_data(uploaded_bytes=uploaded.getvalue())
            data_label = "nahraty soubor"
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
        st.error(f"Nepodarilo se nacist Parquet data: {exc}")
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
        st.success(f"Nacteno {len(df):,} radku z {data_label}")
    else:
        st.success(f"Zdroj pripraven: {data_label}")

    # V cloud-safe rezimu nemame cely DF; pouzijeme defaultni akcni typy.
    action_options = (
        sorted([x for x in df["ACTION_TYPE"].dropna().unique().tolist() if str(x).strip()])
        if (uploaded is not None and df is not None and "ACTION_TYPE" in df.columns)
        else DEFAULT_ACTION_TYPES
    )
    default_actions = [a for a in DEFAULT_ACTION_TYPES if a in action_options] or action_options
    selected_actions = st.sidebar.multiselect(
        "Ktere ACTION_TYPE se pocitaji jako vypujcky",
        options=action_options if action_options else DEFAULT_ACTION_TYPES,
        default=default_actions,
    )

    if uploaded is None:
        p = Path(data_label)
        if is_precomputed_titles_metrics_parquet(p):
            metrics_df = pd.read_parquet(p)
            prefix_summary = (
                metrics_df["SIGN_PREFIX"].fillna("").astype(str).value_counts().rename_axis("SIGN_PREFIX").reset_index(name="TITULU")
            )
            prefix_options = ["(vsechny)"] + prefix_summary["SIGN_PREFIX"].astype(str).tolist() if not prefix_summary.empty else ["(vsechny)"]
            with st.sidebar:
                chosen_prefix = st.selectbox(
                    "Vyber SIGN_PREFIX (doporuceno pro rychlost)",
                    options=prefix_options,
                    index=1 if len(prefix_options) > 1 else 0,
                    help="Aby appka neběhala nad miliony titulů najednou, vyber prefix signatury.",
                )
            st.subheader("Dostupne prefixy signatur (souhrn)")
            st.dataframe(prefix_summary, use_container_width=True)
            if chosen_prefix != "(vsechny)":
                metrics_df = metrics_df[metrics_df["SIGN_PREFIX"].astype(str) == chosen_prefix]
            st.caption("Pouzivam predpocitany dataset po titulech (titles_metrics.parquet).")
        else:
            prefix_summary = get_sign_prefix_summary_from_parquet(p)
            prefix_options = ["(vsechny)"] + prefix_summary["SIGN_PREFIX"].astype(str).tolist() if not prefix_summary.empty else ["(vsechny)"]
            with st.sidebar:
                chosen_prefix = st.selectbox(
                    "Vyber SIGN_PREFIX (doporuceno pro rychlost)",
                    options=prefix_options,
                    index=1 if len(prefix_options) > 1 else 0,
                    help="Aby appka neběhala nad miliony titulů najednou, vyber prefix signatury.",
                )
            st.subheader("Dostupne prefixy signatur (souhrn)")
            st.dataframe(prefix_summary, use_container_width=True)

            metrics_df = compute_title_metrics_from_parquet(
                p,
                years_window=int(years_window),
                action_types_for_loans=list(selected_actions),
                sign_prefix=None if chosen_prefix == "(vsechny)" else chosen_prefix,
            )
            st.caption(
                "Metriky pocitany cloud-friendly pres DuckDB (bez nacteni celeho Parquetu do pameti). "
                "Pro stabilitu doporucujeme vybrat konkretni SIGN_PREFIX."
            )
    else:
        window_df, start_date, end_date = filter_to_relevant_window(df, int(years_window), selected_actions)
        st.caption(
            f"Obdobi analyzy: {start_date.date()} - {end_date.date()} "
            "(konec = nejnovejsi datum v datasetu, fallback = dnes)."
        )
        metrics_df = compute_title_metrics(df, window_df)
    metrics_df["vyjimka_flag"] = identify_exceptions(metrics_df, parsed_rules, exceptions_enabled)
    classified_df = classify_titles(metrics_df, low_min, low_max, bottom_percentile, stale_years)

    with st.sidebar:
        all_sign = sorted(classified_df["SIGN_PREFIX"].fillna("").unique().tolist())
        all_lang = sorted(classified_df["TITUL_JAZYK"].fillna("").unique().tolist())
        all_doc = sorted(classified_df["TITUL_DRUH_DOKUMENTU"].fillna("").unique().tolist())
        all_cls = ["AUTO_KANDIDAT", "RUCNI_POSOUZENI", "CHRANENO_VYJIMKOU", "PONECHAT"]
        f_sign = st.multiselect("Filtrovat signatury (prefix, napr. JD)", all_sign)
        f_lang = st.multiselect("Filtrovat jazyk", all_lang)
        f_doc = st.multiselect("Filtrovat typ dokumentu", all_doc)
        f_cls = st.multiselect("Filtrovat klasifikaci", all_cls, default=all_cls)
        f_name = st.text_input("Vyhledat podle nazvu")

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

    summary_signature = aggregate_by_signature(filtered)
    signature_focus = st.selectbox(
        "Prehled po signaturach - detail prefixu",
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

    st.subheader("Souhrn")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Titulu celkem", f"{total_titles:,}")
    k2.metric("AUTO_KANDIDAT", f"{c_auto:,}")
    k3.metric("RUCNI_POSOUZENI", f"{c_manual:,}")
    k4.metric("CHRANENO_VYJIMKOU", f"{c_exc:,}")
    k5.metric("PONECHAT", f"{c_keep:,}")
    if total_titles > 0:
        st.caption(
            f"Podily: AUTO {100*c_auto/total_titles:.1f}% | RUCNI {100*c_manual/total_titles:.1f}% | "
            f"VYJIMKY {100*c_exc/total_titles:.1f}% | PONECHAT {100*c_keep/total_titles:.1f}%"
        )

    st.subheader("Tabulka vysledku")
    cols = [
        "TITUL_NAZEV",
        "SIGN_PREFIX",
        "TITUL_SIGN_FULL",
        "TITUL_DRUH_DOKUMENTU",
        "TITUL_JAZYK",
        "TITUL_ROK_VYDANI",
        "vypujcky_5_let",
        "datum_posledni_vypujcky",
        "roky_od_posledni_vypujcky",
        "percentil_v_signature",
        "relativni_pozice_v_signature",
        "rizikove_skore",
        "klasifikace",
        "duvod_oznaceni",
        "vyjimka_flag",
    ]
    cols = [c for c in cols if c in filtered.columns]
    st.dataframe(filtered[cols], use_container_width=True)

    st.subheader("Souhrn po signaturach")
    st.dataframe(summary_signature, use_container_width=True)

    st.subheader("Vizualizace")
    c1, c2, c3 = st.columns(3)
    with c1:
        chart_data = filtered["klasifikace"].value_counts().rename_axis("klasifikace").reset_index(name="pocet")
        st.plotly_chart(
            px.bar(chart_data, x="klasifikace", y="pocet", title="Pocet titulu podle klasifikace"),
            use_container_width=True,
        )
    with c2:
        top_auto = summary_signature.sort_values("AUTO_KANDIDAT", ascending=False).head(10)
        st.plotly_chart(
            px.bar(top_auto, x="SIGN_PREFIX", y="AUTO_KANDIDAT", title="Top prefixy signatur dle AUTO_KANDIDAT"),
            use_container_width=True,
        )
    with c3:
        st.plotly_chart(
            px.histogram(filtered, x="vypujcky_5_let", nbins=30, title=f"Distribuce vypujcek ({years_window} let)"),
            use_container_width=True,
        )

    st.subheader("Export")
    st.download_button(
        "Export aktualne filtrovaných dat do CSV",
        data=export_filtered_to_csv_bytes(filtered[cols]),
        file_name="vyrazovani_filtrovano.csv",
        mime="text/csv",
    )
    st.download_button(
        "Export aktualne filtrovaných dat do Excelu",
        data=export_filtered_to_excel_bytes(filtered[cols], summary_signature),
        file_name="vyrazovani_filtrovano.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    main()
