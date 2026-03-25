"""Datova logika pro aplikaci analyzy vyrazovani."""

from __future__ import annotations

import io
import os
import tempfile
import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import duckdb

from .config import DEFAULT_REQUIRED_COLUMNS


@dataclass
class ValidationResult:
    missing_columns: List[str]
    present_columns: List[str]
    warnings: List[str]


def _is_http_url(s: str) -> bool:
    t = s.strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _is_s3_uri(s: str) -> bool:
    return s.strip().lower().startswith("s3://")


def validate_dataframe(df: pd.DataFrame) -> ValidationResult:
    """Validace pozadovanych sloupcu nad jiz nactenym DataFrame."""
    missing = [col for col in DEFAULT_REQUIRED_COLUMNS if col not in df.columns]
    present = [col for col in DEFAULT_REQUIRED_COLUMNS if col in df.columns]
    warnings: List[str] = []
    if missing:
        warnings.append(
            "Chybejici sloupce: " + ", ".join(missing) + ". Pouziji se fallbacky tam, kde to jde."
        )
    return ValidationResult(missing_columns=missing, present_columns=present, warnings=warnings)


def load_and_validate_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, ValidationResult]:
    """Vrati DataFrame a validaci (napr. po nacteni z jineho zdroje nez Parquet)."""
    return df, validate_dataframe(df)


def _download_url_to_cache_file(url: str, *, force: bool = False) -> Path:
    """Stahne velky soubor z URL do cache na disk (streaming, bez nahrani do RAM)."""
    # Na Streamlit Cloud muze byt repo read-only; preferujeme HOME cache nebo /tmp.
    env_dir = os.environ.get("VYRAZ_DATA_CACHE_DIR")
    candidates: List[Path] = []
    if env_dir:
        candidates.append(Path(env_dir).expanduser())
    candidates.append(Path.home() / ".cache" / "vyrazovani")
    candidates.append(Path(tempfile.gettempdir()) / "vyrazovani_cache")

    cache_dir: Optional[Path] = None
    last_err: Optional[Exception] = None
    for c in candidates:
        try:
            c.mkdir(parents=True, exist_ok=True)
            test = c / ".write_test"
            test.write_bytes(b"ok")
            test.unlink(missing_ok=True)
            cache_dir = c
            break
        except Exception as e:
            last_err = e
            continue
    if cache_dir is None:
        raise PermissionError(f"Nepodarilo se najit zapisovatelny cache adresar. Posledni chyba: {last_err}")

    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    target = cache_dir / f"source_{url_hash}.parquet"
    if target.exists() and target.stat().st_size > 0 and not force:
        return target

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
    }

    with requests.get(url, headers=headers, allow_redirects=True, stream=True, timeout=300) as resp:
        if resp.status_code != 200:
            raise ValueError(f"HTTP {resp.status_code} pri stahovani dat z URL.")

        fd, tmp_name = tempfile.mkstemp(prefix=target.name + ".", suffix=".tmp", dir=str(cache_dir))
        try:
            with os.fdopen(fd, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            Path(tmp_name).replace(target)
        finally:
            try:
                if os.path.exists(tmp_name):
                    os.remove(tmp_name)
            except Exception:
                pass

    return target


def compute_title_metrics_from_parquet(
    parquet_file: Path,
    *,
    years_window: int,
    action_types_for_loans: List[str],
    signature_col: str = "TITUL_SIGN_FULL",
    prefix_len: int = 2,
) -> pd.DataFrame:
    """Cloud-friendly varianta: agregace po titulu pres DuckDB bez nacteni celeho DF do pandas."""
    loan_actions = [a.lower().strip() for a in action_types_for_loans if str(a).strip()]
    def _sql_quote(s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    action_list_sql = ", ".join([_sql_quote(a) for a in loan_actions]) or "''"

    # DuckDB: nacte jen potrebne sloupce a spocte agregace + ranky v signature.
    # Pozn.: desk_subjects muze byt list -> bereme jako VARCHAR.
    path_sql = str(parquet_file).replace("'", "''")
    query = f"""
WITH src AS (
  SELECT
    CAST({signature_col} AS VARCHAR) AS signature,
    CAST(TITUL_NAZEV AS VARCHAR) AS title_name,
    CAST(TITUL_DRUH_DOKUMENTU AS VARCHAR) AS doc_type,
    CAST(TITUL_JAZYK AS VARCHAR) AS lang,
    TRY_CAST(TITUL_ROK_VYDANI AS DOUBLE) AS year_pub,
    CAST(desk_subjects AS VARCHAR) AS desk_subjects,
    TRY_CAST(DATE AS TIMESTAMP) AS dt,
    TRY_CAST(YEAR AS INTEGER) AS yr,
    LOWER(TRIM(CAST(ACTION_TYPE AS VARCHAR))) AS action_type
  FROM read_parquet('{path_sql}')
),
ref AS (
  SELECT COALESCE(MAX(dt), CURRENT_TIMESTAMP) AS end_dt FROM src
),
window_src AS (
  SELECT s.*
  FROM src s, ref r
  WHERE
    (
      s.dt IS NOT NULL
      AND s.dt >= (r.end_dt - INTERVAL '{int(years_window)} years')
      AND s.dt <= r.end_dt
    )
    OR (
      s.dt IS NULL
      AND s.yr IS NOT NULL
      AND s.yr >= EXTRACT(YEAR FROM (r.end_dt - INTERVAL '{int(years_window)} years'))::INTEGER
      AND s.yr <= EXTRACT(YEAR FROM r.end_dt)::INTEGER
    )
),
window_loans AS (
  SELECT *
  FROM window_src
  WHERE action_type IN ({action_list_sql})
),
base_titles AS (
  SELECT
    signature || '||' || title_name AS title_key,
    signature AS TITUL_SIGN_FULL,
    UPPER(SUBSTR(COALESCE(NULLIF(REGEXP_EXTRACT(TRIM(signature), '^([A-Za-z]+)', 1), ''), SPLIT_PART(TRIM(signature), ' ', 1)), 1, {int(prefix_len)})) AS SIGN_PREFIX,
    title_name AS TITUL_NAZEV,
    ANY_VALUE(doc_type) AS TITUL_DRUH_DOKUMENTU,
    ANY_VALUE(lang) AS TITUL_JAZYK,
    ANY_VALUE(year_pub) AS TITUL_ROK_VYDANI,
    ANY_VALUE(desk_subjects) AS desk_subjects
  FROM src
  GROUP BY 1, 2, 3, 4
),
metrics AS (
  SELECT
    b.*,
    COALESCE(l.loan_cnt, 0) AS vypujcky_5_let,
    l.last_dt AS datum_posledni_vypujcky,
    r.end_dt AS reference_end
  FROM base_titles b
  LEFT JOIN (
    SELECT
      signature || '||' || title_name AS title_key,
      COUNT(*) AS loan_cnt,
      MAX(dt) AS last_dt
    FROM window_loans
    GROUP BY 1
  ) l ON b.title_key = l.title_key
  CROSS JOIN ref r
),
ranked AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY SIGN_PREFIX ORDER BY vypujcky_5_let DESC) AS rank_v_signature,
    COUNT(*) OVER (PARTITION BY SIGN_PREFIX) AS pocet_v_signature
  FROM metrics
)
SELECT
  title_key,
  TITUL_SIGN_FULL,
  SIGN_PREFIX,
  TITUL_NAZEV,
  TITUL_DRUH_DOKUMENTU,
  TITUL_JAZYK,
  TITUL_ROK_VYDANI,
  desk_subjects,
  CAST(vypujcky_5_let AS BIGINT) AS vypujcky_5_let,
  datum_posledni_vypujcky,
  CAST(rank_v_signature AS BIGINT) AS rank_v_signature,
  CAST(pocet_v_signature AS BIGINT) AS pocet_v_signature,
  COALESCE(((pocet_v_signature - rank_v_signature) * 100.0) / NULLIF(pocet_v_signature, 0), 0) AS percentil_v_signature,
  CAST(rank_v_signature AS BIGINT)::VARCHAR || '/' || CAST(pocet_v_signature AS BIGINT)::VARCHAR AS relativni_pozice_v_signature,
  ROUND(DATE_DIFF('day', datum_posledni_vypujcky, reference_end) / 365.25, 2) AS roky_od_posledni_vypujcky
FROM ranked
"""
    con = duckdb.connect(database=":memory:")
    try:
        out = con.execute(query).fetchdf()
    finally:
        con.close()

    out["vypujcky_5_let"] = out["vypujcky_5_let"].fillna(0).astype(int)
    # Rizikove skore zachovame stejne jako pandas varianta
    out["rizikove_skore"] = (
        (out["vypujcky_5_let"] == 0).astype(int) * 70
        + (out["vypujcky_5_let"].between(1, 2)).astype(int) * 20
        + (out["percentil_v_signature"] <= 30).astype(int) * 10
        + (out["roky_od_posledni_vypujcky"].fillna(99) > 3).astype(int) * 15
    )
    return out


def load_and_validate_data(
    parquet_path: Optional[str] = None,
    *,
    uploaded_bytes: Optional[bytes] = None,
    storage_options: Optional[Dict[str, Any]] = None,
    force_download: bool = False,
) -> Tuple[pd.DataFrame, ValidationResult]:
    """Nacte Parquet data a vrati validaci sloupcu.

    Podporuje:
    - nahraty soubor (uploaded_bytes),
    - lokalni cestu,
    - http(s) URL (vcetne predpodepsaneho odkazu),
    - s3://... (volitelne storage_options pro pristupovy klic).
    """
    if uploaded_bytes is not None:
        df = pd.read_parquet(io.BytesIO(uploaded_bytes))
    elif parquet_path is not None:
        raw = parquet_path.strip()
        if not raw:
            raise ValueError("Prazdna cesta k Parquet.")
        if _is_http_url(raw):
            cached_file = _download_url_to_cache_file(raw, force=force_download)
            df = pd.read_parquet(cached_file)
        elif _is_s3_uri(raw):
            df = pd.read_parquet(raw, storage_options=storage_options or {})
        else:
            path = Path(raw).expanduser()
            if not path.exists():
                raise FileNotFoundError(f"Soubor neexistuje: {path}")
            df = pd.read_parquet(path)
    else:
        raise ValueError("Zadej cestu k Parquet nebo nahraj soubor.")
    return load_and_validate_dataframe(df)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Vyčisti data, robustne preved datumy a vypln chybejici hodnoty."""
    out = df.copy()
    for col in ["TITUL_NAZEV", "TITUL_SIGN_FULL", "TITUL_JAZYK", "TITUL_DRUH_DOKUMENTU", "desk_subjects"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("").astype(str).str.strip()

    if "TITUL_ROK_VYDANI" not in out.columns:
        out["TITUL_ROK_VYDANI"] = np.nan
    out["TITUL_ROK_VYDANI"] = pd.to_numeric(out["TITUL_ROK_VYDANI"], errors="coerce")

    if "DATE" in out.columns:
        out["DATE"] = pd.to_datetime(out["DATE"], errors="coerce", dayfirst=True)
    else:
        out["DATE"] = pd.NaT

    if "YEAR" not in out.columns:
        out["YEAR"] = out["DATE"].dt.year
    out["YEAR"] = pd.to_numeric(out["YEAR"], errors="coerce")

    if "ACTION_TYPE" not in out.columns:
        out["ACTION_TYPE"] = ""
    out["ACTION_TYPE"] = out["ACTION_TYPE"].fillna("").astype(str).str.lower().str.strip()
    return out


def get_reference_end_date(df: pd.DataFrame) -> pd.Timestamp:
    """Reference datum: max(DATE) pokud existuje, jinak dnes."""
    max_date = df["DATE"].max() if "DATE" in df.columns else pd.NaT
    if pd.notna(max_date):
        return pd.Timestamp(max_date)
    return pd.Timestamp(datetime.today().date())


def _build_title_key(df: pd.DataFrame) -> pd.Series:
    sign = df.get("TITUL_SIGN_FULL", pd.Series("", index=df.index)).fillna("").astype(str)
    name = df.get("TITUL_NAZEV", pd.Series("", index=df.index)).fillna("").astype(str)
    return sign + "||" + name


def extract_signature_prefix(signature_series: pd.Series, prefix_len: int = 2) -> pd.Series:
    """Vrati prefix signatury (napr. 'JD' z 'JD 30652')."""
    cleaned = signature_series.fillna("").astype(str).str.strip().str.upper()
    # Vezmeme prvni blok pismen, fallback je prvni cast pred mezerou.
    letters = cleaned.str.extract(r"^([A-Z]+)", expand=False).fillna("")
    fallback = cleaned.str.split().str[0].fillna("")
    base = letters.where(letters != "", fallback)
    return base.str[:prefix_len]


def filter_to_relevant_window(
    df: pd.DataFrame,
    years_window: int,
    action_types_for_loans: List[str],
) -> Tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    """Filtruje data na sledovane obdobi a vybrane akce."""
    end_date = get_reference_end_date(df)
    start_date = end_date - pd.DateOffset(years=years_window)

    window_df = df.copy()
    if "DATE" in window_df.columns and window_df["DATE"].notna().any():
        window_df = window_df[(window_df["DATE"] >= start_date) & (window_df["DATE"] <= end_date)]
    elif "YEAR" in window_df.columns and window_df["YEAR"].notna().any():
        window_df = window_df[
            (window_df["YEAR"] >= int(start_date.year)) & (window_df["YEAR"] <= int(end_date.year))
        ]

    if action_types_for_loans:
        window_df = window_df[window_df["ACTION_TYPE"].isin([a.lower().strip() for a in action_types_for_loans])]
    return window_df, pd.Timestamp(start_date), pd.Timestamp(end_date)


def identify_exceptions(
    titles_df: pd.DataFrame,
    exception_keywords: Dict[str, List[str]],
    exceptions_enabled: bool,
) -> pd.Series:
    """Detekuje vyjimky podle konfigurovatelnych klicovych slov."""
    if not exceptions_enabled:
        return pd.Series(False, index=titles_df.index)

    flag = pd.Series(False, index=titles_df.index)
    for col, keywords in exception_keywords.items():
        if col not in titles_df.columns:
            continue
        txt = titles_df[col].fillna("").astype(str).str.lower()
        col_flag = pd.Series(False, index=titles_df.index)
        for kw in keywords:
            kw = kw.strip().lower()
            if kw:
                col_flag = col_flag | txt.str.contains(kw, na=False)
        flag = flag | col_flag
    return flag


def compute_title_metrics(
    source_df: pd.DataFrame,
    window_df: pd.DataFrame,
    signature_col: str = "TITUL_SIGN_FULL",
) -> pd.DataFrame:
    """Spocita metriky na uroven titulu v ramci signatury."""
    all_titles = source_df.copy()
    all_titles["SIGN_PREFIX"] = extract_signature_prefix(all_titles[signature_col])
    all_titles["title_key"] = _build_title_key(all_titles)
    window_titles = window_df.copy()
    window_titles["SIGN_PREFIX"] = extract_signature_prefix(window_titles[signature_col])
    window_titles["title_key"] = _build_title_key(window_titles)

    group_cols = ["title_key", signature_col, "SIGN_PREFIX", "TITUL_NAZEV"]
    meta_cols = ["TITUL_DRUH_DOKUMENTU", "TITUL_JAZYK", "TITUL_ROK_VYDANI", "desk_subjects"]
    existing_meta = [c for c in meta_cols if c in all_titles.columns]

    base = all_titles[group_cols + existing_meta].drop_duplicates(subset=["title_key"]).set_index("title_key")
    loans_5y = window_titles.groupby("title_key").size().rename("vypujcky_5_let")
    last_date = window_titles.groupby("title_key")["DATE"].max().rename("datum_posledni_vypujcky")

    result = base.join(loans_5y, how="left").join(last_date, how="left").reset_index()
    result["vypujcky_5_let"] = result["vypujcky_5_let"].fillna(0).astype(int)

    result["rank_v_signature"] = result.groupby("SIGN_PREFIX")["vypujcky_5_let"].rank(
        method="min", ascending=False
    )
    result["pocet_v_signature"] = result.groupby("SIGN_PREFIX")["title_key"].transform("count")
    result["percentil_v_signature"] = (
        (result["pocet_v_signature"] - result["rank_v_signature"])
        / result["pocet_v_signature"].replace(0, np.nan)
        * 100
    ).fillna(0)
    result["relativni_pozice_v_signature"] = (
        result["rank_v_signature"].astype(int).astype(str)
        + "/"
        + result["pocet_v_signature"].astype(int).astype(str)
    )

    reference_end = get_reference_end_date(source_df)
    result["roky_od_posledni_vypujcky"] = (
        (reference_end - result["datum_posledni_vypujcky"]).dt.days / 365.25
    ).round(2)
    result.loc[result["datum_posledni_vypujcky"].isna(), "roky_od_posledni_vypujcky"] = np.nan

    result["rizikove_skore"] = (
        (result["vypujcky_5_let"] == 0).astype(int) * 70
        + (result["vypujcky_5_let"].between(1, 2)).astype(int) * 20
        + (result["percentil_v_signature"] <= 30).astype(int) * 10
        + (result["roky_od_posledni_vypujcky"].fillna(99) > 3).astype(int) * 15
    )
    return result


def _exception_reason(row: pd.Series) -> str:
    text = " ".join(
        [
            str(row.get("TITUL_DRUH_DOKUMENTU", "")),
            str(row.get("desk_subjects", "")),
            str(row.get("TITUL_SIGN_FULL", "")),
        ]
    ).lower()
    for kw in ["beletrie", "poezie", "drama", "pragensia", "praha", "prazske"]:
        if kw in text:
            return f"chraneno vyjimkou: {kw}"
    return "chraneno vyjimkou"


def classify_titles(
    titles_df: pd.DataFrame,
    low_loan_min: int,
    low_loan_max: int,
    bottom_percentile_threshold: float,
    stale_years_threshold: float,
) -> pd.DataFrame:
    """Klasifikace titulu do 4 kategorii + duvod_oznaceni."""
    out = titles_df.copy()
    out["klasifikace"] = "PONECHAT"
    out["duvod_oznaceni"] = "recentni vypujcka nebo lepsi pozice v signature"

    is_exception = out["vyjimka_flag"] == True  # noqa: E712
    out.loc[is_exception, "klasifikace"] = "CHRANENO_VYJIMKOU"
    out.loc[is_exception, "duvod_oznaceni"] = out.loc[is_exception].apply(_exception_reason, axis=1)

    auto_mask = (~is_exception) & (out["vypujcky_5_let"] == 0)
    out.loc[auto_mask, "klasifikace"] = "AUTO_KANDIDAT"
    out.loc[auto_mask, "duvod_oznaceni"] = "0 vypujcek za poslednich 5 let"

    manual_low = (
        (~is_exception)
        & out["vypujcky_5_let"].between(low_loan_min, low_loan_max)
        & (out["percentil_v_signature"] <= bottom_percentile_threshold)
    )
    manual_stale = (~is_exception) & (out["roky_od_posledni_vypujcky"].fillna(999) > stale_years_threshold)
    out.loc[manual_low | manual_stale, "klasifikace"] = "RUCNI_POSOUZENI"
    out.loc[manual_low, "duvod_oznaceni"] = (
        f"nizka vypujcnost ({low_loan_min}-{low_loan_max}) a spodni {bottom_percentile_threshold:.0f}% signatury"
    )
    out.loc[manual_stale, "duvod_oznaceni"] = f"posledni vypujcka starsi nez {stale_years_threshold:.1f} roku"
    return out


def aggregate_by_signature(classified_df: pd.DataFrame, signature_col: str = "SIGN_PREFIX") -> pd.DataFrame:
    """Agregace po prefixu signatury s rozpadem na kategorie."""
    summary = (
        classified_df.groupby([signature_col, "klasifikace"])["title_key"]
        .nunique()
        .unstack(fill_value=0)
        .reset_index()
    )
    for col in ["AUTO_KANDIDAT", "RUCNI_POSOUZENI", "CHRANENO_VYJIMKOU", "PONECHAT"]:
        if col not in summary.columns:
            summary[col] = 0
    summary["CELKEM_TITULU"] = (
        summary["AUTO_KANDIDAT"] + summary["RUCNI_POSOUZENI"] + summary["CHRANENO_VYJIMKOU"] + summary["PONECHAT"]
    )
    return summary.sort_values("AUTO_KANDIDAT", ascending=False)

