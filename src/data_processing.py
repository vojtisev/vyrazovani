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
import pyarrow.parquet as pq

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


def resolve_parquet_source_to_local_file(
    parquet_path: str,
    *,
    storage_options: Optional[Dict[str, Any]] = None,
    force_download: bool = False,
) -> Path:
    """Vrati lokalni cestu k Parquetu.

    - pro https://... stahne do cache souboru a vrati cestu
    - pro s3://... zatim vyzaduje prime cteni (nepouziva se v cloud-safe toku)
    - pro lokalni cestu jen overi existenci
    """
    raw = parquet_path.strip()
    if not raw:
        raise ValueError("Prazdna cesta k Parquet.")
    if _is_http_url(raw):
        return _download_url_to_cache_file(raw, force=force_download)
    if _is_s3_uri(raw):
        # S3 se cte primo pres pandas/pyarrow (fsspec); pro cloud-safe by bylo lepsi
        # mit predem vygenerovany HTTPS link nebo mirne upravit logiku.
        raise ValueError("Zdroj s3:// neni v tomto rezimu podporovan. Pouzij https:// nebo lokalni soubor.")
    path = Path(raw).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Soubor neexistuje: {path}")
    return path.resolve()


def is_precomputed_titles_metrics_parquet(parquet_file: Path) -> bool:
    """Pozna, zda je Parquet uz na urovni titulu (vystup builderu), ne raw vypujcky."""
    schema = pq.read_schema(parquet_file)
    names = set(schema.names)
    required = {"title_key", "SIGN_PREFIX", "vypujcky_5_let"}
    return required.issubset(names) and "ACTION_TYPE" not in names and "DATE" not in names


def get_distinct_action_types_from_parquet(parquet_file: Path) -> List[str]:
    """Vsechny DISTINCT hodnoty ACTION_TYPE (text z OPIDY) pro filtr v UI."""
    path_sql = str(parquet_file.resolve()).replace("'", "''")
    con = duckdb.connect(database=":memory:")
    try:
        one = con.execute(f"SELECT * FROM read_parquet('{path_sql}') LIMIT 1").fetchdf()
    except Exception:
        return []
    if one.empty or "ACTION_TYPE" not in one.columns:
        return []
    try:
        df = con.execute(
            f"""
            SELECT DISTINCT TRIM(CAST(ACTION_TYPE AS VARCHAR)) AS a
            FROM read_parquet('{path_sql}')
            WHERE ACTION_TYPE IS NOT NULL AND TRIM(CAST(ACTION_TYPE AS VARCHAR)) <> ''
            ORDER BY 1
            """
        ).fetchdf()
    except Exception:
        return []
    return [str(x).strip() for x in df["a"].tolist() if str(x).strip()]


def get_distinct_opids_from_parquet(parquet_file: Path, *, max_opids: int = 64) -> List[int]:
    """DISTINCT VYP_OPID z enriched Parquetu (pro agregace po OPID)."""
    try:
        names = set(pq.read_schema(parquet_file).names)
    except Exception:
        return []
    if "VYP_OPID" not in names:
        return []
    path_sql = str(parquet_file.resolve()).replace("'", "''")
    con = duckdb.connect(database=":memory:")
    try:
        df = con.execute(
            f"""
            SELECT DISTINCT TRY_CAST(VYP_OPID AS BIGINT) AS opid
            FROM read_parquet('{path_sql}')
            WHERE VYP_OPID IS NOT NULL
            ORDER BY 1
            """
        ).fetchdf()
    except Exception:
        return []
    out: List[int] = []
    for x in df["opid"].tolist():
        try:
            if x is None or (isinstance(x, float) and np.isnan(x)):
                continue
            out.append(int(x))
        except Exception:
            continue
    return out[:max_opids]


def get_opid_legend_from_parquet(parquet_file: Path) -> pd.DataFrame:
    """Mapa OPID -> text (OPIDY_POPIS) pro legendu ve Streamlitu."""
    try:
        names = set(pq.read_schema(parquet_file).names)
    except Exception:
        return pd.DataFrame()
    if "VYP_OPID" not in names:
        return pd.DataFrame()
    path_sql = str(parquet_file.resolve()).replace("'", "''")
    has_popis = "OPIDY_POPIS" in names
    popis_expr = "MAX(TRIM(CAST(OPIDY_POPIS AS VARCHAR)))" if has_popis else "CAST(NULL AS VARCHAR)"
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(
            f"""
            SELECT
              TRY_CAST(VYP_OPID AS BIGINT) AS opid,
              {popis_expr} AS popis
            FROM read_parquet('{path_sql}')
            WHERE VYP_OPID IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchdf()
    except Exception:
        return pd.DataFrame()


def suggest_loan_action_types_from_parquet(parquet_file: Path) -> List[str]:
    """Vychozi vyber typu operaci: ACTION_TYPE kde IS_LOAN nebo ACTION_GROUP='loan'."""
    path_sql = str(parquet_file.resolve()).replace("'", "''")
    con = duckdb.connect(database=":memory:")
    try:
        one = con.execute(f"SELECT * FROM read_parquet('{path_sql}') LIMIT 1").fetchdf()
    except Exception:
        return []
    if one.empty or "ACTION_TYPE" not in one.columns:
        return []
    has_loan = "IS_LOAN" in one.columns
    has_group = "ACTION_GROUP" in one.columns
    if not has_loan and not has_group:
        return []
    loan_cond = "FALSE"
    if has_loan:
        loan_cond += " OR TRY_CAST(IS_LOAN AS BOOLEAN) = TRUE"
    if has_group:
        loan_cond += " OR LOWER(TRIM(CAST(ACTION_GROUP AS VARCHAR))) = 'loan'"
    try:
        df = con.execute(
            f"""
            SELECT DISTINCT TRIM(CAST(ACTION_TYPE AS VARCHAR)) AS a
            FROM read_parquet('{path_sql}')
            WHERE ({loan_cond})
              AND ACTION_TYPE IS NOT NULL
              AND TRIM(CAST(ACTION_TYPE AS VARCHAR)) <> ''
            ORDER BY 1
            """
        ).fetchdf()
    except Exception:
        return []
    return [str(x).strip() for x in df["a"].tolist() if str(x).strip()]


def compute_title_metrics_from_parquet(
    parquet_file: Path,
    *,
    years_window: int,
    action_types_for_loans: List[str],
    sign_prefix: Optional[str] = None,
    svazky_parquet_file: Optional[Path] = None,
    signature_col: str = "TITUL_SIGN_FULL",
    prefix_len: int = 2,
) -> pd.DataFrame:
    """Cloud-friendly varianta: agregace po titulu pres DuckDB bez nacteni celeho DF do pandas.

    - `vypujcky_5_let` / `vypujcky_okno`: pocet radku v okne podle vybraneho filtru ACTION_TYPE (ci IS_LOAN),
      stejne jako drive — pro rizikove skore a „vykon“.
    - `pocet_operaci_v_obdobi`, `pocet_vypujcek_is_loan`, `pocet_vraceni_is_return`, `pocet_opid_<id>`:
      vzdy z celeho casoveho okna (vsechny operacni radky v obdobi; nefiltruje se podle multiselectu ACTION_TYPE),
      aby byly pocty podle OPID oddelene a neslucovane do jednoho cisla.
    """
    loan_actions = [a.lower().strip() for a in action_types_for_loans if str(a).strip()]
    def _sql_quote(s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    action_list_sql = ", ".join([_sql_quote(a) for a in loan_actions]) or "''"
    if loan_actions:
        # action_type je v src jiz lower(trim(ACTION_TYPE))
        loan_filter_sql = f"action_type IN ({action_list_sql})"
    else:
        # Zpetna kompatibilita: kdyz nezadano, pouzij IS_LOAN / ACTION_GROUP.
        loan_filter_sql = "(is_loan_flag = TRUE) OR (action_group = 'loan')"

    try:
        schema_names = set(pq.read_schema(parquet_file).names)
    except Exception:
        schema_names = set()

    vyp_opid_expr = "TRY_CAST(VYP_OPID AS BIGINT) AS vyp_opid" if "VYP_OPID" in schema_names else "CAST(NULL AS BIGINT) AS vyp_opid"
    is_loan_sql = (
        "TRY_CAST(IS_LOAN AS BOOLEAN) AS is_loan_flag"
        if "IS_LOAN" in schema_names
        else "FALSE AS is_loan_flag"
    )
    if "IS_RETURN" in schema_names:
        is_return_expr = "COALESCE(TRY_CAST(IS_RETURN AS BOOLEAN), FALSE) AS is_return_flag"
    elif "ACTION_GROUP" in schema_names:
        is_return_expr = "(LOWER(TRIM(CAST(ACTION_GROUP AS VARCHAR))) = 'return') AS is_return_flag"
    else:
        is_return_expr = "FALSE AS is_return_flag"

    opids = get_distinct_opids_from_parquet(parquet_file) if "VYP_OPID" in schema_names else []
    opid_sum_parts: List[str] = []
    opid_coalesce_parts: List[str] = []
    for oid in opids:
        opid_sum_parts.append(
            f"SUM(CASE WHEN vyp_opid = {int(oid)} THEN 1 ELSE 0 END)::BIGINT AS pocet_opid_{int(oid)}"
        )
        opid_coalesce_parts.append(f"COALESCE(oa.pocet_opid_{int(oid)}, 0) AS pocet_opid_{int(oid)}")

    op_agg_select_extra = ""
    op_metrics_coalesce = ""
    if opid_sum_parts:
        op_agg_select_extra = ",\n    " + ",\n    ".join(opid_sum_parts)
        op_metrics_coalesce = ",\n    " + ",\n    ".join(opid_coalesce_parts)

    op_agg_cte = f"""
op_agg AS (
  SELECT
    signature || '||' || title_name AS title_key,
    COUNT(*)::BIGINT AS pocet_operaci_v_obdobi,
    SUM(CASE WHEN is_loan_flag THEN 1 ELSE 0 END)::BIGINT AS pocet_vypujcek_is_loan,
    SUM(CASE WHEN is_return_flag THEN 1 ELSE 0 END)::BIGINT AS pocet_vraceni_is_return
    {op_agg_select_extra}
  FROM window_src
  GROUP BY 1
)
"""
    op_metrics_extra = (
        """
    COALESCE(oa.pocet_operaci_v_obdobi, 0) AS pocet_operaci_v_obdobi,
    COALESCE(oa.pocet_vypujcek_is_loan, 0) AS pocet_vypujcek_is_loan,
    COALESCE(oa.pocet_vraceni_is_return, 0) AS pocet_vraceni_is_return"""
        + (op_metrics_coalesce if op_metrics_coalesce else "")
    )

    op_breakdown_lines = [
        "  CAST(pocet_operaci_v_obdobi AS BIGINT) AS pocet_operaci_v_obdobi",
        "  CAST(pocet_vypujcek_is_loan AS BIGINT) AS pocet_vypujcek_is_loan",
        "  CAST(pocet_vraceni_is_return AS BIGINT) AS pocet_vraceni_is_return",
    ]
    for oid in opids:
        op_breakdown_lines.append(f"  CAST(pocet_opid_{int(oid)} AS BIGINT) AS pocet_opid_{int(oid)}")
    op_breakdown_select = ",\n".join(op_breakdown_lines) + ",\n"

    # DuckDB: nacte jen potrebne sloupce a spocte agregace + ranky v signature.
    # Pozn.: desk_subjects muze byt list -> bereme jako VARCHAR.
    path_sql = str(parquet_file).replace("'", "''")
    svazky_sql = (
        str(svazky_parquet_file).replace("'", "''") if svazky_parquet_file is not None else ""
    )
    sign_prefix_filter = ""
    if sign_prefix and str(sign_prefix).strip() and str(sign_prefix).strip() != "(vsechny)":
        sp = str(sign_prefix).strip().upper().replace("'", "''")
        sign_prefix_filter = f"WHERE SIGN_PREFIX = '{sp}'"

    desk_src_sql = (
        "CAST(desk_subjects AS VARCHAR) AS desk_subjects"
        if "desk_subjects" in schema_names
        else "CAST('' AS VARCHAR) AS desk_subjects"
    )
    och_src_sql = (
        "CAST(och_subjects AS VARCHAR) AS och_subjects"
        if "och_subjects" in schema_names
        else "CAST('' AS VARCHAR) AS och_subjects"
    )

    # Pocet svazku:
    # - pokud mame externi soubor se svazky: COUNT(DISTINCT svazky_key) per svazky_ptr_titul (TITUL_KEY)
    # - jinak fallback: COUNT(DISTINCT SVAZKY_KEY) z raw Parquetu (videne ve vypujckach / udalostech)
    svazky_cte = ""
    svazky_join = ""
    if svazky_parquet_file is not None:
        svazky_cte = f"""
,svazky_cnt AS (
  SELECT
    TRY_CAST(SVAZKY_PTR_TITUL AS BIGINT) AS TITUL_KEY,
    COUNT(DISTINCT TRY_CAST(SVAZKY_KEY AS BIGINT)) AS pocet_svazku
  FROM read_parquet('{svazky_sql}')
  GROUP BY 1
)
"""
        svazky_join = "LEFT JOIN svazky_cnt sc ON sc.TITUL_KEY = m.TITUL_KEY"
    else:
        svazky_cte = """
,svazky_cnt AS (
  SELECT
    TITUL_KEY,
    COUNT(DISTINCT TRY_CAST(SVAZKY_KEY AS BIGINT)) AS pocet_svazku
  FROM src
  WHERE TITUL_KEY IS NOT NULL
  GROUP BY 1
)
"""
        svazky_join = "LEFT JOIN svazky_cnt sc ON sc.TITUL_KEY = m.TITUL_KEY"

    query = f"""
WITH src AS (
  SELECT
    TRY_CAST(TITUL_KEY AS BIGINT) AS TITUL_KEY,
    CAST({signature_col} AS VARCHAR) AS signature,
    CAST(TITUL_NAZEV AS VARCHAR) AS title_name,
    CAST(TITUL_DRUH_DOKUMENTU AS VARCHAR) AS doc_type,
    CAST(TITUL_JAZYK AS VARCHAR) AS lang,
    TRY_CAST(TITUL_ROK_VYDANI AS DOUBLE) AS year_pub,
    {desk_src_sql},
    {och_src_sql},
    TRY_CAST(DATE AS TIMESTAMP) AS dt,
    TRY_CAST(YEAR AS INTEGER) AS yr,
    LOWER(TRIM(CAST(ACTION_TYPE AS VARCHAR))) AS action_type,
    LOWER(TRIM(CAST(ACTION_GROUP AS VARCHAR))) AS action_group,
    {is_loan_sql},
    {vyp_opid_expr},
    {is_return_expr},
    TRY_CAST(SVAZKY_KEY AS BIGINT) AS SVAZKY_KEY
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
{op_agg_cte},
window_loans AS (
  SELECT *
  FROM window_src
  WHERE {loan_filter_sql}
),
base_titles AS (
  SELECT
    signature || '||' || title_name AS title_key,
    ANY_VALUE(TITUL_KEY) AS TITUL_KEY,
    signature AS TITUL_SIGN_FULL,
    UPPER(SUBSTR(COALESCE(NULLIF(REGEXP_EXTRACT(TRIM(signature), '^([A-Za-z]+)', 1), ''), SPLIT_PART(TRIM(signature), ' ', 1)), 1, {int(prefix_len)})) AS SIGN_PREFIX,
    title_name AS TITUL_NAZEV,
    ANY_VALUE(doc_type) AS TITUL_DRUH_DOKUMENTU,
    ANY_VALUE(lang) AS TITUL_JAZYK,
    ANY_VALUE(year_pub) AS TITUL_ROK_VYDANI,
    ANY_VALUE(desk_subjects) AS desk_subjects,
    ANY_VALUE(och_subjects) AS och_subjects
  FROM src
  GROUP BY 1, 3, 4, 5
),
metrics AS (
  SELECT
    b.*,
    COALESCE(l.loan_cnt, 0) AS vypujcky_5_let,
    l.last_dt AS datum_posledni_vypujcky,
    r.end_dt AS reference_end,
{op_metrics_extra}
  FROM base_titles b
  LEFT JOIN (
    SELECT
      signature || '||' || title_name AS title_key,
      COUNT(*) AS loan_cnt,
      MAX(dt) AS last_dt
    FROM window_loans
    GROUP BY 1
  ) l ON b.title_key = l.title_key
  LEFT JOIN op_agg oa ON oa.title_key = b.title_key
  CROSS JOIN ref r
)
{svazky_cte}
,metrics2 AS (
  SELECT
    m.*,
    COALESCE(sc.pocet_svazku, 0) AS pocet_svazku
  FROM metrics m
  {svazky_join}
),
ranked AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY SIGN_PREFIX ORDER BY vypujcky_5_let DESC) AS rank_v_signature,
    COUNT(*) OVER (PARTITION BY SIGN_PREFIX) AS pocet_v_signature
  FROM metrics2
)
SELECT
  title_key,
  TITUL_KEY,
  TITUL_SIGN_FULL,
  SIGN_PREFIX,
  TITUL_NAZEV,
  TITUL_DRUH_DOKUMENTU,
  TITUL_JAZYK,
  TITUL_ROK_VYDANI,
  desk_subjects,
  och_subjects,
{op_breakdown_select}
  CAST(vypujcky_5_let AS BIGINT) AS vypujcky_5_let,
  CAST(pocet_svazku AS BIGINT) AS pocet_svazku,
  COALESCE(CAST(vypujcky_5_let AS DOUBLE) / NULLIF(CAST(pocet_svazku AS DOUBLE), 0), 0) AS vykon_na_svazek,
  datum_posledni_vypujcky,
  CAST(rank_v_signature AS BIGINT) AS rank_v_signature,
  CAST(pocet_v_signature AS BIGINT) AS pocet_v_signature,
  COALESCE(((pocet_v_signature - rank_v_signature) * 100.0) / NULLIF(pocet_v_signature, 0), 0) AS percentil_v_signature,
  CAST(rank_v_signature AS BIGINT)::VARCHAR || '/' || CAST(pocet_v_signature AS BIGINT)::VARCHAR AS relativni_pozice_v_signature,
  ROUND(DATE_DIFF('day', datum_posledni_vypujcky, reference_end) / 365.25, 2) AS roky_od_posledni_vypujcky
FROM ranked
{sign_prefix_filter}
"""
    con = duckdb.connect(database=":memory:")
    try:
        out = con.execute(query).fetchdf()
    finally:
        con.close()

    if "pocet_udalosti_okno" in out.columns and "pocet_operaci_v_obdobi" not in out.columns:
        out = out.rename(columns={"pocet_udalosti_okno": "pocet_operaci_v_obdobi"})

    out["vypujcky_5_let"] = out["vypujcky_5_let"].fillna(0).astype(int)
    # Neutrální pojmenování (aby okno nebylo v názvu sloupce).
    out["okno_let"] = int(years_window)
    out["vypujcky_okno"] = out["vypujcky_5_let"]
    out["pocet_svazku"] = out["pocet_svazku"].fillna(0).astype(int)
    out["vykon_na_svazek"] = pd.to_numeric(out["vykon_na_svazek"], errors="coerce").fillna(0.0)

    for _c in list(out.columns):
        if _c.startswith("pocet_opid_") or _c in (
            "pocet_operaci_v_obdobi",
            "pocet_vypujcek_is_loan",
            "pocet_vraceni_is_return",
        ):
            out[_c] = pd.to_numeric(out[_c], errors="coerce").fillna(0).astype(int)

    out["bez_vypujcek"] = out["vypujcky_5_let"] == 0
    out["bez_svazku"] = out["pocet_svazku"] == 0
    # Rizikove skore zachovame stejne jako pandas varianta
    out["rizikove_skore"] = (
        (out["vypujcky_5_let"] == 0).astype(int) * 70
        + (out["vypujcky_5_let"].between(1, 2)).astype(int) * 20
        + (out["percentil_v_signature"] <= 30).astype(int) * 10
        + (out["roky_od_posledni_vypujcky"].fillna(99) > 3).astype(int) * 15
    )
    return out


def get_sign_prefix_summary_from_parquet(
    parquet_file: Path,
    *,
    signature_col: str = "TITUL_SIGN_FULL",
    prefix_len: int = 2,
) -> pd.DataFrame:
    """Vrati prehled prefixu signatur (pocet titulu) z Parquetu bez nacteni do pandas."""
    path_sql = str(parquet_file).replace("'", "''")
    query = f"""
WITH src AS (
  SELECT
    CAST({signature_col} AS VARCHAR) AS signature,
    CAST(TITUL_NAZEV AS VARCHAR) AS title_name
  FROM read_parquet('{path_sql}')
),
base_titles AS (
  SELECT
    signature || '||' || title_name AS title_key,
    UPPER(SUBSTR(COALESCE(NULLIF(REGEXP_EXTRACT(TRIM(signature), '^([A-Za-z]+)', 1), ''), SPLIT_PART(TRIM(signature), ' ', 1)), 1, {int(prefix_len)})) AS SIGN_PREFIX
  FROM src
  GROUP BY 1, 2
)
SELECT
  SIGN_PREFIX,
  COUNT(*) AS TITULU
FROM base_titles
GROUP BY 1
ORDER BY TITULU DESC
"""
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(query).fetchdf()
    finally:
        con.close()


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
    for col in ["TITUL_NAZEV", "TITUL_SIGN_FULL", "TITUL_JAZYK", "TITUL_DRUH_DOKUMENTU", "desk_subjects", "och_subjects"]:
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
    *,
    loan_filter: bool = True,
) -> Tuple[pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    """Filtruje data na sledovane obdobi a volitelne jen vybrane akce (vypujcky).

    loan_filter=False: pouze casove okno (vsechny operacni radky ve zvolenem obdobi) — pro rozpad podle OPID stejne jako DuckDB.
    """
    end_date = get_reference_end_date(df)
    start_date = end_date - pd.DateOffset(years=years_window)

    window_df = df.copy()
    if "DATE" in window_df.columns and window_df["DATE"].notna().any():
        window_df = window_df[(window_df["DATE"] >= start_date) & (window_df["DATE"] <= end_date)]
    elif "YEAR" in window_df.columns and window_df["YEAR"].notna().any():
        window_df = window_df[
            (window_df["YEAR"] >= int(start_date.year)) & (window_df["YEAR"] <= int(end_date.year))
        ]

    if not loan_filter:
        return window_df, pd.Timestamp(start_date), pd.Timestamp(end_date)

    if "IS_LOAN" in window_df.columns:
        loan_mask = window_df["IS_LOAN"].astype("boolean").fillna(False)
        window_df = window_df[loan_mask]
    elif "ACTION_GROUP" in window_df.columns:
        window_df = window_df[window_df["ACTION_GROUP"].fillna("").astype(str).str.lower().eq("loan")]
    elif action_types_for_loans:
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
    *,
    window_all_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Spocita metriky na uroven titulu v ramci signatury.

    window_all_df: volitelne vsechny operacni radky ve zvolenem obdobi (vcetne vraceni apod.) pro rozpad OPID.
    """
    all_titles = source_df.copy()
    all_titles["SIGN_PREFIX"] = extract_signature_prefix(all_titles[signature_col])
    all_titles["title_key"] = _build_title_key(all_titles)
    window_titles = window_df.copy()
    window_titles["SIGN_PREFIX"] = extract_signature_prefix(window_titles[signature_col])
    window_titles["title_key"] = _build_title_key(window_titles)

    group_cols = ["title_key", signature_col, "SIGN_PREFIX", "TITUL_NAZEV"]
    meta_cols = ["TITUL_DRUH_DOKUMENTU", "TITUL_JAZYK", "TITUL_ROK_VYDANI", "desk_subjects", "och_subjects"]
    existing_meta = [c for c in meta_cols if c in all_titles.columns]

    extra_cols: List[str] = []
    if "TITUL_KEY" in all_titles.columns:
        extra_cols.append("TITUL_KEY")
    base = (
        all_titles[group_cols + existing_meta + extra_cols]
        .drop_duplicates(subset=["title_key"])
        .set_index("title_key")
    )
    loans_5y = window_titles.groupby("title_key").size().rename("vypujcky_5_let")
    last_date = window_titles.groupby("title_key")["DATE"].max().rename("datum_posledni_vypujcky")

    result = base.join(loans_5y, how="left").join(last_date, how="left").reset_index()
    result["vypujcky_5_let"] = result["vypujcky_5_let"].fillna(0).astype(int)

    if window_all_df is not None and not window_all_df.empty:
        wa = window_all_df.copy()
        wa["SIGN_PREFIX"] = extract_signature_prefix(wa[signature_col])
        wa["title_key"] = _build_title_key(wa)
        pocet_all = wa.groupby("title_key").size().rename("pocet_operaci_v_obdobi")
        result = result.merge(pocet_all.reset_index(), on="title_key", how="left")
        result["pocet_operaci_v_obdobi"] = result["pocet_operaci_v_obdobi"].fillna(0).astype(int)
        if "IS_LOAN" in wa.columns:
            mloan = wa["IS_LOAN"].fillna(False).astype(bool)
            pl = wa.loc[mloan].groupby("title_key").size().rename("pocet_vypujcek_is_loan")
            result = result.merge(pl.reset_index(), on="title_key", how="left")
            result["pocet_vypujcek_is_loan"] = result["pocet_vypujcek_is_loan"].fillna(0).astype(int)
        if "IS_RETURN" in wa.columns:
            mret = wa["IS_RETURN"].fillna(False).astype(bool)
            pr = wa.loc[mret].groupby("title_key").size().rename("pocet_vraceni_is_return")
            result = result.merge(pr.reset_index(), on="title_key", how="left")
            result["pocet_vraceni_is_return"] = result["pocet_vraceni_is_return"].fillna(0).astype(int)
        if "VYP_OPID" in wa.columns:
            wa["_opid"] = pd.to_numeric(wa["VYP_OPID"], errors="coerce")
            for oid in sorted(wa["_opid"].dropna().unique()):
                try:
                    o = int(oid)
                except Exception:
                    continue
                cnt = wa.loc[wa["_opid"] == oid].groupby("title_key").size().rename(f"pocet_opid_{o}")
                result = result.merge(cnt.reset_index(), on="title_key", how="left")
                result[f"pocet_opid_{o}"] = result[f"pocet_opid_{o}"].fillna(0).astype(int)
    # Neutrální pojmenování (aby okno nebylo v názvu sloupce).
    # V pandas toku neznáme přímo počet let z argumentu; okno je dané filtrovaným `window_df`.
    # Sloupec necháme pro konzistenci, ale hodnotu vyplní UI (Streamlit) podle nastavení.
    result["okno_let"] = np.nan
    result["vypujcky_okno"] = result["vypujcky_5_let"]

    if "SVAZKY_KEY" in all_titles.columns and "TITUL_KEY" in all_titles.columns:
        sv = (
            all_titles.dropna(subset=["TITUL_KEY"])
            .groupby("TITUL_KEY")["SVAZKY_KEY"]
            .nunique()
            .rename("pocet_svazku")
            .reset_index()
        )
        result = result.merge(sv, on="TITUL_KEY", how="left")
        result["pocet_svazku"] = result["pocet_svazku"].fillna(0).astype(int)
        result["vykon_na_svazek"] = (
            result["vypujcky_5_let"] / result["pocet_svazku"].replace(0, np.nan)
        ).fillna(0.0)
        result["bez_svazku"] = result["pocet_svazku"] == 0
    else:
        result["pocet_svazku"] = 0
        result["vykon_na_svazek"] = 0.0
        result["bez_svazku"] = True
    result["bez_vypujcek"] = result["vypujcky_5_let"] == 0

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
            str(row.get("och_subjects", "")),
            str(row.get("TITUL_SIGN_FULL", "")),
            str(row.get("TITUL_NAZEV", "")),
        ]
    ).lower()
    for kw in ["beletrie", "poezie", "drama", "pragensia", "praha", "prazske", "pražsk"]:
        if kw in text:
            return f"chráněno výjimkou: {kw}"
    return "chráněno výjimkou (shoda s pravidlem)"


def classify_titles(
    titles_df: pd.DataFrame,
    low_loan_min: int,
    low_loan_max: int,
    bottom_percentile_threshold: float,
    stale_years_threshold: float,
    *,
    auto_max_loans: int = 0,
) -> pd.DataFrame:
    """Klasifikace titulu do 4 kategorii + duvod_oznaceni.

    auto_max_loans: tituly s vypujcky_5_let <= teto hodnote (a bez vyjimky) jdou do AUTO_KANDIDAT,
    pokud na ne pozdeji nesedi RUCNI_POSOUZENI.
    """
    out = titles_df.copy()
    out["klasifikace"] = "PONECHAT"
    out["duvod_oznaceni"] = "nedostatečný důvod pro změnu stavu (běžný výskyt / lepší pozice ve signatuře)"

    is_exception = out["vyjimka_flag"] == True  # noqa: E712
    out.loc[is_exception, "klasifikace"] = "CHRANENO_VYJIMKOU"
    out.loc[is_exception, "duvod_oznaceni"] = out.loc[is_exception].apply(_exception_reason, axis=1)

    auto_mask = (~is_exception) & (out["vypujcky_5_let"] <= int(auto_max_loans))
    out.loc[auto_mask, "klasifikace"] = "AUTO_KANDIDAT"
    if int(auto_max_loans) <= 0:
        auto_reason = "0 výpůjček v okně analýzy (automatický kandidát)"
    else:
        auto_reason = (
            f"nejvýše {int(auto_max_loans)} výpůjček v okně analýzy (automatický kandidát)"
        )
    out.loc[auto_mask, "duvod_oznaceni"] = auto_reason

    manual_low = (
        (~is_exception)
        & out["vypujcky_5_let"].between(low_loan_min, low_loan_max)
        & (out["percentil_v_signature"] <= bottom_percentile_threshold)
    )
    manual_stale = (~is_exception) & (out["roky_od_posledni_vypujcky"].fillna(999) > stale_years_threshold)
    out.loc[manual_low | manual_stale, "klasifikace"] = "RUCNI_POSOUZENI"
    out.loc[manual_low, "duvod_oznaceni"] = (
        f"nízká výpůjčnost ({low_loan_min}–{low_loan_max}) a spodní {bottom_percentile_threshold:.0f} % ve signatuře"
    )
    out.loc[manual_stale, "duvod_oznaceni"] = f"poslední výpůjčka starší než {stale_years_threshold:.1f} roku"
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

