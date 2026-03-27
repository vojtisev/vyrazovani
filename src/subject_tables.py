"""Sestavení agregovaných textů deskriptorů a OCH (obsahová charakteristika) po TITUL_KEY.

Očekávané vstupy (Parquet z `build_parquet_from_esql_txt.py`):
- och.parquet, txoch.parquet — vazba titul ↔ text OCH přes ukazatele
- desk.parquet, deskt.parquet, txdesk.parquet — deskriptory a jejich texty

Názvy sloupců se berou z reálného schématu (první shoda z kandidátů).
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Sequence

import duckdb
import pyarrow.parquet as pq


def _parquet_columns(path: Path) -> List[str]:
    if not path.exists():
        return []
    try:
        return list(pq.read_schema(path).names)
    except Exception:
        return []


def _pick(cols: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    u = {c.upper(): c for c in cols}
    for cand in candidates:
        if cand.upper() in u:
            return u[cand.upper()]
    return None


def _esc_sql_path(p: Path) -> str:
    return str(p.resolve()).replace("'", "''")


def write_titul_subjects_parquet(derived_dir: Path) -> Optional[Path]:
    """Zapíše `titul_subjects.parquet` (TITUL_KEY, desk_subjects, och_subjects) nebo vrátí None."""
    derived_dir = derived_dir.expanduser().resolve()
    och_p = derived_dir / "och.parquet"
    txoch_p = derived_dir / "txoch.parquet"
    desk_p = derived_dir / "desk.parquet"
    deskt_p = derived_dir / "deskt.parquet"
    txdesk_p = derived_dir / "txdesk.parquet"

    out = derived_dir / "titul_subjects.parquet"
    if out.exists():
        out.unlink()

    och_sql = _build_och_agg(och_p, txoch_p)
    desk_sql = _build_desk_agg(desk_p, deskt_p, txdesk_p)

    if not och_sql and not desk_sql:
        return None

    con = duckdb.connect(database=":memory:")
    try:
        if och_sql and desk_sql:
            q = f"""
COPY (
  WITH
  {och_sql},
  {desk_sql},
  keys AS (
    SELECT TITUL_KEY FROM och_agg
    UNION
    SELECT TITUL_KEY FROM desk_agg
  ),
  merged AS (
    SELECT
      k.TITUL_KEY,
      COALESCE(o.och_subjects, '') AS och_subjects,
      COALESCE(d.desk_subjects, '') AS desk_subjects
    FROM keys k
    LEFT JOIN och_agg o ON o.TITUL_KEY = k.TITUL_KEY
    LEFT JOIN desk_agg d ON d.TITUL_KEY = k.TITUL_KEY
  )
  SELECT * FROM merged
) TO '{_esc_sql_path(out)}' (FORMAT PARQUET, COMPRESSION ZSTD)
"""
        elif och_sql:
            q = f"""
COPY (
  WITH
  {och_sql}
  SELECT TITUL_KEY, och_subjects, CAST('' AS VARCHAR) AS desk_subjects FROM och_agg
) TO '{_esc_sql_path(out)}' (FORMAT PARQUET, COMPRESSION ZSTD)
"""
        else:
            q = f"""
COPY (
  WITH
  {desk_sql}
  SELECT TITUL_KEY, CAST('' AS VARCHAR) AS och_subjects, desk_subjects FROM desk_agg
) TO '{_esc_sql_path(out)}' (FORMAT PARQUET, COMPRESSION ZSTD)
"""
        con.execute(q)
    finally:
        con.close()

    return out if out.exists() and out.stat().st_size > 0 else None


def _build_och_agg(och_p: Path, txoch_p: Path) -> str:
    if not och_p.exists() or not txoch_p.exists():
        return ""
    co = _parquet_columns(och_p)
    ctx = _parquet_columns(txoch_p)
    col_tit = _pick(co, ("OCH_PTR_TITUL", "PTR_TITUL", "OCH_TITUL_KEY"))
    col_txptr = _pick(
        co,
        (
            "OCH_PTR_TXOCH",
            "OCH_TXOCH_KEY",
            "OCH_PTR_OCH_TXOCH",
            "PTR_TXOCH",
            "TXOCH_KEY",
        ),
    )
    col_txk = _pick(ctx, ("TXOCH_KEY", "OCH_TXOCH_KEY"))
    col_txt = _pick(ctx, ("TXOCH_TEXT", "TXOCH_TXT", "TXOCH_POPIS", "TXOCH_TX_TEXT"))
    if not all([col_tit, col_txptr, col_txk, col_txt]):
        return ""
    op = _esc_sql_path(och_p)
    tp = _esc_sql_path(txoch_p)
    return f"""
och_agg AS (
  SELECT
    TRY_CAST(o.{col_tit} AS BIGINT) AS TITUL_KEY,
    STRING_AGG(DISTINCT CASE
      WHEN LENGTH(TRIM(COALESCE(CAST(tx.{col_txt} AS VARCHAR), ''))) > 0
      THEN TRIM(COALESCE(CAST(tx.{col_txt} AS VARCHAR), ''))
      ELSE NULL
    END, ' | ') AS och_subjects
  FROM read_parquet('{op}') o
  LEFT JOIN read_parquet('{tp}') tx
    ON TRY_CAST(tx.{col_txk} AS BIGINT) = TRY_CAST(o.{col_txptr} AS BIGINT)
  WHERE TRY_CAST(o.{col_tit} AS BIGINT) IS NOT NULL
  GROUP BY 1
)
"""


def _build_desk_agg(desk_p: Path, deskt_p: Path, txdesk_p: Path) -> str:
    if not desk_p.exists():
        return ""
    cd = _parquet_columns(desk_p)
    col_tit = _pick(cd, ("DESK_PTR_TITUL", "DESK_TITUL_KEY", "PTR_TITUL", "TITUL_KEY"))
    col_desk = _pick(
        cd,
        (
            "DESK_PTR_TXDESK",
            "DESK_TXDESK_KEY",
            "DESK_PTR_DESKT",
            "DESK_DESKT_KEY",
            "PTR_DESKT",
            "PTR_TXDESK",
            "TXDESK_KEY",
            "DESKT_KEY",
        ),
    )
    if not col_tit:
        return ""

    dp = _esc_sql_path(desk_p)
    text_selects: List[str] = []

    # Varianta A: desk přímo na txdesk (nejčastější)
    if txdesk_p.exists() and col_desk:
        ctx = _parquet_columns(txdesk_p)
        tx_txt = _pick(
            ctx,
            (
                "TXDESK_DESKRIPTOR",
                "TXDESK_TEXT",
                "TXDESK_TXT",
                "DES_TXT",
                "TXDESK_POPIS",
                "TEXT",
            ),
        )
        tx_key = _pick(
            ctx,
            ("TXDESK_KEY", "DESK_KEY", "DESKT_KEY", "TXDESK_PTR_DESKT", "PTR_DESKT"),
        )
        if tx_txt and tx_key:
            tp = _esc_sql_path(txdesk_p)
            text_selects.append(
                f"""
SELECT
  TRY_CAST(d.{col_tit} AS BIGINT) AS TITUL_KEY,
  TRIM(COALESCE(CAST(tx.{tx_txt} AS VARCHAR), '')) AS subject_text
FROM read_parquet('{dp}') d
LEFT JOIN read_parquet('{tp}') tx
  ON TRY_CAST(tx.{tx_key} AS BIGINT) = TRY_CAST(d.{col_desk} AS BIGINT)
WHERE TRY_CAST(d.{col_tit} AS BIGINT) IS NOT NULL
"""
            )

    # Varianta B: desk -> deskt -> txdesk
    if deskt_p.exists() and txdesk_p.exists() and col_desk:
        cs = _parquet_columns(deskt_p)
        ctx = _parquet_columns(txdesk_p)
        deskt_key = _pick(cs, ("DESKT_KEY", "DESK_KEY", "DESKT_ID"))
        tx_ptr = _pick(ctx, ("TXDESK_PTR_DESKT", "DESK_PTR_DESKT", "PTR_DESKT", "DESKT_KEY", "DESK_KEY"))
        tx_txt = _pick(
            ctx,
            (
                "TXDESK_DESKRIPTOR",
                "TXDESK_TEXT",
                "TXDESK_TXT",
                "DES_TXT",
                "TXDESK_POPIS",
                "TEXT",
            ),
        )
        if deskt_key and tx_ptr and tx_txt:
            sp = _esc_sql_path(deskt_p)
            tp = _esc_sql_path(txdesk_p)
            text_selects.append(
                f"""
SELECT
  TRY_CAST(d.{col_tit} AS BIGINT) AS TITUL_KEY,
  TRIM(COALESCE(CAST(tx.{tx_txt} AS VARCHAR), '')) AS subject_text
FROM read_parquet('{dp}') d
LEFT JOIN read_parquet('{sp}') ds
  ON TRY_CAST(ds.{deskt_key} AS BIGINT) = TRY_CAST(d.{col_desk} AS BIGINT)
LEFT JOIN read_parquet('{tp}') tx
  ON TRY_CAST(tx.{tx_ptr} AS BIGINT) = TRY_CAST(ds.{deskt_key} AS BIGINT)
WHERE TRY_CAST(d.{col_tit} AS BIGINT) IS NOT NULL
"""
            )

    # Varianta C: text je přímo v desk
    col_txt_d = _pick(cd, ("DESK_TEXT", "DES_TXT", "DESK_POPIS", "TEXT"))
    if col_txt_d:
        text_selects.append(
            f"""
SELECT
  TRY_CAST(d.{col_tit} AS BIGINT) AS TITUL_KEY,
  TRIM(COALESCE(CAST(d.{col_txt_d} AS VARCHAR), '')) AS subject_text
FROM read_parquet('{dp}') d
WHERE TRY_CAST(d.{col_tit} AS BIGINT) IS NOT NULL
"""
        )

    if not text_selects:
        return ""

    union_sql = "\nUNION ALL\n".join(text_selects)
    return f"""
desk_src AS (
  {union_sql}
),
desk_agg AS (
  SELECT
    TITUL_KEY,
    STRING_AGG(DISTINCT subject_text, ' | ')
      FILTER (WHERE subject_text IS NOT NULL AND subject_text <> '') AS desk_subjects
  FROM desk_src
  GROUP BY 1
)
"""


def titul_subjects_exists(derived_dir: Path) -> bool:
    p = derived_dir.expanduser().resolve() / "titul_subjects.parquet"
    return p.exists() and p.stat().st_size > 0
