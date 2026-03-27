"""Build `vypujcky_enriched.parquet` from derived Parquets.

Inputs (typically produced by `build_parquet_from_esql_txt.py`):
- derived/vypujcky.parquet
- derived/svazky.parquet
- derived/tituly_*.parquet (signatura, nazev, zahlavi, meta)
- derived/opidy.parquet
- derived/knoddel.parquet
- derived/druhdokumentu.parquet (optional mapping code -> label)

Output:
- a rebuilt enriched Parquet suitable for the Streamlit dashboard.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.subject_tables import write_titul_subjects_parquet  # noqa: E402


LOAN_OPIDS = (4, 94, 97, 225)
RETURN_OPIDS = (5, 95, 98, 226)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--derived-dir", required=True, help="Slozka s derived Parquety")
    ap.add_argument(
        "--out",
        required=True,
        help="Kam ulozit vysledny vypujcky_enriched Parquet (dop. raw/vypujcky_enriched.rebuilt.parquet)",
    )
    ap.add_argument(
        "--include-reader-demo",
        action="store_true",
        help="Prida privacy-safe demografii ctenare (vek_bucket, pohlavi, psc_prefix) a zahodi identifikatory.",
    )
    args = ap.parse_args()

    d = Path(args.derived_dir).expanduser().resolve()
    out = Path(args.out).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    vyp = d / "vypujcky.parquet"
    sv = d / "svazky.parquet"
    tit = d / "tituly.parquet"
    op = d / "opidy.parquet"
    kn = d / "knoddel.parquet"
    dd = d / "druhdokumentu.parquet"
    lg = d / "legitky.parquet"

    missing = [p for p in (vyp, sv, tit, op, kn) if not p.exists()]
    if missing:
        raise SystemExit(f"Missing required inputs: {missing}")
    if args.include_reader_demo and not lg.exists():
        raise SystemExit("Missing required input for --include-reader-demo: legitky.parquet")

    write_titul_subjects_parquet(d)

    subj = d / "titul_subjects.parquet"
    has_subj = subj.exists() and subj.stat().st_size > 0
    if has_subj:
        subj_join = "LEFT JOIN read_parquet($subj) tj ON tj.TITUL_KEY = COALESCE(t.TITUL_KEY, sv.SVAZKY_PTR_TITUL)"
        desk_expr = "COALESCE(tj.desk_subjects, '') AS desk_subjects"
        och_expr = "COALESCE(tj.och_subjects, '') AS och_subjects"
    else:
        subj_join = ""
        desk_expr = "'' AS desk_subjects"
        och_expr = "'' AS och_subjects"

    con = duckdb.connect(database=":memory:")
    try:
        # Optional mapping of document type code -> label
        has_dd = dd.exists()
        dd_join = ""
        dd_select = "t.TITUL_DRUH_DOKUMENTU AS TITUL_DRUH_DOKUMENTU"
        if has_dd:
            # druhdokumentu.txt: CISELNIKY_ZKRATKA = kod (napr. 'a', 'divh'), CISELNIKY_TEXT = popis
            dd_join = "LEFT JOIN read_parquet($dd) dd ON dd.CISELNIKY_ZKRATKA = t.TITUL_DRUH_DOKUMENTU"
            dd_select = "COALESCE(dd.CISELNIKY_TEXT, t.TITUL_DRUH_DOKUMENTU) AS TITUL_DRUH_DOKUMENTU"

        # Note: columns come from eSQL dumps, normalized by the parser:
        # - loans: STAT_VYP_PTR_SVAZKY, STAT_VYP_TIME, STAT_VYP_OPID, STAT_VYP_PTR_KNODDEL, STAT_VYP_PTR_LEG
        # - svazky: SVAZKY_KEY, SVAZKY_PTR_TITUL
        demo_cte = ""
        demo_join = ""
        demo_select = ""
        if args.include_reader_demo:
            demo_cte = """
legitky AS (
  SELECT
    TRY_CAST(LEG_KEY AS BIGINT) AS LEG_KEY,
    TRY_CAST(VEK AS BIGINT) AS VEK,
    CAST(POHLAVI AS VARCHAR) AS POHLAVI,
    CAST(ADRESY_PSC AS VARCHAR) AS ADRESY_PSC
  FROM read_parquet($lg)
),
legitky_demo AS (
  SELECT
    LEG_KEY,
    VEK,
    CASE
      WHEN VEK IS NULL THEN NULL
      WHEN VEK < 0 THEN NULL
      WHEN VEK < 6 THEN '0-5'
      WHEN VEK < 13 THEN '6-12'
      WHEN VEK < 18 THEN '13-17'
      WHEN VEK < 25 THEN '18-24'
      WHEN VEK < 35 THEN '25-34'
      WHEN VEK < 45 THEN '35-44'
      WHEN VEK < 55 THEN '45-54'
      WHEN VEK < 65 THEN '55-64'
      WHEN VEK < 75 THEN '65-74'
      WHEN VEK < 85 THEN '75-84'
      ELSE '85+'
    END AS vek_bucket,
    NULLIF(TRIM(POHLAVI), '') AS pohlavi_raw,
    CASE
      WHEN ADRESY_PSC IS NULL THEN NULL
      WHEN ADRESY_PSC IN ('$NIC$', '') THEN NULL
      ELSE REGEXP_REPLACE(ADRESY_PSC, '[^0-9]', '')
    END AS psc_digits
  FROM legitky
),
legitky_demo2 AS (
  SELECT
    LEG_KEY,
    VEK,
    vek_bucket,
    CASE
      WHEN pohlavi_raw IN ('5','6') THEN pohlavi_raw
      WHEN pohlavi_raw IN ('0','1') THEN NULL
      ELSE pohlavi_raw
    END AS pohlavi,
    CASE
      WHEN psc_digits IS NULL OR LENGTH(psc_digits) < 3 THEN NULL
      ELSE SUBSTR(psc_digits, 1, 3)
    END AS psc_prefix
  FROM legitky_demo
)
,
"""
            demo_join = "LEFT JOIN legitky_demo2 lgd ON lgd.LEG_KEY = l.VYP_PTR_LEG"
            demo_select = """,
    lgd.VEK AS VEK,
    lgd.vek_bucket AS VEK_BUCKET,
    lgd.pohlavi AS POHLAVI,
    lgd.psc_prefix AS PSC_PREFIX
"""

        query = f"""
WITH loans AS (
  SELECT
    TRY_CAST(STAT_VYP_PTR_SVAZKY AS BIGINT) AS VYP_PTR_SVAZKY,
    TRY_CAST(STAT_VYP_PTR_LEG AS BIGINT) AS VYP_PTR_LEG,
    COALESCE(
      TRY_CAST(STAT_VYP_TIME AS TIMESTAMP),
      TRY_CAST(CAST(STAT_VYP_TIME AS VARCHAR) AS TIMESTAMP),
      TRY_STRPTIME(CAST(STAT_VYP_TIME AS VARCHAR), '%Y-%m-%d %H:%M:%S')
    ) AS VYP_TIME,
    TRY_CAST(STAT_VYP_OPID AS BIGINT) AS VYP_OPID,
    TRY_CAST(STAT_VYP_PTR_KNODDEL AS BIGINT) AS VYP_PTR_KNODDEL
  FROM read_parquet($vyp)
),
svazky AS (
  SELECT
    TRY_CAST(SVAZKY_KEY AS BIGINT) AS SVAZKY_KEY,
    TRY_CAST(SVAZKY_PTR_TITUL AS BIGINT) AS SVAZKY_PTR_TITUL
  FROM read_parquet($sv)
),
 tituly AS (
  SELECT
    TRY_CAST(TITUL_KEY AS BIGINT) AS TITUL_KEY,
    TITUL_SIGN_FULL AS TITUL_SIGN_FULL,
    TITUL_NAZEV AS TITUL_NAZEV,
    TITUL_ZAHLAVI AS TITUL_ZAHLAVI,
    TRY_CAST(TITUL_ROK_VYDANI AS BIGINT) AS TITUL_ROK_VYDANI,
    TITUL_JAZYK AS TITUL_JAZYK,
    {dd_select}
  FROM read_parquet($tit) t
  {dd_join}
),
{demo_cte}
enr AS (
  SELECT
    l.VYP_PTR_SVAZKY,
    l.VYP_TIME,
    l.VYP_OPID,
    l.VYP_PTR_KNODDEL,
    op.OPIDY_OPID,
    op.OPIDY_POPIS,
    kn.KNODDEL_KEY,
    kn.KNODDEL_NAZEV,
    kn.KNODDEL_ULICE,
    kn.KNODDEL_PSC,
    sv.SVAZKY_KEY,
    sv.SVAZKY_PTR_TITUL,
    CAST(sv.SVAZKY_PTR_TITUL AS VARCHAR) AS SVAZKY_PTR_TITUL_STR,
    COALESCE(t.TITUL_KEY, sv.SVAZKY_PTR_TITUL) AS TITUL_KEY,
    t.TITUL_SIGN_FULL,
    t.TITUL_ZAHLAVI,
    t.TITUL_NAZEV,
    t.TITUL_ROK_VYDANI,
    t.TITUL_JAZYK,
    t.TITUL_DRUH_DOKUMENTU,
    CAST(COALESCE(t.TITUL_KEY, sv.SVAZKY_PTR_TITUL) AS VARCHAR) AS TITUL_KEY_STR,
    CAST(l.VYP_TIME AS DATE) AS DATE,
    EXTRACT(YEAR FROM l.VYP_TIME)::INTEGER AS YEAR,
    EXTRACT(MONTH FROM l.VYP_TIME)::INTEGER AS MONTH,
    (EXTRACT(YEAR FROM l.VYP_TIME)::INTEGER * 100 + EXTRACT(MONTH FROM l.VYP_TIME)::INTEGER) AS YEAR_MONTH,
    EXTRACT(WEEK FROM l.VYP_TIME)::INTEGER AS WEEK,
    EXTRACT(DOW FROM l.VYP_TIME)::INTEGER AS WEEKDAY,
    CASE WHEN l.VYP_OPID IN {LOAN_OPIDS} THEN TRUE ELSE FALSE END AS IS_LOAN,
    CASE WHEN l.VYP_OPID IN {RETURN_OPIDS} THEN TRUE ELSE FALSE END AS IS_RETURN,
    FALSE AS IS_RENEWAL,
    CASE
      WHEN l.VYP_OPID IN {LOAN_OPIDS} THEN 'loan'
      WHEN l.VYP_OPID IN {RETURN_OPIDS} THEN 'return'
      ELSE 'other'
    END AS ACTION_GROUP,
    CASE
      WHEN op.OPIDY_POPIS IS NOT NULL AND TRIM(CAST(op.OPIDY_POPIS AS VARCHAR)) <> '' THEN CAST(op.OPIDY_POPIS AS VARCHAR)
      ELSE CAST(l.VYP_OPID AS VARCHAR)
    END AS ACTION_TYPE,
    {desk_expr},
    {och_expr}
    {demo_select}
  FROM loans l
  LEFT JOIN read_parquet($op) op ON TRY_CAST(op.OPIDY_OPID AS BIGINT) = l.VYP_OPID
  LEFT JOIN read_parquet($kn) kn ON TRY_CAST(kn.KNODDEL_KEY AS BIGINT) = l.VYP_PTR_KNODDEL
  LEFT JOIN svazky sv ON sv.SVAZKY_KEY = l.VYP_PTR_SVAZKY
  LEFT JOIN tituly t ON t.TITUL_KEY = sv.SVAZKY_PTR_TITUL
  {subj_join}
  {demo_join}
)
SELECT * FROM enr
"""

        out_sql = str(out).replace("'", "''")
        params: dict[str, str] = {
            "vyp": str(vyp),
            "sv": str(sv),
            "tit": str(tit),
            "op": str(op),
            "kn": str(kn),
            "dd": str(dd),
            "lg": str(lg),
        }
        if has_subj:
            params["subj"] = str(subj)
        con.execute(
            f"COPY ({query}) TO '{out_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)",
            params,
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()

