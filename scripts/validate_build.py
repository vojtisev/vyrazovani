"""Sanity checks for derived/enriched datasets."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--derived-dir", required=True)
    ap.add_argument("--fact", default="vypujcky_enriched.parquet")
    args = ap.parse_args()

    d = Path(args.derived_dir).expanduser().resolve()
    fact = d / args.fact
    sv = d / "svazky.parquet"
    tit = d / "tituly.parquet"

    missing = [p for p in (fact, sv, tit) if not p.exists()]
    if missing:
        raise SystemExit(f"Missing inputs: {missing}")

    con = duckdb.connect(database=":memory:")
    try:
        q = """
WITH f AS (SELECT * FROM read_parquet($fact)),
sv AS (SELECT * FROM read_parquet($sv)),
t AS (SELECT * FROM read_parquet($tit)),
stats AS (
  SELECT
    COUNT(*) AS fact_rows,
    COUNT(*) FILTER (WHERE TITUL_KEY IS NULL) AS fact_rows_missing_titul,
    COUNT(*) FILTER (WHERE SVAZKY_KEY IS NULL) AS fact_rows_missing_svazek,
    MIN(DATE) AS min_date,
    MAX(DATE) AS max_date,
    COUNT(DISTINCT TITUL_KEY) AS fact_unique_tituly
  FROM f
),
join_cov AS (
  SELECT
    COUNT(*) AS fact_rows,
    COUNT(*) FILTER (WHERE sv.SVAZKY_PTR_TITUL IS NOT NULL) AS rows_with_svazky_ptr_titul
  FROM f
  LEFT JOIN sv ON TRY_CAST(sv.SVAZKY_KEY AS BIGINT) = TRY_CAST(f.SVAZKY_KEY AS BIGINT)
),
sv_by_titul AS (
  SELECT
    TRY_CAST(SVAZKY_PTR_TITUL AS BIGINT) AS TITUL_KEY,
    COUNT(DISTINCT TRY_CAST(SVAZKY_KEY AS BIGINT)) AS pocet_svazku
  FROM sv
  GROUP BY 1
),
sv_dist AS (
  SELECT
    MIN(pocet_svazku) AS min_svazku,
    APPROX_QUANTILE(pocet_svazku, 0.5) AS median_svazku,
    MAX(pocet_svazku) AS max_svazku
  FROM sv_by_titul
),
titul_cov AS (
  SELECT
    (SELECT COUNT(*) FROM t) AS dim_tituly_rows,
    (SELECT COUNT(DISTINCT TITUL_KEY) FROM t) AS dim_tituly_unique,
    (SELECT COUNT(DISTINCT TITUL_KEY) FROM f WHERE TITUL_KEY IS NOT NULL) AS fact_tituly_unique
)
SELECT
  stats.*,
  join_cov.rows_with_svazky_ptr_titul,
  (100.0 * join_cov.rows_with_svazky_ptr_titul / NULLIF(join_cov.fact_rows, 0)) AS pct_rows_with_titul_via_svazky,
  sv_dist.*,
  titul_cov.*
FROM stats
CROSS JOIN join_cov
CROSS JOIN sv_dist
CROSS JOIN titul_cov
"""
        df = con.execute(
            q,
            {"fact": str(fact), "sv": str(sv), "tit": str(tit)},
        ).fetchdf()
        print(df.to_string(index=False))
    finally:
        con.close()


if __name__ == "__main__":
    main()

