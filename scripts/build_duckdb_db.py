"""Create a DuckDB database file from derived Parquets.

This makes interactive analysis fast (joins, group-bys) while keeping Parquet as source of truth.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--derived-dir", required=True, help="Slozka s derived Parquety")
    ap.add_argument("--db", required=True, help="Cesta k vystupni DuckDB databazi (.duckdb)")
    ap.add_argument(
        "--fact",
        default="vypujcky_enriched.parquet",
        help="Nazev enriched Parquetu v derived-dir (default: vypujcky_enriched.parquet)",
    )
    args = ap.parse_args()

    d = Path(args.derived_dir).expanduser().resolve()
    db_path = Path(args.db).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    fact = d / args.fact
    tituly = d / "tituly.parquet"
    svazky = d / "svazky.parquet"
    knoddel = d / "knoddel.parquet"
    opidy = d / "opidy.parquet"

    missing = [p for p in (fact, tituly, svazky, knoddel, opidy) if not p.exists()]
    if missing:
        raise SystemExit(f"Missing inputs: {missing}")

    con = duckdb.connect(database=str(db_path))
    try:
        con.execute("PRAGMA threads=4")

        con.execute("CREATE OR REPLACE TABLE fact_vypujcky AS SELECT * FROM read_parquet(?)", [str(fact)])
        con.execute("CREATE OR REPLACE TABLE dim_tituly AS SELECT * FROM read_parquet(?)", [str(tituly)])
        con.execute("CREATE OR REPLACE TABLE dim_svazky AS SELECT * FROM read_parquet(?)", [str(svazky)])
        con.execute("CREATE OR REPLACE TABLE dim_knoddel AS SELECT * FROM read_parquet(?)", [str(knoddel)])
        con.execute("CREATE OR REPLACE TABLE dim_opidy AS SELECT * FROM read_parquet(?)", [str(opidy)])

        # Helpful views
        con.execute(
            """
CREATE OR REPLACE VIEW v_titul_svazky AS
SELECT
  TRY_CAST(SVAZKY_PTR_TITUL AS BIGINT) AS TITUL_KEY,
  COUNT(DISTINCT TRY_CAST(SVAZKY_KEY AS BIGINT)) AS pocet_svazku
FROM dim_svazky
GROUP BY 1
"""
        )
        con.execute(
            """
CREATE OR REPLACE VIEW v_loans_by_titul AS
SELECT
  TITUL_KEY,
  COUNT(*) FILTER (WHERE ACTION_TYPE = 'loan') AS loan_cnt,
  MIN(DATE) AS first_date,
  MAX(DATE) AS last_date
FROM fact_vypujcky
GROUP BY 1
"""
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()

