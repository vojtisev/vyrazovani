"""Jednorazovy builder: z raw vypujcek vytvori metriky po titulech pro dashboard.

Pouziti (lokalne):
  python3 scripts/build_titles_metrics.py --input vypujcky_enriched.parquet --output titles_metrics.parquet

Poznamka:
- Bezi lokalne (ne na Streamlit Cloud). Muzes to poustet i mesicne.
- Vystup je mnohem mensi nez raw vypujcky a je vhodny pro publikaci na SharePoint (download=1).
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data_processing import (  # noqa: E402
    compute_title_metrics_from_parquet,
    get_distinct_action_types_from_parquet,
    suggest_loan_action_types_from_parquet,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Cesta k raw Parquetu s vypujckami")
    ap.add_argument("--output", required=True, help="Cesta k vystupnimu Parquetu (po titulech)")
    ap.add_argument(
        "--svazky",
        default="",
        help="Volitelne: Parquet se svazky (sloupce SVAZKY_KEY, SVAZKY_PTR_TITUL) pro vypocet poctu svazku na titul",
    )
    ap.add_argument("--years", type=int, default=5, help="Okno let pro vypujcky (default 5)")
    ap.add_argument(
        "--actions",
        default="auto",
        help="Čárkou oddělené ACTION_TYPE (text z OPIDY), nebo 'auto' = typy s IS_LOAN / ACTION_GROUP=loan.",
    )
    args = ap.parse_args()

    inp = Path(args.input).expanduser().resolve()
    out = Path(args.output).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    raw_actions = str(args.actions).strip()
    if raw_actions.lower() == "auto":
        actions = suggest_loan_action_types_from_parquet(inp)
        if not actions:
            actions = get_distinct_action_types_from_parquet(inp)
    else:
        actions = [a.strip() for a in raw_actions.split(",") if a.strip()]
    svazky = Path(args.svazky).expanduser().resolve() if str(args.svazky).strip() else None
    df = compute_title_metrics_from_parquet(
        inp,
        years_window=int(args.years),
        action_types_for_loans=actions,
        svazky_parquet_file=svazky,
    )
    df.to_parquet(out, index=False)
    print(f"OK: {len(df):,} titulu -> {out}")


if __name__ == "__main__":
    main()

