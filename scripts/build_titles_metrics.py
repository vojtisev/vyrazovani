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

from src.data_processing import compute_title_metrics_from_parquet  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Cesta k raw Parquetu s vypujckami")
    ap.add_argument("--output", required=True, help="Cesta k vystupnimu Parquetu (po titulech)")
    ap.add_argument("--years", type=int, default=5, help="Okno let pro vypujcky (default 5)")
    ap.add_argument(
        "--actions",
        default="loan,vypujcka,checkout,borrow",
        help="Carka-oddeleny seznam ACTION_TYPE, ktere se pocitaji jako vypujcky",
    )
    args = ap.parse_args()

    inp = Path(args.input).expanduser().resolve()
    out = Path(args.output).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    actions = [a.strip() for a in str(args.actions).split(",") if a.strip()]
    df = compute_title_metrics_from_parquet(inp, years_window=int(args.years), action_types_for_loans=actions)
    df.to_parquet(out, index=False)
    print(f"OK: {len(df):,} titulu -> {out}")


if __name__ == "__main__":
    main()

