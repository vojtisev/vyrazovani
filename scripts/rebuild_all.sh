#!/usr/bin/env bash
set -euo pipefail

RAW_DIR="${1:-${VYRAZ_RAW_DIR:-}}"
DERIVED_DIR="${2:-${VYRAZ_DERIVED_DIR:-}}"

if [[ -z "${RAW_DIR}" || -z "${DERIVED_DIR}" ]]; then
  echo "Usage:"
  echo "  bash scripts/rebuild_all.sh <raw_dir> <derived_dir>"
  echo
  echo "Or set env vars:"
  echo "  export VYRAZ_RAW_DIR=/path/to/raw"
  echo "  export VYRAZ_DERIVED_DIR=/path/to/derived"
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[1/4] build_parquet_from_esql_txt.py"
python3 "$SCRIPT_DIR/build_parquet_from_esql_txt.py" \
  --raw-dir "$RAW_DIR" \
  --out-dir "$DERIVED_DIR"

echo "[2/4] build_vypujcky_enriched_from_derived.py"
python3 "$SCRIPT_DIR/build_vypujcky_enriched_from_derived.py" \
  --derived-dir "$DERIVED_DIR" \
  --out "$DERIVED_DIR/vypujcky_enriched.parquet" \
  --include-reader-demo

echo "[3/4] build_titles_metrics.py"
python3 "$SCRIPT_DIR/build_titles_metrics.py" \
  --input "$DERIVED_DIR/vypujcky_enriched.parquet" \
  --svazky "$DERIVED_DIR/svazky.parquet" \
  --output "$DERIVED_DIR/titles_metrics.parquet"

echo "[4/4] validate_build.py"
python3 "$SCRIPT_DIR/validate_build.py" \
  --derived-dir "$DERIVED_DIR" \
  --fact "vypujcky_enriched.parquet"

echo "Hotovo: rebuild + validace dokonceny."

