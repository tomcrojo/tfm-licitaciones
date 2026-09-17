#!/usr/bin/env bash
# Local synthetic fixture only; never downloads procurement data.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
output_root="${1:-data/demo}"

uv run --locked --with-editable . licitaciones-pipeline run \
  --raw-dir examples/demo/raw --output-root "$output_root"

uv run --locked --with-editable . python - "$output_root/reference/cpv_codes.parquet" <<'PY'
import sys
from pathlib import Path
from tfm_licitaciones.cpv import DEFAULT_CPV_SOURCE, build_cpv_dimension
from tfm_licitaciones.io import write_parquet
write_parquet(Path(sys.argv[1]), build_cpv_dimension(DEFAULT_CPV_SOURCE))
PY

uv run --locked --with 'pyspark==4.0.1' --with-editable . \
  licitaciones-pipeline build-gold --silver-dir "$output_root/silver" \
  --gold-dir "$output_root/gold" --reference-dir "$output_root/reference" --as-of 2026-01-01
uv run --locked --with-editable . licitaciones-pipeline build-analytics \
  --gold-dir "$output_root/gold" --analytics-dir "$output_root/analytics" \
  --reference-dir "$output_root/reference"
uv run --locked --with-editable . licitaciones-pipeline export-tableau \
  --analytics-dir "$output_root/analytics" --exports-dir "$output_root/exports"
