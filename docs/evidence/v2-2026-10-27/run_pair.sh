#!/usr/bin/env bash
# Run from the repository root. Raw includes payloads AND verified sidecars.
set -euo pipefail
run_root=${1:?Usage: bash run_pair.sh RUN_ROOT RAW_DIR}
raw_dir=${2:?Usage: bash run_pair.sh RUN_ROOT RAW_DIR}
mkdir -p "$run_root"
git rev-parse HEAD > "$run_root/code-revision.txt"
for run_number in 1 2; do
  out_dir="$run_root/run$run_number"
  if [ -e "$out_dir" ]; then
    echo "Refusing to reuse output directory: $out_dir" >&2
    exit 1
  fi
  echo "$(date -Is) run$run_number Raw -> Bronze -> Silver -> legacy Gold"
  timeout 30m uv run --locked --with-editable . licitaciones-pipeline run \
    --raw-dir "$raw_dir" --output-root "$out_dir"
  echo "$(date -Is) run$run_number Silver -> Spark Gold (as-of 2026-06-30)"
  timeout 10m uv run --locked --with 'pyspark==4.0.1' --with-editable . \
    licitaciones-pipeline build-gold --silver-dir "$out_dir/silver" \
    --gold-dir "$out_dir/gold2" --reference-dir "$out_dir/reference" --as-of 2026-06-30
  echo "$(date -Is) run$run_number Gold -> DuckDB -> deterministic CSV exports"
  uv run --locked --with-editable . licitaciones-pipeline build-analytics \
    --gold-dir "$out_dir/gold2" --analytics-dir "$out_dir/analytics" --reference-dir "$out_dir/reference"
  uv run --locked --with-editable . licitaciones-pipeline export-tableau \
    --analytics-dir "$out_dir/analytics" --exports-dir "$out_dir/exports"
  echo "$(date -Is) run$run_number completed"
done
