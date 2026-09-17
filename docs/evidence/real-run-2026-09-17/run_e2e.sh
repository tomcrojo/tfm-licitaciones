#!/usr/bin/env bash
# E2E desde extractor hasta CSV de Tableau, ventana enero-junio, con duracion por fase.
set -uo pipefail
ROOT=/mnt/pop-work/tfm-e2e-from-source
RAW=$ROOT/raw
OUT=$ROOT/out
LOG=$ROOT/e2e.log
REPO=/home/tomcrojo/.t3/worktrees/tfm-licitaciones/t3code-bd2f0a7a
cd "$REPO"

log() { echo "[$(date -Is)] $*" >> "$LOG"; }

: > "$LOG"
log "E2E from-source run (ventana jun). repo=$REPO rev=$(git rev-parse --short HEAD)"

# --- Fase 1+2: ingesta en paralelo (placsp y ted escriben ficheros distintos) ---
log "START ingest (placsp || ted)"
T0=$(date +%s)
uv run --locked --with-editable . licitaciones-pipeline ingest \
  --source placsp --start 2026-01-01 --end 2026-06-30 --raw-dir "$RAW" >> "$LOG.placsp" 2>&1 &
P1=$!
uv run --locked --with-editable . licitaciones-pipeline ingest \
  --source ted --start 2026-01-01 --end 2026-06-30 --raw-dir "$RAW" >> "$LOG.ted" 2>&1 &
P2=$!
RC=0
wait $P1 || RC=$?
wait $P2 || RC=$?
log "$(cat "$LOG.placsp" "$LOG.ted" | tail -2)"
if [ $RC -ne 0 ]; then log "FAIL ingest (${RC})"; exit $RC; fi
log "OK    ingest ($(( $(date +%s) - T0 ))s)"

# --- Fase 3: bronze + silver + gold legado ---
T0=$(date +%s); log "START run-bronze-silver-gold"
uv run --locked --with-editable . licitaciones-pipeline run \
  --raw-dir "$RAW" --output-root "$OUT" >> "$LOG" 2>&1
RC=$?
if [ $RC -ne 0 ]; then log "FAIL run (${RC})"; exit $RC; fi
log "OK    run-bronze-silver-gold ($(( $(date +%s) - T0 ))s)"

# --- Fase 4: gold spark ---
T0=$(date +%s); log "START build-gold"
uv run --locked --with 'pyspark==4.0.1' --with-editable . licitaciones-pipeline build-gold \
  --silver-dir "$OUT/silver" --gold-dir "$OUT/gold2" --reference-dir "$OUT/reference" \
  --as-of 2026-06-30 >> "$LOG" 2>&1
RC=$?
if [ $RC -ne 0 ]; then log "FAIL build-gold (${RC})"; exit $RC; fi
log "OK    build-gold ($(( $(date +%s) - T0 ))s)"

# --- Fase 5: analytics duckdb ---
T0=$(date +%s); log "START build-analytics"
uv run --locked --with-editable . licitaciones-pipeline build-analytics \
  --gold-dir "$OUT/gold2" --analytics-dir "$OUT/analytics" --reference-dir "$OUT/reference" >> "$LOG" 2>&1
RC=$?
if [ $RC -ne 0 ]; then log "FAIL build-analytics (${RC})"; exit $RC; fi
log "OK    build-analytics ($(( $(date +%s) - T0 ))s)"

# --- Fase 6: export tableau ---
T0=$(date +%s); log "START export-tableau"
uv run --locked --with-editable . licitaciones-pipeline export-tableau \
  --analytics-dir "$OUT/analytics" --exports-dir "$OUT/exports" >> "$LOG" 2>&1
RC=$?
if [ $RC -ne 0 ]; then log "FAIL export-tableau (${RC})"; exit $RC; fi
log "OK    export-tableau ($(( $(date +%s) - T0 ))s)"

log "DONE"
sha256sum "$OUT"/exports/tableau/*.csv > "$ROOT/fresh_tableau_sha256.txt" 2>> "$LOG"
log "checksums en $ROOT/fresh_tableau_sha256.txt"
