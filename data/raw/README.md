# Datos raw

Esta carpeta se reserva para los payloads descargados por la CLI.
Los lotes reales no se versionan debido a su tamaño; los fixtures deterministas
viven en `tests/fixtures/raw/`.

```bash
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source ted

uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source placsp

uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-09-15 --end 2026-09-15 --source dir3
```

El estado actual evita sustituir un ZIP de OpenPLACSP que ya sea válido, pero
todavía no implementa el registro completo de ventanas, checksums e
idempotencia definido para P0. Véanse [la arquitectura](../../docs/architecture.md)
y [la guía de ejecución](../../docs/run.md).
