# Guía de ejecución

## Prerrequisitos

Se usa Python 3.10 o superior y `uv`, siguiendo la convención del repositorio.
No hay credenciales; la única dependencia de red es la ingesta explícita.
El TLS de OpenPLACSP se ancla con la raíz FNMT incluida en
`config/certs/` — no se desactiva la verificación de certificados.

## Run local con fixtures

```bash
uv run --with-editable . python -m unittest discover -s tests -v
uv run --with-editable . python -m tfm_licitaciones.cli run
```

El comando genera:

- `data/bronze/records.jsonl`: payloads con procedencia;
- `data/silver/tenders.jsonl`: registros normalizados y plegados;
- `data/gold/opportunities.jsonl` y `opportunities.csv`: tabla principal;
- `data/gold/technology_summary.csv`: volumen por categoría y mes (canónicos);
- `data/gold/buyer_summary.csv`: volumen por comprador y categoría (canónicos);
- `data/gold/quality_report.json`: checks, métricas e incidencias;
- `data/gold/classifier_evaluation.json`: P/R/F1 del clasificador vs CPV;
- `data/gold/run_manifest.json`: conteos, checksums, ingesta y linkage.

## Ingesta live

La ingesta se mantiene separada del procesamiento para que un run sea
reproducible a partir de archivos. Se lanza de forma explícita y por fuente:

```bash
# TED (query tecnológica, avisos españoles; ~4 min por semestre)
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source ted

# OpenPLACSP (6 ZIPs mensuales de ~200 MB; reanudable, valida cada zip)
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source placsp

uv run --with-editable . python -m tfm_licitaciones.cli run
```

`run` no hace llamadas externas. Sobre el corpus completo ene-jun 2026
(TED + 6 meses de sindicación 643) el run tarda ~15-17 minutos: el coste
dominante es el parseo CODICE de ~350.000 entries y la escritura de las capas.

Si un endpoint no responde, el adaptador reintenta dentro del límite
configurado y no sustituye los raw existentes. Las ventanas, el patrón de ZIP
y los parámetros de linkage se controlan en `config/pipeline.json`.

## Revisión rápida

```bash
uv run --with-editable . python -m tfm_licitaciones.cli report
uv run python -c "import json; m=json.load(open('data/gold/run_manifest.json')); print(m['counts'], m['linkage'])"
```
