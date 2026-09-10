# Datos raw

Esta carpeta contiene únicamente descargas reales inmutables (regenerables con
`ingest`, fuera de git). Corpus actual: TED España ene-jun 2026 con la query
tecnológica del pipeline (9.905 avisos, 105 MB).

```bash
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30
```

Los fixtures deterministas para tests viven en `tests/fixtures/raw/`.

Hallazgos verificados con la API real:

- TED: `PC` aporta el CPV principal en el 100% de los avisos; el importe
  estimado (`estimated-value-lot`) llega al ~33%; `links.html` es la fuente
  correcta de la URL del aviso.
- BOE: el sumario diario no publica anuncios de licitación (verificado junio
  2026 completo, solo secciones I-III). Las licitaciones españolas se publican
  en PLACSP; BOE queda restringido a contexto normativo.
