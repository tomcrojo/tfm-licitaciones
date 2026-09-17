# Demo local Raw → Tableau

Desde la raíz del repositorio, con `uv`, Python y Java 17 disponibles:

```bash
bash examples/demo/run.sh
# Directorio alternativo (no utilizar un directorio con resultados que quiera conservar):
bash examples/demo/run.sh /tmp/tfm-demo
```

La instalación de dependencias puede necesitar red; la demo no consulta las
fuentes de contratación. El script llama a los comandos productivos `run`,
`build-gold`, `build-analytics` y `export-tableau`, y construye la dimensión CPV
con el CSV oficial ya versionado. Fija `--as-of 2026-01-01` y deja los resultados
en `data/demo/` por defecto. Las siguientes ejecuciones reconstruyen esas salidas.

## Qué se demuestra

`raw/placsp/demo.atom` es una **fixture sintética**, derivada del mini Atom de
las pruebas. Se cambia el primer aviso de `EV` a `PUB` para representar una
licitación abierta; el segundo permanece cerrado y se conserva un tombstone
sobre otro identificador. Nombres, identificadores, enlaces e importes son
material de prueba: no deben utilizarse como oportunidades reales.

El `retrieved_at` del sidecar es **simulado**. Su checksum autentica estos bytes
locales, no una descarga histórica. Esta fixture nunca debe mezclarse con un
corpus real. No se cambia la política Gold para hacer visible el ejemplo.

| Frontera | Resultado esperado |
| --- | ---: |
| Bronze: avisos aceptados / rechazados | 2 / 0 |
| Bronze: tombstones | 1 |
| Silver: eventos canónicos | 3 |
| Gold: abiertos / cerrados / borrados | 1 / 1 / 1 |
| Gold: incidencias de estado vigente | 0 |

Los cinco CSV se conservan en `data/demo/exports/tableau/`:

| CSV | Filas de datos |
| --- | ---: |
| `open_opportunities.csv` | 1 |
| `opportunity_cpv.csv` | 1 |
| `buyer_summary.csv` | 1 |
| `cpv_summary.csv` | 1 |
| `opportunities_monthly.csv` | 0 |

La oportunidad visible corresponde al servicio cloud de la fixture, con estado
`PUB`, importe estimado de 240.000 EUR y CPV `72415000`. La etiqueta CPV procede
del vocabulario oficial versionado. DIR3 no se descarga: su ausencia queda
registrada en el manifest Gold.

El CSV mensual conserva solo la cabecera porque Silver no atribuye una fecha
de publicación a PLACSP. DuckDB registra la fila en
`opportunities_without_publication_date`; no se inventa una fecha para llenar
el gráfico. `as_of` evalúa la política de plazos, no reconstruye qué datos
estaban disponibles históricamente ese día.

## Verificación

La prueba integrada consume exactamente esta fixture y sus sidecars, llama al
pipeline productivo, comprueba los conteos anteriores y verifica que un rebuild
DuckDB/export conserva los bytes de los CSV:

```bash
uv run --locked --with 'pyspark==4.0.1' --with-editable . \
  python -m unittest tests.test_e2e_raw_to_tableau -v
```

Los fixtures anteriores de `tests/fixtures/raw` siguen disponibles para probar
el caso sin oportunidades abiertas. Los resultados de una ejecución con datos
reales están documentados por separado en
[la evidencia del corpus](../../docs/real-run-evidence.md).
