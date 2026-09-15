# Guía de ejecución

## Prerrequisitos

- Python 3.10 o posterior;
- `uv`;
- red únicamente para la ingesta explícita.

OpenPLACSP utiliza una cadena TLS de la FNMT. El adaptador carga la raíz
incluida en `config/certs/` y no desactiva la validación de certificados.

## Pruebas offline

```bash
uv run --with-editable . python -m unittest discover -s tests -v
```

La suite utiliza fixtures locales y no necesita consultar las fuentes
oficiales.

## Ejecución con fixtures

Es recomendable indicar un directorio de salida temporal para no sustituir los
artefactos Gold versionados:

```bash
uv run --with-editable . python -m tfm_licitaciones.cli run \
  --raw-dir tests/fixtures/raw \
  --output-root /tmp/tfm-licitaciones-fixture
```

Se generan:

```text
/tmp/tfm-licitaciones-fixture/bronze/records.jsonl
/tmp/tfm-licitaciones-fixture/silver/tenders.jsonl
/tmp/tfm-licitaciones-fixture/gold/opportunities.jsonl
/tmp/tfm-licitaciones-fixture/gold/opportunities.csv
/tmp/tfm-licitaciones-fixture/gold/technology_summary.csv
/tmp/tfm-licitaciones-fixture/gold/buyer_summary.csv
/tmp/tfm-licitaciones-fixture/gold/quality_report.json
/tmp/tfm-licitaciones-fixture/gold/classifier_evaluation.json
/tmp/tfm-licitaciones-fixture/gold/run_manifest.json
```

## Ingesta desde fuentes oficiales

La descarga y la transformación son operaciones separadas. Las ventanas se
indican de forma explícita:

```bash
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source ted

uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-06-30 --source placsp
```

Después puede ejecutarse el procesamiento sobre los ficheros descargados:

```bash
uv run --with-editable . python -m tfm_licitaciones.cli run
uv run --with-editable . python -m tfm_licitaciones.cli report
```

El corpus raw no se versiona. El manifest Gold conserva rutas, tamaños y
checksums de los inputs utilizados en la última ejecución publicada.

## Comportamiento y límites de la versión 0.1

- TED se consulta por páginas y ventanas acotadas, con retries y throttle.
- La configuración actual de TED incluye términos tecnológicos.
- OpenPLACSP descarga ZIP mensuales, los valida antes de moverlos a su destino
  y reutiliza un ZIP existente si sigue siendo legible.
- Un mes de OpenPLACSP que agota los reintentos se registra en logs, pero la
  CLI puede continuar con el resto. Todavía no existe un estado persistente de
  completitud de ventana.
- La transformación `run` es offline y determinista respecto a sus inputs de
  negocio, salvo por los timestamps técnicos de ejecución.
- JSONL y CSV son formatos transitorios de la implementación actual; la
  arquitectura P0 utiliza Parquet.

Estas limitaciones se mantienen visibles para que los siguientes cambios
puedan demostrar qué propiedad añaden.
