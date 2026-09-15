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
Los sidecars de `tests/fixtures/raw` usan un timestamp de recuperación
**sintético**, fijado para las pruebas; no documentan descargas históricas reales.

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
/tmp/tfm-licitaciones-fixture/bronze/records.parquet
/tmp/tfm-licitaciones-fixture/bronze/rejections.parquet
/tmp/tfm-licitaciones-fixture/bronze/tombstones.parquet
/tmp/tfm-licitaciones-fixture/bronze/ingestion_report.json
/tmp/tfm-licitaciones-fixture/silver/tenders.jsonl
/tmp/tfm-licitaciones-fixture/gold/opportunities.jsonl
/tmp/tfm-licitaciones-fixture/gold/opportunities.csv
/tmp/tfm-licitaciones-fixture/gold/technology_summary.csv
/tmp/tfm-licitaciones-fixture/gold/buyer_summary.csv
/tmp/tfm-licitaciones-fixture/gold/quality_report.json
/tmp/tfm-licitaciones-fixture/gold/classifier_evaluation.json
/tmp/tfm-licitaciones-fixture/gold/run_manifest.json
```

Bronze ya no escribe `records.jsonl`. Los Parquet de registros, rechazos y tombstones
conservan esquema explícito, incluso vacíos. El [contrato](data_contract.md)
define payload, procedencia, motivos y la unidad de conteo.

Revise `bronze/ingestion_report.json` junto a `gold/quality_report.json`:
el primero informa `parsed`, `accepted`, `rejected`, tombstones válidos, errores
de documento y control, y su estado `passed`, también con conteos por fuente. El manifest
incluye el mismo informe bajo `ingestion`. Los errores de parsing no detienen
los demás candidatos o miembros Atom recuperables.

El estado `quality_passed` que muestra la CLI y su código de salida conservan
los gates Silver/Gold anteriores: un resultado verde no implica cero rechazos
Bronze. La agregación de ambos estados de calidad queda pendiente.

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
Cada nueva descarga conserva además `<fichero>.provenance.json` junto al Raw.
Copie ambos al mover un corpus, manteniendo sus rutas relativas dentro de Raw.
La transformación verifica este sidecar y hereda su `retrieved_at` y `sha256`
en los tres Parquet Bronze. No utiliza la fecha del manifest. El sidecar es la
raíz de confianza local: una edición externa semánticamente válida de su
timestamp no es detectable sin una autoridad adicional.

### Corpus anteriores sin evidencia de recuperación

`run` y la reutilización del caché OpenPLACSP fallan explícitamente si falta
el sidecar o si no coincide con el fichero. No hay backfill automático del
timestamp. Para recuperar un corpus histórico se necesita evidencia verificable
del instante de descarga **y del mismo checksum y fichero**; por ejemplo, un
registro original de descarga cuya identidad permita establecer ese vínculo.
Un `downloaded_at` operativo, especialmente el de aceptación de una corrección,
no basta por sí solo. Sin evidencia, descargue de nuevo en un directorio vacío:

```bash
uv run --locked --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-01-01 --end 2026-01-31 --source placsp \
  --raw-dir /tmp/tfm-licitaciones-raw-con-evidencia
```

Esta nueva recuperación tiene su propio timestamp; no reconstruye el antiguo.
Un checksum distinto para la misma partición produce un fichero adicional con
sufijo SHA-256 y sidecar propio. La publicación usa ficheros temporales y enlaces
locales sin sobrescritura: payload y sidecar no forman una transacción de dos
ficheros. `run` rechaza tanto un payload sin sidecar como un sidecar sin payload.
El helper de persistencia permite reintentar con una descarga nueva:

- payload sin sidecar: exige bytes idénticos y registra el instante real del
  reintento en el nuevo sidecar;
- sidecar sin payload: exige los mismos bytes, fuente, partición y ventana,
  repone el payload y conserva el timestamp ya registrado.

Las diferencias fallan sin sobrescribir lo existente. No se puede completar la
evidencia con el reloj de transformación ni con mtime. El caché OpenPLACSP aún
rechaza un ZIP existente sin evidencia: para recuperarlo mediante la CLI,
descargue en un directorio nuevo como en el ejemplo anterior; el refresco de
meses cacheados sigue pendiente.

Bronze conserva todos los snapshots recuperados, incluidos sus tombstones y
rechazos. Silver/Gold legado recibe únicamente el snapshot más reciente por
fuente, partición y ventana, según el sidecar; por ello no reaplica un tombstone
retirado de un ZIP corregido ni conserva una versión TED/BOE antigua por orden
léxico. El informe muestra `superseded_artifacts` y `superseded_records` para
explicar la diferencia. Un empate de timestamps máximos con bytes distintos
falla explícitamente. Consulte el [contrato](data_contract.md) para las reglas
de inputs sin partición y la compatibilidad de tombstones Atom planos.

## Comportamiento y límites de la versión 0.1

- TED se consulta por páginas y ventanas acotadas, con retries y throttle.
- La configuración actual de TED incluye términos tecnológicos.
- OpenPLACSP descarga ZIP mensuales, los valida antes de moverlos a su destino
  y reutiliza un ZIP existente si es legible y su evidencia coincide. No vuelve
  a consultar automáticamente meses ya cacheados para buscar correcciones.
- Un mes de OpenPLACSP que agota los reintentos se registra en logs, pero la
  CLI puede continuar con el resto. Todavía no existe un estado persistente de
  completitud de ventana.
- La transformación `run` es offline y determinista respecto a sus inputs de
  negocio, salvo por los timestamps técnicos de ejecución.
- Bronze utiliza Parquet; JSONL y CSV siguen siendo formatos transitorios de
  Silver y Gold.

Estas limitaciones se mantienen visibles para que los siguientes cambios
puedan demostrar qué propiedad añaden.
