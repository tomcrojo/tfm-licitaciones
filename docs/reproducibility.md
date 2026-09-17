# Reproducibilidad y ejecución

## Prerrequisitos

- Python 3.10 o posterior;
- `uv`;
- Java 17 o posterior y PySpark 4.0.1 para Gold;
- red para instalar dependencias y para la ingesta explícita. Las pruebas y
  transformaciones usan datos locales una vez instalado el entorno.

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

Los datos y resultados locales quedan excluidos de Git. Para una comprobación
aislada se puede usar un directorio temporal:

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
/tmp/tfm-licitaciones-fixture/silver/procurement_events.parquet
/tmp/tfm-licitaciones-fixture/gold/opportunities.jsonl
/tmp/tfm-licitaciones-fixture/gold/opportunities.csv
/tmp/tfm-licitaciones-fixture/gold/technology_summary.csv
/tmp/tfm-licitaciones-fixture/gold/buyer_summary.csv
/tmp/tfm-licitaciones-fixture/gold/quality_report.json
/tmp/tfm-licitaciones-fixture/gold/classifier_evaluation.json
/tmp/tfm-licitaciones-fixture/gold/run_manifest.json
```

Bronze ya no escribe `records.jsonl` y Silver ya no escribe `tenders.jsonl`;
si ese fichero legado existe de ejecuciones anteriores exactamente en el
directorio Silver seleccionado, `run` lo elimina para que no parezca vigente.
Los Parquet de registros, rechazos, tombstones y eventos canónicos conservan
esquema explícito, incluso vacíos. El [contrato](data_contract.md) define
payload, procedencia, motivos, identidad canónica y la unidad de conteo.

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

El corpus raw no se versiona. El manifest de cada ejecución conserva rutas,
tamaños y checksums de sus inputs.
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
evidencia con el reloj de transformación ni con mtime. Ante un ZIP OpenPLACSP
válido sin sidecar, `ingest` realiza una descarga oficial nueva: solo completa
la pareja si los bytes coinciden y registra el timestamp real de ese reintento.
Un sidecar presente pero inválido nunca se repara automáticamente. El refresco
ordinario de meses cacheados sigue pendiente.

Bronze conserva todos los snapshots recuperados, incluidos sus tombstones y
rechazos. Silver canónico (`silver/procurement_events.parquet`) se construye
desde todas esas filas y conserva el historial completo de eventos: revisiones
con nuevo marcador de versión publicado y controles de borrado son filas, no
operaciones destructivas. Un cambio canónico material bajo el mismo
`event_id` (por ejemplo, un mismo `ND` con otro título) hace fallar la
transformación de forma explícita.

La vista legada que consume Gold 0.1 continúa en memoria: recibe únicamente el
snapshot más reciente por fuente, partición y ventana, según el sidecar; por
ello puede colapsar revisiones y no reaplica un tombstone retirado de un ZIP
corregido. El informe muestra `superseded_artifacts` y `superseded_records`
para explicar la diferencia, y el manifiesto separa
`silver_procurement_events` (histórico canónico) de
`legacy_current_state_records` (vista legada), con `silver_source_counts` y
`legacy_source_counts` por fuente. Un empate de timestamps máximos con bytes
distintos falla explícitamente. Consulte el [contrato](data_contract.md) para
las reglas de inputs sin partición y la compatibilidad de tombstones Atom
planos.

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
- `run` conserva el Gold de compatibilidad JSONL/CSV. El flujo analítico actual
  continúa con `build-gold` (Parquet), `build-analytics` y `export-tableau`.

La arquitectura distingue estas limitaciones del trabajo futuro.

## Gold, DuckDB y Tableau

El [README](../README.md#ejecución-local) contiene el recorrido completo con
fixtures. Para procesar los datos de los directorios configurados:

```bash
uv run --locked --with 'pyspark==4.0.1' --with-editable . \
  licitaciones-pipeline build-gold --as-of 2026-09-17
uv run --locked --with-editable . licitaciones-pipeline build-analytics
uv run --locked --with-editable . licitaciones-pipeline export-tableau
```

Fije `--as-of` según la fecha que quiera evaluar y conserve el manifest junto
con el corpus y la revisión de código (`git rev-parse HEAD`). La descarga de
hoy no reconstruye por sí sola el estado conocido en una fecha histórica.
`build-gold` escribe `open_opportunities/` y `gold_manifest.json`; cuenta las
incidencias de estado vigente pero no persiste sus filas. Las salidas del
baseline permanecen separadas. La [guía analítica](analytics.md) documenta las
vistas, sus comprobaciones y el bundle de cinco CSV para Tableau.

Los datos de `data/` no se versionan. Se conservan únicamente fixtures pequeños,
el vocabulario CPV y evidencia seleccionada en `docs/evidence/` y
`experiments/silver_engine_comparison/results/`. Los sidecars de fixtures
contienen fechas sintéticas; no son evidencia de descargas reales.

## Validación completa y comprobaciones rápidas

Las pruebas utilizan `unittest`, no requieren consultas a fuentes oficiales y
pueden instalar sus dependencias desde red. Con PySpark y Java disponibles:

```bash
uv run --locked --with 'pyspark==4.0.1' --with-editable . \
  python -m unittest discover -s tests -v
uv run --locked --with 'pyspark==4.0.1' --with-editable . \
  python -m unittest discover -s experiments/silver_engine_comparison/tests -t . -v
```

Sin el extra PySpark las pruebas Spark se omiten. La configuración CI está en
[tests.yml](../.github/workflows/tests.yml). Los smoke tests disponibles son
el recorrido de fixtures del README y el perfil `tiny` del benchmark:

```bash
uv run --locked --with-editable . python -m tfm_licitaciones.bench_silver \
  --profile tiny --seed 7
uv run --locked --with-editable . python tests/fuzz_silver_guard.py \
  --iterations 100 --seed 20260917
```

El fuzzer es una comprobación diferencial acotada, no una prueba universal de
paridad. El [experimento de motores](../experiments/silver_engine_comparison/README.md)
explica el protocolo, la referencia congelada y la evidencia conservada. No se
confunden los tiempos de los smoke tests con resultados de escalabilidad.
