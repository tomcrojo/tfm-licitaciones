# Evidencia de ejecución con datos reales

## Identificación y alcance

El 17 de septiembre de 2026 se ejecutó la cadena TED/OpenPLACSP → Bronze →
Silver → Gold Spark → DuckDB → CSV Tableau sobre una ventana solicitada de
**2026-01-01 a 2026-06-30**. El resultado contiene **359.304 avisos Bronze**,
**359.180 eventos Silver** y **14.763 oportunidades abiertas**. Son unidades
distintas: no deben presentarse como 359.304 licitaciones únicas.

La evidencia procede del directorio local `tfm-e2e-from-source`, localizado y
verificado durante la preparación de la entrega. Los archivos originales
pequeños se conservan bajo [evidence/real-run-2026-09-17](evidence/real-run-2026-09-17/).
Este documento no describe la demo sintética ni el benchmark de motores.

| Dato | Evidencia |
| --- | --- |
| Commit registrado por el script | `9fa795037f1ee008be164fbd59e4ed54e16428da` |
| Inicio / fin del script | 2026-09-17 21:55:54 / 22:15:46, UTC+02:00 |
| Fecha de política Gold (`as_of`) | `2026-06-30` |
| Raw consumido | 6 ZIP mensuales PLACSP y 1 JSONL TED |
| Tamaño total Raw | 1.224.424.182 bytes |
| PySpark solicitado explícitamente | `4.0.1` |
| DuckDB registrado por el builder | `1.5.5` |

El [log](evidence/real-run-2026-09-17/e2e.log) registra el commit corto `9fa7950`;
el identificador completo se resuelve en Git. Se adjuntan
[configuración](evidence/real-run-2026-09-17/recorded-code/config/pipeline.json),
[declaración de dependencias](evidence/real-run-2026-09-17/recorded-code/pyproject.toml)
y [lock](evidence/real-run-2026-09-17/recorded-code/uv.lock), reconstruidos desde
ese commit. No son una captura adicional del entorno durante la ejecución:
el log no registra `git status`, hardware ni versiones exactas de Python/Java.
No se atribuyen esas versiones retrospectivamente.

El código productivo, configuración, SQL y dependencias de ese commit coinciden
con los de `eed0258`, la consolidación posterior de la entrega; esta incorporó
la prueba integrada, sin cambiar las transformaciones.

## Resultados y reconciliación

| Etapa / unidad de conteo | Resultado |
| --- | ---: |
| Bronze: avisos PLACSP aceptados | 349.399 |
| Bronze: avisos TED aceptados | 9.905 |
| Bronze: avisos parseados / aceptados / rechazados | 359.304 / 359.304 / 0 |
| Bronze: errores de documento / control | 0 / 0 |
| Bronze: observaciones de tombstones, aparte de los avisos | 1.046 |
| Silver: eventos canónicos PLACSP / TED | 349.275 / 9.905 |
| Silver: total de eventos canónicos | 359.180 |
| Spark: filas de estado vigente | 130.118 |
| Spark: incidencias de estado vigente | 9.905 |
| Gold: abiertas | 14.763 |
| Gold: excluidas por estado cerrado | 113.957 |
| Gold: excluidas por borrado | 521 |
| Gold: excluidas por evidencia insuficiente | 877 |
| Gold: excluidas por plazo vencido | 0 |

Los avisos y tombstones se consolidan en eventos; las observaciones repetidas
no añaden un evento canónico nuevo. Las razones Gold reconcilian las 130.118
filas de estado vigente: `14.763 + 113.957 + 521 + 877 = 130.118`.
Las 9.905 incidencias se contabilizan **aparte**; no se presentan como resueltas.
El builder no persistió sus filas individuales en esta ejecución, por lo que
el manifest no permite auditar aquí su distribución por motivo.

Fuentes: [informe Bronze](evidence/real-run-2026-09-17/out/bronze/ingestion_report.json),
[manifest del pipeline](evidence/real-run-2026-09-17/out/gold/run_manifest.json)
y [manifest Gold](evidence/real-run-2026-09-17/out/gold2/gold_manifest.json).
Los conteos de los Parquet retenidos y la unicidad de los 14.763 `procedure_id`
Gold se contrastaron con esos manifests en la auditoría de evidencia.

El pipeline también generó **138.995 filas de estado vigente legado** y
**136.608 filas canónicas de su baseline de linkage**. Esas cifras pertenecen
a la ruta de compatibilidad de `run`, no al estado vigente Spark ni al Gold
`open_opportunities`. El [informe de calidad legado](evidence/real-run-2026-09-17/out/gold/quality_report.json)
indica `passed=true`; no certifica la completitud del corpus ni la calidad de
todas las etapas. El informe Bronze tiene su propio estado.

## Productos Tableau y referencias

El [manifest de exportación](evidence/real-run-2026-09-17/out/exports/tableau/tableau_export_manifest.json)
registra los cinco CSV siguientes; se verificaron sus filas y checksums:

| CSV | Filas |
| --- | ---: |
| `open_opportunities.csv` | 14.763 |
| `opportunity_cpv.csv` | 28.730 |
| `buyer_summary.csv` | 4.027 |
| `cpv_summary.csv` | 3.751 |
| `opportunities_monthly.csv` | 0 |

Las **14.763 oportunidades carecen de `publication_date` canónica**. El CSV
mensual tiene únicamente cabecera, y la vista auxiliar
`opportunities_without_publication_date` contabiliza esas oportunidades.
No se sustituyó la fecha de publicación por la de actualización.

Gold resolvió sus 28.730 ocurrencias CPV con el vocabulario CSV oficial
(`dimension_source=csv-rebuild`, 9.454 códigos). En cambio, **DuckDB no encontró
`reference/cpv_codes.parquet`**: declaró `cpv_dimension_available=false`,
`cpv_matched=0` y `cpv_unmatched=28730`. Por eso los CSV históricos no contienen
etiquetas CPV resueltas en la capa analítica. DIR3 tampoco estaba disponible.
Estas ausencias se conservan en la evidencia; no se regeneraron los resultados
para hacerlos parecer enriquecidos. La demo nueva sí materializa CPV antes de
construir DuckDB y documenta su comportamiento por separado.

## Duraciones observadas y límites

El script midió segundos enteros con el reloj de pared:

| Fase | Segundos registrados |
| --- | ---: |
| Ingesta TED/PLACSP en paralelo | 179 |
| `run`: Bronze, Silver y baseline legado | 956 |
| `build-gold` | 56 |
| `build-analytics` | 1 |
| `export-tableau` | 0 (resolución de un segundo) |

Son tiempos de **una ejecución local**, no un benchmark repetido ni una prueba
de escalabilidad. Los sidecars de enero y febrero PLACSP registran recuperaciones
anteriores al inicio del script (19:40:25 y 19:47:13 UTC); el comando reutilizó
esos archivos. Por tanto, **179 s no mide una descarga de todo el corpus desde
cero**. Las restantes recuperaciones registradas ocurrieron entre 19:56:39 y
19:58:53 UTC. El comentario «ventana jun» del log es una etiqueta abreviada
imprecisa: los argumentos del script, los nombres y los sidecars acreditan
enero–junio.

La extracción es posterior a la ventana solicitada. `as_of=2026-06-30` fija
la evaluación de la política, pero **no reconstruye el estado conocido el
30 de junio**. El mapeo actual de Silver no puebla `deadline`; cero exclusiones
por plazo no acredita que todos los plazos estuvieran vigentes. TED usa los
filtros de país y términos tecnológicos del config adjunto; no representa todo
TED. Se verificaron los siete archivos consumidos, no una certificación global
de cobertura de las fuentes oficiales.

## Descarga opcional de resultados

Los [cinco CSV y su evidencia están disponibles en una carpeta pública de Google Drive](https://drive.google.com/drive/folders/1-UEy9W_3cmbVlUrCLwmiMFmBwI_H_gXc?usp=sharing),
con acceso de lectura para cualquiera con el enlace. Los archivos sin comprimir
ocupan **16.991.655 bytes (16,99 MB)**: los CSV están en `out/exports/tableau/`
y la evidencia en `docs/evidence/real-run-2026-09-17/`. No contienen el corpus
Raw ni los Parquet. El archivo `SHA256SUMS` permite verificar los CSV con
`sha256sum -c SHA256SUMS` desde la raíz de la descarga, conservando las rutas.
La demo y la evidencia versionadas se pueden consultar y ejecutar sin Drive.

La carpeta conserva también el ZIP opcional de **2.737.168 bytes (2,74 MB)**.

SHA-256 del ZIP publicado:

```text
b3ccf1edc3fe3c0ebe3f9234b60ab5bd157e286e1484a7ecdc0cf0c9a7fa655f
```

Para el límite de 50 MB del Campus Virtual, el código, la demo y la evidencia
ligera se empaquetan aparte de los datos. La descarga en Drive es opcional;
si se adjunta también el ZIP de resultados, su tamaño debe sumarse al de la
memoria y al resto de archivos de entrega. El corpus Raw completo permanece
como respaldo local para reconstruir exactamente este snapshot.

## Verificación del snapshot retenido

El [inventario de artefactos](evidence/real-run-2026-09-17/artifact_index.json)
registra tamaño, SHA-256 y, para CSV/Parquet, filas de **37 archivos**. Los hashes
Raw se contrastaron con el manifest original y sus sidecars; los CSV con
[fresh_tableau_sha256.txt](evidence/real-run-2026-09-17/fresh_tableau_sha256.txt).
Los hashes Parquet se calcularon por primera vez durante esta auditoría, no se
atribuyen al instante de la ejecución histórica. El
[resultado de verificación](evidence/real-run-2026-09-17/verification.json)
permite identificar todos los archivos comprobados.

Para comprobar una copia que conserve el árbol completo de esa ejecución:

```bash
uv run --locked --with-editable . python scripts/verify_run_evidence.py \
  --run-root /ruta/al/snapshot/tfm-e2e-from-source
```

El verificador solo lee: falla ante archivos ausentes, bytes diferentes o
conteos distintos. El repositorio conserva los metadatos pequeños; **no incluye
los payloads Raw ni los Parquet/CSV completos**. Sus hashes no permiten
reconstruirlos: para repetir esta verificación se necesita la copia retenida.
Las rutas absolutas en los manifests y el script original se mantienen como
evidencia de origen, no como rutas que deba tener el evaluador.

## Repetición del procedimiento

Desde un checkout del commit registrado, con `uv` y Java 17 disponibles, los
comandos equivalentes con rutas elegidas por el evaluador son:

```bash
TFM_EVIDENCE_RUN="$PWD/data/real-run"
uv run --locked --with-editable . licitaciones-pipeline ingest \
  --source placsp --start 2026-01-01 --end 2026-06-30 --raw-dir "$TFM_EVIDENCE_RUN/raw"
uv run --locked --with-editable . licitaciones-pipeline ingest \
  --source ted --start 2026-01-01 --end 2026-06-30 --raw-dir "$TFM_EVIDENCE_RUN/raw"
uv run --locked --with-editable . licitaciones-pipeline run \
  --raw-dir "$TFM_EVIDENCE_RUN/raw" --output-root "$TFM_EVIDENCE_RUN/out"
uv run --locked --with 'pyspark==4.0.1' --with-editable . licitaciones-pipeline build-gold \
  --silver-dir "$TFM_EVIDENCE_RUN/out/silver" --gold-dir "$TFM_EVIDENCE_RUN/out/gold2" \
  --reference-dir "$TFM_EVIDENCE_RUN/out/reference" --as-of 2026-06-30
uv run --locked --with-editable . licitaciones-pipeline build-analytics \
  --gold-dir "$TFM_EVIDENCE_RUN/out/gold2" --analytics-dir "$TFM_EVIDENCE_RUN/out/analytics" \
  --reference-dir "$TFM_EVIDENCE_RUN/out/reference"
uv run --locked --with-editable . licitaciones-pipeline export-tableau \
  --analytics-dir "$TFM_EVIDENCE_RUN/out/analytics" --exports-dir "$TFM_EVIDENCE_RUN/out/exports"
```

Usar una raíz nueva para una nueva descarga. Para repetir solo transformaciones,
copiar primero el Raw retenido **con sus sidecars** y omitir los dos `ingest`.
No fabricar timestamps para un corpus sin evidencia. Una descarga nueva puede
contener correcciones, timestamps y checksums diferentes; no se promete que
repita exactamente estos conteos. El procedimiento anterior deja CPV Parquet
y DIR3 ausentes, como en la ejecución documentada. Construir esas referencias
sería otra ejecución que debe identificarse como tal.

El [script original](evidence/real-run-2026-09-17/run_e2e.sh) queda archivado para
auditar los argumentos y el paralelismo de la ingesta; sus rutas absolutas no
son un entrypoint portable. La [demo](../examples/demo/README.md) ofrece una
comprobación breve, determinista y sin consultas a las fuentes.
