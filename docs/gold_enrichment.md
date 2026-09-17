# Enriquecimiento Gold mínimo: CPV y DIR3

Este documento define los dos enriquecimientos que aportan valor directo al
Gold/Tableau final y que pueden incorporarse a Gold sin depender de cómo se
implemente `current_state`. La frontera es la establecida en
[Gold/Spark](gold_spark_contract.md): Silver canónico Parquet → PySpark →
Gold Parquet. Este módulo no resuelve estado vigente, no modifica Silver y no
construye los marts Gold.

Implementación: `src/tfm_licitaciones/gold_enrichment.py` (contratos en el
propio módulo para permitir el desarrollo en paralelo con la rama de
current-state). Tests: `tests/test_gold_enrichment.py`.

## `cpv_enriched`

**Grano:** una fila por ocurrencia de `(event_id, cpv_code)` tras explotar el
array canónico `cpv_codes`. Los múltiples CPV de un evento **nunca se
colapsan**: se conserva el código original, su posición (`cpv_position`,
índice 0-based del array canónico, orden de publicación estable) y se añaden
únicamente los atributos oficiales disponibles en la dimensión de referencia
`data/reference/cpv_codes.parquet` ([CPV](cpv-reference.md)).

Versión 1 (`CPV_ENRICHED_FIELDS`):

| Campo | Tipo | Nulo | Origen |
| --- | --- | --- | --- |
| `event_id` | string | no | Silver canónico |
| `procedure_id` | string | sí | Silver canónico |
| `source` | string | no | Silver canónico |
| `cpv_position` | int | no | `posexplode(cpv_codes)` |
| `cpv_code` | string | no | código original, sin transformar |
| `cpv_matched` | boolean | no | presente en la dimensión oficial |
| `label_es` | string | sí | atributo oficial |
| `label_en` | string | sí | atributo oficial (3 códigos sin etiqueta) |
| `level` | tinyint | sí | atributo oficial (1–7) |
| `parent_code` | string | sí | atributo oficial (nulo en divisiones) |
| `is_leaf` | boolean | sí | atributo oficial |

Semántica:

- left join por `cpv_code` contra la dimensión validada; un código ausente del
  vocabulario se conserva con atributos nulos y `cpv_matched=false`
  (unmatched contabilizado en las métricas);
- `cpv_matched` distingue «código oficial sin etiqueta EN» (match con
  atributo nulo) de «código que no está en la dimensión»;
- un `cpv_codes` con duplicados dentro de un evento viola el contrato
  canónico de Silver y hace fallar el enriquecimiento en la frontera: no se
  vuelve a deduplicar con otra política;
- los eventos sin CPV no producen filas y se contabilizan como
  `events_without_cpv_codes`.

## `buyer_dir3_enriched`

**Grano:** el evento canónico de Silver, sin cambios: todas las columnas
canónicas en su orden, más atributos DIR3 añadidos. El join es
`buyer_id = dir3_code` (left) contra
`data/reference/dir3/dir3_units.parquet` ([DIR3](dir3-reference.md)), la
dimensión fiable ya presente en el repositorio (108.261 unidades orgánicas,
PK `dir3_code` validada en construcción).

`buyer_id`/`buyer_name` **nunca se reescriben**: son los valores canónicos de
Silver y ya son válidos para Gold. DIR3 solo añade atributos oficiales:
`buyer_dir3_matched`, `buyer_dir3_name`, `buyer_dir3_scope`,
`buyer_dir3_entity_type`, `buyer_dir3_hierarchy_level`,
`buyer_dir3_parent_code`, `buyer_dir3_principal_code`, `buyer_dir3_status`,
`buyer_dir3_nif`. El `status` se expone en vez de filtrarse: el snapshot
actual solo contiene `V`, pero un snapshot futuro con unidades E/A/T debe ser
observable por el consumidor.

### Disponibilidad de la dimensión DIR3

La dimensión es un artefacto local que requiere descarga explícita
(`ingest --source dir3` + `dir3`; el host PAe está tras F5/TSPD). Cuando no
existe, `dir3_units_dimension(reference_dir)` devuelve `None` y la decisión
documentada es **saltar el enriquecimiento DIR3 y continuar**: los campos
canónicos `buyer_id`/`buyer_name` ya son válidos para Gold. No se busca una
fuente DIR3 alternativa en tiempo de ejecución ni se inventa un fallback.

## Determinismo y cardinalidad

Ambos enriquecimientos aplican las mismas invariantes:

1. **Clave de dimensión validada antes del join**: `read_cpv_dimension` /
   `read_dir3_dimension` (y los propios `enrich_*`) rechazan claves duplicadas
   o nulas. Con PK validada el join es 1:1 como máximo, por lo que no pueden
   existir hechos «ambiguos»: una dimensión rota falla de forma observable en
   lugar de multiplicar hechos en silencio. Las métricas reportan
   `dimension_duplicate_keys=0` tras superar el guard.
2. **Cardinalidad antes/después**: el número de filas de hechos se comprueba
   idéntico antes y después de cada join (`_assert_row_count_unchanged`).
3. **Unmatched conservados y contabilizados**: left joins; cada función
   devuelve métricas medidas sobre los datos reales (`resolved_*`,
   `unresolved_*`, `distinct_unresolved_cpv_codes`,
   `events_without_buyer_id`, ...).
4. **Esquema explícito**: los datasets de salida se validan con
   `assert_contract_schema` y se persisten con `write_typed_parquet`
   (`order_by` solo ordena la escritura; el consumidor nunca debe usar el
   orden de part-files como identidad, ver
   [Gold/Spark](gold_spark_contract.md)).

## Incorporación a Gold

Ambos datasets son independientes entre sí y del resolutor de current-state:
se unen a cualquier dataset de eventos por `event_id` (CPV) o se consumen
como eventos con atributos añadidos (DIR3). El builder de Gold que salga de
la rama `identity/current-state` puede componerlos sin cambios de contrato;
las versiones (`CPV_ENRICHED_SCHEMA_VERSION`,
`BUYER_DIR3_ENRICHED_SCHEMA_VERSION`) quedan listas para persistirse en el
manifiesto de construcción cuando esos artefactos se produzcan.

## Composición en el Gold principal (`open_opportunities`)

El builder `tfm_licitaciones.gold_open_opportunities` compone ambos
enriquecimientos **solo para métricas medidas**, sin duplicar su lógica:

- CPV: el array canónico `cpv_codes` se conserva intacto en
  `open_opportunities` (una fila por `procedure_id`, sin explosión ni
  colapso); `enrich_cpv` corre sobre el conjunto abierto y sus
  `resolved/unresolved` van al `gold_manifest.json`. No se persiste ningún
  dataset secundario `opportunity_cpv`: el array ya transporta todos los
  códigos publicados.
- DIR3: `enrich_buyers_dir3` corre sobre el conjunto abierto solo para
  métricas; `buyer_id`/`buyer_name` nunca se reescriben y una dimensión
  ausente deja `dir3.available=false` sin bloquear Gold.

## Ejecución

```bash
uv run --with 'pyspark==4.0.1' --with-editable . \
  python -m unittest tests.test_gold_enrichment -v
```
