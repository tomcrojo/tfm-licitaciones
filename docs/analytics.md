# Capa analítica DuckDB

Frontera implementada:

```text
Gold open_opportunities Parquet
        -> DuckDB views (src/tfm_licitaciones/analytics_duckdb.py, sql/duckdb/)
        -> Tableau CSV exports (src/tfm_licitaciones/tableau_export.py)
        -> Tableau workbook / consumo manual
```

Gold sigue siendo la fuente de verdad. La capa analítica **solo lee** el
Parquet Gold y las referencias existentes mediante vistas: no redefine
current-state ni la política open/actionable, no toca Silver, no inventa
identidad y no duplica la lógica de negocio Spark. Una reconstrucción desde
el mismo Gold produce la misma semántica porque todo objeto del fichero
`.duckdb` es una vista determinista. La exportación Tableau consume
**exclusivamente esas vistas DuckDB**: no vuelve a leer Gold, no repite joins
ni agregaciones y no introduce semántica de negocio adicional.

## Reconstrucción

```bash
uv run --with-editable . licitaciones-pipeline build-analytics \
  --gold-dir data/gold \
  --analytics-dir data/analytics \
  --reference-dir data/reference
```

Los tres directorios admiten también resolución desde `config/pipeline.json`
(claves `storage.gold_dir`, `storage.analytics_dir`, `storage.reference_dir`).
La salida imprime la ruta del fichero `licitaciones.duckdb`, las vistas
creadas y los conteos de reconciliación. El build es siempre una
reconstrucción completa: un fichero existente se elimina antes de crearse,
de forma que no sobreviven vistas obsoletas.

**Limitación de portabilidad:** DuckDB persiste las definiciones de vista
con las rutas literales resueltas en tiempo de build (rutas absolutas del
Gold y de la dimensión CPV). Mover el repositorio o regenerar el Gold en
otra ubicación no rompe los datos, pero las vistas dejan de apuntar al
Parquet correcto; el comando `build-analytics` documentado arriba es la
forma reproducible de reconstruir. El fichero no contiene datos de negocio
copiados, solo vistas, por lo que regenerarlo es barato.

## Exportación Tableau

La exportación reproducible se genera desde el fichero DuckDB existente:

```bash
uv run --with-editable . licitaciones-pipeline export-tableau \
  --analytics-dir data/analytics \
  --exports-dir data/exports
```

`--analytics-dir` y `--exports-dir` son opcionales y, si se omiten, se
resuelven desde `storage.analytics_dir` y `storage.exports_dir` en
`config/pipeline.json`. Los artefactos quedan bajo
`<exports_dir>/tableau/`. Son **derivados y regenerables**: no se versionan y
pueden reconstruirse siempre a partir de la misma DuckDB analítica.

| Fichero | Vista DuckDB | Grano / orden congelado |
| --- | --- | --- |
| `open_opportunities.csv` | `open_opportunities` | una fila por `procedure_id`; `ORDER BY procedure_id` |
| `opportunity_cpv.csv` | `opportunity_cpv` | una fila por `(procedure_id, cpv_code, cpv_position)`; `ORDER BY procedure_id, cpv_position, cpv_code` |
| `buyer_summary.csv` | `buyer_summary` | una fila por identidad analítica de comprador; `ORDER BY identity_basis, buyer_id, buyer_name` |
| `cpv_summary.csv` | `cpv_summary` | una fila por `cpv_code`; `ORDER BY cpv_code` |
| `opportunities_monthly.csv` | `opportunities_monthly` | una fila por `(month, source)`; `ORDER BY month, source` |

La vista auxiliar `opportunities_without_publication_date` permanece en
DuckDB para observabilidad y reconciliación, pero no se incluye en el bundle
Tableau principal: no aporta un grano adicional necesario para el dashboard.

### Serialización CSV

- UTF-8, cabecera explícita y delimitador coma.
- `DATE` se escribe como `YYYY-MM-DD`.
- `TIMESTAMP`/`TIMESTAMPTZ` se exporta en UTC con formato
  `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
- `NULL` se representa como campo CSV vacío de forma consistente.
- Los `DECIMAL` pasan directamente por `COPY` de DuckDB; no se convierten a
  `float`.
- `open_opportunities.cpv_codes` mantiene la fila base y se serializa como
  JSON compacto estable (por ejemplo `["48000000","72210000"]`); el grano
  relacional CPV está en `opportunity_cpv.csv` y no se explota la fila base.

El comando verifica primero que exista `licitaciones.duckdb` y que estén
todas las views requeridas. Para cada export compara el recuento CSV con el
recuento de la view, valida cabeceras y UTF-8 y solo entonces publica el
fichero. Un segundo export sobre la misma DuckDB produce el mismo contenido
y orden lógico.

Se genera además `tableau_export_manifest.json`, con versión de formato,
ruta de la DuckDB fuente, fichero/vista y `row_count` de cada CSV. El
manifest no contiene tiempo de negocio ni un timestamp de ejecución: no es
necesario para reproducir el bundle y así permanece determinista.

## Vistas y granos

Todas las vistas viven versionadas en `sql/duckdb/` y se crean en orden
numérico. `dim_cpv` tiene dos variantes (ver política CPV).

| Vista | Grano | Significado |
| --- | --- | --- |
| `open_opportunities` | una fila por `procedure_id` | Proyección 1:1 del Gold principal: todos los campos Gold con su significado y tipos intactos. Nunca filtra ni deriva columnas de negocio. |
| `dim_cpv` | una fila por `cpv_code` | Vista sobre `data/reference/cpv_codes.parquet` ([CPV](cpv-reference.md)) o stub vacío tipado si la dimensión no existe. |
| `opportunity_cpv` | una fila por `(procedure_id, cpv_code, cpv_position)` | Explosión del array canónico `cpv_codes` con la posición original (0-based) preservada y atributos oficiales CPV por LEFT JOIN. |
| `buyer_summary` | una fila por identidad analítica de comprador | Agregados por comprador desde Gold: `opportunities_count`, `total_estimated_value`, `avg_estimated_value`, `first/last_publication_date`, `earliest_deadline`, `distinct_cpv_count`. |
| `cpv_summary` | una fila por `cpv_code` | Agregados por categoría CPV desde `opportunity_cpv` (ver política de atribución). |
| `opportunities_monthly` | una fila por `(month, source)` | Agregados mensuales por `publication_date`. |
| `opportunities_without_publication_date` | una fila por `source` | Observabilidad de las oportunidades sin `publication_date`: nunca reciben una fecha inventada. |

## Política CPV multi-valued

- `open_opportunities` nunca se modifica: el array `cpv_codes` se conserva
  intacto y una oportunidad con varios CPV sigue siendo una fila.
- `opportunity_cpv` es la única vista explotada: conserva **todas** las
  ocurrencias con su `cpv_position` original (determinista), incluidos los
  códigos unmatched (`cpv_matched = false`, atributos oficiales null). No se
  elimina ni corrige ningún código publicado.
- Si existe `data/reference/cpv_codes.parquet`, la vista `dim_cpv` se valida
  (clave no nula y única) y aporta `label_es`, `label_en`, `level`,
  `parent_code`, `is_leaf` y `cpv_matched` por LEFT JOIN. Si no existe, la
  capa sigue funcionando con el stub vacío tipado: todos los códigos quedan
  `cpv_matched = false` con atributos null. Reconstruir tras crear la
  dimensión restaura los atributos oficiales. La dimensión Parquet se
  materializa con el comando documentado en
  [cpv-reference](cpv-reference.md) desde el CSV oficial versionado.

### Atribución de importes por categoría

`cpv_summary` atribuye el `estimated_value` completo **a cada categoría CPV**
que lleva la oportunidad: una oportunidad con N códigos contribuye N veces
al total global de categorías. Ese es el significado documentado de
`total_estimated_value`/`avg_estimated_value` en `cpv_summary` y no debe
usarse para reconciliar totales globales; la vista base
`open_opportunities` es el único grano global. Los importes se mantienen
DECIMAL en toda la cadena (`DECIMAL(20,2)` en origen,
`DECIMAL(38,2)`/`DECIMAL(38,6)` en agregados); las medias se redondean
explícitamente a 6 dígitos fraccionarios, nunca a float silencioso.

## Identidad de comprador

`buyer_summary` no inventa IDs de comprador. Política explícita:

- filas con `buyer_id` agrupan por `buyer_id` (`identity_basis = 'buyer_id'`);
  `buyer_name` es el máximo lexicográfico de los nombres publicados para ese
  id (determinista, sin reescribir Gold);
- filas sin `buyer_id` agrupan por el `buyer_name` publicado
  (`identity_basis = 'buyer_name'`), de modo que compradores distintos sin
  id nunca se mezclan entre sí; las oportunidades sin id ni nombre quedan
  observables en el bucket `(null, null)`.

`distinct_buyers` en `cpv_summary`/`opportunities_monthly` usa
`count(DISTINCT buyer_id)` y por tanto no cuenta oportunidades sin
`buyer_id` (visibles en `buyer_summary`).

## Limitación DIR3

Esta capa no incorpora atributos DIR3: expone el `buyer_id`/`buyer_name`
canónicos de Gold. El enriquecimiento DIR3 existe hoy en la frontera Gold
solo como métricas medidas (`gold_enrichment`), no como columnas publicadas,
por lo que no hay nada que proyectar aún; añadirlo será un cambio separado
cuando DIR3 forme parte del contrato Gold.

## Guards de consistencia

El builder falla de forma observable si:

- no existe `open_opportunities/*.parquet` bajo `gold_dir` (mensaje con el
  comando `build-gold` requerido);
- las columnas/tipos de la vista base divergen del contrato Gold
  (`gold_contract.GOLD_OPEN_OPPORTUNITIES_FIELDS`; los timestamps aceptan
  `TIMESTAMP` y `TIMESTAMP WITH TIME ZONE` según cómo se materializó el
  Parquet);
- la dimensión CPV existe pero su clave no es única o contiene nulos;
- el recuento de la vista base no coincide con el Parquet Gold, o
  `procedure_id` deja de ser único/no nulo;
- `opportunity_cpv` no conserva exactamente todas las ocurrencias CPV;
- `buyer_summary` no cuadra todas las oportunidades del Gold;
- las vistas mensuales (fechadas + sin fecha) no reconcilian con el Gold.

Un build que falla en cualquier guard elimina el fichero `.duckdb` generado
a medias: nunca queda un artefacto no verificado.

Tests DuckDB: `tests/test_analytics_duckdb.py` (fixtures Parquet mínimos
escritos con DuckDB, sin PySpark ni red). Tests de exportación Tableau:
`tests/test_tableau_export.py` (DuckDB local mínimo, sin PySpark, red ni
Tableau instalado).

```bash
uv run --with-editable . python -m unittest tests.test_analytics_duckdb -v
uv run --with-editable . python -m unittest tests.test_tableau_export -v
```
