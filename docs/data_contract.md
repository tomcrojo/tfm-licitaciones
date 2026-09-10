# Contrato de datos

## Silver: `TenderRecord`

| Campo | Tipo | Regla |
| --- | --- | --- |
| `tender_id` | string | Identificador natural de la fuente (número de aviso TED o id numérico PLACSP) |
| `source` | string | `ted`, `boe` o `placsp` |
| `title` | string | Texto del aviso; obligatorio para gold |
| `summary` | string | Descripción disponible, vacía si no existe |
| `buyer` | string/null | Organismo comprador |
| `published_date` | date/null | Fecha ISO normalizada (TED `PD`; PLACSP fecha de `updated`) |
| `amount` | number/null | Importe no negativo; opcional a nivel de aviso |
| `currency` | string/null | Informado siempre que hay importe: `currencyID` PLACSP; `EUR` inferido en TED (Search API normaliza valores a EUR) |
| `country` | string/null | País o ámbito de publicación |
| `url` | string/null | Enlace público original |
| `cpv_main` | string/null | Código CPV principal (TED `PC`[0]; PLACSP `ItemClassificationCode`[0]) |
| `buyer_id` | string/null | Identificador DIR3 del comprador (PLACSP) |
| `region` | string/null | NUTS/CountrySubentity de ejecución (PLACSP) |
| `status` | string/null | Estado del expediente (PLACSP `ContractFolderStatusCode`) |
| `raw` | object | Campos source-specific para trazabilidad |

El plegado de actualizaciones garantiza una fila por `(source, tender_id)`;
los avisos anulados (tombstones PLACSP) se retiran de silver.

## Gold: oportunidad

Gold añade clasificación y enlace sobre cada aviso de silver:

| Campo | Tipo | Regla |
| --- | --- | --- |
| `category` | string | Primera categoría con keywords coincidentes; si no hay, categoría del mapa CPV inequívoco; si no, `Other` |
| `category_source` | string | `keywords`, `cpv` o `none`: qué señal decidió la categoría |
| `technology_score` | int | Número de keywords únicas detectadas en título y resumen (señal keywords) |
| `matched_keywords` | list | Keywords normalizadas sin acentos que coincidieron |
| `dup_group` | int/null | Identificador de grupo de linkage cuando el aviso enlaza con otro |
| `is_canonical` | bool | `true` en el aviso representativo del grupo (fecha más antigua, luego título más largo) |
| `duplicate_of` | string/null | `tender_id` del canónico para los no canónicos |

Los marts (`technology_summary.csv`, `buyer_summary.csv`) se calculan solo
sobre filas canónicas: cuentan procedimientos únicos, no avisos.

## Evaluación del clasificador: `classifier_evaluation.json`

Métricas de precisión, recall, F1 y soporte por categoría comparando el
señal de keywords contra etiquetas CPV inequívocas, más macro-F1 y cobertura.
Los registros decididos por CPV quedan fuera para evitar circularidad.

## Trazabilidad mínima

| Criterio | Código | Evidencia | Interpretación | Estado |
| --- | --- | --- | --- | --- |
| Integración de fuentes | `fetch.py`, `normalize.py`, `atom.py` | `bronze/records.jsonl`, `run_manifest.json` | Cada fila conserva fuente, fichero y payload; PLACSP con CA FNMT | OK |
| Plegado de actualizaciones | `pipeline.fold_latest_updates` | `ingestion.updates_folded` en manifest | Una fila por clave; se conserva la última revisión | OK |
| Normalización | `models.py`, `normalize.py` | `silver/tenders.jsonl` | TED/PLACSP comparten contrato comparable con CPV, DIR3 y EUR | OK |
| Calidad | `quality.py` | `gold/quality_report.json` | Las puertas muestran valor, umbral y estado | OK |
| Clasificación | `classify.py` | `opportunities.csv` | Score y keywords explican cada categoría; `category_source` audita la señal | OK |
| Evaluación vs CPV | `evaluation.py` | `gold/classifier_evaluation.json` | P/R/F1 por categoría y macro-F1 contra proxy CPV | OK |
| Linkage | `linkage.py` | `linkage` en manifest, `dup_group`/`is_canonical` en gold | Pares candidatos, enlazados y grupos; canónico por grupo | OK |
| Explotación | `marts.py` | `technology_summary.csv`, `buyer_summary.csv` | Procedimientos únicos (canónicos) listos para análisis | OK |
| Reproducibilidad | `pipeline.py`, `tests/` | smoke test y checksums | El mismo raw produce las mismas filas de negocio | OK |
