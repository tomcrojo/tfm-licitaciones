# Contrato de datos actual

Este documento describe la implementación 0.1. La arquitectura objetivo y el
futuro contrato `procurement_events` se recogen en
[architecture.md](architecture.md). Mantener ambos estados separados evita
presentar como implementado un modelo que todavía está en migración.

## Silver: `TenderRecord`

Grano actual: una versión consolidada por `(source, tender_id)`. Las revisiones
de OpenPLACSP se pliegan mediante el timestamp `updated` y los identificadores
marcados como tombstone se excluyen.

| Campo | Tipo | Regla actual |
| --- | --- | --- |
| `tender_id` | string | Identificador proporcionado por la fuente |
| `source` | string | `ted`, `placsp` o `boe`; BOE está deshabilitado en la configuración normal |
| `title` | string | Título disponible en el aviso |
| `summary` | string | Descripción disponible; cadena vacía si falta |
| `buyer` | string/null | Nombre del organismo comprador |
| `published_date` | date/null | TED: `PD`; PLACSP: actualmente deriva de `updated` |
| `amount` | number/null | Importe no negativo cuando puede interpretarse |
| `currency` | string/null | Moneda publicada; TED se marca como EUR cuando la API aporta importe normalizado |
| `country` | string/null | País o ámbito publicado |
| `url` | string/null | URL pública del aviso |
| `cpv_main` | string/null | Primer código CPV disponible |
| `buyer_id` | string/null | DIR3 cuando OpenPLACSP lo publica |
| `region` | string/null | Región textual de ejecución |
| `status` | string/null | Estado publicado por OpenPLACSP |
| `raw` | object | Payload source-specific conservado para trazabilidad |

## Gold: oportunidad 0.1

Gold aplana `TenderRecord` y añade la clasificación y el resultado del enlace:

| Campo | Tipo | Regla actual |
| --- | --- | --- |
| `category` | string | Primera categoría con keywords; fallback CPV; `Other` si no hay coincidencia |
| `category_source` | string | `keywords`, `cpv` o `none` |
| `technology_score` | int | Número de keywords distintas encontradas |
| `matched_keywords` | list | Keywords normalizadas que coincidieron |
| `dup_group` | int/null | Grupo asignado por el linkage heurístico |
| `is_canonical` | bool | Marca el representante seleccionado del grupo |
| `duplicate_of` | string/null | `tender_id` del representante actual |

Los CSV `technology_summary` y `buyer_summary` se calculan sobre registros
marcados como canónicos.

## Evaluación actual

`classifier_evaluation.json` compara la señal de keywords con prefijos CPV
inequívocos configurados. CPV actúa como proxy, no como anotación humana. La
cobertura y el soporte por clase forman parte del resultado y deben citarse al
interpretar las métricas.

La implementación actual incluye en el macro-F1 clases con soporte cero. Esa
agregación se corregirá antes de utilizarla como resultado académico; el
artefacto versionado se conserva para representar fielmente el baseline.

## Limitaciones conocidas

| Área | Situación actual | Corrección prevista |
| --- | --- | --- |
| Fechas PLACSP | `updated` se reutiliza como `published_date` | Separar publicación y actualización |
| Linkage | La generación de candidatos no excluye la misma fuente | Exigir fuentes diferentes |
| Bloques grandes | Los bloques por encima del límite se omiten | Subdividir o reportar como no evaluados |
| Rechazos | Algunos errores pueden descartarse antes de quality | Contabilizar `parsed`, `accepted`, `rejected` y motivo |
| Completitud | Una partición que falla puede no impedir el run | Registrar esperadas/descargadas y estado incompleto |
| Persistencia | Bronze, Silver y Gold principales usan JSONL/CSV | Migrar por límites a Parquet |
| TED | La configuración aplica una query tecnológica | Hacer el filtro sectorial opcional y downstream |

Estas limitaciones son trabajo pendiente conocido. No invalidan las pruebas del
comportamiento actual, pero impiden presentar todavía la versión 0.1 como la
plataforma P0 terminada.
