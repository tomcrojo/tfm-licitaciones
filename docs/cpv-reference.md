# Dimensión de referencia CPV 2008

Fuente de referencia oficial para la taxonomía de categorías de contratación.
Consumible por Silver (resolución de `cpv_codes`) y por productos Gold sin
dependencia de red.

## Origen y fichero fuente

| Aspecto | Valor |
| --- | --- |
| Vocabulario | Common Procurement Vocabulary (CPV) 2008, Reglamento (CE) 213/2008 |
| Editor | Comisión Europea (portal SIMAP/TED) |
| Fichero local | `config/reference/cpv2008_es.csv` (etiquetas ES + EN) |
| Método de captura | descarga puntual consolidada en el repositorio (sin red en pruebas) |
| Cobertura | vocabulario principal completo: 9.454 códigos (medido) |
| Cadencia / revisiones | estático: vocabulario cerrado desde 2008, sin actualizaciones ni borrados |
| Clave natural | `cpv_code` (8 dígitos, cadena; los ceros a la izquierda son significativos) |
| Formato raw | CSV UTF-8 con columnas `code`, `nombre`, `name` |

El vocabulario complementario (códigos `EXXXX`) no está incluido.

## Semántica de la jerarquía

El nivel de cada código se deriva de forma determinista por su sufijo de ceros:

| Nivel | Forma | Ejemplo |
| --- | --- | --- |
| 1 | `XX000000` (división) | `03000000` |
| 2 | `XXX00000` (grupo) | `03100000` |
| 3 | `XXXX0000` (clase) | `03110000` |
| 4 | `XXXXX000` (categoría) | `03111000` |
| 5 | 8 dígitos (detalle) | `03111100` |

El padre estructural de un código es su prefijo significativo rellenado con
ceros. La tabla oficial omite 8 nodos intermedios (`34511000`, `35611000`,
`35612000`, `35811000`, `38527000`, `39250000`, `42924000`, `60110000`): los
32 códigos afectados (medido) resuelven su `parent_code` al ancestro existente
más cercano, por lo que la dimensión nunca contiene claves ajenas huérfanas.
`is_leaf` indica que ningún otro código de la tabla tiene ese código por padre.

## Esquema de salida (`data/reference/cpv_codes.parquet`)

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `cpv_code` | String | código oficial de 8 dígitos |
| `label_es` | String | etiqueta oficial en español (`nombre`) |
| `label_en` | String | etiqueta oficial en inglés (`name`; nula en 3 códigos sin etiqueta) |
| `level` | Int8 | nivel 1-5 |
| `parent_code` | String | ancestro existente más cercano; nulo en las 45 divisiones |
| `is_leaf` | Boolean | verdadero si el código no tiene hijos |

La salida se ordena por `cpv_code` y es reproducible: reconstruir desde el
CSV produce un DataFrame idéntico. No se commita: se regenera con:

```sh
uv run --locked python -c "from tfm_licitaciones.cpv import build_cpv_dimension, DEFAULT_CPV_SOURCE; from tfm_licitaciones.io import write_parquet; from pathlib import Path; print(write_parquet(Path('data/reference/cpv_codes.parquet'), build_cpv_dimension(DEFAULT_CPV_SOURCE)))"
```

## Limitaciones conocidas

- Sin vocabulario complementario CPV (`EXXXX`).
- Los códigos `14820000`, `14830000` y `14930000` carecen de etiqueta inglesa
  en la fuente oficial: se almacenan como nulos en `label_en`.
- Las etiquetas se conservan tal cual publica la fuente (incluido el punto
  final); no se normaliza el texto.
