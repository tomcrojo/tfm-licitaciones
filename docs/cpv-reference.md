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
| Clave natural | cuerpo de 8 dígitos como cadena (ver «dígito de control») |
| Formato raw | CSV UTF-8 con columnas `code`, `nombre`, `name` |

El vocabulario complementario (códigos `EXXXX`) no está incluido.

## Cuerpo de 8 dígitos y dígito de control

La jerarquía reside íntegramente en el cuerpo de 8 dígitos. El noveno dígito
(separado por guion, `XXXXXXXX-Y`) es un detector de erratas sin semántica
clasificatoria: no es un nivel, no forma parte de ninguna máscara y jamás debe
intervenir en padres, niveles u hojas. La clave canónica de la dimensión es el
cuerpo de 8 dígitos como cadena; los ceros a la izquierda (`03`, `09`) son
significativos y el parseo como entero los corrompería.

Nota de validación: los dígitos de control de la publicación oficial de 2008
son internamente inconsistentes (error reconocido por OP-TED, discusión ePO
#589). Por tanto nunca se rechazará un dato entrante por mismatch de dígito de
control; si una fuente lo incluye (p. ej. TED), se descarta en el límite de
Bronze y puede conservarse como procedencia raw.

## Semántica de la jerarquía

Los dos primeros dígitos se leen como bloque (división) y cada dígito
siguiente añade un nivel de clasificación, hasta 7 niveles. Solo los cuatro
primeros tienen denominación clásica en la normativa; los niveles 5–7 son
grados adicionales de precisión sin nombre oficial propio y se nombran
internamente `level 5/6/7`.

| Nivel | Forma | Denominación | Ejemplo | Códigos (medido) |
| --- | --- | --- | --- | --- |
| 1 | `XX000000` | división | `03000000` | 45 |
| 2 | `XXX00000` | grupo | `03100000` | 272 |
| 3 | `XXXX0000` | clase | `03110000` | 1.002 |
| 4 | `XXXXX000` | categoría | `03111000` | 2.379 |
| 5 | `XXXXXX00` | — (precisión) | `03212200` | 3.140 |
| 6 | `XXXXXXX0` | — (precisión) | `03212210` | 1.811 |
| 7 | `XXXXXXXX` | — (precisión) | `03212211` | 805 |

El nivel se calcula desde la propia cadena (`k` = posición del último dígito
distinto de cero; `k ≤ 2` ⇒ nivel 1, en otro caso nivel `k − 1`), nunca
contando saltos desde la raíz, porque las cadenas publicadas pueden saltarse
niveles. El padre estructural se obtiene poniendo a cero el último dígito
significativo; las divisiones (`30000000`, `14000000`) son raíces: poner a
cero `14000000` daría el inexistente `10000000`.

### Validez estructural de los códigos

En CPV 2008 los ceros solo aparecen como relleno final o dentro del bloque de
división (verificado: 0 violaciones en los 9.454 códigos). Un código con un
cero entre el bloque de división y su último dígito significativo (p. ej.
`03210200`) no es un CPV 2008 válido y se trata como error de calidad del
dato entrante, nunca se «corrige» silenciosamente.

### Huecos del vocabulario publicado

La tabla oficial omite 11 nodos intermedios (`30192120`, `34511000`,
`35611000`, `35612000`, `35811000`, `38527000`, `39250000`, `42924000`,
`44115300`, `44613100`, `60110000`): los 35 códigos afectados (medido; cada
hueco salta exactamente un nivel) resuelven su `parent_code` al ancestro
publicado más cercano. La dimensión contiene solo códigos oficiales: nunca se
materializan nodos sintéticos.

Advertencia semántica: el punto de engarce tras resolver un hueco es
estructural, no semántico (`30192121` «Bolígrafos» cuelga de `30192100`
«Gomas de borrar»). Es válido para recuentos y rollups; no debe usarse para
inferir significado.

## Esquema de salida (`data/reference/cpv_codes.parquet`)

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `cpv_code` | String | cuerpo oficial de 8 dígitos |
| `label_es` | String | etiqueta oficial en español (`nombre`) |
| `label_en` | String | etiqueta oficial en inglés (`name`; nula en 3 códigos sin etiqueta) |
| `level` | Int8 | nivel 1–7 derivado de la cadena |
| `parent_code` | String | ancestro publicado más cercano; nulo en las 45 divisiones |
| `is_leaf` | Boolean | verdadero si ningún código publicado lo tiene por padre |

`is_leaf` es descriptivo de esta versión del vocabulario, no una restricción
de validez: cualquier código publicado puede aparecer legítimamente en una
licitación (Guía CPV 2008), hoja o no. Debe recalcularse si cambia la versión
del vocabulario de referencia.

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
