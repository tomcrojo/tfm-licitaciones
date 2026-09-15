# Dimensión de referencia DIR3 (unidades orgánicas)

Fuente oficial: listados de **unidades orgánicas** del Directorio Común de
Unidades Orgánicas y Oficinas (DIR3), publicados por el Portal de
Administración Electrónica (PAe/CTT, solución DIR3). La dimensión alimenta la
futura identificación de órganos de contratación (`buyer`) con identificadores
oficiales.

## Ficheros fuente y cobertura

Solo se ingieren las seis distribuciones de unidades orgánicas por ámbito de
administración. Quedan fuera del alcance: oficinas, UGEP, SIR, cámaras de
comercio, grupos de acción local y el resto de catálogos auxiliares.

| Ámbito | Nivel | Fichero oficial | idElemento | Filas (snapshot 2026-09-15) |
|---|---|---|---|---|
| AGE | 1 | `Listado Unidades AGE.xlsx` | 2741 | 19.919 |
| CCAA | 2 | `Listado Unidades CCAA.xlsx` | 2742 | 18.825 |
| EELL | 3 | `Listado Unidades EELL.xlsx` | 2744 | 26.606 |
| Universidades | 4 | `Listado Unidades Universidades.xlsx` | 2808 | 7.360 |
| Otras Instituciones | 5 | `Listado Unidades Institucionales.xlsx` | 2810 | 1.338 |
| Justicia | 6 | `Listado-unidades-organicas-Justicia.xlsx` | 7326 | 34.213 |
| **Total** | | | | **108.261** |

URL canónica de cada fichero (verificado 2026-09-15):
`https://administracionelectronica.gob.es/ctt/resources/Soluciones/238/Descargas/<fichero>?idIniciativa=238&idElemento=<id>`

El registro con nombres deterministas, nivel y URL vive en
`src/tfm_licitaciones/dir3.py` (`DIR3_UNIT_SOURCES`). Cada descarga se guarda
como `data/raw/dir3/dir3-unidades-<ambito>.xlsx`.

## Método de descarga y cadencia

- Descarga explícita: `licitaciones-pipeline ingest --source dir3` (requiere
  red). stdlib `urllib` con cookie jar y reintentos con backoff; se valida
  tamaño mínimo, magic bytes `PK` y estructura de cabeceras antes de aceptar el
  fichero (fallo claro si la respuesta es el challenge JavaScript de F5/TSPD
  del host, con instrucciones de descarga manual en el navegador).
- Cadencia: el PAe publica snapshots completos sin changelog ni fecha de corte
  declarada; la identidad del contenido descargado es su URL + `sha256`,
  registrados en `data/reference/dir3/build_manifest.json`.
- La operación es idempotente: un fichero local ya válido no se vuelve a
  descargar (y por tanto tampoco se refresca: ver limitaciones).
- Cobertura temporal: snapshot vigente en el momento de la descarga; todas las
  filas del snapshot actual tienen `status = V`.

## Formato raw y diferencias reales entre ficheros

Cada XLSX tiene una primera hoja de datos, una hoja `Leyenda de campos` y una
hoja `Catálogos de clasificación`. En la hoja de datos: primera fila y primera
columna vacías, cabecera real en la fila 2, y `C_DNM_UD_ORGANICA` repetido
como cabecera en las cuatro columnas de denominación (unidad, superior,
principal y EDP). Por eso las columnas se resuelven **por posición** respecto
de columnas ancla únicas, no por nombre.

Diferencias medidas entre ámbitos:

- AGE y Justicia nombran la denominación EDP `EDP.C_DNM_UD_ORGANICA`; el resto
  repiten `C_DNM_UD_ORGANICA`.
- EELL inserta `C_ID_AMB_PROVINCIA` y `C_DESC_PROV` tras el nivel jerárquico y
  añade `CONTACTOS` al final (19 columnas).
- Justicia añade `CARGADOR` al final (17 columnas).
- Universidades reutiliza el nombre de hoja `Unidades AGE V+T` (artefacto del
  fichero oficial).

## Clave natural y decisión sobre versión (DIR3 2.0)

- `C_ID_UD_ORGANICA` es único dentro de cada fichero (0 duplicados) y entre los
  seis ficheros (0 solapes) en el snapshot medido: 108.261 códigos de 9
  caracteres `[A-Z][A-Z0-9]\d{7}` (patrón verificado en códigos, superiores y
  principales).
- El documento oficial *DIR3 – Nuevo modelo de gestión del cambio* introduce un
  campo `versión` numérico asociado al código **solo en la aplicación web y los
  servicios web** de DIR3 2.0, y afirma que, por compatibilidad, la interfaz
  actual devuelve siempre la última versión vigente de cada unidad «para que no
  haya duplicados». Las distribuciones XLSX no incluyen columna de versión.
- Decisión: la dimensión usa `dir3_code` como clave primaria y **no inventa**
  una columna de versión. La identidad del snapshot queda en la procedencia
  (URL oficial, `sha256`, fecha de construcción en el manifiesto). Si en el
  futuro se consumen servicios web con histórico, la dimensión deberá añadir un
  eje de versión; se recoge como limitación.

## Esquema canónico

Tipado Polars, orden determinista por `dir3_code` ascendente. Cada columna se
justifica en una columna real del fichero; no hay columnas decorativas.

| Columna | Columna fuente | Tipo | Observaciones |
|---|---|---|---|
| `dir3_code` | `C_ID_UD_ORGANICA` | String | PK; validado con el patrón de 9 caracteres |
| `name` | `C_DNM_UD_ORGANICA` (unidad) | String | |
| `administration_scope` | identidad del fichero | String | validado: `C_ID_NIVEL_ADMON` constante y igual al nivel del ámbito |
| `public_entity_type` | `C_ID_TIPO_ENT_PUBLICA` | String/null | catálogo AE, AY, CA, CI, CO, DP, EM, EP, MA, MN, OA, UN; vacío en ~33k filas (mayoría Justicia) |
| `hierarchy_level` | `N_NIVEL_JERARQUICO` | Int16 | |
| `parent_dir3_code` | `C_ID_DEP_UD_SUPERIOR` | String/null | unidad inmediatamente superior |
| `principal_dir3_code` | `C_ID_DEP_UD_PRINCIPAL` | String/null | «unidad orgánica raíz»; medido: coincide con el propio código en las 13.194 filas de nivel 1 |
| `status` | `C_ID_ESTADO` | String | catálogo V/E/A/T; el snapshot actual solo contiene V |
| `official_valid_from_raw` | `D_VIG_ALTA_OFICIAL` | String/null | texto oficial `dd/mm/yy` conservado verbatim; la leyenda la describe como «fecha de creación oficial» |
| `nif_cif` | `NIF_CIF` | String/null | normalizado a mayúsculas para futuro cruce con compradores |

Columnas fuente descartadas (documentadas, no pérdida silenciosa): nombres de
superior/principal/EDP (recomputables por cruce con `dir3_code`), campos EDP
(`B_SW_DEP_EDP_PRINCIPAL`, `C_ID_DEP_EDP_PRINCIPAL`; mayormente vacíos),
`C_ID_NIVEL_ADMON` (1:1 con el ámbito, se valida al ingerir), provincia y
`CONTACTOS` en EELL (enriquecimiento territorial futuro vía INE/NUTS), y
`CARGADOR` en Justicia (interno de la administración de justicia).

### Fechas de dos dígitos: se conserva el valor oficial, sin siglo inventado

DIR3 publica `D_VIG_ALTA_OFICIAL` como texto `dd/mm/yy` y no documenta regla
alguna de resolución de siglo. Por eso la dimensión **conserva el valor
oficial verbatim** en `official_valid_from_raw` (String) y no deriva un
`Date`: cualquier regla de siglo (p. ej. «el año más reciente que no quede en
el futuro») sería una interpretación no oficial y haría que los mismos bytes
raw produjeran salidas canónicas distintas según el momento de
reconstrucción, además de no resolver la ambigüedad real (`01/01/25` puede ser
1925 o 2025). Si DIR3 publica una regla oficial fiable, se derivará la fecha
normalizada como cambio explícito. La única validación es de forma: lo que no
sea `dd/mm/yy` se rechaza de forma observable.

## Reglas de ingesta

- Fila rechazada (contabilizada por motivo en el manifiesto, nunca filtrada en
  silencio): código ausente o con formato inválido, denominación/estado/nivel
  jerárquico/padre/principal ausentes, `C_ID_NIVEL_ADMON` distinto del nivel
  del ámbito, estado fuera de catálogo, nivel jerárquico no numérico, fecha no
  `dd/mm/yy`.
- Duplicados de `dir3_code` dentro de un fichero o entre ámbitos: error
  estructural (rompe la PK), la construcción falla.
- Padres no resueltos (63 en el snapshot, en su mayor parte unidades
  extinguidas que no aparecen en un listado de vigentes): se conservan tal cual
  y se cuentan en `parents_unresolved` como métrica de calidad.

## Ejecución

```bash
# descarga explícita (red); DIR3 nunca forma parte de `--source all`
uv run --with-editable . python -m tfm_licitaciones.cli ingest \
  --start 2026-09-15 --end 2026-09-15 --source dir3

# construcción offline de la dimensión desde data/raw/dir3
uv run --with-editable . python -m tfm_licitaciones.cli dir3
```

Salidas (no versionadas): `data/reference/dir3/dir3_units.parquet` y
`data/reference/dir3/build_manifest.json` (el manifiesto sí se versiona como
evidencia de la construcción).

## Limitaciones

- Snapshot puntual sin histórico: no hay versiones ni ciclo de vida (E/A/T)
  observable con estos ficheros.
- Los 63 padres no resueltos no se pueden resolver con las seis distribuciones
  de vigentes.
- `official_valid_from_raw` no es una fecha tipada: DIR3 no publica regla de
  siglo y no se inventa una.
- El host PAe está detrás de protección F5/TSPD: si endurece el challenge, la
  descarga programática fallará con instrucciones claras y se podrá completar a
  mano sin cambiar el pipeline.
- Sin mecanismo de refresco en P0: un XLSX local estructuralmente válido se
  omite siempre; para forzar un snapshot nuevo hay que borrar manualmente
  `data/raw/dir3/*.xlsx` y volver a ejecutar `ingest --source dir3`.
- La identidad de contenido de cada snapshot es su URL + `sha256` (registrados
  en el manifiesto). `built_at` del manifiesto es la hora de **transformación**,
  no la hora de descarga, que no se persiste.
- La correspondencia `NIF_CIF` → unidad no es 1:1 (varias unidades comparten
  el NIF de su entidad raíz).
