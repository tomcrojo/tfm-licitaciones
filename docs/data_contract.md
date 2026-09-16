# Contratos de datos

Este documento separa el contrato canónico de Silver de la persistencia
histórica 0.1. Silver canónico (`procurement_events.parquet`) es ahora la
salida primaria y se construye desde todos los registros y tombstones tipados
de Bronze. Gold 0.1 continúa consumiendo temporalmente la vista
`TenderRecord` en memoria (selección de snapshot más reciente, tombstones de
ZIP y plegado de revisiones); esa vista ya no se persiste como JSONL.

## Raw: evidencia de recuperación por artefacto

Cada fichero que entra en Bronze requiere un sidecar `<fichero>.provenance.json`.
`RawArtifact` en `src/tfm_licitaciones/raw_provenance.py` define esta evidencia
inmutable, independiente del estado operativo de ventanas y del manifest Gold:

| Campo | Semántica |
| --- | --- |
| `version` | Versión del sidecar, actualmente `1` |
| `source` | Fuente que recuperó el artefacto (`ted`, `placsp`, `boe`) |
| `raw_path` | Ruta relativa al directorio Raw, con separadores `/` |
| `sha256` | SHA-256 hexadecimal de los bytes del fichero completo |
| `retrieved_at` | Finalización de la recuperación del artefacto; ISO 8601 con zona, escrito en UTC |
| `partition` | Identidad lógica del lote/partición, o null si no aplica |
| `window_start`, `window_end` | Límites inclusivos ISO de la ventana, ambos null si no aplica |

TED conserva su lote JSONL por ventana solicitada: `partition=YYYYMMDD-YYYYMMDD`
y `retrieved_at` corresponde al final de la recuperación del lote agregado,
antes de serializarlo. No representa la publicación del aviso ni el instante
individual de cada página HTTP. OpenPLACSP conserva el ZIP mensual, con
`partition=YYYYMM` y los límites del mes completo, incluso cuando se solicita
solo parte del mes; el timestamp se captura al terminar la transferencia.

Una descarga idéntica reutiliza el fichero y su primera evidencia. Una descarga
distinta bajo la misma identidad conserva la versión anterior y publica la
nueva como `<nombre>-<sha256>.<extensión>`, con su propio sidecar y timestamp.
Bronze comprueba ruta y checksum antes de parsear. Evidencia ausente, inválida
o que no coincide con los bytes detiene la transformación; no se inventan
rechazos de registros cuyo origen no puede verificarse. Ni la transformación,
ni el manifest del run, ni el mtime proporcionan `retrieved_at`.

Payload y sidecar se publican mediante enlaces locales sin sobrescritura; no
forman una transacción de dos ficheros. Bronze detecta ambos huérfanos, incluso
un directorio que solo contiene sidecars, y detiene el procesamiento. Una
nueva descarga puede completar una publicación interrumpida:

- Si existe solo el payload, los bytes recuperados deben tener el mismo SHA.
  El nuevo sidecar registra el timestamp real de **ese reintento**, sin atribuir
  una fecha histórica al payload huérfano. Bytes distintos fallan sin sobrescribir.
- Si existe solo el sidecar, el reintento debe coincidir en bytes, fuente,
  partición y ventana. Se repone el payload y se conserva el sidecar, incluido
  su primer timestamp; cualquier diferencia falla.

El sidecar es la raíz de confianza local. Se valida su estructura, ruta e
integridad respecto al payload, pero no puede detectarse una edición externa
semánticamente válida de su timestamp o identidad sin otra autoridad externa.

`ingestion_state` sigue siendo estado de ejecución. No se duplica allí
una segunda autoridad de recuperación: sus timestamps operativos y de
aceptación de cambios no sustituyen al sidecar. La integración de completitud
de ventanas sigue pendiente. Para datos históricos, solo una evidencia de
descarga vinculada al mismo fichero y checksum puede justificar un sidecar;
sin ella se necesita una nueva recuperación en un directorio Raw separado.

## Bronze: parsing source-specific en Parquet

`bronze/records.parquet` es la salida primaria de registros aceptados antes del
plegado de revisiones y de aplicar tombstones. `BRONZE_SCHEMA` en
`src/tfm_licitaciones/bronze.py` fija todas las columnas, incluso para un lote
vacío. No se infiere un Struct desde payloads heterogéneos:

| Campo | Tipo Polars/Parquet | Semántica |
| --- | --- | --- |
| `source` | string | Fuente autoritativa del sidecar (`ted`, `placsp`, `boe`) |
| `source_file` | string | Ruta relativa al directorio Raw del run |
| `source_member` | string/null | Nombre del miembro Atom cuando procede de un ZIP |
| `source_member_index` | uint32/null | Posición del miembro en el directorio central ZIP, desde 1, incluyendo miembros no Atom |
| `record_locator` | string | Línea JSONL (`line:N`) o posición de entrada Atom (`entry:N`), desde 1 |
| `source_record_id` | string/null | Identificador publicado; en Atom conserva el URI completo |
| `raw_sha256` | string | Checksum heredado del sidecar verificado del fichero Raw; en ZIP identifica el contenedor |
| `raw_retrieved_at` | timestamp[us, UTC] | Instante heredado del mismo sidecar, sin consultar el reloj de transformación |
| `payload_json` | string | Objeto source-specific serializado como JSON UTF-8 con claves ordenadas |

El payload se serializa una vez y se verifica su codificación UTF-8 antes de
aceptar el candidato; Parquet reutiliza esa serialización validada. Los
surrogates Unicode aislados se rechazan, incluso en campos anidados o claves.
`json.loads(payload_json)` recupera el objeto del adaptador. TED conserva sus
campos heterogéneos; Atom conserva el payload CODICE plano y `_atom_file` cuando
procede de un ZIP. Los importes CODICE conservan además el texto numérico
publicado en claves `*_raw` (`amount_tax_exclusive_raw`,
`amount_estimated_overall_raw`, ...) junto a los campos float legados, para
que Silver pueda construir `Decimal` exactos sin artefactos binarios; los
payloads anteriores sin esas claves siguen siendo válidos. Este esquema no
sustituye al contrato canónico Silver.
La reproducción utiliza `source_file` + `raw_sha256` para identificar el
artefacto y su sidecar determinista, y miembro/índice/localizador para encontrar
el registro. El índice desambigua miembros con el mismo nombre dentro de un
ZIP. No se crean sidecars ni checksums independientes por miembro. Fuente y
partición/ventana del artefacto se consultan en su sidecar; no se copian ventanas
en cada fila. `ProcurementEvent.ingested_at` recibe `raw_retrieved_at`.

`bronze/tombstones.parquet` conserva **cada** control de borrado válido de
Atom/XML plano y ZIP, incluidos controles repetidos y snapshots anteriores.
`TOMBSTONE_SCHEMA` contiene exactamente las columnas de procedencia anteriores,
sin `payload_json`: `source_record_id` es el `ref` completo, y
`record_locator=deleted-entry:N` localiza el control desde 1, contando también
controles inválidos. Miembro, índice central ZIP, checksum y timestamp tienen
la misma semántica que en registros/rechazos; en ficheros planos el miembro y
su índice son nulos. El esquema se mantiene incluso cuando no hay controles.
Silver canónico consume todas estas filas como eventos `tombstone`; el atributo
Atom `when` no se retiene hoy (véase más abajo).

`bronze/rejections.parquet` utiliza las mismas columnas de procedencia,
más `rejection_reason` y `rejection_scope`, ambas string; no contiene
`payload_json`. Los motivos incluyen `invalid_json`, `invalid_unicode_payload`, `expected_json_object`,
`unsupported_source`, `source_mismatch`, `missing_tender_id`, `missing_title`, `invalid_atom_id`,
`invalid_xml`, `expected_atom_feed`, `invalid_zip`, `unreadable_zip_member`,
`no_atom_members`, `non_finite_amount` y `missing_tombstone_ref`. El contenido original se consulta
en Raw mediante su procedencia. Sin ID se conserva la posición; para un
documento ilegible la posición y el ID son nulos.
Si una etiqueta de fuente soportada del payload contradice la fuente del
sidecar, el registro se rechaza con `source_mismatch`. `source` y las métricas
`by_source` siempre siguen el sidecar, también ante etiquetas desconocidas o
Unicode inválido. La etiqueta conflictiva se recupera del Raw con la procedencia
del rechazo. Los errores de documento y de control también heredan checksum y
timestamp; el miembro/índice es null cuando no se llegó a abrir el contenedor.
Las corrupciones CRC, DEFLATE, BZIP2 y LZMA de un miembro se registran como
`unreadable_zip_member`, sin impedir el procesamiento de los demás miembros.

`bronze/ingestion_report.json` y `manifest.ingestion` contienen los mismos
conteos agregados y `by_source`:

- `parsed`: candidatos examinados, incluyendo los que no pueden parsearse;
  una línea JSONL no vacía o un elemento Atom `entry` es un candidato;
- `accepted`: candidatos con payload soportado, identificador y título,
  escritos en `records.parquet`;
- `rejected`: candidatos descartados con motivo y scope `record`;
- `document_errors`: documentos/contenedores ilegibles o incompatibles,
  scope `document`; no se inventa cuántas entradas contenían;
- `control_errors`: controles de borrado sin `ref`, scope `control`;
- `tombstones`: controles válidos escritos en `tombstones.parquet`, sin
  deduplicarlos ni incluirlos en `parsed`, `accepted` o `rejected`;
- `passed`: ausencia de rechazos y errores de documento/control.

Se cumple `parsed = accepted + rejected`, también por fuente, y el número de
filas en rechazos es `rejected + document_errors + control_errors`. Las líneas
en blanco y los miembros ZIP que no son Atom no son candidatos.
`placsp_entries` ahora cuenta todos los candidatos de ZIP, incluidos rechazados;
`atom_files` cuenta los documentos Atom intentados en ZIP y ficheros planos.

### Compatibilidad temporal con Silver/Gold legado

Bronze y sus métricas conservan todas las recuperaciones. La lista normalizada
que recibe el pipeline legado usa solo el snapshot más reciente de cada
`(source, partition, window_start, window_end)`, según `retrieved_at` del
sidecar. Los artefactos sin partición ni ventana siguen siendo independientes.
Un empate en el máximo timestamp con checksums distintos detiene la ejecución:
no hay evidencia suficiente para elegir una versión por nombre de fichero.
Esta selección no acredita la completitud de la descarga.

El snapshot se selecciona entero, antes del plegado de avisos y tombstones.
Así, una corrección TED/BOE no pierde frente al fichero base por orden léxico;
un ZIP corregido tampoco conserva efectos de tombstones retirados de su versión
anterior. Se mantiene el plegado por `updated` dentro del conjunto seleccionado,
con orden de recuperación como desempate. La vista legada sigue aplicando solo
tombstones de ZIP; los de Atom/XML plano ya se conservan en Bronze, pero aún
no se aplican a esta vista. Esta selección afecta solo a la vista legada en
memoria que consume Gold 0.1: Silver canónico se construye desde todas las
recuperaciones y no pliega historial.

El informe añade `superseded_artifacts` y `superseded_records` a nivel de run
para contar los artefactos y registros aceptados históricos excluidos de esa
vista. `tombstone_ids` conserva el número de refs únicos de ZIP seleccionados;
`tombstoned_removed` y `updates_folded` cuentan las operaciones legadas sobre
los registros seleccionados. Los rechazos de snapshots anteriores siguen
siendo visibles y mantienen `ingestion.passed=false`.

La aceptación Bronze comprueba la estructura y los campos identificador/título,
no certifica la validez de fechas, importes ni otros campos opcionales. Los
adaptadores históricos `parse_atom_file`, `parse_placsp_atom` e
`iter_placsp_zip` mantienen su forma de retorno para entradas válidas, pero
lanzan `ValueError` ante rechazos; el pipeline usa sus variantes con resultados
de parsing y continúa con los demás registros/documentos.

**Límite de calidad:** `quality_passed` y el código de salida de la CLI siguen
representando los gates Silver/Gold existentes. Pueden ser correctos aunque
`ingestion.passed` sea falso; deben consultarse ambos estados. Integrar los
rechazos en la decisión global de calidad queda para la tarea de quality gates,
sin cambiar aquí umbrales ni sustituir los artefactos Gold históricos.

## Silver canónico: `procurement_events`

### Grano e identidad

Una fila representa un evento o estado publicado por una fuente, no el estado
final completo de un expediente. Un procedimiento puede tener varios eventos:
anuncio, corrección, adjudicación o anulación, entre otros. Esto permite
reconstruir su evolución sin sobrescribir evidencia anterior.

Silver canónico se construye desde **todas** las filas aceptadas de
`bronze/records.parquet` y **todos** los controles de
`bronze/tombstones.parquet`, incluidos snapshots sustituidos. Nunca se pliega
el historial ni se aplican tombstones como borrados en esta capa.

Cada adaptador construye identificadores deterministas a partir de la
identidad publicada por la fuente (la implementación productiva es el motor
nativo de Polars en `src/tfm_licitaciones/silver_native.py`; la semántica
exacta está congelada como oráculo de paridad en
`src/tfm_licitaciones/silver_reference.py`):

- TED: `event_id = ted:notice:<ND>`. Un número de aviso publicado (`ND`) es un
  evento; observaciones repetidas o corregidas con la misma identidad no
  crean filas nuevas. `procedure_id = ted:procedure:<identificador>` solo
  cuando el payload publica exactamente un valor distinto no vacío de
  `procedure-identifier` / `BT-04-notice`; `PR` es tipo de procedimiento y no
  se usa. `source_event_type = notice` con `notice-type` vacío, o
  `notice:<notice-type>` cuando existe.
- OpenPLACSP entrada: `event_id = placsp:notice:<atom_id completo>@<updated
  normalizado a UTC ISO>` y `procedure_id = placsp:procedure:<atom_id
  completo>`. Cada instante válido de `updated` es una revisión distinta del
  mismo expediente; `source_event_type = notice_snapshot`, sin etiquetar
  primera/revisión porque el corpus puede estar incompleto. Si `updated` falta
  o es inválido o sin zona, el marcador de identidad es `@undated`.
- OpenPLACSP tombstone: `event_id = placsp:tombstone:<ref completo>`,
  `procedure_id = placsp:procedure:<ref completo>` y
  `source_event_type = tombstone`.
- BOE (compatibilidad): `event_id = boe:notice:<item_id>`, sin
  `procedure_id` inferido y `source_event_type = notice`.

No se usan UUIDs aleatorios, posición de fila/fichero, checksum de Raw, horas
de recuperación/transformación/ejecución, mtime ni orden de procesamiento en
la identidad. Reprocesar el mismo payload produce el mismo `event_id`. Los
identificadores de fuentes diferentes no se unifican en Silver; el linkage
posterior conserva la evidencia de esa decisión.

### Duplicados y colisiones

Cada observación se mapea a un `ProcurementEvent` y se agrupa por `event_id`.
Se comparan todos los campos canónicos excepto `ingested_at`:

- si son iguales, son observaciones repetidas del mismo evento fuente y se
  conserva una, seleccionada por el mínimo tuple determinista
  `(raw_retrieved_at, source_file, source_member_index, source_member,
  record_locator, raw_sha256)`, con `source_member_index` nulo ordenado de
  forma explícita después de cualquier valor concreto;
- si cualquier otro campo canónico difiere bajo el mismo `event_id`, la
  transformación falla con un `ValueError` que nombra el identificador en
  colisión; nunca se elige arbitrariamente el contenido más reciente o antiguo.

Esto aplica a snapshots Raw corregidos con el mismo ID: una corrección que
solo afecta campos no canónicos puede deduplicarse; una corrección con un
marcador de versión publicado nuevo (nuevo `ND`, nuevo `updated`) es un
evento nuevo; un cambio canónico material bajo el mismo ID falla. El
`ingested_at` retenido es exactamente el `raw_retrieved_at` de la fila Bronze
seleccionada. El frame final tiene `event_id` único y orden ascendente.

### Esquema

El orden y los tipos físicos están definidos en `PROCUREMENT_EVENT_SCHEMA`.
Solo `event_id`, `source`, `source_event_type` e `ingested_at` son obligatorios;
`cpv_codes` es una lista no nula que puede estar vacía. Los demás campos son
nulos cuando la fuente no publica el dato o su semántica todavía no puede
establecerse de forma fiable.

| Campo | Tipo Polars/Parquet | Semántica |
| --- | --- | --- |
| `event_id` | string | Clave determinista del evento, con namespace de fuente |
| `procedure_id` | string/null | Clave del expediente en esa fuente |
| `source` | string | Sistema oficial que publicó el evento |
| `source_event_type` | string | Tipo extensible y explícito, por ejemplo `notice`, `notice_revision`, `award` o `tombstone` |
| `buyer_id` | string/null | Identificador oficial del comprador cuando existe, preferentemente DIR3 en España |
| `buyer_name` | string/null | Denominación publicada del comprador |
| `title` | string/null | Título del evento o expediente |
| `description` | string/null | Descripción publicada |
| `cpv_codes` | list[string] | Todos los códigos CPV publicados, en orden estable y sin duplicados |
| `estimated_value` | decimal(20,2)/null | Presupuesto o valor estimado; no es el importe adjudicado |
| `awarded_value` | decimal(20,2)/null | Importe adjudicado cuando el evento lo publica |
| `currency` | string/null | Código ISO 4217 asociado a los importes |
| `publication_date` | date/null | Fecha oficial de publicación del evento |
| `source_updated_at` | timestamp[us, UTC]/null | Momento en que la fuente declara haber actualizado el evento |
| `deadline` | timestamp[us, UTC]/null | Fin del plazo cuando la fuente aporta hora y zona interpretables |
| `status` | string/null | Estado publicado; su armonización entre fuentes es posterior |
| `nuts_code` | string/null | Código territorial oficial; nunca se rellena desde un nombre libre |
| `country` | string/null | Código ISO 3166-1 alpha-2 |
| `source_url` | string/null | URL pública del evento o expediente |
| `ingested_at` | timestamp[us, UTC] | Timestamp de recuperación heredado de la evidencia Raw |

`publication_date` no puede derivarse de `source_updated_at`: representan
hechos diferentes. Los timestamps aceptados por el modelo deben incluir zona y
se normalizan a UTC. Los importes usan decimal de precisión fija para evitar
introducir errores binarios en agregaciones monetarias.

`source_event_type` no es todavía una enumeración cerrada porque las fuentes
publican ciclos de vida diferentes. Los valores que emiten los adaptadores
actuales son `notice`, `notice:<tipo>` (TED), `notice_snapshot` (OpenPLACSP) y
`tombstone`. Nunca se usa `status` como sustituto del tipo de evento.

### Reglas temporales y de importes

- `ingested_at` procede únicamente del `raw_retrieved_at` de la observación
  Bronze retenida; nunca de la hora de transformación/ejecución, mtime ni
  estado de ingesta.
- TED: `PD`/`publication-date` → `publication_date`; `source_updated_at` es
  nulo porque el payload actual no aporta un timestamp de actualización
  autoritativo; `deadline` y `status`/`nuts_code` permanecen nulos porque los
  campos actuales son solo fecha o texto libre.
- OpenPLACSP: `updated` → `source_updated_at` solo cuando lleva zona horaria
  (normalizado a UTC); `publication_date` es nulo, nunca se reutiliza
  `updated` como publicación.
- BOE: fecha oficial de publicación → `publication_date`.
- Importes: Silver prefiere el texto numérico publicado (texto TED o claves
  CODICE `*_raw`) y construye `Decimal` exactos. Payloads antiguos sin texto
  usan `Decimal(str(valor_float_legado))`, sin artefactos de coma flotante.
  Todo importe persistido debe ser representable exactamente como
  `decimal(20,2)` no negativo: más de dos dígitos fraccionarios no nulos o
  exceso de precisión fallan explícitamente en lugar de redondearse; los
  ceros fraccionarios finales pueden normalizarse. Importes opcionales
  malformados o negativos permanecen nulos, como permite el contrato de
  aceptación Bronze; no se fabrica otro valor numérico. La moneda está
  apareada al importe canónico aceptado: si el importe seleccionado es
  nulo, `currency` es nula aunque la fuente publique una moneda.

### Mapeo por fuente

TED: `buyer_id` solo con exactamente un valor distinto no vacío publicado de
`buyer-identifier`/`BI`; nombre/título/descripción con la extracción localizada
existente; `cpv_codes` con todos los valores `PC`/cpv en orden fuente
deduplicados de forma estable (un orden CPV distinto bajo la misma identidad es
una diferencia material); `estimated_value` con la precedencia
`estimated-value-lot` → `framework-maximum-value-lot` → `amount` leída del
texto fuente; `awarded_value` nulo; moneda explícita o EUR solo bajo el
comportamiento documentado de TED Search con importe normalizado; país acepta
alpha-2 mayúscula, mapea `ESP`→`ES` y deja nulo un alpha-3 desconocido;
`source_url` de URL explícita o la extracción determinista de `links`.

OpenPLACSP: `buyer_id` es el `buyer_dir3` publicado (sin join DIR3); todos los
`cpv` en orden fuente deduplicado; `estimated_value` prefiere
`amount_estimated_overall` y luego `amount_tax_exclusive`, con moneda del
campo elegido; `awarded_value` nulo (no se infiere); `status` de
`status_code`; `nuts_code` solo del `nuts_code` publicado (nunca de la región
de texto libre); país `ES`; `source_url` de `url`.

BOE: item/título/sumario/comprador-o-departamento/fecha/URL directos y país
`ES`; identificador de comprador, CPV, importes, moneda, estado, NUTS,
deadline y timestamp de actualización permanecen nulos salvo que el payload
oficial los proporcione de forma explícita e inequívoca.

No hay joins ni enriquecimiento CPV/DIR3 en esta capa.

### Persistencia

La salida primaria es `<silver_dir>/procurement_events.parquet` con el esquema
exacto `PROCUREMENT_EVENT_SCHEMA`, tipado incluso vacío, `event_id` único y
orden determinista por `event_id`. El JSONL legado
`<silver_dir>/tenders.jsonl` ya no se escribe; si existe de una ejecución
anterior exactamente en esa ruta, se elimina para que no parezca vigente.

### Motores y neutralidad del contrato

El contrato de esta sección (grano, identidad, duplicados/colisiones,
esquema, reglas temporales y de importes, mapeo por fuente y persistencia)
es independiente del motor de ejecución. Polars nativo
(`silver_native.py`) es el motor productivo por defecto dentro del envelope
medido de un solo nodo (ver `docs/benchmarks.md`); la semántica exacta
queda además congelada en la referencia python-row (`silver_reference.py`),
usada como oráculo de paridad por los tests y por los benchmarks, y se
conserva una implementación PySpark semánticamente equivalente como ruta de
scale-out evaluada fuera del pipeline productivo. No existe selección de
motor por configuración en el pipeline productivo.

### Revisiones y tombstones

Una revisión obtiene su propio `event_id`, conserva el mismo `procedure_id` y
usa un `source_event_type` explícito. Un tombstone también es una fila: puede
tener vacíos los atributos descriptivos, pero conserva identidad, fuente y
`ingested_at`. No borra físicamente los eventos anteriores; una vista posterior
podrá calcular el estado vigente sin perder el historial.

Los tombstones son evidencia de borrado sin orden: el contrato Bronze actual
no retiene el atributo Atom `when`, por lo que ciclos repetidos de
borrado/publicación/borrado sobre el mismo `ref` no se pueden distinguir hoy y
nunca se inventa orden a partir del tiempo de recuperación. Conservar `when`
es un prerrequisito futuro para derivar estado vigente. Una corrección Raw
posterior que omita un evento o tombstone no elimina la fila histórica
canónica anterior.

### Frontera de compatibilidad con Gold 0.1

Gold 0.1 sigue consumiendo la vista legada en memoria `TenderRecord`: selección
del snapshot más reciente por partición, eliminación de tombstones de ZIP y
plegado de revisiones (`fold_latest_updates`) existen solo en esa frontera y
pueden colapsar el historial que Silver canónico conserva. El
`silver/tenders.jsonl` ya no se persiste. `procurement_event_from_tender`
permite migrar gradualmente consumidores restantes, pero exige que el
adaptador entregue `event_id`, `procedure_id`, tipo de evento y las tres
decisiones temporales. No copia automáticamente `published_date`, porque en el
adaptador OpenPLACSP legado ese campo procede de `updated`.

El manifiesto Gold expone conteos explícitos y no ambiguos:
`counts.bronze_records`, `counts.silver_procurement_events`,
`counts.legacy_current_state_records`, `counts.gold_opportunities` y
`counts.gold_canonical_opportunities`, junto a `silver_source_counts` (eventos
canónicos por fuente, tombstones incluidos) y `legacy_source_counts` (vista
legada por fuente). El `run_at` operativo permanece fuera de la identidad y
del determinismo de los datos canónicos.

El contrato no contiene supuestos sectoriales: CPV, comprador, territorio,
fechas, importes y estado sirven para obras, restauración, sanidad, logística,
energía, servicios profesionales o tecnología. Las etiquetas semánticas de
negocio pertenecen a enriquecimiento, no a esta entidad canónica.

## Silver legado en memoria: `TenderRecord`

Vista de estado vigente consumida por Gold 0.1. Grano: una versión consolidada
por `(source, tender_id)`. Las revisiones de OpenPLACSP se pliegan mediante el
timestamp `updated` y los identificadores marcados como tombstone (solo ZIP)
se excluyen. Ya no se persiste como JSONL.

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
| Fechas PLACSP | Silver canónico separa `source_updated_at` de `publication_date` (nulo); la vista legada para Gold aún deriva `published_date` de `updated` | Migrar Gold al contrato canónico y eliminar la vista legada |
| Tombstones | Silver conserva el control histórico, pero Bronze no retiene Atom `when`, así que el orden de borrados no es derivable | Retener `when` y derivar estado vigente |
| Rechazos | Bronze los contabiliza; `quality_passed` solo evalúa Silver/Gold | Integrar el estado de ingesta en la decisión global de calidad |
| Completitud | Una partición que falla puede no impedir el run | Registrar esperadas/descargadas y estado incompleto |
| Persistencia | Bronze y Silver canónico usan Parquet; Gold sigue en JSONL/CSV | Migrar Gold por límites a Parquet |
| TED | La configuración aplica una query tecnológica | Hacer el filtro sectorial opcional y downstream |

Estas limitaciones son trabajo pendiente conocido. No invalidan las pruebas del
comportamiento actual, pero impiden presentar todavía la versión 0.1 como la
plataforma P0 terminada.
