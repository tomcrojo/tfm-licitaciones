"""Native Polars Bronze-to-Silver candidate (EXPERIMENT ONLY).

EXPERIMENT SCOPE (loud limitation): this engine supports only the
TED/OpenPLACSP synthetic Bronze contract emitted by
:mod:`tfm_licitaciones.bench_silver` (scalar string/number JSON payloads,
CPV as JSON arrays of strings, whole-second ``updated`` instants). It is a
controlled comparison candidate, NOT a production replacement for
:mod:`tfm_licitaciones.silver`:

- BOE records and any non-TED/non-PLACSP source fail explicitly;
- CPV payloads must be JSON arrays (scalar CPV shapes fail explicitly);
- PLACSP ``updated`` instants with sub-second precision fail explicitly
  (the benchmark contract only emits whole seconds);
- amount key-presence follows the benchmark contract (amount keys always
  carry JSON numbers when present; explicit JSON nulls are treated as
  missing, exactly as the baseline treats ``None`` values);
- single-value TED fields (procedure identifier, buyer identifier) are
  handled as scalars; list-valued shapes fail explicitly via the parity
  check rather than silently.

Genuineness: the transform below uses only Polars expressions, joins and
grouping. Each source branch decodes ``payload_json`` exactly once into an
explicitly typed Struct; every field is then derived from that decoded
value, never by reparsing the payload per field. It performs no per-row
Python: no ``iter_rows``/``to_dicts``/``map_elements``/``apply`` object
materialization, no ``ProcurementEvent`` allocation, no ``json`` module
parsing loop. The success path performs zero driver row fetches (only
scalar ``height`` validations run engine-side); failure paths fetch at
most 5 labels solely to name them in the raised ``ValueError``
(``event_id`` once identity exists, otherwise the Bronze
``record_locator``, mirroring the frozen baseline).
"""

from __future__ import annotations

import polars as pl

from tfm_licitaciones.models import PROCUREMENT_EVENT_SCHEMA

CONTRACT_SCOPE = (
    "experiment-only: TED/PLACSP synthetic Bronze contract from "
    "tfm_licitaciones.bench_silver (JSON arrays for CPV, scalar identifier "
    "fields, whole-second updated instants); BOE and non-synthetic payload "
    "shapes are unsupported and fail explicitly"
)
IMPLEMENTATION = "polars-native"
IMPLEMENTATION_DETAIL = (
    "native Polars expressions/joins/grouping over the Polars/Parquet "
    "boundary (one str.json_decode into an explicitly typed Struct per "
    "branch, string/datetime/decimal expressions, group_by struct "
    "dedup/collision check, sort + group_by first for provenance-minimum "
    "selection); no per-row Python"
)

_CANONICAL_ORDER = list(PROCUREMENT_EVENT_SCHEMA.names())
_COLLISION_ORDER = [name for name in _CANONICAL_ORDER if name != "ingested_at"]
_PROVENANCE_ORDER = [
    "raw_retrieved_at",
    "source_file",
    "source_member_index",
    "source_member",
    "record_locator",
    "raw_sha256",
]

# Closed decode schemas for the synthetic Bronze contract. Every field is
# nullable: a missing key and an explicit JSON null both decode to null,
# exactly like the previous per-field extraction. JSON numbers decoded
# into String fields use Polars' canonical formatting (e.g. ``240000.0``
# becomes ``"240000"``); this preserves the amount pipeline outcome on the
# contract because the float gate, the scale/precision validation and the
# Decimal value only depend on the numeric value, while retained ``_raw``
# texts (the preferred representation) decode verbatim.
_TED_SCHEMA = pl.Struct(
    {
        "ND": pl.String,
        "PD": pl.String,
        "publication_date": pl.String,
        "publication-date": pl.String,
        "TI": pl.Struct({"spa": pl.String}),
        "description-glo": pl.Struct({"spa": pl.String}),
        "buyer-name": pl.Struct({"spa": pl.List(pl.String)}),
        "CY": pl.String,
        "PC": pl.List(pl.String),
        "estimated-value-lot": pl.String,
        "framework-maximum-value-lot": pl.String,
        "amount": pl.String,
        "currency": pl.String,
        "procedure-identifier": pl.String,
        "BT-04-notice": pl.String,
        "notice-type": pl.String,
        "buyer-identifier": pl.String,
        "BI": pl.String,
        "url": pl.String,
        "links": pl.Struct(
            {
                "html": pl.Struct(
                    {
                        "ENG": pl.String,
                        "SPA": pl.String,
                        "DEU": pl.String,
                        "FRA": pl.String,
                    }
                )
            }
        ),
    }
)

_PLACSP_SCHEMA = pl.Struct(
    {
        "atom_id": pl.String,
        "updated": pl.String,
        "title": pl.String,
        "summary": pl.String,
        "url": pl.String,
        "status_code": pl.String,
        "buyer": pl.String,
        "buyer_dir3": pl.String,
        "nuts_code": pl.String,
        "cpv": pl.List(pl.String),
        "amount_estimated_overall": pl.String,
        "amount_estimated_overall_currency": pl.String,
        "amount_estimated_overall_raw": pl.String,
        "amount_tax_exclusive": pl.String,
        "amount_tax_exclusive_currency": pl.String,
        "amount_tax_exclusive_raw": pl.String,
    }
)

_TZ_SUFFIX = r"(Z|[+-][0-9]{2}:?[0-9]{2})$"
_MAX_IDS_IN_ERROR = 5


def _clean_text(expression: pl.Expr) -> pl.Expr:
    """Strip surrounding whitespace; nulls stay null."""

    return expression.str.strip_chars()


def _null_if_empty(expression: pl.Expr) -> pl.Expr:
    """Map empty strings to null, keeping nulls null."""

    return pl.when(expression.is_null() | (expression == "")).then(None).otherwise(expression)


def _normalize_amount_text(expression: pl.Expr) -> pl.Expr:
    """Replicate ``_normalize_amount_text`` with native string expressions."""

    nospace = expression.str.strip_chars().str.replace_all(" ", "", literal=True)
    has_comma = nospace.str.contains(",", literal=True)
    has_dot = nospace.str.contains(".", literal=True)
    last_separator = nospace.str.extract(r"([,.])[^,.]*$", 1)
    both = (
        pl.when(last_separator == ",")
        .then(
            nospace.str.replace_all(".", "", literal=True).str.replace_all(",", ".", literal=True)
        )
        .otherwise(nospace.str.replace_all(",", "", literal=True))
    )
    many_dots = nospace.str.replace_all(".", "", literal=True)
    single = (
        pl.when(has_dot & (nospace.str.count_matches(r"\.") > 1))
        .then(many_dots)
        .otherwise(nospace)
    )
    return (
        pl.when(has_comma & has_dot)
        .then(both)
        .when(has_comma)
        .then(nospace.str.replace_all(",", ".", literal=True))
        .otherwise(single)
    )


def _exact_decimal(normalized: pl.Expr) -> pl.Expr:
    """Convert normalized amount text to ``Decimal(20,2)`` (validated input)."""

    return normalized.str.to_decimal(scale=2).cast(pl.Decimal(20, 2))


def _amount_from_text(source_text: pl.Expr, accepted_float: pl.Expr) -> tuple[pl.Expr, pl.Expr]:
    """Build ``(estimated, amount_invalid)`` for one amount source text.

    ``accepted_float`` is the legacy float gate (null/malformed/negative
    stays null). ``amount_invalid`` flags rows whose accepted amount is not
    representable as ``decimal(20,2)``; those must fail, never round.
    """

    normalized = _normalize_amount_text(source_text)
    gate = accepted_float.is_not_null() & (accepted_float >= 0) & accepted_float.is_finite()
    fraction = normalized.str.extract(r"\.([0-9]+)$", 1)
    fraction_significant = fraction.fill_null("").str.strip_chars_end("0")
    bad_scale = fraction.is_not_null() & (fraction_significant.str.len_chars() > 2)
    integer_part = (
        normalized.fill_null("").str.split(".").list.first().fill_null("")
        .str.replace("-", "", literal=True)
        .str.replace("+", "", literal=True)
    )
    bad_precision = integer_part.str.len_chars() > 18
    invalid = gate & (bad_scale.fill_null(False) | bad_precision.fill_null(False))
    estimated = pl.when(gate).then(_exact_decimal(normalized)).otherwise(None)
    return estimated, invalid


def _scalar_single(first: pl.Expr, second: pl.Expr) -> pl.Expr:
    """Return the value only when both scalar texts agree on one distinct value."""

    first_clean, second_clean = _null_if_empty(first), _null_if_empty(second)
    return (
        pl.when(first_clean.is_null() & second_clean.is_null())
        .then(None)
        .when(first_clean.is_null())
        .then(second_clean)
        .when(second_clean.is_null() | (first_clean == second_clean))
        .then(first_clean)
        .otherwise(None)
    )


def _dedup_codes(codes: pl.Expr) -> pl.Expr:
    """Deduplicate a decoded CPV list preserving order; null becomes ``[]``."""

    return (
        pl.when(codes.is_null())
        .then(pl.lit([], dtype=pl.List(pl.String)))
        .otherwise(codes.list.unique(maintain_order=True))
    )


def _array_shape_invalid(payload: pl.Expr, key: str) -> pl.Expr:
    """Flag a CPV key that is present but not a JSON array.

    A scalar CPV value decodes into a single-element list, so the shape
    must be validated against the raw payload text (a string scan, not a
    second JSON decode) to keep the explicit-failure contract. An explicit
    JSON null counts as absent, like a missing key.
    """

    present = payload.str.contains(f'"{key}"\\s*:')
    is_array = payload.str.contains(f'"{key}"\\s*:\\s*\\[')
    is_null = payload.str.contains(f'"{key}"\\s*:\\s*null\\s*[,}}]')
    return present & ~is_array & ~is_null


def _failure_ids(frame: pl.DataFrame, column: str = "event_id") -> list[str]:
    """Fetch a bounded sample of labels for an explicit failure message.

    Identity checks (missing TED notice number, missing PLACSP atom id,
    missing/invalid tombstone refs) run BEFORE ``event_id`` exists, so
    they name ``record_locator`` instead — mirroring the frozen python-row
    baseline, which reports the Bronze locator there. Requesting a missing
    column falls back to ``record_locator`` rather than raising
    ``ColumnNotFoundError`` and hiding the real validation error.
    """

    if column not in frame.columns:
        column = "record_locator" if "record_locator" in frame.columns else frame.columns[0]
    return frame.head(_MAX_IDS_IN_ERROR).get_column(column).to_list()


def _raise_if_invalid(
    frame: pl.DataFrame, mask: pl.Expr, message: str, *, id_column: str = "event_id"
) -> None:
    """Raise ``ValueError`` naming offending rows when ``mask`` matches rows."""

    bad = frame.filter(mask)
    if bad.height:
        raise ValueError(f"{message}: {_failure_ids(bad, id_column)!r}")


def _ted_events(records: pl.DataFrame) -> pl.DataFrame:
    """Map TED Bronze rows to canonical event rows with native expressions.

    ``payload_json`` is decoded exactly once into :data:`_TED_SCHEMA`;
    every field below is a struct projection of that decoded value.
    """

    ted = records.filter(pl.col("source") == "ted").with_columns(
        pl.col("payload_json").str.json_decode(dtype=_TED_SCHEMA).alias("__doc")
    )
    doc = pl.col("__doc")
    payload = pl.col("payload_json")
    notice_id = _null_if_empty(_clean_text(doc.struct.field("ND")))
    _raise_if_invalid(
        ted.with_columns(notice_id.alias("__notice_id")),
        pl.col("__notice_id").is_null(),
        "TED bronze record without a published notice number",
        id_column="record_locator",
    )
    title = _null_if_empty(_clean_text(doc.struct.field("TI").struct.field("spa")))
    description = _null_if_empty(_clean_text(doc.struct.field("description-glo").struct.field("spa")))
    buyer_name = _null_if_empty(
        _clean_text(doc.struct.field("buyer-name").struct.field("spa").list.first())
    )
    publication_date = pl.coalesce(
        [
            _null_if_empty(doc.struct.field("PD")),
            _null_if_empty(doc.struct.field("publication_date")),
            _null_if_empty(doc.struct.field("publication-date")),
        ]
    )
    selected = pl.coalesce(
        [
            doc.struct.field("estimated-value-lot"),
            doc.struct.field("framework-maximum-value-lot"),
            doc.struct.field("amount"),
        ]
    )
    accepted = _normalize_amount_text(selected).cast(pl.Float64, strict=False)
    estimated, amount_invalid = _amount_from_text(selected, accepted)
    currency_raw = _null_if_empty(_clean_text(doc.struct.field("currency")))
    currency = (
        pl.when(estimated.is_not_null())
        .then(pl.when(currency_raw.is_null()).then(pl.lit("EUR")).otherwise(currency_raw))
        .otherwise(None)
    )
    procedure = _scalar_single(
        _clean_text(doc.struct.field("procedure-identifier")),
        _clean_text(doc.struct.field("BT-04-notice")),
    )
    notice_type = _null_if_empty(_clean_text(doc.struct.field("notice-type")))
    buyer_id = _scalar_single(
        _clean_text(doc.struct.field("buyer-identifier")),
        _clean_text(doc.struct.field("BI")),
    )
    country_raw = _clean_text(doc.struct.field("CY"))
    country = (
        pl.when(country_raw == "ESP")
        .then(pl.lit("ES"))
        .when(country_raw.str.contains("^[A-Z]{2}$"))
        .then(country_raw)
        .otherwise(None)
    )
    links_html = doc.struct.field("links").struct.field("html")
    url = pl.coalesce(
        [
            doc.struct.field("url"),
            links_html.struct.field("ENG"),
            links_html.struct.field("SPA"),
            links_html.struct.field("DEU"),
            links_html.struct.field("FRA"),
        ]
    )
    cpv_codes = _dedup_codes(doc.struct.field("PC"))
    cpv_invalid = _array_shape_invalid(payload, "PC")
    frame = ted.select(
        (pl.lit("ted:notice:") + notice_id).alias("event_id"),
        pl.when(procedure.is_null())
        .then(None)
        .otherwise(pl.lit("ted:procedure:") + procedure)
        .alias("procedure_id"),
        pl.lit("ted").alias("source"),
        pl.when(notice_type.is_null())
        .then(pl.lit("notice"))
        .otherwise(pl.lit("notice:") + notice_type)
        .alias("source_event_type"),
        buyer_id.alias("buyer_id"),
        buyer_name.alias("buyer_name"),
        title.alias("title"),
        description.alias("description"),
        cpv_codes.alias("cpv_codes"),
        estimated.cast(pl.Decimal(20, 2)).alias("estimated_value"),
        pl.lit(None, dtype=pl.Decimal(20, 2)).alias("awarded_value"),
        currency.alias("currency"),
        publication_date.str.slice(0, 10).str.to_date(strict=False).alias("publication_date"),
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("source_updated_at"),
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("deadline"),
        pl.lit(None, dtype=pl.String).alias("status"),
        pl.lit(None, dtype=pl.String).alias("nuts_code"),
        country.alias("country"),
        _null_if_empty(_clean_text(url)).alias("source_url"),
        pl.col("raw_retrieved_at").alias("ingested_at"),
        pl.col("raw_retrieved_at").alias("raw_retrieved_at"),
        *[(pl.col(name).alias(name)) for name in _PROVENANCE_ORDER if name != "raw_retrieved_at"],
        amount_invalid.alias("__amount_invalid"),
        cpv_invalid.alias("__cpv_invalid"),
    )
    _raise_if_invalid(frame, pl.col("__amount_invalid"), "Amount is not representable as decimal(20,2)")
    _raise_if_invalid(frame, pl.col("__cpv_invalid"), "Experiment contract requires CPV JSON arrays")
    return frame


def _placsp_events(records: pl.DataFrame) -> pl.DataFrame:
    """Map OpenPLACSP Bronze rows to canonical snapshot rows natively.

    ``payload_json`` is decoded exactly once into :data:`_PLACSP_SCHEMA`;
    every field below is a struct projection of that decoded value (the
    amount-key presence probes remain raw-text scans, since key presence
    is not a decodable value).
    """

    placsp = records.filter(pl.col("source") == "placsp").with_columns(
        pl.col("payload_json").str.json_decode(dtype=_PLACSP_SCHEMA).alias("__doc")
    )
    doc = pl.col("__doc")
    payload = pl.col("payload_json")
    atom_id = _null_if_empty(_clean_text(doc.struct.field("atom_id")))
    _raise_if_invalid(
        placsp.with_columns(atom_id.alias("__atom_id")),
        pl.col("__atom_id").is_null(),
        "PLACSP bronze record without an atom id",
        id_column="record_locator",
    )
    updated_raw = _clean_text(doc.struct.field("updated"))
    has_zone = updated_raw.str.contains(_TZ_SUFFIX)
    updated = (
        pl.when(has_zone)
        .then(updated_raw.str.to_datetime(time_zone="UTC", strict=False))
        .otherwise(None)
    )
    sub_second = updated.is_not_null() & (updated != updated.dt.truncate("1s"))
    marker = (
        pl.when(updated.is_null())
        .then(pl.lit("undated"))
        .otherwise(updated.dt.to_string("%Y-%m-%dT%H:%M:%S") + "+00:00")
    )
    has_estimated = payload.str.contains(r'"amount_estimated_overall"\s*:')
    has_tax = payload.str.contains(r'"amount_tax_exclusive"\s*:')
    chosen = (
        pl.when(has_estimated)
        .then(pl.lit("estimated"))
        .when(has_tax)
        .then(pl.lit("tax"))
        .otherwise(None)
    )
    number_text = (
        pl.when(chosen == "estimated")
        .then(doc.struct.field("amount_estimated_overall"))
        .when(chosen == "tax")
        .then(doc.struct.field("amount_tax_exclusive"))
        .otherwise(None)
    )
    raw_text = (
        pl.when(chosen == "estimated")
        .then(doc.struct.field("amount_estimated_overall_raw"))
        .when(chosen == "tax")
        .then(doc.struct.field("amount_tax_exclusive_raw"))
        .otherwise(None)
    )
    source_text = pl.coalesce([raw_text, number_text])
    accepted = _normalize_amount_text(number_text).cast(pl.Float64, strict=False)
    estimated, amount_invalid = _amount_from_text(source_text, accepted)
    chosen_currency = (
        pl.when(chosen == "estimated")
        .then(doc.struct.field("amount_estimated_overall_currency"))
        .when(chosen == "tax")
        .then(doc.struct.field("amount_tax_exclusive_currency"))
        .otherwise(None)
    )
    currency = (
        pl.when(estimated.is_not_null())
        .then(_null_if_empty(_clean_text(chosen_currency)))
        .otherwise(None)
    )
    cpv_codes = _dedup_codes(doc.struct.field("cpv"))
    cpv_invalid = _array_shape_invalid(payload, "cpv")
    frame = placsp.select(
        (pl.lit("placsp:notice:") + atom_id + "@" + marker).alias("event_id"),
        (pl.lit("placsp:procedure:") + atom_id).alias("procedure_id"),
        pl.lit("placsp").alias("source"),
        pl.lit("notice_snapshot").alias("source_event_type"),
        _null_if_empty(_clean_text(doc.struct.field("buyer_dir3"))).alias("buyer_id"),
        _null_if_empty(_clean_text(doc.struct.field("buyer"))).alias("buyer_name"),
        _null_if_empty(_clean_text(doc.struct.field("title"))).alias("title"),
        _null_if_empty(_clean_text(doc.struct.field("summary"))).alias("description"),
        cpv_codes.alias("cpv_codes"),
        estimated.cast(pl.Decimal(20, 2)).alias("estimated_value"),
        pl.lit(None, dtype=pl.Decimal(20, 2)).alias("awarded_value"),
        currency.alias("currency"),
        pl.lit(None, dtype=pl.Date).alias("publication_date"),
        updated.alias("source_updated_at"),
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("deadline"),
        _null_if_empty(_clean_text(doc.struct.field("status_code"))).alias("status"),
        _null_if_empty(_clean_text(doc.struct.field("nuts_code"))).alias("nuts_code"),
        pl.lit("ES").alias("country"),
        _null_if_empty(_clean_text(doc.struct.field("url"))).alias("source_url"),
        pl.col("raw_retrieved_at").alias("ingested_at"),
        pl.col("raw_retrieved_at").alias("raw_retrieved_at"),
        *[(pl.col(name).alias(name)) for name in _PROVENANCE_ORDER if name != "raw_retrieved_at"],
        sub_second.alias("__sub_second"),
        amount_invalid.alias("__amount_invalid"),
        cpv_invalid.alias("__cpv_invalid"),
    )
    _raise_if_invalid(
        frame, pl.col("__sub_second"), "Experiment contract supports whole-second updated instants only"
    )
    _raise_if_invalid(frame, pl.col("__amount_invalid"), "Amount is not representable as decimal(20,2)")
    _raise_if_invalid(frame, pl.col("__cpv_invalid"), "Experiment contract requires CPV JSON arrays")
    return frame


def _tombstone_events(tombstones: pl.DataFrame) -> pl.DataFrame:
    """Map PLACSP deletion controls to canonical tombstone rows natively."""

    _raise_if_invalid(
        tombstones.with_columns(pl.lit("tombstone").alias("__kind")),
        (pl.col("source").is_null() | (pl.col("source") != "placsp")),
        "Unsupported tombstone source for canonical Silver",
        id_column="record_locator",
    )
    ref = _null_if_empty(pl.col("source_record_id").str.strip_chars())
    _raise_if_invalid(
        tombstones.with_columns(ref.alias("__ref")),
        pl.col("__ref").is_null(),
        "PLACSP tombstone without a full ref",
        id_column="record_locator",
    )
    return tombstones.select(
        (pl.lit("placsp:tombstone:") + ref).alias("event_id"),
        (pl.lit("placsp:procedure:") + ref).alias("procedure_id"),
        pl.lit("placsp").alias("source"),
        pl.lit("tombstone").alias("source_event_type"),
        pl.lit(None, dtype=pl.String).alias("buyer_id"),
        pl.lit(None, dtype=pl.String).alias("buyer_name"),
        pl.lit(None, dtype=pl.String).alias("title"),
        pl.lit(None, dtype=pl.String).alias("description"),
        pl.lit([], dtype=pl.List(pl.String)).alias("cpv_codes"),
        pl.lit(None, dtype=pl.Decimal(20, 2)).alias("estimated_value"),
        pl.lit(None, dtype=pl.Decimal(20, 2)).alias("awarded_value"),
        pl.lit(None, dtype=pl.String).alias("currency"),
        pl.lit(None, dtype=pl.Date).alias("publication_date"),
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("source_updated_at"),
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("deadline"),
        pl.lit(None, dtype=pl.String).alias("status"),
        pl.lit(None, dtype=pl.String).alias("nuts_code"),
        pl.lit(None, dtype=pl.String).alias("country"),
        pl.lit(None, dtype=pl.String).alias("source_url"),
        pl.col("raw_retrieved_at").alias("ingested_at"),
        pl.col("raw_retrieved_at").alias("raw_retrieved_at"),
        *[(pl.col(name).alias(name)) for name in _PROVENANCE_ORDER if name != "raw_retrieved_at"],
    )


def _check_experiment_scope(records: pl.DataFrame) -> None:
    """Fail explicitly on Bronze sources outside the experiment contract.

    Null counts as outside the contract: ``~is_in(...)`` alone evaluates to
    null on null inputs and ``filter`` would drop those rows silently. The
    success path fetches zero rows (scalar ``height`` only); offending
    values are fetched, bounded, solely to name them on failure.
    """

    unexpected = records.filter(
        pl.col("source").is_null() | ~pl.col("source").is_in(("ted", "placsp"))
    ).limit(1)
    if unexpected.height:
        distinct = (
            records.select("source").unique().head(10).get_column("source").to_list()
        )
        unsupported = [source for source in distinct if source not in ("ted", "placsp")]
        raise ValueError(
            "Experiment polars-native engine supports TED/PLACSP synthetic Bronze only; "
            f"found sources: {unsupported!r}"
        )


def _collapse_events(frame: pl.DataFrame) -> pl.DataFrame:
    """Deduplicate observations, detect material collisions, select provenance minimum.

    All operations are native: a ``group_by`` struct-uniqueness check detects
    material collisions (any canonical difference beyond ``ingested_at``),
    then a provenance sort plus ``group_by(...).first()`` keeps the
    deterministic minimum observation per ``event_id``.
    """

    grouped = frame.group_by("event_id").agg(
        pl.struct(_COLLISION_ORDER).n_unique().alias("__variants"),
    )
    bad_ids = grouped.filter(pl.col("__variants") > 1).head(_MAX_IDS_IN_ERROR)
    if bad_ids.height:
        raise ValueError(
            "Conflicting canonical mappings for event_id(s) "
            f"{_failure_ids(bad_ids)!r}; differing fields check required"
        )
    ordered = frame.sort(
        ["raw_retrieved_at", "source_file", "__member_nulls_last", "__member_index_or_zero",
         "__source_member", "__record_locator", "raw_sha256"]
    )
    first = ordered.group_by("event_id").first()
    return first.select(_CANONICAL_ORDER).sort("event_id")


def build_procurement_events_polars(records: pl.DataFrame, tombstones: pl.DataFrame) -> pl.DataFrame:
    """Build the canonical Silver frame with native Polars execution.

    Raises ``ValueError`` naming the ``event_id`` on material collisions,
    exactly like the python-row baseline.
    """

    _check_experiment_scope(records)
    ted = _ted_events(records)
    placsp = _placsp_events(records)
    tomb = _tombstone_events(tombstones)
    combined = pl.concat(
        [
            ted.select(_CANONICAL_ORDER + _PROVENANCE_ORDER),
            placsp.select(_CANONICAL_ORDER + _PROVENANCE_ORDER),
            tomb.select(_CANONICAL_ORDER + _PROVENANCE_ORDER),
        ],
        how="vertical",
    ).with_columns(
        pl.col("source_member_index").is_null().alias("__member_nulls_last"),
        pl.col("source_member_index").fill_null(0).alias("__member_index_or_zero"),
        pl.col("source_member").fill_null("").alias("__source_member"),
        pl.col("record_locator").fill_null("").alias("__record_locator"),
    )
    collapsed = _collapse_events(combined)
    return collapsed.cast({name: dtype for name, dtype in PROCUREMENT_EVENT_SCHEMA.items()})


def build_from_parquet(records_glob: str, tombstones_glob: str) -> pl.DataFrame:
    """Read partitioned Bronze Parquet and build Silver with the native engine."""

    return build_procurement_events_polars(
        pl.read_parquet(records_glob), pl.read_parquet(tombstones_glob)
    )
