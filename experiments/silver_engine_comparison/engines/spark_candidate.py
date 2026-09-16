"""Native PySpark Bronze-to-Silver candidate (EXPERIMENT ONLY).

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
  handled as scalars;
- output is a sharded Parquet directory (no ``coalesce(1)``): global
  ``event_id`` order holds across ``part-*.parquet`` files in read order.

Genuineness: the transform below uses only built-in Spark SQL functions
(``get_json_object``/``from_json``, string/datetime/decimal functions,
``array_distinct``, ``row_number`` windows, ``groupBy`` aggregation). It
uses no Python UDF/pandas UDF, no ``coalesce(1)``, and no driver-side row
grouping/sorting/canonicalization. The success path performs zero
driver row fetches (only scalar ``count()`` validations run engine-side);
failure paths fetch at most 5 ``event_id`` values via ``limit``-bounded
``head`` solely to name them in the raised ``ValueError`` (a ``take``-free
Spark cannot surface an offending id at all). ``F.coalesce`` below is the
row-wise SQL null function, unrelated to ``DataFrame.coalesce``.

Materialization (recorded as ``CACHE_STRATEGY``): per-branch native frames
are persisted with ``MEMORY_AND_DISK`` with validation flags retained, so
the valid path parses each Bronze row once; validation counts, the
collision aggregation and the window/write then scan cache instead of
reparsing. The combined union frame is persisted as well. All persisted
frames are unpersisted after the write (see ``unpersist_frames``).

PySpark is an optional experiment dependency and must stay out of the base
project dependencies. Reproducible setup (pinned)::

    uv run --with 'pyspark==4.0.1' --with-editable . \\
        python -m experiments.silver_engine_comparison.bench_engines --profile tiny --seed 7 \\
        --work-dir /tmp/silver-engines-tiny --output /tmp/silver-engines-tiny.json

All ``pyspark`` imports live inside functions so importing this module
never requires PySpark to be installed.
"""

from __future__ import annotations

from typing import Any

CONTRACT_SCOPE = (
    "experiment-only: TED/PLACSP synthetic Bronze contract from "
    "tfm_licitaciones.bench_silver (JSON arrays for CPV, scalar identifier "
    "fields, whole-second updated instants); BOE and non-synthetic payload "
    "shapes are unsupported and fail explicitly; sharded Parquet output "
    "without coalesce(1)"
)
IMPLEMENTATION = "spark-native"
IMPLEMENTATION_DETAIL = (
    "native Spark SQL DataFrame execution (built-in functions and windows "
    "only: get_json_object/from_json, string/datetime/decimal expressions, "
    "array_distinct dedup, groupBy struct collision check, row_number "
    "window for provenance-minimum selection, global orderBy); no Python "
    "UDF, no collect/toPandas, no driver-side canonicalization, no "
    "coalesce(1); MEMORY_AND_DISK persist of branch and combined frames, "
    "unpersisted after write"
)
CACHE_STRATEGY = (
    "MEMORY_AND_DISK persist of the per-branch native frames (validation "
    "flags retained) plus the combined union frame; the valid path parses "
    "each Bronze row once and validation/collision/window/write scan "
    "cache; all persisted frames are unpersisted after the write"
)
SPARK_VERSION_PIN = "4.0.1"
INSTALL_HINT = (
    "PySpark is experiment-only: uv run --with 'pyspark==4.0.1' "
    "--with-editable . python -m experiments.silver_engine_comparison.bench_engines ..."
)
APP_NAME = "silver-engine-experiment"
# Parquet codec pinned to the Polars default so the engine-comparison write
# boundary is apples-to-apples: Spark's default is snappy while Polars
# writes zstd by default, which made Spark outputs ~5x larger on the same
# logical content (small profile: 698 kB vs 145 kB). Logical parity is
# codec-independent; the pin only removes compression as a confound.
PARQUET_CODEC = "zstd"

_CANONICAL_ORDER = (
    "event_id", "procedure_id", "source", "source_event_type", "buyer_id",
    "buyer_name", "title", "description", "cpv_codes", "estimated_value",
    "awarded_value", "currency", "publication_date", "source_updated_at",
    "deadline", "status", "nuts_code", "country", "source_url", "ingested_at",
)
_COLLISION_ORDER = tuple(name for name in _CANONICAL_ORDER if name != "ingested_at")
_PROVENANCE_ORDER = (
    "raw_retrieved_at", "source_file", "source_member_index", "source_member",
    "record_locator", "raw_sha256",
)

_TED_AMOUNT_FIELDS = ("estimated-value-lot", "framework-maximum-value-lot", "amount")
_TZ_SUFFIX = r"(Z|[+-][0-9]{2}:?[0-9]{2})$"
_MAX_IDS_IN_ERROR = 5
_UPDATED_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX"


def _localized_string() -> Any:
    """Schema fragment for TED ``{"spa": ...}`` localized text."""

    from pyspark.sql import types as T

    return T.StructType([T.StructField("spa", T.StringType())])


def _ted_schema() -> Any:
    """Closed schema for the synthetic TED Bronze contract (all nullable)."""

    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("ND", T.StringType()),
            T.StructField("PD", T.StringType()),
            T.StructField("publication_date", T.StringType()),
            T.StructField("publication-date", T.StringType()),
            T.StructField("TI", _localized_string()),
            T.StructField("description-glo", _localized_string()),
            T.StructField(
                "buyer-name",
                T.StructType([T.StructField("spa", T.ArrayType(T.StringType()))]),
            ),
            T.StructField("CY", T.StringType()),
            T.StructField("PC", T.ArrayType(T.StringType())),
            T.StructField("estimated-value-lot", T.StringType()),
            T.StructField("framework-maximum-value-lot", T.StringType()),
            T.StructField("amount", T.StringType()),
            T.StructField("currency", T.StringType()),
            T.StructField("procedure-identifier", T.StringType()),
            T.StructField("BT-04-notice", T.StringType()),
            T.StructField("notice-type", T.StringType()),
            T.StructField("buyer-identifier", T.StringType()),
            T.StructField("BI", T.StringType()),
            T.StructField("url", T.StringType()),
            T.StructField(
                "links",
                T.StructType(
                    [
                        T.StructField(
                            "html",
                            T.StructType(
                                [
                                    T.StructField("ENG", T.StringType()),
                                    T.StructField("SPA", T.StringType()),
                                    T.StructField("DEU", T.StringType()),
                                    T.StructField("FRA", T.StringType()),
                                ]
                            ),
                        )
                    ]
                ),
            ),
        ]
    )


def _placsp_schema() -> Any:
    """Closed schema for the synthetic OpenPLACSP Bronze contract (all nullable)."""

    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("atom_id", T.StringType()),
            T.StructField("updated", T.StringType()),
            T.StructField("title", T.StringType()),
            T.StructField("summary", T.StringType()),
            T.StructField("url", T.StringType()),
            T.StructField("status_code", T.StringType()),
            T.StructField("buyer", T.StringType()),
            T.StructField("buyer_dir3", T.StringType()),
            T.StructField("nuts_code", T.StringType()),
            T.StructField("cpv", T.ArrayType(T.StringType())),
            T.StructField("amount_estimated_overall", T.StringType()),
            T.StructField("amount_estimated_overall_currency", T.StringType()),
            T.StructField("amount_estimated_overall_raw", T.StringType()),
            T.StructField("amount_tax_exclusive", T.StringType()),
            T.StructField("amount_tax_exclusive_currency", T.StringType()),
            T.StructField("amount_tax_exclusive_raw", T.StringType()),
        ]
    )


def build_session(
    app_name: str = APP_NAME,
    master: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> Any:
    """Build a local Spark session with UTC timestamps and quiet logs."""

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        # Malformed casts yield null (Spark 3 behavior): this engine validates
        # amounts/dates explicitly with native expressions and raises loudly
        # where the contract requires failure, so silent nulls never hide a
        # required error. Recorded in session_config_snapshot().
        .config("spark.sql.ansi.enabled", "false")
    )
    if master is not None:
        builder = builder.master(master)
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


def session_config_snapshot(session: Any) -> dict[str, str]:
    """Return the subset of Spark configuration recorded for comparison."""

    keys = (
        "spark.master", "spark.app.name", "spark.sql.session.timeZone",
        "spark.sql.shuffle.partitions", "spark.default.parallelism",
        "spark.driver.memory", "spark.executor.memory", "spark.executor.cores",
        "spark.executor.instances", "spark.cores.max",
        "spark.eventLog.enabled", "spark.eventLog.dir",
        "spark.sql.adaptive.enabled", "spark.sql.ansi.enabled",
        "spark.sql.parquet.writeLegacyFormat",
    )
    snapshot: dict[str, str] = {}
    for key in keys:
        try:
            snapshot[key] = session.conf.get(key)
        except Exception:  # noqa: BLE001 - missing keys are recorded as such
            snapshot[key] = "<unset>"
    return snapshot


def _clean(F: Any, column: Any) -> Any:
    return F.trim(column)


def _null_if_empty(F: Any, column: Any) -> Any:
    return F.when(column.isNull() | (column == ""), F.lit(None)).otherwise(column)


def _json(F: Any, column: Any, path: str) -> Any:
    return F.get_json_object(column, path)


def _normalize_amount_text(F: Any, expression: Any) -> Any:
    """Replicate ``_normalize_amount_text`` with built-in string functions."""

    nospace = F.regexp_replace(F.trim(expression), " ", "")
    has_comma = F.instr(nospace, ",") > 0
    has_dot = F.instr(nospace, ".") > 0
    last_separator = F.regexp_extract(nospace, r"([,.])[^,.]*$", 1)
    both = F.when(
        last_separator == ",",
        F.regexp_replace(F.regexp_replace(nospace, r"\.", ""), ",", "."),
    ).otherwise(F.regexp_replace(nospace, ",", ""))
    dot_count = F.length(nospace) - F.length(F.regexp_replace(nospace, r"\.", ""))
    single = F.when(has_dot & (dot_count > 1), F.regexp_replace(nospace, r"\.", "")).otherwise(nospace)
    return (
        F.when(has_comma & has_dot, both)
        .when(has_comma, F.regexp_replace(nospace, ",", "."))
        .otherwise(single)
    )


def _amount_columns(
    F: Any, T: Any, source_text: Any, number_text: Any
) -> tuple[Any, Any]:
    """Build ``(estimated, amount_invalid)`` for one amount source text."""

    normalized = _normalize_amount_text(F, F.coalesce(source_text, F.lit("")))
    accepted = _normalize_amount_text(F, F.coalesce(number_text, F.lit(""))).cast("double")
    # ANSI is off by default, so malformed casts yield null instead of raising.
    finite = ~F.isnan(accepted) & (F.abs(accepted) < float("inf"))
    gate = accepted.isNotNull() & (accepted >= 0) & finite
    fraction = F.regexp_extract(normalized, r"\.([0-9]+)$", 1)
    fraction_significant = F.regexp_replace(fraction, "0+$", "")
    bad_scale = (fraction != "") & (F.length(fraction_significant) > 2)
    integer_part = F.regexp_replace(
        F.regexp_replace(F.split(normalized, r"\.").getItem(0), "-", ""), r"\+", ""
    )
    bad_precision = F.length(integer_part) > 18
    invalid = gate & F.coalesce(bad_scale | bad_precision, F.lit(False))
    estimated = (
        F.when(gate, normalized.cast(T.DecimalType(20, 2)))
        .otherwise(F.lit(None).cast(T.DecimalType(20, 2)))
    )
    return estimated, invalid


def _scalar_single(F: Any, first: Any, second: Any) -> Any:
    """Return the value only when both scalar texts agree on one distinct value."""

    first_clean = _null_if_empty(F, first)
    second_clean = _null_if_empty(F, second)
    return (
        F.when(first_clean.isNull() & second_clean.isNull(), F.lit(None))
        .when(first_clean.isNull(), second_clean)
        .when(second_clean.isNull() | (first_clean == second_clean), first_clean)
        .otherwise(F.lit(None))
    )


def _cpv_columns(F: Any, T: Any, codes_array: Any, json_text: Any) -> tuple[Any, Any]:
    """Decode a CPV array column preserving order without duplicates.

    First-occurrence order from ``array_distinct`` is relied upon ONLY on
    the pinned ``pyspark==4.0.1`` build: it is guarded by an explicit order
    unit test plus full-frame parity on every comparison run, so any
    reorder would fail loudly instead of passing silently.
    ``json_text`` (one extra scalar extraction) validates the array shape
    so scalar payloads fail explicitly instead of collapsing to ``[]``.
    """

    empty = F.array().cast(T.ArrayType(T.StringType()))
    trimmed = F.trim(F.coalesce(json_text, F.lit("")))
    is_array = json_text.isNull() | (F.substring(trimmed, 1, 1) == "[")
    deduped = F.array_distinct(codes_array)
    codes = F.when(codes_array.isNull() & json_text.isNull(), empty).otherwise(F.coalesce(deduped, empty))
    return codes, ~is_array


def _persist(frame: Any) -> Any:
    """Persist a native intermediate frame (see ``CACHE_STRATEGY``)."""

    from pyspark.storagelevel import StorageLevel

    return frame.persist(StorageLevel.MEMORY_AND_DISK)


def unpersist_frames(frames: Any) -> None:
    """Release persisted intermediate frames after the write."""

    for frame in frames:
        frame.unpersist()


def _failure_ids(frame: Any, order: tuple[str, ...] = ("event_id",)) -> list[str]:
    """Fetch a bounded sample of event ids for an explicit failure message."""

    return [row[0] for row in frame.select(*order).distinct().limit(_MAX_IDS_IN_ERROR).head(_MAX_IDS_IN_ERROR)]


def _raise_if_matches(frame: Any, condition: Any, message: str) -> None:
    """Raise ``ValueError`` naming offending events when ``condition`` matches."""

    if frame.filter(condition).limit(1).count():
        raise ValueError(f"{message}: {_failure_ids(frame.filter(condition))!r}")


def _ted_events(F: Any, T: Any, records: Any) -> Any:
    """Map TED Bronze rows to canonical event rows with built-in functions.

    The payload is parsed once with ``from_json`` under a closed schema;
    every field below is a struct reference, not a repeated JSON parse.
    Returns ``(clean, persisted)``: the flag-free frame for the union plus
    the persisted flag-bearing frame for cache release. One combined-flag
    action both materializes the parse and validates, so the valid path
    never reparses.
    """

    ted = records.filter(F.col("source") == "ted").withColumn(
        "__doc", F.from_json(F.col("payload_json"), _ted_schema())
    )
    doc = F.col("__doc")
    notice_id = _null_if_empty(F, _clean(F, doc.getField("ND")))
    title = _null_if_empty(F, _clean(F, doc.getField("TI").getField("spa")))
    description = _null_if_empty(F, _clean(F, doc.getField("description-glo").getField("spa")))
    buyer_name = _null_if_empty(F, _clean(F, doc.getField("buyer-name").getField("spa").getItem(0)))
    publication_raw = F.coalesce(
        doc.getField("PD"),
        doc.getField("publication_date"),
        doc.getField("publication-date"),
    )
    selected = F.coalesce(
        doc.getField("estimated-value-lot"),
        doc.getField("framework-maximum-value-lot"),
        doc.getField("amount"),
    )
    estimated, amount_invalid = _amount_columns(F, T, selected, selected)
    currency_raw = _null_if_empty(F, _clean(F, doc.getField("currency")))
    currency = F.when(
        estimated.isNotNull(), F.coalesce(currency_raw, F.lit("EUR"))
    ).otherwise(F.lit(None))
    procedure = _scalar_single(
        F, _clean(F, doc.getField("procedure-identifier")),
        _clean(F, doc.getField("BT-04-notice")),
    )
    notice_type = _null_if_empty(F, _clean(F, doc.getField("notice-type")))
    buyer_id = _scalar_single(
        F, _clean(F, doc.getField("buyer-identifier")), _clean(F, doc.getField("BI"))
    )
    country_raw = _clean(F, doc.getField("CY"))
    country = (
        F.when(country_raw == "ESP", "ES")
        .when(country_raw.rlike("^[A-Z]{2}$"), country_raw)
        .otherwise(F.lit(None))
    )
    links_html = doc.getField("links").getField("html")
    url = F.coalesce(
        doc.getField("url"),
        links_html.getField("ENG"),
        links_html.getField("SPA"),
        links_html.getField("DEU"),
        links_html.getField("FRA"),
    )
    cpv_codes, cpv_invalid = _cpv_columns(
        F, T, doc.getField("PC"), _json(F, F.col("payload_json"), "$.PC")
    )
    frame = ted.select(
        F.concat(F.lit("ted:notice:"), notice_id).alias("event_id"),
        F.when(procedure.isNull(), F.lit(None))
        .otherwise(F.concat(F.lit("ted:procedure:"), procedure))
        .alias("procedure_id"),
        F.lit("ted").alias("source"),
        F.when(notice_type.isNull(), "notice")
        .otherwise(F.concat(F.lit("notice:"), notice_type))
        .alias("source_event_type"),
        buyer_id.alias("buyer_id"),
        buyer_name.alias("buyer_name"),
        title.alias("title"),
        description.alias("description"),
        cpv_codes.alias("cpv_codes"),
        estimated.alias("estimated_value"),
        F.lit(None).cast(T.DecimalType(20, 2)).alias("awarded_value"),
        currency.alias("currency"),
        F.to_date(F.substring(publication_raw, 1, 10)).alias("publication_date"),
        F.lit(None).cast(T.TimestampType()).alias("source_updated_at"),
        F.lit(None).cast(T.TimestampType()).alias("deadline"),
        F.lit(None).cast(T.StringType()).alias("status"),
        F.lit(None).cast(T.StringType()).alias("nuts_code"),
        country.alias("country"),
        _null_if_empty(F, _clean(F, url)).alias("source_url"),
        F.col("raw_retrieved_at").alias("ingested_at"),
        *[F.col(name) for name in _PROVENANCE_ORDER],
        notice_id.isNull().alias("__notice_missing"),
        amount_invalid.alias("__amount_invalid"),
        cpv_invalid.alias("__cpv_invalid"),
    ).transform(_persist)
    if frame.filter(
        F.col("__notice_missing") | F.col("__amount_invalid") | F.col("__cpv_invalid")
    ).limit(1).count():
        _raise_if_matches(
            frame, F.col("__notice_missing"), "TED bronze record without a published notice number"
        )
        _raise_if_matches(frame, F.col("__amount_invalid"), "Amount is not representable as decimal(20,2)")
        _raise_if_matches(frame, F.col("__cpv_invalid"), "Experiment contract requires CPV JSON arrays")
    return frame.drop("__notice_missing", "__amount_invalid", "__cpv_invalid"), frame


def _placsp_events(F: Any, T: Any, records: Any) -> Any:
    """Map OpenPLACSP Bronze rows to canonical snapshot rows with built-ins.

    Single ``from_json`` parse. Returns ``(clean, persisted)`` like
    :func:`_ted_events`: one combined-flag action materializes and
    validates, so the valid path never reparses.
    """

    placsp = records.filter(F.col("source") == "placsp").withColumn(
        "__doc", F.from_json(F.col("payload_json"), _placsp_schema())
    )
    doc = F.col("__doc")
    payload = F.col("payload_json")
    atom_id = _null_if_empty(F, _clean(F, doc.getField("atom_id")))
    updated_raw = _clean(F, doc.getField("updated"))
    has_zone = updated_raw.rlike(_TZ_SUFFIX)
    updated = F.when(
        has_zone, F.to_timestamp(updated_raw, _UPDATED_FORMAT)
    ).otherwise(F.lit(None))
    sub_second = updated.isNotNull() & (updated != F.date_trunc("second", updated))
    marker = F.when(
        updated.isNull(), "undated"
    ).otherwise(F.concat(F.date_format(updated, "yyyy-MM-dd'T'HH:mm:ss"), F.lit("+00:00")))
    has_estimated = payload.rlike(r'"amount_estimated_overall"\s*:')
    has_tax = payload.rlike(r'"amount_tax_exclusive"\s*:')
    chosen = (
        F.when(has_estimated, "estimated").when(has_tax, "tax").otherwise(F.lit(None))
    )
    number_text = (
        F.when(chosen == "estimated", doc.getField("amount_estimated_overall"))
        .when(chosen == "tax", doc.getField("amount_tax_exclusive"))
        .otherwise(F.lit(None))
    )
    raw_text = (
        F.when(chosen == "estimated", doc.getField("amount_estimated_overall_raw"))
        .when(chosen == "tax", doc.getField("amount_tax_exclusive_raw"))
        .otherwise(F.lit(None))
    )
    source_text = F.coalesce(raw_text, number_text)
    estimated, amount_invalid = _amount_columns(F, T, source_text, number_text)
    chosen_currency = (
        F.when(chosen == "estimated", doc.getField("amount_estimated_overall_currency"))
        .when(chosen == "tax", doc.getField("amount_tax_exclusive_currency"))
        .otherwise(F.lit(None))
    )
    currency = F.when(
        estimated.isNotNull(), _null_if_empty(F, _clean(F, chosen_currency))
    ).otherwise(F.lit(None))
    cpv_codes, cpv_invalid = _cpv_columns(
        F, T, doc.getField("cpv"), _json(F, payload, "$.cpv")
    )
    frame = placsp.select(
        F.concat(F.lit("placsp:notice:"), atom_id, F.lit("@"), marker).alias("event_id"),
        F.concat(F.lit("placsp:procedure:"), atom_id).alias("procedure_id"),
        F.lit("placsp").alias("source"),
        F.lit("notice_snapshot").alias("source_event_type"),
        _null_if_empty(F, _clean(F, doc.getField("buyer_dir3"))).alias("buyer_id"),
        _null_if_empty(F, _clean(F, doc.getField("buyer"))).alias("buyer_name"),
        _null_if_empty(F, _clean(F, doc.getField("title"))).alias("title"),
        _null_if_empty(F, _clean(F, doc.getField("summary"))).alias("description"),
        cpv_codes.alias("cpv_codes"),
        estimated.alias("estimated_value"),
        F.lit(None).cast(T.DecimalType(20, 2)).alias("awarded_value"),
        currency.alias("currency"),
        F.lit(None).cast(T.DateType()).alias("publication_date"),
        updated.cast(T.TimestampType()).alias("source_updated_at"),
        F.lit(None).cast(T.TimestampType()).alias("deadline"),
        _null_if_empty(F, _clean(F, doc.getField("status_code"))).alias("status"),
        _null_if_empty(F, _clean(F, doc.getField("nuts_code"))).alias("nuts_code"),
        F.lit("ES").alias("country"),
        _null_if_empty(F, _clean(F, doc.getField("url"))).alias("source_url"),
        F.col("raw_retrieved_at").alias("ingested_at"),
        *[F.col(name) for name in _PROVENANCE_ORDER],
        atom_id.isNull().alias("__atom_missing"),
        sub_second.alias("__sub_second"),
        amount_invalid.alias("__amount_invalid"),
        cpv_invalid.alias("__cpv_invalid"),
    ).transform(_persist)
    if frame.filter(
        F.col("__atom_missing")
        | F.col("__sub_second")
        | F.col("__amount_invalid")
        | F.col("__cpv_invalid")
    ).limit(1).count():
        _raise_if_matches(
            frame, F.col("__atom_missing"), "PLACSP bronze record without an atom id"
        )
        _raise_if_matches(
            frame, F.col("__sub_second"), "Experiment contract supports whole-second updated instants only"
        )
        _raise_if_matches(frame, F.col("__amount_invalid"), "Amount is not representable as decimal(20,2)")
        _raise_if_matches(frame, F.col("__cpv_invalid"), "Experiment contract requires CPV JSON arrays")
    return frame.drop("__atom_missing", "__sub_second", "__amount_invalid", "__cpv_invalid"), frame


def _tombstone_events(F: Any, T: Any, tombstones: Any) -> Any:
    """Map PLACSP deletion controls to canonical tombstone rows with built-ins.

    Returns ``(clean, persisted)`` like :func:`_ted_events`.
    """

    ref = _null_if_empty(F, F.trim(F.col("source_record_id")))
    empty_codes = F.array().cast(T.ArrayType(T.StringType()))
    frame = tombstones.select(
        F.concat(F.lit("placsp:tombstone:"), ref).alias("event_id"),
        F.concat(F.lit("placsp:procedure:"), ref).alias("procedure_id"),
        F.lit("placsp").alias("source"),
        F.lit("tombstone").alias("source_event_type"),
        F.lit(None).cast(T.StringType()).alias("buyer_id"),
        F.lit(None).cast(T.StringType()).alias("buyer_name"),
        F.lit(None).cast(T.StringType()).alias("title"),
        F.lit(None).cast(T.StringType()).alias("description"),
        empty_codes.alias("cpv_codes"),
        F.lit(None).cast(T.DecimalType(20, 2)).alias("estimated_value"),
        F.lit(None).cast(T.DecimalType(20, 2)).alias("awarded_value"),
        F.lit(None).cast(T.StringType()).alias("currency"),
        F.lit(None).cast(T.DateType()).alias("publication_date"),
        F.lit(None).cast(T.TimestampType()).alias("source_updated_at"),
        F.lit(None).cast(T.TimestampType()).alias("deadline"),
        F.lit(None).cast(T.StringType()).alias("status"),
        F.lit(None).cast(T.StringType()).alias("nuts_code"),
        F.lit(None).cast(T.StringType()).alias("country"),
        F.lit(None).cast(T.StringType()).alias("source_url"),
        F.col("raw_retrieved_at").alias("ingested_at"),
        *[F.col(name) for name in _PROVENANCE_ORDER],
        (F.col("source") != "placsp").alias("__bad_source"),
        ref.isNull().alias("__ref_missing"),
    ).transform(_persist)
    if frame.filter(F.col("__bad_source") | F.col("__ref_missing")).limit(1).count():
        _raise_if_matches(
            frame, F.col("__bad_source"), "Unsupported tombstone source for canonical Silver"
        )
        _raise_if_matches(
            frame, F.col("__ref_missing"), "PLACSP tombstone without a full ref"
        )
    return frame.drop("__bad_source", "__ref_missing"), frame


def _check_experiment_scope(F: Any, records: Any) -> None:
    """Fail explicitly on Bronze sources outside the experiment contract.

    The success path fetches zero rows (scalar ``count`` only); offending
    values are fetched, bounded, solely to name them on failure.
    """

    if records.filter(~F.col("source").isin("ted", "placsp")).limit(1).count():
        distinct = [row[0] for row in records.select("source").distinct().limit(10).head(10)]
        unsupported = [source for source in distinct if source not in ("ted", "placsp")]
        raise ValueError(
            "Experiment spark-native engine supports TED/PLACSP synthetic Bronze only; "
            f"found sources: {unsupported!r}"
        )


def _collapse_events(F: Any, W: Any, frame: Any) -> Any:
    """Deduplicate observations, detect collisions, select provenance minimum.

    All operations are engine-side: a ``groupBy`` struct-distinctness check
    detects material collisions, then a ``row_number`` window keeps the
    deterministic minimum observation per ``event_id``. Callers pass the
    persisted combined frame so validation, collision and window jobs share
    one materialization.
    """

    variants = frame.groupBy("event_id").agg(
        F.countDistinct(F.struct(*_COLLISION_ORDER)).alias("__variants")
    )
    bad = variants.filter(F.col("__variants") > 1)
    if bad.limit(1).count():
        raise ValueError(
            f"Conflicting canonical mappings for event_id(s) {_failure_ids(bad)!r}; "
            "differing fields check required"
        )
    keyed = frame.withColumns(
        {
            "__member_nulls_last": F.col("source_member_index").isNull(),
            "__member_index_or_zero": F.coalesce(F.col("source_member_index"), F.lit(0)),
            "__source_member": F.coalesce(F.col("source_member"), F.lit("")),
            "__record_locator": F.coalesce(F.col("record_locator"), F.lit("")),
        }
    )
    window = W.partitionBy("event_id").orderBy(
        F.col("raw_retrieved_at").asc(),
        F.col("source_file").asc(),
        F.col("__member_nulls_last").asc(),
        F.col("__member_index_or_zero").asc_nulls_last(),
        F.col("__source_member").asc(),
        F.col("__record_locator").asc(),
        F.col("raw_sha256").asc(),
    )
    ranked = keyed.withColumn("__rank", F.row_number().over(window)).filter(F.col("__rank") == 1)
    return ranked.select(*_CANONICAL_ORDER).orderBy("event_id")


def build_spark_events(session: Any, records_glob: str, tombstones_glob: str) -> tuple[Any, list[Any]]:
    """Build the canonical Silver DataFrame with native Spark SQL execution.

    Returns ``(frame, cached_frames)``: the ordered lazy Silver frame plus
    every persisted intermediate (per-branch frames with validation flags
    and the combined union frame). Valid-path jobs are: one scope count
    over the pruned Bronze read, one materialize-and-validate action per
    branch (each Bronze row parsed once), one collision aggregation and
    the window/write job, all after the first sharing cache. Callers must
    pass ``cached_frames`` to :func:`unpersist_frames` after the write
    (see ``CACHE_STRATEGY``).
    """

    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window as W

    records = session.read.parquet(records_glob)
    tombstones = session.read.parquet(tombstones_glob)
    _check_experiment_scope(F, records)
    ted, ted_cached = _ted_events(F, T, records)
    placsp, placsp_cached = _placsp_events(F, T, records)
    tomb, tomb_cached = _tombstone_events(F, T, tombstones)
    combined = ted.unionByName(placsp).unionByName(tomb).transform(_persist)
    return _collapse_events(F, W, combined), [ted_cached, placsp_cached, tomb_cached, combined]


def write_silver_spark(frame: Any, output_dir: str) -> None:
    """Write the Silver DataFrame as sharded Parquet (never ``coalesce(1)``).

    Compression is pinned to :data:`PARQUET_CODEC` (zstd, the Polars
    default) so Spark and Polars outputs are comparable byte-wise.
    """

    frame.write.mode("overwrite").option("compression", PARQUET_CODEC).parquet(output_dir)
