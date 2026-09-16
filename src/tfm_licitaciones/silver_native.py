"""Native Polars production engine for canonical Silver procurement events.

This module implements the same engine-neutral canonical contract as the
frozen python-row reference (:mod:`tfm_licitaciones.silver_reference`):
identity derivation, revision/tombstone semantics, explicit material
collisions, deterministic provenance selection, ``decimal(20,2)`` amounts,
UTC timestamps and the exact :data:`PROCUREMENT_EVENT_SCHEMA
<tfm_licitaciones.models.PROCUREMENT_EVENT_SCHEMA>`. Polars is the default
production engine inside the measured single-node envelope; a semantically
equivalent PySpark implementation is retained as an experimental scale-out
candidate (see ``experiments/silver_engine_comparison/``).

Genuineness: the transform uses only Polars expressions, grouping and
sorting. Each source branch resolves its payload fields through bounded
``json_path_match`` access on the canonical ``payload_json`` (one JSON
traversal per field, never a per-field re-parse of the whole payload), and
the success path performs no per-row Python (no ``iter_rows``/``to_dicts``/
``map_elements``/``apply`` and no ``ProcurementEvent`` allocation). Failure
paths fetch at most 5 offending ids (plus one scalar message value each) to
name them in the raised ``ValueError``.

Amount semantics are asymmetric on purpose, mirroring the reference: TED
amounts pass a ``_parse_amount`` gate (unparseable or negative -> null) and
always normalize separator style, while PLACSP amounts gate on key presence
with a non-null value, prefer the retained ``*_raw`` published text and fall
back to the unnormalized string of the value.

Documented micro-divergences vs the python-row reference (none of them
reachable from TED/OpenPLACSP/BOE adapter payloads, which are the only
producers of Bronze rows):

- number formatting is compared at value level: Python ``repr`` of a parsed
  float and Polars canonical number text agree on the value, so the
  ``decimal(20,2)`` columns are identical;
- timestamp parsing accepts RFC 3339 shapes (``T``/space separator plus
  ``Z``/``±HH:MM`` offset); a few extra forms ``datetime.fromisoformat``
  tolerates (compact ``YYYYMMDDThhmmss``, hour-only offsets) resolve to the
  ``undated`` marker instead of an instant;
- dates must be zero-padded ISO ``YYYY-MM-DD`` after slicing to 10 chars;
- JSON ``true``/``false`` scalars inside free-text fields render as
  ``true``/``false`` instead of Python's ``True``/``False``;
- underscore-containing amount texts (Python ``float`` accepts ``1_0``)
  stay null instead of raising the reference's not-representable error;
- container-valued amount fields (dict/list shapes) resolve to null instead
  of recursing into localized text.
"""

from __future__ import annotations

import polars as pl

from .models import PROCUREMENT_EVENT_SCHEMA

IMPLEMENTATION = "polars-native"
IMPLEMENTATION_DETAIL = (
    "native Polars expressions/group_by/sort over the Polars/Parquet "
    "boundary: bounded json_path_match field resolution per source branch, "
    "expression-level amount/timestamp/date derivation, group_by struct "
    "collision detection, provenance-minimum selection via stable sort + "
    "group_by first; no per-row Python on the success path"
)

_MAX_FAILURE_IDS = 5
_CANONICAL_ORDER = list(PROCUREMENT_EVENT_SCHEMA.names())
_COLLISION_FIELDS = [name for name in _CANONICAL_ORDER if name not in {"ingested_at", "event_id"}]
_PROVENANCE_COLUMNS = [
    "source",
    "source_file",
    "source_member",
    "source_member_index",
    "record_locator",
    "source_record_id",
    "raw_sha256",
    "raw_retrieved_at",
]
# Branch selects re-derive ``source`` as a per-source literal, so they carry
# the provenance columns without it.
_BRANCH_PROVENANCE = [name for name in _PROVENANCE_COLUMNS if name != "source"]
# Python selection tuples order null member indexes after concrete ones; the
# sort uses nulls_last on that single column instead of a numeric sentinel.
_DECIMAL_TEXT_PATTERN = r"^[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?$"


# ---------------------------------------------------------------------------
# Bounded JSON access helpers (pure expressions)
# ---------------------------------------------------------------------------


def _path(*keys: str) -> str:
    """Return a JSONPath using bracket member access (dashed keys included)."""

    return "$" + "".join(f'["{key}"]' for key in keys)


def _json(payload: pl.Expr, *keys: str) -> pl.Expr:
    """Raw JSON subtree text of one payload field; null when absent."""

    return payload.str.json_path_match(_path(*keys))


def _unquote(raw: pl.Expr) -> pl.Expr:
    """Decode a quoted JSON string scalar (escapes included), else null."""

    return raw.str.extract(r'^"((?:[^"\\]|\\.)*)"$', 1).str.replace_all(r"\\(.)", "$1")


def _leaf_text(raw: pl.Expr) -> pl.Expr:
    """Terminal ``_first_text``: scalar string/number as text, containers null."""

    value = pl.coalesce(_unquote(raw), raw).str.strip_chars()
    return (
        pl.when(raw.is_null() | (raw == "null") | raw.str.starts_with("[") | raw.str.starts_with("{"))
        .then(None)
        .otherwise(value)
    )


def _array_texts(raw: pl.Expr) -> pl.Expr:
    """Properly unescaped string elements of a JSON array text (or null)."""

    return pl.when(raw.str.starts_with("[")).then(raw.str.json_decode(pl.List(pl.String))).otherwise(None)


def _list_first_nonempty(raw: pl.Expr) -> pl.Expr:
    """First non-empty stripped element of an array text."""

    return (
        _array_texts(raw)
        .list.eval(pl.element().str.strip_chars())
        .list.eval(pl.element().filter(pl.element() != ""))
        .list.first()
    )


def _object_string_values(raw: pl.Expr) -> pl.Expr:
    """String values of an object text in document (sorted-key) order."""

    return (
        raw.str.extract_all(r':\s*"(?:[^"\\]|\\.)*"')
        .list.eval(
            pl.element()
            .str.extract(r'"((?:[^"\\]|\\.)*)"$', 1)
            .str.replace_all(r"\\(.)", "$1")
            .str.strip_chars()
        )
        .list.eval(pl.element().filter(pl.element() != ""))
    )


def _scalar_or_list_text(raw: pl.Expr) -> pl.Expr:
    return pl.when(raw.str.starts_with("[")).then(_list_first_nonempty(raw)).otherwise(_leaf_text(raw))


def _first_text(raw: pl.Expr) -> pl.Expr:
    """Bounded native equivalent of ``normalize._first_text`` on a subtree.

    Objects prefer the ``spa`` member (its value may be a scalar or an
    array) and otherwise return the first non-empty string value in
    document order, which matches the reference because ``payload_json``
    is stored with sorted keys.
    """

    spa = raw.str.json_path_match("$.spa")
    object_text = pl.coalesce(
        pl.when(spa.is_not_null()).then(_scalar_or_list_text(spa)).otherwise(None),
        _object_string_values(raw).list.first(),
    )
    return (
        pl.when(raw.is_null() | (raw == "null"))
        .then(None)
        .when(raw.str.starts_with("{"))
        .then(object_text)
        .when(raw.str.starts_with("["))
        .then(_list_first_nonempty(raw))
        .otherwise(_leaf_text(raw))
    )


def _value_truthy(raw: pl.Expr) -> pl.Expr:
    """Python truthiness of a JSON value for ``A or B`` field selection.

    Scalar strings arrive as their unquoted text, so an empty JSON string
    is an empty column value; containers arrive as their ``{}``/``[]``
    text and explicit JSON null is the string ``null``.
    """

    return (
        raw.is_not_null()
        & (raw != "")
        & (raw != "null")
        & (raw != "{}")
        & (raw != "[]")
        & (raw != "0")
        & (raw != "false")
    )


def _or_value(*raws: pl.Expr) -> pl.Expr:
    """Pick the first Python-truthy subtree, else the last one."""

    chosen = raws[-1]
    for candidate in reversed(raws[:-1]):
        chosen = pl.when(_value_truthy(candidate)).then(candidate).otherwise(chosen)
    return chosen


def _text(value):
    return pl.lit(value) if isinstance(value, str) else value


def _or_text(*values) -> pl.Expr:
    """First non-null non-empty text in a chain (literals allowed), else null."""

    chain: list[pl.Expr] = [_text(value) for value in values]
    chosen: pl.Expr | None = None
    for expression in chain:
        candidate = pl.when(expression.is_not_null() & (expression != "")).then(expression).otherwise(None)
        chosen = candidate if chosen is None else pl.coalesce(chosen, candidate)
    assert chosen is not None
    return chosen


def _elements(raw: pl.Expr) -> pl.Expr:
    """Flatten one identifier value to its string elements (or null).

    Scalars become single-element lists; arrays decode to their elements;
    objects reduce to their bounded ``_first_text``.
    """

    single = _first_text(raw)
    return (
        pl.when(raw.is_null() | (raw == "null"))
        .then(None)
        .when(raw.str.starts_with("["))
        .then(_array_texts(raw))
        .when(raw.str.starts_with("{"))
        .then(
            pl.when(single.is_not_null() & (single != "")).then(pl.concat_list([single])).otherwise(None)
        )
        .otherwise(pl.concat_list([_leaf_text(raw)]))
    )


def _dedup_texts(elements: pl.Expr) -> pl.Expr:
    return (
        elements.fill_null([])
        .list.eval(pl.element().str.strip_chars())
        .list.eval(pl.element().filter(pl.element() != ""))
        .list.unique(maintain_order=True)
    )


def _single_published(*raws: pl.Expr) -> pl.Expr:
    """Native ``_single_published_value`` across identifier keys."""

    collected: pl.Expr | None = None
    for raw in raws:
        parts = _elements(raw).fill_null([])
        collected = parts if collected is None else pl.concat_list([collected, parts])
    assert collected is not None
    unique = _dedup_texts(collected)
    return pl.when(unique.list.len() == 1).then(unique.list.first()).otherwise(None)


def _code_list(raw: pl.Expr) -> pl.Expr:
    """Native ``_as_code_list``: strip, drop empties, dedup keeping order."""

    single = _first_text(raw)
    # Only string/number scalars can wrap into a decodable one-element array;
    # booleans and other exotic tokens reduce to their bounded first text.
    wrappable = raw.str.contains(r'^(?:"(?:[^"\\]|\\.)*"|[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)$')
    wrapped = pl.when(raw.is_null() | (raw == "null") | ~wrappable).then(None).otherwise("[" + raw + "]")
    as_scalar = pl.when(single.is_not_null() & (single != "")).then(pl.concat_list([single])).otherwise(None)
    return _dedup_texts(pl.coalesce(_array_texts(wrapped), _array_texts(raw), as_scalar))


# ---------------------------------------------------------------------------
# Amount machinery
# ---------------------------------------------------------------------------


def _normalize_amount_text(text: pl.Expr) -> pl.Expr:
    """Native ``_normalize_amount_text`` (spaces out, separators resolved)."""

    squeezed = text.str.replace_all(" ", "")
    has_comma = squeezed.str.contains(r",")
    has_dot = squeezed.str.contains(r"\.")
    comma_is_decimal = squeezed.str.contains(r"\.[^.,]*,")
    dots = squeezed.str.count_matches(r"\.")
    return (
        pl.when(has_comma & has_dot & comma_is_decimal)
        .then(squeezed.str.replace_all(".", "", literal=True).str.replace_all(",", ".", literal=True))
        .when(has_comma & has_dot)
        .then(squeezed.str.replace_all(",", "", literal=True))
        .when(has_comma)
        .then(squeezed.str.replace_all(",", ".", literal=True))
        .when(dots == 1)
        .then(squeezed)
        .when(dots > 1)
        .then(squeezed.str.replace_all(".", "", literal=True))
        .otherwise(squeezed)
    )


def _amount_gate(norm: pl.Expr) -> pl.Expr:
    """Native TED ``_parse_amount`` gate: non-negative float or null."""

    parsed = norm.cast(pl.Float64, strict=False)
    return pl.when(parsed.is_not_null() & (parsed >= 0)).then(parsed).otherwise(None)


def _attach_amount(
    frame: pl.DataFrame,
    *,
    event_id: pl.Expr,
    norm: pl.Expr,
    fallback: pl.Expr,
    gate_ok: pl.Expr,
    numeric: pl.Expr,
) -> pl.DataFrame:
    """Attach the shared amount columns for one branch.

    ``norm`` is the normalized published text (null when it carried
    nothing), ``fallback`` the unnormalized string of the value (PLACSP
    legacy payloads without retained raw text), ``gate_ok`` whether the
    reference gate passes, and ``numeric`` the float used for value-level
    checks on exponent notations.
    """

    return frame.with_columns(
        __amount_event_id=event_id,
        __amount_norm=norm,
        __amount_fallback=fallback,
        __amount_gate_ok=gate_ok,
        __amount_numeric=numeric,
    ).with_columns(
        __amount_rep=pl.coalesce(
            pl.when(pl.col("__amount_norm").is_not_null() & (pl.col("__amount_norm") != "")).then(
                pl.col("__amount_norm")
            ).otherwise(None),
            pl.col("__amount_fallback"),
        ),
    ).with_columns(
        __amount_has_exponent=pl.col("__amount_rep").str.contains("e") | pl.col("__amount_rep").str.contains("E"),
        __amount_number=pl.coalesce(
            pl.col("__amount_numeric"),
            pl.col("__amount_rep").cast(pl.Float64, strict=False),
        ),
    ).with_columns(
        # Fallback texts are unnormalized: only decimal syntax is acceptable,
        # exactly like handing the raw string to Decimal().
        __bad_representable=(
            pl.col("__amount_gate_ok")
            & (
                ~pl.col("__amount_rep").str.contains(_DECIMAL_TEXT_PATTERN)
                | pl.col("__amount_number").is_null()
                | ~pl.col("__amount_number").is_finite()
            )
        ),
    ).with_columns(
        __bad_precision=(
            pl.col("__amount_gate_ok")
            & ~pl.col("__bad_representable")
            & pl.when(pl.col("__amount_has_exponent")).then(
                pl.col("__amount_number").abs() >= 1e18
            ).otherwise(
                pl.col("__amount_rep").str.replace(r"^[-+]", "")
                .str.extract(r"^(\d*)", 1)
                .str.strip_chars_start("0")
                .str.len_chars()
                >= 19
            )
        ),
        __amount_fraction=pl.col("__amount_rep").str.extract(r"\.(\d+)$", 1).str.strip_chars_end("0"),
    ).with_columns(
        __bad_scale=(
            pl.col("__amount_gate_ok")
            & ~pl.col("__bad_representable")
            & ~pl.col("__bad_precision")
            & pl.when(pl.col("__amount_has_exponent")).then(
                ((pl.col("__amount_number") * 100.0) - (pl.col("__amount_number") * 100.0).round()) != 0.0
            ).otherwise(
                pl.col("__amount_fraction").is_not_null() & (pl.col("__amount_fraction").str.len_chars() > 2)
            )
        ),
    ).with_columns(
        # Exact decimal text for validated rows: trimmed fraction on the
        # plain path, canonical float text on the exponent path.
        __amount_decimal_text=pl.when(pl.col("__amount_has_exponent")).then(
            pl.col("__amount_number").cast(pl.String)
        ).otherwise(
            pl.format(
                "{}{}{}",
                pl.col("__amount_rep").str.extract(r"^[-+]?\d*", 0),
                pl.when(pl.col("__amount_fraction").is_not_null() & (pl.col("__amount_fraction") != "")).then(
                    pl.lit(".")
                ).otherwise(pl.lit("")),
                pl.when(pl.col("__amount_fraction").is_not_null() & (pl.col("__amount_fraction") != "")).then(
                    pl.col("__amount_fraction")
                ).otherwise(pl.lit("")),
            )
        ),
    )


def _estimated_expr() -> pl.Expr:
    """Exact ``decimal(20,2)`` expression over the shared amount columns."""

    return (
        pl.when(
            pl.col("__amount_gate_ok")
            & ~pl.col("__bad_representable")
            & ~pl.col("__bad_precision")
            & ~pl.col("__bad_scale")
        )
        .then(pl.col("__amount_decimal_text"))
        .otherwise(None)
        .str.to_decimal(scale=2)
        .cast(pl.Decimal(20, 2))
    )


# ---------------------------------------------------------------------------
# Temporal helpers
# ---------------------------------------------------------------------------


def _aware_instant(text: pl.Expr) -> pl.Expr:
    """Native ``_aware_instant``: RFC 3339 text to a UTC instant or null."""

    guarded = (
        text.is_not_null()
        & text.str.contains(r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}")
        & text.str.contains(r"(?:[Zz]|[+-]\d{2}:\d{2})$")
    )
    # Normalize the separator to ``T`` and trailing ``Z`` to an explicit
    # offset so one explicit, non-inferred chrono format parses every
    # guarded row; non-guarded rows carry a parseable sentinel that is
    # nulled afterwards (strict=False leaves genuine junk as null too).
    normalized = (
        pl.when(guarded)
        .then(
            pl.format("{}T{}", text.str.slice(0, 10), text.str.slice(11)).str.replace(
                r"[Zz]$", "+00:00"
            )
        )
        .otherwise(pl.lit("1970-01-01T00:00:00+00:00"))
    )
    parsed = normalized.str.to_datetime(
        time_unit="us", format="%Y-%m-%dT%H:%M:%S%.f%:z", strict=False, time_zone="UTC"
    )
    return pl.when(guarded).then(parsed).otherwise(None)


def _iso_marker(instant: pl.Expr) -> pl.Expr:
    """UTC ISO-8601 marker identical to Python ``datetime.isoformat()``."""

    return instant.dt.to_string("%Y-%m-%dT%H:%M:%S%.f") + "+00:00"


def _parse_date(text: pl.Expr) -> pl.Expr:
    """Native ``_parse_date``: first 10 chars as zero-padded ISO date or null."""

    return pl.when(text.is_not_null() & (text != "")).then(
        text.str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    ).otherwise(None)


# ---------------------------------------------------------------------------
# Failure paths (the only place rows reach the driver, bounded)
# ---------------------------------------------------------------------------

# Helper/mask columns every branch carries so a single collect serves both
# the failure checks and the canonical output.
_HELPER_COLUMNS = [
    "__raise_ted_id",
    "__raise_placsp_id",
    "__raise_boe_id",
    "__raise_tomb_ref",
    "__raise_tomb_source",
    "__tomb_source",
    "__amount_event_id",
    "__amount_rep",
    "__amount_gate_ok",
    "__bad_representable",
    "__bad_scale",
    "__bad_precision",
]


def _raise_missing_identity(combined: pl.DataFrame) -> None:
    for column, message in (
        ("__raise_ted_id", "TED bronze record without a published notice number"),
        ("__raise_placsp_id", "PLACSP bronze record without an atom id"),
        ("__raise_boe_id", "BOE bronze record without an item id"),
        ("__raise_tomb_ref", "PLACSP tombstone without a full ref"),
    ):
        offenders = combined.filter(pl.col(column))
        if offenders.height:
            locator = offenders.get_column("record_locator").head(_MAX_FAILURE_IDS).to_list()[0]
            raise ValueError(f"{message}: {locator if locator is not None else ''}")


def _raise_unsupported_sources(records: pl.DataFrame) -> None:
    offenders = records.filter(~pl.col("source").is_in(["ted", "placsp", "boe"]))
    if offenders.height:
        source = offenders.get_column("source").head(_MAX_FAILURE_IDS).to_list()[0]
        raise ValueError(f"Unsupported bronze source for canonical Silver: {source!r}")


def _raise_unsupported_tombstones(combined: pl.DataFrame) -> None:
    offenders = combined.filter(pl.col("__raise_tomb_source"))
    if offenders.height:
        source = offenders.get_column("__tomb_source").head(_MAX_FAILURE_IDS).to_list()[0]
        raise ValueError(f"Unsupported tombstone source for canonical Silver: {source!r}")


def _raise_amounts(combined: pl.DataFrame) -> None:
    for column, template in (
        ("__bad_representable", "amount {!r} is not representable as decimal(20,2)"),
        ("__bad_scale", "amount {!r} has more than two fractional digits"),
        ("__bad_precision", "amount {!r} exceeds decimal(20,2) precision"),
    ):
        offenders = combined.filter(pl.col("__amount_gate_ok") & pl.col(column))
        if offenders.height:
            rows = offenders.select("__amount_event_id", "__amount_rep").head(_MAX_FAILURE_IDS).to_dicts()
            raise ValueError(f"{rows[0]['__amount_event_id']}: " + template.format(rows[0]["__amount_rep"]))


def _raise_collisions(combined: pl.DataFrame) -> None:
    grouped = combined.group_by("event_id").agg(pl.struct(_COLLISION_FIELDS).n_unique().alias("variants"))
    colliding = grouped.filter(pl.col("variants") > 1)
    if colliding.height:
        offenders = combined.join(colliding.select("event_id"), on="event_id", how="semi")
        per_field = offenders.group_by("event_id").agg(
            [pl.col(field).ne_missing(pl.col(field).first()).any().alias(field) for field in _COLLISION_FIELDS]
        )
        rows = per_field.sort("event_id").head(_MAX_FAILURE_IDS).to_dicts()
        first = rows[0]
        differing = sorted(field for field, differs in first.items() if field != "event_id" and differs)
        raise ValueError(
            f"Conflicting canonical mappings for event_id {first['event_id']!r}; "
            f"differing fields: {', '.join(differing)}"
        )


def _filler_helpers(**overrides: pl.Expr) -> list[tuple[str, pl.Expr]]:
    """Helper columns for a branch, with inert defaults and per-branch masks."""

    defaults: dict[str, pl.Expr] = {
        "__raise_ted_id": pl.lit(False),
        "__raise_placsp_id": pl.lit(False),
        "__raise_boe_id": pl.lit(False),
        "__raise_tomb_ref": pl.lit(False),
        "__raise_tomb_source": pl.lit(False),
        "__tomb_source": pl.lit(None, dtype=pl.String),
        "__amount_event_id": pl.lit(None, dtype=pl.String),
        "__amount_rep": pl.lit(None, dtype=pl.String),
        "__amount_gate_ok": pl.lit(False),
        "__bad_representable": pl.lit(False),
        "__bad_scale": pl.lit(False),
        "__bad_precision": pl.lit(False),
    }
    defaults.update(overrides)
    return [expression.alias(name) for name, expression in defaults.items() if name in _HELPER_COLUMNS]


# ---------------------------------------------------------------------------
# Source branches (lazy: they compose one plan per source)
# ---------------------------------------------------------------------------


def _ted_branch(ted: pl.LazyFrame) -> pl.LazyFrame:
    payload = pl.col("payload_json")
    # Stage 1: every payload field is resolved ONCE as a raw JSON subtree
    # column; downstream expressions reference these columns, keeping the
    # plan linear instead of re-traversing the payload per derivation.
    raws = ted.with_columns(
        __nd=_json(payload, "ND"),
        __notice_id_raw=_json(payload, "notice_id"),
        __ti=_json(payload, "TI"),
        __title_raw=_json(payload, "title"),
        __description_glo=_json(payload, "description-glo"),
        __description_raw=_json(payload, "description"),
        __buyer_name_raw=_json(payload, "buyer-name"),
        __buyer_raw=_json(payload, "buyer"),
        __pc=_json(payload, "PC"),
        __cpv_raw=_json(payload, "cpv"),
        __estimated_lot=_json(payload, "estimated-value-lot"),
        __framework_max=_json(payload, "framework-maximum-value-lot"),
        __amount_raw=_json(payload, "amount"),
        __currency=_json(payload, "currency"),
        __procedure_id=_json(payload, "procedure-identifier"),
        __bt04=_json(payload, "BT-04-notice"),
        __buyer_identifier=_json(payload, "buyer-identifier"),
        __bi=_json(payload, "BI"),
        __notice_type=_json(payload, "notice-type"),
        __cy=_json(payload, "CY"),
        __country=_json(payload, "country"),
        __pd=_json(payload, "PD"),
        __publication_date=_json(payload, "publication_date"),
        __publication_date_hyphen=_json(payload, "publication-date"),
        __url=_json(payload, "url"),
        __html=_json(payload, "links", "html"),
    )
    notice_id = _first_text(_or_value(pl.col("__nd"), pl.col("__notice_id_raw")))
    selected = pl.coalesce(pl.col("__estimated_lot"), pl.col("__framework_max"), pl.col("__amount_raw"))
    amount_leaf = _leaf_text(selected)
    amount_norm = _normalize_amount_text(amount_leaf.fill_null(""))
    gate = _amount_gate(amount_norm)
    base = raws.with_columns(
        __notice_id=notice_id,
        __amount_norm=amount_norm,
        __amount_gate=gate,
    )
    base = _attach_amount(
        base,
        event_id=pl.format("ted:notice:{}", pl.col("__notice_id")),
        norm=pl.col("__amount_norm"),
        fallback=pl.lit(None, dtype=pl.String),
        gate_ok=pl.col("__amount_gate").is_not_null(),
        numeric=pl.col("__amount_gate"),
    )
    procedure = _single_published(pl.col("__procedure_id"), pl.col("__bt04"))
    buyer_id = _single_published(pl.col("__buyer_identifier"), pl.col("__bi"))
    notice_type = _first_text(pl.col("__notice_type"))
    country_text = _first_text(_or_value(pl.col("__cy"), pl.col("__country")))
    url = _or_text(
        _first_text(pl.col("__url")),
        _leaf_text(pl.col("__html").str.json_path_match("$.ENG")),
        _leaf_text(pl.col("__html").str.json_path_match("$.SPA")),
        _leaf_text(pl.col("__html").str.json_path_match("$.DEU")),
        _leaf_text(pl.col("__html").str.json_path_match("$.FRA")),
        _object_string_values(pl.col("__html")).list.first(),
    )
    estimated = _estimated_expr()
    helpers = {
        "__raise_ted_id": pl.col("__notice_id").is_null() | (pl.col("__notice_id") == ""),
        "__amount_event_id": pl.col("__amount_event_id"),
        "__amount_rep": pl.col("__amount_rep"),
        "__amount_gate_ok": pl.col("__amount_gate_ok"),
        "__bad_representable": pl.col("__bad_representable"),
        "__bad_scale": pl.col("__bad_scale"),
        "__bad_precision": pl.col("__bad_precision"),
    }
    return base.select(
        *_BRANCH_PROVENANCE,
        event_id=pl.format("ted:notice:{}", pl.col("__notice_id")),
        procedure_id=pl.when(procedure.is_not_null() & (procedure != "")).then(
            pl.format("ted:procedure:{}", procedure)
        ).otherwise(None),
        source=pl.lit("ted"),
        source_event_type=pl.when(notice_type.is_not_null() & (notice_type != "")).then(
            pl.format("notice:{}", notice_type)
        ).otherwise(pl.lit("notice")),
        buyer_id=buyer_id,
        buyer_name=_or_text(_first_text(_or_value(pl.col("__buyer_name_raw"), pl.col("__buyer_raw")))),
        title=_or_text(_first_text(_or_value(pl.col("__ti"), pl.col("__title_raw")))),
        description=_or_text(_first_text(_or_value(pl.col("__description_glo"), pl.col("__description_raw")))),
        cpv_codes=_code_list(_or_value(pl.col("__pc"), pl.col("__cpv_raw"))),
        estimated_value=estimated,
        awarded_value=pl.lit(None, dtype=pl.Decimal(20, 2)),
        currency=pl.when(estimated.is_not_null()).then(
            _or_text(_leaf_text(pl.col("__currency")), "EUR")
        ).otherwise(None),
        publication_date=_parse_date(
            _first_text(
                _or_value(
                    pl.col("__pd"),
                    pl.col("__publication_date"),
                    pl.col("__publication_date_hyphen"),
                )
            )
        ),
        source_updated_at=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
        deadline=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
        status=pl.lit(None, dtype=pl.String),
        nuts_code=pl.lit(None, dtype=pl.String),
        country=(
            pl.when(country_text.is_null() | (country_text == ""))
            .then(None)
            .when(country_text == "ESP")
            .then(pl.lit("ES"))
            .when(country_text.str.len_chars() == 2)
            .then(pl.when(country_text.str.contains(r"^[A-Z]{2}$")).then(country_text).otherwise(None))
            .otherwise(None)
        ),
        source_url=url,
        ingested_at=pl.col("raw_retrieved_at"),
        *_filler_helpers(**helpers),
    )


def _placsp_branch(placsp: pl.LazyFrame) -> pl.LazyFrame:
    payload = pl.col("payload_json")
    raws = placsp.with_columns(
        __atom_id_raw=_json(payload, "atom_id"),
        __updated_raw=_json(payload, "updated"),
        __buyer_dir3=_json(payload, "buyer_dir3"),
        __buyer=_json(payload, "buyer"),
        __p_title=_json(payload, "title"),
        __summary=_json(payload, "summary"),
        __cpv=_json(payload, "cpv"),
        __status_code=_json(payload, "status_code"),
        __nuts_code=_json(payload, "nuts_code"),
        __p_url=_json(payload, "url"),
        __overall_value=_json(payload, "amount_estimated_overall"),
        __tax_value=_json(payload, "amount_tax_exclusive"),
        __overall_raw=_json(payload, "amount_estimated_overall_raw"),
        __tax_raw=_json(payload, "amount_tax_exclusive_raw"),
        __overall_currency=_json(payload, "amount_estimated_overall_currency"),
        __tax_currency=_json(payload, "amount_tax_exclusive_currency"),
    )
    atom_id = _first_text(pl.col("__atom_id_raw"))
    updated = _aware_instant(_leaf_text(pl.col("__updated_raw")))
    marker = pl.when(updated.is_not_null()).then(_iso_marker(updated)).otherwise(pl.lit("undated"))
    # Key presence (explicit JSON null included) selects the amount field,
    # mirroring ``key in payload`` in the reference.
    overall_present = payload.str.contains(r'"amount_estimated_overall"\s*:')
    tax_present = payload.str.contains(r'"amount_tax_exclusive"\s*:')
    overall_leaf = _leaf_text(pl.col("__overall_value"))
    tax_leaf = _leaf_text(pl.col("__tax_value"))
    base = raws.with_columns(
        __atom_id=atom_id,
        __updated=updated,
        __marker=marker,
        __overall_present=overall_present,
        __tax_present=tax_present,
        __overall_leaf=overall_leaf,
        __tax_leaf=tax_leaf,
        __overall_norm=_normalize_amount_text(_leaf_text(pl.col("__overall_raw")).fill_null("")),
        __tax_norm=_normalize_amount_text(_leaf_text(pl.col("__tax_raw")).fill_null("")),
    )
    base = _attach_amount(
        base,
        event_id=pl.format("placsp:notice:{}@{}", pl.col("__atom_id"), pl.col("__marker")),
        norm=pl.when(pl.col("__overall_present")).then(pl.col("__overall_norm")).otherwise(pl.col("__tax_norm")),
        fallback=pl.when(pl.col("__overall_present")).then(pl.col("__overall_leaf")).otherwise(pl.col("__tax_leaf")),
        gate_ok=(
            pl.when(pl.col("__overall_present"))
            .then(pl.col("__overall_leaf").is_not_null())
            .otherwise(pl.col("__tax_present") & pl.col("__tax_leaf").is_not_null())
        ),
        numeric=pl.when(pl.col("__overall_present"))
        .then(pl.col("__overall_leaf").cast(pl.Float64, strict=False))
        .otherwise(pl.col("__tax_leaf").cast(pl.Float64, strict=False)),
    )
    currency_text = pl.when(pl.col("__overall_present")).then(
        _leaf_text(pl.col("__overall_currency"))
    ).otherwise(_leaf_text(pl.col("__tax_currency")))
    estimated = _estimated_expr()
    helpers = {
        "__raise_placsp_id": pl.col("__atom_id").is_null() | (pl.col("__atom_id") == ""),
        "__amount_event_id": pl.col("__amount_event_id"),
        "__amount_rep": pl.col("__amount_rep"),
        "__amount_gate_ok": pl.col("__amount_gate_ok"),
        "__bad_representable": pl.col("__bad_representable"),
        "__bad_scale": pl.col("__bad_scale"),
        "__bad_precision": pl.col("__bad_precision"),
    }
    return base.select(
        *_BRANCH_PROVENANCE,
        event_id=pl.format("placsp:notice:{}@{}", pl.col("__atom_id"), pl.col("__marker")),
        procedure_id=pl.format("placsp:procedure:{}", pl.col("__atom_id")),
        source=pl.lit("placsp"),
        source_event_type=pl.lit("notice_snapshot"),
        buyer_id=_or_text(_leaf_text(pl.col("__buyer_dir3"))),
        buyer_name=_or_text(_leaf_text(pl.col("__buyer"))),
        title=_or_text(_leaf_text(pl.col("__p_title"))),
        description=_or_text(_leaf_text(pl.col("__summary"))),
        cpv_codes=_code_list(pl.col("__cpv")),
        estimated_value=estimated,
        awarded_value=pl.lit(None, dtype=pl.Decimal(20, 2)),
        currency=pl.when(estimated.is_not_null()).then(_or_text(currency_text)).otherwise(None),
        publication_date=pl.lit(None, dtype=pl.Date),
        source_updated_at=pl.col("__updated"),
        deadline=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
        status=_or_text(_leaf_text(pl.col("__status_code"))),
        nuts_code=_or_text(_leaf_text(pl.col("__nuts_code"))),
        country=pl.lit("ES"),
        source_url=_or_text(_leaf_text(pl.col("__p_url"))),
        ingested_at=pl.col("raw_retrieved_at"),
        *_filler_helpers(**helpers),
    )


def _boe_branch(boe: pl.LazyFrame) -> pl.LazyFrame:
    payload = pl.col("payload_json")
    raws = boe.with_columns(
        __item_id_raw=_json(payload, "item_id"),
        __identificador=_json(payload, "identificador"),
        __b_title=_json(payload, "title"),
        __titulo=_json(payload, "titulo"),
        __b_buyer=_json(payload, "buyer"),
        __department=_json(payload, "department"),
        __b_summary=_json(payload, "summary"),
        __b_publication_date=_json(payload, "publication_date"),
        __b_url=_json(payload, "url"),
    )
    item_id = _first_text(_or_value(pl.col("__item_id_raw"), pl.col("__identificador")))
    base = raws.with_columns(__item_id=item_id)
    return base.select(
        *_BRANCH_PROVENANCE,
        event_id=pl.format("boe:notice:{}", pl.col("__item_id")),
        procedure_id=pl.lit(None, dtype=pl.String),
        source=pl.lit("boe"),
        source_event_type=pl.lit("notice"),
        buyer_id=pl.lit(None, dtype=pl.String),
        buyer_name=_or_text(_first_text(_or_value(pl.col("__b_buyer"), pl.col("__department")))),
        title=_or_text(_first_text(_or_value(pl.col("__b_title"), pl.col("__titulo")))),
        description=_or_text(_first_text(pl.col("__b_summary"))),
        cpv_codes=pl.lit([], dtype=pl.List(pl.String)),
        estimated_value=pl.lit(None, dtype=pl.Decimal(20, 2)),
        awarded_value=pl.lit(None, dtype=pl.Decimal(20, 2)),
        currency=pl.lit(None, dtype=pl.String),
        publication_date=_parse_date(_first_text(pl.col("__b_publication_date"))),
        source_updated_at=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
        deadline=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
        status=pl.lit(None, dtype=pl.String),
        nuts_code=pl.lit(None, dtype=pl.String),
        country=pl.lit("ES"),
        source_url=_or_text(_first_text(pl.col("__b_url"))),
        ingested_at=pl.col("raw_retrieved_at"),
        *_filler_helpers(__raise_boe_id=pl.col("__item_id").is_null() | (pl.col("__item_id") == "")),
    )


def _tombstone_branch(tombstones: pl.LazyFrame) -> pl.LazyFrame:
    ref = pl.col("source_record_id").fill_null("").str.strip_chars()
    return tombstones.select(
        *_BRANCH_PROVENANCE,
        event_id=pl.format("placsp:tombstone:{}", ref),
        procedure_id=pl.format("placsp:procedure:{}", ref),
        source=pl.lit("placsp"),
        source_event_type=pl.lit("tombstone"),
        buyer_id=pl.lit(None, dtype=pl.String),
        buyer_name=pl.lit(None, dtype=pl.String),
        title=pl.lit(None, dtype=pl.String),
        description=pl.lit(None, dtype=pl.String),
        cpv_codes=pl.lit([], dtype=pl.List(pl.String)),
        estimated_value=pl.lit(None, dtype=pl.Decimal(20, 2)),
        awarded_value=pl.lit(None, dtype=pl.Decimal(20, 2)),
        currency=pl.lit(None, dtype=pl.String),
        publication_date=pl.lit(None, dtype=pl.Date),
        source_updated_at=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
        deadline=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
        status=pl.lit(None, dtype=pl.String),
        nuts_code=pl.lit(None, dtype=pl.String),
        country=pl.lit(None, dtype=pl.String),
        source_url=pl.lit(None, dtype=pl.String),
        ingested_at=pl.col("raw_retrieved_at"),
        *_filler_helpers(
            __raise_tomb_ref=ref == "",
            __raise_tomb_source=pl.col("source") != "placsp",
            __tomb_source=pl.col("source"),
        ),
    )


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------


def build_procurement_events_native(records: pl.DataFrame, tombstones: pl.DataFrame) -> pl.DataFrame:
    """Build the canonical Silver frame with native Polars expressions.

    Same contract as ``build_procurement_events_reference``: repeated
    observations of one source event collapse to the deterministic minimum
    provenance tuple, any other canonical difference under one ``event_id``
    fails explicitly, and the result is sorted ascending by ``event_id``.

    The four source branches compose a single lazy plan that is collected
    once; the explicit-failure checks are native filters over that one
    materialized frame.
    """

    if records.height:
        _raise_unsupported_sources(records)
    combined = pl.concat(
        [
            _ted_branch(records.lazy().filter(pl.col("source") == "ted")),
            _placsp_branch(records.lazy().filter(pl.col("source") == "placsp")),
            _boe_branch(records.lazy().filter(pl.col("source") == "boe")),
            _tombstone_branch(tombstones.lazy()),
        ],
        how="vertical",
    ).collect()
    if combined.height == 0:
        return pl.DataFrame(schema=PROCUREMENT_EVENT_SCHEMA)
    _raise_missing_identity(combined)
    _raise_unsupported_tombstones(combined)
    _raise_amounts(combined)
    _raise_collisions(combined)
    # Python selection tuples substitute "" for null member/locator values,
    # so those nulls sort FIRST among strings; only a null member index
    # sorts after concrete ones.
    ordered = combined.with_columns(
        __key_member=pl.col("source_member").fill_null(""),
        __key_locator=pl.col("record_locator").fill_null(""),
    ).sort(
        [
            "raw_retrieved_at",
            "source_file",
            "source_member_index",
            "__key_member",
            "__key_locator",
            "raw_sha256",
        ],
        nulls_last=True,
    )
    selected = (
        ordered.group_by("event_id")
        .agg(pl.exclude("event_id", "__key_member", "__key_locator", *_HELPER_COLUMNS).first())
        .sort("event_id")
    )
    return selected.select(
        [pl.col(name).cast(dtype) for name, dtype in PROCUREMENT_EVENT_SCHEMA.items()]
    )
