"""Native Polars kernel for the canonical Silver contract (guarded domain).

The production entry point is
:func:`tfm_licitaciones.silver.build_procurement_events`, which first asks
:mod:`tfm_licitaciones.silver_guard` whether the *complete* batch (records
and tombstones) belongs to the eligible domain documented there, and only
then runs this kernel; otherwise the frozen python-row reference processes
the original frames. This module therefore implements the historical
semantics only for that domain instead of carrying approximations for
payloads the guard excludes:

- identity values resolve through the historical ``or`` chains to non-empty
  strings;
- localized text is a string, a flat string list, or an object of
  strings/flat string lists (the ``spa`` preference and document-order
  fallback of ``_first_text`` are kept);
- code lists are strings or flat string lists;
- amounts are plain decimals (optional sign, at most 18 integer digits and
  two fractional digits), digit-free strings whose historical float gate
  yields null, or JSON numbers whose Python ``str()`` is a plain decimal;
  every eligible decimal is representable as ``decimal(20,2)``, so no
  scale/precision failure classification is reachable here;
- publication dates start with a strict ``YYYY-MM-DD``;
- ``updated`` is empty or a strict RFC 3339 instant with seconds and a
  whole-minute offset;
- no probed key appears nested (the guard rejects shadowing), which keeps
  the textual quoted-string probes exact over Bronze's canonical JSON;
- tombstones are PLACSP deletions with a non-empty reference.

Within that domain the kernel mirrors the reference exactly: identity
derivation, revisions/tombstone rows, explicit material-collision
diagnostics in reference order, deterministic provenance selection,
``decimal(20,2)`` amounts, UTC timestamps and
:data:`PROCUREMENT_EVENT_SCHEMA <tfm_licitaciones.models.PROCUREMENT_EVENT_SCHEMA>`.

Execution uses only Polars expressions, grouping and sorting; the success
path performs no per-row Python (no ``iter_rows``/``to_dicts``/
``map_elements``/``apply`` and no ``ProcurementEvent`` allocation). The only
failure path is the material-collision diagnostic, which fetches a bounded
number of rows to name the first conflict exactly like the reference.
"""

from __future__ import annotations

import polars as pl

from .models import PROCUREMENT_EVENT_SCHEMA

IMPLEMENTATION = "polars-native"
IMPLEMENTATION_DETAIL = (
    "native Polars expressions/group_by/sort over the guarded eligible "
    "domain: bounded json_path_match field resolution per source branch, "
    "textual quoted-string probes exact under the guard's flat-probe-key "
    "requirement, localized string/list recursion, lexical decimal(20,2) "
    "decisions for plain decimals, strict RFC 3339 parsing, group_by struct "
    "collision detection with first-conflict diagnostics, provenance-minimum "
    "selection via stable sort + group_by first; no per-row Python"
)

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
_ZERO_NUMBER_PATTERN = r"^[-+]?(?:0+(?:\.0*)?|\.0+)(?:[eE][-+]?\d+)?$"
# Flat JSON value fragment at object-value position: quoted strings and flat
# arrays of quoted strings. The eligibility guard rejects anything else
# (numbers, booleans, nulls inside lists, nested members).
_OBJECT_VALUE_PATTERN = r':\s*("(?:[^"\\]|\\.)*"|\[(?:\s*"(?:[^"\\]|\\.)*"\s*,?)*\s*\])'


# ---------------------------------------------------------------------------
# Bounded JSON access helpers (pure expressions)
# ---------------------------------------------------------------------------


def _path(*keys: str) -> str:
    """Return a JSONPath using bracket member access (dashed keys included)."""

    return "$" + "".join(f'["{key}"]' for key in keys)


def _json(payload: pl.Expr, *keys: str) -> pl.Expr:
    """Raw JSON subtree text of one payload field; null when absent."""

    return payload.str.json_path_match(_path(*keys))


def _is_json_string(container: pl.Expr, key: str) -> pl.Expr:
    """Whether the JSON value for ``key`` in ``container`` is a quoted string.

    Textual probe over the canonical payload: Bronze serializes payloads with
    :func:`json.dumps`, so escaped quotes inside string content cannot fake
    the ``"key": "`` pattern, and the eligibility guard rejects payloads
    where a probed key also appears nested. The first occurrence of the
    pattern is therefore the value of the top-level route.
    """

    return container.str.contains(f'"{key}"\\s*:\\s*"', literal=False).fill_null(False)


def _is_zero_number(text: pl.Expr) -> pl.Expr:
    """Whether decimal text is lexically a numeric zero (any exponent)."""

    return text.str.contains(_ZERO_NUMBER_PATTERN).fill_null(False)


def _truthy(decoded: pl.Expr, is_string: pl.Expr) -> pl.Expr:
    """Python truthiness of one JSON value from its decoded text and type tag.

    ``decoded`` is the ``json_path_match`` text (strings decoded, other
    scalars/containers as JSON text, null for missing/JSON-null).
    ``is_string`` records whether the JSON value was quoted. Falsy cases:
    missing/JSON-null, empty string, empty array/object (non-string),
    numeric zero (non-string) and boolean false (non-string). A string
    ``"0"``/``"false"``/``"[]"``/``"{}"``/``"null"`` is always truthy when
    non-empty; ``"true"`` and non-zero numbers are truthy either way.
    """

    return (
        decoded.is_not_null()
        & (decoded != "")
        & (is_string | (decoded != "[]"))
        & (is_string | (decoded != "{}"))
        & (is_string | ~_is_zero_number(decoded))
        & (is_string | (decoded != "false"))
    ).fill_null(False)


def _or_typed(pairs: list[tuple[pl.Expr, pl.Expr]]) -> tuple[pl.Expr, pl.Expr]:
    """Pick the first Python-truthy ``(raw, is_string)`` pair, else the last.

    Returns ``(chosen_raw, chosen_is_string)`` expressions. Mirrors the
    reference ``A or B [or C]`` chains evaluated on raw JSON values before
    ``_first_text``.
    """

    raws = [raw for raw, _ in pairs]
    tags = [tag for _, tag in pairs]
    flags = [_truthy(raw, tag) for raw, tag in pairs]
    chosen_raw: pl.Expr = raws[-1]
    chosen_tag: pl.Expr = tags[-1]
    for flag, raw, tag in reversed(list(zip(flags[:-1], raws[:-1], tags[:-1]))):
        chosen_raw = pl.when(flag).then(raw).otherwise(chosen_raw)
        chosen_tag = pl.when(flag).then(tag).otherwise(chosen_tag)
    return chosen_raw, chosen_tag


def _text(value):
    # NB: string concatenation uses ``+`` throughout this module, never
    # ``pl.format``: on this Polars version ``pl.format`` panics
    # (``assertion failed: i < self.len()`` in binview) when any argument
    # derives from the ``_first_text`` expression graph, while ``+``
    # concatenation is exact and panic-free.
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


def _leaf_text(raw: pl.Expr, is_string: pl.Expr) -> pl.Expr:
    """Terminal ``_first_text`` for scalar JSON values (exact, no unescape).

    ``raw`` is already exactly decoded by ``json_path_match``. Strings keep
    their text verbatim (including ``"null"``/``"[]"``/``"{}"`` and values
    with surrounding quotes); numbers keep their JSON text, which the guard
    restricts to plain decimals (identical to Python ``str()`` for the
    eligible amount values); containers reduce to null so callers route them
    to list/object logic. Booleans are outside the eligible domain.
    """

    stripped = raw.str.strip_chars()
    return (
        pl.when(raw.is_null())
        .then(None)
        .when(is_string)
        .then(stripped)
        .when(raw.str.starts_with("[") | raw.str.starts_with("{"))
        .then(None)
        .otherwise(stripped)
    )


def _array_texts(raw: pl.Expr) -> pl.Expr:
    """Decoded string elements of an eligible flat JSON string array (or null)."""

    return pl.when(raw.str.starts_with("[")).then(raw.str.json_decode(pl.List(pl.String))).otherwise(None)


def _list_first_nonempty(raw: pl.Expr) -> pl.Expr:
    """First non-empty stripped element of a flat string array text."""

    return (
        _array_texts(raw)
        .list.eval(pl.element().str.strip_chars())
        .list.eval(pl.element().filter(pl.element().is_not_null() & (pl.element() != "")))
        .list.first()
    )


def _object_string_values(raw: pl.Expr) -> pl.Expr:
    """First-text candidates of an eligible object text in document order.

    Extracts flat value fragments at value position in document order
    (quoted strings and flat string arrays; the guard rejects anything
    else), decodes each exactly (strings via the throw-safe
    ``json_path_match``; arrays via ``json_decode`` to their first non-empty
    element) and returns the list of stripped non-empty candidates.
    """

    full = raw.str.extract_all(_OBJECT_VALUE_PATTERN)
    frags = full.list.eval(pl.element().str.extract(_OBJECT_VALUE_PATTERN, 1))
    decoded = (
        frags.list.eval(
            pl.when(pl.element().str.starts_with('"'))
            .then(pl.element().str.json_path_match("$").str.strip_chars())
            .when(pl.element().str.starts_with("["))
            .then(
                pl.element()
                .str.json_decode(pl.List(pl.String))
                .list.eval(pl.element().str.strip_chars())
                .list.eval(pl.element().filter(pl.element().is_not_null() & (pl.element() != "")))
                .list.first()
            )
            .otherwise(None)
        )
        .list.eval(pl.element().filter(pl.element().is_not_null() & (pl.element() != "")))
    )
    return decoded


def _object_first_value(raw: pl.Expr) -> pl.Expr:
    """First value text of a string-valued object in document order.

    Mirrors ``_url_from_links``: the fallback is the first published value
    verbatim (even when empty), not the first non-empty one. The guard
    restricts those values to strings.
    """

    full = raw.str.extract_all(_OBJECT_VALUE_PATTERN)
    frag = full.list.eval(pl.element().str.extract(_OBJECT_VALUE_PATTERN, 1)).list.first()
    return (
        pl.when(frag.is_null())
        .then(None)
        .when(frag.str.starts_with('"'))
        .then(frag.str.json_path_match("$").str.strip_chars())
        .otherwise(None)
    )


def _scalar_or_list_text(raw: pl.Expr, is_string: pl.Expr) -> pl.Expr:
    """``_first_text`` of a value known to be a scalar or a flat string array."""

    return (
        pl.when(is_string)
        .then(_leaf_text(raw, pl.lit(True)))
        .when(raw.str.starts_with("["))
        .then(_list_first_nonempty(raw))
        .otherwise(_leaf_text(raw, is_string))
    )


def _first_text(raw: pl.Expr, is_string: pl.Expr) -> pl.Expr:
    """Bounded native equivalent of ``normalize._first_text`` on a subtree.

    Objects prefer the ``spa`` member when present (its value may be a
    string or a flat string list; an empty-but-present ``spa`` blocks
    fallback, exactly like the reference ``is not None`` check) and
    otherwise return the first non-empty value text in document order.
    Strings (including ``"0"``/``"false"``/``"[]"``/``"{}"``) always resolve
    to their decoded text; containers recurse. The guard rejects every
    shape outside this domain.
    """

    spa = raw.str.json_path_match("$.spa")
    spa_present = spa.is_not_null()
    spa_is_string = _is_json_string(raw, "spa")
    spa_text = _scalar_or_list_text(spa, spa_is_string)
    # ``spa`` present (even empty) blocks fallback; only missing/JSON-null
    # ``spa`` falls through to document-order values.
    fallback = _object_string_values(raw).list.first()
    object_text = pl.when(spa_present).then(spa_text).otherwise(fallback)
    return (
        pl.when(raw.is_null())
        .then(None)
        .when(is_string)
        .then(raw.str.strip_chars())
        .when(raw.str.starts_with("{"))
        .then(object_text)
        .when(raw.str.starts_with("["))
        .then(_list_first_nonempty(raw))
        .otherwise(raw.str.strip_chars())
    )


def _dedup_texts(elements: pl.Expr) -> pl.Expr:
    return (
        elements.fill_null([])
        .list.eval(pl.element().str.strip_chars())
        .list.eval(pl.element().filter(pl.element().is_not_null() & (pl.element() != "")))
        .list.unique(maintain_order=True)
    )


def _single_published(pairs: list[tuple[pl.Expr, pl.Expr]]) -> pl.Expr:
    """Native ``_single_published_value`` across scalar identifier keys.

    The guard restricts these keys to strings or null, so each key
    contributes at most one text and the reference reduces to "the single
    distinct non-empty text across the keys, in key order".
    """

    collected = pl.concat_list([_first_text(raw, tag) for raw, tag in pairs])
    unique = collected.list.eval(
        pl.element().filter(pl.element().is_not_null() & (pl.element() != ""))
    ).list.unique(maintain_order=True)
    return pl.when(unique.list.len() == 1).then(unique.list.first()).otherwise(None)


def _code_list(raw: pl.Expr, is_string: pl.Expr) -> pl.Expr:
    """Native ``_as_code_list``: strip, drop empties, dedup keeping order.

    String scalars are single codes (even numeric-looking ones like
    ``"72415000"``); eligible flat string arrays decode element-wise.
    """

    array_path = pl.when(is_string).then(pl.lit(None, dtype=pl.List(pl.String))).otherwise(_array_texts(raw))
    single = _first_text(raw, is_string)
    as_scalar = (
        pl.when(single.is_not_null() & (single != ""))
        .then(pl.concat_list([single]))
        .otherwise(pl.lit(None, dtype=pl.List(pl.String)))
    )
    return _dedup_texts(pl.coalesce(array_path, as_scalar))


# ---------------------------------------------------------------------------
# Temporal helpers
# ---------------------------------------------------------------------------


def _aware_instant(text: pl.Expr) -> pl.Expr:
    """Strict RFC 3339 instant to a UTC datetime or null.

    The eligibility guard admits only
    ``YYYY-MM-DD[T ]HH:MM:SS[.f{1,9}](Z|±HH:MM)`` with in-range components,
    so one explicit chrono format parses every admitted row; the separator
    is normalized to ``T`` and a trailing ``Z`` to an explicit offset,
    mirroring the reference ``text.replace("Z", "+00:00")`` for this
    domain. Everything else resolves to null (and is routed to the
    reference by the guard before execution).
    """

    guarded = text.is_not_null() & text.str.contains(
        r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$"
    )
    normalized = (
        pl.when(guarded)
        .then(
            (text.str.slice(0, 10) + pl.lit("T") + text.str.slice(11)).str.replace(
                r"Z$", "+00:00"
            )
        )
        .otherwise(pl.lit("1970-01-01T00:00:00+00:00"))
    )
    parsed = normalized.str.to_datetime(
        time_unit="us", format="%Y-%m-%dT%H:%M:%S%.f%:z", strict=False, time_zone="UTC"
    )
    return pl.when(guarded).then(parsed).otherwise(None)


def _iso_marker(instant: pl.Expr) -> pl.Expr:
    """UTC ISO-8601 marker identical to Python ``datetime.isoformat()``.

    Microsecond components render as exactly six digits (``.123000``,
    ``.001000``) and an exact second renders no fraction at all, matching
    ``datetime.isoformat()``; ``%f`` alone would drop trailing zeros.
    """

    seconds = instant.dt.to_string("%Y-%m-%dT%H:%M:%S")
    fraction = instant.dt.to_string("%.6f")
    return (
        pl.when(instant.dt.microsecond() != 0)
        .then(seconds + fraction)
        .otherwise(seconds)
        + "+00:00"
    )


def _parse_date(text: pl.Expr) -> pl.Expr:
    """Native ``_parse_date``: first 10 chars as zero-padded ISO date or null.

    The guard admits only strings whose first ten stripped characters are
    ``YYYY-MM-DD``; validity (month/day ranges, leap years) is decided by
    the same calendar rules in chrono and CPython, so invalid dates resolve
    to null in both.
    """

    return pl.when(text.is_not_null() & (text != "")).then(
        text.str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    ).otherwise(None)


# ---------------------------------------------------------------------------
# Collision diagnostic (the only failure path; bounded)
# ---------------------------------------------------------------------------


def _raise_collisions(combined: pl.DataFrame, records_height: int) -> None:
    """Raise the reference's first material conflict, field set included.

    The reference compares each event's observations, in global input order
    (records in input order, then tombstones), against the **first**
    observation and raises at the first differing one, naming only the
    fields that differ between that specific pair. The first-conflict pair
    is reconstructed natively here and only those two rows are fetched, so
    the diagnostic stays bounded and byte-identical.
    """

    ordered = combined.with_columns(
        __order=pl.coalesce(
            pl.col("__rec_order"),
            pl.lit(records_height, dtype=pl.UInt32) + pl.col("__tomb_order"),
        )
    )
    grouped = ordered.group_by("event_id").agg(pl.struct(_COLLISION_FIELDS).n_unique().alias("variants"))
    colliding = grouped.filter(pl.col("variants") > 1)
    if colliding.height == 0:
        return
    offenders = ordered.join(colliding.select("event_id"), on="event_id", how="semi")
    canonical = offenders.group_by("event_id").agg(
        [
            pl.col(field).sort_by("__order").first().alias(f"__canon_{field}")
            for field in _COLLISION_FIELDS
        ]
    )
    joined = offenders.join(canonical, on="event_id", how="left")
    differs = pl.any_horizontal(
        [pl.col(field).ne_missing(pl.col(f"__canon_{field}")) for field in _COLLISION_FIELDS]
    )
    first_diff = (
        joined.filter(differs)
        .group_by("event_id")
        .agg(pl.col("__order").min().alias("__first_diff_order"))
        .sort("event_id")
        .head(1)
    )
    if first_diff.height == 0:
        # Unreachable: ``n_unique > 1`` implies at least one row differs.
        return
    event_id = first_diff["event_id"][0]
    canonical_row = canonical.filter(pl.col("event_id") == event_id).row(0, named=True)
    culprit = (
        joined.filter(
            (pl.col("event_id") == event_id) & (pl.col("__order") == first_diff["__first_diff_order"][0])
        )
        .select(_COLLISION_FIELDS)
        .head(1)
        .to_dicts()[0]
    )
    differing = sorted(
        field
        for field in _COLLISION_FIELDS
        if culprit[field] != canonical_row[f"__canon_{field}"]
    )
    raise ValueError(
        f"Conflicting canonical mappings for event_id {event_id!r}; "
        f"differing fields: {', '.join(differing)}"
    )


# ---------------------------------------------------------------------------
# Source branches (lazy: they compose one plan per source)
# ---------------------------------------------------------------------------


def _order_columns(*, records: bool) -> list[pl.Expr]:
    """Carry the input positions used by collision diagnostics and ordering."""

    if records:
        return [
            pl.col("__rec_order").alias("__rec_order"),
            pl.lit(None, dtype=pl.UInt32).alias("__tomb_order"),
        ]
    return [
        pl.lit(None, dtype=pl.UInt32).alias("__rec_order"),
        pl.col("__tomb_order").alias("__tomb_order"),
    ]


def _ted_estimated() -> pl.Expr:
    """Exact ``decimal(20,2)`` amount for the guarded TED amount domain."""

    parsed = pl.col("__amount_text").cast(pl.Float64, strict=False)
    return (
        pl.when(parsed.is_not_null() & (parsed >= 0))
        .then(pl.col("__amount_text"))
        .otherwise(None)
        .str.to_decimal(scale=2)
        .cast(pl.Decimal(20, 2))
    )


def _ted_branch(ted: pl.LazyFrame) -> pl.LazyFrame:
    payload = pl.col("payload_json")
    # Stage 1: every payload field is resolved ONCE as a raw JSON subtree
    # column plus its quoted-string tag; downstream expressions reference
    # these columns, keeping the plan linear instead of re-traversing the
    # payload per derivation.
    raws = ted.with_columns(
        __nd=_json(payload, "ND"),
        __nd_str=_is_json_string(payload, "ND"),
        __notice_id_raw=_json(payload, "notice_id"),
        __notice_id_str=_is_json_string(payload, "notice_id"),
        __ti=_json(payload, "TI"),
        __ti_str=_is_json_string(payload, "TI"),
        __title_raw=_json(payload, "title"),
        __title_str=_is_json_string(payload, "title"),
        __description_glo=_json(payload, "description-glo"),
        __description_glo_str=_is_json_string(payload, "description-glo"),
        __description_raw=_json(payload, "description"),
        __description_str=_is_json_string(payload, "description"),
        __buyer_name_raw=_json(payload, "buyer-name"),
        __buyer_name_str=_is_json_string(payload, "buyer-name"),
        __buyer_raw=_json(payload, "buyer"),
        __buyer_str=_is_json_string(payload, "buyer"),
        __pc=_json(payload, "PC"),
        __pc_str=_is_json_string(payload, "PC"),
        __cpv_raw=_json(payload, "cpv"),
        __cpv_str=_is_json_string(payload, "cpv"),
        __estimated_lot=_json(payload, "estimated-value-lot"),
        __estimated_lot_str=_is_json_string(payload, "estimated-value-lot"),
        __framework_max=_json(payload, "framework-maximum-value-lot"),
        __framework_max_str=_is_json_string(payload, "framework-maximum-value-lot"),
        __amount_raw=_json(payload, "amount"),
        __amount_str=_is_json_string(payload, "amount"),
        __currency=_json(payload, "currency"),
        __currency_str=_is_json_string(payload, "currency"),
        __procedure_id=_json(payload, "procedure-identifier"),
        __procedure_id_str=_is_json_string(payload, "procedure-identifier"),
        __bt04=_json(payload, "BT-04-notice"),
        __bt04_str=_is_json_string(payload, "BT-04-notice"),
        __buyer_identifier=_json(payload, "buyer-identifier"),
        __buyer_identifier_str=_is_json_string(payload, "buyer-identifier"),
        __bi=_json(payload, "BI"),
        __bi_str=_is_json_string(payload, "BI"),
        __notice_type=_json(payload, "notice-type"),
        __notice_type_str=_is_json_string(payload, "notice-type"),
        __cy=_json(payload, "CY"),
        __cy_str=_is_json_string(payload, "CY"),
        __country=_json(payload, "country"),
        __country_str=_is_json_string(payload, "country"),
        __pd=_json(payload, "PD"),
        __pd_str=_is_json_string(payload, "PD"),
        __publication_date=_json(payload, "publication_date"),
        __publication_date_str=_is_json_string(payload, "publication_date"),
        __publication_date_hyphen=_json(payload, "publication-date"),
        __publication_date_hyphen_str=_is_json_string(payload, "publication-date"),
        __url=_json(payload, "url"),
        __url_str=_is_json_string(payload, "url"),
        __html=_json(payload, "links", "html"),
    )
    notice_raw, notice_str = _or_typed(
        [(pl.col("__nd"), pl.col("__nd_str")), (pl.col("__notice_id_raw"), pl.col("__notice_id_str"))]
    )
    notice_id = _first_text(notice_raw, notice_str)
    # TED amount selection is presence-based (first non-None field), exactly
    # like the reference ``next(... if payload.get(key) is not None)``: a
    # JSON null never selects its field. ``json_path_match`` yields null for
    # both missing and JSON null, so presence == decoded non-null here.
    selected = pl.coalesce(pl.col("__estimated_lot"), pl.col("__framework_max"), pl.col("__amount_raw"))
    selected_str = (
        pl.when(pl.col("__estimated_lot").is_not_null())
        .then(pl.col("__estimated_lot_str"))
        .when(pl.col("__framework_max").is_not_null())
        .then(pl.col("__framework_max_str"))
        .otherwise(pl.col("__amount_str"))
    )
    base = raws.with_columns(
        __notice_id=notice_id,
        __amount_text=_first_text(selected, selected_str),
    )
    base = base.with_columns(__estimated=_ted_estimated())
    estimated = pl.col("__estimated")
    procedure = _single_published(
        [(pl.col("__procedure_id"), pl.col("__procedure_id_str")), (pl.col("__bt04"), pl.col("__bt04_str"))]
    )
    buyer_id = _single_published(
        [
            (pl.col("__buyer_identifier"), pl.col("__buyer_identifier_str")),
            (pl.col("__bi"), pl.col("__bi_str")),
        ]
    )
    notice_type = _first_text(pl.col("__notice_type"), pl.col("__notice_type_str"))
    cy_raw, cy_str = _or_typed(
        [(pl.col("__cy"), pl.col("__cy_str")), (pl.col("__country"), pl.col("__country_str"))]
    )
    country_text = _first_text(cy_raw, cy_str)
    ti_raw, ti_str = _or_typed(
        [(pl.col("__ti"), pl.col("__ti_str")), (pl.col("__title_raw"), pl.col("__title_str"))]
    )
    desc_raw, desc_str = _or_typed(
        [
            (pl.col("__description_glo"), pl.col("__description_glo_str")),
            (pl.col("__description_raw"), pl.col("__description_str")),
        ]
    )
    buyer_raw, buyer_str = _or_typed(
        [(pl.col("__buyer_name_raw"), pl.col("__buyer_name_str")), (pl.col("__buyer_raw"), pl.col("__buyer_str"))]
    )
    pc_raw, pc_str = _or_typed(
        [(pl.col("__pc"), pl.col("__pc_str")), (pl.col("__cpv_raw"), pl.col("__cpv_str"))]
    )
    pd_raw, pd_str = _or_typed(
        [
            (pl.col("__pd"), pl.col("__pd_str")),
            (pl.col("__publication_date"), pl.col("__publication_date_str")),
            (pl.col("__publication_date_hyphen"), pl.col("__publication_date_hyphen_str")),
        ]
    )
    url = _or_text(
        _first_text(pl.col("__url"), pl.col("__url_str")),
        _leaf_text(pl.col("__html").str.json_path_match("$.ENG"), pl.lit(False)),
        _leaf_text(pl.col("__html").str.json_path_match("$.SPA"), pl.lit(False)),
        _leaf_text(pl.col("__html").str.json_path_match("$.DEU"), pl.lit(False)),
        _leaf_text(pl.col("__html").str.json_path_match("$.FRA"), pl.lit(False)),
        _object_first_value(pl.col("__html")),
    )
    return base.select(
        *_BRANCH_PROVENANCE,
        *_order_columns(records=True),
        event_id=(pl.lit("ted:notice:") + pl.col("__notice_id")),
        procedure_id=pl.when(procedure.is_not_null() & (procedure != "")).then(
            (pl.lit("ted:procedure:") + procedure)
        ).otherwise(None),
        source=pl.lit("ted"),
        source_event_type=pl.when(notice_type.is_not_null() & (notice_type != "")).then(
            (pl.lit("notice:") + notice_type)
        ).otherwise(pl.lit("notice")),
        buyer_id=buyer_id,
        buyer_name=_or_text(_first_text(buyer_raw, buyer_str)),
        title=_or_text(_first_text(ti_raw, ti_str)),
        description=_or_text(_first_text(desc_raw, desc_str)),
        cpv_codes=_code_list(pc_raw, pc_str),
        estimated_value=estimated,
        awarded_value=pl.lit(None, dtype=pl.Decimal(20, 2)),
        currency=pl.when(estimated.is_not_null()).then(
            _or_text(_first_text(pl.col("__currency"), pl.col("__currency_str")), "EUR")
        ).otherwise(None),
        publication_date=_parse_date(_first_text(pd_raw, pd_str)),
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
            .then(
                # ``str.isupper`` semantics (two cased-uppercase letters,
                # Unicode-aware) mirroring ``_ted_country``.
                pl.when(country_text.str.contains(r"^\p{Lu}{2}$")).then(country_text).otherwise(None)
            )
            .otherwise(None)
        ),
        source_url=url,
        ingested_at=pl.col("raw_retrieved_at"),
    )


def _placsp_branch(placsp: pl.LazyFrame) -> pl.LazyFrame:
    payload = pl.col("payload_json")
    raws = placsp.with_columns(
        __atom_id_raw=_json(payload, "atom_id"),
        __atom_id_str=_is_json_string(payload, "atom_id"),
        __updated_raw=_json(payload, "updated"),
        __updated_str=_is_json_string(payload, "updated"),
        __buyer_dir3=_json(payload, "buyer_dir3"),
        __buyer_dir3_str=_is_json_string(payload, "buyer_dir3"),
        __buyer=_json(payload, "buyer"),
        __buyer_str=_is_json_string(payload, "buyer"),
        __p_title=_json(payload, "title"),
        __p_title_str=_is_json_string(payload, "title"),
        __summary=_json(payload, "summary"),
        __summary_str=_is_json_string(payload, "summary"),
        __cpv=_json(payload, "cpv"),
        __cpv_str=_is_json_string(payload, "cpv"),
        __status_code=_json(payload, "status_code"),
        __status_code_str=_is_json_string(payload, "status_code"),
        __nuts_code=_json(payload, "nuts_code"),
        __nuts_code_str=_is_json_string(payload, "nuts_code"),
        __p_url=_json(payload, "url"),
        __p_url_str=_is_json_string(payload, "url"),
        __overall_value=_json(payload, "amount_estimated_overall"),
        __overall_value_str=_is_json_string(payload, "amount_estimated_overall"),
        __tax_value=_json(payload, "amount_tax_exclusive"),
        __tax_value_str=_is_json_string(payload, "amount_tax_exclusive"),
        __overall_raw=_json(payload, "amount_estimated_overall_raw"),
        __overall_raw_str=_is_json_string(payload, "amount_estimated_overall_raw"),
        __tax_raw=_json(payload, "amount_tax_exclusive_raw"),
        __tax_raw_str=_is_json_string(payload, "amount_tax_exclusive_raw"),
        __overall_currency=_json(payload, "amount_estimated_overall_currency"),
        __overall_currency_str=_is_json_string(payload, "amount_estimated_overall_currency"),
        __tax_currency=_json(payload, "amount_tax_exclusive_currency"),
        __tax_currency_str=_is_json_string(payload, "amount_tax_exclusive_currency"),
    )
    atom_id = _first_text(pl.col("__atom_id_raw"), pl.col("__atom_id_str"))
    updated = _aware_instant(_first_text(pl.col("__updated_raw"), pl.col("__updated_str")))
    marker = pl.when(updated.is_not_null()).then(_iso_marker(updated)).otherwise(pl.lit("undated"))
    # Key presence (explicit JSON null included) selects the amount field,
    # mirroring ``key in payload`` in the reference. The textual probe is
    # exact under the guard (canonical JSON, no nested probed keys) and the
    # pattern cannot match the ``*_raw``/``*_currency`` suffixes because the
    # closing quote must follow the key.
    overall_present = payload.str.contains(r'"amount_estimated_overall"\s*:').fill_null(False)
    tax_present = payload.str.contains(r'"amount_tax_exclusive"\s*:').fill_null(False)
    base = raws.with_columns(
        __atom_id=atom_id,
        __updated=updated,
        __marker=marker,
        __overall_present=overall_present,
        __tax_present=tax_present,
    )
    base = base.with_columns(
        __overall_text=_first_text(pl.col("__overall_raw"), pl.col("__overall_raw_str")),
        __tax_text=_first_text(pl.col("__tax_raw"), pl.col("__tax_raw_str")),
        # Legacy payloads without retained raw text fall back to the Python
        # ``str()`` of the value; the guard admits only values whose ``str``
        # equals the canonical JSON text (plain decimals).
        __overall_legacy=_first_text(pl.col("__overall_value"), pl.col("__overall_value_str")),
        __tax_legacy=_first_text(pl.col("__tax_value"), pl.col("__tax_value_str")),
    )
    base = base.with_columns(
        __amount_text=pl.when(pl.col("__overall_present"))
        .then(pl.coalesce(pl.col("__overall_text"), pl.col("__overall_legacy")))
        .otherwise(pl.coalesce(pl.col("__tax_text"), pl.col("__tax_legacy"))),
        # Presence plus JSON-null check: ``payload[key] is not None``. Only
        # an explicit JSON null stays null without failing.
        __amount_gate_ok=pl.when(pl.col("__overall_present"))
        .then(pl.col("__overall_value").is_not_null())
        .otherwise(pl.col("__tax_value").is_not_null()),
    )
    base = base.with_columns(
        __estimated=pl.when(pl.col("__amount_gate_ok"))
        .then(pl.col("__amount_text"))
        .otherwise(None)
        .str.to_decimal(scale=2)
        .cast(pl.Decimal(20, 2)),
    )
    estimated = pl.col("__estimated")
    currency_text = pl.when(pl.col("__overall_present")).then(
        _first_text(pl.col("__overall_currency"), pl.col("__overall_currency_str"))
    ).otherwise(_first_text(pl.col("__tax_currency"), pl.col("__tax_currency_str")))
    return base.select(
        *_BRANCH_PROVENANCE,
        *_order_columns(records=True),
        event_id=(pl.lit("placsp:notice:") + pl.col("__atom_id") + pl.lit("@") + pl.col("__marker")),
        procedure_id=(pl.lit("placsp:procedure:") + pl.col("__atom_id")),
        source=pl.lit("placsp"),
        source_event_type=pl.lit("notice_snapshot"),
        buyer_id=_or_text(_first_text(pl.col("__buyer_dir3"), pl.col("__buyer_dir3_str"))),
        buyer_name=_or_text(_first_text(pl.col("__buyer"), pl.col("__buyer_str"))),
        title=_or_text(_first_text(pl.col("__p_title"), pl.col("__p_title_str"))),
        description=_or_text(_first_text(pl.col("__summary"), pl.col("__summary_str"))),
        cpv_codes=_code_list(pl.col("__cpv"), pl.col("__cpv_str")),
        estimated_value=estimated,
        awarded_value=pl.lit(None, dtype=pl.Decimal(20, 2)),
        currency=pl.when(estimated.is_not_null()).then(_or_text(currency_text)).otherwise(None),
        publication_date=pl.lit(None, dtype=pl.Date),
        source_updated_at=pl.col("__updated"),
        deadline=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
        status=_or_text(_first_text(pl.col("__status_code"), pl.col("__status_code_str"))),
        nuts_code=_or_text(_first_text(pl.col("__nuts_code"), pl.col("__nuts_code_str"))),
        country=pl.lit("ES"),
        source_url=_or_text(_first_text(pl.col("__p_url"), pl.col("__p_url_str"))),
        ingested_at=pl.col("raw_retrieved_at"),
    )


def _boe_branch(boe: pl.LazyFrame) -> pl.LazyFrame:
    payload = pl.col("payload_json")
    raws = boe.with_columns(
        __item_id_raw=_json(payload, "item_id"),
        __item_id_str=_is_json_string(payload, "item_id"),
        __identificador=_json(payload, "identificador"),
        __identificador_str=_is_json_string(payload, "identificador"),
        __b_title=_json(payload, "title"),
        __b_title_str=_is_json_string(payload, "title"),
        __titulo=_json(payload, "titulo"),
        __titulo_str=_is_json_string(payload, "titulo"),
        __b_buyer=_json(payload, "buyer"),
        __b_buyer_str=_is_json_string(payload, "buyer"),
        __department=_json(payload, "department"),
        __department_str=_is_json_string(payload, "department"),
        __b_summary=_json(payload, "summary"),
        __b_summary_str=_is_json_string(payload, "summary"),
        __b_publication_date=_json(payload, "publication_date"),
        __b_publication_date_str=_is_json_string(payload, "publication_date"),
        __b_url=_json(payload, "url"),
        __b_url_str=_is_json_string(payload, "url"),
    )
    item_raw, item_str = _or_typed(
        [(pl.col("__item_id_raw"), pl.col("__item_id_str")), (pl.col("__identificador"), pl.col("__identificador_str"))]
    )
    item_id = _first_text(item_raw, item_str)
    title_raw, title_str = _or_typed(
        [(pl.col("__b_title"), pl.col("__b_title_str")), (pl.col("__titulo"), pl.col("__titulo_str"))]
    )
    buyer_raw, buyer_str = _or_typed(
        [(pl.col("__b_buyer"), pl.col("__b_buyer_str")), (pl.col("__department"), pl.col("__department_str"))]
    )
    base = raws.with_columns(__item_id=item_id)
    return base.select(
        *_BRANCH_PROVENANCE,
        *_order_columns(records=True),
        event_id=(pl.lit("boe:notice:") + pl.col("__item_id")),
        procedure_id=pl.lit(None, dtype=pl.String),
        source=pl.lit("boe"),
        source_event_type=pl.lit("notice"),
        buyer_id=pl.lit(None, dtype=pl.String),
        buyer_name=_or_text(_first_text(buyer_raw, buyer_str)),
        title=_or_text(_first_text(title_raw, title_str)),
        description=_or_text(_first_text(pl.col("__b_summary"), pl.col("__b_summary_str"))),
        cpv_codes=pl.lit([], dtype=pl.List(pl.String)),
        estimated_value=pl.lit(None, dtype=pl.Decimal(20, 2)),
        awarded_value=pl.lit(None, dtype=pl.Decimal(20, 2)),
        currency=pl.lit(None, dtype=pl.String),
        publication_date=_parse_date(_first_text(pl.col("__b_publication_date"), pl.col("__b_publication_date_str"))),
        source_updated_at=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
        deadline=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
        status=pl.lit(None, dtype=pl.String),
        nuts_code=pl.lit(None, dtype=pl.String),
        country=pl.lit("ES"),
        source_url=_or_text(_first_text(pl.col("__b_url"), pl.col("__b_url_str"))),
        ingested_at=pl.col("raw_retrieved_at"),
    )


def _tombstone_branch(tombstones: pl.LazyFrame) -> pl.LazyFrame:
    ref = pl.col("source_record_id").fill_null("").str.strip_chars()
    return tombstones.select(
        *_BRANCH_PROVENANCE,
        *_order_columns(records=False),
        event_id=(pl.lit("placsp:tombstone:") + ref),
        procedure_id=(pl.lit("placsp:procedure:") + ref),
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
    )


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------


def build_procurement_events_native(records: pl.DataFrame, tombstones: pl.DataFrame) -> pl.DataFrame:
    """Build the canonical Silver frame with native Polars expressions.

    The eligibility guard has already accepted the complete batch, so every
    row maps to a source branch and the only failure path left is a material
    collision. Repeated observations of one source event collapse to the
    deterministic minimum provenance tuple and the result is sorted
    ascending by ``event_id``.
    """

    records_indexed = records.with_row_index("__rec_order")
    tombstones_indexed = tombstones.with_row_index("__tomb_order")
    combined = pl.concat(
        [
            _ted_branch(records_indexed.lazy().filter(pl.col("source") == "ted")),
            _placsp_branch(records_indexed.lazy().filter(pl.col("source") == "placsp")),
            _boe_branch(records_indexed.lazy().filter(pl.col("source") == "boe")),
            _tombstone_branch(tombstones_indexed.lazy()),
        ],
        how="vertical",
    ).collect()
    if combined.height == 0:
        # The guard guarantees a branch per row, so this only happens when
        # both inputs are empty.
        return pl.DataFrame(schema=PROCUREMENT_EVENT_SCHEMA)
    _raise_collisions(combined, records.height)
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
        .agg(pl.exclude("event_id", "__key_member", "__key_locator", "__rec_order", "__tomb_order").first())
        .sort("event_id")
    )
    return selected.select(
        [pl.col(name).cast(dtype) for name, dtype in PROCUREMENT_EVENT_SCHEMA.items()]
    )
