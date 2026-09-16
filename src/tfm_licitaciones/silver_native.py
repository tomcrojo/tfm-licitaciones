"""Native Polars production engine for canonical Silver procurement events.

This module implements the same engine-neutral canonical contract as the
frozen python-row reference (:mod:`tfm_licitaciones.silver_reference`):
identity derivation, revision/tombstone semantics, explicit material
collisions, deterministic provenance selection, ``decimal(20,2)`` amounts,
UTC timestamps and the exact :data:`PROCUREMENT_EVENT_SCHEMA
<tfm_licitaciones.models.PROCUREMENT_EVENT_SCHEMA>`. Polars is the default
production engine inside the measured single-node envelope.

Genuineness: the transform uses only Polars expressions, grouping and
sorting. Each source branch resolves its payload fields through bounded
``json_path_match`` access on the canonical ``payload_json`` (one JSON
traversal per field, never a per-field re-parse of the whole payload), and
the success path performs no per-row Python (no ``iter_rows``/``to_dicts``/
``map_elements``/``apply`` and no ``ProcurementEvent`` allocation). Failure
paths fetch at most 5 offending rows (plus scalar message values) to name
them in the raised ``ValueError``. Input positions (``__rec_order`` /
``__tomb_order``) are carried so multi-error batches raise the same first
failure as the reference (records in input order, then tombstones in input
order, then collisions in sorted ``event_id`` order).

Type preservation: :meth:`str.json_path_match` decodes JSON strings (quotes
removed, escapes resolved) but renders numbers, booleans, ``null``,
arrays and objects as their JSON text, so a string ``"0"`` and a number
``0`` both arrive as ``0``. Every field whose semantics depend on Python
truthiness, presence or type therefore carries a parallel ``is_string``
probe (``container.str.contains('"key"\\s*:\\s*"')``) that records whether
the JSON value was quoted. String probes are path-aware through
``json_path_match`` for the value plus a probe on the immediate container;
nested shadowing (a nested object reusing a top-level key name) cannot be
produced by the TED/OpenPLACSP/BOE adapters (flat payloads with disjoint
top-level vs language-key namespaces, enforced by
``tests/test_silver_native.py::AdapterContractTests``) and is out of scope
for the adversarial matrix.

``_first_text`` uses only ``json_path_match`` decoding (exact, including
``\\uXXXX``, escapes and non-ASCII) plus ``str.json_decode`` for
unescaping extracted string literals; there is no hand-written JSON
unescape. Objects prefer the ``spa`` member when present (even when its
text is empty, matching the reference) and otherwise return the first
non-empty value text in document order for the reachable flat domain
(string/number/bool/array-of-strings values). Deeper nesting
(object-in-object, object-in-array) cannot be emitted by the adapters and
is covered by contract tests.

Amount semantics are asymmetric on purpose, mirroring the reference: TED
amounts pass a ``_parse_amount`` float gate (unparseable or negative ->
null; the gate itself may use Float64) and always normalize separator
style, while PLACSP amounts gate on key presence with a JSON-null check,
prefer the retained ``*_raw`` published text and fall back to the Python
``str()`` of the value. After the gate, the ``decimal(20,2)`` contract
(representability, scale, precision and decimal text) is decided lexically
from the published text with exact decimal-compatible string operations;
binary Float64 is never the authority there. Exponent notation is expanded
lexically. Underscore-containing amount texts are stripped for the Decimal
decision (matching ``Decimal`` laxity); the TED float gate still rejects
all underscore forms while the reference accepts PEP-515-valid ones --
that gate-level shape cannot be emitted by the adapters (numeric texts
without underscores, enforced by contract tests).
"""

from __future__ import annotations

import polars as pl

from .models import PROCUREMENT_EVENT_SCHEMA

IMPLEMENTATION = "polars-native"
IMPLEMENTATION_DETAIL = (
    "native Polars expressions/group_by/sort over the Polars/Parquet "
    "boundary: bounded json_path_match field resolution per source branch, "
    "type-preserving truthiness via is_string probes, exact _first_text "
    "recursion without hand unescape, lexical decimal(20,2) decisions, "
    "group_by struct collision detection, provenance-minimum selection via "
    "stable sort + group_by first; no per-row Python on the success path"
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
_ZERO_NUMBER_PATTERN = r"^[-+]?(?:0+(?:\.0*)?|\.0+)(?:[eE][-+]?\d+)?$"
# Decimal lexical pattern (after underscore stripping): optional sign,
# mantissa with optional fraction, optional exponent. Mirrors what
# ``Decimal()`` accepts for the reachable domain (plain/exponent forms,
# leading/trailing dot digits, plus sign). ``inf``/``nan`` never match and
# therefore raise not-representable, exactly like the reference quantize
# step.
_DECIMAL_PARTS_PATTERN = r"^([-+]?)((?:\d+)?)(?:\.(\d*))?(?:[eE]([-+]?\d+))?$"
# Flat JSON value fragment at object-value position (string with quotes,
# flat string array, bare literals/numbers). Nested objects/arrays as values
# are outside the reachable adapter domain.
_OBJECT_VALUE_PATTERN = (
    r':\s*("(?:[^"\\]|\\.)*"'
    r'|\[(?:\s*"(?:[^"\\]|\\.)*"\s*,?)*\s*\]'
    r'|true|false|null'
    r'|-?\d+(?:\.\d*)?(?:[eE][-+]?\d+)?'
    r'|-?\.\d+(?:[eE][-+]?\d+)?)'
)


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

    ``container`` is either the full ``payload_json`` (top-level fields) or
    an object-subtree text from ``json_path_match`` (nested fields such as
    ``spa`` inside ``TI``). The probe looks for an opening quote as the
    first non-space character of the value. Nested shadowing with the same
    key name cannot be emitted by the production adapters (flat payloads,
    disjoint namespaces; see ``AdapterContractTests``).
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

    ``raw`` is already exactly decoded by ``json_path_match`` (escapes,
    ``\\uXXXX`` and non-ASCII resolved by the JSON engine). Strings keep
    their text verbatim (including ``"null"``/``"[]"``/``"{}"`` and values
    with surrounding quotes); non-string ``true``/``false`` map to Python
    ``"True"``/``"False"``; numbers keep their JSON text (identical to
    Python ``str()`` for int/float); containers (non-string ``[...]`` /
    ``{...}``) reduce to null so callers route them to list/object logic.
    """

    stripped = raw.str.strip_chars()
    return (
        pl.when(raw.is_null())
        .then(None)
        .when(is_string)
        .then(stripped)
        .when((raw == "true"))
        .then(pl.lit("True"))
        .when((raw == "false"))
        .then(pl.lit("False"))
        .when(raw.str.starts_with("[") | raw.str.starts_with("{"))
        .then(None)
        .otherwise(stripped)
    )


def _array_texts(raw: pl.Expr) -> pl.Expr:
    """Decoded string elements of a JSON array text (or null).

    ``str.json_decode(List(String))`` coerces numbers to their text
    (identical to Python ``str()``) and JSON-null elements to null (later
    filtered like the reference ``""``). Boolean elements would render
    lowercase (vs Python ``True``/``False``) and nested objects/arrays
    would raise; neither shape can be emitted by the production adapters
    (flat string/number lists, enforced by ``AdapterContractTests``).
    """

    return pl.when(raw.str.starts_with("[")).then(raw.str.json_decode(pl.List(pl.String))).otherwise(None)


def _list_first_nonempty(raw: pl.Expr) -> pl.Expr:
    """First non-empty stripped element of an array text."""

    return (
        _array_texts(raw)
        .list.eval(pl.element().str.strip_chars())
        .list.eval(pl.element().filter(pl.element().is_not_null() & (pl.element() != "")))
        .list.first()
    )


def _object_string_values(raw: pl.Expr) -> pl.Expr:
    """First-text candidates of an object text in document order (exact).

    Extracts flat value fragments at value position in document order (the
    ``extract_all`` full matches include the leading colon; the inner
    ``extract`` pulls group 1 so quotes are preserved for strings and type
    information survives), decodes each exactly (strings and array first
    elements via the throw-safe ``json_path_match`` so escapes/``\\uXXXX``
    resolve without hand-written unescape; bare ``true``/``false`` map
    to Python case; numbers keep text; ``null`` becomes empty) and returns
    the list of stripped non-empty candidates. Nested objects/arrays as
    values are outside the reachable adapter domain (flat localized dicts).
    """

    full = raw.str.extract_all(_OBJECT_VALUE_PATTERN)
    frags = full.list.eval(pl.element().str.extract(_OBJECT_VALUE_PATTERN, 1))
    # NB: ``list.eval`` evaluates every branch eagerly, so string decoding
    # must use the throw-safe ``json_path_match`` (``$`` decodes a JSON
    # string fragment exactly, ``$[0]`` an array first element) rather than
    # ``json_decode``, which would raise on non-matching elements.
    decoded = (
        frags.list.eval(
            pl.when(pl.element().str.starts_with('"'))
            .then(pl.element().str.json_path_match("$").str.strip_chars())
            .when(pl.element().str.starts_with("["))
            .then(pl.element().str.json_path_match("$[0]").str.strip_chars())
            .when(pl.element() == "true")
            .then(pl.lit("True"))
            .when(pl.element() == "false")
            .then(pl.lit("False"))
            .when(pl.element() == "null")
            .then(pl.lit(""))
            .otherwise(pl.element().str.strip_chars())
        )
        .list.eval(pl.element().filter(pl.element().is_not_null() & (pl.element() != "")))
    )
    return decoded


def _object_first_value(raw: pl.Expr) -> pl.Expr:
    """First value text of an object in document order, empties included.

    Mirrors ``_url_from_links``: the fallback is the first published value
    verbatim (even when empty), not the first non-empty one. Decoded
    exactly like ``_object_string_values`` (throw-safe ``json_path_match``).
    """

    full = raw.str.extract_all(_OBJECT_VALUE_PATTERN)
    frag = full.list.eval(pl.element().str.extract(_OBJECT_VALUE_PATTERN, 1)).list.first()
    return (
        pl.when(frag.is_null())
        .then(None)
        .when(frag.str.starts_with('"'))
        .then(frag.str.json_path_match("$").str.strip_chars())
        .when(frag.str.starts_with("["))
        .then(frag.str.json_path_match("$[0]").str.strip_chars())
        .when(frag == "true")
        .then(pl.lit("True"))
        .when(frag == "false")
        .then(pl.lit("False"))
        .when(frag == "null")
        .then(pl.lit(""))
        .otherwise(frag.str.strip_chars())
    )


def _scalar_or_list_text(raw: pl.Expr, is_string: pl.Expr) -> pl.Expr:
    """``_first_text`` of a value known to be a scalar or an array."""

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
    scalar or an array; an empty-but-present ``spa`` blocks fallback,
    exactly like the reference ``is not None`` check) and otherwise return
    the first non-empty value text in document order. Strings (including
    ``"0"``/``"false"``/``"[]"``/``"{}"``) always resolve to their decoded
    text; non-string booleans map to Python case; containers recurse.
    """

    spa = raw.str.json_path_match("$.spa")
    spa_present = spa.is_not_null()
    spa_is_string = raw.str.contains(r'"spa"\s*:\s*"').fill_null(False)
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
        .when((raw == "true"))
        .then(pl.lit("True"))
        .when((raw == "false"))
        .then(pl.lit("False"))
        .otherwise(raw.str.strip_chars())
    )


def _elements(raw: pl.Expr, is_string: pl.Expr) -> pl.Expr:
    """Flatten one identifier value to its string elements (or null).

    Scalars become single-element lists via exact ``_first_text``; arrays
    decode to their elements; objects reduce to their bounded first text.
    """

    single = _first_text(raw, is_string)
    return (
        pl.when(raw.is_null())
        .then(None)
        .when(is_string)
        .then(
            pl.when(single.is_not_null() & (single != ""))
            .then(pl.concat_list([single]))
            .otherwise(None)
        )
        .when(raw.str.starts_with("["))
        .then(_array_texts(raw))
        .when(raw.str.starts_with("{"))
        .then(
            pl.when(single.is_not_null() & (single != "")).then(pl.concat_list([single])).otherwise(None)
        )
        .otherwise(pl.concat_list([_leaf_text(raw, is_string)]))
    )


def _dedup_texts(elements: pl.Expr) -> pl.Expr:
    return (
        elements.fill_null([])
        .list.eval(pl.element().str.strip_chars())
        .list.eval(pl.element().filter(pl.element().is_not_null() & (pl.element() != "")))
        .list.unique(maintain_order=True)
    )


def _single_published(pairs: list[tuple[pl.Expr, pl.Expr]]) -> pl.Expr:
    """Native ``_single_published_value`` across identifier keys."""

    collected: pl.Expr | None = None
    for raw, is_string in pairs:
        parts = _elements(raw, is_string).fill_null([])
        collected = parts if collected is None else pl.concat_list([collected, parts])
    assert collected is not None
    unique = _dedup_texts(collected)
    return pl.when(unique.list.len() == 1).then(unique.list.first()).otherwise(None)


def _code_list(raw: pl.Expr, is_string: pl.Expr) -> pl.Expr:
    """Native ``_as_code_list``: strip, drop empties, dedup keeping order."""

    single = _first_text(raw, is_string)
    as_scalar = pl.when(single.is_not_null() & (single != "")).then(pl.concat_list([single])).otherwise(None)
    # String scalars (even numeric-looking ones like ``"72415000"`` or
    # container-looking ones like ``"[]"``) are single codes, never decoded
    # as JSON arrays; only non-string scalars may wrap, and non-string
    # arrays decode element-wise.
    wrappable = raw.str.contains(r'^(?:"(?:[^"\\]|\\.)*"|[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)$').fill_null(False)
    wrapped = pl.when(raw.is_null() | is_string | ~wrappable).then(None).otherwise("[" + raw + "]")
    array_path = pl.when(is_string).then(None).otherwise(_array_texts(raw))
    return _dedup_texts(pl.coalesce(_array_texts(wrapped), array_path, as_scalar))


# ---------------------------------------------------------------------------
# Amount machinery (lexical decimal(20,2) after the reference gate)
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
    """Native TED ``_parse_amount`` gate: non-negative float or null.

    The gate itself may use Float64 (the frozen reference gates on
    ``float()``); every Decimal decision after it is lexical.
    """

    parsed = norm.cast(pl.Float64, strict=False)
    return pl.when(parsed.is_not_null() & (parsed >= 0)).then(parsed).otherwise(None)


def _attach_amount(
    frame: pl.DataFrame,
    *,
    event_id: pl.Expr,
    norm: pl.Expr,
    fallback: pl.Expr,
    gate_ok: pl.Expr,
) -> pl.DataFrame:
    """Attach the shared lexical amount columns for one branch.

    ``norm`` is the normalized published text (null when it carried
    nothing), ``fallback`` the Python-``str()`` of the value for legacy
    PLACSP payloads without retained raw text, ``gate_ok`` whether the
    reference gate passes. All ``decimal(20,2)`` decisions below are
    lexical; no Float64 participates.
    """

    staged = frame.with_columns(
        __amount_event_id=event_id,
        __amount_norm=norm,
        __amount_fallback=fallback,
        __amount_gate_ok=gate_ok,
    ).with_columns(
        __amount_rep=pl.coalesce(
            pl.when(pl.col("__amount_norm").is_not_null() & (pl.col("__amount_norm") != "")).then(
                pl.col("__amount_norm")
            ).otherwise(None),
            pl.col("__amount_fallback"),
        ),
    ).with_columns(
        # ``Decimal`` accepts underscores laxly and ignores surrounding
        # whitespace; strip both for parsing so the lexical decision
        # matches (``__amount_rep`` keeps the original text for the exact
        # reference error messages).
        __amount_clean=pl.col("__amount_rep").str.replace_all("_", "", literal=True).str.strip_chars(),
    ).with_columns(
        __dec_sign=pl.col("__amount_clean").str.extract(_DECIMAL_PARTS_PATTERN, 1),
        __dec_int=pl.col("__amount_clean").str.extract(_DECIMAL_PARTS_PATTERN, 2),
        __dec_frac=pl.col("__amount_clean").str.extract(_DECIMAL_PARTS_PATTERN, 3),
        __dec_exp=pl.col("__amount_clean").str.extract(_DECIMAL_PARTS_PATTERN, 4),
    ).with_columns(
        __dec_syntax_ok=pl.col("__dec_int").is_not_null()
        & (
            (pl.col("__dec_int") != "") | (pl.col("__dec_frac").is_not_null() & (pl.col("__dec_frac") != ""))
        ),
    ).with_columns(
        __dec_frac_n=pl.col("__dec_frac").fill_null(""),
        __dec_exp_n=pl.col("__dec_exp").fill_null("0"),
    ).with_columns(
        __dec_exp_i=pl.col("__dec_exp_n").cast(pl.Int32, strict=False),
        __mantissa=(pl.col("__dec_int").fill_null("") + pl.col("__dec_frac_n")),
    ).with_columns(
        # Significance is counted on the fully stripped mantissa (leading
        # *and* trailing zeros removed, mirroring Decimal equality); the
        # adjusted exponent compensates for the removed trailing zeros.
        # E.g. "1000000000000000000.00" -> sig "1", adj +18 -> 19 integer
        # digits (precision error), not 21 + 18 (spurious not-representable).
        __mantissa_sig=pl.col("__mantissa").str.strip_chars_start("0").str.strip_chars_end("0"),
        __mantissa_stripped=pl.col("__mantissa").str.strip_chars_end("0"),
    ).with_columns(
        __mantissa_sig_len=pl.when(pl.col("__mantissa_sig") == "").then(0).otherwise(
            pl.col("__mantissa_sig").str.len_chars()
        ),
        __trailing_zeros=pl.col("__mantissa").str.len_chars() - pl.col("__mantissa_stripped").str.len_chars(),
        __frac_len=pl.col("__dec_frac_n").str.len_chars(),
    ).with_columns(
        # Adjusted exponent after removing trailing zeros from the mantissa:
        # value = stripped_mantissa * 10^(exp - frac_len + trailing_zeros).
        __adj_exp=pl.col("__dec_exp_i") - pl.col("__frac_len") + pl.col("__trailing_zeros"),
    ).with_columns(
        # Scale = significant fraction digits = max(0, -adj_exp) when the
        # value is non-zero; exact zero always has scale 0.
        __amount_scale=pl.when(pl.col("__mantissa_sig_len") == 0).then(0).otherwise(
            pl.when(pl.col("__adj_exp") >= 0).then(0).otherwise(-pl.col("__adj_exp"))
        ),
        # Integer digits = significant mantissa digits + adj_exp (floored
        # at 0 for sub-unit values); exact zero has 1 integer digit.
        __amount_int_digits=pl.when(pl.col("__mantissa_sig_len") == 0).then(1).otherwise(
            pl.when(pl.col("__mantissa_sig_len") + pl.col("__adj_exp") <= 0).then(0).otherwise(
                pl.col("__mantissa_sig_len") + pl.col("__adj_exp")
            )
        ),
    ).with_columns(
        # ``quantize(0.01)`` raises InvalidOperation (not-representable)
        # when the quantized result would need more than the default 28
        # significant digits, i.e. more than 26 integer digits (26 + 2
        # fraction digits). Larger integer counts therefore report
        # not-representable, not precision, exactly like the reference.
        __bad_representable=(
            pl.col("__amount_gate_ok")
            & (
                ~pl.col("__dec_syntax_ok")
                | pl.col("__dec_exp_i").is_null()
                | (pl.col("__amount_int_digits") > 26)
            )
        ),
        __bad_scale=(
            pl.col("__amount_gate_ok")
            & pl.col("__dec_syntax_ok")
            & pl.col("__dec_exp_i").is_not_null()
            & (pl.col("__amount_scale") > 2)
        ),
        __bad_precision=(
            pl.col("__amount_gate_ok")
            & pl.col("__dec_syntax_ok")
            & pl.col("__dec_exp_i").is_not_null()
            & (pl.col("__amount_scale") <= 2)
            & (pl.col("__amount_int_digits") >= 19)
        ),
    )
    return staged.with_columns(
        # Exact decimal text for validated rows: the cleaned published text
        # itself (underscores stripped like ``Decimal``). ``str.to_decimal``
        # parses plain and exponent notation with exact decimal arithmetic
        # (never binary float); invalid rows carry null and fail explicitly
        # via the __bad_* flags with reference-identical messages.
        __amount_decimal_text=pl.when(
            pl.col("__amount_gate_ok")
            & pl.col("__dec_syntax_ok")
            & pl.col("__dec_exp_i").is_not_null()
            & (pl.col("__amount_scale") <= 2)
            & (pl.col("__amount_int_digits") < 19)
        )
        .then(pl.col("__amount_clean"))
        .otherwise(None),
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
    """Native ``_aware_instant``: RFC 3339 text to a UTC instant or null.

    Accepts the adapter-emitted shapes (``T``/space separator, seconds,
    optional fraction of any length truncated to microseconds exactly like
    the reference, uppercase-``Z``/``±HH:MM`` offset), mirroring the
    reference ``text.replace("Z", "+00:00")`` exactly (lowercase ``z``
    stays undated in both). Wilder ``fromisoformat`` forms
    (seconds-less instants, hour-only offsets, compact dates) cannot be
    emitted by the Atom/TED adapters -- Atom ``updated`` is always full
    RFC 3339 with seconds -- and resolve to null here; that constraint is
    enforced by ``AdapterContractTests``.
    """

    guarded = (
        text.is_not_null()
        & text.str.contains(r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")
    )
    # Normalize the separator to ``T`` and trailing ``Z`` to an explicit
    # offset so one explicit, non-inferred chrono format parses every
    # guarded row; non-guarded rows carry a parseable sentinel that is
    # nulled afterwards (strict=False leaves genuine junk as null too).
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
    "__rec_order",
    "__tomb_order",
]


def _fetch_first_by_order(frame: pl.DataFrame, mask: pl.Expr, order_col: str) -> dict | None:
    """Fetch the first row (by input order) matching ``mask`` (bounded).

    Explicit-failure diagnostic only: at most ``_MAX_FAILURE_IDS`` rows
    reach the driver, and only to name the offending row in the raised
    ``ValueError``. Never on the success path.
    """

    offenders = frame.filter(mask).sort(order_col).head(_MAX_FAILURE_IDS)
    if offenders.height == 0:
        return None
    return offenders.to_dicts()[0]


def _fetch_first_unsupported_source(records_indexed: pl.DataFrame) -> dict | None:
    """Fetch the first null/unsupported-source record (bounded diagnostic).

    Null-safe: ``source.is_null()`` rows never match the per-source branch
    filters, so without this fetch they would disappear silently. Only to
    name the offending row in the raised ``ValueError``.
    """

    if records_indexed.height == 0:
        return None
    bad = records_indexed.filter(
        pl.col("source").is_null() | ~pl.col("source").is_in(["ted", "placsp", "boe"])
    ).sort("__rec_order").head(_MAX_FAILURE_IDS)
    if bad.height == 0:
        return None
    return bad.to_dicts()[0]


def _raise_failures_in_reference_order(
    combined: pl.DataFrame, first_unsupported: dict | None
) -> None:
    """Raise the first reference-equivalent failure (records, tombstones, amounts).

    Reference order: record row-level failures in input order (unsupported
    source rank 0, missing identity rank 1, amount rank 2, ties broken by
    rank), then tombstone failures in input order (bad source rank 0,
    missing ref rank 1), then amounts already interleaved above. Collisions
    are checked separately afterwards, exactly like the reference.
    Amount failures carry three sub-ranks (representable 0, scale 1,
    precision 2) evaluated per row; the first failing row in input order
    wins and its sub-rank decides the message, mirroring the per-row
    ``_exact_amount`` sequence.
    """

    # --- Record phase: collect first candidate of each kind by input order.
    rec_candidates: list[tuple[int, int, str, dict]] = []
    if first_unsupported is not None:
        rec_candidates.append((first_unsupported["__rec_order"], 0, "unsupported", first_unsupported))
    row = _fetch_first_by_order(
        combined,
        pl.col("__raise_ted_id") | pl.col("__raise_placsp_id") | pl.col("__raise_boe_id"),
        "__rec_order",
    )
    if row is not None and row.get("__rec_order") is not None:
        rec_candidates.append((row["__rec_order"], 1, "identity", row))
    for sub_rank, column, template in (
        (0, "__bad_representable", "amount {!r} is not representable as decimal(20,2)"),
        (1, "__bad_scale", "amount {!r} has more than two fractional digits"),
        (2, "__bad_precision", "amount {!r} exceeds decimal(20,2) precision"),
    ):
        candidate = _fetch_first_by_order(
            combined, pl.col("__amount_gate_ok") & pl.col(column), "__rec_order"
        )
        if candidate is not None and candidate.get("__rec_order") is not None:
            rec_candidates.append((candidate["__rec_order"], 2, f"amount:{sub_rank}:{template}", candidate))
    if rec_candidates:
        # Amount sub-kinds on the SAME row must follow per-row order
        # (representable, scale, precision), not global category order: pick
        # the smallest input order, then the smallest rank/template.
        rec_candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        order, rank, kind, culprit = rec_candidates[0]
        if kind == "unsupported":
            raise ValueError(
                f"Unsupported bronze source for canonical Silver: {culprit['source']!r}"
            )
        if kind == "identity":
            if culprit.get("__raise_ted_id"):
                locator = culprit.get("record_locator")
                raise ValueError(
                    f"TED bronze record without a published notice number: {locator}"
                )
            if culprit.get("__raise_placsp_id"):
                locator = culprit.get("record_locator")
                raise ValueError(
                    f"PLACSP bronze record without an atom id: {locator}"
                )
            locator = culprit.get("record_locator")
            raise ValueError(
                f"BOE bronze record without an item id: {locator}"
            )
        # Amount failure: recompute which sub-kind THIS row triggers first.
        assert kind.startswith("amount:")
        for column, template in (
            ("__bad_representable", "amount {!r} is not representable as decimal(20,2)"),
            ("__bad_scale", "amount {!r} has more than two fractional digits"),
            ("__bad_precision", "amount {!r} exceeds decimal(20,2) precision"),
        ):
            if culprit.get(column):
                raise ValueError(
                    f"{culprit.get('__amount_event_id')}: " + template.format(culprit.get("__amount_rep"))
                )
        raise AssertionError("unreachable amount failure")

    # --- Tombstone phase (only when no record failed).
    tomb_bad_source = _fetch_first_by_order(combined, pl.col("__raise_tomb_source"), "__tomb_order")
    tomb_missing_ref = _fetch_first_by_order(combined, pl.col("__raise_tomb_ref"), "__tomb_order")
    tomb_candidates: list[tuple[int, int, dict]] = []
    if tomb_bad_source is not None and tomb_bad_source.get("__tomb_order") is not None:
        tomb_candidates.append((tomb_bad_source["__tomb_order"], 0, tomb_bad_source))
    if tomb_missing_ref is not None and tomb_missing_ref.get("__tomb_order") is not None:
        tomb_candidates.append((tomb_missing_ref["__tomb_order"], 1, tomb_missing_ref))
    if tomb_candidates:
        tomb_candidates.sort(key=lambda item: (item[0], item[1]))
        _, rank, culprit = tomb_candidates[0]
        if rank == 0:
            raise ValueError(
                f"Unsupported tombstone source for canonical Silver: {culprit.get('__tomb_source')!r}"
            )
        locator = culprit.get("record_locator")
        raise ValueError(f"PLACSP tombstone without a full ref: {locator}")

    # --- Amount failures on tombstone rows are impossible (no amounts there).


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
        "__rec_order": pl.lit(None, dtype=pl.UInt32),
        "__tomb_order": pl.lit(None, dtype=pl.UInt32),
    }
    defaults.update(overrides)
    return [expression.alias(name) for name, expression in defaults.items() if name in _HELPER_COLUMNS]


# ---------------------------------------------------------------------------
# Source branches (lazy: they compose one plan per source)
# ---------------------------------------------------------------------------


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
    amount_text = _first_text(selected, selected_str)
    amount_norm = _normalize_amount_text(amount_text.fill_null(""))
    gate = _amount_gate(amount_norm)
    base = raws.with_columns(
        __notice_id=notice_id,
        __amount_norm=amount_norm,
        __amount_gate=gate,
    )
    base = _attach_amount(
        base,
        event_id=(pl.lit("ted:notice:") + pl.col("__notice_id")),
        norm=pl.col("__amount_norm"),
        fallback=pl.lit(None, dtype=pl.String),
        gate_ok=pl.col("__amount_gate").is_not_null(),
    )
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
    estimated = _estimated_expr()
    helpers = {
        "__raise_ted_id": pl.col("__notice_id").is_null() | (pl.col("__notice_id") == ""),
        "__amount_event_id": pl.col("__amount_event_id"),
        "__amount_rep": pl.col("__amount_rep"),
        "__amount_gate_ok": pl.col("__amount_gate_ok"),
        "__bad_representable": pl.col("__bad_representable"),
        "__bad_scale": pl.col("__bad_scale"),
        "__bad_precision": pl.col("__bad_precision"),
        "__rec_order": pl.col("__rec_order"),
    }
    return base.select(
        *_BRANCH_PROVENANCE,
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
        *_filler_helpers(**helpers),
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
    # mirroring ``key in payload`` in the reference.
    overall_present = payload.str.contains(r'"amount_estimated_overall"\s*:').fill_null(False)
    tax_present = payload.str.contains(r'"amount_tax_exclusive"\s*:').fill_null(False)
    overall_text = _first_text(pl.col("__overall_raw"), pl.col("__overall_raw_str"))
    tax_text = _first_text(pl.col("__tax_raw"), pl.col("__tax_raw_str"))
    overall_norm = _normalize_amount_text(overall_text.fill_null(""))
    tax_norm = _normalize_amount_text(tax_text.fill_null(""))
    # Legacy fallback is Python ``str()`` of the value: strings as-is
    # (whitespace kept; ``Decimal`` tolerates it), numbers as JSON text
    # (identical to ``str()``), booleans in Python case, JSON null as null
    # (gated upstream). List/dict values as JSON text match ``str()`` only
    # for empty containers; non-empty containers cannot be emitted by the
    # adapters (flat float values, enforced by ``AdapterContractTests``).
    overall_fallback = (
        pl.when(pl.col("__overall_value").is_null())
        .then(None)
        .when(pl.col("__overall_value_str"))
        .then(pl.col("__overall_value"))
        .when(pl.col("__overall_value") == "true")
        .then(pl.lit("True"))
        .when(pl.col("__overall_value") == "false")
        .then(pl.lit("False"))
        .otherwise(pl.col("__overall_value"))
    )
    tax_fallback = (
        pl.when(pl.col("__tax_value").is_null())
        .then(None)
        .when(pl.col("__tax_value_str"))
        .then(pl.col("__tax_value"))
        .when(pl.col("__tax_value") == "true")
        .then(pl.lit("True"))
        .when(pl.col("__tax_value") == "false")
        .then(pl.lit("False"))
        .otherwise(pl.col("__tax_value"))
    )
    base = raws.with_columns(
        __atom_id=atom_id,
        __updated=updated,
        __marker=marker,
        __overall_present=overall_present,
        __tax_present=tax_present,
        __overall_norm=overall_norm,
        __tax_norm=tax_norm,
        __overall_fallback=overall_fallback,
        __tax_fallback=tax_fallback,
    )
    base = _attach_amount(
        base,
        event_id=(pl.lit("placsp:notice:") + pl.col("__atom_id") + pl.lit("@") + pl.col("__marker")),
        norm=pl.when(pl.col("__overall_present")).then(pl.col("__overall_norm")).otherwise(pl.col("__tax_norm")),
        fallback=pl.when(pl.col("__overall_present")).then(pl.col("__overall_fallback")).otherwise(
            pl.col("__tax_fallback")
        ),
        # Presence plus JSON-null check: ``payload[key] is not None``. A
        # present-but-empty/zero/false/container value still passes the
        # gate; only explicit JSON null (decoded null with present key)
        # stays null without failing.
        gate_ok=(
            pl.when(pl.col("__overall_present"))
            .then(pl.col("__overall_value").is_not_null())
            .otherwise(pl.col("__tax_present") & pl.col("__tax_value").is_not_null())
        ),
    )
    currency_text = pl.when(pl.col("__overall_present")).then(
        _first_text(pl.col("__overall_currency"), pl.col("__overall_currency_str"))
    ).otherwise(_first_text(pl.col("__tax_currency"), pl.col("__tax_currency_str")))
    estimated = _estimated_expr()
    helpers = {
        "__raise_placsp_id": pl.col("__atom_id").is_null() | (pl.col("__atom_id") == ""),
        "__amount_event_id": pl.col("__amount_event_id"),
        "__amount_rep": pl.col("__amount_rep"),
        "__amount_gate_ok": pl.col("__amount_gate_ok"),
        "__bad_representable": pl.col("__bad_representable"),
        "__bad_scale": pl.col("__bad_scale"),
        "__bad_precision": pl.col("__bad_precision"),
        "__rec_order": pl.col("__rec_order"),
    }
    return base.select(
        *_BRANCH_PROVENANCE,
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
        *_filler_helpers(**helpers),
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
        *_filler_helpers(
            __raise_boe_id=pl.col("__item_id").is_null() | (pl.col("__item_id") == ""),
            __rec_order=pl.col("__rec_order"),
        ),
    )


def _tombstone_branch(tombstones: pl.LazyFrame) -> pl.LazyFrame:
    ref = pl.col("source_record_id").fill_null("").str.strip_chars()
    return tombstones.select(
        *_BRANCH_PROVENANCE,
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
        *_filler_helpers(
            __raise_tomb_ref=ref == "",
            # Null-safe: a null tombstone source is an explicit failure,
            # never silently rewritten to PLACSP.
            __raise_tomb_source=pl.col("source").is_null() | (pl.col("source") != "placsp"),
            __tomb_source=pl.col("source"),
            __tomb_order=pl.col("__tomb_order"),
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

    The source branches compose a single lazy plan that is collected once;
    the explicit-failure checks are native filters over that one
    materialized frame, ordered exactly like the reference (record input
    order, tombstone input order, then collisions in sorted event order).
    """

    records_indexed = records.with_row_index("__rec_order")
    tombstones_indexed = tombstones.with_row_index("__tomb_order")
    # Null-safe unsupported-source detection on the indexed records before
    # branching, so no row can disappear: rows whose source is null or
    # outside {ted, placsp, boe} match no source branch.
    first_unsupported = _fetch_first_unsupported_source(records_indexed)
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
        # No mappable rows: either truly empty input or only unsupported
        # sources (which raise below, exactly like the reference, instead
        # of returning an empty frame).
        if first_unsupported is not None:
            raise ValueError(
                f"Unsupported bronze source for canonical Silver: {first_unsupported['source']!r}"
            )
        return pl.DataFrame(schema=PROCUREMENT_EVENT_SCHEMA)
    _raise_failures_in_reference_order(combined, first_unsupported)
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
