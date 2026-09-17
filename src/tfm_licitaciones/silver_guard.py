"""Conservative batch eligibility guard for the native Silver kernel.

Production Silver is hybrid by design: :func:`build_procurement_events`
inspects the complete batch (records and tombstones) with this module and
either runs the native Polars kernel on the *whole* batch or hands the
*original* frames to the frozen python-row reference. The guard never runs
the native engine "to see if it works": selection happens first, and a
single ineligible row routes the entire batch to the reference, which keeps
the historical values, first-error ordering and failure messages.

The native kernel only claims exactness inside the small domain documented
here. The guard is the authority that decides membership; it parses each
``payload_json`` with CPython :func:`json.loads` (the same parser the
reference uses, so arbitrary-precision integers and JSON types are seen
exactly as the oracle sees them) and rejects any shape the kernel does not
implement exactly. When in doubt it falls back.

Before any field check, the **whole parsed document** (unused fields,
keys and values included) must stay inside the backend JSON parser's
domain, because ``json_path_match`` parses the complete payload and fails
silently -- every path resolves to null -- outside its own limits:

- at most 64 nested containers (the backend rejects deeper documents at
  ~127 levels; the guard keeps a 2x margin and real payloads are shallow);
- integers within signed 64-bit range (JSON integers of arbitrary
  magnitude are parsed by the backend as doubles until they overflow);
- finite floats only (``1e309`` parses as infinity in CPython and is
  rejected by the backend);
- object keys match ``[A-Za-z0-9_-]+`` at every depth, including unused
  objects: quotes can spoof type/presence probes and colons can spoof the
  object-value extractor, even in canonical JSON;
- no string containing the ASCII information separators U+001C-U+001F:
  CPython ``str.strip`` treats them as whitespace and the backend
  ``strip_chars`` does not, so any trimmed use would diverge.

Eligible domain (per record ``source``):

- ``ted``: identity keys (``ND``/``notice_id``) resolve through the
  historical ``or`` chain to a non-empty string after stripping. Text keys
  (``TI``/``title``/``description-glo``/``description``/``buyer-name``/
  ``buyer``) are strings, flat lists of strings, or objects whose members
  are strings/flat string lists. Code keys (``PC``/``cpv``) are strings or
  flat string lists. Amount keys are plain decimals
  (``[+-]?digits(.[0-9]{1,2})?``, at most 18 integer digits), digit-free
  strings whose separator-free projection is not a positive-infinity
  spelling (both engines resolve them to null) or JSON integers of at most
  18 digits. JSON floats fall back: their decimal text can change in the
  backend parser even when Python ``str()`` looks like a plain decimal.
  Positive-infinity spellings and non-plain
  digit-bearing spellings are decided by the reference. Scalar keys (``currency``,
  ``procedure-identifier``, ``BT-04-notice``, ``buyer-identifier``, ``BI``,
  ``notice-type``, ``CY``, ``country``, ``url``) and ``links.html`` values
  are strings. Country keys additionally require ASCII: ``ESP`` maps to
  ``ES``, uppercase pairs are preserved, and other ASCII text becomes null
  in both engines. Non-ASCII countries fall back in both directions of
  Unicode disagreement. Date keys
  (``PD``/``publication_date``/``publication-date``) are strings whose
  first ten stripped characters are ``YYYY-MM-DD`` with a valid
  year/month/day range.
- ``placsp``: ``atom_id`` is a non-empty string; ``updated`` is empty, a
  strict RFC 3339 instant with seconds (``YYYY-MM-DD[T ]HH:MM:SS[.f{1,9}]
  (Z|±HH:MM)``) whose components are in range, or a shape the reference
  also resolves to "undated" (invalid/naive instants, checked with the same
  ``datetime.fromisoformat`` step as the oracle); text/code/scalar keys are
  strings or flat string lists. Both amount fields are checked eagerly: a
  present ``*_raw`` must be a plain-decimal string (or absent/null) even
  when the value gate is null or the value key is absent, because the
  kernel's plan decodes it regardless; a present value must be null or a
  plain-decimal string/integer. Floats whose Python ``str()`` is a plain
  decimal remain eligible only with validated, non-null retained raw text:
  the float is then a presence gate, never the source of the Decimal.
- ``boe``: identity/text/scalar keys as above; ``publication_date`` follows
  the TED date rule.
- tombstones: ``source`` is exactly ``placsp`` and ``source_record_id``
  strips to a non-empty string without the divergent separators above.

Additionally, no probed key may appear nested anywhere in a payload: the
kernel's quoted-string probes are textual and rely on Bronze's canonical
JSON serialization plus the restricted key alphabet, so a shadowed key
would make them ambiguous. Escaped quotes in values cannot fake these
probes when all keys satisfy that alphabet.

Everything outside this domain (numeric identities, exponential or
underscored amounts, offset seconds/fractions, basic/week dates, nested
localized members, unsupported sources, invalid JSON, ...) is deliberately
ineligible and stays exact through the reference.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import polars as pl

# Fallback reason categories: batch-level observability only, no payloads.
REASON_UNSUPPORTED_SOURCE = "unsupported_source"
REASON_INVALID_JSON = "invalid_json"
REASON_PAYLOAD_NOT_OBJECT = "payload_not_object"
REASON_DOCUMENT_DEPTH = "document_depth_out_of_domain"
REASON_NUMBER_DOMAIN = "numeric_domain_out_of_domain"
REASON_STRING_DOMAIN = "string_domain_out_of_domain"
REASON_KEY_DOMAIN = "key_domain_out_of_domain"
REASON_IDENTITY = "identity_out_of_domain"
REASON_LOCALIZED_TEXT = "localized_text_out_of_domain"
REASON_CODE_LIST = "cpv_out_of_domain"
REASON_SCALAR_TEXT = "scalar_text_out_of_domain"
REASON_COUNTRY = "country_out_of_domain"
REASON_AMOUNT = "amount_out_of_domain"
REASON_DATE = "publication_date_out_of_domain"
REASON_TIMESTAMP = "timestamp_out_of_domain"
REASON_NESTED_KEY = "nested_probed_key"
REASON_TOMBSTONE_SOURCE = "tombstone_source_out_of_domain"
REASON_TOMBSTONE_REF = "tombstone_ref_out_of_domain"

_DATE_PREFIX = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_RFC3339 = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ]"
    r"(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?"
    r"(Z|[+-](\d{2}):(\d{2}))$"
)
_PLAIN_DECIMAL = re.compile(r"^[+-]?[0-9]{1,18}(?:\.[0-9]{1,2})?$")
_DIGIT_FREE = re.compile(r"^[^0-9]*$")
_INFINITY_SPELLING = re.compile(r"^\+?(?:inf|infinity)$", re.IGNORECASE)
# This alphabet excludes JSON syntax/escapes in keys, at every depth.
# Values retain their original Unicode and punctuation domain.
_SAFE_KEY = re.compile(r"[A-Za-z0-9_-]+")
# CPython ``str.strip`` trims the ASCII information separators; the native
# ``strip_chars`` (Rust ``char::is_whitespace``) does not. Every other
# whitespace code point behaves identically in both runtimes.
_DIVERGENT_WHITESPACE = re.compile(r"[\x1c-\x1f]")

# Whole-document limits of the backend JSON parser. Polars 1.44.2 resolves
# every path to null once a payload nests 127 containers and rejects integer
# or float literals beyond the double range; the guard keeps a 2x margin on
# depth and a signed-64-bit bound for integers instead of depending on the
# exact parser ceiling.
_MAX_CONTAINER_DEPTH = 64
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

# Keys whose value/type the native kernel probes textually. The guard rejects
# payloads where any of them also appears nested (see module docstring).
_TED_PROBED = frozenset(
    {
        "ND", "notice_id", "TI", "title", "description-glo", "description",
        "buyer-name", "buyer", "PC", "cpv", "estimated-value-lot",
        "framework-maximum-value-lot", "amount", "currency",
        "procedure-identifier", "BT-04-notice", "buyer-identifier", "BI",
        "notice-type", "CY", "country", "PD", "publication_date",
        "publication-date", "url",
    }
)
_PLACSP_PROBED = frozenset(
    {
        "atom_id", "updated", "buyer_dir3", "buyer", "title", "summary",
        "cpv", "status_code", "nuts_code", "url",
        "amount_estimated_overall", "amount_tax_exclusive",
        "amount_estimated_overall_raw", "amount_tax_exclusive_raw",
        "amount_estimated_overall_currency", "amount_tax_exclusive_currency",
    }
)
_BOE_PROBED = frozenset(
    {
        "item_id", "identificador", "title", "titulo", "buyer",
        "department", "summary", "publication_date", "url",
    }
)

_TED_AMOUNT_KEYS = ("estimated-value-lot", "framework-maximum-value-lot", "amount")
_TED_SCALAR_KEYS = (
    "currency", "procedure-identifier", "BT-04-notice", "buyer-identifier",
    "BI", "notice-type", "CY", "country", "url",
)
_TED_TEXT_KEYS = ("TI", "title", "description-glo", "description", "buyer-name", "buyer")
_PLACSP_SCALAR_KEYS = ("buyer_dir3", "buyer", "title", "summary", "url", "status_code", "nuts_code")
_PLACSP_CURRENCY_KEYS = (
    "amount_estimated_overall_currency",
    "amount_tax_exclusive_currency",
)
_BOE_TEXT_KEYS = ("title", "titulo", "summary", "buyer", "department")


@dataclass(frozen=True)
class NativeEligibility:
    """Batch-level verdict of the native-kernel guard."""

    eligible: bool
    reason: str | None = None


# ---------------------------------------------------------------------------
# Whole-document domain of the backend JSON parser
# ---------------------------------------------------------------------------


def _scalar_domain_reason(value: Any) -> str | None:
    """Reject one scalar outside the numeric/string domain of the backend."""

    kind = type(value)
    if kind is int:
        if value < _INT64_MIN or value > _INT64_MAX:
            return REASON_NUMBER_DOMAIN
    elif kind is float:
        if not math.isfinite(value):
            return REASON_NUMBER_DOMAIN
    elif kind is str:
        if _DIVERGENT_WHITESPACE.search(value) is not None:
            return REASON_STRING_DOMAIN
    return None


def _document_domain_reason(payload: dict[str, Any]) -> str | None:
    """Check the whole parsed document, unused fields and keys included.

    The backend parses the complete payload for every path expression and
    silently resolves every path to null once the document leaves its own
    parser limits, so field-level validation is not enough. Keys are checked
    too, both for divergent whitespace and for the alphabet required by
    the kernel's textual probes/extractors. Traversal is iterative so an
    extreme document cannot raise ``RecursionError`` here.
    """

    stack: list[tuple[Any, int]] = [(payload, 1)]
    search = _DIVERGENT_WHITESPACE.search
    safe_key = _SAFE_KEY.fullmatch
    while stack:
        value, depth = stack.pop()
        kind = type(value)
        if kind is dict:
            if depth > _MAX_CONTAINER_DEPTH:
                return REASON_DOCUMENT_DEPTH
            for key, item in value.items():
                if search(key) is not None:
                    return REASON_STRING_DOMAIN
                if safe_key(key) is None:
                    return REASON_KEY_DOMAIN
                item_kind = type(item)
                if item_kind is str:
                    if search(item) is not None:
                        return REASON_STRING_DOMAIN
                elif item_kind is dict or item_kind is list:
                    stack.append((item, depth + 1))
                else:
                    reason = _scalar_domain_reason(item)
                    if reason is not None:
                        return reason
        elif kind is list:
            if depth > _MAX_CONTAINER_DEPTH:
                return REASON_DOCUMENT_DEPTH
            for item in value:
                item_kind = type(item)
                if item_kind is str:
                    if search(item) is not None:
                        return REASON_STRING_DOMAIN
                elif item_kind is dict or item_kind is list:
                    stack.append((item, depth + 1))
                else:
                    reason = _scalar_domain_reason(item)
                    if reason is not None:
                        return reason
        else:
            reason = _scalar_domain_reason(value)
            if reason is not None:
                return reason
    return None


# ---------------------------------------------------------------------------
# Exact shape predicates (parsed values only; no type conversion)
# ---------------------------------------------------------------------------


def _resolved_or(*values: Any) -> Any:
    """Return Python's ``a or b or ...`` result (first truthy, else the last)."""

    resolved: Any = None
    for value in values:
        resolved = value
        if value:
            break
    return resolved


def _identity(value: Any) -> bool:
    """Whether an ``or``-resolved identity is usable by the frozen oracle."""

    return isinstance(value, str) and value.strip() != ""


def _optional_string(value: Any) -> bool:
    return value is None or isinstance(value, str)


def _text_leaf(value: Any) -> bool:
    """Eligible member of a localized object: string, flat string list or null."""

    if value is None or isinstance(value, str):
        return True
    if isinstance(value, list):
        return all(isinstance(item, str) for item in value)
    return False


def _text_value(value: Any) -> bool:
    if value is None or isinstance(value, str):
        return True
    if isinstance(value, list):
        return all(isinstance(item, str) for item in value)
    if isinstance(value, dict):
        return all(_text_leaf(item) for item in value.values())
    return False


def _code_value(value: Any) -> bool:
    if value is None or isinstance(value, str):
        return True
    if isinstance(value, list):
        return all(isinstance(item, str) for item in value)
    return False


def _plain_decimal(value: Any) -> bool:
    """Plain string/integer without a JSON float round trip.

    Finiteness and a plain Python spelling cannot establish exactness for
    floats: e.g. 123456789012345.67 becomes 123456789012345.69 when resolved
    by the backend JSON parser. Monetary floats need the reference unless
    PLACSP supplies validated retained text instead (checked separately).
    """

    if isinstance(value, bool):
        return False
    if isinstance(value, str):
        return _PLAIN_DECIMAL.fullmatch(value) is not None
    if isinstance(value, int):
        return _PLAIN_DECIMAL.fullmatch(str(value)) is not None
    return False


def _ted_amount_value(value: Any) -> bool:
    """Eligible TED amount value: plain decimal or a proven textual gate failure.

    For strings with digits, only the plain-decimal spelling is admitted; the
    reference normalizes separators and exponents before parsing, and that
    normalizer is out of the contract. For digit-free strings the historical
    float gate can only yield null (``float`` accepts ``inf``/``nan`` and
    fails on everything else), and the kernel's float cast also yields null,
    with one exception: when the reference normalizer produces a
    positive-infinity spelling it raises after the gate, while the kernel
    stays null. The normalizer only strips outer whitespace and removes or
    replaces separators, never reordering letters or signs, so that class is
    exactly the strings whose separator-free projection is an ``inf``/
    ``infinity`` spelling; those fall back. Every other digit-free spelling
    (``"n/a"``, ``"abc"``, ``"nan"``, ``"-inf"``) is exact in both engines.
    """

    if value is None:
        return True
    if isinstance(value, str):
        if not value.isascii():
            return False
        if _PLAIN_DECIMAL.fullmatch(value):
            return True
        if _DIGIT_FREE.fullmatch(value) is None:
            return False
        projection = value.strip().replace(" ", "").replace(",", "").replace(".", "")
        return _INFINITY_SPELLING.fullmatch(projection) is None
    if isinstance(value, bool):
        return False
    return _plain_decimal(value)


def _placsp_amount_field_eligible(payload: dict[str, Any], key: str) -> bool:
    """Restrict one PLACSP amount field and its retained raw text.

    The kernel plan resolves the retained text and the legacy fallback of
    **both** amount fields eagerly, even when the value gate is null or the
    value key is absent, so a present ``*_raw`` must always be a
    plain-decimal string (or null). The legacy value is only consulted when
    the key is present and not JSON-null, exactly like the reference
    presence gate. A float needs validated raw text, so only its non-null
    presence is used; its backend-decoded spelling never reaches Decimal.
    Keep the previous plain-spelling bound even on these float gates to
    keep the eligible domain limited to values with exact reference parity.
    """

    raw = payload.get(f"{key}_raw")
    if raw is not None and not (
        isinstance(raw, str) and _PLAIN_DECIMAL.fullmatch(raw) is not None
    ):
        return False
    if key not in payload:
        return True
    value = payload[key]
    if value is None:
        return True
    if isinstance(value, float):
        return raw is not None and _PLAIN_DECIMAL.fullmatch(str(value)) is not None
    return _plain_decimal(value)


def _country_value(value: Any) -> bool:
    """Eligible country value for the TED ``_ted_country`` mapping.

    ASCII makes the reference's ``isalpha()/isupper()`` and the kernel's
    Unicode uppercase-letter regex agree, including null results for junk
    ASCII. Reject all other strings: Unicode disagreement goes both ways
    (Python accepts ``A中``; the backend accepts newer letters such as
    U+1C89 that Python 3.11's Unicode tables do not recognize as alphabetic).
    """

    if value is None:
        return True
    return isinstance(value, str) and value.isascii()


def _placsp_amount_eligible(payload: dict[str, Any]) -> bool:
    """Both amount fields must be in-domain.

    The reference selects the first present field, but the kernel resolves
    the retained text and legacy fallback of *both* fields while composing
    its plan, so a shape it cannot resolve (for example an array value in the
    unchosen field) must exclude the batch even when the chosen field is
    fine.
    """

    return _placsp_amount_field_eligible(payload, "amount_estimated_overall") and _placsp_amount_field_eligible(
        payload, "amount_tax_exclusive"
    )


def _date_value(value: Any) -> bool:
    """First ten stripped characters form ``YYYY-MM-DD`` with sane ranges."""

    if value is None:
        return True
    if not isinstance(value, str):
        return False
    text = value.strip()
    if text == "":
        return True
    match = _DATE_PREFIX.match(text)
    if match is None:
        return False
    year, month, day = (int(part) for part in match.groups())
    return 1 <= year <= 9999 and 1 <= month <= 12 and 1 <= day <= 31


def _timestamp_value(value: Any) -> bool:
    """Backend-exact ``updated`` domain for the kernel.

    The kernel parses only strict RFC 3339 instants with seconds and a
    whole-minute offset; every other string resolves to null there. The
    guard therefore admits:

    - strict instants whose components are in range (the kernel parses them
      and the differential tests pin the same instant as CPython);
    - strings outside that pattern for which the *reference* is also
      undated, decided with the same ``datetime.fromisoformat`` step the
      reference performs (invalid or naive instants, including the "not a
      timestamp" sentinels that exist in Bronze) -- null natively is exact
      there;

    and rejects everything else (offset seconds/fractions, hour-only
    offsets, basic/week dates, out-of-range components, ...), so the
    reference decides those batches.
    """

    if value is None:
        return True
    if not isinstance(value, str):
        return False
    text = value.strip()
    if text == "":
        return True
    match = _RFC3339.fullmatch(text)
    if match is not None:
        year, month, day, hour, minute, second = (int(part) for part in match.groups()[:6])
        zone, offset_hour, offset_minute = match.groups()[7], match.groups()[8], match.groups()[9]
        # Year bounds stay away from the edges where the reference's
        # ``astimezone(utc)`` could overflow (an exception, not a value).
        if not (2 <= year <= 9998 and 1 <= month <= 12 and 1 <= day <= 31):
            return False
        if hour > 23 or minute > 59 or second > 59:
            return False
        if zone != "Z" and (int(offset_hour) > 23 or int(offset_minute) > 59):
            return False
        return True
    # Outside the kernel pattern the kernel result is null, which is exact
    # only when the reference also resolves the text to "undated".
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return True
    return parsed.tzinfo is None or parsed.utcoffset() is None


def _links_value(value: Any) -> bool:
    """``links`` is null or an object whose ``html`` maps to strings."""

    if value is None:
        return True
    if not isinstance(value, dict):
        return False
    html = value.get("html")
    if html is None:
        return True
    if not isinstance(html, dict):
        return False
    return all(isinstance(item, str) for item in html.values())


def _has_nested_probed_key(value: Any, probed: frozenset[str]) -> bool:
    """Whether a probed key appears at depth >= 1 under ``value``."""

    kind = type(value)
    if kind is dict:
        for key, item in value.items():
            if key in probed or _has_nested_probed_key(item, probed):
                return True
    elif kind is list:
        for item in value:
            if _has_nested_probed_key(item, probed):
                return True
    return False


def _nested_probed_key(payload: dict[str, Any], probed: frozenset[str]) -> bool:
    """Fast path: scalar-only payloads (the common case) skip recursion."""

    for value in payload.values():
        kind = type(value)
        if kind is dict:
            for key, item in value.items():
                if key in probed or _has_nested_probed_key(item, probed):
                    return True
        elif kind is list:
            for item in value:
                if _has_nested_probed_key(item, probed):
                    return True
    return False


# ---------------------------------------------------------------------------
# Per-source checkers (return a reason category or None)
# ---------------------------------------------------------------------------


def _check_ted(payload: dict[str, Any]) -> str | None:
    if _nested_probed_key(payload, _TED_PROBED):
        return REASON_NESTED_KEY
    if not _identity(_resolved_or(payload.get("ND"), payload.get("notice_id"))):
        return REASON_IDENTITY
    for key in _TED_TEXT_KEYS:
        if not _text_value(payload.get(key)):
            return REASON_LOCALIZED_TEXT
    for key in ("PC", "cpv"):
        if not _code_value(payload.get(key)):
            return REASON_CODE_LIST
    for key in _TED_AMOUNT_KEYS:
        if not _ted_amount_value(payload.get(key)):
            return REASON_AMOUNT
    for key in _TED_SCALAR_KEYS:
        if not _optional_string(payload.get(key)):
            return REASON_SCALAR_TEXT
    for key in ("CY", "country"):
        if not _country_value(payload.get(key)):
            return REASON_COUNTRY
    for key in ("PD", "publication_date", "publication-date"):
        if not _date_value(payload.get(key)):
            return REASON_DATE
    if not _links_value(payload.get("links")):
        return REASON_SCALAR_TEXT
    return None


def _check_placsp(payload: dict[str, Any]) -> str | None:
    if _nested_probed_key(payload, _PLACSP_PROBED):
        return REASON_NESTED_KEY
    if not _identity(payload.get("atom_id")):
        return REASON_IDENTITY
    if not _timestamp_value(payload.get("updated")):
        return REASON_TIMESTAMP
    for key in _PLACSP_SCALAR_KEYS + _PLACSP_CURRENCY_KEYS:
        if not _optional_string(payload.get(key)):
            return REASON_SCALAR_TEXT
    if not _code_value(payload.get("cpv")):
        return REASON_CODE_LIST
    if not _placsp_amount_eligible(payload):
        return REASON_AMOUNT
    return None


def _check_boe(payload: dict[str, Any]) -> str | None:
    if _nested_probed_key(payload, _BOE_PROBED):
        return REASON_NESTED_KEY
    if not _identity(_resolved_or(payload.get("item_id"), payload.get("identificador"))):
        return REASON_IDENTITY
    for key in _BOE_TEXT_KEYS:
        if not _text_value(payload.get(key)):
            return REASON_LOCALIZED_TEXT
    if not _date_value(payload.get("publication_date")):
        return REASON_DATE
    if not _optional_string(payload.get("url")):
        return REASON_SCALAR_TEXT
    return None


_RECORD_CHECKERS = {
    "ted": _check_ted,
    "placsp": _check_placsp,
    "boe": _check_boe,
}


def assess_native_eligibility(records: pl.DataFrame, tombstones: pl.DataFrame) -> NativeEligibility:
    """Decide whether the native kernel may process the complete batch.

    Every record is parsed with :func:`json.loads` and checked against the
    eligible domain; tombstones check their source and full reference. The
    first violation returns immediately with its reason category, and the
    caller routes the original frames to the frozen reference. No rows are
    filtered, reordered, normalized or deduplicated here.
    """

    for source, payload_json in zip(
        records.get_column("source"), records.get_column("payload_json")
    ):
        checker = _RECORD_CHECKERS.get(source)
        if checker is None:
            return NativeEligibility(False, REASON_UNSUPPORTED_SOURCE)
        try:
            payload = json.loads(payload_json)
        except RecursionError:
            # The document is far deeper than any admitted depth; the
            # reference hits the same CPython limit on these payloads.
            return NativeEligibility(False, REASON_DOCUMENT_DEPTH)
        except (TypeError, ValueError):
            return NativeEligibility(False, REASON_INVALID_JSON)
        if not isinstance(payload, dict):
            return NativeEligibility(False, REASON_PAYLOAD_NOT_OBJECT)
        reason = _document_domain_reason(payload)
        if reason is not None:
            return NativeEligibility(False, reason)
        reason = checker(payload)
        if reason is not None:
            return NativeEligibility(False, reason)

    for source, reference in zip(
        tombstones.get_column("source"), tombstones.get_column("source_record_id")
    ):
        if source != "placsp":
            return NativeEligibility(False, REASON_TOMBSTONE_SOURCE)
        if not _identity(reference):
            return NativeEligibility(False, REASON_TOMBSTONE_REF)
        if isinstance(reference, str) and _DIVERGENT_WHITESPACE.search(reference) is not None:
            return NativeEligibility(False, REASON_TOMBSTONE_REF)

    return NativeEligibility(True)
