"""Frozen python-row reference implementation of the canonical Silver contract.

This module preserves, unchanged, the per-row Python semantics that
:mod:`tfm_licitaciones.silver` shipped before the native Polars engine became
the production default. It is the historical parity oracle for tests and for
the engine benchmarks, and it is also the production **fallback**: the hybrid
routing in :mod:`tfm_licitaciones.silver` hands it the original frames for
every batch outside the native eligibility domain, so the historical values,
first errors and failure messages are preserved for the whole accepted
Bronze domain.

It is NOT refactored or "improved": its value is being frozen and independent
from the native kernel. Do not make its semantics depend on mutable
production code.

Frozen baseline: ``e69016f62fb985639e17153e24b3a570f2f39c20`` (the ``main``
merge of PR #15, the parent of the engine migration). The reference copies
the historical Silver transform plus its exact **transitive** semantics, so a
future change to any production module cannot silently move the oracle:

- ``src/tfm_licitaciones/silver.py`` (sha256
  ``529f2d1a0a1cc1910baaaa556e16ebab4a571dfa11f3431a9194be166a15aea8``):
  ``_exact_amount``, ``_aware_instant``, ``_single_published_value``,
  ``_ted_country``, ``_ted_event``, ``_placsp_event``, ``_boe_event``,
  ``_placsp_tombstone_event``, ``_record_event``, ``_selection_key``,
  ``_canonical_fields`` and ``build_procurement_events``.
- ``src/tfm_licitaciones/normalize.py`` (sha256
  ``9038e2af024994b7549da0f75a739e25d9b4b5e6be9c8fa2dcb6c10f711fe907``):
  ``_first_text``, ``_parse_date``, ``_normalize_amount_text``,
  ``_parse_amount``, ``_url_from_links``, ``_as_code_list``,
  ``normalize_ted`` and ``normalize_boe`` (``normalize_placsp`` and
  ``normalize_record`` were never called on this path).
- ``src/tfm_licitaciones/models.py`` (sha256
  ``b8452eca1276d592b0cafdfe23c3409eeb8ad54672dc2dd2d46fc8cbf61de6cb``):
  ``PROCUREMENT_EVENT_SCHEMA``, ``TenderRecord``, ``ProcurementEvent``
  (including the UTC enforcement in ``__post_init__``) and
  ``procurement_events_frame``.

Nothing here is "improved" or refactored: identifiers, messages, ordering,
Decimal(20,2) validation, timestamp handling, identity derivation, provenance
selection and collision semantics are preserved verbatim. Only imports and the
module docstring were adapted to make the copy self-contained; this module has
zero imports from ``tfm_licitaciones`` (stdlib + polars only, enforced by
``tests/test_silver_native.py::FrozenReferenceGuardTests``).

The output frame uses the frozen schema. If production ever changes the live
canonical schema, parity comparison fails loudly (frozen frame vs new
canonical schema) instead of silently moving the historical baseline.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import polars as pl

FROZEN_BASELINE_COMMIT = "e69016f62fb985639e17153e24b3a570f2f39c20"
FROZEN_SOURCE_SILVER = "src/tfm_licitaciones/silver.py @ e69016f62fb985639e17153e24b3a570f2f39c20"
FROZEN_SOURCE_NORMALIZE = "src/tfm_licitaciones/normalize.py @ e69016f62fb985639e17153e24b3a570f2f39c20"
FROZEN_SOURCE_MODELS = "src/tfm_licitaciones/models.py @ e69016f62fb985639e17153e24b3a570f2f39c20"
IMPLEMENTATION = "python-row"

# ---------------------------------------------------------------------------
# Frozen canonical schema (verbatim copy of PROCUREMENT_EVENT_SCHEMA @ e69016f)
# ---------------------------------------------------------------------------

PROCUREMENT_EVENT_SCHEMA = pl.Schema(
    {
        "event_id": pl.String,
        "procedure_id": pl.String,
        "source": pl.String,
        "source_event_type": pl.String,
        "buyer_id": pl.String,
        "buyer_name": pl.String,
        "title": pl.String,
        "description": pl.String,
        "cpv_codes": pl.List(pl.String),
        "estimated_value": pl.Decimal(precision=20, scale=2),
        "awarded_value": pl.Decimal(precision=20, scale=2),
        "currency": pl.String,
        "publication_date": pl.Date,
        "source_updated_at": pl.Datetime("us", "UTC"),
        "deadline": pl.Datetime("us", "UTC"),
        "status": pl.String,
        "nuts_code": pl.String,
        "country": pl.String,
        "source_url": pl.String,
        "ingested_at": pl.Datetime("us", "UTC"),
    }
)


# ---------------------------------------------------------------------------
# Frozen TenderRecord / ProcurementEvent (verbatim copy of models.py @ e69016f)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TenderRecord:
    """Represent one normalized tender at the silver-layer grain."""

    tender_id: str
    source: str
    title: str
    summary: str = ""
    buyer: str | None = None
    published_date: date | None = None
    amount: float | None = None
    currency: str | None = None
    country: str | None = None
    url: str | None = None
    cpv_main: str | None = None
    buyer_id: str | None = None
    region: str | None = None
    status: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the record using ISO dates for JSONL persistence."""

        payload = asdict(self)
        if self.published_date is not None:
            payload["published_date"] = self.published_date.isoformat()
        return payload


@dataclass(frozen=True)
class ProcurementEvent:
    """Represent one source-published event in the canonical Silver contract."""

    event_id: str
    procedure_id: str | None
    source: str
    source_event_type: str
    ingested_at: datetime
    buyer_id: str | None = None
    buyer_name: str | None = None
    title: str | None = None
    description: str | None = None
    cpv_codes: tuple[str, ...] = ()
    estimated_value: Decimal | None = None
    awarded_value: Decimal | None = None
    currency: str | None = None
    publication_date: date | None = None
    source_updated_at: datetime | None = None
    deadline: datetime | None = None
    status: str | None = None
    nuts_code: str | None = None
    country: str | None = None
    source_url: str | None = None

    def __post_init__(self) -> None:
        """Enforce the identifiers and UTC timestamp semantics of the contract."""

        for field_name in ("event_id", "source", "source_event_type"):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.procedure_id is not None and not self.procedure_id.strip():
            raise ValueError("procedure_id must be null or non-empty")
        if any(not code.strip() for code in self.cpv_codes):
            raise ValueError("cpv_codes must not contain empty values")
        if len(set(self.cpv_codes)) != len(self.cpv_codes):
            raise ValueError("cpv_codes must not contain duplicates")
        if self.ingested_at is None:
            raise ValueError("ingested_at must not be null")
        for field_name in ("source_updated_at", "deadline", "ingested_at"):
            value = getattr(self, field_name)
            if value is None:
                continue
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must include a timezone")
            object.__setattr__(self, field_name, value.astimezone(timezone.utc))

    def to_row(self) -> dict[str, Any]:
        """Serialize native values in the stable Polars schema order."""

        return {
            "event_id": self.event_id,
            "procedure_id": self.procedure_id,
            "source": self.source,
            "source_event_type": self.source_event_type,
            "buyer_id": self.buyer_id,
            "buyer_name": self.buyer_name,
            "title": self.title,
            "description": self.description,
            "cpv_codes": list(self.cpv_codes),
            "estimated_value": self.estimated_value,
            "awarded_value": self.awarded_value,
            "currency": self.currency,
            "publication_date": self.publication_date,
            "source_updated_at": self.source_updated_at,
            "deadline": self.deadline,
            "status": self.status,
            "nuts_code": self.nuts_code,
            "country": self.country,
            "source_url": self.source_url,
            "ingested_at": self.ingested_at,
        }


def procurement_events_frame(events: Iterable[ProcurementEvent]) -> pl.DataFrame:
    """Build a typed Silver frame, preserving the schema for an empty batch."""

    return pl.DataFrame((event.to_row() for event in events), schema=PROCUREMENT_EVENT_SCHEMA)


# ---------------------------------------------------------------------------
# Frozen normalization semantics (verbatim copy of normalize.py @ e69016f)
# ---------------------------------------------------------------------------


def _first_text(value: Any, language: str = "spa") -> str:
    """Extract a useful string from TED's localized dict/list structures."""

    if value is None:
        return ""
    if isinstance(value, dict):
        preferred = value.get(language)
        if preferred is not None:
            return _first_text(preferred, language)
        for candidate in value.values():
            text = _first_text(candidate, language)
            if text:
                return text
        return ""
    if isinstance(value, list):
        for candidate in value:
            text = _first_text(candidate, language)
            if text:
                return text
        return ""
    return str(value).strip()


def _parse_date(value: Any) -> date | None:
    """Parse an ISO-like source date, ignoring time and timezone suffixes."""

    text = _first_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _normalize_amount_text(value: Any) -> str | None:
    """Normalize decimal/thousands separators, or None when not parseable text."""

    text = _first_text(value).replace(" ", "")
    if not text:
        return None
    if "," in text and "." in text:
        decimal_separator = "," if text.rfind(",") > text.rfind(".") else "."
        thousands_separator = "." if decimal_separator == "," else ","
        return text.replace(thousands_separator, "").replace(decimal_separator, ".")
    if "," in text:
        return text.replace(",", ".")
    if text.count(".") == 1:
        return text
    return text.replace(".", "")


def _parse_amount(value: Any) -> float | None:
    """Parse a simple numeric amount while leaving ambiguous values null."""

    normalized = _normalize_amount_text(value)
    if normalized is None:
        return None
    try:
        amount = float(normalized)
    except ValueError:
        return None
    return amount if amount >= 0 else None


def _url_from_links(links: Any) -> str:
    """Extract a stable notice URL from TED's per-language links payload."""

    if not isinstance(links, dict):
        return ""
    html = links.get("html")
    if not isinstance(html, dict):
        return ""
    for language in ("ENG", "SPA", "DEU", "FRA"):
        if html.get(language):
            return str(html[language]).strip()
    return str(next(iter(html.values()), "")).strip()


def _as_code_list(value: Any) -> list[str]:
    """Flatten a list/scalar of CPV codes while preserving order."""

    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    codes: list[str] = []
    for item in items:
        text = _first_text(item)
        if text and text not in codes:
            codes.append(text)
    return codes


def normalize_ted(raw: dict[str, Any]) -> TenderRecord:
    """Normalize one TED eForms record."""

    title = _first_text(raw.get("TI") or raw.get("title"))
    summary = _first_text(raw.get("description-glo") or raw.get("description"))
    buyer = _first_text(raw.get("buyer-name") or raw.get("buyer")) or None
    amount = _parse_amount(
        raw.get("estimated-value-lot") or raw.get("framework-maximum-value-lot") or raw.get("amount")
    )
    cpv_codes = _as_code_list(raw.get("PC") or raw.get("cpv"))
    return TenderRecord(
        tender_id=_first_text(raw.get("ND") or raw.get("notice_id")),
        source="ted",
        title=title,
        summary=summary,
        buyer=buyer,
        published_date=_parse_date(raw.get("PD") or raw.get("publication_date") or raw.get("publication-date")),
        amount=amount,
        # The TED Search API normalizes value fields to EUR, so the currency
        # is inferred and kept explicit for downstream aggregation.
        currency=_first_text(raw.get("currency")) or ("EUR" if amount is not None else None),
        country=_first_text(raw.get("CY") or raw.get("country")) or None,
        url=_first_text(raw.get("url")) or _url_from_links(raw.get("links")) or None,
        cpv_main=cpv_codes[0] if cpv_codes else None,
        raw=raw,
    )


def normalize_boe(raw: dict[str, Any]) -> TenderRecord:
    """Normalize one BOE item as contextual procurement evidence."""

    return TenderRecord(
        tender_id=_first_text(raw.get("item_id") or raw.get("identificador")),
        source="boe",
        title=_first_text(raw.get("title") or raw.get("titulo")),
        summary=_first_text(raw.get("summary")),
        buyer=_first_text(raw.get("buyer") or raw.get("department")) or None,
        published_date=_parse_date(raw.get("publication_date")),
        country="ES",
        url=_first_text(raw.get("url")) or None,
        raw=raw,
    )


# ---------------------------------------------------------------------------
# Frozen Silver transform (verbatim copy of silver.py @ e69016f)
# ---------------------------------------------------------------------------

_MONEY_QUANTUM = Decimal("0.01")
_MONEY_INTEGER_MAX = Decimal(10) ** 18  # decimal(20,2) admits at most 18 integer digits
_TED_ALPHA3_COUNTRIES = {"ESP": "ES"}
_TED_ESTIMATED_FIELDS = ("estimated-value-lot", "framework-maximum-value-lot", "amount")
_PLACSP_ESTIMATED_FIELDS = ("amount_estimated_overall", "amount_tax_exclusive")


def _exact_amount(source_text: Any, accepted_value: float | None, *, context: str) -> Decimal | None:
    """Build an exact ``decimal(20,2)`` amount or fail explicitly.

    ``accepted_value`` is the legacy Bronze float gate: when it is null the
    amount was malformed or negative and stays null instead of being repaired.
    The preferred representation is the published text; only Bronze payloads
    predating raw-text retention fall back to ``str`` of the legacy float,
    which never carries binary-float artifacts.
    """

    if accepted_value is None:
        return None
    normalized = _normalize_amount_text(source_text) if source_text is not None else None
    representation = normalized if normalized is not None else str(accepted_value)
    try:
        value = Decimal(representation)
        quantized = value.quantize(_MONEY_QUANTUM)
    except InvalidOperation as exc:
        raise ValueError(f"{context}: amount {representation!r} is not representable as decimal(20,2)") from exc
    if quantized != value:
        raise ValueError(f"{context}: amount {representation!r} has more than two fractional digits")
    if abs(quantized) >= _MONEY_INTEGER_MAX:
        raise ValueError(f"{context}: amount {representation!r} exceeds decimal(20,2) precision")
    return quantized


def _aware_instant(value: Any) -> datetime | None:
    """Parse a timezone-aware source instant normalized to UTC, else None."""

    text = _first_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _single_published_value(payload: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    """Return the value only when the payload publishes exactly one distinct one."""

    values: list[str] = []
    for key in keys:
        raw = payload.get(key)
        if raw is None:
            continue
        for item in raw if isinstance(raw, list) else [raw]:
            text = _first_text(item)
            if text and text not in values:
                values.append(text)
    return values[0] if len(values) == 1 else None


def _ted_country(country: str | None) -> str | None:
    """Accept uppercase ISO alpha-2, map known TED alpha-3, else null."""

    if not country:
        return None
    mapped = _TED_ALPHA3_COUNTRIES.get(country)
    if mapped:
        return mapped
    if len(country) == 2 and country.isalpha() and country.isupper():
        return country
    return None


def _ted_event(payload: dict[str, Any], row: dict[str, Any]) -> ProcurementEvent:
    """Map one TED notice observation: one published notice number is one event."""

    notice_id = _first_text(payload.get("ND") or payload.get("notice_id"))
    if not notice_id:
        raise ValueError(f"TED bronze record without a published notice number: {row['record_locator']}")
    context = f"ted:notice:{notice_id}"
    record = normalize_ted(payload)
    # Select the first present (not None) field regardless of truthiness:
    # numeric zero is a valid exact amount and an invalid present value stays
    # null instead of falling through to a later field.
    selected = next((payload.get(key) for key in _TED_ESTIMATED_FIELDS if payload.get(key) is not None), None)
    estimated = _exact_amount(selected, _parse_amount(selected), context=context)
    # Currency is paired with an accepted canonical amount: when the selected
    # amount is rejected (estimated None) the currency stays null even if the
    # source publishes one.
    if estimated is None:
        currency = None
    else:
        currency = _first_text(payload.get("currency")) or "EUR"
    procedure = _single_published_value(payload, ("procedure-identifier", "BT-04-notice"))
    notice_type = _first_text(payload.get("notice-type"))
    return ProcurementEvent(
        event_id=context,
        procedure_id=f"ted:procedure:{procedure}" if procedure else None,
        source="ted",
        source_event_type=f"notice:{notice_type}" if notice_type else "notice",
        buyer_id=_single_published_value(payload, ("buyer-identifier", "BI")),
        buyer_name=record.buyer,
        title=record.title or None,
        description=record.summary or None,
        cpv_codes=tuple(_as_code_list(payload.get("PC") or payload.get("cpv"))),
        estimated_value=estimated,
        awarded_value=None,
        currency=currency,
        publication_date=record.published_date,
        source_updated_at=None,
        deadline=None,
        status=None,
        nuts_code=None,
        country=_ted_country(record.country),
        source_url=record.url,
        ingested_at=row["raw_retrieved_at"],
    )


def _placsp_event(payload: dict[str, Any], row: dict[str, Any]) -> ProcurementEvent:
    """Map one PLACSP entry: one (atom id, valid updated instant) is one snapshot."""

    atom_id = _first_text(payload.get("atom_id"))
    if not atom_id:
        raise ValueError(f"PLACSP bronze record without an atom id: {row['record_locator']}")
    updated = _aware_instant(payload.get("updated"))
    marker = updated.isoformat() if updated is not None else "undated"
    context = f"placsp:notice:{atom_id}@{marker}"
    chosen = next((key for key in _PLACSP_ESTIMATED_FIELDS if key in payload), None)
    if chosen is None:
        estimated, currency = None, None
    else:
        estimated = _exact_amount(payload.get(f"{chosen}_raw"), payload[chosen], context=context)
        # Currency is paired with an accepted canonical amount.
        currency = (_first_text(payload.get(f"{chosen}_currency")) or None) if estimated is not None else None
    return ProcurementEvent(
        event_id=context,
        procedure_id=f"placsp:procedure:{atom_id}",
        source="placsp",
        source_event_type="notice_snapshot",
        buyer_id=_first_text(payload.get("buyer_dir3")) or None,
        buyer_name=_first_text(payload.get("buyer")) or None,
        title=_first_text(payload.get("title")) or None,
        description=_first_text(payload.get("summary")) or None,
        cpv_codes=tuple(_as_code_list(payload.get("cpv"))),
        estimated_value=estimated,
        awarded_value=None,
        currency=currency,
        publication_date=None,
        source_updated_at=updated,
        deadline=None,
        status=_first_text(payload.get("status_code")) or None,
        nuts_code=_first_text(payload.get("nuts_code")) or None,
        country="ES",
        source_url=_first_text(payload.get("url")) or None,
        ingested_at=row["raw_retrieved_at"],
    )


def _boe_event(payload: dict[str, Any], row: dict[str, Any]) -> ProcurementEvent:
    """Map one BOE item as a contextual notice event without inferring a procedure."""

    item_id = _first_text(payload.get("item_id") or payload.get("identificador"))
    if not item_id:
        raise ValueError(f"BOE bronze record without an item id: {row['record_locator']}")
    record = normalize_boe(payload)
    return ProcurementEvent(
        event_id=f"boe:notice:{item_id}",
        procedure_id=None,
        source="boe",
        source_event_type="notice",
        buyer_name=record.buyer,
        title=record.title or None,
        description=record.summary or None,
        country="ES",
        publication_date=record.published_date,
        source_url=record.url,
        ingested_at=row["raw_retrieved_at"],
    )


def _placsp_tombstone_event(row: dict[str, Any]) -> ProcurementEvent:
    """Map one deletion control; it never deletes the prior historical rows."""

    if row["source"] != "placsp":
        raise ValueError(f"Unsupported tombstone source for canonical Silver: {row['source']!r}")
    ref = (row["source_record_id"] or "").strip()
    if not ref:
        raise ValueError(f"PLACSP tombstone without a full ref: {row['record_locator']}")
    return ProcurementEvent(
        event_id=f"placsp:tombstone:{ref}",
        procedure_id=f"placsp:procedure:{ref}",
        source="placsp",
        source_event_type="tombstone",
        ingested_at=row["raw_retrieved_at"],
    )


def _record_event(row: dict[str, Any]) -> ProcurementEvent:
    payload = json.loads(row["payload_json"])
    if row["source"] == "ted":
        return _ted_event(payload, row)
    if row["source"] == "placsp":
        return _placsp_event(payload, row)
    if row["source"] == "boe":
        return _boe_event(payload, row)
    raise ValueError(f"Unsupported bronze source for canonical Silver: {row['source']!r}")


def _selection_key(row: dict[str, Any]) -> tuple:
    """Deterministic observation order; ``source_member_index`` nulls sort last."""

    member_index = row["source_member_index"]
    return (
        row["raw_retrieved_at"],
        row["source_file"],
        (1, member_index) if member_index is not None else (2, 0),
        row["source_member"] or "",
        row["record_locator"] or "",
        row["raw_sha256"],
    )


def _canonical_fields(event: ProcurementEvent) -> dict[str, Any]:
    fields = event.to_row()
    fields.pop("ingested_at")
    return fields


def build_procurement_events_reference(records: pl.DataFrame, tombstones: pl.DataFrame) -> pl.DataFrame:
    """Build the canonical Silver frame from every accepted Bronze observation.

    Repeated observations of the same source event collapse to the row chosen
    by the deterministic minimum provenance tuple. Any other canonical
    difference under the same ``event_id`` fails explicitly instead of picking
    a latest or earliest payload. The result is sorted ascending by
    ``event_id``.
    """

    grouped: dict[str, list[tuple[ProcurementEvent, tuple]]] = {}
    for row in records.iter_rows(named=True):
        event = _record_event(row)
        grouped.setdefault(event.event_id, []).append((event, _selection_key(row)))
    for row in tombstones.iter_rows(named=True):
        event = _placsp_tombstone_event(row)
        grouped.setdefault(event.event_id, []).append((event, _selection_key(row)))

    events: list[ProcurementEvent] = []
    for event_id in sorted(grouped):
        observations = grouped[event_id]
        canonical = _canonical_fields(observations[0][0])
        for event, _ in observations[1:]:
            other = _canonical_fields(event)
            if other != canonical:
                differing = sorted(key for key, value in canonical.items() if other.get(key) != value)
                raise ValueError(
                    f"Conflicting canonical mappings for event_id {event_id!r}; "
                    f"differing fields: {', '.join(differing)}"
                )
        events.append(min(observations, key=lambda observation: observation[1])[0])
    return procurement_events_frame(events)
