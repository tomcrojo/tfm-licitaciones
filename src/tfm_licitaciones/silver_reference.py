"""Frozen python-row reference implementation of the canonical Silver contract.

This module preserves, unchanged, the per-row Python semantics that
:mod:`tfm_licitaciones.silver` shipped before the native Polars engine became
the production default. It is retained as the parity oracle for tests and for
the engine benchmarks: :mod:`tfm_licitaciones.bench_silver` measures it as the
``python-row`` baseline, and
``tests/test_silver_native.py`` asserts that the production native engine
produces exactly the same frames and the same explicit failures.

It is NOT used by the production ``run`` pipeline. Do not refactor it into the
native engine or "improve" its semantics: its value is being frozen.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import polars as pl

from .models import ProcurementEvent, procurement_events_frame
from .normalize import (
    _as_code_list,
    _first_text,
    _normalize_amount_text,
    _parse_amount,
    normalize_boe,
    normalize_ted,
)

IMPLEMENTATION = "python-row"

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
