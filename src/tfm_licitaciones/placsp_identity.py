"""Reference rule for PLACSP notice/tombstone procedure identity.

Canonical Silver already stores OpenPLACSP notice ``atom:id`` values and Atom
Tombstone ``at:deleted-entry/@ref`` values in the same source-namespaced
``procedure_id`` shape. RFC 6721 section 3 defines ``ref`` as the value of the
``atom:id`` of the removed entry, so exact published-URI equality is the
identity bridge. No URL normalization, fuzzy matching or ingestion-order
heuristic belongs here.

This module is deliberately a small engine-neutral oracle. Spark current-state
code may group on the canonical ``procedure_id`` directly; these helpers exist
to make the identity assumption explicit, testable and observable when a
future/eventually malformed canonical row does not satisfy it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


PLACSP_PROCEDURE_PREFIX = "placsp:procedure:"
PLACSP_NOTICE_EVENT_PREFIX = "placsp:notice:"
PLACSP_TOMBSTONE_EVENT_PREFIX = "placsp:tombstone:"
PLACSP_DATED_TOMBSTONE_EVENT_PREFIX = "placsp:tombstone-dated:"

PLACSP_IDENTITY_RULE = "exact-rfc6721-ref-equals-atom-id"
PLACSP_IDENTITY_EVIDENCE = "RFC 6721 section 3: deleted-entry ref equals removed atom:id"

IdentityStatus = Literal["resolved", "unresolved"]


@dataclass(frozen=True)
class PlacspIdentityResolution:
    """Explain whether one canonical PLACSP event has a usable procedure key."""

    status: IdentityStatus
    procedure_id: str | None
    published_ref: str | None
    reason: str
    evidence: str | None = None


def _published_ref(procedure_id: str | None) -> str | None:
    if not isinstance(procedure_id, str) or not procedure_id.startswith(PLACSP_PROCEDURE_PREFIX):
        return None
    value = procedure_id[len(PLACSP_PROCEDURE_PREFIX) :]
    return value or None


def _dated_tombstone_matches(event_id: str, published_ref: str) -> bool:
    suffix = f":{published_ref}"
    if not event_id.startswith(PLACSP_DATED_TOMBSTONE_EVENT_PREFIX) or not event_id.endswith(suffix):
        return False
    marker = event_id[len(PLACSP_DATED_TOMBSTONE_EVENT_PREFIX) : -len(suffix)]
    return bool(marker) and marker.lstrip("-").isdigit()


def resolve_placsp_identity(
    *,
    event_id: str,
    procedure_id: str | None,
    source_event_type: str,
) -> PlacspIdentityResolution:
    """Validate the canonical PLACSP identity bridge without rewriting it.

    ``resolved`` means that the event identity and the existing canonical
    ``procedure_id`` agree on one exact published Atom URI. ``unresolved`` is
    returned for missing/malformed procedure keys, unsupported future event
    types or an event/procedure mismatch. The function never synthesizes a
    replacement key.
    """

    published_ref = _published_ref(procedure_id)
    if published_ref is None:
        return PlacspIdentityResolution(
            status="unresolved",
            procedure_id=procedure_id,
            published_ref=None,
            reason="missing_or_invalid_procedure_id",
        )

    if source_event_type == "notice_snapshot":
        matches = event_id.startswith(f"{PLACSP_NOTICE_EVENT_PREFIX}{published_ref}@")
        reason = "notice_atom_id_exact"
    elif source_event_type == "tombstone":
        matches = (
            event_id == f"{PLACSP_TOMBSTONE_EVENT_PREFIX}{published_ref}"
            or _dated_tombstone_matches(event_id, published_ref)
        )
        reason = "tombstone_ref_exact"
    else:
        return PlacspIdentityResolution(
            status="unresolved",
            procedure_id=procedure_id,
            published_ref=published_ref,
            reason="unsupported_placsp_event_type",
        )

    if not matches:
        return PlacspIdentityResolution(
            status="unresolved",
            procedure_id=procedure_id,
            published_ref=published_ref,
            reason="event_procedure_identity_mismatch",
        )

    return PlacspIdentityResolution(
        status="resolved",
        procedure_id=procedure_id,
        published_ref=published_ref,
        reason=reason,
        evidence=PLACSP_IDENTITY_EVIDENCE,
    )
