"""Canonical Silver procurement events: hybrid contract facade and routing.

One row is one source-published notice snapshot/state transition or deletion
control. History is never folded here: revision folding and legacy tombstone
deletes remain exclusive to the temporary in-memory ``TenderRecord`` path that
Gold 0.1 still consumes (see `pipeline.fold_latest_updates`).

PLACSP tombstones retain the source-published Atom ``when`` instant when it is
valid and timezone-aware. Dated delete controls therefore receive distinct
canonical event identities and expose that instant through ``source_updated_at``;
undated/invalid legacy controls remain explicit unordered evidence instead of
borrowing retrieval time. This allows downstream current-state logic to order
source-dated deletes without inventing time from ingestion provenance.

Production routing is hybrid by construction:

    complete Bronze batch -> conservative eligibility guard
        -> native Polars kernel for the whole batch, or
        -> frozen python-row reference for the whole batch.

:mod:`tfm_licitaciones.silver_guard` inspects every row (records and
tombstones) *before* execution and admits only the narrow eligible domain
documented there; one ineligible row routes the original frames to
:mod:`tfm_licitaciones.silver_reference`, which is also the historical parity
oracle. Both routes emit the same canonical contract
(``PROCUREMENT_EVENT_SCHEMA``, identity, revisions, tombstones, collision
semantics, provenance selection, ``Decimal(20,2)`` amounts and UTC
timestamps); a fallback run is never labelled as native. The routing decision
is logged once per batch (route, fallback reason category and row counts).

The frozen reference predates source-dated tombstones and stays untouched.
Before routing, the facade decorates only dated tombstone refs with their
source-time microsecond epoch so the historical builders preserve repeated
delete cycles as distinct events. After either route, the facade restores the
published ref as ``procedure_id`` and attaches ``source_updated_at``. Undated
rows keep the historical identity and semantics.

A PySpark candidate is retained as an experimental scale-out path (see
``experiments/``) to be productionized separately if larger deployments
justify it; its measured parity covers only the synthetic TED/PLACSP
experimental contract (scalar payloads, CPV string lists, whole-second
instants), not the full production contract.
"""

from __future__ import annotations

import logging

import polars as pl

from .silver_guard import NativeEligibility, assess_native_eligibility
from .silver_native import IMPLEMENTATION_DETAIL as _NATIVE_KERNEL_DETAIL
from .silver_native import build_procurement_events_native
from .silver_reference import build_procurement_events_reference

PROCUREMENT_EVENTS_FILENAME = "procurement_events.parquet"
LEGACY_TENDERS_FILENAME = "tenders.jsonl"

IMPLEMENTATION = "hybrid-native-reference"
"""Identifier of the production route: guarded native kernel or exact fallback."""

IMPLEMENTATION_DETAIL = (
    f"eligibility-guarded hybrid: {_NATIVE_KERNEL_DETAIL} for batches inside "
    "the documented domain; otherwise the frozen python-row reference over the "
    "original frames (selection before execution, never a try/except fallback); "
    "dated PLACSP tombstones are identity-decorated before either route and "
    "source-time enriched afterwards without changing the frozen reference"
)

_TOMBSTONE_SOURCE_TIME = "source_deleted_at"
_TOMBSTONE_EVENT_MARKER = "@deleted-us:"
_LOGGER = logging.getLogger(__name__)


def _select_route(eligibility: NativeEligibility) -> str:
    """Name the executed route for logs; a fallback is never 'native'."""

    return "native" if eligibility.eligible else "reference-fallback"


def _prepare_dated_tombstones(tombstones: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    """Give source-dated tombstones distinct identities without moving the oracle.

    The frozen builders derive tombstone identity from ``source_record_id``.
    For dated controls only, route a temporary decorated ref through that
    historical code. A compact lookup then restores the published ref as the
    procedure identity and exposes the authoritative Atom ``when`` instant.
    """

    if _TOMBSTONE_SOURCE_TIME not in tombstones.columns:
        return tombstones, None
    timed = tombstones.filter(pl.col(_TOMBSTONE_SOURCE_TIME).is_not_null())
    if timed.height == 0:
        return tombstones, None

    source_ref = pl.col("source_record_id")
    marker = pl.col(_TOMBSTONE_SOURCE_TIME).cast(pl.Int64).cast(pl.String)
    decorated_ref = source_ref + pl.lit(_TOMBSTONE_EVENT_MARKER) + marker
    routed = tombstones.with_columns(
        pl.when(pl.col(_TOMBSTONE_SOURCE_TIME).is_not_null())
        .then(decorated_ref)
        .otherwise(source_ref)
        .alias("source_record_id")
    )
    enrichment = (
        timed.select(
            event_id=pl.lit("placsp:tombstone:") + decorated_ref,
            __procedure_id=pl.lit("placsp:procedure:") + source_ref,
            __source_updated_at=pl.col(_TOMBSTONE_SOURCE_TIME),
        )
        .unique(subset=["event_id"], maintain_order=True)
    )
    return routed, enrichment


def _apply_tombstone_source_time(events: pl.DataFrame, enrichment: pl.DataFrame | None) -> pl.DataFrame:
    """Attach source time/procedure identity after either historical route."""

    if enrichment is None or enrichment.height == 0:
        return events
    return (
        events.join(enrichment, on="event_id", how="left")
        .with_columns(
            procedure_id=pl.coalesce(pl.col("__procedure_id"), pl.col("procedure_id")),
            source_updated_at=pl.coalesce(pl.col("__source_updated_at"), pl.col("source_updated_at")),
        )
        .drop("__procedure_id", "__source_updated_at")
        .sort("event_id")
    )


def build_procurement_events(records: pl.DataFrame, tombstones: pl.DataFrame) -> pl.DataFrame:
    """Build the canonical Silver frame from every accepted Bronze observation.

    Repeated observations of the same source event collapse to the row chosen
    by the deterministic minimum provenance tuple. Any other canonical
    difference under the same ``event_id`` fails explicitly instead of picking
    a latest or earliest payload. The result is sorted ascending by
    ``event_id``.

    Dated PLACSP tombstones are distinct source events keyed by their published
    delete instant. Repeated observations of the same ref/instant still
    collapse by provenance; separate delete instants remain separate history.

    The eligibility guard inspects the complete batch first. Eligible batches
    run the native Polars kernel (:mod:`tfm_licitaciones.silver_native`);
    every other batch runs the frozen python-row reference
    (:mod:`tfm_licitaciones.silver_reference`) over the *original* record frame
    and the semantically equivalent decorated tombstone frame, so historical
    values, first-error ordering and messages are preserved while the frozen
    oracle itself remains unchanged.
    """

    routed_tombstones, enrichment = _prepare_dated_tombstones(tombstones)
    eligibility = assess_native_eligibility(records, routed_tombstones)
    _LOGGER.info(
        "silver_batch_route",
        extra={
            "route": _select_route(eligibility),
            "fallback_reason": eligibility.reason,
            "records": records.height,
            "tombstones": tombstones.height,
        },
    )
    if eligibility.eligible:
        events = build_procurement_events_native(records, routed_tombstones)
    else:
        events = build_procurement_events_reference(records, routed_tombstones)
    return _apply_tombstone_source_time(events, enrichment)
