"""Canonical Silver procurement events: hybrid contract facade and routing.

One row is one source-published notice snapshot/state transition or deletion
control. History is never folded here: revision folding and legacy tombstone
deletes remain exclusive to the temporary in-memory ``TenderRecord`` path that
Gold 0.1 still consumes (see `pipeline.fold_latest_updates`).

PLACSP tombstones retain the source-published Atom ``when`` instant when it is
valid and timezone-aware. Dated delete controls therefore receive distinct
canonical event identities and expose that instant through ``source_updated_at``;
missing source time remains explicit unordered evidence, while malformed
published ``when`` values are also surfaced as Bronze control errors. Downstream
current-state logic can therefore order source-dated deletes without inventing
time from ingestion provenance. Canonical timestamp precision is microseconds
(``timestamp[us, UTC]``), matching the existing Silver schema and Parquet
boundary; finer source fractions, if ever published, collapse to that precision.

Production routing is hybrid by construction:

    complete Bronze batch -> conservative eligibility guard
        -> native Polars kernel for the whole batch, or
        -> frozen python-row reference for the whole batch.

:mod:`tfm_licitaciones.silver_guard` inspects every original row (records and
tombstones) *before* execution and admits only the narrow eligible domain
documented there. Both execution routes emit the same canonical contract
(``PROCUREMENT_EVENT_SCHEMA``, identity, revisions, tombstones, collision
semantics, provenance selection, ``Decimal(20,2)`` amounts and UTC
timestamps); a fallback run is never labelled as native. The routing decision
is logged once per batch (route, fallback reason category and row counts).

The frozen reference predates source-dated tombstones and stays untouched.
After route selection, the facade gives each guard-compatible tombstone a
temporary, injective routing identity. Dated and undated controls use disjoint
internal namespaces, so an arbitrary published ref cannot collide with the
source-time encoding. After either route, the facade restores canonical
tombstone event ids, the published ref as ``procedure_id`` and the
authoritative source time. Tombstone rows outside the guard's reference domain
are never decorated, preserving historical failure/value semantics and
first-error behavior on the fallback route.

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
    "the documented domain; otherwise the frozen python-row reference after "
    "selection (never a try/except fallback); guard-compatible PLACSP tombstones "
    "receive injective internal routing identities and canonical source-time "
    "enrichment without changing the frozen reference"
)

_TOMBSTONE_SOURCE_TIME = "source_deleted_at"
_ROUTED_UNDATED_PREFIX = "__tfm_tombstone_undated_v1__:"
_ROUTED_DATED_PREFIX = "__tfm_tombstone_dated_v1__:"
_CANONICAL_DATED_PREFIX = "placsp:tombstone-dated:"
_DIVERGENT_WHITESPACE_PATTERN = r"[\x1c-\x1f]"
_LOGGER = logging.getLogger(__name__)


def _select_route(eligibility: NativeEligibility) -> str:
    """Name the executed route for logs; a fallback is never 'native'."""

    return "native" if eligibility.eligible else "reference-fallback"


def _prepare_tombstone_identities(
    tombstones: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    """Route guard-compatible tombstones through an injective namespace.

    The frozen builders derive tombstone identity from ``source_record_id``.
    Every guard-compatible PLACSP tombstone is therefore decorated after the
    eligibility decision: undated and dated controls occupy disjoint internal
    namespaces, and dated identities put the semantic discriminator before the
    arbitrary source ref. Rows the guard rejects for source/ref semantics are
    left byte-for-byte unchanged so reference fallback sees the original input.
    """

    if _TOMBSTONE_SOURCE_TIME not in tombstones.columns or tombstones.height == 0:
        return tombstones, None

    original_ref = pl.col("source_record_id")
    source_ref = original_ref.fill_null("").str.strip_chars()
    source_time = pl.col(_TOMBSTONE_SOURCE_TIME)
    micros = source_time.cast(pl.Int64).cast(pl.String)
    guard_compatible = (
        (pl.col("source") == "placsp")
        & original_ref.is_not_null()
        & ~original_ref.str.contains(_DIVERGENT_WHITESPACE_PATTERN)
        & (source_ref != "")
    )

    routed_ref = (
        pl.when(source_time.is_not_null())
        .then(pl.lit(_ROUTED_DATED_PREFIX) + micros + pl.lit(":") + source_ref)
        .otherwise(pl.lit(_ROUTED_UNDATED_PREFIX) + source_ref)
    )
    routed = tombstones.with_columns(
        pl.when(guard_compatible)
        .then(routed_ref)
        .otherwise(original_ref)
        .alias("source_record_id")
    )

    canonical_event_id = (
        pl.when(source_time.is_not_null())
        .then(pl.lit(_CANONICAL_DATED_PREFIX) + micros + pl.lit(":") + source_ref)
        .otherwise(pl.lit("placsp:tombstone:") + source_ref)
    )
    enrichment = (
        tombstones.filter(guard_compatible)
        .select(
            event_id=pl.lit("placsp:tombstone:") + routed_ref,
            __canonical_event_id=canonical_event_id,
            __procedure_id=pl.lit("placsp:procedure:") + source_ref,
            __source_updated_at=source_time,
        )
        .unique(subset=["event_id"], maintain_order=True)
    )
    return routed, enrichment


def _apply_tombstone_source_time(events: pl.DataFrame, enrichment: pl.DataFrame | None) -> pl.DataFrame:
    """Restore canonical tombstone identity and source-time fields after routing."""

    if enrichment is None or enrichment.height == 0:
        return events
    return (
        events.join(enrichment, on="event_id", how="left")
        .with_columns(
            event_id=pl.coalesce(pl.col("__canonical_event_id"), pl.col("event_id")),
            procedure_id=pl.coalesce(pl.col("__procedure_id"), pl.col("procedure_id")),
            source_updated_at=pl.coalesce(pl.col("__source_updated_at"), pl.col("source_updated_at")),
        )
        .drop("__canonical_event_id", "__procedure_id", "__source_updated_at")
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
    Undated tombstones retain the historical ``placsp:tombstone:<ref>`` event
    identity. Dated events use the disjoint
    ``placsp:tombstone-dated:<epoch-us>:<ref>`` namespace.

    The eligibility guard inspects the complete *original* batch first.
    Eligible batches run the native Polars kernel
    (:mod:`tfm_licitaciones.silver_native`); every other batch runs the frozen
    python-row reference (:mod:`tfm_licitaciones.silver_reference`). Only after
    that route decision are guard-compatible tombstone refs replaced by
    injective internal identities required to express source-dated deletion
    cycles. Guard-rejected rows remain unchanged, preserving reference errors,
    values and ordering.
    """

    eligibility = assess_native_eligibility(records, tombstones)
    routed_tombstones, enrichment = _prepare_tombstone_identities(tombstones)
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
