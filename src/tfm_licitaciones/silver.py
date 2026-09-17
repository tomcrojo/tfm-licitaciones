"""Canonical Silver procurement events: hybrid contract facade and routing.

One row is one source-published notice snapshot/state transition or deletion
control. History is never folded here: revision folding and legacy tombstone
deletes remain exclusive to the temporary in-memory ``TenderRecord`` path that
Gold 0.1 still consumes (see `pipeline.fold_latest_updates`).

Tombstones are unordered deletion evidence. They become canonical events but
cannot derive an authoritative current state: the current Bronze contract does
not retain the Atom ``when`` attribute, so repeated delete/re-publish/delete
cycles for the same ref are indistinguishable and order is never invented from
retrieval time.

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
    "original frames (selection before execution, never a try/except fallback)"
)

_LOGGER = logging.getLogger(__name__)


def _select_route(eligibility: NativeEligibility) -> str:
    """Name the executed route for logs; a fallback is never 'native'."""

    return "native" if eligibility.eligible else "reference-fallback"


def build_procurement_events(records: pl.DataFrame, tombstones: pl.DataFrame) -> pl.DataFrame:
    """Build the canonical Silver frame from every accepted Bronze observation.

    Repeated observations of the same source event collapse to the row chosen
    by the deterministic minimum provenance tuple. Any other canonical
    difference under the same ``event_id`` fails explicitly instead of picking
    a latest or earliest payload. The result is sorted ascending by
    ``event_id``.

    The eligibility guard inspects the complete batch first. Eligible batches
    run the native Polars kernel (:mod:`tfm_licitaciones.silver_native`);
    every other batch runs the frozen python-row reference
    (:mod:`tfm_licitaciones.silver_reference`) over the *original* frames, so
    the historical values, first-error ordering and messages are preserved.
    """

    eligibility = assess_native_eligibility(records, tombstones)
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
        return build_procurement_events_native(records, tombstones)
    return build_procurement_events_reference(records, tombstones)
