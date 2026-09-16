"""Canonical Silver procurement events: contract facade and engine wiring.

One row is one source-published notice snapshot/state transition or deletion
control. History is never folded here: revision folding and legacy tombstone
deletes remain exclusive to the temporary in-memory ``TenderRecord`` path that
Gold 0.1 still consumes (see `pipeline.fold_latest_updates`).

Tombstones are unordered deletion evidence. They become canonical events but
cannot derive an authoritative current state: the current Bronze contract does
not retain the Atom ``when`` attribute, so repeated delete/re-publish/delete
cycles for the same ref are indistinguishable and order is never invented from
retrieval time.

Engine decision (measured, see ``docs/benchmarks.md`` and the Silver engine
comparison evidence curated under ``experiments/silver_engine_comparison/``):
Polars is the default canonical Silver engine inside the measured
single-node envelope. The
canonical input/output contract — identity, revisions, tombstones, collision
semantics, provenance selection, Decimal(20,2), UTC timestamps and
``PROCUREMENT_EVENT_SCHEMA`` — is engine-neutral by construction:
:mod:`tfm_licitaciones.silver_reference` freezes the python-row semantics as
the parity oracle, :mod:`tfm_licitaciones.silver_native` is the production
engine, and a PySpark candidate is retained as an experimental scale-out
path (see ``experiments/``) to be productionized separately if larger
deployments justify it. That candidate's measured parity covers only the
synthetic TED/PLACSP experimental contract (scalar payloads, CPV string
lists, whole-second instants), not the full production contract (BOE,
subsecond instants).
"""

from __future__ import annotations

import polars as pl

from .silver_native import IMPLEMENTATION as _NATIVE_IMPLEMENTATION
from .silver_native import IMPLEMENTATION_DETAIL
from .silver_native import build_procurement_events_native

PROCUREMENT_EVENTS_FILENAME = "procurement_events.parquet"
LEGACY_TENDERS_FILENAME = "tenders.jsonl"

IMPLEMENTATION = _NATIVE_IMPLEMENTATION
"""Identifier of the production engine currently backing ``run``."""


def build_procurement_events(records: pl.DataFrame, tombstones: pl.DataFrame) -> pl.DataFrame:
    """Build the canonical Silver frame from every accepted Bronze observation.

    Repeated observations of the same source event collapse to the row chosen
    by the deterministic minimum provenance tuple. Any other canonical
    difference under the same ``event_id`` fails explicitly instead of picking
    a latest or earliest payload. The result is sorted ascending by
    ``event_id``.

    The production engine is native Polars (:mod:`tfm_licitaciones.silver_native`);
    the frozen python-row semantics live in :mod:`tfm_licitaciones.silver_reference`
    and parity between the two is enforced by ``tests/test_silver_native.py``.
    """

    return build_procurement_events_native(records, tombstones)
