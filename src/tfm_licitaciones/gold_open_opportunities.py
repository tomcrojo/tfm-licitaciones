"""Principal Gold dataset: open/actionable opportunities from current-state.

Boundary::

    canonical Silver Parquet
        -> current_state via ``tfm_licitaciones.current_state`` (PR #24, reused)
        -> open/actionable policy (this module; frozen below)
        -> open_opportunities Parquet + gold_manifest.json

**Grain:** one row per resolvable source ``procedure_id`` that can be
conservatively proven to be an open/actionable opportunity. The contract is
``gold_contract.GOLD_OPEN_OPPORTUNITIES_FIELDS``; this module never widens it.

This module composes the already-merged interfaces without reimplementing
them: current-state resolution (``current_state.build_current_state``),
CPV/DIR3 enrichment (``gold_enrichment.enrich_cpv`` /
``enrich_buyers_dir3``, metrics only) and typed IO
(``spark_foundation``). It contains no Silver semantics, no identity logic
and no ranking/ML.

Frozen open/actionable policy (conservative, deterministic)
-----------------------------------------------------------

Evidence inspection (see ``docs/gold_spark_contract.md`` for the full
record):

- ``is_deleted`` (PR #24: true exactly when the selected current-state
  event is a tombstone) is the only deletion signal. Deleted procedures are
  never opportunities.
- ``deadline`` is structurally always null in canonical Silver today (no
  TED/PLACSP/BOE mapping populates it); the column exists in the contract
  and the policy evaluates it whenever it is populated.
- ``status`` is structurally always null for TED and BOE. For PLACSP it
  carries the ``ContractFolderStatusCode`` lifecycle value, whose semantics
  are frozen by the official DGPE CODICE codelist
  ``SyndicationContractFolderStatusCode`` 2.04: only ``"PUB"`` ("EN PLAZO")
  proves an actionable bidding window. ``EV`` ("PENDIENTE DE ADJUDICACION")
  means the submission phase is finished and is never actionable. No
  cross-source taxonomy is invented. ``PRE`` ("Anuncio Previo") never opens:
  a prior notice does not prove bids can be submitted.
- ``ingested_at``, retrieval time, processing time, filenames, row order
  and part-file order are technical provenance and never business time.

Decision for one current-state row, in precedence order:

1. ``is_deleted`` is true -> ``deleted`` (never published).
2. ``deadline`` is not null, ``as_of`` is provided and
   ``deadline < as_of`` (strict, UTC; ``as_of`` is the start of the
   evaluation day) -> ``deadline_passed`` (not actionable, wins over any
   status text).
3. ``source == "placsp"``:
   - ``status == "PUB"`` -> ``open`` (official "EN PLAZO"; null deadlines
     do not block it).
   - ``status`` in ``{"EV", "ADJ", "ADJ_PAR", "RES", "RES_PAR", "ANUL"}`` ->
     ``status_closed`` (not actionable, wins over a stale future
     deadline).
   - ``"PRE"``, null or any other string -> ``insufficient_evidence``
     (observable, never silently opened; exact case-sensitive match, no
     normalization).
4. Any other source (TED/BOE/...):
   - ``status`` is not null -> ``insufficient_evidence`` (the contract says
     TED/BOE status is always null, so any value is unexpected taxonomy).
   - ``status`` is null, ``deadline`` is not null, ``as_of`` is provided
     and ``deadline >= as_of`` -> ``open`` (an explicit future deadline is
     direct actionability evidence requiring no taxonomy).
   - otherwise -> ``insufficient_evidence`` (in particular: null deadline
     with null status can never prove actionability).

Consequences: a null ``deadline`` never excludes by itself; without
``as_of`` no deadline is evaluated (production runs must pass ``--as-of``
for deadline enforcement); ``source_event_type``, ``awarded_value``,
``publication_date`` and ``source_updated_at`` are not openness signals.

Enrichment composition
----------------------

- CPV: the canonical ``cpv_codes`` array is preserved untouched in
  ``open_opportunities`` (no explosion, no collapsing, grain unchanged).
  ``enrich_cpv`` runs on the open set for measured resolved/unresolved
  metrics only; no ``opportunity_cpv`` secondary dataset is persisted
  because the array already carries every published code.
- DIR3: ``enrich_buyers_dir3`` runs on the open set for measured metrics
  only. Canonical ``buyer_id``/``buyer_name`` are never rewritten and a
  missing DIR3 dimension never blocks Gold (recorded as
  ``dir3.available=false``).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from .gold_contract import (
    CURRENT_STATE_ISSUES_SCHEMA_VERSION,
    CURRENT_STATE_SCHEMA_VERSION,
    GOLD_OPEN_OPPORTUNITIES_FIELDS,
    GOLD_OPEN_OPPORTUNITIES_SCHEMA_VERSION,
)
from .gold_enrichment import (
    BUYER_DIR3_ENRICHED_SCHEMA_VERSION,
    CPV_DIMENSION_FIELDS,
    CPV_ENRICHED_SCHEMA_VERSION,
    dir3_units_dimension,
    enrich_buyers_dir3,
    enrich_cpv,
    read_cpv_dimension,
    read_dir3_dimension,
)
from .spark_foundation import (
    assert_contract_schema,
    read_canonical_silver,
    schema_from_fields,
    write_typed_parquet,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


GOLD_OPEN_OPPORTUNITIES_DATASET = "open_opportunities"
GOLD_MANIFEST_FILENAME = "gold_manifest.json"

CPV_CODES_FILENAME = "cpv_codes.parquet"

# Official PLACSP lifecycle evidence: DGPE CODICE codelist
# SyndicationContractFolderStatusCode 2.04
# (https://contrataciondelestado.es/codice/cl/2.04/SyndicationContractFolderStatusCode-2.04.gc):
# PRE=Anuncio Previo, PUB=EN PLAZO, EV=PENDIENTE DE ADJUDICACION,
# ADJ=Adjudicada, RES=Resuelta, ANUL=Anulada. Only PUB ("EN PLAZO") proves
# an actionable bidding window. ADJ_PAR/RES_PAR (partial award/resolution
# variants seen in newer feeds) are closed by the same definition: the
# submission phase is finished. PRE stays insufficient evidence: a prior
# notice never proves that bids can be submitted.
PLACSP_OPEN_STATUSES = frozenset({"PUB"})
PLACSP_CLOSED_STATUSES = frozenset({"EV", "ADJ", "ADJ_PAR", "RES", "RES_PAR", "ANUL"})

PLACSP_SOURCE = "placsp"

DECISION_OPEN = "open"
DECISION_DELETED = "deleted"
DECISION_DEADLINE_PASSED = "deadline_passed"
DECISION_STATUS_CLOSED = "status_closed"
DECISION_INSUFFICIENT_EVIDENCE = "insufficient_evidence"

_DECISION_COLUMN = "___gold_decision"

# Manifest buckets: deadline/status-closed share "not actionable".
_DECISION_TO_BUCKET = {
    DECISION_DELETED: "excluded_deleted",
    DECISION_DEADLINE_PASSED: "excluded_not_actionable",
    DECISION_STATUS_CLOSED: "excluded_not_actionable",
    DECISION_INSUFFICIENT_EVIDENCE: "excluded_insufficient_evidence",
}


def open_opportunities_schema() -> "StructType":
    """Return the explicit Spark schema for ``open_opportunities``."""

    return schema_from_fields(GOLD_OPEN_OPPORTUNITIES_FIELDS)


def parse_as_of(value: str | date | datetime | None) -> datetime | None:
    """Normalize an evaluation instant to UTC midnight, or None when absent.

    Accepts ``None``, a ``datetime.date``, a tz-aware ``datetime`` (reduced
    to its UTC calendar day) or a ``"YYYY-MM-DD"`` string. Returns a
    tz-aware UTC midnight ``datetime`` used for deterministic deadline
    comparison, or ``None`` when no evaluation date applies (deadline gate
    disabled). Never consults the system clock.
    """

    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"as_of datetimes must carry a timezone: {value!r}")
        day = value.astimezone(timezone.utc).date()
        return datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        try:
            day = date.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(
                f"as_of must use the unequivocal YYYY-MM-DD format: {value!r}"
            ) from exc
        if len(text) != 10:
            raise ValueError(
                f"as_of must use the unequivocal YYYY-MM-DD format: {value!r}"
            )
        return datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    raise ValueError(f"unsupported as_of value: {value!r}")


def as_of_iso_day(moment: datetime | None) -> str | None:
    """Render a normalized ``as_of`` instant as ``YYYY-MM-DD`` (or None)."""

    return moment.date().isoformat() if moment is not None else None


def _guard_current_state(current: "DataFrame") -> None:
    """Enforce the current-state input contract and procedure uniqueness."""

    from pyspark.sql import functions as F

    from .current_state import current_state_schema

    assert_contract_schema(current, current_state_schema())
    duplicates = (
        current.groupBy("procedure_id").count().filter(F.col("count") > 1).limit(1).count()
    )
    if duplicates:
        offending = (
            current.groupBy("procedure_id")
            .count()
            .filter(F.col("count") > 1)
            .orderBy("procedure_id")
            .limit(1)
            .collect()[0]["procedure_id"]
        )
        raise ValueError(
            f"duplicate procedure_id at Gold boundary: {offending!r}"
        )


def _decide_column(current: "DataFrame", as_of: datetime | None):
    """Build the deterministic per-row decision expression (see policy)."""

    from pyspark.sql import functions as F

    def _lit_str(values: frozenset[str]) -> list:
        return [F.lit(value) for value in sorted(values)]

    deadline_past = (
        F.lit(as_of is not None)
        & F.col("deadline").isNotNull()
        & (F.col("deadline") < F.lit(as_of))
    )
    is_placsp = F.col("source") == F.lit(PLACSP_SOURCE)
    placsp_open = F.col("status").isin(*_lit_str(PLACSP_OPEN_STATUSES))
    placsp_closed = F.col("status").isin(*_lit_str(PLACSP_CLOSED_STATUSES))
    other_open = (
        F.col("status").isNull()
        & F.lit(as_of is not None)
        & F.col("deadline").isNotNull()
        & (F.col("deadline") >= F.lit(as_of))
    )
    return (
        F.when(F.col("is_deleted"), F.lit(DECISION_DELETED))
        .when(deadline_past, F.lit(DECISION_DEADLINE_PASSED))
        .when(
            is_placsp,
            F.when(placsp_open, F.lit(DECISION_OPEN))
            .when(placsp_closed, F.lit(DECISION_STATUS_CLOSED))
            .otherwise(F.lit(DECISION_INSUFFICIENT_EVIDENCE)),
        )
        .when(other_open, F.lit(DECISION_OPEN))
        .otherwise(F.lit(DECISION_INSUFFICIENT_EVIDENCE))
    )


def build_open_opportunities(
    current: "DataFrame",
    *,
    as_of: str | date | datetime | None = None,
) -> tuple["DataFrame", dict]:
    """Filter resolvable current-state rows to open/actionable opportunities.

    Returns the ``open_opportunities`` frame (frozen
    ``GOLD_OPEN_OPPORTUNITIES_FIELDS`` order) and a metrics dict with
    ``as_of``, per-decision counts and manifest buckets. Guards: input
    contract, unique ``procedure_id`` in and out, exact output schema, no
    deleted publication, and full row accounting
    (open + excluded == input).
    """

    from pyspark.sql import functions as F

    from .current_state import current_state_schema

    _guard_current_state(current)
    moment = parse_as_of(as_of)
    _, _, open_frame, metrics = _apply_policy(current, moment)
    return open_frame, metrics


def _apply_policy(
    current: "DataFrame", moment: datetime | None
) -> tuple["DataFrame", "DataFrame", "DataFrame", dict]:
    """Apply the frozen policy once; share between filter and full build.

    Returns ``(decided, open_current, open_frame, metrics)`` where
    ``open_current`` keeps the current-state schema (enrichment input) and
    ``open_frame`` keeps the frozen Gold schema (persisted output).
    """

    from pyspark.sql import functions as F

    from .current_state import current_state_schema

    decided = current.withColumn(_DECISION_COLUMN, _decide_column(current, moment))
    reason_counts, buckets = _summarize_decisions(decided)

    input_rows = current.count()
    accounted = reason_counts[DECISION_OPEN] + sum(buckets.values())
    if accounted != input_rows:
        raise ValueError(
            "Gold decision accounting mismatch: "
            f"input={input_rows} accounted={accounted} reasons={reason_counts}"
        )

    open_current = _open_frame(decided, current_state_schema())
    gold_columns = [field.name for field in GOLD_OPEN_OPPORTUNITIES_FIELDS]
    open_frame = open_current.select(*gold_columns)
    assert_contract_schema(open_frame, open_opportunities_schema())
    if open_frame.count() != reason_counts[DECISION_OPEN]:
        raise ValueError("Gold projection changed the open row count")
    open_duplicates = (
        open_frame.groupBy("procedure_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if open_duplicates:
        raise ValueError("duplicate procedure_id in open_opportunities output")

    metrics = {
        "as_of": as_of_iso_day(moment),
        "current_state_rows": input_rows,
        "open_opportunities_rows": reason_counts[DECISION_OPEN],
        "reasons": dict(reason_counts),
        **buckets,
    }
    return decided, open_current, open_frame, metrics


def _summarize_decisions(decided: "DataFrame") -> tuple[dict, dict]:
    """Count policy decisions and map them to manifest buckets."""

    reason_counts = {
        row[_DECISION_COLUMN]: row["count"]
        for row in decided.groupBy(_DECISION_COLUMN).count().collect()
    }
    for decision in (
        DECISION_OPEN,
        DECISION_DELETED,
        DECISION_DEADLINE_PASSED,
        DECISION_STATUS_CLOSED,
        DECISION_INSUFFICIENT_EVIDENCE,
    ):
        reason_counts.setdefault(decision, 0)
    buckets = {
        "excluded_deleted": 0,
        "excluded_not_actionable": 0,
        "excluded_insufficient_evidence": 0,
    }
    for decision, total in reason_counts.items():
        if decision == DECISION_OPEN:
            continue
        buckets[_DECISION_TO_BUCKET[decision]] += total
    return reason_counts, buckets


def _open_frame(decided: "DataFrame", expected_schema: "StructType") -> "DataFrame":
    """Return the open rows of a decided frame in the input contract schema."""

    from pyspark.sql import functions as F

    open_rows = decided.filter(F.col(_DECISION_COLUMN) == F.lit(DECISION_OPEN)).drop(
        _DECISION_COLUMN
    )
    assert_contract_schema(open_rows, expected_schema)
    return open_rows


def resolve_cpv_dimension(
    spark: "SparkSession",
    reference_dir: str | Path | None,
    explicit: "DataFrame | None" = None,
) -> tuple["DataFrame | None", dict]:
    """Resolve the CPV reference dimension, rebuilding from CSV if needed.

    Returns ``(dimension_or_None, info)`` where ``info`` carries
    ``available`` and ``dimension_source`` (``parquet`` or ``csv-rebuild``).
    Unlike DIR3, CPV is required for measured Gold metrics because its
    source CSV is committed in the repository; a completely missing
    dimension fails loudly instead of publishing decorative metrics.
    """

    if explicit is not None:
        return explicit, {"available": True, "dimension_source": "explicit"}
    if reference_dir is None:
        raise ValueError(
            "no CPV dimension supplied and no reference_dir to resolve it from"
        )
    parquet_path = Path(reference_dir) / CPV_CODES_FILENAME
    if parquet_path.is_file():
        return read_cpv_dimension(spark, parquet_path), {
            "available": True,
            "dimension_source": "parquet",
        }
    from .cpv import DEFAULT_CPV_SOURCE, build_cpv_dimension

    if not Path(DEFAULT_CPV_SOURCE).is_file():
        raise ValueError(
            f"CPV dimension not found at {parquet_path} and "
            f"source CSV {DEFAULT_CPV_SOURCE} is absent"
        )
    frame = build_cpv_dimension(DEFAULT_CPV_SOURCE)
    rows = frame.to_dicts()
    dimension = spark.createDataFrame(rows, schema=schema_from_fields(CPV_DIMENSION_FIELDS))
    return dimension, {"available": True, "dimension_source": "csv-rebuild"}


def resolve_dir3_dimension(
    spark: "SparkSession",
    reference_dir: str | Path | None,
    explicit: "DataFrame | None" = None,
) -> tuple["DataFrame | None", dict]:
    """Resolve the DIR3 dimension, returning None when it is not built.

    A missing DIR3 dimension never blocks Gold: the caller records
    ``available=false`` and keeps the canonical buyer identity.
    """

    if explicit is not None:
        return explicit, {"available": True, "dimension_source": "explicit"}
    if reference_dir is None:
        return None, {"available": False, "dimension_source": None}
    path = dir3_units_dimension(reference_dir)
    if path is None:
        return None, {"available": False, "dimension_source": None}
    return read_dir3_dimension(spark, path), {
        "available": True,
        "dimension_source": "parquet",
    }


def build_gold_from_silver(
    spark: "SparkSession",
    silver_path: str | Path,
    output_dir: str | Path,
    *,
    reference_dir: str | Path | None = None,
    cpv_dimension: "DataFrame | None" = None,
    dir3_dimension: "DataFrame | None" = None,
    as_of: str | date | datetime | None = None,
    open_opportunities_name: str = GOLD_OPEN_OPPORTUNITIES_DATASET,
    manifest_name: str = GOLD_MANIFEST_FILENAME,
) -> dict:
    """Build ``open_opportunities`` from canonical Silver Parquet.

    Chain: ``read_canonical_silver`` -> ``build_current_state`` (reused) ->
    open/actionable policy -> CPV/DIR3 enrichment metrics (reused) ->
    typed ``open_opportunities`` Parquet + minimal ``gold_manifest.json``.
    Enrichment never changes the Gold grain: CPV arrays stay canonical and
    DIR3 attributes are metrics-only. Returns output paths and row counts.
    """

    from .current_state import build_current_state

    silver = read_canonical_silver(spark, silver_path)
    current, issues = build_current_state(silver)
    current_rows = current.count()
    issues_rows = issues.count()

    moment = parse_as_of(as_of)
    _guard_current_state(current)
    _, open_current, open_frame, decision_metrics = _apply_policy(current, moment)

    cpv_dimension, cpv_info = resolve_cpv_dimension(spark, reference_dir, cpv_dimension)
    _, cpv_metrics = enrich_cpv(open_current, cpv_dimension)
    cpv_section = {**cpv_info, **cpv_metrics}

    dir3_dimension, dir3_info = resolve_dir3_dimension(spark, reference_dir, dir3_dimension)
    if dir3_dimension is None:
        dir3_section: dict = {
            **dir3_info,
            "input_events": open_frame.count(),
            "resolved_events": None,
            "unresolved_events_with_buyer_id": None,
        }
    else:
        _, dir3_metrics = enrich_buyers_dir3(open_current, dir3_dimension)
        dir3_section = {**dir3_info, **dir3_metrics}

    output_root = Path(output_dir)
    open_path = output_root / open_opportunities_name
    manifest_path = output_root / manifest_name
    write_typed_parquet(
        open_frame,
        open_path,
        expected_schema=open_opportunities_schema(),
        order_by=("procedure_id",),
    )
    open_rows = open_frame.count()

    manifest = {
        "gold_open_opportunities_schema_version": GOLD_OPEN_OPPORTUNITIES_SCHEMA_VERSION,
        "current_state_schema_version": CURRENT_STATE_SCHEMA_VERSION,
        "current_state_issues_schema_version": CURRENT_STATE_ISSUES_SCHEMA_VERSION,
        "cpv_enriched_schema_version": CPV_ENRICHED_SCHEMA_VERSION,
        "buyer_dir3_enriched_schema_version": BUYER_DIR3_ENRICHED_SCHEMA_VERSION,
        "as_of": decision_metrics["as_of"],
        "counts": {
            "current_state": current_rows,
            "current_state_issues": issues_rows,
            "open_opportunities": open_rows,
            "excluded_deleted": decision_metrics["excluded_deleted"],
            "excluded_not_actionable": decision_metrics["excluded_not_actionable"],
            "excluded_insufficient_evidence": decision_metrics[
                "excluded_insufficient_evidence"
            ],
        },
        "reasons": decision_metrics["reasons"],
        "cpv": cpv_section,
        "dir3": dir3_section,
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    if (
        manifest["counts"]["open_opportunities"]
        + manifest["counts"]["excluded_deleted"]
        + manifest["counts"]["excluded_not_actionable"]
        + manifest["counts"]["excluded_insufficient_evidence"]
        != manifest["counts"]["current_state"]
    ):
        raise ValueError("Gold manifest counts do not account for every current-state row")
    return {
        "open_opportunities": str(open_path),
        "manifest": str(manifest_path),
        "current_state_rows": current_rows,
        "current_state_issues_rows": issues_rows,
        "open_opportunities_rows": open_rows,
    }
