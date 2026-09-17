"""Offline Bronze-Parquet-to-Silver-Parquet benchmark harness.

Baseline under measurement: a python-row baseline over a Polars/Parquet
boundary. The current canonical implementation receives Polars frames but
executes per-row Python: ``iter_rows``, one ``json.loads`` per row,
``ProcurementEvent`` object allocation, Python dict grouping with
dedup/collision comparison, and a Python sort. This module must keep naming
that accurately: it is not a vectorized Polars execution.

The manual scale path never holds the corpus in memory. Dataset generation
streams logical rows through a bounded buffer and writes deterministic Bronze
Parquet part files incrementally::

    <work_dir>/dataset.json
    <work_dir>/records/part-00000.parquet ...
    <work_dir>/tombstones/part-00000.parquet ...

That partitioned Bronze input is reusable unchanged by a future Spark
benchmark. The measured stage (Bronze Parquet read, canonical transform,
Silver Parquet write) runs in an isolated spawn child process. The reported
``peak_rss_bytes`` is the child process lifetime high-water mark: it
excludes the parent process and dataset generation, but it still includes
child interpreter, module-import and runtime startup before the transform
timer. Dataset generation is timed separately and excluded from the
measurement.

No network access and no repository inputs are required. Nothing is written
inside the repository: the dataset lives in an explicit ``--work-dir`` (or a
temporary directory that is discarded) and metrics are returned as a dict,
persisted only to an explicit ``--output`` path. No performance result
exists until this harness is actually run and its JSON metrics retained.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import os
import platform
import sys
import tempfile
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Empty as QueueEmpty
from typing import Any

import polars as pl

from .bronze import BRONZE_SCHEMA, TOMBSTONE_SCHEMA, bronze_frame, tombstone_frame
from .silver_reference import build_procurement_events_reference as build_procurement_events

UTC = timezone.utc
BASE_RETRIEVED_AT = datetime(2026, 3, 2, 9, 0, 0, tzinfo=UTC)

TED_CPVS = ("72415000", "48662000", "30200000", "72266000", "48000000")
PLACSP_CPVS = ("72415000", "79100000", "90500000", "30200000")
UPDATED_STYLES = ("offset", "zulu", "utc", "naive", "invalid", "missing")

# Fixed edge-case records always appended to a generated dataset. They use
# unique synthetic identities so they never collide with bulk records. Every
# value here survives both the retained-text path and the legacy float
# fallback (``str(float)``); the decimal(20,2) upper boundary is covered by
# dedicated records below because ``float("999999999999999999.99")`` cannot
# round-trip through ``str``.
EDGE_DECIMALS = ("0.01", "100.00", "1.10", "2500.50", "123456789012.34")
MAX_DECIMAL_TEXT = "999999999999999999.99"

IMPLEMENTATION = "python-row"
IMPLEMENTATION_DETAIL = (
    "per-row Python over a Polars/Parquet frame boundary: iter_rows, "
    "json.loads per row, ProcurementEvent allocation, dict grouping with "
    "dedup/collision comparison, Python sort"
)
STAGE_DESCRIPTION = (
    "bronze-parquet-read -> build_procurement_events -> silver-parquet-write, "
    "run in an isolated spawn child process; dataset generation is timed "
    "separately and excluded"
)
PEAK_RSS_SCOPE = (
    "child-process lifetime high-water mark (ru_maxrss at child exit): "
    "excludes the parent process and dataset generation, but includes child "
    "interpreter, module-import and runtime startup before the transform timer"
)
RECORDS_DIRNAME = "records"
TOMBSTONES_DIRNAME = "tombstones"
SILVER_FILENAME = "procurement_events.parquet"
DATASET_FILENAME = "dataset.json"

PROFILES: dict[str, dict[str, int]] = {
    # Small enough for unit tests and CI (hundreds of rows, milliseconds).
    "tiny": {"n_ted": 120, "n_placsp": 120, "rows_per_part": 50},
    # Seconds-scale local run for quick comparisons (~25k input rows).
    "small": {"n_ted": 10_000, "n_placsp": 10_000, "rows_per_part": 5_000},
    # Routing-heuristic scale (~250k input rows). Manual runs only, never CI.
    "medium": {"n_ted": 100_000, "n_placsp": 100_000, "rows_per_part": 10_000},
    # Planning scale (~2.0M input rows). Manual runs only, never CI.
    "large": {"n_ted": 800_000, "n_placsp": 800_000, "rows_per_part": 100_000},
    # Full-backfill scale (~5.1M input rows). Manual runs only, never CI.
    "backfill": {"n_ted": 2_000_000, "n_placsp": 2_000_000, "rows_per_part": 250_000},
}


def dataset_profile(name: str) -> dict[str, int]:
    """Return a copy of the named dataset profile parameters."""

    try:
        return dict(PROFILES[name])
    except KeyError:
        raise ValueError(f"Unknown benchmark profile {name!r}; expected one of {sorted(PROFILES)}") from None


def _file_sha(seed: int, source_file: str) -> str:
    """Derive a deterministic synthetic checksum for one synthetic file."""

    return hashlib.sha256(f"bench-silver/{seed}/{source_file}".encode("utf-8")).hexdigest()


def _updated_text(index: int, revision: int) -> Any:
    """Return a deterministic ``updated`` value cycling through edge styles."""

    base = datetime(2026, 1, 6 + (index % 20), 10 + (revision * 3 + index) % 9, 15, 0)
    style = UPDATED_STYLES[(index + revision) % len(UPDATED_STYLES)]
    if style == "offset":
        return base.strftime("%Y-%m-%dT%H:%M:%S.000+01:00")
    if style == "zulu":
        return base.strftime("%Y-%m-%dT%H:%M:%SZ")
    if style == "utc":
        return base.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    if style == "naive":
        return base.strftime("%Y-%m-%dT%H:%M:%S")
    if style == "invalid":
        return "not-a-timestamp"
    return None


def _ted_payload(
    notice: str,
    index: int,
    *,
    title: str | None = None,
    variant: int = 0,
) -> dict[str, Any]:
    """Build a deterministic synthetic TED notice payload."""

    amount_text = EDGE_DECIMALS[index % len(EDGE_DECIMALS)] if variant == 0 else f"{1000 + index}.{(7 * index) % 100:02d}"
    payload: dict[str, Any] = {
        "_source": "ted",
        "ND": notice,
        "PD": f"2026-01-{(index % 28) + 1:02d}",
        "TI": {"spa": title or f"Servicio sintético TED {notice}"},
        "description-glo": {"spa": f"Descripción sintética {notice} ({index})"},
        "buyer-name": {"spa": [f"Organismo sintético {index % 25:02d}"]},
        "CY": "ESP" if index % 3 == 0 else "ES",
        "PC": [TED_CPVS[index % len(TED_CPVS)], TED_CPVS[(index + 1) % len(TED_CPVS)], TED_CPVS[index % len(TED_CPVS)]],
        "estimated-value-lot": amount_text,
        "currency": "EUR",
        "url": f"https://example.invalid/ted/{notice}",
    }
    if index % 4 == 0:
        payload["notice-type"] = "CN-standard"
    if index % 5 == 0:
        payload["BT-04-notice"] = f"PROC-SYN-{index // 5:05d}"
    if variant == 1:
        # Exercises the framework-maximum fallback amount field.
        payload.pop("estimated-value-lot")
        payload["framework-maximum-value-lot"] = amount_text
    if variant == 2:
        # Malformed optional amount: stays null and drops the currency pairing.
        payload["estimated-value-lot"] = "n/a"
    if variant == 3:
        payload["estimated-value-lot"] = "0"
    return payload


def _placsp_payload(
    atom_id: str,
    index: int,
    revision: int,
    *,
    title: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic synthetic OpenPLACSP entry payload."""

    amount_text = EDGE_DECIMALS[(index + revision) % len(EDGE_DECIMALS)]
    amount_float = float(amount_text)
    if revision == 0:
        updated = _updated_text(index, revision)
    else:
        # Revisions of the same atom id share one procedure but need distinct
        # valid instants: an undated marker would collapse two different
        # revisions into one event_id and fail as a material collision.
        valid = ("offset", "zulu", "utc")[(index + revision) % 3]
        base = datetime(2026, 2, 1 + (index % 20), 9 + revision * 3, 30, 0)
        if valid == "offset":
            updated = base.strftime("%Y-%m-%dT%H:%M:%S.000+01:00")
        elif valid == "zulu":
            updated = base.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            updated = base.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    payload: dict[str, Any] = {
        "_source": "placsp",
        "tender_no": f"SYN-{index:06d}",
        "atom_id": atom_id,
        "title": title or f"Licitación sintética {atom_id.rsplit('/', 1)[-1]} r{revision}",
        "summary": f"Resumen sintético {index} revisión {revision}",
        "url": f"https://example.invalid/placsp/{index:06d}",
        "status_code": "EV" if revision == 0 else "ADJ",
        "buyer": f"Ayuntamiento sintético {index % 40:02d}",
        "buyer_dir3": f"L010000{(index % 40) + 1:02d}",
        "nuts_code": "ES300",
        "cpv": [PLACSP_CPVS[index % len(PLACSP_CPVS)], PLACSP_CPVS[index % len(PLACSP_CPVS)]],
    }
    if updated is not None:
        payload["updated"] = updated
    if index % 6 == 5:
        # Legacy payload without retained raw text: exact float fallback path.
        payload["amount_tax_exclusive"] = amount_float
        payload["amount_tax_exclusive_currency"] = "EUR"
    else:
        payload["amount_estimated_overall"] = amount_float
        payload["amount_estimated_overall_currency"] = "EUR"
        payload["amount_estimated_overall_raw"] = amount_text
    return payload


def _atom_id(index: int) -> str:
    """Return the deterministic synthetic atom id for one PLACSP procedure."""

    return f"https://example.invalid/sindicacion/{index:08d}"


def iter_record_specs(
    *,
    n_ted: int,
    n_placsp: int,
    with_collision: bool = False,
) -> Iterator[tuple[dict[str, Any], str, str | None]]:
    """Yield ``(payload, source, member)`` specs in deterministic phase order.

    Phases stream primaries before repeats and revisions, so repeated
    observations always land in later part files than their first sighting
    without holding the corpus in memory. Exact Decimal source text is built
    into the payloads here and never altered downstream.
    """

    if n_ted < 0 or n_placsp < 0:
        raise ValueError("n_ted/n_placsp must be non-negative")
    if with_collision and n_ted < 1:
        raise ValueError("with_collision needs at least one TED notice to collide with")
    for index in range(n_ted):
        yield _ted_payload(f"SYN-TED-{index:06d}", index, variant=index % 4), "ted", None
    for index in range(n_placsp):
        yield _placsp_payload(_atom_id(index), index, 0), "placsp", f"entry-{(index % 90) + 1}.atom"
    for index in range(n_placsp):
        if index % 10 == 0:
            for revision in (1, 2):
                yield (
                    _placsp_payload(_atom_id(index), index, revision),
                    "placsp",
                    f"entry-{(index % 90) + 1}.atom",
                )
    for index in range(n_ted):
        if index % 7 == 0:
            yield _ted_payload(f"SYN-TED-{index:06d}", index, variant=index % 4), "ted", None
    for index in range(n_placsp):
        if index % 7 == 1:
            yield _placsp_payload(_atom_id(index), index, 0), "placsp", f"entry-{(index % 90) + 1}.atom"
    # Fixed edge-case procedures with unique identities.
    yield _ted_payload("SYN-TED-EDGE-AMOUNT", 0, title="Importe mínimo exacto"), "ted", None
    max_ted = _ted_payload("SYN-TED-EDGE-MAX", 1, title="Importe en el límite de decimal(20,2)")
    max_ted["estimated-value-lot"] = MAX_DECIMAL_TEXT
    yield max_ted, "ted", None
    max_placsp = _placsp_payload(
        "https://example.invalid/sindicacion/edge-max-amount", 2, 0, title="Importe máximo con texto retenido"
    )
    max_placsp["amount_estimated_overall_raw"] = MAX_DECIMAL_TEXT
    max_placsp["amount_estimated_overall"] = float(MAX_DECIMAL_TEXT)
    yield max_placsp, "placsp", "edge-max.atom"
    yield (
        _placsp_payload(
            "https://example.invalid/sindicacion/edge-undated", 3, 0, title="Evento sintético sin instante válido"
        ),
        "placsp",
        "edge.atom",
    )
    if with_collision:
        yield (
            _ted_payload("SYN-TED-000000", 0, title="Título materialmente distinto"),
            "ted",
            None,
        )


def iter_tombstone_refs(*, n_placsp: int) -> Iterator[str]:
    """Yield deterministic synthetic tombstone refs, ending with a repeat.

    Fully streaming: no list is materialized regardless of ``n_placsp``.
    """

    if n_placsp < 0:
        raise ValueError("n_placsp must be non-negative")
    limit = max(n_placsp // 20, 0)
    first: str | None = None
    emitted = 0
    for position in range(n_placsp):
        if position % 20 != 0 or emitted >= limit:
            continue
        ref = _atom_id(position)
        if first is None:
            first = ref
        emitted += 1
        yield ref
    if first is not None:
        # A repeated deletion control must collapse to one tombstone event.
        yield first


def _record_row(
    payload: dict[str, Any],
    source: str,
    member: str | None,
    *,
    part_index: int,
    line: int,
    entry: int,
    retrieved_at: datetime,
    sha: str,
) -> dict[str, Any]:
    """Build one Bronze record row with deterministic part provenance."""

    if source == "ted":
        return {
            "source": "ted",
            "source_file": f"synthetic/ted-{part_index:05d}.jsonl",
            "source_member": None,
            "source_member_index": None,
            "record_locator": f"line:{line}",
            "source_record_id": payload["ND"],
            "raw_sha256": sha,
            "raw_retrieved_at": retrieved_at,
            "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
        }
    return {
        "source": "placsp",
        "source_file": f"synthetic/placsp-{part_index:05d}.zip",
        "source_member": member or f"entry-{entry}.atom",
        "source_member_index": entry,
        "record_locator": f"entry:{entry}",
        "source_record_id": payload["atom_id"],
        "raw_sha256": sha,
        "raw_retrieved_at": retrieved_at,
        "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
    }


def _tombstone_row(
    ref: str,
    *,
    part_index: int,
    position: int,
    retrieved_at: datetime,
    sha: str,
) -> dict[str, Any]:
    """Build one Bronze tombstone row with deterministic part provenance."""

    return {
        "source": "placsp",
        "source_file": f"synthetic/placsp-{part_index:05d}.zip",
        "source_member": "deleted.atom",
        "source_member_index": position,
        "record_locator": f"deleted-entry:{position}",
        "source_record_id": ref,
        "raw_sha256": sha,
        "raw_retrieved_at": retrieved_at,
    }


def _managed_artifacts(target: Path) -> list[str]:
    """List existing managed benchmark artifacts under a dataset target."""

    found: list[str] = []
    for name in (DATASET_FILENAME, SILVER_FILENAME):
        if (target / name).exists():
            found.append(name)
    for dirname in (RECORDS_DIRNAME, TOMBSTONES_DIRNAME):
        directory = target / dirname
        if directory.is_dir():
            found.extend(f"{dirname}/{part.name}" for part in sorted(directory.glob("part-*.parquet")))
    return found


def write_bronze_parts(
    target_dir: str | Path,
    *,
    seed: int,
    n_ted: int,
    n_placsp: int,
    rows_per_part: int,
    with_collision: bool = False,
) -> dict[str, Any]:
    """Stream specs through a bounded buffer into Bronze Parquet part files.

    At most ``rows_per_part`` Python row dicts are held at once; each full
    buffer is written as one part file and released. The layout
    (``records/part-*.parquet``, ``tombstones/part-*.parquet`` plus
    ``dataset.json``) is reusable unchanged by a future Spark benchmark.
    A target that already holds managed dataset artifacts (part files,
    ``dataset.json`` or the Silver output) is rejected with
    ``FileExistsError`` instead of being silently overwritten or mixed with
    another profile: use a newly created empty directory. Unrelated user
    files are never deleted or touched.
    Returns the dataset manifest (also written as ``dataset.json``).
    """

    if rows_per_part <= 0:
        raise ValueError("rows_per_part must be positive")
    target = Path(target_dir)
    if target.exists() and not target.is_dir():
        raise NotADirectoryError(f"Benchmark dataset target is not a directory: {target}")
    existing = _managed_artifacts(target)
    if existing:
        sample = ", ".join(existing[:5])
        raise FileExistsError(
            f"Refusing to mix benchmark artifacts into non-empty dataset target {target} "
            f"(found: {sample}); use a newly created empty directory"
        )
    records_dir = target / RECORDS_DIRNAME
    tombstones_dir = target / TOMBSTONES_DIRNAME
    records_dir.mkdir(parents=True, exist_ok=True)
    tombstones_dir.mkdir(parents=True, exist_ok=True)

    record_parts = 0
    bronze_records = 0
    buffer: list[dict[str, Any]] = []
    ted_lines = 0
    placsp_entries = 0

    def flush_records() -> None:
        nonlocal record_parts, bronze_records, buffer
        bronze_frame(buffer).write_parquet(records_dir / f"part-{record_parts:05d}.parquet")
        record_parts += 1
        bronze_records += len(buffer)
        buffer = []

    for payload, source, member in iter_record_specs(n_ted=n_ted, n_placsp=n_placsp, with_collision=with_collision):
        # The part index is the count of already flushed parts: the buffer in
        # progress always belongs to the next unflushed part.
        part_index = record_parts
        retrieved_at = BASE_RETRIEVED_AT + timedelta(hours=part_index)
        sha = _file_sha(seed, f"records-part-{part_index:05d}")
        if source == "ted":
            ted_lines += 1
            buffer.append(
                _record_row(
                    payload, source, member, part_index=part_index,
                    line=ted_lines, entry=0, retrieved_at=retrieved_at, sha=sha,
                )
            )
        else:
            placsp_entries += 1
            buffer.append(
                _record_row(
                    payload, source, member, part_index=part_index,
                    line=0, entry=placsp_entries, retrieved_at=retrieved_at, sha=sha,
                )
            )
        if len(buffer) >= rows_per_part:
            flush_records()
    if buffer:
        flush_records()
    if record_parts == 0:
        bronze_frame([]).write_parquet(records_dir / "part-00000.parquet")
        record_parts = 1

    tombstone_parts = 0
    bronze_tombstones = 0
    tomb_buffer: list[dict[str, Any]] = []
    position = 0
    for ref in iter_tombstone_refs(n_placsp=n_placsp):
        position += 1
        part_index = tombstone_parts
        tomb_buffer.append(
            _tombstone_row(
                ref, part_index=part_index, position=position,
                retrieved_at=BASE_RETRIEVED_AT + timedelta(hours=10_000 + part_index),
                sha=_file_sha(seed, f"tombstones-part-{part_index:05d}"),
            )
        )
        if len(tomb_buffer) >= rows_per_part:
            tombstone_frame(tomb_buffer).write_parquet(tombstones_dir / f"part-{tombstone_parts:05d}.parquet")
            tombstone_parts += 1
            bronze_tombstones += len(tomb_buffer)
            tomb_buffer = []
    if tomb_buffer:
        tombstone_frame(tomb_buffer).write_parquet(tombstones_dir / f"part-{tombstone_parts:05d}.parquet")
        tombstone_parts += 1
        bronze_tombstones += len(tomb_buffer)
    if tombstone_parts == 0:
        tombstone_frame([]).write_parquet(tombstones_dir / "part-00000.parquet")
        tombstone_parts = 1

    manifest: dict[str, Any] = {
        "dataset": "bench-silver-bronze",
        "seed": seed,
        "n_ted": n_ted,
        "n_placsp": n_placsp,
        "rows_per_part": rows_per_part,
        "with_collision": with_collision,
        "layout": {
            "records": f"{RECORDS_DIRNAME}/part-*.parquet",
            "tombstones": f"{TOMBSTONES_DIRNAME}/part-*.parquet",
            "records_schema": {name: str(dtype) for name, dtype in BRONZE_SCHEMA.items()},
            "tombstones_schema": {name: str(dtype) for name, dtype in TOMBSTONE_SCHEMA.items()},
        },
        "inputs": {
            "bronze_records": bronze_records,
            "bronze_tombstones": bronze_tombstones,
            "input_rows": bronze_records + bronze_tombstones,
            "record_parts": record_parts,
            "tombstone_parts": tombstone_parts,
        },
    }
    (target / DATASET_FILENAME).write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def generate_bronze_in_memory(
    *,
    seed: int = 7,
    n_ted: int = 120,
    n_placsp: int = 120,
    with_collision: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Materialize the whole synthetic dataset as Python row dicts.

    Test and small-scale helper only: it holds every row in memory. The
    manual scale path must use :func:`iter_record_specs` /
    :func:`iter_tombstone_refs` with :func:`write_bronze_parts` instead.
    Semantic coverage matches the part files: TED and PLACSP observations,
    repeats that must deduplicate, revisions that must stay distinct,
    tombstones including a repeated control, exact Decimal and timestamp edge
    cases, and CPV duplication in source order. With ``with_collision=True``
    one extra TED observation reuses a published notice number with a
    different title, so the build must fail with a material-collision
    ``ValueError`` on every engine; that dataset is a correctness check, not
    a performance profile.
    """

    records = [
        _record_row(
            payload, source, member, part_index=0,
            line=position + 1 if source == "ted" else 0,
            entry=position + 1 if source == "placsp" else 0,
            retrieved_at=BASE_RETRIEVED_AT, sha=_file_sha(seed, "in-memory"),
        )
        for position, (payload, source, member) in enumerate(
            iter_record_specs(n_ted=n_ted, n_placsp=n_placsp, with_collision=with_collision)
        )
    ]
    tombstones = [
        _tombstone_row(
            ref, part_index=0, position=position,
            retrieved_at=BASE_RETRIEVED_AT, sha=_file_sha(seed, "in-memory-tombstones"),
        )
        for position, ref in enumerate(iter_tombstone_refs(n_placsp=n_placsp), start=1)
    ]
    return {"records": records, "tombstones": tombstones}


def _child_peak_rss_bytes() -> int | None:
    """Return this process peak RSS in bytes when the platform exposes it."""

    try:
        import resource
    except ImportError:
        return None
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return int(peak)
    # Linux reports ru_maxrss in kibibytes.
    return int(peak) * 1024


def _measured_stage_worker(payload: dict[str, Any], queue: Any) -> None:
    """Run the measured Bronze-Parquet to Silver-Parquet stage in a child."""

    try:
        import time

        import polars as pl

        started = time.perf_counter()
        records = pl.read_parquet(payload["records_glob"])
        tombstones = pl.read_parquet(payload["tombstones_glob"])
        events = build_procurement_events(records, tombstones)
        Path(payload["silver_path"]).parent.mkdir(parents=True, exist_ok=True)
        events.write_parquet(payload["silver_path"])
        queue.put(
            {
                "ok": True,
                "transform_s": time.perf_counter() - started,
                "silver_events": events.height,
                "peak_rss_bytes": _child_peak_rss_bytes(),
            }
        )
    except Exception as exc:  # noqa: BLE001 - transported back to the parent
        queue.put({"ok": False, "error_type": type(exc).__name__, "error": str(exc)})


def measure_stage(
    records_glob: str,
    tombstones_glob: str,
    silver_path: str | Path,
    *,
    timeout_s: int = 3600,
) -> dict[str, Any]:
    """Measure the parquet-to-parquet stage in an isolated spawn child process.

    Returns the child report plus the parent-observed ``wall_s``. The child
    ``peak_rss_bytes`` is the child lifetime high-water mark (see
    ``PEAK_RSS_SCOPE``): it excludes the parent and dataset generation but
    includes child startup before the transform timer. A canonical
    material collision is re-raised as ``ValueError`` with the original
    message; any other child failure becomes ``RuntimeError``.
    """

    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    payload = {"records_glob": records_glob, "tombstones_glob": tombstones_glob, "silver_path": str(silver_path)}
    process = context.Process(target=_measured_stage_worker, args=(payload, queue))
    started = time.perf_counter()
    process.start()
    try:
        process.join(timeout_s)
        wall_s = time.perf_counter() - started
        if process.is_alive():
            process.terminate()
            process.join()
            raise TimeoutError(f"measured stage exceeded {timeout_s}s")
        try:
            report = queue.get(timeout=30)
        except QueueEmpty:
            raise RuntimeError(
                "measured stage child exited "
                f"with code {process.exitcode} without a report"
            ) from None
    finally:
        queue.close()
        queue.join_thread()
    if not report.get("ok"):
        if report.get("error_type") == "ValueError":
            raise ValueError(report["error"])
        raise RuntimeError(f"{report.get('error_type')}: {report.get('error')}")
    report["wall_s"] = wall_s
    return report


def run_benchmark(
    profile: str = "tiny",
    *,
    seed: int = 7,
    with_collision: bool = False,
    output: str | Path | None = None,
    work_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Run one offline benchmark of the parquet-to-parquet stage.

    Dataset generation streams Bronze part files into ``work_dir`` (a
    temporary directory that is discarded when ``work_dir`` is None, or a
    retained directory reusable unchanged by a future Spark benchmark) and is
    timed separately as ``generation_s``, excluded from the measurement. The
    measured stage runs in an isolated child process; with
    ``with_collision=True`` it raises the canonical material-collision
    ``ValueError`` and no metrics are recorded.
    """

    params = dataset_profile(profile)
    temporary = tempfile.TemporaryDirectory(prefix="bench-silver-") if work_dir is None else None
    try:
        if temporary is not None:
            dataset_dir = Path(temporary.name)
        else:
            assert work_dir is not None
            dataset_dir = Path(work_dir)
            dataset_dir.mkdir(parents=True, exist_ok=True)
        started_generation = time.perf_counter()
        manifest = write_bronze_parts(dataset_dir, seed=seed, with_collision=with_collision, **params)
        generation_s = time.perf_counter() - started_generation

        silver_path = dataset_dir / SILVER_FILENAME
        report = measure_stage(
            str(dataset_dir / RECORDS_DIRNAME / "part-*.parquet"),
            str(dataset_dir / TOMBSTONES_DIRNAME / "part-*.parquet"),
            silver_path,
        )
    finally:
        if temporary is not None:
            temporary.cleanup()

    metrics: dict[str, Any] = {
        "benchmark": "bronze-parquet-to-silver-parquet",
        "implementation": IMPLEMENTATION,
        "implementation_detail": IMPLEMENTATION_DETAIL,
        "python_version": platform.python_version(),
        "polars_version": pl.__version__,
        "profile": profile,
        "profile_params": params,
        "seed": seed,
        "with_collision": with_collision,
        "inputs": {
            "dataset_dir": str(dataset_dir),
            **manifest["inputs"],
            "layout": manifest["layout"],
        },
        "outputs": {"silver_path": str(silver_path), "silver_events": report["silver_events"]},
        "stage": STAGE_DESCRIPTION,
        "timing_s": {
            "generation_s": generation_s,
            "transform_s": report["transform_s"],
            "wall_s": report["wall_s"],
        },
        "throughput_rows_per_s": (
            manifest["inputs"]["input_rows"] / report["transform_s"] if report["transform_s"] > 0 else None
        ),
        "throughput_events_per_s": (
            report["silver_events"] / report["transform_s"] if report["transform_s"] > 0 else None
        ),
        # Child lifetime high-water mark (see PEAK_RSS_SCOPE): unlike the
        # parent lifetime ru_maxrss, it excludes the parent and dataset
        # generation, but it still covers child startup before the timer.
        "peak_rss_bytes": report["peak_rss_bytes"],
        "peak_rss_scope": PEAK_RSS_SCOPE,
        "cpu_count": os.cpu_count(),
        # Operational timestamp for run bookkeeping only; never compared
        # for semantic parity.
        "measured_at": datetime.now(UTC).isoformat(),
    }
    if output is not None:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return metrics


def build_parser() -> argparse.ArgumentParser:
    """Build the benchmark CLI parser."""

    parser = argparse.ArgumentParser(
        description="Offline Bronze-Parquet-to-Silver-Parquet benchmark "
        "(python-row baseline over a Polars/Parquet boundary)"
    )
    parser.add_argument("--profile", default="tiny", choices=sorted(PROFILES), help="dataset profile to generate")
    parser.add_argument("--seed", default=7, type=int, help="deterministic dataset seed")
    parser.add_argument(
        "--with-collision",
        action="store_true",
        help="add a deliberate material collision; the transform must fail explicitly",
    )
    parser.add_argument(
        "--work-dir",
        default=None,
        type=Path,
        help="retain the generated Bronze part files here for reuse (e.g. by a future Spark benchmark)",
    )
    parser.add_argument("--output", default=None, type=Path, help="write machine-readable JSON metrics here")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Execute the benchmark and print the metrics JSON to stdout."""

    args = build_parser().parse_args(argv)
    metrics = run_benchmark(
        args.profile, seed=args.seed, with_collision=args.with_collision, output=args.output, work_dir=args.work_dir
    )
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
