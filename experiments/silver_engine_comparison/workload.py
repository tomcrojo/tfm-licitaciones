"""Historical workload pin + fingerprint for the Silver engine comparison (EXPERIMENT).

The ``python-row`` baseline is frozen (see
:mod:`experiments.silver_engine_comparison.engines.python_row_reference`),
but the measured workload itself comes from the live production dataset
generator (:mod:`tfm_licitaciones.bench_silver`). If a future PR changes
that generator while keeping the same row counts, ``--profile small
--seed 7`` could silently denote a different workload and the
"reproducible" benchmark would move without anyone noticing.

Cheapest robust fix (no 700-line generator copy): pin the generator commit
used for the retained measurements and assert a deterministic fingerprint
of the generated Bronze dataset for the pinned workloads. The fingerprint
is computed over canonicalized logical row values (payload + provenance),
never over Parquet writer bytes, so it is stable across writer versions
but sensitive to any generator change.

One compatibility rule is intentionally narrow: an additive
``source_deleted_at`` tombstone column is omitted from the historical digest
*only while every generated value is null*. The retained synthetic workload
predates source-dated tombstones and therefore has no deletion instant to add;
including a schema-only null column would make the historical fingerprint
move despite identical logical inputs. As soon as that column carries any
non-null value it participates in the digest and the pin fails loudly.

Pinned workloads: ``tiny`` and ``small`` with ``seed=7`` and
``with_collision=False`` — the profiles covered by the experiment tests
and the retained ``results/engines-small-seed7-2026-09-16.json`` evidence
(small: 24 862 records + 501 tombstones = 25 363 inputs → 22 504 events).
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

# Generator logic at measurement time. Verified: src/tfm_licitaciones/bench_silver.py
# (and bronze.py) are byte-identical between this commit and the frozen
# baseline commit, so the pin below describes both.
WORKLOAD_GENERATOR_COMMIT = "e69016f62fb985639e17153e24b3a570f2f39c20"

# Pinned historical workloads: (profile, seed, with_collision) -> expectations.
# Fingerprints cover the historical logical Bronze row content as written by
# write_bronze_parts (payloads + per-part provenance + partitioning). A later
# production-only nullable tombstone column is ignored only while all-null;
# see `_historical_fingerprint_frame`.
HISTORICAL_WORKLOADS: dict[tuple[str, int, bool], dict[str, Any]] = {
    ("tiny", 7, False): {
        "sha256": "f6867d5387f7bf42e946c8a1a927011f4da5ed419b6dee5a2945607d399967c4",
        "bronze_records": 303,
        "bronze_tombstones": 7,
    },
    ("small", 7, False): {
        "sha256": "abbbe67b99bbcee97c0fa6f05183efc4dcbad3493ac08d422d595b5fb961bc40",
        "bronze_records": 24862,
        "bronze_tombstones": 501,
    },
}


def _canonical_value(value: Any) -> Any:
    """Render one cell deterministically for fingerprinting."""

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _historical_fingerprint_frame(tag: str, frame: pl.DataFrame) -> pl.DataFrame:
    """Project schema-only tombstone evolution out of the historical digest.

    ``source_deleted_at`` did not exist in the retained benchmark workload.
    Current production Bronze adds it so real Atom tombstones can preserve
    their source-published ``when`` instant. The synthetic historical generator
    still creates no such instants, so an all-null column is schema evolution,
    not workload evolution. Any non-null value is a real logical change and is
    deliberately retained so the pinned digest fails.
    """

    if tag != "T" or "source_deleted_at" not in frame.columns:
        return frame
    if frame["source_deleted_at"].is_not_null().any():
        return frame
    return frame.drop("source_deleted_at")


def fingerprint_frames(records: pl.DataFrame, tombstones: pl.DataFrame) -> str:
    """Hash canonicalized historical Bronze row values (writer-byte independent).

    Rows are sorted deterministically and serialized as canonical JSON, so
    the digest is stable across Parquet writer versions but changes whenever
    the historical logical workload alters payloads, provenance, partitioning
    or counts. The one documented all-null tombstone schema extension is
    projected out before hashing.
    """

    lines: list[str] = []
    for tag, raw_frame in (("R", records), ("T", tombstones)):
        frame = _historical_fingerprint_frame(tag, raw_frame)
        columns = sorted(frame.columns)
        ordered = frame.sort(by=columns, nulls_last=True)
        for row in ordered.to_dicts():
            canonical = {key: _canonical_value(row[key]) for key in sorted(row)}
            lines.append(
                tag + json.dumps(canonical, sort_keys=True, ensure_ascii=False, default=str)
            )
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def fingerprint_dataset_dir(dataset_dir: str | Path) -> dict[str, Any]:
    """Fingerprint a dataset previously written by ``write_bronze_parts``."""

    root = Path(dataset_dir)
    records = pl.read_parquet(str(root / "records" / "part-*.parquet"))
    tombstones = pl.read_parquet(str(root / "tombstones" / "part-*.parquet"))
    return {
        "sha256": fingerprint_frames(records, tombstones),
        "bronze_records": records.height,
        "bronze_tombstones": tombstones.height,
    }


def is_historical_workload_pinned(
    profile: str, seed: int, with_collision: bool = False
) -> bool:
    """Return whether a workload key is pinned (cheap; no dataset I/O)."""

    return (profile, seed, with_collision) in HISTORICAL_WORKLOADS


def _manifest_counts(dataset_dir: str | Path) -> dict[str, int]:
    """Read Bronze counts from ``dataset.json`` without touching part files."""

    manifest = json.loads((Path(dataset_dir) / "dataset.json").read_text(encoding="utf-8"))
    inputs = manifest["inputs"]
    return {
        "bronze_records": int(inputs["bronze_records"]),
        "bronze_tombstones": int(inputs["bronze_tombstones"]),
    }


def assert_historical_workload(
    dataset_dir: str | Path,
    *,
    profile: str,
    seed: int,
    with_collision: bool = False,
) -> dict[str, Any]:
    """Fail loudly when a pinned historical workload no longer matches.

    The pin is checked BEFORE any fingerprinting: unpinned workloads
    (anything outside :data:`HISTORICAL_WORKLOADS`, e.g. medium/large/backfill
    or other seeds) return lightweight manifest metadata with
    ``pinned=False`` and ``sha256=None`` without reading, sorting or
    materializing the Bronze frames — fingerprinting multi-million-row
    exploratory datasets just to discard the digest would be pure overhead
    before measurement. On mismatch a ``ValueError`` names the expected
    and actual digests and points at the generator commit pin.
    """

    key = (profile, seed, with_collision)
    expected = HISTORICAL_WORKLOADS.get(key)
    if expected is None:
        return {"pinned": False, "sha256": None, **_manifest_counts(dataset_dir)}
    actual = fingerprint_dataset_dir(dataset_dir)
    problems: list[str] = []
    if actual["sha256"] != expected["sha256"]:
        problems.append(f"sha256 {actual['sha256']} != pinned {expected['sha256']}")
    for count in ("bronze_records", "bronze_tombstones"):
        if actual[count] != expected[count]:
            problems.append(f"{count} {actual[count]} != pinned {expected[count]}")
    if problems:
        raise ValueError(
            f"Historical workload drift for profile={profile!r} seed={seed} "
            f"with_collision={with_collision}: {'; '.join(problems)}. "
            f"The retained measurements were generated with "
            f"tfm_licitaciones.bench_silver @ {WORKLOAD_GENERATOR_COMMIT}; "
            f"the generator changed since. Do not compare new timings against "
            f"the retained reports/results."
        )
    return {"pinned": True, **actual}
