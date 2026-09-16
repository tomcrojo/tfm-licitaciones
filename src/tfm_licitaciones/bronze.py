"""Typed source-specific Bronze persistence and ingestion accounting.

Engine boundary: Python owns the parsing of Raw artifacts (ZIP/Atom/XML/JSONL);
Polars owns tabular assembly, accounting and the Parquet boundary. Candidate
rows are normalized once at the adapter boundary into typed columns and
accumulated in bounded batches, so a full corpus is never materialized as
Python objects.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import polars as pl

from .atom import parse_atom_batch, parse_placsp_zip_batch
from .models import TenderRecord
from .normalize import _as_code_list, normalize_record
from .raw_provenance import RawArtifact, RawProvenanceError, load_raw_artifact


PROVENANCE_SCHEMA = {
    "source": pl.String,
    "source_file": pl.String,
    "source_member": pl.String,
    "source_member_index": pl.UInt32,
    "record_locator": pl.String,
    "source_record_id": pl.String,
    "raw_sha256": pl.String,
    "raw_retrieved_at": pl.Datetime("us", "UTC"),
}
#: Adapter-normalized columns persisted beside the source-specific payload so
#: downstream engines never re-parse JSON. Values are extracted exactly once,
#: by the same :mod:`tfm_licitaciones.normalize` adapters that build
#: ``TenderRecord`` for the legacy path.
ADAPTER_SCHEMA = {
    "tender_id": pl.String,
    "title": pl.String,
    "summary": pl.String,
    "buyer": pl.String,
    "published_date": pl.Date,
    "amount": pl.Float64,
    "currency": pl.String,
    "country": pl.String,
    "url": pl.String,
    "cpv_codes": pl.List(pl.String),
    "buyer_id": pl.String,
    "region": pl.String,
    "status": pl.String,
    "nuts_code": pl.String,
    "updated": pl.String,
    "raw_amount_present": pl.Boolean,
}
BRONZE_SCHEMA = {**PROVENANCE_SCHEMA, "payload_json": pl.String, **ADAPTER_SCHEMA}
TOMBSTONE_SCHEMA = dict(PROVENANCE_SCHEMA)
REJECTION_SCHEMA = {
    **PROVENANCE_SCHEMA,
    "rejection_reason": pl.String,
    "rejection_scope": pl.String,
}
#: Legacy normalized view of the latest snapshot per partition/window. This is
#: the frame consumed by the compatibility Silver path until Gold migrates to
#: canonical events.
SILVER_CANDIDATE_SCHEMA = {
    "position": pl.UInt32,
    "source": pl.String,
    **ADAPTER_SCHEMA,
    "payload_json": pl.String,
}

#: Rows buffered before a candidate batch is converted to a Polars frame.
BATCH_ROWS = 50_000

_RAW_AMOUNT_KEYS = ("estimated-value-lot", "framework-maximum-value-lot", "amount")


class _FrameAccumulator:
    """Collect typed rows and flush them to frames in bounded batches."""

    def __init__(self, schema: dict[str, pl.DataType], batch_rows: int = BATCH_ROWS) -> None:
        self._schema = schema
        self._batch_rows = batch_rows
        self._rows: list[dict[str, Any]] = []
        self._parts: list[pl.DataFrame] = []
        self._count = 0

    def append(self, row: dict[str, Any]) -> None:
        self._rows.append(row)
        self._count += 1
        if len(self._rows) >= self._batch_rows:
            self._flush()

    def _flush(self) -> None:
        if self._rows:
            self._parts.append(pl.DataFrame(self._rows, schema=self._schema))
            self._rows = []

    @property
    def count(self) -> int:
        return self._count

    def frame(self) -> pl.DataFrame:
        self._flush()
        if not self._parts:
            return pl.DataFrame(schema=self._schema)
        if len(self._parts) == 1:
            return self._parts[0]
        return pl.concat(self._parts, rechunk=True)


def bronze_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Assemble accepted candidates into the typed Bronze frame."""

    return pl.DataFrame(rows, schema=BRONZE_SCHEMA)


def rejection_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Keep rejection columns typed even when the run has no rejected inputs."""

    return pl.DataFrame(rows, schema=REJECTION_SCHEMA)


def tombstone_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Persist deletion controls; source_record_id is the full Atom ref."""

    return pl.DataFrame(rows, schema=TOMBSTONE_SCHEMA)


def discover_raw_files(raw_dir: Path) -> list[Path]:
    """Return supported raw files in deterministic source/path order."""

    return sorted(path for path in raw_dir.rglob("*")
                  if path.suffix.lower() in {".jsonl", ".xml", ".atom", ".zip"} and path.is_file())


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Non-JSON numeric constant: {value}")


def _finite_json_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"Non-finite JSON number: {value}")
    return number


def _latest_snapshot_paths(artifacts: list[RawArtifact]) -> set[str]:
    """Select artifact versions for the temporary legacy view only.

    Unpartitioned inputs remain independent. Equal retrieval times with
    different bytes cannot establish a latest snapshot and fail explicitly.
    """

    partitions: dict[tuple, list[RawArtifact]] = defaultdict(list)
    for artifact in artifacts:
        key = (artifact.source, artifact.partition, artifact.window_start, artifact.window_end,
               artifact.raw_path if artifact.partition is None and artifact.window_start is None else None)
        partitions[key].append(artifact)
    selected = set()
    for versions in partitions.values():
        latest = max(versions, key=lambda artifact: artifact.timestamp)
        if any(version.timestamp == latest.timestamp and version.sha256 != latest.sha256 for version in versions):
            raise RawProvenanceError(f"Ambiguous latest Raw snapshot: {latest.raw_path}")
        selected.add(latest.raw_path)
    return selected


def _adapter_row(record: TenderRecord, payload: dict[str, Any], payload_source: str) -> dict[str, Any]:
    """Flatten one normalized record into typed Bronze columns."""

    if payload_source == "ted":
        cpv_codes = _as_code_list(payload.get("PC") or payload.get("cpv"))
    elif payload_source == "placsp":
        cpv_codes = _as_code_list(payload.get("cpv"))
    else:
        cpv_codes = []
    raw_amount = next((payload.get(key) for key in _RAW_AMOUNT_KEYS if key in payload), None)
    return {
        "tender_id": record.tender_id or None,
        "title": record.title or "",
        "summary": record.summary,
        "buyer": record.buyer,
        "published_date": record.published_date,
        "amount": record.amount,
        "currency": record.currency,
        "country": record.country,
        "url": record.url,
        "cpv_codes": cpv_codes,
        "buyer_id": record.buyer_id,
        "region": record.region,
        "status": record.status,
        "nuts_code": payload.get("nuts_code") if payload_source == "placsp" else None,
        "updated": payload.get("updated") or "",
        "raw_amount_present": raw_amount not in (None, "", []),
    }


def _source_metrics(
    records: pl.DataFrame,
    rejections: pl.DataFrame,
    tombstones: pl.DataFrame,
    sources: set[str],
) -> dict[str, dict[str, int]]:
    """Compute per-source ingestion accounting with native aggregations."""

    accepted = dict(records.group_by("source").len().iter_rows()) if records.height else {}
    def _counts(frame: pl.DataFrame, scope: str) -> dict[str, int]:
        if not frame.height:
            return {}
        scoped = frame.filter(pl.col("rejection_scope") == scope).group_by("source").len()
        return dict(scoped.iter_rows())
    rejected = _counts(rejections, "record")
    document_errors = _counts(rejections, "document")
    control_errors = _counts(rejections, "control")
    tombstone_counts = dict(tombstones.group_by("source").len().iter_rows()) if tombstones.height else {}
    metrics: dict[str, dict[str, int]] = {}
    for source in sorted(sources):
        source_accepted = int(accepted.get(source, 0))
        source_rejected = int(rejected.get(source, 0))
        metrics[source] = {
            "parsed": source_accepted + source_rejected,
            "accepted": source_accepted,
            "rejected": source_rejected,
            "document_errors": int(document_errors.get(source, 0)),
            "control_errors": int(control_errors.get(source, 0)),
            "tombstones": int(tombstone_counts.get(source, 0)),
        }
    return metrics


def load_raw_records(raw_dir: Path) -> dict[str, Any]:
    """Parse each candidate once, retaining errors and source-local provenance.

    ``parsed`` counts nonblank JSONL lines and Atom entries, including failed
    candidates. Unreadable containers/documents have unknown entry counts and
    are accounted separately; deletion controls are not tender candidates.
    Bronze retains all snapshots; normalized records and legacy tombstone ids
    use only the latest retrieval of each source partition/window.

    Returns Polars frames for Bronze records, rejections, tombstones and the
    legacy silver candidates, plus the ingestion report and verified raw
    artifacts (so callers can reuse checksums without re-hashing payloads).
    """

    bronze = _FrameAccumulator(BRONZE_SCHEMA)
    candidates = _FrameAccumulator(SILVER_CANDIDATE_SCHEMA)
    rejections = _FrameAccumulator(REJECTION_SCHEMA)
    tombstones = _FrameAccumulator(TOMBSTONE_SCHEMA)
    legacy_tombstones: set[str] = set()
    sources: set[str] = set()
    position = 0
    zip_files = zip_entries = atom_files = 0

    def reject(location: dict[str, Any], reason: str, scope: str) -> None:
        rejections.append({**location, "rejection_reason": reason, "rejection_scope": scope})

    def accept(candidate: dict[str, Any], artifact: RawArtifact, latest_paths: set[str]) -> None:
        nonlocal position
        payload = candidate["payload"]
        location = {key: candidate.get(key) for key in PROVENANCE_SCHEMA}
        # All row and metric provenance follows the sidecar. A conflicting or
        # malformed payload label remains recoverable in the immutable Raw.
        source = location["source"]
        payload_source = payload["_source"]
        sources.add(source)
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False)
        try:
            payload_json.encode("utf-8")
        except UnicodeEncodeError:
            reject(location, "invalid_unicode_payload", "record")
            return
        if source not in {"ted", "placsp", "boe"} or payload_source not in {"ted", "placsp", "boe"}:
            reason = "unsupported_source"
        elif payload_source != source:
            reason = "source_mismatch"
        else:
            record = normalize_record(payload)
            location["source_record_id"] = location["source_record_id"] or record.tender_id or None
            reason = None
            if not record.tender_id:
                reason = "missing_tender_id"
            elif not record.title:
                reason = "missing_title"
        if reason:
            reject(location, reason, "record")
            return
        adapter = _adapter_row(record, payload, payload_source)
        bronze.append({**location, "payload_json": payload_json, **adapter})
        if artifact.raw_path in latest_paths:
            candidates.append(
                {"position": position, "source": record.source, "payload_json": payload_json, **adapter}
            )
            position += 1

    for sidecar in sorted(raw_dir.rglob("*.provenance.json")):
        payload_path = sidecar.with_name(sidecar.name.removesuffix(".provenance.json"))
        if not payload_path.is_file():
            raise RawProvenanceError(f"Raw evidence exists without its payload: {sidecar}")
    artifacts = [load_raw_artifact(raw_dir, path) for path in discover_raw_files(raw_dir)]
    latest_paths = _latest_snapshot_paths(artifacts)
    # Recency also provides a stable tie-break for overlapping legacy windows.
    for artifact in sorted(artifacts, key=lambda item: (item.timestamp, item.raw_path)):
        path = raw_dir / artifact.raw_path
        provenance = {
            "source": artifact.source, "source_file": artifact.raw_path,
            "raw_sha256": artifact.sha256, "raw_retrieved_at": artifact.timestamp,
            "source_member_index": None,
        }
        suffix = path.suffix.lower()
        if suffix in {".zip", ".xml", ".atom"}:
            if artifact.source != "placsp":
                raise RawProvenanceError(f"Expected PLACSP evidence for {path}")
            sources.add("placsp")
            batch = parse_placsp_zip_batch(path) if suffix == ".zip" else parse_atom_batch(path.read_bytes())
            atom_files += batch.atom_files
            if suffix == ".zip":
                zip_files += 1
                zip_entries += len(batch.entries) + sum(r["rejection_scope"] == "record" for r in batch.rejections)
                # Preserve the existing Silver deletion behavior for ZIP inputs.
                if artifact.raw_path in latest_paths:
                    legacy_tombstones |= batch.tombstones
            for control in batch.tombstone_rows:
                tombstones.append({**provenance, **control})
            for rejection in batch.rejections:
                reject({**provenance, **rejection}, rejection["rejection_reason"], rejection["rejection_scope"])
            for entry in batch.entries:
                accept({**provenance, **entry}, artifact, latest_paths)
            continue
        source_hint = artifact.source
        with path.open("rb") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                location = {**provenance, "source_member": None,
                            "record_locator": f"line:{line_number}", "source_record_id": None}
                try:
                    payload = json.loads(line.decode("utf-8"), parse_constant=_reject_json_constant,
                                         parse_float=_finite_json_float)
                except (ValueError, UnicodeDecodeError):
                    reject(location, "invalid_json", "record")
                    sources.add(source_hint)
                    continue
                if not isinstance(payload, dict):
                    reject(location, "expected_json_object", "record")
                    sources.add(source_hint)
                    continue
                source = str(payload.get("_source") or payload.get("source") or source_hint).lower()
                payload["_source"] = source
                accept({**location, "payload": payload}, artifact, latest_paths)

    bronze_frame_, rejection_frame_ = bronze.frame(), rejections.frame()
    tombstone_frame_ = tombstones.frame()
    candidate_frame = candidates.frame()
    by_source = _source_metrics(bronze_frame_, rejection_frame_, tombstone_frame_, sources)
    totals = {key: sum(counts[key] for counts in by_source.values())
              for key in ("parsed", "accepted", "rejected", "document_errors", "control_errors", "tombstones")}
    ingestion = {
        **totals, "by_source": by_source,
        "passed": rejection_frame_.height == 0,
        "placsp_zip_files": zip_files, "placsp_entries": zip_entries,
        "atom_files": atom_files, "tombstone_ids": len(legacy_tombstones),
        "superseded_artifacts": len(artifacts) - len(latest_paths),
        "superseded_records": bronze_frame_.height - candidate_frame.height,
    }
    return {
        "bronze": bronze_frame_,
        "rejections": rejection_frame_,
        "tombstones": tombstone_frame_,
        "silver_candidates": candidate_frame,
        "tombstone_ids": {ref.rsplit("/", 1)[-1] for ref in legacy_tombstones},
        "ingestion": ingestion,
        "artifacts": artifacts,
    }
