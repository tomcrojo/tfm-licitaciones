"""Typed source-specific Bronze persistence and ingestion accounting."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import polars as pl

from .atom import parse_atom_batch, parse_placsp_zip_batch
from .models import TenderRecord
from .normalize import normalize_record
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
BRONZE_SCHEMA = {**PROVENANCE_SCHEMA, "payload_json": pl.String}
TOMBSTONE_SCHEMA = dict(PROVENANCE_SCHEMA)
REJECTION_SCHEMA = {
    **PROVENANCE_SCHEMA,
    "rejection_reason": pl.String,
    "rejection_scope": pl.String,
}


def bronze_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Persist the JSON serialization already validated at acceptance."""

    return pl.DataFrame(
        [{key: row[key] for key in BRONZE_SCHEMA} for row in rows],
        schema=BRONZE_SCHEMA,
    )


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


def load_raw_records(raw_dir: Path) -> dict[str, Any]:
    """Parse each candidate once, retaining errors and source-local provenance.

    ``parsed`` counts nonblank JSONL lines and Atom entries, including failed
    candidates. Unreadable containers/documents have unknown entry counts and
    are accounted separately; deletion controls are not tender candidates.
    Bronze retains all snapshots; normalized records and legacy tombstone ids
    use only the latest retrieval of each source partition/window.
    """

    bronze: list[dict[str, Any]] = []
    records: list[TenderRecord] = []
    rejections: list[dict[str, Any]] = []
    tombstones: list[dict[str, Any]] = []
    legacy_tombstones: set[str] = set()
    sources: set[str] = set()
    zip_files = zip_entries = atom_files = 0

    def accept(candidate: dict[str, Any]) -> None:
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
            rejections.append({**location, "rejection_reason": "invalid_unicode_payload", "rejection_scope": "record"})
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
            rejections.append({**location, "rejection_reason": reason, "rejection_scope": "record"})
        else:
            bronze.append({**location, "payload": payload, "payload_json": payload_json})
            if artifact.raw_path in latest_paths:
                records.append(record)

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
                rejections.append({**provenance, **rejection})
            for entry in batch.entries:
                accept({**provenance, **entry})
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
                    rejections.append({**location, "rejection_reason": "invalid_json", "rejection_scope": "record"})
                    sources.add(source_hint)
                    continue
                if not isinstance(payload, dict):
                    rejections.append({**location, "rejection_reason": "expected_json_object", "rejection_scope": "record"})
                    sources.add(source_hint)
                    continue
                source = str(payload.get("_source") or payload.get("source") or source_hint).lower()
                payload["_source"] = source
                accept({**location, "payload": payload})

    by_source = {}
    for source in sorted(sources):
        accepted = sum(row["source"] == source for row in bronze)
        rejected = sum(row["source"] == source and row["rejection_scope"] == "record" for row in rejections)
        by_source[source] = {
            "parsed": accepted + rejected, "accepted": accepted, "rejected": rejected,
            "document_errors": sum(row["source"] == source and row["rejection_scope"] == "document" for row in rejections),
            "control_errors": sum(row["source"] == source and row["rejection_scope"] == "control" for row in rejections),
            "tombstones": sum(row["source"] == source for row in tombstones),
        }
    totals = {key: sum(counts[key] for counts in by_source.values())
              for key in ("parsed", "accepted", "rejected", "document_errors", "control_errors", "tombstones")}
    ingestion = {
        **totals, "by_source": by_source, "passed": not rejections,
        "placsp_zip_files": zip_files, "placsp_entries": zip_entries,
        "atom_files": atom_files, "tombstone_ids": len(legacy_tombstones),
        "superseded_artifacts": len(artifacts) - len(latest_paths),
        "superseded_records": len(bronze) - len(records),
    }
    return {"bronze": bronze, "records": records, "rejections": rejections, "tombstones": tombstones,
            "tombstone_ids": {ref.rsplit("/", 1)[-1] for ref in legacy_tombstones}, "ingestion": ingestion}
