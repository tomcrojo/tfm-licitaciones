"""Typed source-specific Bronze persistence and ingestion accounting."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import polars as pl

from .atom import parse_atom_batch, parse_placsp_zip_batch
from .models import TenderRecord
from .normalize import normalize_record


PROVENANCE_SCHEMA = {
    "source": pl.String,
    "source_file": pl.String,
    "source_member": pl.String,
    "record_locator": pl.String,
    "source_record_id": pl.String,
}
BRONZE_SCHEMA = {**PROVENANCE_SCHEMA, "payload_json": pl.String}
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


def load_raw_records(raw_dir: Path) -> dict[str, Any]:
    """Parse each candidate once, retaining errors and source-local provenance.

    ``parsed`` counts nonblank JSONL lines and Atom entries, including failed
    candidates. Unreadable containers/documents have unknown entry counts and
    are accounted separately; deletion controls are not tender candidates.
    """

    bronze: list[dict[str, Any]] = []
    records: list[TenderRecord] = []
    rejections: list[dict[str, Any]] = []
    tombstones: set[str] = set()
    sources: set[str] = set()
    zip_files = zip_entries = atom_files = 0

    def accept(candidate: dict[str, Any]) -> None:
        payload = candidate["payload"]
        location = {key: candidate.get(key) for key in PROVENANCE_SCHEMA}
        # Even malformed source labels must remain representable in rejection
        # provenance. Raw retains the original escaped Unicode code units.
        source = location["source"].encode("utf-8", errors="backslashreplace").decode("utf-8")
        location["source"] = source
        sources.add(source)
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False)
        try:
            payload_json.encode("utf-8")
        except UnicodeEncodeError:
            rejections.append({**location, "rejection_reason": "invalid_unicode_payload", "rejection_scope": "record"})
            return
        if source not in {"ted", "placsp", "boe"}:
            reason = "unsupported_source"
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
            records.append(record)

    for path in discover_raw_files(raw_dir):
        relative = str(path.relative_to(raw_dir))
        suffix = path.suffix.lower()
        if suffix in {".zip", ".xml", ".atom"}:
            sources.add("placsp")
            batch = parse_placsp_zip_batch(path) if suffix == ".zip" else parse_atom_batch(path.read_bytes())
            atom_files += batch.atom_files
            if suffix == ".zip":
                zip_files += 1
                zip_entries += len(batch.entries) + sum(r["rejection_scope"] == "record" for r in batch.rejections)
                # Preserve the existing Silver deletion behavior for ZIP inputs.
                tombstones |= batch.tombstones
            for rejection in batch.rejections:
                rejections.append({"source": "placsp", "source_file": relative, **rejection})
            for entry in batch.entries:
                accept({"source": "placsp", "source_file": relative, **entry})
            continue
        source_hint = path.parent.name.lower()
        with path.open("rb") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                location = {"source": source_hint, "source_file": relative, "source_member": None,
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
                accept({**location, "source": source, "payload": payload})

    by_source = {}
    for source in sorted(sources):
        accepted = sum(row["source"] == source for row in bronze)
        rejected = sum(row["source"] == source and row["rejection_scope"] == "record" for row in rejections)
        by_source[source] = {
            "parsed": accepted + rejected, "accepted": accepted, "rejected": rejected,
            "document_errors": sum(row["source"] == source and row["rejection_scope"] == "document" for row in rejections),
            "control_errors": sum(row["source"] == source and row["rejection_scope"] == "control" for row in rejections),
        }
    totals = {key: sum(counts[key] for counts in by_source.values())
              for key in ("parsed", "accepted", "rejected", "document_errors", "control_errors")}
    ingestion = {
        **totals, "by_source": by_source, "passed": not rejections,
        "placsp_zip_files": zip_files, "placsp_entries": zip_entries,
        "atom_files": atom_files, "tombstone_ids": len(tombstones),
    }
    return {"bronze": bronze, "records": records, "rejections": rejections,
            "tombstone_ids": {ref.rsplit("/", 1)[-1] for ref in tombstones}, "ingestion": ingestion}
