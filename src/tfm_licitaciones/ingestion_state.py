"""Local ingestion-state primitives for idempotent historical and incremental runs.

One window of one source is represented by a single small JSON document under a
deterministic path, so reruns of the same window converge on the same file and
Raw payloads stay immutable. The module is infrastructure only: adapters are
expected to call these helpers around their own download logic.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

STATE_VERSION = 1
_SOURCE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_WINDOW_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class IngestionStatus(str, Enum):
    """Execution status of one ingestion window."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    FAILED = "failed"


class PartitionChange(str, Enum):
    """Outcome of recording one partition download attempt."""

    DOWNLOADED = "downloaded"
    UNCHANGED = "unchanged"
    CHANGED = "changed"


@dataclass
class PartitionState:
    """Checksum identity of one successfully downloaded partition."""

    checksum: str
    bytes: int | None = None
    downloaded_at: str | None = None
    superseded_checksums: list[str] = field(default_factory=list)


@dataclass
class ChecksumMismatch:
    """Payload change detected for a partition identity that was already stored."""

    partition: str
    previous_checksum: str
    new_checksum: str
    detected_at: str | None = None
    resolved_at: str | None = None

    @property
    def is_resolved(self) -> bool:
        return self.resolved_at is not None


@dataclass
class ErrorRecord:
    """Window-level error attributed to one execution attempt."""

    message: str
    at: str | None = None


@dataclass
class WindowState:
    """Full inspectable state of one source window."""

    source: str
    window_start: str
    window_end: str
    expected_partitions: list[str]
    version: int = STATE_VERSION
    status: str = IngestionStatus.PENDING.value
    downloaded_partitions: dict[str, PartitionState] = field(default_factory=dict)
    failures: dict[str, str] = field(default_factory=dict)
    checksum_mismatches: list[ChecksumMismatch] = field(default_factory=list)
    errors: list[ErrorRecord] = field(default_factory=list)
    error_history: list[ErrorRecord] = field(default_factory=list)
    started_at: str | None = None
    finished_at: str | None = None

    @property
    def missing_partitions(self) -> list[str]:
        """Expected partitions without a successful download on record."""

        return [p for p in self.expected_partitions if p not in self.downloaded_partitions]

    @property
    def unresolved_mismatches(self) -> list[ChecksumMismatch]:
        return [m for m in self.checksum_mismatches if not m.is_resolved]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_or_default(now: datetime | None) -> str:
    """Format an injectable timestamp, defaulting to the current UTC instant."""

    return now.isoformat() if now is not None else _utc_now()


def _validate_window_bound(value: str, name: str) -> date:
    """Validate one strict ISO calendar date (YYYY-MM-DD) and return it."""

    if not isinstance(value, str) or not _WINDOW_PATTERN.match(value):
        raise ValueError(f"{name} must be an ISO date string (YYYY-MM-DD): {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} is not a real calendar date: {value!r}") from exc


def new_window(source: str, window_start: str, window_end: str, expected_partitions: list[str]) -> WindowState:
    """Create a pending window state for one source."""

    _validate_source(source)
    start = _validate_window_bound(window_start, "window_start")
    end = _validate_window_bound(window_end, "window_end")
    if end < start:
        raise ValueError("window_end must not be earlier than window_start")
    if len(set(expected_partitions)) != len(expected_partitions):
        raise ValueError("expected_partitions must not contain duplicates")
    return WindowState(
        source=source,
        window_start=window_start,
        window_end=window_end,
        expected_partitions=list(expected_partitions),
    )


def _validate_source(source: str) -> None:
    if not _SOURCE_PATTERN.match(source):
        raise ValueError(f"source must match {_SOURCE_PATTERN.pattern!r}: {source!r}")


def window_path(state_dir: Path, source: str, window_start: str, window_end: str) -> Path:
    """Return the deterministic file path for one window state."""

    _validate_source(source)
    _validate_window_bound(window_start, "window_start")
    _validate_window_bound(window_end, "window_end")
    return Path(state_dir) / source / f"{window_start}__{window_end}.json"


def load_window(state_dir: Path, source: str, window_start: str, window_end: str) -> WindowState | None:
    """Load a persisted window state, or None when it does not exist yet."""

    path = window_path(state_dir, source, window_start, window_end)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid ingestion-state JSON in {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return _state_from_payload(payload, path)


def _state_from_payload(payload: dict[str, Any], path: Path) -> WindowState:
    required = {
        "version",
        "source",
        "window_start",
        "window_end",
        "expected_partitions",
        "status",
        "downloaded_partitions",
    }
    missing = sorted(required - payload.keys())
    if missing:
        raise ValueError(f"Missing keys {missing} in {path}")
    if payload["version"] != STATE_VERSION:
        raise ValueError(f"Unsupported state version {payload['version']!r} in {path}")
    try:
        status = IngestionStatus(payload["status"])
    except ValueError as exc:
        raise ValueError(f"Unknown status {payload['status']!r} in {path}") from exc
    partitions: dict[str, PartitionState] = {}
    for name, record in payload["downloaded_partitions"].items():
        partitions[name] = PartitionState(
            checksum=record["checksum"],
            bytes=record.get("bytes"),
            downloaded_at=record.get("downloaded_at"),
            superseded_checksums=list(record.get("superseded_checksums", [])),
        )
    mismatches = [
        ChecksumMismatch(
            partition=item["partition"],
            previous_checksum=item["previous_checksum"],
            new_checksum=item["new_checksum"],
            detected_at=item.get("detected_at"),
            resolved_at=item.get("resolved_at"),
        )
        for item in payload.get("checksum_mismatches", [])
    ]

    def _error_records(items: Any) -> list[ErrorRecord]:
        return [ErrorRecord(message=item["message"], at=item.get("at")) for item in items]

    return WindowState(
        source=payload["source"],
        window_start=payload["window_start"],
        window_end=payload["window_end"],
        expected_partitions=list(payload["expected_partitions"]),
        version=payload["version"],
        status=status.value,
        downloaded_partitions=partitions,
        failures=dict(payload.get("failures", {})),
        checksum_mismatches=mismatches,
        errors=_error_records(payload.get("errors", [])),
        error_history=_error_records(payload.get("error_history", [])),
        started_at=payload.get("started_at"),
        finished_at=payload.get("finished_at"),
    )


def save_window(state_dir: Path, state: WindowState) -> Path:
    """Persist one window state with an atomic replace, returning its path.

    The document is fully serialized first and committed via a temporary file
    in the destination directory, so an interrupted save cannot leave a
    half-written state file in place of a valid previous one.
    """

    path = window_path(state_dir, state.source, state.window_start, state.window_end)
    text = json.dumps(asdict(state), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False
    )
    temporary = Path(handle.name)
    try:
        with handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def mark_running(state: WindowState, *, now: datetime | None = None) -> None:
    """Start (or restart) execution of a window for a fresh attempt.

    Active window errors move to ``error_history`` and active partition
    failures are cleared, so a rerun can recover and finalize as complete.
    The audited history, downloaded partitions and mismatches persist.
    """

    if state.status == IngestionStatus.RUNNING.value:
        raise ValueError("Cannot mark a running window as running")
    state.status = IngestionStatus.RUNNING.value
    state.error_history.extend(state.errors)
    state.errors = []
    state.failures = {}
    state.started_at = _now_or_default(now)
    state.finished_at = None


def record_partition(
    state: WindowState,
    partition: str,
    checksum: str,
    *,
    bytes: int | None = None,
    now: datetime | None = None,
) -> PartitionChange:
    """Record one downloaded partition and report how it affected the state.

    - new partition: stored with its checksum (DOWNLOADED);
    - identical checksum: no-op, timestamps preserved (UNCHANGED);
    - different checksum for a known identity: the stored checksum is kept,
      a ChecksumMismatch is recorded (CHANGED) and nothing is overwritten.

    Any successful observation of the expected partition, including an
    unchanged checksum, clears an active partition failure from the current
    attempt. Repeated observations of the same changed checksum record a
    single mismatch; a different new checksum records a separate one.
    """

    if partition not in state.expected_partitions:
        raise ValueError(f"Partition {partition!r} is not expected for this window")
    known = state.downloaded_partitions.get(partition)
    if known is None:
        state.downloaded_partitions[partition] = PartitionState(
            checksum=checksum, bytes=bytes, downloaded_at=_now_or_default(now)
        )
        state.failures.pop(partition, None)
        return PartitionChange.DOWNLOADED
    state.failures.pop(partition, None)
    if known.checksum == checksum:
        return PartitionChange.UNCHANGED
    already_recorded = any(
        mismatch.partition == partition and mismatch.new_checksum == checksum
        for mismatch in state.unresolved_mismatches
    )
    if not already_recorded:
        state.checksum_mismatches.append(
            ChecksumMismatch(
                partition=partition,
                previous_checksum=known.checksum,
                new_checksum=checksum,
                detected_at=_now_or_default(now),
            )
        )
    return PartitionChange.CHANGED


def record_partition_failure(state: WindowState, partition: str, message: str) -> None:
    """Record that one expected partition could not be downloaded."""

    if partition not in state.expected_partitions:
        raise ValueError(f"Partition {partition!r} is not expected for this window")
    state.failures[partition] = message


def accept_payload_change(
    state: WindowState,
    partition: str,
    checksum: str,
    *,
    bytes: int | None = None,
    now: datetime | None = None,
) -> None:
    """Explicitly accept a changed payload, superseding the previous checksum.

    The previous checksum is kept in the partition history and the mismatch is
    marked resolved, so corrections are auditable instead of silent.
    """

    candidates = [m for m in state.unresolved_mismatches if m.partition == partition and m.new_checksum == checksum]
    if not candidates:
        raise ValueError(f"No unresolved mismatch for partition {partition!r} with checksum {checksum!r}")
    known = state.downloaded_partitions.get(partition)
    if known is None:
        raise ValueError(f"Partition {partition!r} has no stored checksum to supersede")
    for mismatch in candidates:
        mismatch.resolved_at = _now_or_default(now)
    known.superseded_checksums.append(known.checksum)
    known.checksum = checksum
    known.bytes = bytes
    known.downloaded_at = _now_or_default(now)
    state.failures.pop(partition, None)


def mark_failed(state: WindowState, message: str, *, now: datetime | None = None) -> None:
    """Fail the whole window with an explicit message."""

    if state.status not in {IngestionStatus.PENDING.value, IngestionStatus.RUNNING.value}:
        raise ValueError(f"Cannot mark a {state.status} window as failed")
    state.status = IngestionStatus.FAILED.value
    state.errors.append(ErrorRecord(message=message, at=_now_or_default(now)))
    state.finished_at = _now_or_default(now)


def finalize_window(state: WindowState, *, now: datetime | None = None) -> str:
    """Derive the final status of a window from its recorded evidence.

    failed     some download failed this attempt, the run has active window
               errors, or an unresolved payload change exists for a partition
               identity;
    incomplete expected partitions are missing without any recorded failure;
    complete   every expected partition is downloaded with a stable identity.

    Historical errors from previous attempts stay in ``error_history`` and do
    not affect the status of the current attempt.
    """

    if state.status not in {IngestionStatus.PENDING.value, IngestionStatus.RUNNING.value}:
        raise ValueError(f"Cannot finalize a {state.status} window")
    if state.failures or state.errors or state.unresolved_mismatches:
        status = IngestionStatus.FAILED
    elif state.missing_partitions:
        status = IngestionStatus.INCOMPLETE
    else:
        status = IngestionStatus.COMPLETE
    state.status = status.value
    state.finished_at = _now_or_default(now)
    return status.value


def summary(state: WindowState) -> dict[str, Any]:
    """Return a compact machine-readable overview of one window."""

    return {
        "source": state.source,
        "window_start": state.window_start,
        "window_end": state.window_end,
        "status": state.status,
        "expected_partitions": len(state.expected_partitions),
        "downloaded_partitions": len(state.downloaded_partitions),
        "missing_partitions": len(state.missing_partitions),
        "failed_partitions": sorted(state.failures),
        "unresolved_mismatches": [m.partition for m in state.unresolved_mismatches],
        "errors": len(state.errors),
        "error_history": len(state.error_history),
        "started_at": state.started_at,
        "finished_at": state.finished_at,
    }
