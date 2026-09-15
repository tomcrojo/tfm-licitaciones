"""Immutable, file-level retrieval evidence shared by downloaders and Bronze.

Window execution state is operational bookkeeping. This sidecar is the
authority for the identity and retrieval time of a concrete Raw artifact.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath


class RawProvenanceError(ValueError):
    """Raw evidence is missing, invalid, or does not describe the stored bytes."""


@dataclass(frozen=True)
class RawArtifact:
    source: str
    raw_path: str
    sha256: str
    retrieved_at: str
    partition: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    version: int = 1

    def __post_init__(self) -> None:
        try:
            if self.version != 1 or not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", self.source):
                raise ValueError("invalid version or source")
            relative = PurePosixPath(self.raw_path)
            if relative.is_absolute() or ".." in relative.parts or str(relative) != self.raw_path or not relative.name:
                raise ValueError("raw_path must be a normalized relative path")
            if not re.fullmatch(r"[0-9a-f]{64}", self.sha256):
                raise ValueError("sha256 must be a lowercase SHA-256 hex digest")
            if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
                raise ValueError("retrieved_at must include a timezone")
            if self.partition is not None and (not isinstance(self.partition, str) or not self.partition.strip()):
                raise ValueError("partition must be a nonempty string or null")
            if (self.window_start is None) != (self.window_end is None):
                raise ValueError("window requires both bounds")
            if self.window_start is not None:
                start, end = date.fromisoformat(self.window_start), date.fromisoformat(self.window_end)
                if start.isoformat() != self.window_start or end.isoformat() != self.window_end or end < start:
                    raise ValueError("invalid window bounds")
        except (TypeError, ValueError, AttributeError) as exc:
            raise RawProvenanceError(f"Invalid Raw evidence: {exc}") from exc

    @property
    def timestamp(self) -> datetime:
        return datetime.fromisoformat(self.retrieved_at.replace("Z", "+00:00"))


def provenance_path(path: Path) -> Path:
    return path.with_name(path.name + ".provenance.json")


def file_sha256(path: Path) -> str:
    """Hash the exact stored bytes without loading a bulk ZIP into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_raw_artifact(raw_dir: Path, path: Path) -> RawArtifact:
    """Require persisted retrieval evidence and verify it against this file."""

    sidecar = provenance_path(path)
    try:
        artifact = RawArtifact(**json.loads(sidecar.read_text(encoding="utf-8")))
    except (OSError, ValueError, TypeError) as exc:
        raise RawProvenanceError(f"Missing or invalid Raw provenance: {sidecar}: {exc}") from exc
    if artifact.raw_path != path.relative_to(raw_dir).as_posix():
        raise RawProvenanceError(f"Raw path mismatch: {sidecar}")
    if artifact.sha256 != file_sha256(path):
        raise RawProvenanceError(f"Raw checksum mismatch: {path}")
    return artifact


def persist_raw_artifact(
    temporary: Path,
    destination: Path,
    raw_dir: Path,
    *,
    source: str,
    retrieved_at: datetime,
    partition: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> Path:
    """Publish a staged download and its evidence; never overwrite Raw.

    Identical downloads reuse the first retrieval evidence. A changed payload
    gets a checksum-suffixed filename within the same logical partition.
    The caller must supply the download-completion timestamp, never a default
    derived here from persistence time. An orphan after interruption fails
    closed on the next read; two filesystem files are not one transaction.
    """

    if retrieved_at.tzinfo is None or retrieved_at.utcoffset() is None:
        raise RawProvenanceError("retrieved_at must include a timezone")
    checksum = file_sha256(temporary)
    identity = (source, partition, window_start, window_end)
    if destination.exists():
        known = load_raw_artifact(raw_dir, destination)
        if (known.source, known.partition, known.window_start, known.window_end) != identity:
            raise RawProvenanceError(f"Raw partition identity mismatch: {destination}")
        if known.sha256 == checksum:
            return destination
        destination = destination.with_name(f"{destination.stem}-{checksum}{destination.suffix}")
        if destination.exists():
            known = load_raw_artifact(raw_dir, destination)
            if known.sha256 != checksum or (known.source, known.partition, known.window_start, known.window_end) != identity:
                raise RawProvenanceError(f"Raw revision identity mismatch: {destination}")
            return destination
    artifact = RawArtifact(
        source=source, raw_path=destination.relative_to(raw_dir).as_posix(), sha256=checksum,
        retrieved_at=retrieved_at.astimezone(timezone.utc).isoformat(), partition=partition,
        window_start=window_start, window_end=window_end,
    )
    sidecar = provenance_path(destination)
    if sidecar.exists():
        raise RawProvenanceError(f"Raw evidence exists without its payload: {sidecar}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Hard links publish complete files without replacing an existing name.
    # Staging files live on the same filesystem as the destination.
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as handle:
        metadata_temporary = Path(handle.name)
    try:
        metadata_temporary.write_text(json.dumps(asdict(artifact), sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.link(temporary, destination)
        os.link(metadata_temporary, sidecar)
    finally:
        metadata_temporary.unlink(missing_ok=True)
    return destination
