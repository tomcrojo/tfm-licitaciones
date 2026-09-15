"""Explicit synthetic retrieval evidence for locally authored test payloads."""

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from tfm_licitaciones.raw_provenance import RawArtifact, file_sha256, provenance_path

RETRIEVED_AT = datetime(2026, 2, 1, 12, 30, 45, 123456, tzinfo=timezone.utc)


def evidence_for_fixture(raw_dir: Path, path: Path, source: str) -> None:
    """Test setup only: these dates are simulated downloads, not historical claims."""

    artifact = RawArtifact(source, path.relative_to(raw_dir).as_posix(), file_sha256(path), RETRIEVED_AT.isoformat())
    provenance_path(path).write_text(json.dumps(asdict(artifact), indent=2) + "\n", encoding="utf-8")
