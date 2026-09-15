"""Small deterministic JSONL, CSV and Parquet persistence helpers."""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import polars as pl


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Yield JSON objects from a JSONL file, reporting the failing line."""

    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in {path}:{line_number}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"Expected an object in {path}:{line_number}")
            yield value


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    """Write JSON objects with stable key ordering and return row count."""

    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            count += 1
    return count


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> int:
    """Write a UTF-8 CSV with a stable column order and return row count."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def write_json(path: Path, value: Any) -> None:
    """Write one JSON document with UTF-8 encoding and stable formatting."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_parquet(path: Path, frame: pl.DataFrame) -> int:
    """Write a typed DataFrame to Parquet and return the row count."""

    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)
    return frame.height


def read_parquet(path: Path) -> pl.DataFrame:
    """Read a Parquet file into a typed DataFrame."""

    return pl.read_parquet(path)
