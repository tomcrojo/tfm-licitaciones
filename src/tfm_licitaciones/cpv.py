"""CPV 2008 hierarchical reference dimension built from the official table."""

from __future__ import annotations

import csv
import re
from pathlib import Path

import polars as pl

from .config import project_root

DEFAULT_CPV_SOURCE = project_root() / "config" / "reference" / "cpv2008_es.csv"

REQUIRED_SOURCE_COLUMNS = {"code", "nombre", "name"}

CPV_CODE_PATTERN = re.compile(r"^[0-9]{8}$")

CPV_SCHEMA = pl.Schema(
    {
        "cpv_code": pl.String,
        "label_es": pl.String,
        "label_en": pl.String,
        "level": pl.Int8,
        "parent_code": pl.String,
        "is_leaf": pl.Boolean,
    }
)


def cpv_level(code: str) -> int:
    """Return the official CPV level (1 division - 5 detail) of a valid code.

    The level is determined by the zero suffix of the 8-digit code:
    ``XX000000`` divisions, ``XXX00000`` groups, ``XXXX0000`` classes and
    ``XXXXX000`` categories; codes without such a suffix are level-5 details.
    """

    if code[2:] == "000000":
        return 1
    if code[3:] == "00000":
        return 2
    if code[4:] == "0000":
        return 3
    if code[5:] == "000":
        return 4
    return 5


def structural_parent_code(code: str) -> str | None:
    """Return the immediate structural parent, or None for divisions."""

    level = cpv_level(code)
    if level == 1:
        return None
    return code[:level] + "0" * (8 - level)


def _resolve_parent(code: str, codes: set[str]) -> str | None:
    """Return the nearest existing ancestor of ``code``.

    The official CPV 2008 table omits a handful of intermediate nodes (for
    example ``39250000``). When the structural parent is absent, the parent
    resolves to the closest ancestor that exists in the vocabulary, so the
    dimension never contains dangling foreign keys.
    """

    parent = structural_parent_code(code)
    while parent is not None and parent not in codes:
        parent = structural_parent_code(parent)
    return parent


def _clean_label(value: str | None) -> str | None:
    """Preserve the official label text, mapping blank labels to null."""

    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def build_cpv_dimension(source: str | Path = DEFAULT_CPV_SOURCE) -> pl.DataFrame:
    """Build the typed CPV 2008 reference dimension from the official CSV.

    Rows are sorted by ``cpv_code`` so the output is deterministic. Invalid
    or duplicate codes, missing source columns and codes whose ancestry
    cannot be resolved to an existing node all raise ``ValueError``.
    """

    path = Path(source)
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing_columns = REQUIRED_SOURCE_COLUMNS - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(f"Missing required columns in {path}: {sorted(missing_columns)}")
        rows = list(reader)

    invalid = sorted(
        {row["code"] for row in rows if not CPV_CODE_PATTERN.match(row["code"])}
    )
    if invalid:
        raise ValueError(f"Invalid CPV codes (expected 8 digits as string): {invalid}")

    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        if row["code"] in seen:
            duplicates.add(row["code"])
        seen.add(row["code"])
    if duplicates:
        raise ValueError(f"Duplicate CPV codes: {sorted(duplicates)}")

    codes = seen
    parents = {code: _resolve_parent(code, codes) for code in codes}
    unresolvable = sorted(
        code for code, parent in parents.items() if parent is None and cpv_level(code) > 1
    )
    if unresolvable:
        raise ValueError(f"CPV codes without any existing ancestor: {unresolvable}")

    internal_codes = {parent for parent in parents.values() if parent is not None}
    labels = {row["code"]: row for row in rows}
    ordered_codes = sorted(codes)
    frame = pl.DataFrame(
        {
            "cpv_code": ordered_codes,
            "label_es": [_clean_label(labels[code]["nombre"]) for code in ordered_codes],
            "label_en": [_clean_label(labels[code]["name"]) for code in ordered_codes],
            "level": [cpv_level(code) for code in ordered_codes],
            "parent_code": [parents[code] for code in ordered_codes],
            "is_leaf": [code not in internal_codes for code in ordered_codes],
        },
        schema=CPV_SCHEMA,
    )
    return frame
