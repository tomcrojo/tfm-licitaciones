"""CPV 2008 hierarchical reference dimension built from the official table.

The hierarchy lives in the 8-digit code body (the ninth check digit carries
no classification semantics). The first two digits form the division block;
each subsequent digit is one additional classification level, so codes reach
level 7 (Reglamento (CE) 213/2008; Guía CPV 2008). A code's parent is
obtained by zeroing its last significant digit, unless it is a division.
"""

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


def last_significant_position(code: str) -> int:
    """Return the 1-indexed position of the last non-zero digit (0 if none)."""

    for position in range(8, 0, -1):
        if code[position - 1] != "0":
            return position
    return 0


def cpv_level(code: str) -> int:
    """Return the CPV level (1 division block - 7) of a valid 8-digit code.

    The first two digits are read together as the division block, so a last
    significant digit at position 1 or 2 means level 1 (``30000000`` and
    ``03000000`` are both divisions). Each further significant position adds
    one level: ``60100000`` is level 2, ``03212200`` level 5, ``03212210``
    level 6 and ``03212211`` level 7. The level is always derived from the
    code string, never by counting hops from the root, because published
    ancestor chains can skip levels.
    """

    position = last_significant_position(code)
    return 1 if position <= 2 else position - 1


def structural_parent_code(code: str) -> str | None:
    """Return the immediate structural parent, or None for divisions.

    The parent zeroes the last significant digit of the code. Divisions
    (last significant digit inside the two-digit block) are roots: zeroing
    ``14000000`` would yield the non-existent ``10000000``, so they stop at
    the root instead.
    """

    position = last_significant_position(code)
    if position <= 2:
        return None
    return code[: position - 1] + "0" * (9 - position)


def _resolve_parent(code: str, codes: set[str]) -> str | None:
    """Return the nearest published ancestor of ``code``.

    The official CPV 2008 table omits a handful of intermediate nodes (for
    example ``39250000``). When the structural parent is absent, the parent
    resolves to the closest ancestor published in the vocabulary, so the
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

    Rows are sorted by ``cpv_code`` so the output is deterministic. Codes
    that are not 8-digit strings, that place a zero between the division
    block and their last significant digit (impossible in CPV 2008), that
    are duplicated, or whose ancestry cannot be resolved to a published
    node all raise ``ValueError``.
    """

    path = Path(source)
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing_columns = REQUIRED_SOURCE_COLUMNS - set(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(f"Missing required columns in {path}: {sorted(missing_columns)}")
        rows = list(reader)

    malformed = sorted(
        {row["code"] for row in rows if not CPV_CODE_PATTERN.match(row["code"])}
    )
    if malformed:
        raise ValueError(f"Invalid CPV codes (expected 8 digits as string): {malformed}")

    structural = sorted(
        {
            row["code"]
            for row in rows
            if last_significant_position(row["code"]) == 0
            or "0" in row["code"][2 : last_significant_position(row["code"])]
        }
    )
    if structural:
        raise ValueError(
            f"Structurally invalid CPV codes "
            f"(zero inside significant positions or no significant digit): {structural}"
        )

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
        raise ValueError(f"CPV codes without any published ancestor: {unresolvable}")

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
