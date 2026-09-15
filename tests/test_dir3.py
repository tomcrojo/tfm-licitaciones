"""Offline tests for the DIR3 organisational-unit reference dimension.

Fixtures are minimal XLSX files written with the standard library (inline
strings, no shared strings) that mirror the heterogeneous layouts of the six
official distributions. No network access is required.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path
from typing import Any

import polars as pl

from tfm_licitaciones.cli import main as cli_main
from tfm_licitaciones.dir3 import (
    ADMINISTRATION_SCOPES,
    DIR3_UNITS_SCHEMA,
    build_dir3_units,
    fetch_dir3_units,
    normalize_dir3_units,
)
from tfm_licitaciones.io import read_parquet, write_parquet


def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _column_letter(index: int) -> str:
    letters = ""
    while index:
        index, rest = divmod(index - 1, 26)
        letters = chr(65 + rest) + letters
    return letters


def write_xlsx(path: Path, rows: list[list[Any]]) -> None:
    """Write a minimal valid XLSX sheet using inline strings only."""

    width = max(len(row) for row in rows)
    row_xml: list[str] = []
    for row_number, row in enumerate(rows, start=1):
        cells = []
        for position in range(1, width + 1):
            value = row[position - 1] if position - 1 < len(row) else None
            if value is None or value == "":
                continue
            reference = f"{_column_letter(position)}{row_number}"
            if isinstance(value, int):
                cells.append(f'<c r="{reference}"><v>{value}</v></c>')
            else:
                cells.append(f'<c r="{reference}" t="inlineStr"><is><t>{_escape(str(value))}</t></is></c>')
        row_xml.append(f'<row r="{row_number}">{"".join(cells)}</row>')
    files = {
        "[Content_Types].xml": (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            "</Types>"
        ),
        "_rels/.rels": (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            "</Relationships>"
        ),
        "xl/workbook.xml": (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<sheets><sheet name="Unidades" sheetId="1" r:id="rId1"/></sheets></workbook>'
        ),
        "xl/_rels/workbook.xml.rels": (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
            "</Relationships>"
        ),
        "xl/worksheets/sheet1.xml": (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f'<sheetData>{"".join(row_xml)}</sheetData></worksheet>'
        ),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)


_BASE_HEADERS = [
    "C_ID_UD_ORGANICA",
    "C_DNM_UD_ORGANICA",
    "C_ID_NIVEL_ADMON",
    "C_ID_TIPO_ENT_PUBLICA",
    "N_NIVEL_JERARQUICO",
    "C_ID_DEP_UD_SUPERIOR",
    "C_DNM_UD_ORGANICA",
    "C_ID_DEP_UD_PRINCIPAL",
    "C_DNM_UD_ORGANICA",
    "B_SW_DEP_EDP_PRINCIPAL",
    "C_ID_DEP_EDP_PRINCIPAL",
    "C_DNM_UD_ORGANICA",
    "C_ID_ESTADO",
    "D_VIG_ALTA_OFICIAL",
    "NIF_CIF",
]


def _headers(variant: str) -> list[str]:
    """Reproduce the real header differences of the official distributions."""

    if variant == "AGE":
        headers = list(_BASE_HEADERS)
        headers[11] = "EDP.C_DNM_UD_ORGANICA"
        return headers
    if variant == "EELL":
        headers = list(_BASE_HEADERS)
        headers[5:5] = ["C_ID_AMB_PROVINCIA", "C_DESC_PROV"]
        return headers + ["CONTACTOS"]
    if variant == "JUSTICIA":
        headers = list(_BASE_HEADERS)
        headers[11] = "EDP.C_DNM_UD_ORGANICA"
        return headers + ["CARGADOR"]
    return list(_BASE_HEADERS)


def _unit_row(variant: str, values: dict[str, Any]) -> list[Any]:
    """Build one data row for a header variant from a canonical-ish mapping."""

    row = [None]  # empty leading column, as in the official distributions
    for header in _headers(variant):
        if header == "C_ID_AMB_PROVINCIA":
            row.append(values.get("province", "28"))
        elif header == "C_DESC_PROV":
            row.append(values.get("province_name", "Madrid"))
        elif header == "CONTACTOS":
            row.append(values.get("contact", ""))
        elif header == "CARGADOR":
            row.append(values.get("cargador", "N"))
        else:
            row.append(values.get(header, ""))
    return row


def write_scope_file(raw_dir: Path, scope: str, rows: list[dict[str, Any]], variant: str | None = None) -> Path:
    """Write one scope distribution with the official empty first row/column."""

    from tfm_licitaciones.dir3 import DIR3_SOURCES_BY_SCOPE

    source = DIR3_SOURCES_BY_SCOPE[scope]
    header_variant = variant or (scope if scope in {"AGE", "EELL", "JUSTICIA"} else "base")
    table = [[None] * (len(_headers(header_variant)) + 1), [None] + _headers(header_variant)]
    table.extend(_unit_row(header_variant, row) for row in rows)
    path = raw_dir / "dir3" / source.filename
    write_xlsx(path, table)
    return path


def sample_rows(scope: str, *, n: int = 2) -> list[dict[str, Any]]:
    """Deterministic representative rows for one scope."""

    from tfm_licitaciones.dir3 import DIR3_SOURCES_BY_SCOPE

    level = DIR3_SOURCES_BY_SCOPE[scope].nivel_admin
    prefix = {"AGE": "EA", "CCAA": "A0", "EELL": "LA", "UNIVERSIDADES": "U0", "OTRAS_INSTITUCIONES": "I0", "JUSTICIA": "J0"}[scope]
    root = f"{prefix}9999999"
    rows = [
        {
            "C_ID_UD_ORGANICA": f"{prefix}1000001",
            "C_DNM_UD_ORGANICA": f"Unidad raíz {scope}",
            "C_ID_NIVEL_ADMON": level,
            "C_ID_TIPO_ENT_PUBLICA": "MN",
            "N_NIVEL_JERARQUICO": "1",
            "C_ID_DEP_UD_SUPERIOR": root,
            "C_ID_DEP_UD_PRINCIPAL": f"{prefix}1000001",
            "C_ID_ESTADO": "V",
            "D_VIG_ALTA_OFICIAL": "",
            "NIF_CIF": f"S280100{scope[0]}",
        },
        {
            "C_ID_UD_ORGANICA": f"{prefix}1000002",
            "C_DNM_UD_ORGANICA": f"Unidad dependiente {scope}",
            "C_ID_NIVEL_ADMON": level,
            "C_ID_TIPO_ENT_PUBLICA": "",
            "N_NIVEL_JERARQUICO": "2",
            "C_ID_DEP_UD_SUPERIOR": f"{prefix}1000001",
            "C_ID_DEP_UD_PRINCIPAL": f"{prefix}1000001",
            "C_ID_ESTADO": "V",
            "D_VIG_ALTA_OFICIAL": "27/11/23",
            "NIF_CIF": "q0332001g",
        },
    ]
    return rows[:n]


class Dir3FixtureTestCase(unittest.TestCase):
    """Base case providing a temporary raw directory with all six scopes."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        self.raw_dir = self.tmp / "raw"
        for scope in ADMINISTRATION_SCOPES:
            write_scope_file(self.raw_dir, scope, sample_rows(scope))

    def build(self) -> tuple[pl.DataFrame, dict[str, Any]]:
        return build_dir3_units(self.raw_dir)


class NormalizeDir3UnitsTests(Dir3FixtureTestCase):
    """Schema, typing and value semantics of the canonical dimension."""

    def test_each_administration_scope_is_present(self) -> None:
        units, build = self.build()
        counts = {row["administration_scope"]: row["count"] for row in build["manifest"]["administration_scope_counts"]}
        self.assertEqual(set(counts), set(ADMINISTRATION_SCOPES))
        self.assertEqual(units.height, 12)
        self.assertEqual(build["manifest"]["row_count"], 12)
        self.assertEqual(sum(counts.values()), 12)

    def test_schema_normalization_matches_canonical_contract(self) -> None:
        units, _ = self.build()
        self.assertEqual(units.schema, DIR3_UNITS_SCHEMA)
        self.assertEqual(units.columns, list(DIR3_UNITS_SCHEMA.keys()))

    def test_dir3_identifiers_stay_strings(self) -> None:
        units, _ = self.build()
        self.assertEqual(units.schema["dir3_code"], pl.String)
        self.assertEqual(units.schema["parent_dir3_code"], pl.String)
        self.assertIn("A01000001", units.get_column("dir3_code").to_list())
        code = units.filter(pl.col("administration_scope") == "CCAA").get_column("dir3_code")[0]
        self.assertIsInstance(code, str)
        self.assertEqual(units.schema["hierarchy_level"], pl.Int16)
        self.assertEqual(set(units.get_column("hierarchy_level").to_list()), {1, 2})

    def test_hierarchy_references_are_preserved(self) -> None:
        units, build = self.build()
        child = units.filter(pl.col("hierarchy_level") == 2).sort("dir3_code")
        for row in child.to_dicts():
            parent = units.filter(pl.col("dir3_code") == row["parent_dir3_code"])
            self.assertEqual(parent.height, 1, row["dir3_code"])
            self.assertEqual(row["principal_dir3_code"], row["parent_dir3_code"])
        self.assertEqual(build["manifest"]["parents_unresolved"], 6)  # synthetic roots per scope

    def test_missing_optional_values_become_nulls(self) -> None:
        units, _ = self.build()
        dependent = units.filter(pl.col("hierarchy_level") == 2)
        self.assertTrue(dependent.get_column("public_entity_type").null_count() > 0)
        row = dependent.to_dicts()[0]
        self.assertIsNone(row["public_entity_type"])
        self.assertEqual(row["nif_cif"], "Q0332001G")  # lowercase NIF uppercased
        roots = units.filter(pl.col("hierarchy_level") == 1)
        self.assertEqual(roots.get_column("official_valid_from_raw").null_count(), roots.height)

    def test_official_valid_from_raw_preserves_source_string_verbatim(self) -> None:
        rows = sample_rows("AGE") + [
            {
                "C_ID_UD_ORGANICA": "EA1000003",
                "C_DNM_UD_ORGANICA": "Unidad histórica",
                "C_ID_NIVEL_ADMON": "1",
                "C_ID_TIPO_ENT_PUBLICA": "",
                "N_NIVEL_JERARQUICO": "2",
                "C_ID_DEP_UD_SUPERIOR": "EA1000001",
                "C_ID_DEP_UD_PRINCIPAL": "EA1000001",
                "C_ID_ESTADO": "V",
                "D_VIG_ALTA_OFICIAL": "01/01/25",  # genuinely ambiguous century: 1925 or 2025
                "NIF_CIF": "",
            }
        ]
        write_scope_file(self.raw_dir, "AGE", rows)
        units, _ = self.build()
        values = {row["dir3_code"]: row["official_valid_from_raw"] for row in units.to_dicts()}
        self.assertEqual(values["EA1000002"], "27/11/23")  # official string, byte for byte
        self.assertEqual(values["EA1000003"], "01/01/25")  # no century invented
        self.assertEqual(units.schema["official_valid_from_raw"], pl.String)

    def test_deterministic_ordering(self) -> None:
        rows = sample_rows("JUSTICIA", n=2)
        reversed_rows = list(reversed(rows))
        write_scope_file(self.raw_dir, "JUSTICIA", reversed_rows)
        units_a, _ = self.build()
        write_scope_file(self.raw_dir, "JUSTICIA", rows)
        units_b, _ = self.build()
        self.assertTrue(units_a.equals(units_b))
        self.assertEqual(units_a.get_column("dir3_code").to_list(), sorted(units_a.get_column("dir3_code").to_list()))

    def test_parquet_round_trip(self) -> None:
        units, _ = self.build()
        path = self.tmp / "reference" / "dir3" / "dir3_units.parquet"
        self.assertEqual(write_parquet(path, units), units.height)
        restored = read_parquet(path)
        self.assertEqual(restored.schema, DIR3_UNITS_SCHEMA)
        self.assertTrue(restored.equals(units))


class RejectionTests(Dir3FixtureTestCase):
    """Malformed or inconsistent rows are rejected observably, never silently."""

    def _rejected_reasons(self, scope: str) -> dict[str, int]:
        _, build = self.build()
        return next(source["rejection_reasons"] for source in build["manifest"]["sources"] if source["scope"] == scope)

    def test_missing_mandatory_field_is_counted(self) -> None:
        rows = sample_rows("AGE")
        rows[1]["C_DNM_UD_ORGANICA"] = ""
        write_scope_file(self.raw_dir, "AGE", rows)
        reasons = self._rejected_reasons("AGE")
        self.assertEqual(reasons.get("missing_name"), 1)
        units, build = self.build()
        self.assertEqual(next(s for s in build["manifest"]["sources"] if s["scope"] == "AGE")["accepted"], 1)

    def test_nivel_admin_mismatch_is_rejected(self) -> None:
        rows = sample_rows("CCAA")
        rows[1]["C_ID_NIVEL_ADMON"] = "1"
        write_scope_file(self.raw_dir, "CCAA", rows)
        self.assertEqual(self._rejected_reasons("CCAA").get("nivel_admin_mismatch"), 1)

    def test_invalid_status_is_rejected(self) -> None:
        rows = sample_rows("EELL")
        rows[1]["C_ID_ESTADO"] = "X"
        write_scope_file(self.raw_dir, "EELL", rows)
        self.assertEqual(self._rejected_reasons("EELL").get("invalid_status"), 1)

    def test_invalid_date_is_rejected(self) -> None:
        rows = sample_rows("UNIVERSIDADES")
        rows[1]["D_VIG_ALTA_OFICIAL"] = "2023-11-27"
        write_scope_file(self.raw_dir, "UNIVERSIDADES", rows)
        self.assertEqual(self._rejected_reasons("UNIVERSIDADES").get("invalid_valid_from_date"), 1)

    def test_invalid_code_format_is_rejected(self) -> None:
        rows = sample_rows("JUSTICIA")
        rows[1]["C_ID_UD_ORGANICA"] = "J1"
        write_scope_file(self.raw_dir, "JUSTICIA", rows)
        self.assertEqual(self._rejected_reasons("JUSTICIA").get("invalid_dir3_code_format"), 1)

    def test_invalid_hierarchy_level_is_rejected(self) -> None:
        rows = sample_rows("OTRAS_INSTITUCIONES")
        rows[1]["N_NIVEL_JERARQUICO"] = "dos"
        write_scope_file(self.raw_dir, "OTRAS_INSTITUCIONES", rows)
        self.assertEqual(self._rejected_reasons("OTRAS_INSTITUCIONES").get("invalid_hierarchy_level"), 1)


class DuplicateHandlingTests(Dir3FixtureTestCase):
    """The dir3_code primary key must be unique within and across scopes."""

    def test_within_scope_duplicate_raises(self) -> None:
        rows = sample_rows("AGE")
        duplicate = dict(rows[1])
        duplicate["C_DNM_UD_ORGANICA"] = "Unidad duplicada"
        rows.append(duplicate)
        write_scope_file(self.raw_dir, "AGE", rows)
        with self.assertRaises(ValueError):
            self.build()

    def test_across_scope_duplicate_raises(self) -> None:
        rows = sample_rows("CCAA")
        rows[1]["C_ID_UD_ORGANICA"] = sample_rows("AGE")[0]["C_ID_UD_ORGANICA"]
        rows[1]["C_ID_NIVEL_ADMON"] = "2"
        write_scope_file(self.raw_dir, "CCAA", rows)
        with self.assertRaises(ValueError):
            self.build()


class MalformedInputTests(Dir3FixtureTestCase):
    """Structural problems fail loudly instead of producing empty dimensions."""

    def test_missing_file_raises(self) -> None:
        (self.raw_dir / "dir3" / "dir3-unidades-age.xlsx").unlink()
        with self.assertRaises(RuntimeError):
            self.build()

    def test_not_an_xlsx_raises(self) -> None:
        path = self.raw_dir / "dir3" / "dir3-unidades-age.xlsx"
        path.write_bytes(b"not a spreadsheet")
        with self.assertRaises(RuntimeError):
            self.build()

    def test_missing_anchor_columns_raise(self) -> None:
        table = [
            [None] * 4,
            [None, "CODIGO", "NOMBRE", "OTRO"],
            [None, "EA00000011", "Unidad", "X"],
        ]
        path = self.raw_dir / "dir3" / "dir3-unidades-age.xlsx"
        write_xlsx(path, table)
        with self.assertRaises(ValueError):
            self.build()

    def test_header_in_first_row_without_padding_fails(self) -> None:
        table = [[None] + _BASE_HEADERS]
        table.append([None] + list(sample_rows("AGE")[0].values()))
        path = self.raw_dir / "dir3" / "dir3-unidades-age.xlsx"
        write_xlsx(path, table)
        with self.assertRaises(ValueError):
            self.build()

    def test_unknown_scope_is_rejected(self) -> None:
        path = self.raw_dir / "dir3" / "dir3-unidades-age.xlsx"
        with self.assertRaises(ValueError):
            normalize_dir3_units(path, "GALAXIA")


class FakeResponse:
    """Minimal context-managed HTTP response for fetch tests."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._position = 0

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = len(self._payload)
        chunk = self._payload[self._position : self._position + size]
        self._position += len(chunk)
        return chunk

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: Any) -> None:
        return None


class FakeOpener:
    """Opener returning queued payloads; records requested URLs."""

    def __init__(self, payloads: list[bytes]) -> None:
        self._payloads = list(payloads)
        self.requests: list[str] = []

    def open(self, request: Any, timeout: int = 0) -> FakeResponse:
        self.requests.append(request.full_url)
        if not self._payloads:
            raise OSError("no more canned responses")
        return FakeResponse(self._payloads.pop(0))


class FetchDir3UnitsTests(unittest.TestCase):
    """Deterministic download behaviour without network access."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        self.raw_dir = self.tmp / "raw"
        self.config = {
            "_project_root": str(self.tmp),
            "sources": {
                "dir3": {
                    "enabled": True,
                    "base_url": "https://example.test",
                    "timeout_seconds": 5,
                    "retry_attempts": 1,
                }
            },
        }
        from tfm_licitaciones.dir3 import DIR3_UNIT_SOURCES, _build_opener

        self.sources = DIR3_UNIT_SOURCES
        self._real_opener = _build_opener

    def _patch_opener(self, payloads: list[bytes]) -> FakeOpener:
        opener = FakeOpener(payloads)
        import tfm_licitaciones.dir3 as dir3_module

        dir3_module._build_opener = lambda: opener
        self.addCleanup(setattr, dir3_module, "_build_opener", self._real_opener)
        return opener

    def raw_dir_and_copy(self, scope: str) -> Path:
        from tfm_licitaciones.dir3 import DIR3_SOURCES_BY_SCOPE

        staging = self.tmp / "staging"
        target = staging / "dir3" / DIR3_SOURCES_BY_SCOPE[scope].filename
        if not target.exists():
            write_scope_file(staging, scope, sample_rows(scope))
        return target

    def test_successful_download_writes_deterministic_files(self) -> None:
        payloads = [self.raw_dir_and_copy(scope).read_bytes() for scope in ADMINISTRATION_SCOPES]
        opener = self._patch_opener(payloads)
        saved = fetch_dir3_units(self.raw_dir, self.config)
        self.assertEqual([path.name for path in saved], [source.filename for source in self.sources])
        self.assertEqual(len(opener.requests), 6)
        self.assertTrue(all(path.parent == self.raw_dir / "dir3" for path in saved))

    def test_existing_valid_files_are_skipped(self) -> None:
        for scope in ADMINISTRATION_SCOPES:
            write_scope_file(self.raw_dir, scope, sample_rows(scope))
        opener = self._patch_opener([])
        saved = fetch_dir3_units(self.raw_dir, self.config)
        self.assertEqual(len(saved), 6)
        self.assertEqual(opener.requests, [])

    def test_html_challenge_fails_clearly_and_leaves_no_partial_file(self) -> None:
        payloads = [b"<html>bobcmn TSPD challenge</html>"] * 6
        self._patch_opener(payloads)
        with self.assertRaises(RuntimeError) as ctx:
            fetch_dir3_units(self.raw_dir, self.config)
        self.assertIn("Descarga manual", str(ctx.exception))
        self.assertEqual(list((self.raw_dir / "dir3").glob("*.part")) if (self.raw_dir / "dir3").exists() else [], [])
        self.assertEqual(list((self.raw_dir / "dir3").glob("*.xlsx")) if (self.raw_dir / "dir3").exists() else [], [])

    def test_disabled_source_downloads_nothing(self) -> None:
        self.config["sources"]["dir3"]["enabled"] = False
        opener = self._patch_opener([])
        saved = fetch_dir3_units(self.raw_dir, self.config)
        self.assertEqual(saved, [])
        self.assertEqual(opener.requests, [])

    def test_missing_scope_after_attempts_fails(self) -> None:
        payloads = [self.raw_dir_and_copy(scope).read_bytes() for scope in ADMINISTRATION_SCOPES[:5]]
        payloads.append(b"PK\x03\x04 truncated")
        self._patch_opener(payloads)
        with self.assertRaises(RuntimeError) as ctx:
            fetch_dir3_units(self.raw_dir, self.config)
        self.assertIn("JUSTICIA", str(ctx.exception))


class CliDir3Tests(Dir3FixtureTestCase):
    """The offline ``dir3`` subcommand builds and persists the dimension."""

    def test_build_subcommand_writes_parquet_and_manifest(self) -> None:
        reference_dir = self.tmp / "reference"
        code = cli_main(
            [
                "dir3",
                "--raw-dir",
                str(self.raw_dir),
                "--reference-dir",
                str(reference_dir),
                "--config",
                str(_config_path()),
            ]
        )
        self.assertEqual(code, 0)
        units_path = reference_dir / "dir3" / "dir3_units.parquet"
        manifest_path = reference_dir / "dir3" / "build_manifest.json"
        self.assertTrue(units_path.is_file())
        self.assertTrue(manifest_path.is_file())
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["row_count"], 12)
        self.assertEqual(len(manifest["sources"]), 6)
        self.assertEqual(read_parquet(units_path).height, 12)


def _config_path() -> Path:
    from tfm_licitaciones.config import project_root

    return project_root() / "config" / "pipeline.json"


class CliIngestIsolationTests(unittest.TestCase):
    """DIR3 reference data must stay out of ordinary ``--source all`` ingestion."""

    def setUp(self) -> None:
        import json as json_module
        from unittest.mock import patch

        self._patch = patch
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        config = {
            "storage": {"raw_dir": str(self.tmp / "raw")},
            "sources": {
                "ted": {"enabled": False},
                "boe": {"enabled": False},
                "placsp": {"enabled": False},
                "dir3": {"enabled": True, "base_url": "https://example.test", "timeout_seconds": 5, "retry_attempts": 1},
            },
        }
        self.config_path = self.tmp / "pipeline.json"
        self.config_path.write_text(json_module.dumps(config), encoding="utf-8")

    def _ingest(self, source: str | None) -> None:
        from tfm_licitaciones import cli as cli_module

        arguments = [
            "ingest",
            "--start",
            "2026-09-15",
            "--end",
            "2026-09-15",
            "--config",
            str(self.config_path),
        ]
        if source is not None:
            arguments.extend(["--source", source])
        with self._patch.object(cli_module, "fetch_dir3_units", return_value=[]) as spy:
            code = cli_main(arguments)
        self.assertEqual(code, 0)
        return spy

    def test_default_ingestion_does_not_fetch_dir3(self) -> None:
        spy = self._ingest(None)  # default --source all
        self.assertEqual(spy.call_count, 0)

    def test_explicit_dir3_source_is_fetched(self) -> None:
        spy = self._ingest("dir3")
        self.assertEqual(spy.call_count, 1)


if __name__ == "__main__":
    unittest.main()
