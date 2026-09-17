"""Fast offline tests for deterministic DuckDB -> Tableau CSV exports."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

from tfm_licitaciones.analytics_duckdb import ANALYTICS_DUCKDB_FILENAME
from tfm_licitaciones.tableau_export import (
    TABLEAU_EXPORT_MANIFEST_FILENAME,
    export_tableau_csvs,
)


EXPECTED_HEADERS = {
    "open_opportunities.csv": [
        "procedure_id",
        "event_id",
        "source",
        "buyer_id",
        "buyer_name",
        "title",
        "description",
        "cpv_codes",
        "estimated_value",
        "awarded_value",
        "currency",
        "publication_date",
        "source_updated_at",
        "deadline",
        "status",
        "nuts_code",
        "country",
        "source_url",
        "ingested_at",
    ],
    "opportunity_cpv.csv": [
        "procedure_id",
        "source",
        "buyer_id",
        "publication_date",
        "estimated_value",
        "currency",
        "cpv_position",
        "cpv_code",
        "cpv_matched",
        "label_es",
        "label_en",
        "level",
        "parent_code",
        "is_leaf",
    ],
    "buyer_summary.csv": [
        "buyer_id",
        "buyer_name",
        "identity_basis",
        "opportunities_count",
        "total_estimated_value",
        "avg_estimated_value",
        "first_publication_date",
        "last_publication_date",
        "earliest_deadline",
        "distinct_cpv_count",
    ],
    "cpv_summary.csv": [
        "cpv_code",
        "label_es",
        "label_en",
        "opportunities_count",
        "distinct_buyers",
        "total_estimated_value",
        "avg_estimated_value",
    ],
    "opportunities_monthly.csv": [
        "month",
        "source",
        "opportunities_count",
        "total_estimated_value",
        "avg_estimated_value",
        "distinct_buyers",
    ],
}

EXPECTED_COUNTS = {
    "open_opportunities": 2,
    "opportunity_cpv": 3,
    "buyer_summary": 3,
    "cpv_summary": 2,
    "opportunities_monthly": 2,
}


def write_analytics_fixture(analytics_dir: Path) -> Path:
    analytics_dir.mkdir(parents=True, exist_ok=True)
    db = analytics_dir / ANALYTICS_DUCKDB_FILENAME
    con = duckdb.connect(str(db))
    con.execute(
        """
        CREATE TABLE _open_opportunities (
            procedure_id VARCHAR,
            event_id VARCHAR,
            source VARCHAR,
            buyer_id VARCHAR,
            buyer_name VARCHAR,
            title VARCHAR,
            description VARCHAR,
            cpv_codes VARCHAR[],
            estimated_value DECIMAL(20,2),
            awarded_value DECIMAL(20,2),
            currency VARCHAR,
            publication_date DATE,
            source_updated_at TIMESTAMPTZ,
            deadline TIMESTAMPTZ,
            status VARCHAR,
            nuts_code VARCHAR,
            country VARCHAR,
            source_url VARCHAR,
            ingested_at TIMESTAMPTZ
        );
        INSERT INTO _open_opportunities VALUES
            ('procedure:b', 'event:b', 'ted', 'B2', 'Universidad — Ñ',
             'Segundo expediente', NULL, ['72210000'], 0.10, NULL, 'EUR',
             DATE '2026-02-01', TIMESTAMPTZ '2026-01-20 08:00:00+00', NULL,
             'PUB', NULL, 'ES', 'https://example.test/b',
             TIMESTAMPTZ '2026-01-20 08:05:00+00'),
            ('procedure:a', 'event:a', 'placsp', 'B1', 'Ministerio de Pruebas',
             'Servicio — Cañón 😊', 'descripción', ['48000000', '72210000'],
             12345.67, 100.00, 'EUR', DATE '2026-01-15',
             TIMESTAMPTZ '2026-01-10 08:00:00+00',
             TIMESTAMPTZ '2026-03-01 12:00:00+00', 'PUB', 'ES300', 'ES',
             'https://example.test/a', TIMESTAMPTZ '2026-01-10 08:05:00+00');
        CREATE VIEW open_opportunities AS SELECT * FROM _open_opportunities;

        CREATE TABLE _opportunity_cpv (
            procedure_id VARCHAR,
            source VARCHAR,
            buyer_id VARCHAR,
            publication_date DATE,
            estimated_value DECIMAL(20,2),
            currency VARCHAR,
            cpv_position BIGINT,
            cpv_code VARCHAR,
            cpv_matched BOOLEAN,
            label_es VARCHAR,
            label_en VARCHAR,
            level TINYINT,
            parent_code VARCHAR,
            is_leaf BOOLEAN
        );
        INSERT INTO _opportunity_cpv VALUES
            ('procedure:b', 'ted', 'B2', DATE '2026-02-01', 0.10, 'EUR', 0,
             '72210000', TRUE, 'Programación', 'Programming', 3, '72200000', TRUE),
            ('procedure:a', 'placsp', 'B1', DATE '2026-01-15', 12345.67, 'EUR', 1,
             '72210000', TRUE, 'Programación', 'Programming', 3, '72200000', TRUE),
            ('procedure:a', 'placsp', 'B1', DATE '2026-01-15', 12345.67, 'EUR', 0,
             '48000000', TRUE, 'Paquetes de software', 'Software package', 1, NULL, FALSE);
        CREATE VIEW opportunity_cpv AS SELECT * FROM _opportunity_cpv;

        CREATE TABLE _buyer_summary (
            buyer_id VARCHAR,
            buyer_name VARCHAR,
            identity_basis VARCHAR,
            opportunities_count BIGINT,
            total_estimated_value DECIMAL(38,2),
            avg_estimated_value DECIMAL(38,6),
            first_publication_date DATE,
            last_publication_date DATE,
            earliest_deadline TIMESTAMPTZ,
            distinct_cpv_count BIGINT
        );
        INSERT INTO _buyer_summary VALUES
            (NULL, 'Ayuntamiento sin ID', 'buyer_name', 1, NULL, NULL, NULL, NULL, NULL, 1),
            ('B2', 'Universidad — Ñ', 'buyer_id', 1, 0.10, 0.100000,
             DATE '2026-02-01', DATE '2026-02-01', NULL, 1),
            ('B1', 'Ministerio de Pruebas', 'buyer_id', 1, 12345.67, 12345.670000,
             DATE '2026-01-15', DATE '2026-01-15',
             TIMESTAMPTZ '2026-03-01 12:00:00+00', 2);
        CREATE VIEW buyer_summary AS SELECT * FROM _buyer_summary;

        CREATE TABLE _cpv_summary (
            cpv_code VARCHAR,
            label_es VARCHAR,
            label_en VARCHAR,
            opportunities_count BIGINT,
            distinct_buyers BIGINT,
            total_estimated_value DECIMAL(38,2),
            avg_estimated_value DECIMAL(38,6)
        );
        INSERT INTO _cpv_summary VALUES
            ('72210000', 'Programación', 'Programming', 2, 2, 12345.77, 6172.885000),
            ('48000000', 'Paquetes de software', 'Software package', 1, 1,
             12345.67, 12345.670000);
        CREATE VIEW cpv_summary AS SELECT * FROM _cpv_summary;

        CREATE TABLE _opportunities_monthly (
            month DATE,
            source VARCHAR,
            opportunities_count BIGINT,
            total_estimated_value DECIMAL(38,2),
            avg_estimated_value DECIMAL(38,6),
            distinct_buyers BIGINT
        );
        INSERT INTO _opportunities_monthly VALUES
            (DATE '2026-02-01', 'ted', 1, 0.10, 0.100000, 1),
            (DATE '2026-01-01', 'placsp', 1, 12345.67, 12345.670000, 1);
        CREATE VIEW opportunities_monthly AS SELECT * FROM _opportunities_monthly;
        """
    )
    con.close()
    return db


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


class TableauExportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.analytics_dir = self.root / "analytics"
        self.exports_dir = self.root / "exports"
        self.db = write_analytics_fixture(self.analytics_dir)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def export(self) -> dict:
        return export_tableau_csvs(self.analytics_dir, self.exports_dir)

    def test_01_exports_from_valid_duckdb_and_creates_directory(self) -> None:
        result = self.export()
        export_dir = self.exports_dir / "tableau"
        self.assertTrue(export_dir.is_dir())
        self.assertEqual(Path(result["export_dir"]), export_dir)

    def test_02_generates_five_required_csvs(self) -> None:
        self.export()
        generated = {path.name for path in (self.exports_dir / "tableau").glob("*.csv")}
        self.assertEqual(generated, set(EXPECTED_HEADERS))

    def test_03_manifest_exists_and_reconciles_row_counts(self) -> None:
        result = self.export()
        manifest_path = self.exports_dir / "tableau" / TABLEAU_EXPORT_MANIFEST_FILENAME
        self.assertTrue(manifest_path.is_file())
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["format_version"], 1)
        self.assertEqual(Path(manifest["source_duckdb"]), self.db.resolve())
        self.assertEqual(result["counts"], EXPECTED_COUNTS)
        for filename, entry in manifest["files"].items():
            view = entry["view"]
            self.assertEqual(entry["row_count"], EXPECTED_COUNTS[view])
            _, rows = read_csv(self.exports_dir / "tableau" / filename)
            self.assertEqual(len(rows), EXPECTED_COUNTS[view])

    def test_04_headers_match_view_contracts(self) -> None:
        self.export()
        for filename, expected in EXPECTED_HEADERS.items():
            header, _ = read_csv(self.exports_dir / "tableau" / filename)
            self.assertEqual(header, expected)

    def test_05_utf8_is_preserved(self) -> None:
        self.export()
        _, rows = read_csv(self.exports_dir / "tableau" / "open_opportunities.csv")
        self.assertEqual(rows[0]["title"], "Servicio — Cañón 😊")
        self.assertEqual(rows[0]["description"], "descripción")

    def test_06_decimal_serialization_does_not_degrade_to_float(self) -> None:
        self.export()
        _, rows = read_csv(self.exports_dir / "tableau" / "open_opportunities.csv")
        self.assertEqual(rows[0]["estimated_value"], "12345.67")
        self.assertEqual(rows[1]["estimated_value"], "0.10")
        _, buyers = read_csv(self.exports_dir / "tableau" / "buyer_summary.csv")
        b1 = next(row for row in buyers if row["buyer_id"] == "B1")
        self.assertEqual(b1["avg_estimated_value"], "12345.670000")

    def test_07_dates_and_timestamps_are_stable_iso(self) -> None:
        self.export()
        _, rows = read_csv(self.exports_dir / "tableau" / "open_opportunities.csv")
        self.assertEqual(rows[0]["publication_date"], "2026-01-15")
        self.assertEqual(rows[0]["source_updated_at"], "2026-01-10T08:00:00.000000Z")
        self.assertEqual(rows[0]["deadline"], "2026-03-01T12:00:00.000000Z")

    def test_08_nulls_are_consistently_empty_csv_fields(self) -> None:
        self.export()
        _, rows = read_csv(self.exports_dir / "tableau" / "open_opportunities.csv")
        self.assertEqual(rows[1]["description"], "")
        self.assertEqual(rows[1]["awarded_value"], "")
        self.assertEqual(rows[1]["deadline"], "")
        _, buyers = read_csv(self.exports_dir / "tableau" / "buyer_summary.csv")
        unnamed = next(row for row in buyers if row["identity_basis"] == "buyer_name")
        self.assertEqual(unnamed["buyer_id"], "")
        self.assertEqual(unnamed["total_estimated_value"], "")

    def test_09_open_cpv_codes_are_stable_json_without_exploding_rows(self) -> None:
        self.export()
        _, rows = read_csv(self.exports_dir / "tableau" / "open_opportunities.csv")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["cpv_codes"], '["48000000","72210000"]')
        self.assertEqual(rows[1]["cpv_codes"], '["72210000"]')
        self.assertEqual(len({row["procedure_id"] for row in rows}), 2)

    def test_10_order_is_deterministic_by_documented_grain(self) -> None:
        self.export()
        _, base = read_csv(self.exports_dir / "tableau" / "open_opportunities.csv")
        self.assertEqual([r["procedure_id"] for r in base], ["procedure:a", "procedure:b"])
        _, cpv = read_csv(self.exports_dir / "tableau" / "opportunity_cpv.csv")
        self.assertEqual(
            [(r["procedure_id"], r["cpv_position"], r["cpv_code"]) for r in cpv],
            [
                ("procedure:a", "0", "48000000"),
                ("procedure:a", "1", "72210000"),
                ("procedure:b", "0", "72210000"),
            ],
        )
        _, monthly = read_csv(self.exports_dir / "tableau" / "opportunities_monthly.csv")
        self.assertEqual(
            [(r["month"], r["source"]) for r in monthly],
            [("2026-01-01", "placsp"), ("2026-02-01", "ted")],
        )

    def test_11_second_export_produces_same_logical_content(self) -> None:
        self.export()
        first = {}
        for filename in EXPECTED_HEADERS:
            first[filename] = read_csv(self.exports_dir / "tableau" / filename)
        first_manifest = json.loads(
            (self.exports_dir / "tableau" / TABLEAU_EXPORT_MANIFEST_FILENAME).read_text(
                encoding="utf-8"
            )
        )
        self.export()
        for filename in EXPECTED_HEADERS:
            self.assertEqual(read_csv(self.exports_dir / "tableau" / filename), first[filename])
        self.assertEqual(
            json.loads(
                (self.exports_dir / "tableau" / TABLEAU_EXPORT_MANIFEST_FILENAME).read_text(
                    encoding="utf-8"
                )
            ),
            first_manifest,
        )

    def test_12_missing_duckdb_fails_clearly_without_creating_one(self) -> None:
        missing_dir = self.root / "missing-analytics"
        expected_db = missing_dir / ANALYTICS_DUCKDB_FILENAME
        with self.assertRaisesRegex(ValueError, "DuckDB analytics database not found"):
            export_tableau_csvs(missing_dir, self.exports_dir)
        self.assertFalse(expected_db.exists())

    def test_13_missing_required_view_fails_clearly(self) -> None:
        con = duckdb.connect(str(self.db))
        con.execute("DROP VIEW cpv_summary")
        con.close()
        with self.assertRaisesRegex(ValueError, "missing required Tableau view.*cpv_summary"):
            self.export()

    def test_14_cli_smoke(self) -> None:
        config_path = self.root / "pipeline.json"
        config_path.write_text(
            json.dumps(
                {
                    "storage": {
                        "analytics_dir": str(self.analytics_dir),
                        "exports_dir": str(self.exports_dir),
                    }
                }
            ),
            encoding="utf-8",
        )
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "tfm_licitaciones.cli",
                "export-tableau",
                "--config",
                str(config_path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(Path(payload["export_dir"]), self.exports_dir / "tableau")
        self.assertEqual(payload["counts"], EXPECTED_COUNTS)
        self.assertEqual(set(payload["files"]), set(EXPECTED_COUNTS))

    def test_15_no_empty_csv_when_corresponding_view_has_rows(self) -> None:
        self.export()
        for view, count in EXPECTED_COUNTS.items():
            self.assertGreater(count, 0)
            path = self.exports_dir / "tableau" / f"{view}.csv"
            self.assertGreater(path.stat().st_size, 0)
            _, rows = read_csv(path)
            self.assertEqual(len(rows), count)


if __name__ == "__main__":
    unittest.main()
