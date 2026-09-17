"""Tests for the DuckDB analytical layer over Gold open_opportunities.

The fixtures are minimal Parquet files written with DuckDB itself so the
offline suite needs no PySpark: the typed Parquet boundary (DECIMAL(20,2),
VARCHAR[], DATE, UTC timestamps) matches what the Spark Gold writer emits
and what ``read_parquet`` exposes in DuckDB. Numbered cases map to the
mandatory list: 1 build from fixture; 2 base grain/schema; 3 multi-CPV rows
only in opportunity_cpv; 4 stable cpv_position; 5 matched CPV label; 6
unmatched CPV preserved; 7 buyer_summary counts; 8 null buyer identity
policy; 9 cpv_summary cardinality/attribution; 10 monthly grouping; 11 null
publication_date observability; 12 DECIMAL precision; 13 rebuild
equivalence; 14 CLI smoke; 15 missing Gold fails clearly. Two extra cases
freeze the documented CPV-dimension-missing policy and the Gold glob
reconciliation.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import duckdb

from tfm_licitaciones.analytics_duckdb import (
    ANALYTICS_DUCKDB_FILENAME,
    build_analytics_duckdb,
    gold_open_opportunities_glob,
)

GOLD_DDL = """
CREATE TABLE gold (
    procedure_id VARCHAR NOT NULL,
    event_id VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    buyer_id VARCHAR,
    buyer_name VARCHAR,
    title VARCHAR,
    description VARCHAR,
    cpv_codes VARCHAR[] NOT NULL,
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
    ingested_at TIMESTAMPTZ NOT NULL
);
INSERT INTO gold VALUES
    ('placsp:procedure:1', 'placsp:notice:1@2026-01-10T08:00:00Z', 'placsp',
     'EA0000011', 'Ministerio de Pruebas', 'Servicio — Cañón 😊', 'desc 1',
     ['48000000', '72210000'], 12345.67, NULL, 'EUR', DATE '2026-01-15',
     '2026-01-10 08:00:00+00', '2026-03-01 12:00:00+00', 'PUB', 'ES300', 'ES',
     'https://example.test/1', '2026-01-10 08:05:00+00'),
    ('placsp:procedure:2', 'placsp:notice:2@2026-01-20T08:00:00Z', 'placsp',
     'EA0000011', 'Ministerio de Pruebas', 'Segundo expediente', 'desc 2',
     ['48000000'], 100.00, NULL, 'EUR', DATE '2026-02-01',
     '2026-01-20 08:00:00+00', NULL, 'PUB', NULL, 'ES',
     'https://example.test/2', '2026-01-20 08:05:00+00'),
    ('placsp:procedure:3', 'placsp:notice:3@2026-02-02T08:00:00Z', 'placsp',
     NULL, 'Ayuntamiento de Villaluenga', 'Obras — sin fecha', 'desc 3',
     ['45210000'], NULL, NULL, NULL, NULL,
     '2026-02-02 08:00:00+00', NULL, 'PUB', NULL, 'ES',
     'https://example.test/3', '2026-02-02 08:05:00+00'),
    ('ted:procedure:4', 'ted:notice:123457', 'ted',
     NULL, NULL, 'Unted buyerless notice', NULL,
     [], 50000.00, NULL, 'EUR', DATE '2026-01-20',
     NULL, NULL, NULL, NULL, 'DE',
     'https://example.test/4', '2026-01-21 09:00:00+00'),
    ('ted:procedure:5', 'ted:notice:123458', 'ted',
     'S2800123', 'Universidad Nacional', 'Unmatched CPV notice', NULL,
     ['99999999'], 9999.99, NULL, 'EUR', DATE '2026-01-15',
     NULL, NULL, NULL, NULL, 'ES',
     'https://example.test/5', '2026-01-16 09:00:00+00');
"""

CPV_DDL = """
CREATE TABLE cpv (
    cpv_code VARCHAR NOT NULL,
    label_es VARCHAR,
    label_en VARCHAR,
    level TINYINT NOT NULL,
    parent_code VARCHAR,
    is_leaf BOOLEAN NOT NULL
);
INSERT INTO cpv VALUES
    ('48000000', 'Paquetes de software', 'Software package', 1, NULL, FALSE),
    ('72210000', 'Servicios de consultoría en programación', 'Programming services', 3, '72200000', TRUE),
    ('45210000', 'Trabajos de construcción', 'Building construction work', 3, '45200000', TRUE);
"""


def write_gold_fixture(root: Path, *, with_cpv_dimension: bool = True) -> dict[str, Path]:
    """Write a minimal Gold + reference fixture tree and return its paths."""

    gold_dir = root / "gold"
    dataset_dir = gold_dir / "open_opportunities"
    dataset_dir.mkdir(parents=True)
    reference_dir = root / "reference"
    reference_dir.mkdir(parents=True)
    con = duckdb.connect()
    con.execute(GOLD_DDL)
    con.execute(f"COPY gold TO '{dataset_dir / 'part-0000.parquet'}' (FORMAT PARQUET)")
    if with_cpv_dimension:
        con.execute(CPV_DDL)
        con.execute(
            f"COPY cpv TO '{reference_dir / 'cpv_codes.parquet'}' (FORMAT PARQUET)"
        )
    con.close()
    return {
        "gold_dir": gold_dir,
        "analytics_dir": root / "analytics",
        "reference_dir": reference_dir,
    }


def query(db_path: Path, sql: str) -> list[tuple]:
    """Open the built DuckDB file and fetch all rows (UTC session)."""

    connection = duckdb.connect(str(db_path))
    connection.execute("SET TimeZone='UTC'")
    try:
        return connection.execute(sql).fetchall()
    finally:
        connection.close()


class AnalyticsDuckDBTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.paths = write_gold_fixture(self.root)
        self.result = build_analytics_duckdb(
            self.paths["gold_dir"],
            self.paths["analytics_dir"],
            reference_dir=self.paths["reference_dir"],
        )
        self.db = self.root / "analytics" / ANALYTICS_DUCKDB_FILENAME

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_01_builds_duckdb_from_gold_fixture(self) -> None:
        self.assertTrue(self.db.is_file())
        self.assertEqual(self.result["counts"]["open_opportunities"], 5)
        self.assertEqual(self.result["counts"]["opportunity_cpv"], 5)
        self.assertEqual(self.result["counts"]["cpv_matched"], 4)
        self.assertEqual(self.result["counts"]["cpv_unmatched"], 1)
        self.assertTrue(self.result["cpv_dimension"]["available"])
        self.assertEqual(self.result["duckdb_version"], duckdb.__version__)
        for view in (
            "open_opportunities",
            "opportunity_cpv",
            "buyer_summary",
            "cpv_summary",
            "opportunities_monthly",
            "opportunities_without_publication_date",
        ):
            self.assertIn(view, self.result["views"])

    def test_02_base_view_preserves_grain_and_schema(self) -> None:
        described = query(self.db, "DESCRIBE SELECT * FROM open_opportunities")
        types = {row[0]: row[1] for row in described}
        self.assertEqual(len(described), 19)
        self.assertEqual(types["procedure_id"], "VARCHAR")
        self.assertEqual(types["cpv_codes"], "VARCHAR[]")
        self.assertEqual(types["estimated_value"], "DECIMAL(20,2)")
        self.assertEqual(types["publication_date"], "DATE")
        self.assertIn(types["ingested_at"], ("TIMESTAMP", "TIMESTAMP WITH TIME ZONE"))
        rows = query(
            self.db,
            "SELECT count(*), count(DISTINCT procedure_id) FROM open_opportunities",
        )
        self.assertEqual(rows[0][0], 5)
        self.assertEqual(rows[0][0], rows[0][1])

    def test_03_multiple_cpv_rows_only_in_opportunity_cpv(self) -> None:
        base = query(
            self.db,
            "SELECT count(*) FROM open_opportunities "
            "WHERE procedure_id = 'placsp:procedure:1'",
        )[0][0]
        exploded = query(
            self.db,
            "SELECT count(*) FROM opportunity_cpv "
            "WHERE procedure_id = 'placsp:procedure:1'",
        )[0][0]
        self.assertEqual(base, 1)
        self.assertEqual(exploded, 2)
        no_cpv = query(
            self.db,
            "SELECT count(*) FROM opportunity_cpv WHERE procedure_id = 'ted:procedure:4'",
        )[0][0]
        self.assertEqual(no_cpv, 0)

    def test_04_cpv_position_is_stable(self) -> None:
        expected = [(0, "48000000"), (1, "72210000")]
        positions = query(
            self.db,
            "SELECT cpv_position, cpv_code FROM opportunity_cpv "
            "WHERE procedure_id = 'placsp:procedure:1' ORDER BY cpv_position",
        )
        self.assertEqual(positions, expected)
        build_analytics_duckdb(
            self.paths["gold_dir"],
            self.paths["analytics_dir"],
            reference_dir=self.paths["reference_dir"],
        )
        self.assertEqual(
            positions,
            query(
                self.db,
                "SELECT cpv_position, cpv_code FROM opportunity_cpv "
                "WHERE procedure_id = 'placsp:procedure:1' ORDER BY cpv_position",
            ),
        )

    def test_05_matched_cpv_gets_official_labels(self) -> None:
        rows = query(
            self.db,
            "SELECT label_es, label_en, level, parent_code, is_leaf, cpv_matched "
            "FROM opportunity_cpv WHERE cpv_code = '48000000' ORDER BY procedure_id",
        )
        self.assertEqual(len(rows), 2)
        for label_es, label_en, level, parent_code, is_leaf, matched in rows:
            self.assertEqual(label_es, "Paquetes de software")
            self.assertEqual(label_en, "Software package")
            self.assertEqual(level, 1)
            self.assertIsNone(parent_code)
            self.assertFalse(is_leaf)
            self.assertTrue(matched)

    def test_06_unmatched_cpv_is_preserved(self) -> None:
        rows = query(
            self.db,
            "SELECT cpv_matched, label_es, label_en, level FROM opportunity_cpv "
            "WHERE cpv_code = '99999999'",
        )
        self.assertEqual(len(rows), 1)
        matched, label_es, label_en, level = rows[0]
        self.assertFalse(matched)
        self.assertIsNone(label_es)
        self.assertIsNone(label_en)
        self.assertIsNone(level)
        summary = query(
            self.db,
            "SELECT opportunities_count FROM cpv_summary WHERE cpv_code = '99999999'",
        )
        self.assertEqual(summary, [(1,)])

    def test_07_buyer_summary_counts_opportunities(self) -> None:
        rows = dict(
            query(
                self.db,
                "SELECT buyer_id, opportunities_count FROM buyer_summary "
                "WHERE buyer_id IS NOT NULL",
            )
        )
        self.assertEqual(rows.get("EA0000011"), 2)
        self.assertEqual(rows.get("S2800123"), 1)
        total = query(self.db, "SELECT sum(opportunities_count) FROM buyer_summary")[0][0]
        self.assertEqual(total, 5)
        distinct_cpv = query(
            self.db,
            "SELECT distinct_cpv_count FROM buyer_summary WHERE buyer_id = 'EA0000011'",
        )[0][0]
        self.assertEqual(distinct_cpv, 2)

    def test_08_null_buyer_does_not_invent_identity(self) -> None:
        rows = dict(
            query(
                self.db,
                "SELECT (buyer_id, buyer_name, identity_basis), opportunities_count "
                "FROM buyer_summary WHERE buyer_id IS NULL",
            )
        )
        self.assertEqual(
            rows.get((None, "Ayuntamiento de Villaluenga", "buyer_name")), 1
        )
        self.assertEqual(rows.get((None, None, "buyer_name")), 1)
        self.assertEqual(len(rows), 2)

    def test_09_cpv_summary_keeps_cardinality_and_attribution(self) -> None:
        rows = dict(
            query(
                self.db,
                "SELECT cpv_code, (opportunities_count, distinct_buyers) FROM cpv_summary",
            )
        )
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows["48000000"], (2, 1))
        self.assertEqual(rows["72210000"], (1, 1))
        attributed = query(
            self.db,
            "SELECT total_estimated_value FROM cpv_summary WHERE cpv_code = '48000000'",
        )[0][0]
        self.assertEqual(attributed, Decimal("12445.67"))

    def test_10_opportunities_monthly_groups_correctly(self) -> None:
        rows = dict(
            query(
                self.db,
                "SELECT (month, source), (opportunities_count, distinct_buyers) "
                "FROM opportunities_monthly",
            )
        )
        self.assertEqual(rows[(dt.date(2026, 1, 1), "placsp")], (1, 1))
        self.assertEqual(rows[(dt.date(2026, 1, 1), "ted")], (2, 1))
        self.assertEqual(rows[(dt.date(2026, 2, 1), "placsp")], (1, 1))
        self.assertEqual(len(rows), 3)

    def test_11_null_publication_date_stays_observable_without_invention(self) -> None:
        undated = query(
            self.db,
            "SELECT source, opportunities_count "
            "FROM opportunities_without_publication_date",
        )
        self.assertEqual(undated, [("placsp", 1)])
        never_dated = query(
            self.db,
            "SELECT count(*) FROM opportunities_monthly WHERE month IS NULL",
        )[0][0]
        self.assertEqual(never_dated, 0)
        dated_total = query(
            self.db, "SELECT sum(opportunities_count) FROM opportunities_monthly"
        )[0][0]
        self.assertEqual(dated_total, 4)

    def test_12_amounts_keep_decimal_precision(self) -> None:
        types = dict(
            (row[0], row[1])
            for row in query(self.db, "DESCRIBE SELECT * FROM opportunity_cpv")
        )
        self.assertEqual(types["estimated_value"], "DECIMAL(20,2)")
        total = query(
            self.db, "SELECT sum(estimated_value) FROM open_opportunities"
        )[0][0]
        self.assertIsInstance(total, Decimal)
        self.assertEqual(total, Decimal("72445.66"))

    def test_13_rebuild_produces_same_results(self) -> None:
        snapshot = query(
            self.db,
            "SELECT * FROM opportunity_cpv ORDER BY procedure_id, cpv_position",
        )
        buyers = query(
            self.db,
            "SELECT procedure_id FROM open_opportunities ORDER BY procedure_id",
        )
        build_analytics_duckdb(
            self.paths["gold_dir"],
            self.paths["analytics_dir"],
            reference_dir=self.paths["reference_dir"],
        )
        self.assertEqual(
            snapshot,
            query(
                self.db,
                "SELECT * FROM opportunity_cpv ORDER BY procedure_id, cpv_position",
            ),
        )
        self.assertEqual(
            buyers,
            query(
                self.db,
                "SELECT procedure_id FROM open_opportunities ORDER BY procedure_id",
            ),
        )

    def test_14_cli_smoke(self) -> None:
        from tfm_licitaciones.cli import main

        with tempfile.TemporaryDirectory() as cli_tmp:
            paths = write_gold_fixture(Path(cli_tmp))
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                exit_code = main(
                    [
                        "build-analytics",
                        "--gold-dir",
                        str(paths["gold_dir"]),
                        "--analytics-dir",
                        str(paths["analytics_dir"]),
                        "--reference-dir",
                        str(paths["reference_dir"]),
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(buffer.getvalue())
            self.assertEqual(payload["counts"]["open_opportunities"], 5)
            self.assertIn("opportunity_cpv", payload["views"])
            self.assertTrue(payload["cpv_dimension_available"])
            self.assertTrue(
                (Path(cli_tmp) / "analytics" / ANALYTICS_DUCKDB_FILENAME).is_file()
            )

    def test_15_missing_gold_fails_with_clear_error(self) -> None:
        empty = self.root / "empty-gold"
        empty.mkdir()
        with self.assertRaisesRegex(ValueError, "build-gold"):
            build_analytics_duckdb(empty, self.root / "analytics-2")

    def test_missing_cpv_dimension_uses_documented_fallback(self) -> None:
        result = build_analytics_duckdb(
            self.paths["gold_dir"],
            self.root / "analytics-nodim",
            reference_dir=self.root / "no-reference",
        )
        self.assertFalse(result["cpv_dimension"]["available"])
        self.assertEqual(result["counts"]["cpv_matched"], 0)
        self.assertEqual(result["counts"]["cpv_unmatched"], 5)
        labels = query(
            Path(result["duckdb_path"]),
            "SELECT count(*) FROM opportunity_cpv WHERE label_es IS NOT NULL",
        )[0][0]
        self.assertEqual(labels, 0)

    def test_gold_glob_reconciles_with_view(self) -> None:
        glob = gold_open_opportunities_glob(self.paths["gold_dir"])
        direct = (
            duckdb.connect()
            .execute(f"SELECT count(*) FROM read_parquet('{glob}')")
            .fetchone()[0]
        )
        self.assertEqual(direct, self.result["counts"]["open_opportunities"])

    def test_failed_build_leaves_no_unverified_artifact(self) -> None:
        broken = self.root / "broken-gold" / "open_opportunities"
        broken.mkdir(parents=True)
        duckdb.connect().execute(
            f"COPY (SELECT 1 AS wrong_column) TO '{broken / 'part.parquet'}' (FORMAT PARQUET)"
        )
        target = self.root / "analytics-broken"
        with self.assertRaisesRegex(ValueError, "cannot satisfy the analytical views"):
            build_analytics_duckdb(
                self.root / "broken-gold",
                target,
                reference_dir=self.paths["reference_dir"],
            )
        self.assertFalse((target / ANALYTICS_DUCKDB_FILENAME).exists())


if __name__ == "__main__":
    unittest.main()
