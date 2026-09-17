"""End-to-end test: Raw -> Bronze -> Silver -> Spark Gold -> DuckDB -> Tableau CSV.

Chain under test (production builders only, no reimplemented semantics)::

    placsp .atom fixture (1 PUB notice + 1 EV notice + 1 dated tombstone)
        -> load_raw_records / bronze_frame / tombstone_frame
        -> build_procurement_events (canonical Silver Parquet)
        -> build_gold_from_silver (Spark current-state + open policy)
        -> build_analytics_duckdb (versioned DuckDB views)
        -> export_tableau_csvs (deterministic Tableau CSVs + manifest)

The fixture derives from ``tests/fixtures/placsp/mini-placsp.atom`` with the
first entry promoted ``EV -> PUB`` ("EN PLAZO", the only actionable PLACSP
status). Expected policy outcome: 1 open (PUB), 1 status_closed (EV),
1 deleted (dated tombstone over an unknown ref, resolved as its own
current-state row). PLACSP never publishes ``publication_date`` in canonical
Silver, so the open opportunity is observably undated
(``opportunities_monthly`` header-only, ``..._without_publication_date`` = 1).

PySpark follows the repo convention (``skipIf`` when the pinned runtime is
absent) so the plain offline ``discover`` stays green; CI runs this module
explicitly with ``pyspark==4.0.1``. DuckDB is a base dependency and is
required directly.
"""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark = None

try:
    from raw_fixtures import evidence_for_fixture
except ImportError:  # package-style run: python -m unittest tests.<module>
    from tests.raw_fixtures import evidence_for_fixture
from tfm_licitaciones.analytics_duckdb import build_analytics_duckdb
from tfm_licitaciones.bronze import bronze_frame, load_raw_records, tombstone_frame
from tfm_licitaciones.gold_enrichment import CPV_DIMENSION_FIELDS
from tfm_licitaciones.gold_open_opportunities import build_gold_from_silver
from tfm_licitaciones.io import write_parquet
from tfm_licitaciones.silver import PROCUREMENT_EVENTS_FILENAME, build_procurement_events
from tfm_licitaciones.spark_foundation import PYSPARK_VERSION, create_spark_session, schema_from_fields
from tfm_licitaciones.tableau_export import TABLEAU_EXPORT_MANIFEST_FILENAME, export_tableau_csvs

MINI_ATOM = Path(__file__).parent / "fixtures" / "placsp" / "mini-placsp.atom"

OPEN_REF = "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/10000101"
OPEN_PROCEDURE = f"placsp:procedure:{OPEN_REF}"

CPV_ROWS = [
    {
        "cpv_code": "72415000",
        "label_es": "Servicios TI",
        "label_en": "IT services",
        "level": 3,
        "parent_code": "72400000",
        "is_leaf": True,
    },
    {
        "cpv_code": "45212200",
        "label_es": "Deporte",
        "label_en": "Sports",
        "level": 3,
        "parent_code": "45200000",
        "is_leaf": True,
    },
]


def _open_atom_bytes() -> bytes:
    """Return the mini fixture with the first entry promoted EV -> PUB."""

    text = MINI_ATOM.read_text(encoding="utf-8")
    promoted = text.replace(
        '<cbc-place-ext:ContractFolderStatusCode languageID="es">EV</cbc-place-ext:ContractFolderStatusCode>',
        '<cbc-place-ext:ContractFolderStatusCode languageID="es">PUB</cbc-place-ext:ContractFolderStatusCode>',
        1,
    ).replace("Estado: EV", "Estado: PUB", 1)
    assert promoted != text, "promotion must change the first entry status"
    # Only the first entry is actionable; the second stays EV (closed).
    assert promoted.count(">PUB<") == 1, "exactly one PUB entry expected"
    assert promoted.count(">EV<") == 1, "exactly one EV entry expected"
    return promoted.encode("utf-8")


def _write_cpv_reference(reference_dir: Path) -> Path:
    """Write the DuckDB CPV dimension parquet consumed by build-analytics."""

    reference_dir.mkdir(parents=True, exist_ok=True)
    target = reference_dir / "cpv_codes.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE cpv (
                cpv_code VARCHAR NOT NULL,
                label_es VARCHAR,
                label_en VARCHAR,
                level TINYINT NOT NULL,
                parent_code VARCHAR,
                is_leaf BOOLEAN NOT NULL
            );
            INSERT INTO cpv VALUES
                ('72415000', 'Servicios TI', 'IT services', 3, '72400000', TRUE),
                ('45212200', 'Deporte', 'Sports', 3, '45200000', TRUE);
            """
        )
        connection.execute(f"COPY cpv TO '{target}' (FORMAT PARQUET)")
    finally:
        connection.close()
    return target


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


@unittest.skipIf(pyspark is None, f"requires pyspark=={PYSPARK_VERSION}")
class RawToTableauTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark_session(
            app_name="tfm-licitaciones-test-e2e-raw-to-tableau",
            master="local[1]",
            shuffle_partitions=2,
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_raw_to_tableau_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw"
            atom_path = raw / "placsp" / "placsp-open.atom"
            atom_path.parent.mkdir(parents=True)
            atom_path.write_bytes(_open_atom_bytes())
            evidence_for_fixture(raw, atom_path, "placsp")
            reference_dir = root / "reference"
            _write_cpv_reference(reference_dir)

            # -- Raw -> Bronze -> Silver ----------------------------------
            loaded = load_raw_records(raw)
            ingestion = loaded["ingestion"]
            self.assertTrue(ingestion["passed"])
            self.assertEqual(ingestion["parsed"], 2)
            self.assertEqual(ingestion["accepted"], 2)
            self.assertEqual(ingestion["rejected"], 0)
            self.assertEqual(ingestion["tombstones"], 1)

            records_frame = bronze_frame(loaded["bronze"])
            tombstones_frame = tombstone_frame(loaded["tombstones"])
            self.assertEqual(records_frame.height, 2)
            self.assertEqual(tombstones_frame.height, 1)
            events_frame = build_procurement_events(records_frame, tombstones_frame)
            # 2 notices + 1 dated tombstone, all preserved as history.
            self.assertEqual(events_frame.height, 3)

            silver_dir = root / "silver"
            silver_dir.mkdir(parents=True)
            write_parquet(silver_dir / PROCUREMENT_EVENTS_FILENAME, events_frame)

            # -- Silver -> Gold (Spark) -----------------------------------
            cpv_dimension = self.spark.createDataFrame(
                CPV_ROWS, schema=schema_from_fields(CPV_DIMENSION_FIELDS)
            )
            gold_dir = root / "gold"
            gold = build_gold_from_silver(
                self.spark,
                silver_dir / PROCUREMENT_EVENTS_FILENAME,
                gold_dir,
                cpv_dimension=cpv_dimension,
                dir3_dimension=None,
                reference_dir=reference_dir,
            )
            self.assertEqual(gold["current_state_rows"], 3)
            self.assertEqual(gold["open_opportunities_rows"], 1)
            manifest = json.loads(Path(gold["manifest"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["counts"]["open_opportunities"], 1)
            self.assertEqual(
                manifest["reasons"],
                {
                    "deadline_passed": 0,
                    "deleted": 1,
                    "insufficient_evidence": 0,
                    "open": 1,
                    "status_closed": 1,
                },
            )

            # -- Gold -> DuckDB -------------------------------------------
            analytics_dir = root / "analytics"
            analytics = build_analytics_duckdb(gold_dir, analytics_dir, reference_dir=reference_dir)
            self.assertTrue(analytics["cpv_dimension"]["available"])
            self.assertEqual(
                analytics["counts"]["open_opportunities"], 1,
            )
            self.assertEqual(analytics["counts"]["opportunity_cpv"], 1)
            self.assertEqual(analytics["counts"]["cpv_matched"], 1)
            self.assertEqual(analytics["counts"]["cpv_unmatched"], 0)

            # -- DuckDB -> Tableau CSV ------------------------------------
            exports_dir = root / "exports"
            exported = export_tableau_csvs(analytics_dir, exports_dir)
            tableau_dir = exports_dir / "tableau"
            self.assertEqual(
                {path.name for path in tableau_dir.glob("*.csv")},
                {
                    "open_opportunities.csv",
                    "opportunity_cpv.csv",
                    "buyer_summary.csv",
                    "cpv_summary.csv",
                    "opportunities_monthly.csv",
                },
            )
            self.assertEqual(
                exported["counts"],
                {
                    "open_opportunities": 1,
                    "opportunity_cpv": 1,
                    "buyer_summary": 1,
                    "cpv_summary": 1,
                    "opportunities_monthly": 0,
                },
            )
            manifest_path = tableau_dir / TABLEAU_EXPORT_MANIFEST_FILENAME
            self.assertTrue(manifest_path.is_file())
            tableau_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(tableau_manifest["format_version"], 1)
            for filename, entry in tableau_manifest["files"].items():
                _, rows = _read_csv_rows(tableau_dir / filename)
                self.assertEqual(len(rows), entry["row_count"], filename)

            # Representative business values survive the whole chain.
            header, open_rows = _read_csv_rows(tableau_dir / "open_opportunities.csv")
            self.assertIn("procedure_id", header)
            self.assertEqual(len(open_rows), 1)
            self.assertEqual(open_rows[0]["procedure_id"], OPEN_PROCEDURE)
            self.assertEqual(open_rows[0]["source"], "placsp")
            self.assertEqual(open_rows[0]["status"], "PUB")
            self.assertIn("cloud", open_rows[0]["title"].lower())
            self.assertEqual(open_rows[0]["buyer_id"], "E00000001")
            self.assertEqual(open_rows[0]["publication_date"], "")
            self.assertIn("72415000", open_rows[0]["cpv_codes"])

            _, cpv_rows_out = _read_csv_rows(tableau_dir / "opportunity_cpv.csv")
            self.assertEqual(len(cpv_rows_out), 1)
            self.assertEqual(cpv_rows_out[0]["cpv_code"], "72415000")
            self.assertEqual(cpv_rows_out[0]["cpv_position"], "0")
            self.assertEqual(cpv_rows_out[0]["cpv_matched"], "true")
            self.assertEqual(cpv_rows_out[0]["label_es"], "Servicios TI")

            _, buyer_rows = _read_csv_rows(tableau_dir / "buyer_summary.csv")
            self.assertEqual(len(buyer_rows), 1)
            self.assertEqual(buyer_rows[0]["buyer_id"], "E00000001")
            self.assertEqual(buyer_rows[0]["opportunities_count"], "1")

            # PLACSP canonical Silver carries no publication_date: the open
            # opportunity stays observably undated instead of inventing one.
            _, monthly_rows = _read_csv_rows(tableau_dir / "opportunities_monthly.csv")
            self.assertEqual(monthly_rows, [])
            connection = duckdb.connect(str(analytics_dir / "licitaciones.duckdb"), read_only=True)
            try:
                undated = connection.execute(
                    "SELECT count(*) FROM opportunities_without_publication_date"
                ).fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(undated, 1)

            # Rebuild determinism: same Gold rebuilds the same Tableau CSVs.
            before = {
                path.name: path.read_bytes() for path in sorted(tableau_dir.glob("*.csv"))
            }
            build_analytics_duckdb(gold_dir, analytics_dir, reference_dir=reference_dir)
            export_tableau_csvs(analytics_dir, exports_dir)
            after = {
                path.name: path.read_bytes() for path in sorted(tableau_dir.glob("*.csv"))
            }
            self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
