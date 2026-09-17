"""End-to-end test: Raw -> Bronze -> Silver -> Spark Gold -> DuckDB -> Tableau CSV.

Chain under test (production builders only, no reimplemented semantics)::

    placsp .atom fixture (1 PUB notice + 1 EV notice + 1 dated tombstone)
        -> run_pipeline (Bronze + canonical Silver Parquet)
        -> build_gold_from_silver (Spark current-state + open policy)
        -> build_analytics_duckdb (versioned DuckDB views)
        -> export_tableau_csvs (deterministic Tableau CSVs + manifest)

The public fixture in ``examples/demo/raw`` derives from the mini Atom with the
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
from datetime import datetime, timezone
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import duckdb

try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark = None

from tfm_licitaciones.analytics_duckdb import build_analytics_duckdb
from tfm_licitaciones.cpv import DEFAULT_CPV_SOURCE, build_cpv_dimension
from tfm_licitaciones.pipeline import run_pipeline
from tfm_licitaciones.gold_open_opportunities import build_gold_from_silver
from tfm_licitaciones.io import write_parquet
from tfm_licitaciones.silver import PROCUREMENT_EVENTS_FILENAME
from tfm_licitaciones.spark_foundation import PYSPARK_VERSION, create_spark_session
from tfm_licitaciones.tableau_export import TABLEAU_EXPORT_MANIFEST_FILENAME, export_tableau_csvs

DEMO_RAW = Path(__file__).resolve().parents[1] / "examples" / "demo" / "raw"
OPEN_REF = "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/10000101"
OPEN_PROCEDURE = f"placsp:procedure:{OPEN_REF}"


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
            shutil.copytree(DEMO_RAW, raw)
            reference_dir = root / "reference"
            write_parquet(reference_dir / "cpv_codes.parquet", build_cpv_dimension(DEFAULT_CPV_SOURCE))

            # Use the same Raw fixture and production pipeline as the public demo.
            medallion = run_pipeline(raw_dir=raw, output_root=root)
            ingestion = medallion["manifest"]["ingestion"]
            self.assertTrue(ingestion["passed"])
            self.assertTrue(medallion["quality"]["passed"])
            self.assertEqual(ingestion["parsed"], 2)
            self.assertEqual(ingestion["accepted"], 2)
            self.assertEqual(ingestion["rejected"], 0)
            self.assertEqual(ingestion["tombstones"], 1)
            self.assertEqual(medallion["manifest"]["counts"]["bronze_records"], 2)
            self.assertEqual(medallion["manifest"]["counts"]["silver_procurement_events"], 3)
            silver_dir = root / "silver"

            # -- Silver -> Gold (Spark) -----------------------------------
            gold_dir = root / "gold"
            gold = build_gold_from_silver(
                self.spark,
                silver_dir / PROCUREMENT_EVENTS_FILENAME,
                gold_dir,
                dir3_dimension=None,
                reference_dir=reference_dir,
                as_of=datetime(2026, 1, 1, tzinfo=timezone.utc),
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
            self.assertEqual(
                cpv_rows_out[0]["label_es"],
                "Servicios de hospedaje de operación de sitios web WWW.",
            )

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
