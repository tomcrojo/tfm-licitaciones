"""Tests for the minimal Gold enrichments (official CPV and DIR3 attributes).

Contract-level checks run offline; the join/metric tests require the pinned
PySpark runtime. Fixtures are deliberately tiny, while the CPV and DIR3
dimension readers are additionally exercised against real Polars-written
Parquet so the production Polars -> Spark boundary is covered.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import polars as pl

try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark = None

from tfm_licitaciones.cpv import DEFAULT_CPV_SOURCE, build_cpv_dimension
from tfm_licitaciones.dir3 import DIR3_UNITS_SCHEMA
from tfm_licitaciones.gold_contract import CANONICAL_SILVER_FIELDS
from tfm_licitaciones.gold_enrichment import (
    BUYER_DIR3_ATTRIBUTE_FIELDS,
    BUYER_DIR3_ENRICHED_DATASET,
    BUYER_DIR3_ENRICHED_FIELDS,
    CPV_DIMENSION_FIELDS,
    CPV_ENRICHED_DATASET,
    CPV_ENRICHED_FIELDS,
    DIR3_DIMENSION_FIELDS,
    dir3_units_dimension,
    dir3_units_path,
    enrich_buyers_dir3,
    enrich_cpv,
    read_cpv_dimension,
    read_dir3_dimension,
)
from tfm_licitaciones.spark_foundation import (
    PYSPARK_VERSION,
    canonical_silver_schema,
    create_spark_session,
    read_validated_parquet,
    schema_from_fields,
    write_typed_parquet,
)


class EnrichmentContractTests(unittest.TestCase):
    """Offline checks of the enrichment dataset contracts."""

    def test_dataset_names_are_stable(self) -> None:
        self.assertEqual(CPV_ENRICHED_DATASET, "cpv_enriched")
        self.assertEqual(BUYER_DIR3_ENRICHED_DATASET, "buyer_dir3_enriched")

    def test_cpv_contract_preserves_the_original_code(self) -> None:
        names = tuple(field.name for field in CPV_ENRICHED_FIELDS)
        self.assertEqual(
            names[:5], ("event_id", "procedure_id", "source", "cpv_position", "cpv_code")
        )
        self.assertIn("cpv_matched", names)
        for attribute in ("label_es", "label_en", "level", "parent_code", "is_leaf"):
            self.assertIn(attribute, names)

    def test_buyer_dir3_contract_extends_canonical_without_reordering(self) -> None:
        self.assertEqual(
            tuple(field.name for field in BUYER_DIR3_ENRICHED_FIELDS),
            tuple(field.name for field in CANONICAL_SILVER_FIELDS)
            + tuple(field.name for field in BUYER_DIR3_ATTRIBUTE_FIELDS),
        )

    def test_dir3_units_dimension_reports_availability(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            reference = Path(tmp)
            self.assertIsNone(dir3_units_dimension(reference))
            path = dir3_units_path(reference)
            path.parent.mkdir(parents=True)
            path.write_bytes(b"")
            self.assertEqual(dir3_units_dimension(reference), path)


@unittest.skipIf(pyspark is None, f"requires pyspark=={PYSPARK_VERSION}")
class GoldEnrichmentSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark_session(
            app_name="tfm-licitaciones-test-gold-enrichment",
            master="local[1]",
            shuffle_partitions=2,
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _silver_event(self, event_id: str, **overrides) -> dict[str, object]:
        row: dict[str, object] = {
            "event_id": event_id,
            "procedure_id": f"proc:{event_id}",
            "source": "placsp",
            "source_event_type": "notice_snapshot",
            "buyer_id": None,
            "buyer_name": None,
            "title": "Example",
            "description": None,
            "cpv_codes": [],
            "estimated_value": None,
            "awarded_value": None,
            "currency": None,
            "publication_date": date(2026, 1, 1),
            "source_updated_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "deadline": None,
            "status": None,
            "nuts_code": None,
            "country": "ES",
            "source_url": None,
            "ingested_at": datetime(2026, 1, 3, tzinfo=timezone.utc),
        }
        row.update(overrides)
        return row

    def _events(self, rows: list[dict[str, object]]):
        return self.spark.createDataFrame(rows, schema=canonical_silver_schema())

    def _cpv_dimension(self, rows: list[tuple]):
        return self.spark.createDataFrame(
            rows, schema=schema_from_fields(CPV_DIMENSION_FIELDS)
        )

    def _dir3_dimension(self, rows: list[dict[str, object]]):
        return self.spark.createDataFrame(
            rows, schema=schema_from_fields(DIR3_DIMENSION_FIELDS)
        )

    def _dir3_unit(self, **overrides) -> dict[str, object]:
        row: dict[str, object] = {
            "dir3_code": "EA0000101",
            "name": "Dirección General de Ejemplo",
            "administration_scope": "AGE",
            "public_entity_type": "EA",
            "hierarchy_level": 3,
            "parent_dir3_code": "EA0000001",
            "principal_dir3_code": "EA0000001",
            "status": "V",
            "official_valid_from_raw": "01/01/10",
            "nif_cif": "S3000001A",
        }
        row.update(overrides)
        return row

    def test_integer_logical_types_map_to_spark(self) -> None:
        cpv_schema = schema_from_fields(CPV_DIMENSION_FIELDS)
        self.assertEqual(cpv_schema["level"].dataType.simpleString(), "tinyint")
        dir3_schema = schema_from_fields(DIR3_DIMENSION_FIELDS)
        self.assertEqual(
            dir3_schema["hierarchy_level"].dataType.simpleString(), "smallint"
        )
        enriched_schema = schema_from_fields(BUYER_DIR3_ENRICHED_FIELDS)
        self.assertEqual(
            enriched_schema["buyer_dir3_hierarchy_level"].dataType.simpleString(),
            "smallint",
        )
        self.assertEqual(
            enriched_schema["buyer_dir3_matched"].dataType.simpleString(), "boolean"
        )

    def test_cpv_enrichment_resolves_and_preserves_unmatched_and_positions(self) -> None:
        dimension = self._cpv_dimension(
            [
                ("72000000", "Servicios de TI", "IT services", 2, None, True),
                ("03111000", "Servicios de biblioteca", None, 4, "03000000", False),
            ]
        )
        events = self._events(
            [
                self._silver_event("e1", cpv_codes=["72000000", "99999999"]),
                self._silver_event("e2", cpv_codes=[]),
                self._silver_event("e3", cpv_codes=["03111000"]),
            ]
        )
        enriched, metrics = enrich_cpv(events, dimension)

        rows = enriched.orderBy("event_id", "cpv_position").collect()
        self.assertEqual(len(rows), 3)
        self.assertEqual(
            (rows[0]["event_id"], rows[0]["cpv_position"], rows[0]["cpv_code"]),
            ("e1", 0, "72000000"),
        )
        self.assertTrue(rows[0]["cpv_matched"])
        self.assertEqual(rows[0]["label_es"], "Servicios de TI")
        self.assertEqual(rows[1]["cpv_code"], "99999999")
        self.assertFalse(rows[1]["cpv_matched"])
        self.assertIsNone(rows[1]["label_es"])
        # A null official attribute (label_en missing for this code) is still a
        # match; only absence from the dimension means unmatched.
        self.assertTrue(rows[2]["cpv_matched"])
        self.assertIsNone(rows[2]["label_en"])
        self.assertEqual(rows[2]["parent_code"], "03000000")

        self.assertEqual(
            metrics,
            {
                "dataset": "cpv_enriched",
                "schema_version": 1,
                "input_events": 3,
                "events_with_cpv_codes": 2,
                "events_without_cpv_codes": 1,
                "cpv_occurrences": 3,
                "resolved_occurrences": 2,
                "unresolved_occurrences": 1,
                "distinct_cpv_codes": 3,
                "distinct_unresolved_cpv_codes": 1,
                "dimension_rows": 2,
                "dimension_duplicate_keys": 0,
            },
        )

    def test_cpv_enrichment_rejects_dimension_with_duplicate_keys(self) -> None:
        dimension = self._cpv_dimension(
            [
                ("72000000", "Servicios de TI", "IT services", 2, None, True),
                ("72000000", "Duplicado", "Duplicated", 2, None, True),
            ]
        )
        events = self._events([self._silver_event("e1", cpv_codes=["72000000"])])
        with self.assertRaisesRegex(ValueError, "duplicate cpv_code"):
            enrich_cpv(events, dimension)

    def test_cpv_enrichment_rejects_null_dimension_key(self) -> None:
        # Relaxed schema so a null key can reach the guard the same way a
        # direct (non read_*_dimension) DataFrame input would.
        dimension = self.spark.createDataFrame(
            [
                ("72000000", "Servicios de TI", "IT services", 2, None, True),
                (None, "Sin código", "No code", 1, None, True),
            ],
            schema=schema_from_fields(CPV_DIMENSION_FIELDS).toNullable(),
        )
        events = self._events([self._silver_event("e1", cpv_codes=["72000000"])])
        with self.assertRaisesRegex(ValueError, "null cpv_code"):
            enrich_cpv(events, dimension)

    def test_cpv_enrichment_rejects_duplicate_codes_inside_one_event(self) -> None:
        dimension = self._cpv_dimension(
            [("72000000", "Servicios de TI", "IT services", 2, None, True)]
        )
        events = self._events(
            [self._silver_event("e1", cpv_codes=["72000000", "72000000"])]
        )
        with self.assertRaisesRegex(ValueError, "duplicate cpv_codes"):
            enrich_cpv(events, dimension)

    def test_cpv_enrichment_reads_the_official_dimension_from_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dimension_path = Path(tmp) / "cpv_codes.parquet"
            official = build_cpv_dimension(DEFAULT_CPV_SOURCE)
            official.write_parquet(dimension_path)
            dimension = read_cpv_dimension(self.spark, dimension_path)

            events = self._events(
                [
                    self._silver_event(
                        "e1", cpv_codes=["03212211", "09000000", "99999999"]
                    )
                ]
            )
            enriched, metrics = enrich_cpv(events, dimension)
            rows = {row["cpv_code"]: row for row in enriched.collect()}

            for code in ("03212211", "09000000"):
                expected = official.filter(pl.col("cpv_code") == code).to_dicts()[0]
                self.assertTrue(rows[code]["cpv_matched"])
                self.assertEqual(rows[code]["label_es"], expected["label_es"])
                self.assertEqual(rows[code]["label_en"], expected["label_en"])
                self.assertEqual(rows[code]["level"], expected["level"])
                self.assertEqual(rows[code]["parent_code"], expected["parent_code"])
                self.assertEqual(rows[code]["is_leaf"], expected["is_leaf"])

            self.assertFalse(rows["99999999"]["cpv_matched"])
            self.assertEqual(metrics["dimension_rows"], official.height)
            self.assertEqual(metrics["resolved_occurrences"], 2)
            self.assertEqual(metrics["unresolved_occurrences"], 1)

    def test_cpv_enriched_writes_and_rereads_as_a_gold_dataset(self) -> None:
        dimension = self._cpv_dimension(
            [("72000000", "Servicios de TI", "IT services", 2, None, True)]
        )
        events = self._events(
            [self._silver_event("e1", cpv_codes=["72000000", "99999999"])]
        )
        enriched, _ = enrich_cpv(events, dimension)
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "cpv_enriched.parquet"
            write_typed_parquet(
                enriched,
                output,
                expected_schema=schema_from_fields(CPV_ENRICHED_FIELDS),
                order_by=("event_id", "cpv_position"),
            )
            reloaded = read_validated_parquet(
                self.spark, output, schema_from_fields(CPV_ENRICHED_FIELDS)
            )
            self.assertEqual(reloaded.count(), 2)

    def test_dir3_enrichment_resolves_and_preserves_unmatched(self) -> None:
        dimension = self._dir3_dimension([self._dir3_unit()])
        events = self._events(
            [
                self._silver_event(
                    "e1", buyer_id="EA0000101", buyer_name="Dirección General de Ejemplo"
                ),
                self._silver_event("e2", buyer_id="ZZ9999999", buyer_name="Desconocido"),
                self._silver_event("e3"),
            ]
        )
        enriched, metrics = enrich_buyers_dir3(events, dimension)

        self.assertEqual(enriched.count(), 3)
        rows = {row["event_id"]: row for row in enriched.collect()}
        # Canonical buyer identity is never rewritten, only extended.
        self.assertEqual(rows["e1"]["buyer_id"], "EA0000101")
        self.assertEqual(rows["e1"]["buyer_name"], "Dirección General de Ejemplo")
        self.assertTrue(rows["e1"]["buyer_dir3_matched"])
        self.assertEqual(rows["e1"]["buyer_dir3_scope"], "AGE")
        self.assertEqual(rows["e1"]["buyer_dir3_nif"], "S3000001A")
        self.assertFalse(rows["e2"]["buyer_dir3_matched"])
        self.assertIsNone(rows["e2"]["buyer_dir3_name"])
        self.assertEqual(rows["e2"]["buyer_name"], "Desconocido")
        self.assertFalse(rows["e3"]["buyer_dir3_matched"])
        self.assertIsNone(rows["e3"]["buyer_id"])

        self.assertEqual(
            metrics,
            {
                "dataset": "buyer_dir3_enriched",
                "schema_version": 1,
                "input_events": 3,
                "events_with_buyer_id": 2,
                "events_without_buyer_id": 1,
                "resolved_events": 1,
                "unresolved_events_with_buyer_id": 1,
                "events_with_multiple_dir3_matches": 0,
                "dimension_rows": 1,
                "dimension_duplicate_keys": 0,
            },
        )

    def test_dir3_enrichment_rejects_dimension_with_duplicate_keys(self) -> None:
        dimension = self._dir3_dimension([self._dir3_unit(), self._dir3_unit()])
        events = self._events([self._silver_event("e1", buyer_id="EA0000101")])
        with self.assertRaisesRegex(ValueError, "duplicate dir3_code"):
            enrich_buyers_dir3(events, dimension)

    def test_dir3_enrichment_rejects_null_dimension_key(self) -> None:
        dimension = self.spark.createDataFrame(
            [self._dir3_unit(), self._dir3_unit(dir3_code=None)],
            schema=schema_from_fields(DIR3_DIMENSION_FIELDS).toNullable(),
        )
        events = self._events([self._silver_event("e1", buyer_id="EA0000101")])
        with self.assertRaisesRegex(ValueError, "null dir3_code"):
            enrich_buyers_dir3(events, dimension)

    def test_dir3_enrichment_rejects_already_enriched_input(self) -> None:
        dimension = self._dir3_dimension([self._dir3_unit()])
        events = self._events([self._silver_event("e1", buyer_id="EA0000101")])
        enriched, _ = enrich_buyers_dir3(events, dimension)
        with self.assertRaisesRegex(ValueError, "already contain enrichment columns"):
            enrich_buyers_dir3(enriched, dimension)

    def test_dir3_dimension_reads_from_polars_parquet_and_rejects_drift(self) -> None:
        units = pl.DataFrame(
            [
                self._dir3_unit(),
                self._dir3_unit(
                    dir3_code="EA0000202",
                    name="Otra Unidad",
                    public_entity_type=None,
                    hierarchy_level=2,
                    nif_cif=None,
                ),
            ],
            schema=DIR3_UNITS_SCHEMA,
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dir3_units.parquet"
            units.write_parquet(path)
            dimension = read_dir3_dimension(self.spark, path)
            self.assertEqual(dimension.count(), 2)
            self.assertEqual(
                dimension.schema["hierarchy_level"].dataType.simpleString(), "smallint"
            )

            drifted = units.drop("nif_cif")
            drifted_path = Path(tmp) / "drifted.parquet"
            drifted.write_parquet(drifted_path)
            with self.assertRaisesRegex(ValueError, "schema mismatch"):
                read_dir3_dimension(self.spark, drifted_path)


if __name__ == "__main__":
    unittest.main()
