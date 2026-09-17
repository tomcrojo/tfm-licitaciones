"""Gold policy, enrichment metrics, manifests and Silver/Gold boundary tests.

Pure checks cover policy constants and as_of normalization. Spark checks cover
status/deadline precedence, deletion, insufficient evidence, CPV cardinality,
optional DIR3, deterministic results and manifest reconciliation.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark = None

from tfm_licitaciones.gold_contract import (
    CURRENT_STATE_ISSUES_SCHEMA_VERSION,
    CURRENT_STATE_SCHEMA_VERSION,
    GOLD_OPEN_OPPORTUNITIES_FIELDS,
    GOLD_OPEN_OPPORTUNITIES_SCHEMA_VERSION,
)
from tfm_licitaciones.gold_enrichment import (
    BUYER_DIR3_ENRICHED_SCHEMA_VERSION,
    CPV_DIMENSION_FIELDS,
    CPV_ENRICHED_SCHEMA_VERSION,
    DIR3_DIMENSION_FIELDS,
)
from tfm_licitaciones.gold_open_opportunities import (
    DECISION_DEADLINE_PASSED,
    DECISION_DELETED,
    DECISION_INSUFFICIENT_EVIDENCE,
    DECISION_OPEN,
    DECISION_STATUS_CLOSED,
    PLACSP_CLOSED_STATUSES,
    PLACSP_OPEN_STATUSES,
    build_gold_from_silver,
    build_open_opportunities,
    open_opportunities_schema,
    parse_as_of,
)
from tfm_licitaciones.spark_foundation import (
    PYSPARK_VERSION,
    canonical_silver_schema,
    create_spark_session,
    read_validated_parquet,
    schema_from_fields,
)

UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
AS_OF = "2026-01-15"
REF_A = "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/10000101"
REF_B = "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/10000102"


class AsOfPolicyTests(unittest.TestCase):
    """Offline checks: normalization never touches the system clock."""

    def test_policy_status_sets_match_the_official_codelist(self) -> None:
        self.assertEqual(PLACSP_OPEN_STATUSES, frozenset({"PUB"}))
        self.assertEqual(
            PLACSP_CLOSED_STATUSES,
            frozenset({"EV", "ADJ", "ADJ_PAR", "RES", "RES_PAR", "ANUL"}),
        )

    def test_parse_as_of_accepts_unequivocal_forms(self) -> None:
        self.assertEqual(
            parse_as_of("2026-01-15"), datetime(2026, 1, 15, tzinfo=UTC)
        )
        self.assertEqual(
            parse_as_of(date(2026, 1, 15)), datetime(2026, 1, 15, tzinfo=UTC)
        )
        self.assertEqual(
            parse_as_of(datetime(2026, 1, 15, 22, 30, tzinfo=UTC)),
            datetime(2026, 1, 15, tzinfo=UTC),
        )
        self.assertIsNone(parse_as_of(None))

    def test_parse_as_of_rejects_ambiguous_or_naive_values(self) -> None:
        for bad in ("15/01/2026", "2026-01-15T10:00:00", "2026-1-5", "", "yesterday"):
            with self.subTest(value=bad):
                with self.assertRaisesRegex(ValueError, "YYYY-MM-DD"):
                    parse_as_of(bad)
        with self.assertRaisesRegex(ValueError, "timezone"):
            parse_as_of(datetime(2026, 1, 15, 10, 0))
        with self.assertRaisesRegex(ValueError, "unsupported"):
            parse_as_of(20260115)  # type: ignore[arg-type]


@unittest.skipIf(pyspark is None, f"requires pyspark=={PYSPARK_VERSION}")
class GoldOpenOpportunitiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark_session(
            app_name="tfm-licitaciones-test-gold-open",
            master="local[1]",
            shuffle_partitions=2,
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    # -- fixtures ------------------------------------------------------

    def _silver_row(self, **overrides) -> dict:
        row: dict = {
            "event_id": "ted:notice:BASE",
            "procedure_id": "ted:procedure:BASE",
            "source": "ted",
            "source_event_type": "notice",
            "buyer_id": None,
            "buyer_name": "Buyer",
            "title": "Example",
            "description": None,
            "cpv_codes": [],
            "estimated_value": None,
            "awarded_value": None,
            "currency": None,
            "publication_date": None,
            "source_updated_at": None,
            "deadline": None,
            "status": None,
            "nuts_code": None,
            "country": "ES",
            "source_url": None,
            "ingested_at": datetime(2026, 1, 5, tzinfo=UTC),
        }
        row.update(overrides)
        return row

    def _placsp_notice(self, ref: str, when: datetime, **overrides) -> dict:
        marker = when.astimezone(UTC).isoformat()
        row = self._silver_row(
            event_id=f"placsp:notice:{ref}@{marker}",
            procedure_id=f"placsp:procedure:{ref}",
            source="placsp",
            source_event_type="notice_snapshot",
            source_updated_at=when,
            status="PUB",
            title="PLACSP notice",
        )
        row.update(overrides)
        return row

    def _dated_tombstone(self, ref: str, when: datetime) -> dict:
        micros = int((when.astimezone(UTC) - EPOCH).total_seconds() * 1_000_000)
        return self._silver_row(
            event_id=f"placsp:tombstone-dated:{micros}:{ref}",
            procedure_id=f"placsp:procedure:{ref}",
            source="placsp",
            source_event_type="tombstone",
            source_updated_at=when,
            status=None,
            title=None,
        )

    def _current_state(self, rows: list[dict]):
        from tfm_licitaciones.current_state import build_current_state

        silver = self.spark.createDataFrame(rows, schema=canonical_silver_schema())
        current, _ = build_current_state(silver)
        return current

    def _cpv_dimension(self, rows: list[tuple]):
        return self.spark.createDataFrame(
            rows, schema=schema_from_fields(CPV_DIMENSION_FIELDS)
        )

    def _dir3_dimension(self, rows: list[dict]):
        return self.spark.createDataFrame(
            rows, schema=schema_from_fields(DIR3_DIMENSION_FIELDS)
        )

    def _dir3_unit(self, **overrides) -> dict:
        row: dict = {
            "dir3_code": "E00000001",
            "name": "Centro de Sistemas",
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

    def _current_row(self, **overrides) -> dict:
        row: dict = {
            "procedure_id": "placsp:procedure:P",
            "event_id": "placsp:notice:P@x",
            "source": "placsp",
            "source_event_type": "notice_snapshot",
            "buyer_id": None,
            "buyer_name": "Buyer",
            "title": "Example",
            "description": None,
            "cpv_codes": [],
            "estimated_value": None,
            "awarded_value": None,
            "currency": None,
            "publication_date": None,
            "source_updated_at": datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
            "deadline": None,
            "status": "EV",
            "nuts_code": None,
            "country": "ES",
            "source_url": None,
            "ingested_at": datetime(2026, 1, 9, tzinfo=UTC),
            "is_deleted": False,
        }
        row.update(overrides)
        return row

    # -- policy cases --------------------------------------------------

    def test_01_valid_open_appears_in_gold(self) -> None:
        current = self._current_state(
            [
                self._placsp_notice(
                    REF_A,
                    datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                    buyer_id="E00000001",
                    buyer_name="Centro de Sistemas",
                    title="Servicio cloud",
                    description="Soporte",
                    cpv_codes=["72415000"],
                    estimated_value=Decimal("120000.00"),
                    currency="EUR",
                    source_url="https://example.invalid/1",
                )
            ]
        )
        gold, metrics = build_open_opportunities(current)
        self.assertEqual(gold.count(), 1)
        self.assertEqual(metrics["open_opportunities_rows"], 1)
        row = gold.collect()[0]
        self.assertEqual(row["procedure_id"], f"placsp:procedure:{REF_A}")
        self.assertEqual(row["buyer_id"], "E00000001")
        self.assertEqual(row["title"], "Servicio cloud")
        self.assertEqual(row["cpv_codes"], ["72415000"])
        self.assertEqual(row["estimated_value"], Decimal("120000.00"))
        self.assertEqual(
            [f.name for f in gold.schema],
            [f.name for f in GOLD_OPEN_OPPORTUNITIES_FIELDS],
        )

    def test_02_deleted_never_published(self) -> None:
        current = self._current_state(
            [
                self._placsp_notice(REF_A, datetime(2026, 1, 8, 10, 0, tzinfo=UTC)),
                self._dated_tombstone(
                    REF_A, datetime(2026, 1, 10, 16, 1, 39, 351000, tzinfo=UTC)
                ),
            ]
        )
        gold, metrics = build_open_opportunities(current, as_of=AS_OF)
        self.assertEqual(gold.count(), 0)
        self.assertEqual(metrics["reasons"][DECISION_DELETED], 1)
        self.assertEqual(metrics["excluded_deleted"], 1)
        self.assertEqual(metrics["open_opportunities_rows"], 0)

    def test_03_past_deadline_excluded_when_relevant(self) -> None:
        current = self._current_state(
            [
                self._placsp_notice(
                    REF_A,
                    datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                    deadline=datetime(2026, 1, 10, 12, 0, tzinfo=UTC),
                )
            ]
        )
        gold, metrics = build_open_opportunities(current, as_of=AS_OF)
        self.assertEqual(gold.count(), 0)
        self.assertEqual(metrics["reasons"][DECISION_DEADLINE_PASSED], 1)
        self.assertEqual(metrics["excluded_not_actionable"], 1)

    def test_04_future_deadline_included_when_relevant(self) -> None:
        current = self._current_state(
            [
                self._placsp_notice(
                    REF_A,
                    datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                    deadline=datetime(2026, 2, 1, 12, 0, tzinfo=UTC),
                )
            ]
        )
        gold, metrics = build_open_opportunities(current, as_of=AS_OF)
        self.assertEqual(gold.count(), 1)
        self.assertEqual(metrics["reasons"][DECISION_OPEN], 1)

    def test_05_explicitly_closed_status_excluded(self) -> None:
        current = self._current_state(
            [
                self._placsp_notice(
                    REF_A,
                    datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                    status="ADJ",
                    # A stale future deadline must not rescue a closed status.
                    deadline=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
                )
            ]
        )
        gold, metrics = build_open_opportunities(current, as_of=AS_OF)
        self.assertEqual(gold.count(), 0)
        self.assertEqual(metrics["reasons"][DECISION_STATUS_CLOSED], 1)
        self.assertEqual(metrics["excluded_not_actionable"], 1)

    def test_06_open_status_included(self) -> None:
        current = self._current_state(
            [
                self._placsp_notice(
                    REF_A,
                    datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                    status="PUB",
                )
            ]
        )
        gold, metrics = build_open_opportunities(current, as_of=AS_OF)
        self.assertEqual(gold.count(), 1)
        self.assertEqual(metrics["reasons"][DECISION_OPEN], 1)

    def test_07_unknown_status_never_published_and_counted(self) -> None:
        rows = [
            self._placsp_notice(
                REF_A, datetime(2026, 1, 8, 10, 0, tzinfo=UTC), status=status
            )
            for status in ("XYZ", "ev", "pub", "", None)
        ]
        # Distinct procedures: one row per status value under test.
        for index, row in enumerate(rows):
            ref = f"https://example.invalid/status/{index}"
            marker = row["source_updated_at"].astimezone(UTC).isoformat()
            row["event_id"] = f"placsp:notice:{ref}@{marker}"
            row["procedure_id"] = f"placsp:procedure:{ref}"
        current = self._current_state(rows)
        gold, metrics = build_open_opportunities(current, as_of=AS_OF)
        self.assertEqual(gold.count(), 0)
        self.assertEqual(metrics["reasons"][DECISION_INSUFFICIENT_EVIDENCE], 5)
        self.assertEqual(metrics["excluded_insufficient_evidence"], 5)

    def test_08_null_deadline_policy(self) -> None:
        # Null deadlines never exclude: PUB openness comes from status alone.
        open_current = self._current_state(
            [
                self._placsp_notice(
                    REF_A, datetime(2026, 1, 8, 10, 0, tzinfo=UTC), deadline=None
                )
            ]
        )
        gold, _ = build_open_opportunities(open_current, as_of=AS_OF)
        self.assertEqual(gold.count(), 1)
        # TED rows carry no taxonomy and no deadline: insufficient, not open.
        ted_current = self._current_state(
            [self._silver_row(event_id="ted:notice:T", procedure_id="ted:procedure:T")]
        )
        ted_gold, ted_metrics = build_open_opportunities(ted_current, as_of=AS_OF)
        self.assertEqual(ted_gold.count(), 0)
        self.assertEqual(ted_metrics["reasons"][DECISION_INSUFFICIENT_EVIDENCE], 1)

    def test_08b_ted_future_deadline_is_actionable_evidence(self) -> None:
        current = self._current_state(
            [
                self._silver_row(
                    event_id="ted:notice:F",
                    procedure_id="ted:procedure:F",
                    deadline=datetime(2026, 2, 1, 12, 0, tzinfo=UTC),
                )
            ]
        )
        gold, metrics = build_open_opportunities(current, as_of=AS_OF)
        self.assertEqual(gold.count(), 1)
        self.assertEqual(metrics["reasons"][DECISION_OPEN], 1)
        # Without as_of the deadline cannot be evaluated: insufficient.
        gold_na, metrics_na = build_open_opportunities(current, as_of=None)
        self.assertEqual(gold_na.count(), 0)
        self.assertEqual(metrics_na["reasons"][DECISION_INSUFFICIENT_EVIDENCE], 1)

    def test_17_closed_lifecycle_statuses_excluded(self) -> None:
        # Official codelist: EV is evaluation (submission finished), ADJ/RES
        # families are awarded/resolved, ANUL is cancelled. None is open.
        for status in ("EV", "ADJ", "ADJ_PAR", "RES", "RES_PAR", "ANUL"):
            with self.subTest(status=status):
                current = self._current_state(
                    [
                        self._placsp_notice(
                            REF_A,
                            datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                            status=status,
                        )
                    ]
                )
                gold, metrics = build_open_opportunities(current, as_of=AS_OF)
                self.assertEqual(gold.count(), 0)
                self.assertEqual(metrics["reasons"][DECISION_STATUS_CLOSED], 1)
                self.assertEqual(metrics["excluded_not_actionable"], 1)
                self.assertEqual(metrics["open_opportunities_rows"], 0)

    def test_18_prior_notice_never_opens(self) -> None:
        # PRE (Anuncio Previo) never proves bids can be submitted, with or
        # without a future deadline: it stays observable, never published.
        for deadline in (None, datetime(2026, 3, 1, 12, 0, tzinfo=UTC)):
            with self.subTest(deadline=deadline):
                current = self._current_state(
                    [
                        self._placsp_notice(
                            REF_A,
                            datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                            status="PRE",
                            deadline=deadline,
                        )
                    ]
                )
                gold, metrics = build_open_opportunities(current, as_of=AS_OF)
                self.assertEqual(gold.count(), 0)
                self.assertEqual(
                    metrics["reasons"][DECISION_INSUFFICIENT_EVIDENCE], 1
                )
                self.assertEqual(metrics["excluded_insufficient_evidence"], 1)

    # -- enrichment composition -----------------------------------------

    def test_09_multiple_cpv_keep_one_row_per_procedure(self) -> None:
        current = self._current_state(
            [
                self._placsp_notice(
                    REF_A,
                    datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                    cpv_codes=["72000000", "45212200", "99999999"],
                )
            ]
        )
        gold, _ = build_open_opportunities(current)
        self.assertEqual(gold.count(), 1)
        self.assertEqual(gold.collect()[0]["cpv_codes"], ["72000000", "45212200", "99999999"])

    def test_10_unmatched_cpv_conserved_and_counted(self) -> None:
        dimension = self._cpv_dimension(
            [("72000000", "Servicios de TI", "IT services", 2, None, True)]
        )
        with tempfile.TemporaryDirectory() as tmp:
            silver_path = Path(tmp) / "silver" / "procurement_events.parquet"
            silver_path.parent.mkdir(parents=True)
            silver = self.spark.createDataFrame(
                [
                    self._placsp_notice(
                        REF_A,
                        datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                        cpv_codes=["72000000", "99999999"],
                    )
                ],
                schema=canonical_silver_schema(),
            )
            silver.write.mode("overwrite").parquet(str(silver_path))
            out = build_gold_from_silver(
                self.spark,
                silver_path,
                Path(tmp) / "gold",
                cpv_dimension=dimension,
                as_of=AS_OF,
            )
            manifest = json.loads(Path(out["manifest"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["cpv"]["resolved_occurrences"], 1)
            self.assertEqual(manifest["cpv"]["unresolved_occurrences"], 1)
            self.assertEqual(manifest["cpv"]["distinct_unresolved_cpv_codes"], 1)
            gold = read_validated_parquet(
                self.spark, out["open_opportunities"], open_opportunities_schema()
            )
            self.assertEqual(gold.count(), 1)
            self.assertEqual(
                gold.collect()[0]["cpv_codes"], ["72000000", "99999999"]
            )

    def test_11_missing_dir3_dimension_does_not_block_gold(self) -> None:
        dimension = self._cpv_dimension(
            [("72000000", "Servicios de TI", "IT services", 2, None, True)]
        )
        with tempfile.TemporaryDirectory() as tmp:
            silver_path = Path(tmp) / "silver" / "procurement_events.parquet"
            silver_path.parent.mkdir(parents=True)
            silver = self.spark.createDataFrame(
                [
                    self._placsp_notice(
                        REF_A,
                        datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                        buyer_id="E00000001",
                        cpv_codes=["72000000"],
                    )
                ],
                schema=canonical_silver_schema(),
            )
            silver.write.mode("overwrite").parquet(str(silver_path))
            out = build_gold_from_silver(
                self.spark,
                silver_path,
                Path(tmp) / "gold",
                cpv_dimension=dimension,
                dir3_dimension=None,
                reference_dir=None,
                as_of=AS_OF,
            )
            self.assertEqual(out["open_opportunities_rows"], 1)
            manifest = json.loads(Path(out["manifest"]).read_text(encoding="utf-8"))
            self.assertFalse(manifest["dir3"]["available"])

    def test_12_matched_dir3_keeps_canonical_buyer_identity(self) -> None:
        dimension = self._cpv_dimension(
            [("72000000", "Servicios de TI", "IT services", 2, None, True)]
        )
        dir3 = self._dir3_dimension([self._dir3_unit()])
        with tempfile.TemporaryDirectory() as tmp:
            silver_path = Path(tmp) / "silver" / "procurement_events.parquet"
            silver_path.parent.mkdir(parents=True)
            silver = self.spark.createDataFrame(
                [
                    self._placsp_notice(
                        REF_A,
                        datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                        buyer_id="E00000001",
                        buyer_name="Centro de Sistemas",
                        cpv_codes=["72000000"],
                    )
                ],
                schema=canonical_silver_schema(),
            )
            silver.write.mode("overwrite").parquet(str(silver_path))
            out = build_gold_from_silver(
                self.spark,
                silver_path,
                Path(tmp) / "gold",
                cpv_dimension=dimension,
                dir3_dimension=dir3,
                as_of=AS_OF,
            )
            gold = read_validated_parquet(
                self.spark, out["open_opportunities"], open_opportunities_schema()
            )
            row = gold.collect()[0]
            self.assertEqual(row["buyer_id"], "E00000001")
            self.assertEqual(row["buyer_name"], "Centro de Sistemas")
            manifest = json.loads(Path(out["manifest"]).read_text(encoding="utf-8"))
            self.assertTrue(manifest["dir3"]["available"])
            self.assertEqual(manifest["dir3"]["resolved_events"], 1)

    # -- guards, manifest, determinism, boundary -------------------------

    def test_13_procedure_id_stays_unique(self) -> None:
        from tfm_licitaciones.current_state import current_state_schema

        current = self._current_state(
            [
                self._placsp_notice(REF_A, datetime(2026, 1, 8, 10, 0, tzinfo=UTC)),
                self._placsp_notice(REF_B, datetime(2026, 1, 9, 10, 0, tzinfo=UTC)),
            ]
        )
        gold, _ = build_open_opportunities(current)
        procedures = [row["procedure_id"] for row in gold.collect()]
        self.assertEqual(len(procedures), len(set(procedures)))
        self.assertEqual(gold.count(), 2)
        # A broken input with a duplicated procedure key fails loudly.
        broken = self.spark.createDataFrame(
            [
                self._current_row(procedure_id="dup", event_id="e1"),
                self._current_row(procedure_id="dup", event_id="e2"),
            ],
            schema=current_state_schema(),
        )
        with self.assertRaisesRegex(ValueError, "duplicate procedure_id"):
            build_open_opportunities(broken)

    def test_14_manifest_carries_versions_and_real_counts(self) -> None:
        dimension = self._cpv_dimension(
            [("72000000", "Servicios de TI", "IT services", 2, None, True)]
        )
        with tempfile.TemporaryDirectory() as tmp:
            silver_path = Path(tmp) / "silver" / "procurement_events.parquet"
            silver_path.parent.mkdir(parents=True)
            silver = self.spark.createDataFrame(
                [
                    self._placsp_notice(REF_A, datetime(2026, 1, 8, 10, 0, tzinfo=UTC)),
                    self._silver_row(
                        event_id="ted:notice:NULL", procedure_id=None
                    ),
                ],
                schema=canonical_silver_schema(),
            )
            silver.write.mode("overwrite").parquet(str(silver_path))
            out = build_gold_from_silver(
                self.spark,
                silver_path,
                Path(tmp) / "gold",
                cpv_dimension=dimension,
                as_of=AS_OF,
            )
            manifest = json.loads(Path(out["manifest"]).read_text(encoding="utf-8"))
            self.assertEqual(
                manifest["gold_open_opportunities_schema_version"],
                GOLD_OPEN_OPPORTUNITIES_SCHEMA_VERSION,
            )
            self.assertEqual(
                manifest["current_state_schema_version"], CURRENT_STATE_SCHEMA_VERSION
            )
            self.assertEqual(
                manifest["current_state_issues_schema_version"],
                CURRENT_STATE_ISSUES_SCHEMA_VERSION,
            )
            self.assertEqual(
                manifest["cpv_enriched_schema_version"], CPV_ENRICHED_SCHEMA_VERSION
            )
            self.assertEqual(
                manifest["buyer_dir3_enriched_schema_version"],
                BUYER_DIR3_ENRICHED_SCHEMA_VERSION,
            )
            self.assertEqual(manifest["as_of"], AS_OF)
            self.assertEqual(manifest["counts"]["current_state"], 1)
            self.assertEqual(manifest["counts"]["current_state_issues"], 1)
            self.assertEqual(manifest["counts"]["open_opportunities"], 1)
            self.assertEqual(out["open_opportunities_rows"], 1)
            gold = read_validated_parquet(
                self.spark, out["open_opportunities"], open_opportunities_schema()
            )
            self.assertEqual(gold.count(), manifest["counts"]["open_opportunities"])

    def test_15_as_of_is_deterministic(self) -> None:
        rows = [
            self._placsp_notice(
                REF_A,
                datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                deadline=datetime(2026, 1, 20, 12, 0, tzinfo=UTC),
            )
        ]
        first, _ = build_open_opportunities(self._current_state(rows), as_of=AS_OF)
        second, _ = build_open_opportunities(self._current_state(rows), as_of=AS_OF)
        self.assertEqual(
            sorted(r["procedure_id"] for r in first.collect()),
            sorted(r["procedure_id"] for r in second.collect()),
        )
        self.assertEqual(first.count(), 1)
        # A later evaluation day flips the boundary row deterministically.
        late, late_metrics = build_open_opportunities(
            self._current_state(rows), as_of="2026-02-01"
        )
        self.assertEqual(late.count(), 0)
        self.assertEqual(late_metrics["reasons"][DECISION_DEADLINE_PASSED], 1)

    def test_16_silver_to_gold_boundary_roundtrip(self) -> None:
        dimension = self._cpv_dimension(
            [("72000000", "Servicios de TI", "IT services", 2, None, True)]
        )
        with tempfile.TemporaryDirectory() as tmp:
            silver_path = Path(tmp) / "silver" / "procurement_events.parquet"
            silver_path.parent.mkdir(parents=True)
            silver_rows = [
                self._placsp_notice(
                    REF_A,
                    datetime(2026, 1, 8, 10, 0, tzinfo=UTC),
                    cpv_codes=["72000000"],
                    title="Servicio cloud",
                ),
                self._placsp_notice(
                    REF_B, datetime(2026, 1, 9, 10, 0, tzinfo=UTC), status="ADJ"
                ),
                self._silver_row(event_id="boe:notice:B", procedure_id=None, source="boe"),
            ]
            silver = self.spark.createDataFrame(silver_rows, schema=canonical_silver_schema())
            silver.write.mode("overwrite").parquet(str(silver_path))
            out = build_gold_from_silver(
                self.spark,
                silver_path,
                Path(tmp) / "gold",
                cpv_dimension=dimension,
                as_of=AS_OF,
            )
            self.assertEqual(out["current_state_rows"], 2)
            self.assertEqual(out["current_state_issues_rows"], 1)
            self.assertEqual(out["open_opportunities_rows"], 1)
            gold = read_validated_parquet(
                self.spark, out["open_opportunities"], open_opportunities_schema()
            )
            self.assertEqual(
                [f.name for f in gold.schema],
                [f.name for f in GOLD_OPEN_OPPORTUNITIES_FIELDS],
            )
            row = gold.collect()[0]
            self.assertEqual(row["procedure_id"], f"placsp:procedure:{REF_A}")
            self.assertEqual(row["title"], "Servicio cloud")
            manifest = json.loads(Path(out["manifest"]).read_text(encoding="utf-8"))
            counts = manifest["counts"]
            self.assertEqual(
                counts["open_opportunities"]
                + counts["excluded_deleted"]
                + counts["excluded_not_actionable"]
                + counts["excluded_insufficient_evidence"],
                counts["current_state"],
            )


if __name__ == "__main__":
    unittest.main()
