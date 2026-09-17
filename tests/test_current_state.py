from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark = None

from tfm_licitaciones.current_state import (
    REASON_MISSING_PROCEDURE_ID,
    REASON_UNDATED_COMPETING_EVENTS,
    build_current_state,
    build_current_state_from_silver,
    current_state_issues_schema,
    current_state_schema,
)
from tfm_licitaciones.gold_contract import (
    CURRENT_STATE_FIELDS,
    CURRENT_STATE_ISSUES_FIELDS,
)
from tfm_licitaciones.spark_foundation import (
    PYSPARK_VERSION,
    canonical_silver_schema,
    create_spark_session,
    read_canonical_silver,
    schema_from_fields,
    write_typed_parquet,
)

UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
REF_A = "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/10000101"
REF_B = "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/10000102"
PROC_A = f"placsp:procedure:{REF_A}"
PROC_B = f"placsp:procedure:{REF_B}"


def _micros(when: datetime) -> int:
    return int((when.astimezone(UTC) - EPOCH).total_seconds() * 1_000_000)


def _silver_row(
    *,
    event_id: str,
    procedure_id: str | None,
    source: str = "placsp",
    source_event_type: str = "notice_snapshot",
    source_updated_at: datetime | None = None,
    ingested_at: datetime | None = None,
    title: str | None = "Title",
) -> dict:
    return {
        "event_id": event_id,
        "procedure_id": procedure_id,
        "source": source,
        "source_event_type": source_event_type,
        "buyer_id": None,
        "buyer_name": None,
        "title": title,
        "description": None,
        "cpv_codes": [],
        "estimated_value": None,
        "awarded_value": None,
        "currency": None,
        "publication_date": None,
        "source_updated_at": source_updated_at,
        "deadline": None,
        "status": None,
        "nuts_code": None,
        "country": "ES",
        "source_url": None,
        "ingested_at": ingested_at or datetime(2026, 1, 5, tzinfo=UTC),
    }


def _notice(ref: str, when: datetime, *, ingested_at: datetime | None = None, title: str = "Notice") -> dict:
    proc = f"placsp:procedure:{ref}"
    marker = when.astimezone(UTC).isoformat()
    return _silver_row(
        event_id=f"placsp:notice:{ref}@{marker}",
        procedure_id=proc,
        source="placsp",
        source_event_type="notice_snapshot",
        source_updated_at=when,
        ingested_at=ingested_at,
        title=title,
    )


def _dated_tombstone(
    ref: str, when: datetime, *, ingested_at: datetime | None = None
) -> dict:
    proc = f"placsp:procedure:{ref}"
    return _silver_row(
        event_id=f"placsp:tombstone-dated:{_micros(when)}:{ref}",
        procedure_id=proc,
        source="placsp",
        source_event_type="tombstone",
        source_updated_at=when,
        ingested_at=ingested_at,
        title=None,
    )


def _undated_tombstone(
    ref: str, *, ingested_at: datetime | None = None
) -> dict:
    proc = f"placsp:procedure:{ref}"
    return _silver_row(
        event_id=f"placsp:tombstone:{ref}",
        procedure_id=proc,
        source="placsp",
        source_event_type="tombstone",
        source_updated_at=None,
        ingested_at=ingested_at,
        title=None,
    )


def _ted_notice(
    event_id: str,
    procedure_id: str | None,
    *,
    ingested_at: datetime | None = None,
) -> dict:
    return _silver_row(
        event_id=event_id,
        procedure_id=procedure_id,
        source="ted",
        source_event_type="notice",
        source_updated_at=None,
        ingested_at=ingested_at,
        title="TED notice",
    )


@unittest.skipIf(pyspark is None, f"requires pyspark=={PYSPARK_VERSION}")
class CurrentStateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = create_spark_session(
            app_name="tfm-licitaciones-test-current-state",
            master="local[1]",
            shuffle_partitions=2,
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _build(self, rows: list[dict]):
        frame = self.spark.createDataFrame(rows, schema=canonical_silver_schema())
        return build_current_state(frame)

    def test_schemas_match_frozen_contracts(self) -> None:
        self.assertEqual(
            [(f.name, f.dataType.simpleString()) for f in current_state_schema()],
            [(f.name, f.logical_type) for f in CURRENT_STATE_FIELDS],
        )
        self.assertEqual(
            [f.name for f in current_state_schema()],
            [f.name for f in CURRENT_STATE_FIELDS],
        )
        self.assertEqual(
            [f.name for f in current_state_issues_schema()],
            [f.name for f in CURRENT_STATE_ISSUES_FIELDS],
        )
        self.assertEqual(
            [(f.name, f.dataType.simpleString()) for f in current_state_issues_schema()],
            [(f.name, f.logical_type) for f in CURRENT_STATE_ISSUES_FIELDS],
        )
        # Issues must preserve provenance for null keys.
        self.assertTrue(current_state_issues_schema()["procedure_id"].nullable)
        self.assertFalse(current_state_issues_schema()["event_id"].nullable)
        self.assertFalse(current_state_schema()["procedure_id"].nullable)
        self.assertFalse(current_state_schema()["is_deleted"].nullable)

    def test_multiple_placsp_revisions_resolve_to_latest(self) -> None:
        t1 = datetime(2026, 1, 8, 10, 0, tzinfo=UTC)
        t2 = datetime(2026, 1, 9, 11, 30, tzinfo=UTC)
        rows = [_notice(REF_A, t1, title="v1"), _notice(REF_A, t2, title="v2")]
        current, issues = self._build(rows)
        self.assertEqual(current.count(), 1)
        self.assertEqual(issues.count(), 0)
        row = current.collect()[0]
        self.assertEqual(row["procedure_id"], PROC_A)
        self.assertEqual(row["event_id"], f"placsp:notice:{REF_A}@{t2.isoformat()}")
        self.assertEqual(row["title"], "v2")
        self.assertFalse(row["is_deleted"])

    def test_dated_tombstone_after_notice_marks_deleted(self) -> None:
        t1 = datetime(2026, 1, 8, 10, 0, tzinfo=UTC)
        t2 = datetime(2026, 1, 10, 16, 1, 39, 351000, tzinfo=UTC)
        rows = [
            _notice(REF_A, t1, ingested_at=datetime(2026, 1, 11, tzinfo=UTC)),
            _dated_tombstone(REF_A, t2, ingested_at=datetime(2026, 1, 9, tzinfo=UTC)),
        ]
        current, issues = self._build(rows)
        self.assertEqual(current.count(), 1)
        self.assertEqual(issues.count(), 0)
        row = current.collect()[0]
        self.assertEqual(row["procedure_id"], PROC_A)
        self.assertEqual(row["source_event_type"], "tombstone")
        self.assertEqual(row["event_id"], f"placsp:tombstone-dated:{_micros(t2)}:{REF_A}")
        self.assertIsNotNone(row["source_updated_at"])
        self.assertTrue(row["is_deleted"])

    def test_dated_tombstone_before_newer_notice_keeps_notice(self) -> None:
        t1 = datetime(2026, 1, 8, 10, 0, tzinfo=UTC)
        t2 = datetime(2026, 1, 10, 16, 1, 39, 351000, tzinfo=UTC)
        t3 = datetime(2026, 1, 12, 9, 30, tzinfo=UTC)
        # Misleading ingestion order: tombstone observed last, but business
        # time decides.
        rows = [
            _notice(REF_A, t1, ingested_at=datetime(2026, 1, 9, tzinfo=UTC)),
            _dated_tombstone(REF_A, t2, ingested_at=datetime(2026, 1, 15, tzinfo=UTC)),
            _notice(REF_A, t3, ingested_at=datetime(2026, 1, 10, tzinfo=UTC), title="v3"),
        ]
        current, issues = self._build(rows)
        self.assertEqual(current.count(), 1)
        self.assertEqual(issues.count(), 0)
        row = current.collect()[0]
        self.assertEqual(row["event_id"], f"placsp:notice:{REF_A}@{t3.isoformat()}")
        self.assertFalse(row["is_deleted"])
        self.assertEqual(row["title"], "v3")

    def test_undated_competing_tombstone_is_unresolved_despite_ingested_at(self) -> None:
        t1 = datetime(2026, 1, 8, 10, 0, tzinfo=UTC)
        notice = _notice(REF_A, t1, ingested_at=datetime(2026, 1, 9, tzinfo=UTC))
        tombstone = _undated_tombstone(REF_A, ingested_at=datetime(2026, 1, 20, tzinfo=UTC))
        for ordered in ([notice, tombstone], [tombstone, notice]):
            with self.subTest(order=[r["event_id"][:30] for r in ordered]):
                current, issues = self._build(ordered)
                self.assertEqual(current.count(), 0)
                self.assertEqual(issues.count(), 2)
                reasons = {r["reason"] for r in issues.collect()}
                self.assertEqual(reasons, {REASON_UNDATED_COMPETING_EVENTS})
                # No current state is fabricated from retrieval provenance.
                self.assertEqual(
                    {r["procedure_id"] for r in issues.collect()}, {PROC_A}
                )

    def test_null_procedure_id_never_receives_invented_identity(self) -> None:
        null_row = _ted_notice("ted:notice:NULL", None)
        valid_row = _ted_notice(
            "ted:notice:VALID", "ted:procedure:ABC", ingested_at=datetime(2026, 1, 6, tzinfo=UTC)
        )
        current, issues = self._build([null_row, valid_row])
        self.assertEqual(current.count(), 1)
        row = current.collect()[0]
        self.assertEqual(row["procedure_id"], "ted:procedure:ABC")
        self.assertEqual(row["event_id"], "ted:notice:VALID")
        self.assertFalse(row["is_deleted"])
        self.assertEqual(issues.count(), 1)
        issue = issues.collect()[0]
        self.assertIsNone(issue["procedure_id"])
        self.assertEqual(issue["event_id"], "ted:notice:NULL")
        self.assertEqual(issue["reason"], REASON_MISSING_PROCEDURE_ID)

    def test_ted_multi_undated_procedure_is_unresolved(self) -> None:
        rows = [
            _ted_notice("ted:notice:A", "ted:procedure:X"),
            _ted_notice("ted:notice:B", "ted:procedure:X"),
        ]
        current, issues = self._build(rows)
        self.assertEqual(current.count(), 0)
        self.assertEqual(issues.count(), 2)
        self.assertEqual(
            {r["reason"] for r in issues.collect()}, {REASON_UNDATED_COMPETING_EVENTS}
        )

    def test_deterministic_tie_break_by_event_id(self) -> None:
        moment = datetime(2026, 1, 10, 10, 0, tzinfo=UTC)
        notice = _notice(REF_A, moment, title="tie-notice")
        tombstone = _dated_tombstone(REF_A, moment)
        # event_id is unique, so the total order is total even on timestamp ties.
        self.assertNotEqual(notice["event_id"], tombstone["event_id"])
        winners = set()
        for ordered in ([notice, tombstone], [tombstone, notice]):
            with self.subTest(order=[r["event_id"][:40] for r in ordered]):
                current, issues = self._build(ordered)
                self.assertEqual(current.count(), 1)
                self.assertEqual(issues.count(), 0)
                winners.add(current.collect()[0]["event_id"])
        self.assertEqual(len(winners), 1)
        # Largest event_id wins deterministically (tombstone > notice here).
        self.assertEqual(winners.pop(), tombstone["event_id"])

    def test_result_is_independent_of_physical_input_order(self) -> None:
        t1 = datetime(2026, 1, 8, 10, 0, tzinfo=UTC)
        t2 = datetime(2026, 1, 9, 11, 30, tzinfo=UTC)
        t3 = datetime(2026, 1, 9, 11, 30, tzinfo=UTC)
        rows_fwd = [
            _notice(REF_A, t1, title="v1"),
            _notice(REF_A, t2, title="v2"),
            _ted_notice("ted:notice:SINGLE", "ted:procedure:SINGLE"),
        ]
        rows_rev = list(reversed(rows_fwd))
        cur_fwd, iss_fwd = self._build(rows_fwd)
        cur_rev, iss_rev = self._build(rows_rev)
        self.assertEqual(
            sorted(r["event_id"] for r in cur_fwd.collect()),
            sorted(r["event_id"] for r in cur_rev.collect()),
        )
        self.assertEqual(
            sorted(r["event_id"] for r in iss_fwd.collect()),
            sorted(r["event_id"] for r in iss_rev.collect()),
        )
        # One row per resolvable procedure.
        self.assertEqual(cur_fwd.count(), 2)
        self.assertEqual(
            sorted(r["procedure_id"] for r in cur_fwd.collect()),
            sorted([PROC_A, "ted:procedure:SINGLE"]),
        )

    def test_lone_undated_events_resolve_without_competition(self) -> None:
        single_ted = _ted_notice("ted:notice:LONE", "ted:procedure:LONE")
        current, issues = self._build([single_ted])
        self.assertEqual(current.count(), 1)
        self.assertEqual(issues.count(), 0)
        self.assertFalse(current.collect()[0]["is_deleted"])

        lone_tombstone = _undated_tombstone(REF_B)
        current2, issues2 = self._build([lone_tombstone])
        self.assertEqual(current2.count(), 1)
        self.assertEqual(issues2.count(), 0)
        self.assertTrue(current2.collect()[0]["is_deleted"])

    def test_writes_typed_parquet_with_existing_foundation(self) -> None:
        t1 = datetime(2026, 1, 8, 10, 0, tzinfo=UTC)
        t2 = datetime(2026, 1, 9, 11, 30, tzinfo=UTC)
        rows = [
            _notice(REF_A, t1, title="v1"),
            _notice(REF_A, t2, title="v2"),
            _ted_notice("ted:notice:NULL", None),
        ]
        frame = self.spark.createDataFrame(rows, schema=canonical_silver_schema())
        current, issues = build_current_state(frame)
        with tempfile.TemporaryDirectory() as tmp:
            current_path = Path(tmp) / "current_state"
            issues_path = Path(tmp) / "current_state_issues"
            write_typed_parquet(
                current,
                current_path,
                expected_schema=current_state_schema(),
                order_by=("procedure_id",),
            )
            write_typed_parquet(
                issues,
                issues_path,
                expected_schema=current_state_issues_schema(),
                order_by=("event_id",),
            )
            loaded_current = self.spark.read.schema(current_state_schema()).parquet(
                str(current_path)
            )
            loaded_issues = self.spark.read.schema(current_state_issues_schema()).parquet(
                str(issues_path)
            )
            self.assertEqual(loaded_current.count(), 1)
            self.assertEqual(loaded_issues.count(), 1)
            self.assertEqual(
                [f.name for f in loaded_current.schema],
                [f.name for f in CURRENT_STATE_FIELDS],
            )
            self.assertEqual(
                [f.name for f in loaded_issues.schema],
                [f.name for f in CURRENT_STATE_ISSUES_FIELDS],
            )

    def test_parquet_boundary_silver_to_current_state(self) -> None:
        t1 = datetime(2026, 1, 8, 10, 0, tzinfo=UTC)
        rows = [_notice(REF_A, t1, title="v1"), _ted_notice("ted:notice:NULL", None)]
        with tempfile.TemporaryDirectory() as tmp:
            silver_path = Path(tmp) / "silver" / "procurement_events.parquet"
            silver_path.parent.mkdir(parents=True)
            silver_frame = self.spark.createDataFrame(rows, schema=canonical_silver_schema())
            silver_frame.write.mode("overwrite").parquet(str(silver_path))
            # Boundary validation happens on read, as in production.
            checked = read_canonical_silver(self.spark, silver_path)
            self.assertEqual(checked.count(), 2)
            out = build_current_state_from_silver(
                self.spark, silver_path, Path(tmp) / "gold"
            )
            self.assertEqual(out["current_state_rows"], 1)
            self.assertEqual(out["current_state_issues_rows"], 1)
            loaded_current = self.spark.read.schema(current_state_schema()).parquet(
                out["current_state"]
            )
            self.assertEqual(
                loaded_current.collect()[0]["procedure_id"], PROC_A
            )


if __name__ == "__main__":
    unittest.main()
