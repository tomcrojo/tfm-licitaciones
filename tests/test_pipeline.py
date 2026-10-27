"""End-to-end smoke test using only temporary local files."""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path

from tfm_licitaciones.models import TenderRecord
from tfm_licitaciones.pipeline import fold_latest_updates, run_pipeline
from tfm_licitaciones.raw_provenance import RawProvenanceError
from raw_fixtures import evidence_for_fixture

FIXTURES = Path(__file__).parent / "fixtures"


class FoldTests(unittest.TestCase):
    """Check that re-published notices collapse to their latest revision."""

    def test_fold_keeps_most_recent_updated_timestamp(self) -> None:
        first = TenderRecord("1", "placsp", "Primera publicación", raw={"updated": "2026-01-05T10:00:00.000+01:00"})
        stale = TenderRecord("1", "placsp", "Copia obsoleta del solape mensual", raw={"updated": "2026-01-02T10:00:00.000+01:00"})
        update = TenderRecord("1", "placsp", "Rectificación final", raw={"updated": "2026-02-01T12:00:00.000+01:00"})
        untouched = TenderRecord("2", "ted", "Aviso TED único")
        folded, removed = fold_latest_updates([first, stale, update, untouched])
        self.assertEqual(removed, 2)
        self.assertEqual([record.title for record in folded], ["Rectificación final", "Aviso TED único"])

    def test_fold_keeps_position_when_timestamp_missing(self) -> None:
        first = TenderRecord("1", "ted", "A")
        second = TenderRecord("1", "ted", "B")
        folded, removed = fold_latest_updates([first, second])
        self.assertEqual(removed, 1)
        self.assertEqual(folded[0].title, "B")

    def test_fold_compares_source_instants_across_timezone_offsets(self) -> None:
        older = TenderRecord("1", "placsp", "Stale", raw={"updated": "2026-01-01T11:30:00+02:00"})
        newer = TenderRecord("1", "placsp", "Latest", raw={"updated": "2026-01-01T10:00:00Z"})
        for records in ([older, newer], [newer, older]):
            with self.subTest(titles=[record.title for record in records]):
                folded, removed = fold_latest_updates(records)
                self.assertEqual(folded, [newer])
                self.assertEqual(removed, 1)

    def test_fold_breaks_equivalent_timestamp_ties_by_position(self) -> None:
        first = TenderRecord("1", "placsp", "A", raw={"updated": "2026-01-01T11:00:00+01:00"})
        last = TenderRecord("1", "placsp", "B", raw={"updated": "2026-01-01T10:00:00.000Z"})
        folded, removed = fold_latest_updates([first, last])
        self.assertEqual(folded, [last])
        self.assertEqual(removed, 1)

    def test_fold_invalid_or_naive_timestamp_cannot_replace_dated_revision(self) -> None:
        dated = TenderRecord("1", "placsp", "Dated", raw={"updated": "2026-01-01T10:00:00Z"})
        for timestamp in (None, "not-a-date", "2027-01-01T10:00:00"):
            with self.subTest(timestamp=timestamp):
                undated = TenderRecord("1", "placsp", "Undated", raw={"updated": timestamp})
                self.assertEqual(fold_latest_updates([dated, undated])[0], [dated])


class PipelineTests(unittest.TestCase):
    """Verify that all medallion outputs are produced deterministically."""

    def test_run_pipeline_writes_all_layers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw" / "ted"
            raw.mkdir(parents=True)
            (raw / "sample.jsonl").write_text(
                json.dumps(
                    {
                        "_source": "ted",
                        "ND": "1",
                        "PD": "2024-01-01Z",
                        "TI": {"spa": "Servicio cloud"},
                        "description-glo": {"spa": "Migración a nube"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            evidence_for_fixture(raw.parent.parent, raw / "sample.jsonl", "ted")
            result = run_pipeline(raw_dir=raw.parent.parent, output_root=root / "data")
            self.assertTrue(result["quality"]["passed"])
            self.assertTrue((root / "data" / "bronze" / "records.parquet").exists())
            self.assertTrue((root / "data" / "bronze" / "rejections.parquet").exists())
            self.assertTrue((root / "data" / "silver" / "procurement_events.parquet").exists())
            self.assertFalse((root / "data" / "silver" / "tenders.jsonl").exists())
            self.assertTrue((root / "data" / "gold" / "opportunities.csv").exists())
            self.assertTrue((root / "data" / "gold" / "quality_report.json").exists())
            self.assertTrue((root / "data" / "gold" / "classifier_evaluation.json").exists())
            manifest = result["manifest"]
            self.assertIn("linkage", manifest)
            self.assertIn("ingestion", manifest)
            self.assertIn("category_source", result["opportunities"][0].to_dict())

    def test_run_pipeline_mixes_ted_and_placsp_with_linkage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw"
            (raw / "ted").mkdir(parents=True)
            (raw / "placsp").mkdir(parents=True)
            # Same procedure published in both sources: cross-source duplicate.
            (raw / "ted" / "sample.jsonl").write_text(
                json.dumps(
                    {
                        "_source": "ted",
                        "ND": "A-1",
                        "PD": "2026-01-08Z",
                        "TI": {"spa": "Migración a plataforma cloud y soporte de aplicaciones"},
                        "buyer-name": {"spa": ["Centro de Sistemas y Tecnologías de la Información"]},
                        "PC": ["72415000"],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            shutil.copy(FIXTURES / "raw" / "placsp" / "placsp-202601.zip", raw / "placsp" / "placsp-202601.zip")
            evidence_for_fixture(raw, raw / "ted" / "sample.jsonl", "ted")
            evidence_for_fixture(raw, raw / "placsp" / "placsp-202601.zip", "placsp")
            result = run_pipeline(raw_dir=raw, output_root=root / "data")
            counts = result["manifest"]["counts"]
            # 3 raw entries (1 TED + 2 PLACSP), tombstone keeps both PLACSP alive.
            self.assertEqual(counts["bronze_records"], 3)
            self.assertEqual(counts["legacy_current_state_records"], 3)
            self.assertEqual(counts["gold_opportunities"], 3)
            # Canonical Silver preserves history: the tombstone is also an event.
            self.assertEqual(counts["silver_procurement_events"], 4)
            linkage = result["manifest"]["linkage"]
            self.assertGreaterEqual(linkage["linked_pairs"], 1)
            self.assertIn("evaluated_pairs", linkage)
            self.assertIn("records_without_date", linkage)
            groups = [item for item in result["opportunities"] if item.dup_group is not None]
            self.assertTrue(groups)
            canonical = [item for item in groups if item.is_canonical]
            self.assertEqual(len(canonical), 1)
            # Same publication date: the longest title (richer PLACSP notice) wins.
            self.assertEqual(canonical[0].tender.source, "placsp")
            ted_item = next(item for item in groups if item.tender.source == "ted")
            self.assertEqual(ted_item.duplicate_of, canonical[0].tender.tender_id)
            report = result["quality"]
            self.assertIn("duplicate_share", report["metrics"])
            self.assertTrue(report["passed"])

    def test_revision_offsets_select_latest_gold_without_collapsing_silver_history(self) -> None:
        feed = '''<feed xmlns="http://www.w3.org/2005/Atom">
          <entry><id>https://example.invalid/1</id><title>Latest cloud</title>
          <updated>2026-01-01T10:00:00Z</updated></entry>
          <entry><id>https://example.invalid/1</id><title>Stale cloud</title>
          <updated>2026-01-01T11:30:00+02:00</updated></entry>
        </feed>'''
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw"
            archive = raw / "placsp" / "sample.zip"
            archive.parent.mkdir(parents=True)
            with zipfile.ZipFile(archive, "w") as handle:
                handle.writestr("feed.atom", feed)
            evidence_for_fixture(raw, archive, "placsp")
            result = run_pipeline(raw_dir=raw, output_root=root / "out")
            self.assertEqual(result["manifest"]["counts"]["silver_procurement_events"], 2)
            self.assertEqual(result["manifest"]["ingestion"]["updates_folded"], 1)
            self.assertEqual([item.tender.title for item in result["opportunities"]], ["Latest cloud"])
            exported = json.loads((root / "out/gold/opportunities.jsonl").read_text(encoding="utf-8"))
            self.assertEqual(exported["title"], "Latest cloud")

    def test_failed_run_retires_preexisting_legacy_tenders_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw" / "ted"
            raw.mkdir(parents=True)
            sample = raw / "sample.jsonl"
            sample.write_text(
                json.dumps(
                    {
                        "_source": "ted",
                        "ND": "1",
                        "PD": "2024-01-01Z",
                        "TI": {"spa": "Servicio cloud"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            evidence_for_fixture(raw.parent.parent, sample, "ted")
            silver = root / "data" / "silver"
            silver.mkdir(parents=True)
            stale = silver / "tenders.jsonl"
            stale.write_text("{}\n", encoding="utf-8")
            # Corrupt the payload after its provenance sidecar was recorded so
            # the run fails during early Raw provenance validation.
            sample.write_text("{}\n", encoding="utf-8")
            with self.assertRaises(RawProvenanceError):
                run_pipeline(raw_dir=raw.parent.parent, output_root=root / "data")
            self.assertFalse(stale.exists())


if __name__ == "__main__":
    unittest.main()
