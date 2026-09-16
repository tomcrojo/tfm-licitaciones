"""End-to-end smoke test using only temporary local files."""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
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
