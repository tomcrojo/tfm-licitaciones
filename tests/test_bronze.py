"""Offline evidence for the Bronze persistence and rejection contract."""

from __future__ import annotations

import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from tfm_licitaciones.atom import parse_atom_file, parse_placsp_atom, iter_placsp_zip
from tfm_licitaciones.bronze import BRONZE_SCHEMA, REJECTION_SCHEMA, load_raw_records
from tfm_licitaciones.io import read_parquet
from tfm_licitaciones.pipeline import run_pipeline


FEED = '''<feed xmlns="http://www.w3.org/2005/Atom"
    xmlns:at="http://purl.org/atompub/tombstones/1.0">
  <entry><id>https://example.invalid/1</id><title>Servicio cloud</title>
    <updated>2026-01-01T00:00:00Z</updated></entry>
  <entry><id>https://example.invalid/2</id><title> </title></entry>
  <entry><id>bad-id</id><title>Obras</title></entry>
  <entry><title>Sin identificador</title></entry>
  <at:deleted-entry ref="https://example.invalid/999"/>
</feed>'''
TED = {"_source": "ted", "ND": "T1", "TI": {"spa": "Servicio cloud"}, "PD": "2026-01-01Z"}


class BronzeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = self.root / "raw"
        self.raw.mkdir()
        self.output = self.root / "data"

    def write_raw(self, name: str, content: str) -> Path:
        path = self.raw / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def assert_counts(self, report: dict, parsed: int, accepted: int, rejected: int) -> None:
        self.assertEqual((report["parsed"], report["accepted"], report["rejected"]),
                         (parsed, accepted, rejected))
        self.assertEqual(report["parsed"], report["accepted"] + report["rejected"])

    def test_ted_jsonl_rejections_do_not_hide_later_records(self) -> None:
        self.write_raw("ted/mixed.jsonl", "\n".join([
            json.dumps(TED), "{broken", "[]", json.dumps({"_source": "ted", "TI": "Cloud"}),
            json.dumps({"_source": "ted", "ND": "T2"}),
            json.dumps({**TED, "ND": "T3", "extra": [1, {"nested": None}]}), "", "",
        ]))
        loaded = load_raw_records(self.raw)
        self.assert_counts(loaded["ingestion"], 6, 2, 4)
        self.assert_counts(loaded["ingestion"]["by_source"]["ted"], 6, 2, 4)
        self.assertEqual({row["rejection_reason"] for row in loaded["rejections"]},
                         {"invalid_json", "expected_json_object", "missing_tender_id", "missing_title"})
        self.assertEqual({row["record_locator"] for row in loaded["rejections"]},
                         {"line:2", "line:3", "line:4", "line:5"})
        self.assertTrue(all(row["source"] == "ted" and row["source_file"] == "ted/mixed.jsonl"
                            for row in loaded["rejections"]))
        self.assertEqual([r.tender_id for r in loaded["records"]], ["T1", "T3"])

    def test_plain_atom_and_xml_account_for_all_entries(self) -> None:
        for suffix in ("atom", "xml"):
            self.write_raw(f"placsp/sample.{suffix}", FEED)
        loaded = load_raw_records(self.raw)
        self.assert_counts(loaded["ingestion"], 8, 2, 6)
        self.assertEqual(loaded["ingestion"]["atom_files"], 2)
        self.assertEqual({row["record_locator"] for row in loaded["rejections"]},
                         {"entry:2", "entry:3", "entry:4"})
        self.assertTrue(all(row["source_member"] is None for row in loaded["rejections"]))
        self.assertEqual(loaded["rejections"][0]["source_record_id"], "https://example.invalid/2")
        # Existing plain-feed Silver path does not apply tombstones.
        self.assertEqual(loaded["tombstone_ids"], set())

    def test_zip_keeps_member_provenance_and_continues_after_invalid_xml(self) -> None:
        path = self.raw / "sample.zip"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("a-broken.atom", "<feed")
            archive.writestr("nested/valid.atom", FEED)
            archive.writestr("readme.txt", "Metadata, not an Atom feed")
        loaded = load_raw_records(self.raw)
        self.assert_counts(loaded["ingestion"], 4, 1, 3)
        self.assertEqual(loaded["ingestion"]["document_errors"], 1)
        self.assertEqual(loaded["ingestion"]["placsp_entries"], 4)
        self.assertEqual(loaded["ingestion"]["atom_files"], 2)
        self.assertEqual(loaded["tombstone_ids"], {"999"})
        entry = loaded["bronze"][0]
        self.assertEqual((entry["source_file"], entry["source_member"], entry["record_locator"]),
                         ("sample.zip", "nested/valid.atom", "entry:1"))
        self.assertEqual(entry["payload"]["_atom_file"], "nested/valid.atom")
        rejected = loaded["rejections"][0]
        self.assertEqual(rejected["source_member"], "a-broken.atom")
        self.assertIsNone(rejected["record_locator"])

    def test_unreadable_documents_are_not_invented_record_counts(self) -> None:
        self.write_raw("placsp/broken.xml", "<feed")
        self.write_raw("placsp/wrong.xml", "<document />")
        self.write_raw("placsp/broken.zip", "not a ZIP")
        with zipfile.ZipFile(self.raw / "empty.zip", "w") as archive:
            archive.writestr("README.txt", "No feed")
        loaded = load_raw_records(self.raw)
        self.assert_counts(loaded["ingestion"], 0, 0, 0)
        self.assertEqual(loaded["ingestion"]["document_errors"], 4)
        self.assertFalse(loaded["ingestion"]["passed"])
        self.assertEqual({r["rejection_reason"] for r in loaded["rejections"]},
                         {"invalid_xml", "expected_atom_feed", "invalid_zip", "no_atom_members"})

    def test_invalid_deletion_control_is_observable_separately(self) -> None:
        self.write_raw("placsp/controls.atom", FEED.replace('ref="https://example.invalid/999"', 'ref=" "'))
        loaded = load_raw_records(self.raw)
        self.assert_counts(loaded["ingestion"], 4, 1, 3)
        self.assertEqual(loaded["ingestion"]["control_errors"], 1)
        control = next(r for r in loaded["rejections"] if r["rejection_scope"] == "control")
        self.assertEqual(control["record_locator"], "deleted-entry:1")

    def test_legacy_atom_adapters_fail_explicitly_on_rejections(self) -> None:
        path = self.write_raw("placsp/invalid.atom", FEED)
        with self.assertRaises(ValueError):
            parse_atom_file(path)
        with self.assertRaises(ValueError):
            parse_placsp_atom(FEED)
        with zipfile.ZipFile(self.raw / "invalid.zip", "w") as archive:
            archive.writestr("invalid.atom", FEED)
        with self.assertRaises(ValueError):
            iter_placsp_zip(self.raw / "invalid.zip")

    def test_nonfinite_json_and_unsupported_sources_are_rejected(self) -> None:
        self.write_raw("ted/invalid.jsonl", '\n'.join([
            '{"x": NaN}', '{"x": 1e999}', '{"_source": "unknown"}', json.dumps(TED)]))
        loaded = load_raw_records(self.raw)
        self.assert_counts(loaded["ingestion"], 4, 1, 3)
        self.assert_counts(loaded["ingestion"]["by_source"]["unknown"], 1, 0, 1)

    def test_invalid_encodings_are_located_and_do_not_stop_the_batch(self) -> None:
        path = self.write_raw("ted/encoding.jsonl", "")
        path.write_bytes(b'\xff\n' + json.dumps(TED).encode("utf-8") + b'\n')
        self.write_raw("placsp/encoding.atom", '<?xml version="1.0" encoding="unknown-charset"?><feed/>')
        loaded = load_raw_records(self.raw)
        self.assert_counts(loaded["ingestion"], 2, 1, 1)
        self.assertEqual(loaded["ingestion"]["document_errors"], 1)
        self.assertEqual({r["rejection_reason"] for r in loaded["rejections"]},
                         {"invalid_xml", "invalid_json"})

    def test_atom_nonfinite_amount_cannot_break_parquet_serialization(self) -> None:
        fixture = Path(__file__).parent / "fixtures/placsp/mini-placsp.atom"
        self.write_raw("placsp/amount.atom", fixture.read_text().replace(">120000.0<", ">1e999<"))
        result = run_pipeline(raw_dir=self.raw, output_root=self.output)
        self.assert_counts(result["manifest"]["ingestion"], 2, 1, 1)
        rejected = read_parquet(self.output / "bronze/rejections.parquet")
        self.assertEqual(rejected["rejection_reason"].to_list(), ["non_finite_amount"])

    def test_parquet_roundtrip_and_reports_are_reproducible(self) -> None:
        payload = {**TED, "extra": {"nested": [1, True, None, "á"]}}
        self.write_raw("ted/sample.jsonl", json.dumps(payload) + "\n{broken\n")
        first = run_pipeline(raw_dir=self.raw, output_root=self.output)
        accepted = read_parquet(self.output / "bronze/records.parquet")
        rejected = read_parquet(self.output / "bronze/rejections.parquet")
        self.assertEqual(dict(accepted.schema), BRONZE_SCHEMA)
        self.assertEqual(dict(rejected.schema), REJECTION_SCHEMA)
        self.assertEqual(json.loads(accepted["payload_json"][0]), payload)
        self.assertEqual(rejected["rejection_reason"].to_list(), ["invalid_json"])
        report = json.loads((self.output / "bronze/ingestion_report.json").read_text())
        self.assertEqual(report, first["manifest"]["ingestion"])
        self.assertFalse(report["passed"])
        # Silver/Gold gates retain their meaning; ingestion has its own status.
        self.assertTrue(first["manifest"]["quality_passed"])
        self.assertEqual(len(first["manifest"]["raw_files"][0]["sha256"]), 64)
        second = run_pipeline(raw_dir=self.raw, output_root=self.output)
        self.assertTrue(accepted.equals(read_parquet(self.output / "bronze/records.parquet")))
        self.assertTrue(rejected.equals(read_parquet(self.output / "bronze/rejections.parquet")))
        self.assertEqual(first["manifest"]["ingestion"], second["manifest"]["ingestion"])

    def test_empty_batch_and_all_rejected_batch_keep_explicit_schemas(self) -> None:
        for all_rejected in (False, True):
            if all_rejected:
                self.write_raw("ted/invalid.jsonl", "[]\n")
            result = run_pipeline(raw_dir=self.raw, output_root=self.output)
            accepted = read_parquet(self.output / "bronze/records.parquet")
            rejected = read_parquet(self.output / "bronze/rejections.parquet")
            self.assertEqual(accepted.height, 0)
            self.assertEqual(rejected.height, int(all_rejected))
            self.assertEqual(dict(accepted.schema), BRONZE_SCHEMA)
            self.assertEqual(dict(rejected.schema), REJECTION_SCHEMA)
            self.assertFalse(result["quality"]["passed"])


if __name__ == "__main__":
    unittest.main()
