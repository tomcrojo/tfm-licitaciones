"""Offline download → Raw evidence → accepted/rejected Bronze integration."""

from __future__ import annotations

import io
import json
import logging
import lzma
import os
import shutil
import ssl
import struct
import tempfile
import unittest
import zipfile
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

from raw_fixtures import RETRIEVED_AT, evidence_for_fixture
from tfm_licitaciones.bronze import TOMBSTONE_SCHEMA, load_raw_records
from tfm_licitaciones.cli import main
from tfm_licitaciones.config import load_config
from tfm_licitaciones.fetch import _download_zip, fetch_placsp, fetch_raw_batch, save_raw_batches
from tfm_licitaciones.io import read_parquet
from tfm_licitaciones.pipeline import run_pipeline
from tfm_licitaciones.raw_provenance import (
    RawProvenanceError, file_sha256, load_raw_artifact, persist_raw_artifact, provenance_path,
)

START, END = date(2026, 1, 1), date(2026, 1, 31)
LATER = RETRIEVED_AT + timedelta(days=50)
TED = {"ND": "T1", "TI": {"spa": "Servicio cloud"}, "PD": "2026-01-01Z"}
FEED = b'''<feed xmlns="http://www.w3.org/2005/Atom"
  xmlns:at="http://purl.org/atompub/tombstones/1.0">
  <entry><id>https://example.invalid/1</id><title>Servicio cloud</title>
  <updated>2026-01-01T00:00:00Z</updated></entry>
  <entry><id>https://example.invalid/2</id></entry>
  <at:deleted-entry/>
</feed>'''


def zip_bytes(members: list[tuple[str, bytes]], compression=zipfile.ZIP_STORED) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=compression) as archive:
        for name, data in members:
            archive.writestr(name, data)
    return output.getvalue()


class RawProvenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.raw = self.root / "raw"
        self.raw.mkdir()
        self.config = load_config()

    def download_ted(self, rows: list[dict], retrieved_at=RETRIEVED_AT) -> Path:
        response = io.BytesIO(json.dumps({"notices": rows}).encode())
        with mock.patch("tfm_licitaciones.fetch.urlopen", return_value=response), \
             mock.patch("tfm_licitaciones.fetch.datetime") as clock:
            clock.now.return_value = retrieved_at
            count, paths = fetch_raw_batch("ted", START, END, self.config, self.raw)
        self.assertEqual(count, len(rows))
        return paths[0]

    def download_zip(self, content: bytes, retrieved_at=RETRIEVED_AT) -> Path:
        with mock.patch("tfm_licitaciones.fetch.urlopen", return_value=io.BytesIO(content)), \
             mock.patch("tfm_licitaciones.fetch.datetime") as clock:
            clock.now.return_value = retrieved_at
            return _download_zip(
                "https://example.invalid/month.zip", self.raw / "placsp/placsp-202601.zip",
                raw_dir=self.raw, partition="202601", window_start=START.isoformat(),
                window_end=END.isoformat(), context=ssl.create_default_context(),
                timeout=10, attempts=1, logger=logging.getLogger(__name__),
            )

    def test_ted_download_and_parquet_rerun_inherit_raw_evidence(self) -> None:
        path = self.download_ted([TED, {"ND": "missing-title"}])
        sidecar_before = provenance_path(path).read_bytes()
        artifact = load_raw_artifact(self.raw, path)
        self.assertEqual((artifact.source, artifact.partition, artifact.window_start, artifact.window_end),
                         ("ted", "20260101-20260131", "2026-01-01", "2026-01-31"))
        self.assertEqual(artifact.sha256, file_sha256(path))
        self.assertEqual(artifact.retrieved_at, RETRIEVED_AT.isoformat())
        first = self.root / "first"
        second = self.root / "second"
        run_pipeline(raw_dir=self.raw, output_root=first)
        # Neither filesystem timestamps nor the current transformation clock
        # may become retrieval evidence. No downloader runs during transforms.
        os.utime(path, (LATER.timestamp(), LATER.timestamp()))
        with mock.patch("tfm_licitaciones.fetch.datetime") as clock:
            clock.now.side_effect = AssertionError("transformation must not retrieve")
            run_pipeline(raw_dir=self.raw, output_root=second)
        for name in ("records", "rejections"):
            frame = read_parquet(first / f"bronze/{name}.parquet")
            self.assertEqual(frame.height, 1)
            self.assertEqual(frame["raw_sha256"].to_list(), [artifact.sha256])
            self.assertEqual(frame["raw_retrieved_at"].to_list(), [RETRIEVED_AT])
            self.assertEqual(frame["source_file"].to_list(), [artifact.raw_path])
            self.assertEqual(frame["source_member_index"].to_list(), [None])
            self.assertTrue(frame.equals(read_parquet(second / f"bronze/{name}.parquet")))
        self.assertEqual(provenance_path(path).read_bytes(), sidecar_before)

    def test_ted_identical_download_retains_first_timestamp_and_changed_batch_is_versioned(self) -> None:
        first = self.download_ted([TED])
        before = first.read_bytes(), provenance_path(first).read_bytes()
        self.assertEqual(self.download_ted([TED], LATER), first)
        changed = self.download_ted([{**TED, "TI": "Corrección cloud"}], LATER)
        self.assertNotEqual(first, changed)
        self.assertEqual((first.read_bytes(), provenance_path(first).read_bytes()), before)
        newer = load_raw_artifact(self.raw, changed)
        self.assertEqual(newer.partition, load_raw_artifact(self.raw, first).partition)
        self.assertIn(newer.sha256, changed.name)
        self.assertEqual(newer.retrieved_at, LATER.isoformat())
        self.assertEqual(self.download_ted([{**TED, "TI": "Corrección cloud"}], LATER + timedelta(days=1)), changed)
        loaded = load_raw_records(self.raw)
        self.assertEqual({row["raw_retrieved_at"] for row in loaded["bronze"]}, {RETRIEVED_AT, LATER})
        self.assertEqual(len({row["raw_sha256"] for row in loaded["bronze"]}), 2)

    def test_corrected_ted_and_boe_snapshots_reach_legacy_silver_and_gold(self) -> None:
        for source in ("ted", "boe"):
            with self.subTest(source=source):
                raw = self.raw / source
                # A published correction carries a new version marker (TED
                # notice number / BOE item id): canonical Silver keeps both
                # events, the legacy current state uses the latest snapshot.
                first_row = {**TED, "_source": source} if source == "ted" else {
                    "_source": "boe", "item_id": "B1", "title": "Servicio cloud",
                    "publication_date": "2026-01-01",
                }
                id_key = "ND" if source == "ted" else "item_id"
                second_row = {**first_row, id_key: f"{first_row[id_key]}-C",
                              "TI" if source == "ted" else "title": "Corrección cloud"}

                def retrieve(row, timestamp):
                    # Exercise the real download boundary with a local adapter response.
                    with mock.patch(f"tfm_licitaciones.fetch.fetch_{source}", return_value=[row]), \
                         mock.patch("tfm_licitaciones.fetch.datetime") as clock:
                        clock.now.return_value = timestamp
                        return fetch_raw_batch(source, START, END, self.config, raw)[1][0]

                first = retrieve(first_row, RETRIEVED_AT)
                original = first.read_bytes(), provenance_path(first).read_bytes()
                second = retrieve(second_row, LATER)
                # The lexical order that caused the regression puts the correction first.
                self.assertLess(second.name, first.name)
                output = self.root / f"out-{source}"
                result = run_pipeline(raw_dir=raw, output_root=output)
                counts = result["manifest"]["counts"]
                self.assertEqual(counts["bronze_records"], 2)
                self.assertEqual(counts["legacy_current_state_records"], 1)
                self.assertEqual(counts["gold_opportunities"], 1)
                self.assertEqual(result["manifest"]["ingestion"]["superseded_artifacts"], 1)
                self.assertEqual(result["manifest"]["ingestion"]["superseded_records"], 1)
                bronze = read_parquet(output / "bronze/records.parquet")
                self.assertEqual(set(bronze["raw_retrieved_at"]), {RETRIEVED_AT, LATER})
                self.assertEqual(set(bronze["raw_sha256"]), {file_sha256(first), file_sha256(second)})
                canonical = read_parquet(output / "silver/procurement_events.parquet")
                self.assertEqual(canonical.height, 2)
                self.assertEqual(canonical["source_event_type"].to_list(), ["notice", "notice"])
                gold = json.loads((output / "gold/opportunities.jsonl").read_text())
                self.assertEqual(gold["title"], "Corrección cloud")
                self.assertEqual((first.read_bytes(), provenance_path(first).read_bytes()), original)
                # Re-retrieving the old bytes does not redate them or revert the view.
                self.assertEqual(retrieve(first_row, LATER + timedelta(days=1)), first)
                repeated = run_pipeline(raw_dir=raw, output_root=output)
                self.assertEqual(repeated["opportunities"][0].tender.title, "Corrección cloud")

    def test_corrected_placsp_snapshot_retracts_old_tombstone_effects_only_in_legacy_view(self) -> None:
        entry = b'''<feed xmlns="http://www.w3.org/2005/Atom" xmlns:at="http://purl.org/atompub/tombstones/1.0">
          <entry><id>https://example.invalid/1</id><title>Servicio cloud</title>
          <updated>2026-01-01T00:00:00Z</updated></entry></feed>'''
        withdrawn = entry.replace(b"</feed>", b'<at:deleted-entry ref="https://example.invalid/1"/></feed>')
        first = self.download_zip(zip_bytes([("feed.atom", withdrawn)]))
        before = first.read_bytes(), provenance_path(first).read_bytes()
        output = self.root / "out"
        initial = run_pipeline(raw_dir=self.raw, output_root=output)
        # Legacy view removes the tombstoned notice; canonical Silver keeps
        # both the notice snapshot and the deletion control as history.
        self.assertEqual(initial["manifest"]["counts"]["legacy_current_state_records"], 0)
        self.assertEqual(initial["manifest"]["counts"]["silver_procurement_events"], 2)
        corrected = entry.replace(b"cloud", b"cloud corregido").replace(
            b"2026-01-01T00:00:00Z", b"2026-01-02T00:00:00Z"
        )
        second = self.download_zip(zip_bytes([("feed.atom", corrected)]), LATER)
        result = run_pipeline(raw_dir=self.raw, output_root=output)
        counts = result["manifest"]["counts"]
        self.assertEqual(counts["bronze_records"], 2)
        self.assertEqual(counts["legacy_current_state_records"], 1)
        self.assertEqual(counts["gold_opportunities"], 1)
        self.assertEqual(counts["silver_procurement_events"], 3)
        self.assertEqual(result["opportunities"][0].tender.title, "Servicio cloud corregido")
        self.assertEqual(result["manifest"]["ingestion"]["tombstone_ids"], 0)
        self.assertEqual(result["manifest"]["ingestion"]["tombstoned_removed"], 0)
        self.assertEqual(result["manifest"]["ingestion"]["tombstones"], 1)
        control = read_parquet(output / "bronze/tombstones.parquet").row(0, named=True)
        self.assertEqual(control["raw_sha256"], file_sha256(first))
        self.assertEqual(control["raw_retrieved_at"], RETRIEVED_AT)
        self.assertEqual(control["source_record_id"], "https://example.invalid/1")
        self.assertNotEqual(control["raw_sha256"], file_sha256(second))
        # The tombstone event survives even though the corrected snapshot omits it.
        events = read_parquet(output / "silver/procurement_events.parquet")
        self.assertIn("placsp:tombstone:https://example.invalid/1", events["event_id"].to_list())
        self.assertEqual((first.read_bytes(), provenance_path(first).read_bytes()), before)

    def test_latest_snapshot_needs_unambiguous_retrieval_order(self) -> None:
        self.download_ted([TED])
        self.download_ted([{**TED, "TI": "Corrección"}])
        with self.assertRaisesRegex(RawProvenanceError, "Ambiguous latest Raw snapshot"):
            load_raw_records(self.raw)

    def test_snapshot_selection_keeps_other_partitions_and_observes_superseded_rejections(self) -> None:
        self.download_ted([TED, {"ND": "missing-title"}])
        save_raw_batches(self.raw, "20260201-20260228", [{**TED, "ND": "T2"}], [], retrieved_at=LATER,
                         window_start="2026-02-01", window_end="2026-02-28")
        self.download_ted([{**TED, "TI": "Corrección cloud"}], LATER)
        loaded = load_raw_records(self.raw)
        self.assertEqual({record.tender_id: record.title for record in loaded["records"]},
                         {"T1": "Corrección cloud", "T2": "Servicio cloud"})
        self.assertEqual(len(loaded["bronze"]), 3)
        self.assertEqual(loaded["ingestion"]["superseded_artifacts"], 1)
        self.assertEqual(loaded["ingestion"]["superseded_records"], 1)
        self.assertEqual(loaded["ingestion"]["rejected"], 1)
        self.assertFalse(loaded["ingestion"]["passed"])

    def test_persistence_requires_explicit_retrieval_time_and_does_not_replace_it(self) -> None:
        with self.assertRaises(TypeError):
            save_raw_batches(self.raw, "batch", [TED], [])
        with mock.patch("tfm_licitaciones.fetch.datetime") as clock:
            clock.now.side_effect = AssertionError("persistence must not read the clock")
            path = save_raw_batches(self.raw, "batch", [TED], [], retrieved_at=RETRIEVED_AT)[0]
        self.assertEqual(load_raw_artifact(self.raw, path).timestamp, RETRIEVED_AT)

    def test_cli_ted_uses_download_boundary(self) -> None:
        with mock.patch("tfm_licitaciones.fetch.urlopen", return_value=io.BytesIO(json.dumps({"notices": [TED]}).encode())), \
             mock.patch("tfm_licitaciones.fetch.datetime") as clock, mock.patch("builtins.print"):
            clock.now.return_value = RETRIEVED_AT
            self.assertEqual(main(["ingest", "--source", "ted", "--start", str(START), "--end", str(END),
                                   "--raw-dir", str(self.raw)]), 0)
        self.assertEqual(load_raw_records(self.raw)["bronze"][0]["raw_retrieved_at"], RETRIEVED_AT)

    def test_placsp_zip_all_scopes_and_duplicate_member_names_have_container_evidence(self) -> None:
        with self.assertWarns(UserWarning):
            content = zip_bytes([("README.txt", b"metadata"), ("nested/feed.atom", FEED),
                                 ("nested/feed.atom", b"<feed"), ("other.atom", FEED)])
        path = self.download_zip(content)
        artifact = load_raw_artifact(self.raw, path)
        self.assertEqual((artifact.source, artifact.partition, artifact.window_start, artifact.window_end),
                         ("placsp", "202601", "2026-01-01", "2026-01-31"))
        output = self.root / "out"
        run_pipeline(raw_dir=self.raw, output_root=output)
        accepted = read_parquet(output / "bronze/records.parquet").to_dicts()
        rejected = read_parquet(output / "bronze/rejections.parquet").to_dicts()
        self.assertEqual(len(accepted), 2)
        self.assertEqual({r["rejection_scope"] for r in rejected}, {"record", "document", "control"})
        for row in accepted + rejected:
            self.assertEqual(row["source_file"], artifact.raw_path)
            self.assertEqual(row["raw_sha256"], artifact.sha256)
            self.assertEqual(row["raw_retrieved_at"], RETRIEVED_AT)
            self.assertIsNotNone(row["source_member_index"])
        self.assertEqual({(r["source_member"], r["source_member_index"]) for r in accepted},
                         {("nested/feed.atom", 2), ("other.atom", 4)})
        broken = next(r for r in rejected if r["rejection_reason"] == "invalid_xml")
        self.assertEqual((broken["source_member"], broken["source_member_index"]), ("nested/feed.atom", 3))
        self.assertIsNone(broken["record_locator"])
        # Resolve the duplicate filename through its central-directory index.
        with zipfile.ZipFile(path) as archive:
            self.assertEqual(archive.read(archive.infolist()[broken["source_member_index"] - 1]), b"<feed")

    def test_placsp_fetch_cache_checks_evidence_never_redates_and_repairs_an_orphan(self) -> None:
        content = zip_bytes([("feed.atom", FEED)])
        with mock.patch("tfm_licitaciones.fetch.urlopen", return_value=io.BytesIO(content)), \
             mock.patch("tfm_licitaciones.fetch.datetime") as clock:
            clock.now.return_value = RETRIEVED_AT
            paths = fetch_placsp(START, END, self.config, self.raw)
        path = paths[0]
        before = provenance_path(path).read_bytes()
        with mock.patch("tfm_licitaciones.fetch.urlopen", side_effect=AssertionError("cached download")), \
             mock.patch("tfm_licitaciones.fetch.datetime") as clock:
            clock.now.side_effect = AssertionError("cached artifact must not get a timestamp")
            self.assertEqual(fetch_placsp(START, END, self.config, self.raw), paths)
        self.assertEqual(provenance_path(path).read_bytes(), before)
        provenance_path(path).unlink()
        with mock.patch("tfm_licitaciones.fetch.urlopen", return_value=io.BytesIO(content)), \
             mock.patch("tfm_licitaciones.fetch.datetime") as clock:
            clock.now.return_value = LATER
            self.assertEqual(fetch_placsp(START, END, self.config, self.raw), paths)
        repaired = load_raw_artifact(self.raw, path)
        self.assertEqual(repaired.timestamp, LATER)
        self.assertEqual(repaired.sha256, file_sha256(path))

    def test_changed_zip_keeps_both_retrievals_and_same_named_members_distinct(self) -> None:
        first = self.download_zip(zip_bytes([("feed.atom", FEED)]))
        second = self.download_zip(zip_bytes([("feed.atom", FEED.replace(b"cloud", b"nube"))]), LATER)
        self.assertNotEqual(first, second)
        loaded = load_raw_records(self.raw)
        rows = loaded["bronze"]
        self.assertEqual(len(rows), 2)
        self.assertEqual({r["source_member"] for r in rows}, {"feed.atom"})
        self.assertEqual(len({r["raw_sha256"] for r in rows}), 2)
        self.assertEqual({r["raw_retrieved_at"] for r in rows}, {RETRIEVED_AT, LATER})
        self.assertEqual(load_raw_artifact(self.raw, first).partition, load_raw_artifact(self.raw, second).partition)

    def test_unreadable_zip_member_retains_index_and_container_evidence(self) -> None:
        # Corrupt the stored member's CRC while leaving the ZIP directory readable.
        content = zip_bytes([("broken.atom", FEED), ("valid.atom", FEED)])
        path = self.download_zip(content.replace(b"Servicio cloud", b"Servicio clouX", 1))
        loaded = load_raw_records(self.raw)
        rejection = next(r for r in loaded["rejections"] if r["rejection_reason"] == "unreadable_zip_member")
        self.assertEqual((rejection["source_member"], rejection["source_member_index"]), ("broken.atom", 1))
        self.assertEqual(rejection["raw_sha256"], file_sha256(path))
        self.assertEqual(rejection["raw_retrieved_at"], RETRIEVED_AT)
        self.assertEqual(loaded["bronze"][0]["source_member_index"], 2)

    def test_corrupt_bzip2_and_lzma_members_are_independent_located_rejections(self) -> None:
        for compression, error in ((zipfile.ZIP_BZIP2, OSError), (zipfile.ZIP_LZMA, lzma.LZMAError)):
            with self.subTest(compression=compression):
                content = bytearray(zip_bytes([("README.txt", b"metadata"), ("broken.atom", FEED),
                                               ("valid.atom", FEED)], compression))
                with zipfile.ZipFile(io.BytesIO(content)) as archive:
                    member = archive.infolist()[1]
                name_length, extra_length = struct.unpack_from("<HH", content, member.header_offset + 26)
                data_offset = member.header_offset + 30 + name_length + extra_length
                if compression == zipfile.ZIP_BZIP2:
                    content[data_offset:data_offset + 3] = b"BAD"
                else:
                    # Invalid LZMA properties after ZIP's four-byte codec header.
                    content[data_offset + 4] = 255
                # Verify the actual codec failure, not a mock or CRC fallback.
                with zipfile.ZipFile(io.BytesIO(content)) as archive:
                    with self.assertRaises(error):
                        archive.read(archive.infolist()[1])
                raw = self.raw / str(compression)
                raw.mkdir()
                path = raw / "compressed.zip"
                path.write_bytes(content)
                evidence_for_fixture(raw, path, "placsp")
                output = self.root / f"compressed-{compression}"
                result = run_pipeline(raw_dir=raw, output_root=output)
                rows = read_parquet(output / "bronze/rejections.parquet").to_dicts()
                rejection = next(row for row in rows if row["rejection_reason"] == "unreadable_zip_member")
                self.assertEqual((rejection["source_member"], rejection["source_member_index"]), ("broken.atom", 2))
                self.assertEqual(rejection["source_file"], "compressed.zip")
                self.assertEqual(rejection["raw_sha256"], file_sha256(path))
                self.assertEqual(rejection["raw_retrieved_at"], RETRIEVED_AT)
                accepted = read_parquet(output / "bronze/records.parquet")
                self.assertEqual(accepted["source_member_index"].to_list(), [3])
                self.assertEqual(result["manifest"]["ingestion"]["document_errors"], 1)
                self.assertFalse(result["manifest"]["ingestion"]["passed"])

    def test_partial_placsp_request_keeps_month_identity_and_rejects_stale_cache(self) -> None:
        content = zip_bytes([("feed.atom", FEED)])
        with mock.patch("tfm_licitaciones.fetch.urlopen", return_value=io.BytesIO(content)), \
             mock.patch("tfm_licitaciones.fetch.datetime") as clock:
            clock.now.return_value = RETRIEVED_AT
            path = fetch_placsp(date(2026, 1, 10), date(2026, 1, 12), self.config, self.raw)[0]
        evidence = load_raw_artifact(self.raw, path)
        self.assertEqual((evidence.partition, evidence.window_start, evidence.window_end),
                         ("202601", "2026-01-01", "2026-01-31"))
        sidecar = provenance_path(path)
        sidecar.write_text(json.dumps({**json.loads(sidecar.read_text()), "partition": "202602"}))
        with self.assertRaisesRegex(RawProvenanceError, "partition identity mismatch"):
            fetch_placsp(START, END, self.config, self.raw)

    def test_failed_zip_download_publishes_neither_payload_nor_evidence(self) -> None:
        with self.assertRaises(RuntimeError):
            self.download_zip(b"not a zip")
        self.assertEqual([p for p in self.raw.rglob("*") if p.is_file()], [])

    def test_missing_evidence_cannot_be_manufactured_by_transform(self) -> None:
        path = self.raw / "legacy.jsonl"
        path.write_text(json.dumps({**TED, "_source": "ted"}) + "\n")
        with self.assertRaisesRegex(RawProvenanceError, "Missing or invalid"):
            load_raw_records(self.raw)
        self.assertFalse(provenance_path(path).exists())

    def test_external_payload_change_is_detected_by_transform_and_download(self) -> None:
        path = self.download_ted([TED])
        before = provenance_path(path).read_bytes()
        path.write_bytes(path.read_bytes() + b"\n")
        for action in (lambda: load_raw_records(self.raw), lambda: self.download_ted([TED], LATER)):
            with self.assertRaisesRegex(RawProvenanceError, "checksum mismatch"):
                action()
        self.assertEqual(provenance_path(path).read_bytes(), before)

    def test_invalid_metadata_fails_explicitly(self) -> None:
        path = self.download_ted([TED])
        sidecar = provenance_path(path)
        original = json.loads(sidecar.read_text())
        for change in ({"raw_path": "different.jsonl"}, {"raw_path": "../escape.jsonl"},
                       {"sha256": "0" * 64}, {"retrieved_at": None}, {"retrieved_at": "2026-01-01"},
                       {"source": ""}, {"version": 2}, {"window_end": None}):
            with self.subTest(change=change):
                sidecar.write_text(json.dumps({**original, **change}))
                with self.assertRaises(RawProvenanceError):
                    load_raw_records(self.raw)
        for content in ("{broken", "[]", "{}"):
            sidecar.write_text(content)
            with self.assertRaises(RawProvenanceError):
                load_raw_records(self.raw)

    def test_conflicting_record_source_is_rejected_with_artifact_identity(self) -> None:
        path = self.raw / "mixed.jsonl"
        path.write_text(json.dumps({"_source": "boe", "item_id": "B1", "title": "Cloud"}))
        evidence_for_fixture(self.raw, path, "ted")
        loaded = load_raw_records(self.raw)
        row = loaded["rejections"][0]
        self.assertEqual(row["rejection_reason"], "source_mismatch")
        self.assertEqual(row["source"], "ted")
        self.assertEqual(row["raw_sha256"], file_sha256(path))
        self.assertEqual(row["raw_retrieved_at"], RETRIEVED_AT)
        self.assertEqual(set(loaded["ingestion"]["by_source"]), {"ted"})
        self.assertEqual({key: loaded["ingestion"]["by_source"]["ted"][key]
                          for key in ("parsed", "accepted", "rejected")},
                         {"parsed": 1, "accepted": 0, "rejected": 1})
        self.assertEqual(json.loads(path.read_text())["_source"], "boe")

    def test_plain_atom_and_unreadable_container_rejections_inherit_evidence(self) -> None:
        for name, content in (("plain.atom", FEED), ("broken.zip", b"invalid")):
            path = self.raw / name
            path.write_bytes(content)
            evidence_for_fixture(self.raw, path, "placsp")
        loaded = load_raw_records(self.raw)
        for row in loaded["bronze"] + loaded["rejections"]:
            self.assertEqual(row["raw_sha256"], file_sha256(self.raw / row["source_file"]))
            self.assertEqual(row["raw_retrieved_at"], RETRIEVED_AT)
            self.assertIsNone(row["source_member"])
            self.assertIsNone(row["source_member_index"])

    def test_payload_orphan_fails_closed_and_new_matching_download_recovers_with_retry_time(self) -> None:
        staged = self.raw / "staged.tmp"
        staged.write_text(json.dumps({**TED, "_source": "ted"}))
        destination = self.raw / "ted.jsonl"
        link = os.link

        def fail_sidecar(source, target):
            if str(target).endswith(".provenance.json"):
                raise OSError("interrupted")
            link(source, target)

        with mock.patch("tfm_licitaciones.raw_provenance.os.link", side_effect=fail_sidecar):
            with self.assertRaises(OSError):
                persist_raw_artifact(staged, destination, self.raw, source="ted", retrieved_at=RETRIEVED_AT)
        with self.assertRaises(RawProvenanceError):
            load_raw_records(self.raw)
        original = destination.read_bytes()
        other = self.raw / "other.tmp"
        other.write_bytes(original + b"\n")
        with self.assertRaisesRegex(RawProvenanceError, "checksum mismatch"):
            persist_raw_artifact(other, destination, self.raw, source="ted", retrieved_at=LATER)
        self.assertEqual(destination.read_bytes(), original)
        self.assertFalse(provenance_path(destination).exists())
        self.assertEqual(persist_raw_artifact(staged, destination, self.raw, source="ted", retrieved_at=LATER), destination)
        self.assertEqual(load_raw_artifact(self.raw, destination).timestamp, LATER)
        self.assertTrue(load_raw_records(self.raw)["ingestion"]["passed"])
        before = provenance_path(destination).read_bytes()
        persist_raw_artifact(staged, destination, self.raw, source="ted", retrieved_at=LATER + timedelta(days=1))
        self.assertEqual(provenance_path(destination).read_bytes(), before)

    def test_sidecar_orphan_fails_closed_and_matching_retry_retains_original_evidence(self) -> None:
        staged = self.raw / "staged.tmp"
        staged.write_text(json.dumps({**TED, "_source": "ted"}))
        destination = self.raw / "ted.jsonl"
        identity = {"source": "ted", "partition": "202601", "window_start": str(START), "window_end": str(END)}
        persist_raw_artifact(staged, destination, self.raw, retrieved_at=RETRIEVED_AT, **identity)
        sidecar = provenance_path(destination)
        before = sidecar.read_bytes()
        destination.unlink()
        # Orphan-only Raw must not return a successful empty ingestion report.
        with self.assertRaisesRegex(RawProvenanceError, "without its payload"):
            load_raw_records(self.raw)
        with self.assertRaisesRegex(RawProvenanceError, "without its payload"):
            run_pipeline(raw_dir=self.raw, output_root=self.root / "out")
        self.assertFalse((self.root / "out").exists())
        other = self.raw / "other.tmp"
        other.write_bytes(staged.read_bytes() + b"\n")
        for payload, overrides in ((other, {}), (staged, {"source": "boe"}),
                                   (staged, {"partition": "202602"}), (staged, {"window_end": "2026-02-01"})):
            with self.subTest(overrides=overrides, payload=payload.name):
                with self.assertRaises(RawProvenanceError):
                    persist_raw_artifact(payload, destination, self.raw, retrieved_at=LATER, **{**identity, **overrides})
                self.assertFalse(destination.exists())
                self.assertEqual(sidecar.read_bytes(), before)
        self.assertEqual(persist_raw_artifact(staged, destination, self.raw, retrieved_at=LATER, **identity), destination)
        self.assertEqual(sidecar.read_bytes(), before)
        self.assertEqual(load_raw_records(self.raw)["bronze"][0]["raw_retrieved_at"], RETRIEVED_AT)
        persist_raw_artifact(staged, destination, self.raw, retrieved_at=LATER + timedelta(days=1), **identity)
        self.assertEqual(sidecar.read_bytes(), before)

    def test_checksum_named_revision_orphans_are_recoverable_without_replacing_base(self) -> None:
        for missing in ("payload", "sidecar"):
            with self.subTest(missing=missing):
                raw = self.raw / missing
                first = save_raw_batches(raw, "batch", [TED], [], retrieved_at=RETRIEVED_AT)[0]
                original = first.read_bytes(), provenance_path(first).read_bytes()
                correction = [{**TED, "TI": "Corrección cloud"}]
                revision = save_raw_batches(raw, "batch", correction, [], retrieved_at=LATER)[0]
                (revision if missing == "payload" else provenance_path(revision)).unlink()
                with self.assertRaises(RawProvenanceError):
                    load_raw_records(raw)
                retry_time = LATER + timedelta(days=1)
                self.assertEqual(save_raw_batches(raw, "batch", correction, [], retrieved_at=retry_time), [revision])
                self.assertEqual(load_raw_artifact(raw, revision).timestamp,
                                 LATER if missing == "payload" else retry_time)
                self.assertEqual((first.read_bytes(), provenance_path(first).read_bytes()), original)
                self.assertEqual(load_raw_records(raw)["ingestion"]["accepted"], 2)

    def test_tombstones_persist_full_provenance_in_plain_and_duplicate_zip_members(self) -> None:
        feed = b'''<feed xmlns="http://www.w3.org/2005/Atom" xmlns:at="http://purl.org/atompub/tombstones/1.0">
          <at:deleted-entry ref="https://example.invalid/1"/>
          <at:deleted-entry/>
          <at:deleted-entry ref="https://example.invalid/1"/></feed>'''
        plain = self.raw / "plain.atom"
        plain.write_bytes(feed)
        evidence_for_fixture(self.raw, plain, "placsp")
        with self.assertWarns(UserWarning):
            path = self.download_zip(zip_bytes([("README.txt", b"metadata"), ("feed.atom", feed), ("feed.atom", feed)]))
        output = self.root / "out"
        result = run_pipeline(raw_dir=self.raw, output_root=output)
        frame = read_parquet(output / "bronze/tombstones.parquet")
        self.assertEqual(dict(frame.schema), TOMBSTONE_SCHEMA)
        self.assertEqual(frame.height, 6)
        for row in frame.to_dicts():
            self.assertEqual(row["source"], "placsp")
            self.assertEqual(row["source_record_id"], "https://example.invalid/1")
            self.assertIn(row["record_locator"], {"deleted-entry:1", "deleted-entry:3"})
            self.assertEqual(row["raw_sha256"], file_sha256(self.raw / row["source_file"]))
            self.assertEqual(row["raw_retrieved_at"], RETRIEVED_AT)
            if row["source_file"] == "plain.atom":
                self.assertIsNone(row["source_member"])
                self.assertIsNone(row["source_member_index"])
            else:
                self.assertEqual(row["source_member"], "feed.atom")
                self.assertIn(row["source_member_index"], {2, 3})
                with zipfile.ZipFile(path) as archive:
                    self.assertEqual(archive.read(archive.infolist()[row["source_member_index"] - 1]), feed)
        ingestion = result["manifest"]["ingestion"]
        self.assertEqual((ingestion["parsed"], ingestion["accepted"], ingestion["rejected"]), (0, 0, 0))
        self.assertEqual(ingestion["tombstones"], 6)
        self.assertEqual(ingestion["by_source"]["placsp"]["tombstones"], 6)
        self.assertEqual(ingestion["control_errors"], 3)
        self.assertEqual(ingestion["tombstone_ids"], 1)
        run_pipeline(raw_dir=self.raw, output_root=output)
        self.assertTrue(frame.equals(read_parquet(output / "bronze/tombstones.parquet")))

    def test_raw_tree_can_move_with_sidecars(self) -> None:
        self.download_ted([TED])
        before = load_raw_records(self.raw)["bronze"]
        relocated = self.root / "relocated"
        shutil.copytree(self.raw, relocated)
        self.assertEqual(load_raw_records(relocated)["bronze"], before)
