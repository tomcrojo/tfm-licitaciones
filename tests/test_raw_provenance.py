"""Offline download → Raw evidence → accepted/rejected Bronze integration."""

from __future__ import annotations

import io
import json
import logging
import os
import shutil
import ssl
import tempfile
import unittest
import zipfile
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

from raw_fixtures import RETRIEVED_AT, evidence_for_fixture
from tfm_licitaciones.bronze import load_raw_records
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


def zip_bytes(members: list[tuple[str, bytes]]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
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

    def test_placsp_fetch_cache_checks_evidence_and_never_redates(self) -> None:
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
        with self.assertRaisesRegex(RawProvenanceError, "Missing or invalid"):
            fetch_placsp(START, END, self.config, self.raw)
        self.assertFalse(provenance_path(path).exists())

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
        row = load_raw_records(self.raw)["rejections"][0]
        self.assertEqual(row["rejection_reason"], "source_mismatch")
        self.assertEqual(row["raw_sha256"], file_sha256(path))
        self.assertEqual(row["raw_retrieved_at"], RETRIEVED_AT)

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

    def test_interrupted_publication_never_produces_usable_unprovenanced_bronze(self) -> None:
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
        with self.assertRaises(RawProvenanceError):
            persist_raw_artifact(staged, destination, self.raw, source="ted", retrieved_at=LATER)

    def test_raw_tree_can_move_with_sidecars(self) -> None:
        self.download_ted([TED])
        before = load_raw_records(self.raw)["bronze"]
        relocated = self.root / "relocated"
        shutil.copytree(self.raw, relocated)
        self.assertEqual(load_raw_records(relocated)["bronze"], before)
