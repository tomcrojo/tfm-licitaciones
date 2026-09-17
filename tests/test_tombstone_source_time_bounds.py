"""Boundary regressions for PLACSP tombstone source instants."""

from __future__ import annotations

import unittest

from tfm_licitaciones.atom import parse_atom_batch

REF = "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/99900001"


class TombstoneSourceTimeBoundaryTests(unittest.TestCase):
    def test_utc_normalization_overflow_is_observable_control_error(self) -> None:
        for value in (
            "0001-01-01T00:00:00+14:00",
            "9999-12-31T23:59:59-14:00",
        ):
            with self.subTest(value=value):
                xml = f'''<feed xmlns="http://www.w3.org/2005/Atom"
                    xmlns:at="http://purl.org/atompub/tombstones/1.0">
                  <at:deleted-entry ref="{REF}" when="{value}"/>
                </feed>'''
                batch = parse_atom_batch(xml)

                self.assertEqual(len(batch.tombstone_rows), 1)
                self.assertIsNone(batch.tombstone_rows[0]["source_deleted_at"])
                self.assertEqual(len(batch.rejections), 1)
                self.assertEqual(batch.rejections[0]["source_record_id"], REF)
                self.assertEqual(batch.rejections[0]["rejection_reason"], "invalid_tombstone_when")
                self.assertEqual(batch.rejections[0]["rejection_scope"], "control")


if __name__ == "__main__":
    unittest.main()
