from __future__ import annotations

import unittest

from tfm_licitaciones.gold_contract import (
    CURRENT_STATE_FIELDS,
    GOLD_OPEN_OPPORTUNITIES_FIELDS,
    field_names,
)


class GoldContractTests(unittest.TestCase):
    def test_current_state_has_unique_stable_fields(self) -> None:
        names = field_names(CURRENT_STATE_FIELDS)
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual(names[0:4], ("procedure_id", "event_id", "source", "source_event_type"))
        self.assertEqual(names[-1], "is_deleted")

    def test_current_state_requires_real_procedure_identity(self) -> None:
        procedure = CURRENT_STATE_FIELDS[0]
        self.assertEqual(procedure.name, "procedure_id")
        self.assertFalse(procedure.nullable)

    def test_gold_contract_does_not_freeze_unimplemented_ranking_fields(self) -> None:
        names = field_names(GOLD_OPEN_OPPORTUNITIES_FIELDS)
        self.assertNotIn("score", names)
        self.assertNotIn("technology_score", names)
        self.assertNotIn("rank", names)
        self.assertIn("procedure_id", names)
        self.assertIn("cpv_codes", names)


if __name__ == "__main__":
    unittest.main()
