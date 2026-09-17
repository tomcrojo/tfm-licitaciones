from __future__ import annotations

import unittest

import polars as pl

from tfm_licitaciones.gold_contract import (
    CANONICAL_SILVER_FIELDS,
    CURRENT_STATE_FIELDS,
    GOLD_OPEN_OPPORTUNITIES_FIELDS,
)
from tfm_licitaciones.models import PROCUREMENT_EVENT_SCHEMA


def signature(fields):
    return tuple((field.name, field.logical_type, field.nullable) for field in fields)


def polars_logical_type(dtype: pl.DataType) -> str:
    if dtype == pl.String:
        return "string"
    if dtype == pl.Date:
        return "date"
    if dtype == pl.Datetime("us", "UTC"):
        return "timestamp"
    if dtype == pl.Decimal(precision=20, scale=2):
        return "decimal(20,2)"
    if dtype == pl.List(pl.String):
        return "array<string>"
    if dtype == pl.Boolean:
        return "boolean"
    raise AssertionError(f"unmapped canonical Silver Polars dtype: {dtype!r}")


CANONICAL_SILVER_V1 = (
    ("event_id", "string", False),
    ("procedure_id", "string", True),
    ("source", "string", False),
    ("source_event_type", "string", False),
    ("buyer_id", "string", True),
    ("buyer_name", "string", True),
    ("title", "string", True),
    ("description", "string", True),
    ("cpv_codes", "array<string>", False),
    ("estimated_value", "decimal(20,2)", True),
    ("awarded_value", "decimal(20,2)", True),
    ("currency", "string", True),
    ("publication_date", "date", True),
    ("source_updated_at", "timestamp", True),
    ("deadline", "timestamp", True),
    ("status", "string", True),
    ("nuts_code", "string", True),
    ("country", "string", True),
    ("source_url", "string", True),
    ("ingested_at", "timestamp", False),
)

CURRENT_STATE_V1 = (
    ("procedure_id", "string", False),
    ("event_id", "string", False),
    ("source", "string", False),
    ("source_event_type", "string", False),
    ("buyer_id", "string", True),
    ("buyer_name", "string", True),
    ("title", "string", True),
    ("description", "string", True),
    ("cpv_codes", "array<string>", False),
    ("estimated_value", "decimal(20,2)", True),
    ("awarded_value", "decimal(20,2)", True),
    ("currency", "string", True),
    ("publication_date", "date", True),
    ("source_updated_at", "timestamp", True),
    ("deadline", "timestamp", True),
    ("status", "string", True),
    ("nuts_code", "string", True),
    ("country", "string", True),
    ("source_url", "string", True),
    ("ingested_at", "timestamp", False),
    ("is_deleted", "boolean", False),
)

GOLD_OPEN_OPPORTUNITIES_V1 = (
    ("procedure_id", "string", False),
    ("event_id", "string", False),
    ("source", "string", False),
    ("buyer_id", "string", True),
    ("buyer_name", "string", True),
    ("title", "string", True),
    ("description", "string", True),
    ("cpv_codes", "array<string>", False),
    ("estimated_value", "decimal(20,2)", True),
    ("awarded_value", "decimal(20,2)", True),
    ("currency", "string", True),
    ("publication_date", "date", True),
    ("source_updated_at", "timestamp", True),
    ("deadline", "timestamp", True),
    ("status", "string", True),
    ("nuts_code", "string", True),
    ("country", "string", True),
    ("source_url", "string", True),
    ("ingested_at", "timestamp", False),
)


class GoldContractTests(unittest.TestCase):
    def test_canonical_silver_contract_is_pinned_and_tracks_polars_schema(self) -> None:
        self.assertEqual(signature(CANONICAL_SILVER_FIELDS), CANONICAL_SILVER_V1)
        self.assertEqual(
            tuple(field.name for field in CANONICAL_SILVER_FIELDS),
            tuple(PROCUREMENT_EVENT_SCHEMA.keys()),
        )
        self.assertEqual(
            tuple(field.logical_type for field in CANONICAL_SILVER_FIELDS),
            tuple(polars_logical_type(dtype) for dtype in PROCUREMENT_EVENT_SCHEMA.values()),
        )

    def test_current_state_v1_is_fully_pinned(self) -> None:
        self.assertEqual(signature(CURRENT_STATE_FIELDS), CURRENT_STATE_V1)

    def test_open_opportunities_v1_is_fully_pinned(self) -> None:
        self.assertEqual(
            signature(GOLD_OPEN_OPPORTUNITIES_FIELDS),
            GOLD_OPEN_OPPORTUNITIES_V1,
        )

    def test_gold_contract_does_not_freeze_unimplemented_ranking_fields(self) -> None:
        names = {field.name for field in GOLD_OPEN_OPPORTUNITIES_FIELDS}
        self.assertTrue({"score", "technology_score", "rank"}.isdisjoint(names))


if __name__ == "__main__":
    unittest.main()
