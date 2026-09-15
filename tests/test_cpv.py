"""Tests for the CPV 2008 hierarchical reference dimension."""

from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

import polars as pl

from tfm_licitaciones.cpv import (
    CPV_SCHEMA,
    DEFAULT_CPV_SOURCE,
    build_cpv_dimension,
    cpv_level,
    structural_parent_code,
)
from tfm_licitaciones.io import read_parquet, write_parquet

# Codes whose structural parent is absent from the official CPV 2008 table.
OFFICIAL_MISSING_INTERMEDIATES = {
    "34511000",
    "35611000",
    "35612000",
    "35811000",
    "38527000",
    "39250000",
    "42924000",
    "60110000",
}
OFFICIAL_SKIP_LEVEL_CHILDREN = 32


def write_source(path: Path, rows: list[tuple[str, str, str]]) -> Path:
    """Write a CPV source CSV with the official code,nombre,name layout."""

    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["code,nombre,name"]
    lines += [f"{code},{nombre},{name}" for code, nombre, name in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


class CpvLevelTests(unittest.TestCase):
    """Verify the official level semantics derived from the zero suffix."""

    def test_level_matches_zero_suffix_rule(self) -> None:
        self.assertEqual(cpv_level("03000000"), 1)
        self.assertEqual(cpv_level("03100000"), 2)
        self.assertEqual(cpv_level("03110000"), 3)
        self.assertEqual(cpv_level("03111000"), 4)
        self.assertEqual(cpv_level("03111100"), 5)

    def test_divisions_with_zero_second_digit_keep_level_one(self) -> None:
        self.assertEqual(cpv_level("30000000"), 1)
        self.assertEqual(cpv_level("30100000"), 2)
        self.assertEqual(structural_parent_code("30100000"), "30000000")

    def test_structural_parent_truncates_and_pads(self) -> None:
        self.assertIsNone(structural_parent_code("03000000"))
        self.assertEqual(structural_parent_code("03100000"), "03000000")
        self.assertEqual(structural_parent_code("03110000"), "03100000")
        self.assertEqual(structural_parent_code("03111000"), "03110000")
        self.assertEqual(structural_parent_code("03111100"), "03111000")


class SyntheticCpvDimensionTests(unittest.TestCase):
    """Build dimensions from small controlled vocabularies."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)

    def test_levels_parents_and_leaves(self) -> None:
        source = write_source(
            self.tmp / "cpv.csv",
            [
                ("03000000", "Division.", "Division."),
                ("03100000", "Group.", "Group."),
                ("03110000", "Class.", "Class."),
                ("03111000", "Category.", "Category."),
                ("03111100", "Detail.", "Detail."),
                ("03112000", "Childless category.", "Childless category."),
            ],
        )
        frame = build_cpv_dimension(source)
        by_code = {row["cpv_code"]: row for row in frame.iter_rows(named=True)}
        self.assertEqual(by_code["03000000"]["level"], 1)
        self.assertIsNone(by_code["03000000"]["parent_code"])
        self.assertFalse(by_code["03000000"]["is_leaf"])
        self.assertEqual(by_code["03100000"]["parent_code"], "03000000")
        self.assertEqual(by_code["03111000"]["parent_code"], "03110000")
        self.assertEqual(by_code["03111100"]["parent_code"], "03111000")
        self.assertTrue(by_code["03111100"]["is_leaf"])
        self.assertTrue(by_code["03112000"]["is_leaf"])
        self.assertFalse(by_code["03111000"]["is_leaf"])

    def test_missing_intermediate_parent_resolves_to_nearest_ancestor(self) -> None:
        source = write_source(
            self.tmp / "cpv.csv",
            [
                ("55000000", "Division.", "Division."),
                ("55100000", "Group.", "Group."),
                ("55123100", "Detail.", "Detail."),
            ],
        )
        frame = build_cpv_dimension(source)
        by_code = {row["cpv_code"]: row for row in frame.iter_rows(named=True)}
        self.assertEqual(by_code["55123100"]["parent_code"], "55100000")
        self.assertEqual(by_code["55123100"]["level"], 5)
        self.assertTrue(by_code["55123100"]["is_leaf"])
        self.assertFalse(by_code["55100000"]["is_leaf"])

    def test_ancestry_without_existing_root_fails(self) -> None:
        source = write_source(
            self.tmp / "cpv.csv",
            [("99000001", "Detail.", "Detail.")],
        )
        with self.assertRaises(ValueError) as ctx:
            build_cpv_dimension(source)
        self.assertIn("99000001", str(ctx.exception))

    def test_duplicate_codes_fail(self) -> None:
        source = write_source(
            self.tmp / "cpv.csv",
            [("03000000", "A.", "A."), ("03000000", "A.", "A.")],
        )
        with self.assertRaises(ValueError) as ctx:
            build_cpv_dimension(source)
        self.assertIn("03000000", str(ctx.exception))

    def test_invalid_codes_fail(self) -> None:
        source = write_source(
            self.tmp / "cpv.csv",
            [("1234567", "Short.", "Short."), ("123456789", "Long.", "Long."), ("1234567x", "Text.", "Text.")],
        )
        with self.assertRaises(ValueError) as ctx:
            build_cpv_dimension(source)
        self.assertIn("1234567", str(ctx.exception))
        self.assertIn("123456789", str(ctx.exception))
        self.assertIn("1234567x", str(ctx.exception))

    def test_missing_required_column_fails(self) -> None:
        path = self.tmp / "bad.csv"
        path.write_text("code,name\n03000000,Division.\n", encoding="utf-8")
        with self.assertRaises(ValueError) as ctx:
            build_cpv_dimension(path)
        self.assertIn("nombre", str(ctx.exception))

    def test_blank_english_label_is_stored_as_null(self) -> None:
        source = write_source(
            self.tmp / "cpv.csv",
            [("03000000", "Division.", "")],
        )
        frame = build_cpv_dimension(source)
        self.assertEqual(frame["label_es"][0], "Division.")
        self.assertIsNone(frame["label_en"][0])

    def test_empty_source_yields_typed_frame_and_parquet_round_trip(self) -> None:
        source = write_source(self.tmp / "cpv.csv", [])
        frame = build_cpv_dimension(source)
        self.assertEqual(frame.height, 0)
        self.assertEqual(frame.schema, CPV_SCHEMA)
        path = self.tmp / "cpv_codes.parquet"
        self.assertEqual(write_parquet(path, frame), 0)
        restored = read_parquet(path)
        self.assertEqual(restored.height, 0)
        self.assertEqual(restored.schema, CPV_SCHEMA)

    def test_schema_and_deterministic_ordering(self) -> None:
        rows = [
            ("03000000", "Division.", "Division."),
            ("03100000", "Group.", "Group."),
            ("03111000", "Category.", "Category."),
        ]
        source = write_source(self.tmp / "cpv.csv", rows)
        frame = build_cpv_dimension(source)
        self.assertEqual(frame.columns, list(CPV_SCHEMA.keys()))
        self.assertEqual(frame.schema, CPV_SCHEMA)
        self.assertEqual(frame["cpv_code"].to_list(), [code for code, _, _ in sorted(rows)])
        shuffled = write_source(self.tmp / "shuffled.csv", [rows[2], rows[0], rows[1]])
        self.assertTrue(build_cpv_dimension(shuffled).equals(frame))

    def test_parquet_round_trip_preserves_string_codes(self) -> None:
        source = write_source(
            self.tmp / "cpv.csv",
            [
                ("03000000", "Division.", "Division."),
                ("03111100", "Detail.", "Detail."),
            ],
        )
        path = self.tmp / "cpv_codes.parquet"
        frame = build_cpv_dimension(source)
        write_parquet(path, frame)
        restored = read_parquet(path)
        self.assertEqual(restored.schema, CPV_SCHEMA)
        self.assertEqual(restored["cpv_code"].to_list(), ["03000000", "03111100"])
        self.assertEqual(restored["parent_code"].to_list(), [None, "03000000"])
        self.assertTrue(restored.equals(frame))


class OfficialCpvTableTests(unittest.TestCase):
    """Invariants of the dimension built from the repository's official table."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.frame = build_cpv_dimension(DEFAULT_CPV_SOURCE)

    def test_measured_counts_and_level_distribution(self) -> None:
        self.assertEqual(self.frame.height, 9454)
        self.assertEqual(self.frame["cpv_code"].n_unique(), self.frame.height)
        level_counts = self.frame.group_by("level").len().sort("level")
        self.assertEqual(
            {row["level"]: row["len"] for row in level_counts.iter_rows(named=True)},
            {1: 45, 2: 272, 3: 1002, 4: 2379, 5: 5756},
        )

    def test_codes_are_sorted_strings_with_leading_zeroes(self) -> None:
        codes = self.frame["cpv_code"].to_list()
        self.assertEqual(codes, sorted(codes))
        self.assertIn("03000000", codes)
        self.assertTrue(all(len(code) == 8 for code in codes))

    def test_only_divisions_have_null_parent(self) -> None:
        null_parents = self.frame.filter(pl.col("parent_code").is_null())
        self.assertEqual(null_parents.height, 45)
        self.assertTrue((null_parents["level"] == 1).all())

    def test_parent_reference_integrity_and_acyclic_ancestry(self) -> None:
        rows = {row["cpv_code"]: row for row in self.frame.iter_rows(named=True)}
        for code, row in rows.items():
            parent = row["parent_code"]
            if parent is not None:
                self.assertIn(parent, rows)
            hops = 0
            current = row
            while current["parent_code"] is not None:
                current = rows[current["parent_code"]]
                hops += 1
                self.assertLess(hops, 5 + 1)
            self.assertEqual(current["level"], 1)

    def test_leaf_flags_match_children_existence(self) -> None:
        rows = {row["cpv_code"]: row for row in self.frame.iter_rows(named=True)}
        parents_with_children = {
            row["parent_code"] for row in rows.values() if row["parent_code"] is not None
        }
        for code, row in rows.items():
            self.assertEqual(row["is_leaf"], code not in parents_with_children)
        self.assertTrue(self.frame.filter(pl.col("level") == 5)["is_leaf"].all())

    def test_official_gaps_resolve_to_nearest_existing_ancestor(self) -> None:
        rows = {row["cpv_code"]: row for row in self.frame.iter_rows(named=True)}
        skip_children = [
            code
            for code, row in rows.items()
            if (parent := structural_parent_code(code)) is not None and parent not in rows
        ]
        self.assertEqual(len(skip_children), OFFICIAL_SKIP_LEVEL_CHILDREN)
        self.assertEqual(
            {structural_parent_code(code) for code in skip_children},
            OFFICIAL_MISSING_INTERMEDIATES,
        )
        self.assertEqual(rows["39254000"]["parent_code"], "39200000")
        self.assertEqual(rows["35611100"]["parent_code"], "35610000")
        self.assertEqual(rows["60112000"]["parent_code"], "60100000")

    def test_known_labels_and_missing_english_labels(self) -> None:
        rows = {row["cpv_code"]: row for row in self.frame.iter_rows(named=True)}
        self.assertEqual(
            rows["03000000"]["label_es"],
            "Productos de la agricultura, ganadería, pesca, silvicultura y productos afines.",
        )
        blank_english = sorted(
            code for code, row in rows.items() if row["label_en"] is None
        )
        self.assertEqual(blank_english, ["14820000", "14830000", "14930000"])
        self.assertEqual(rows["14820000"]["label_es"], "Vidrio.")

    def test_rebuild_is_deterministic_and_persists_to_parquet(self) -> None:
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp)
        rebuilt = build_cpv_dimension(DEFAULT_CPV_SOURCE)
        self.assertTrue(rebuilt.equals(self.frame))
        path = tmp / "cpv_codes.parquet"
        self.assertEqual(write_parquet(path, self.frame), 9454)
        restored = read_parquet(path)
        self.assertEqual(restored.schema, CPV_SCHEMA)
        self.assertTrue(restored.equals(self.frame))
        self.assertIn("03111100", restored["cpv_code"].to_list())


if __name__ == "__main__":
    unittest.main()
