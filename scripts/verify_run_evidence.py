"""Verify a retained real-run snapshot without downloading or modifying data."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

import polars as pl

DEFAULT_INVENTORY = Path(__file__).resolve().parents[1] / "docs/evidence/real-run-2026-09-17/artifact_index.json"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_snapshot(root: Path, inventory_path: Path = DEFAULT_INVENTORY) -> dict:
    """Compare bytes, hashes and tabular counts with the recorded inventory.

    The inventory authenticates a retained snapshot, not a new download of
    mutable source data. It does not certify that the source was complete.
    """
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    checked = []
    for entry in inventory["artifacts"]:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"invalid inventory path: {relative}")
        path = root / relative
        if path.stat().st_size != entry["bytes"]:
            raise ValueError(f"size mismatch: {relative}")
        if sha256_file(path) != entry["sha256"]:
            raise ValueError(f"SHA-256 mismatch: {relative}")
        if "rows" in entry:
            if path.suffix == ".csv":
                with path.open(encoding="utf-8", newline="") as handle:
                    reader = csv.reader(handle)
                    next(reader)  # Headers are not data rows; embedded newlines are valid.
                    rows = sum(1 for _ in reader)
            elif path.suffix == ".parquet":
                rows = pl.scan_parquet(path).select(pl.len()).collect().item()
            else:
                raise ValueError(f"unsupported row count format: {relative}")
            if rows != entry["rows"]:
                raise ValueError(f"row count mismatch: {relative}: {rows} != {entry['rows']}")
        checked.append(entry["path"])
    result = {"passed": True, "verified_artifacts": len(checked), "paths": checked}
    if "observed_gold" in inventory:
        gold_paths = [root / name for name in checked if name.startswith("out/gold2/open_opportunities/") and name.endswith(".parquet")]
        gold = pl.scan_parquet(gold_paths).select(
            pl.len().alias("rows"),
            pl.col("procedure_id").n_unique().alias("distinct_procedure_ids"),
            pl.col("publication_date").null_count().alias("missing_publication_date"),
        ).collect().to_dicts()[0]
        if gold != inventory["observed_gold"]:
            raise ValueError("Gold identity/date metrics mismatch")
        result["observed_gold"] = gold
    if "observed_silver_source_counts" in inventory:
        silver = pl.scan_parquet(root / "out/silver/procurement_events.parquet")
        counts = {row["source"]: row["len"] for row in silver.group_by("source").len().collect().to_dicts()}
        if counts != inventory["observed_silver_source_counts"]:
            raise ValueError("Silver source counts mismatch")
        result["observed_silver_source_counts"] = counts
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--inventory", default=DEFAULT_INVENTORY, type=Path)
    args = parser.parse_args()
    print(json.dumps(verify_snapshot(args.run_root, args.inventory), indent=2))


if __name__ == "__main__":
    main()
