"""End-to-end orchestration across raw, bronze, silver and gold layers.

Engine boundaries: Python parses Raw artifacts and serializes outputs;
Polars owns Bronze assembly accounting, the compatibility Silver fold,
classification, linkage candidate generation, quality metrics and Gold
aggregations; PySpark owns the canonical ``procurement_events`` build over
the Bronze Parquet boundary. Every stage records its duration in the run
manifest.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import polars as pl

from .bronze import load_raw_records
from .classify import score_frame
from .config import configured_path, load_config
from .evaluation import evaluate_against_cpv
from .io import write_csv, write_json, write_jsonl, write_parquet
from .linkage import link_frame
from .marts import buyer_summary, opportunities_frame, opportunities_rows, technology_summary
from .models import OpportunityRecord, TenderRecord
from .quality import validate_frames
from .silver import build_procurement_events


def fold_frame(frame: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Collapse re-published notices keeping the most recent update.

    OpenPLACSP streams every revision of a notice through the syndication,
    so the same atom id appears in several Atom files and monthly zips.
    Recency uses the entry ``updated`` timestamp rather than file order
    because the base Atom holds the latest state but sorts first inside
    each zip. Raw loading already selects the latest snapshot per partition
    and orders retrievals chronologically; position breaks ties across
    windows. The native sort/group_by keeps one row per
    ``(source, tender_id)`` and restores retrieval order afterwards.
    """

    if frame.height == 0:
        return frame, 0
    latest = (
        frame.sort(["source", "tender_id", "updated", "position"])
        .group_by(["source", "tender_id"])
        .last()
        .sort("position")
    )
    return latest, frame.height - latest.height


def _fold_frame(records: list[TenderRecord]) -> pl.DataFrame:
    """Minimal identity frame used by the row-level fold API."""

    return pl.DataFrame(
        {
            "source": [record.source for record in records],
            "tender_id": [record.tender_id for record in records],
            "updated": [record.raw.get("updated") or "" for record in records],
        },
        schema={"source": pl.String, "tender_id": pl.String, "updated": pl.String},
    ).with_row_index("position")


def fold_latest_updates(records: list[TenderRecord]) -> tuple[list[TenderRecord], int]:
    """Row-level fold API over the native implementation."""

    identities = _fold_frame(records)
    folded, removed = fold_frame(identities)
    positions = folded["position"].to_list()
    return [records[position] for position in positions], removed


def _raw_payload(row: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct the source-specific payload kept by the legacy contract."""

    payload = json.loads(row["payload_json"])
    if row["source"] == "placsp":
        payload.pop("_source", None)
    return payload


def _tender_record(row: dict[str, Any]) -> TenderRecord:
    return TenderRecord(
        tender_id=row["tender_id"],
        source=row["source"],
        title=row["title"] or "",
        summary=row["summary"] or "",
        buyer=row["buyer"],
        published_date=row["published_date"],
        amount=row["amount"],
        currency=row["currency"],
        country=row["country"],
        url=row["url"],
        cpv_main=row["cpv_codes"][0] if row["cpv_codes"] else None,
        buyer_id=row["buyer_id"],
        region=row["region"],
        status=row["status"],
        raw=_raw_payload(row),
    )


def _tender_rows(frame: pl.DataFrame) -> Iterator[dict[str, Any]]:
    """Stream the legacy Silver JSONL shape without materializing objects."""

    for row in frame.iter_rows(named=True):
        record = _tender_record(row)
        yield record.to_dict()


def _opportunity_records(scored: pl.DataFrame) -> list[OpportunityRecord]:
    return [
        OpportunityRecord(
            tender=_tender_record(row),
            category=row["category"],
            technology_score=row["technology_score"],
            matched_keywords=tuple(row["matched_keywords"]),
            category_source=row["category_source"],
            dup_group=row["dup_group"],
            is_canonical=bool(row["is_canonical"]),
            duplicate_of=row["duplicate_of"],
        )
        for row in scored.iter_rows(named=True)
    ]


def _linkage_columns(linkage: Any) -> pl.DataFrame:
    assignments = pl.DataFrame(
        {
            "source": [key[0] for key in linkage.assignments],
            "tender_id": [key[1] for key in linkage.assignments],
            "dup_group": [value["dup_group"] for value in linkage.assignments.values()],
            "is_canonical": [value["is_canonical"] for value in linkage.assignments.values()],
            "duplicate_of": [value["duplicate_of"] for value in linkage.assignments.values()],
        },
        schema={
            "source": pl.String,
            "tender_id": pl.String,
            "dup_group": pl.Int64,
            "is_canonical": pl.Boolean,
            "duplicate_of": pl.String,
        },
    )
    return assignments


def run_pipeline(
    *,
    raw_dir: str | Path | None = None,
    output_root: str | Path | None = None,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Run the deterministic local pipeline and persist every layer output."""

    stages: dict[str, float] = {}
    config = load_config(config_path)
    raw_path = Path(raw_dir) if raw_dir else configured_path(config, "raw_dir")
    if not raw_path.is_absolute():
        raw_path = Path(config["_project_root"]) / raw_path
    if output_root:
        output_path = Path(output_root)
        bronze_path = output_path / "bronze"
        silver_path = output_path / "silver"
        gold_path = output_path / "gold"
    else:
        bronze_path = configured_path(config, "bronze_dir")
        silver_path = configured_path(config, "silver_dir")
        gold_path = configured_path(config, "gold_dir")

    started = time.perf_counter()
    loaded = load_raw_records(raw_path)
    bronze_rows, candidates = loaded["bronze"], loaded["silver_candidates"]
    tombstone_ids = loaded["tombstone_ids"]
    write_parquet(bronze_path / "records.parquet", bronze_rows)
    write_parquet(bronze_path / "rejections.parquet", loaded["rejections"])
    write_parquet(bronze_path / "tombstones.parquet", loaded["tombstones"])
    stages["bronze"] = time.perf_counter() - started

    started = time.perf_counter()
    if tombstone_ids:
        alive = candidates.filter(~pl.col("tender_id").is_in(sorted(tombstone_ids)))
    else:
        alive = candidates
    kept_records, folded_updates = fold_frame(alive)
    ingestion_stats = {
        **loaded["ingestion"],
        "tombstoned_removed": candidates.height - alive.height,
        "updates_folded": folded_updates,
    }
    write_json(bronze_path / "ingestion_report.json", ingestion_stats)
    write_jsonl(silver_path / "tenders.jsonl", _tender_rows(kept_records))
    stages["silver"] = time.perf_counter() - started

    started = time.perf_counter()
    silver_canonical = build_procurement_events(bronze_path, silver_path)
    stages["canonical_silver"] = time.perf_counter() - started

    started = time.perf_counter()
    classifier_config = config["classifier"]
    categories = classifier_config["categories"]
    cpv_map = classifier_config.get("cpv_map", {})
    scored = score_frame(kept_records, categories, cpv_map)
    stages["classify"] = time.perf_counter() - started

    started = time.perf_counter()
    linkage_config = config.get("linkage", {})
    linkage = link_frame(
        kept_records,
        window_days=int(linkage_config.get("window_days", 7)),
        threshold=float(linkage_config.get("threshold", 0.75)),
        pair_budget=int(linkage_config.get("pair_budget", 5_000_000)),
    )
    scored = scored.join(
        _linkage_columns(linkage), on=["source", "tender_id"], how="left", coalesce=True
    ).with_columns(pl.col("is_canonical").fill_null(True))
    stages["linkage"] = time.perf_counter() - started

    started = time.perf_counter()
    quality = validate_frames(kept_records, scored, config["quality"], linkage_stats=linkage.stats)
    opportunities = _opportunity_records(scored)
    evaluation = evaluate_against_cpv(opportunities, cpv_map)
    canonical = [item for item in opportunities if item.is_canonical]
    stages["quality"] = time.perf_counter() - started

    started = time.perf_counter()
    canonical_frame = opportunities_frame(canonical)
    write_jsonl(gold_path / "opportunities.jsonl", (item.to_dict() for item in opportunities))
    write_csv(gold_path / "opportunities.csv", opportunities_rows(scored), _opportunity_fields())
    write_csv(
        gold_path / "technology_summary.csv",
        technology_summary(canonical_frame),
        ["category", "publication_month", "n_tenders", "technology_score"],
    )
    write_csv(
        gold_path / "buyer_summary.csv",
        buyer_summary(canonical_frame),
        ["buyer", "category", "n_tenders", "technology_score"],
    )
    write_json(gold_path / "quality_report.json", quality)
    write_json(gold_path / "classifier_evaluation.json", evaluation)
    stages["gold"] = time.perf_counter() - started

    manifest = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "raw_dir": _display_path(raw_path, Path(config["_project_root"])),
        "raw_files": [_artifact_manifest(artifact, raw_path) for artifact in _manifest_artifacts(loaded["artifacts"])],
        "counts": {
            "bronze": bronze_rows.height,
            "silver": kept_records.height,
            "gold": len(opportunities),
            "gold_canonical": len(canonical),
        },
        "source_counts": dict(sorted(kept_records.group_by("source").len().iter_rows())),
        "ingestion": ingestion_stats,
        "linkage": linkage.stats,
        "silver_canonical": silver_canonical,
        "stages": {name: round(seconds, 4) for name, seconds in stages.items()},
        "quality_passed": quality["passed"],
    }
    write_json(gold_path / "run_manifest.json", manifest)
    return {"manifest": manifest, "quality": quality, "opportunities": opportunities}


def _manifest_artifacts(artifacts: list[Any]) -> list[Any]:
    """Manifest order follows the deterministic raw discovery order."""

    return sorted(artifacts, key=lambda artifact: Path(artifact.raw_path))


def _artifact_manifest(artifact: Any, raw_dir: Path) -> dict[str, Any]:
    """Reuse the checksum already verified against the stored bytes."""

    return {
        "path": artifact.raw_path,
        "bytes": (raw_dir / artifact.raw_path).stat().st_size,
        "sha256": artifact.sha256,
    }


def _opportunity_fields() -> list[str]:
    """Return the stable field order for the main gold mart."""

    return [
        "tender_id",
        "source",
        "title",
        "summary",
        "buyer",
        "published_date",
        "amount",
        "currency",
        "country",
        "url",
        "cpv_main",
        "buyer_id",
        "region",
        "status",
        "category",
        "category_source",
        "technology_score",
        "matched_keywords",
        "dup_group",
        "is_canonical",
        "duplicate_of",
    ]


def _display_path(path: Path, root: Path) -> str:
    """Prefer a project-relative path in manifests and fall back for external inputs."""

    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)
