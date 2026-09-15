"""End-to-end orchestration across raw, bronze, silver and gold layers."""

from __future__ import annotations

import hashlib
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .bronze import bronze_frame, discover_raw_files, load_raw_records, rejection_frame
from .classify import score_tender
from .config import configured_path, load_config
from .evaluation import evaluate_against_cpv
from .io import write_csv, write_json, write_jsonl, write_parquet
from .linkage import link_duplicates
from .marts import buyer_summary, opportunities_rows, technology_summary
from .models import OpportunityRecord, TenderRecord
from .quality import validate_records


def fold_latest_updates(records: list[TenderRecord]) -> tuple[list[TenderRecord], int]:
    """Collapse re-published notices keeping the most recent update.

    OpenPLACSP streams every revision of a notice through the syndication,
    so the same atom id appears in several Atom files and monthly zips.
    Recency uses the entry ``updated`` timestamp rather than file order
    because the base Atom holds the latest state but sorts first inside
    each zip. TED and BOE windows contain each notice once, so the fold
    leaves them untouched.
    """

    latest: dict[tuple[str, str], tuple[Any, int, TenderRecord]] = {}
    for position, record in enumerate(records):
        timestamp = record.raw.get("updated")
        key = (record.source, record.tender_id)
        rank = (timestamp or "", position)
        previous = latest.get(key)
        if previous is None or rank > (previous[0], previous[1]):
            latest[key] = (timestamp or "", position, record)
    folded = [entry[2] for entry in sorted(latest.values(), key=lambda entry: entry[1])]
    return folded, len(records) - len(folded)


def run_pipeline(
    *,
    raw_dir: str | Path | None = None,
    output_root: str | Path | None = None,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Run the deterministic local pipeline and persist every layer output."""

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

    loaded = load_raw_records(raw_path)
    bronze_rows, records = loaded["bronze"], loaded["records"]
    tombstone_ids = loaded["tombstone_ids"]
    alive_records = [record for record in records if record.tender_id not in tombstone_ids]
    kept_records, folded_updates = fold_latest_updates(alive_records)
    ingestion_stats = {
        **loaded["ingestion"],
        "tombstoned_removed": len(records) - len(alive_records),
        "updates_folded": folded_updates,
    }
    write_parquet(bronze_path / "records.parquet", bronze_frame(bronze_rows))
    write_parquet(bronze_path / "rejections.parquet", rejection_frame(loaded["rejections"]))
    write_json(bronze_path / "ingestion_report.json", ingestion_stats)
    write_jsonl(silver_path / "tenders.jsonl", (record.to_dict() for record in kept_records))

    classifier_config = config["classifier"]
    categories = classifier_config["categories"]
    cpv_map = classifier_config.get("cpv_map", {})
    opportunities = [score_tender(record, categories, cpv_map) for record in kept_records]

    linkage_config = config.get("linkage", {})
    linkage = link_duplicates(
        kept_records,
        window_days=int(linkage_config.get("window_days", 7)),
        threshold=float(linkage_config.get("threshold", 0.75)),
        max_pairs_per_block=int(linkage_config.get("max_pairs_per_block", 500)),
    )
    enriched: list[OpportunityRecord] = []
    for item in opportunities:
        assignment = linkage.assignments.get((item.tender.source, item.tender.tender_id))
        if assignment is None:
            enriched.append(item)
        else:
            enriched.append(
                OpportunityRecord(
                    tender=item.tender,
                    category=item.category,
                    technology_score=item.technology_score,
                    matched_keywords=item.matched_keywords,
                    category_source=item.category_source,
                    dup_group=assignment["dup_group"],
                    is_canonical=assignment["is_canonical"],
                    duplicate_of=assignment["duplicate_of"],
                )
            )
    opportunities = enriched

    quality = validate_records(kept_records, opportunities, config["quality"], linkage_stats=linkage.stats)
    evaluation = evaluate_against_cpv(opportunities, cpv_map)
    canonical = [item for item in opportunities if item.is_canonical]
    opportunity_rows = opportunities_rows(opportunities)
    write_jsonl(gold_path / "opportunities.jsonl", (item.to_dict() for item in opportunities))
    write_csv(gold_path / "opportunities.csv", opportunity_rows, _opportunity_fields())
    write_csv(
        gold_path / "technology_summary.csv",
        technology_summary(canonical),
        ["category", "publication_month", "n_tenders", "technology_score"],
    )
    write_csv(
        gold_path / "buyer_summary.csv",
        buyer_summary(canonical),
        ["buyer", "category", "n_tenders", "technology_score"],
    )
    write_json(gold_path / "quality_report.json", quality)
    write_json(gold_path / "classifier_evaluation.json", evaluation)

    manifest = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "raw_dir": _display_path(raw_path, Path(config["_project_root"])),
        "raw_files": [_file_manifest(path, raw_path) for path in discover_raw_files(raw_path)],
        "counts": {
            "bronze": len(bronze_rows),
            "silver": len(kept_records),
            "gold": len(opportunities),
            "gold_canonical": len(canonical),
        },
        "source_counts": dict(sorted(Counter(record.source for record in kept_records).items())),
        "ingestion": ingestion_stats,
        "linkage": linkage.stats,
        "quality_passed": quality["passed"],
    }
    write_json(gold_path / "run_manifest.json", manifest)
    return {"manifest": manifest, "quality": quality, "opportunities": opportunities}


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


def _file_manifest(path: Path, root: Path) -> dict[str, Any]:
    """Return a compact checksum manifest for one raw input file."""

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"path": str(path.relative_to(root)), "bytes": path.stat().st_size, "sha256": digest}


def _display_path(path: Path, root: Path) -> str:
    """Prefer a project-relative path in manifests and fall back for external inputs."""

    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)
