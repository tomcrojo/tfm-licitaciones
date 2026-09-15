"""Command-line interface for local runs and explicit live ingestion."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from .config import configured_path, load_config
from .dir3 import build_dir3_units, fetch_dir3_units, persist_dir3_units
from .fetch import fetch_placsp, fetch_raw_batch
from .pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser used by the console script and module entrypoint."""

    parser = argparse.ArgumentParser(description="TFM DataOps pipeline de licitaciones")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="procesar los ficheros raw disponibles")
    run.add_argument("--raw-dir", type=Path, help="directorio raw alternativo")
    run.add_argument("--output-root", type=Path, help="raíz alternativa para bronze/silver/gold")
    run.add_argument("--config", type=Path, help="configuración JSON alternativa")

    ingest = subparsers.add_parser("ingest", help="descargar TED, BOE y OpenPLACSP explícitamente")
    ingest.add_argument("--start", required=True, type=date.fromisoformat)
    ingest.add_argument("--end", required=True, type=date.fromisoformat)
    ingest.add_argument("--raw-dir", type=Path)
    ingest.add_argument("--config", type=Path)
    ingest.add_argument(
        "--source",
        choices=["ted", "boe", "placsp", "dir3"],
        default="all",
        help="limitar la ingesta a una fuente (por defecto, todas las habilitadas)",
    )

    dir3 = subparsers.add_parser("dir3", help="construir la dimensión DIR3 de unidades orgánicas")
    dir3.add_argument("--raw-dir", type=Path, help="directorio raw alternativo")
    dir3.add_argument("--reference-dir", type=Path, help="directorio de referencia alternativo")
    dir3.add_argument("--config", type=Path)

    report = subparsers.add_parser("report", help="mostrar el último informe de calidad")
    report.add_argument("--gold-dir", type=Path)
    report.add_argument("--config", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Execute a CLI command and return a process-compatible exit code."""

    args = build_parser().parse_args(argv)
    config = load_config(args.config if hasattr(args, "config") else None)
    if args.command == "run":
        result = run_pipeline(
            raw_dir=args.raw_dir,
            output_root=args.output_root,
            config_path=args.config,
        )
        print(json.dumps({"counts": result["manifest"]["counts"], "quality_passed": result["quality"]["passed"]}, ensure_ascii=False))
        return 0 if result["quality"]["passed"] else 2
    if args.command == "ingest":
        if args.end < args.start:
            raise SystemExit("--end debe ser igual o posterior a --start")
        raw_dir = args.raw_dir or configured_path(config, "raw_dir")
        summary: dict[str, object] = {}
        files: list[str] = []
        for source in ("ted", "boe"):
            if args.source in {"all", source}:
                count, paths = fetch_raw_batch(source, args.start, args.end, config, raw_dir)
                summary[source] = count
                files.extend(str(path) for path in paths)
        if args.source in {"all", "placsp"}:
            placsp_paths = fetch_placsp(args.start, args.end, config, raw_dir)
            summary["placsp_files"] = len(placsp_paths)
            files.extend(str(path) for path in placsp_paths)
        if args.source in {"all", "dir3"}:
            dir3_paths = fetch_dir3_units(raw_dir, config)
            summary["dir3_files"] = len(dir3_paths)
            files.extend(str(path) for path in dir3_paths)
        summary["files"] = files
        print(json.dumps(summary, ensure_ascii=False))
        return 0
    if args.command == "dir3":
        raw_dir = args.raw_dir or configured_path(config, "raw_dir")
        if not raw_dir.is_absolute():
            raw_dir = Path(config["_project_root"]) / raw_dir
        reference_dir = args.reference_dir or configured_path(config, "reference_dir")
        if not reference_dir.is_absolute():
            reference_dir = Path(config["_project_root"]) / reference_dir
        units, build = build_dir3_units(raw_dir)
        outputs = persist_dir3_units(units, build, reference_dir)
        print(
            json.dumps(
                {
                    "row_count": build["manifest"]["row_count"],
                    "rejected": sum(source["rejected"] for source in build["manifest"]["sources"]),
                    "parents_unresolved": build["manifest"]["parents_unresolved"],
                    "units": str(outputs["units"]),
                    "manifest": str(outputs["manifest"]),
                },
                ensure_ascii=False,
            )
        )
        return 0
    gold_dir = args.gold_dir or configured_path(config, "gold_dir")
    report_path = gold_dir / "quality_report.json"
    if not report_path.exists():
        print(json.dumps({"error": f"No existe {report_path}"}, ensure_ascii=False))
        return 1
    print(report_path.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
