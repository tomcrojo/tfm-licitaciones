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
        help="limitar la ingesta a una fuente (por defecto, todas las habilitadas; "
        "los datos de referencia DIR3 requieren --source dir3 explícito)",
    )

    dir3 = subparsers.add_parser("dir3", help="construir la dimensión DIR3 de unidades orgánicas")
    dir3.add_argument("--raw-dir", type=Path, help="directorio raw alternativo")
    dir3.add_argument("--reference-dir", type=Path, help="directorio de referencia alternativo")
    dir3.add_argument("--config", type=Path)

    report = subparsers.add_parser("report", help="mostrar el último informe de calidad")
    report.add_argument("--gold-dir", type=Path)
    report.add_argument("--config", type=Path)

    build_gold = subparsers.add_parser(
        "build-gold",
        help="construir el Gold principal open_opportunities desde Silver canónico",
    )
    build_gold.add_argument("--silver-dir", type=Path, help="directorio Silver alternativo")
    build_gold.add_argument("--gold-dir", type=Path, help="directorio Gold alternativo")
    build_gold.add_argument("--reference-dir", type=Path, help="directorio de referencia alternativo")
    build_gold.add_argument("--config", type=Path, help="configuración JSON alternativa")
    build_gold.add_argument(
        "--as-of",
        default=None,
        help="fecha de evaluación YYYY-MM-DD para la política deadline (UTC)",
    )

    build_analytics = subparsers.add_parser(
        "build-analytics",
        help="construir la capa analítica DuckDB desde el Gold open_opportunities",
    )
    build_analytics.add_argument("--gold-dir", type=Path, help="directorio Gold alternativo")
    build_analytics.add_argument("--analytics-dir", type=Path, help="directorio analítico alternativo")
    build_analytics.add_argument("--reference-dir", type=Path, help="directorio de referencia alternativo")
    build_analytics.add_argument("--config", type=Path, help="configuración JSON alternativa")

    export_tableau = subparsers.add_parser(
        "export-tableau",
        help="exportar CSV deterministas para Tableau desde las views DuckDB",
    )
    export_tableau.add_argument("--analytics-dir", type=Path, help="directorio analítico alternativo")
    export_tableau.add_argument("--exports-dir", type=Path, help="directorio de exportación alternativo")
    export_tableau.add_argument("--config", type=Path, help="configuración JSON alternativa")
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
        if args.source == "dir3":
            # Reference data with an independent failure mode (F5/TSPD): only
            # fetched when explicitly requested, never part of --source all.
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
    if args.command == "build-gold":
        from .gold_open_opportunities import build_gold_from_silver, parse_as_of
        from .silver import PROCUREMENT_EVENTS_FILENAME
        from .spark_foundation import create_spark_session

        try:
            moment = parse_as_of(args.as_of)
        except ValueError as exc:
            raise SystemExit(f"--as-of inválido: {exc}") from exc
        silver_dir = args.silver_dir or configured_path(config, "silver_dir")
        if not silver_dir.is_absolute():
            silver_dir = Path(config["_project_root"]) / silver_dir
        gold_dir = args.gold_dir or configured_path(config, "gold_dir")
        if not gold_dir.is_absolute():
            gold_dir = Path(config["_project_root"]) / gold_dir
        reference_dir = args.reference_dir or configured_path(config, "reference_dir")
        if not reference_dir.is_absolute():
            reference_dir = Path(config["_project_root"]) / reference_dir
        spark = create_spark_session(app_name="tfm-licitaciones-build-gold")
        try:
            result = build_gold_from_silver(
                spark,
                silver_dir / PROCUREMENT_EVENTS_FILENAME,
                gold_dir,
                reference_dir=reference_dir,
                as_of=moment,
            )
        finally:
            spark.stop()
        print(
            json.dumps(
                {
                    "open_opportunities": result["open_opportunities"],
                    "manifest": result["manifest"],
                    "current_state_rows": result["current_state_rows"],
                    "current_state_issues_rows": result["current_state_issues_rows"],
                    "open_opportunities_rows": result["open_opportunities_rows"],
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.command == "build-analytics":
        from .analytics_duckdb import build_analytics_duckdb

        def _resolve_dir(explicit: Path | None, key: str) -> Path:
            resolved = explicit or configured_path(config, key)
            if not resolved.is_absolute():
                resolved = Path(config["_project_root"]) / resolved
            return resolved

        try:
            result = build_analytics_duckdb(
                _resolve_dir(args.gold_dir, "gold_dir"),
                _resolve_dir(args.analytics_dir, "analytics_dir"),
                reference_dir=_resolve_dir(args.reference_dir, "reference_dir"),
            )
        except ValueError as exc:
            raise SystemExit(f"build-analytics falló: {exc}") from exc
        print(
            json.dumps(
                {
                    "analytics_duckdb": result["duckdb_path"],
                    "duckdb_version": result["duckdb_version"],
                    "views": result["views"],
                    "cpv_dimension_available": result["cpv_dimension"]["available"],
                    "counts": result["counts"],
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.command == "export-tableau":
        from .tableau_export import export_tableau_csvs

        def _resolve_dir(explicit: Path | None, key: str) -> Path:
            resolved = explicit or configured_path(config, key)
            if not resolved.is_absolute():
                resolved = Path(config["_project_root"]) / resolved
            return resolved

        try:
            result = export_tableau_csvs(
                _resolve_dir(args.analytics_dir, "analytics_dir"),
                _resolve_dir(args.exports_dir, "exports_dir"),
            )
        except ValueError as exc:
            raise SystemExit(f"export-tableau falló: {exc}") from exc
        print(
            json.dumps(
                {
                    "export_dir": result["export_dir"],
                    "files": result["files"],
                    "counts": result["counts"],
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
