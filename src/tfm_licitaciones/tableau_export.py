"""Deterministic Tableau CSV exports from the existing DuckDB analytical views.

Boundary::

    DuckDB analytical views
        -> deterministic CSV files
        -> Tableau/manual consumption

DuckDB is the only business-data source for this module. The exporter does not
read Gold/Silver, repeat analytical joins or aggregations, or redefine any
business semantics. ``open_opportunities.cpv_codes`` is serialized as compact
JSON text so the base one-row-per-procedure grain is preserved; the relational
CPV grain remains available in ``opportunity_cpv.csv``.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb

from .analytics_duckdb import ANALYTICS_DUCKDB_FILENAME


TABLEAU_EXPORT_SUBDIR = "tableau"
TABLEAU_EXPORT_MANIFEST_FILENAME = "tableau_export_manifest.json"
TABLEAU_EXPORT_FORMAT_VERSION = 1

# Each export is sourced only from the identically named analytical view. The
# ORDER BY keys freeze a stable Tableau-facing row order; they do not alter
# business semantics.
_TABLEAU_EXPORTS = (
    (
        "open_opportunities",
        "open_opportunities.csv",
        "procedure_id",
        "* REPLACE (CAST(to_json(cpv_codes) AS VARCHAR) AS cpv_codes)",
    ),
    (
        "opportunity_cpv",
        "opportunity_cpv.csv",
        "procedure_id, cpv_position, cpv_code",
        "*",
    ),
    (
        "buyer_summary",
        "buyer_summary.csv",
        "identity_basis, buyer_id, buyer_name",
        "*",
    ),
    ("cpv_summary", "cpv_summary.csv", "cpv_code", "*"),
    (
        "opportunities_monthly",
        "opportunities_monthly.csv",
        "month, source",
        "*",
    ),
)

REQUIRED_TABLEAU_VIEWS = tuple(item[0] for item in _TABLEAU_EXPORTS)

_DATE_FORMAT = "%Y-%m-%d"
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _view_columns(connection: "duckdb.DuckDBPyConnection", view: str) -> list[str]:
    return [
        row[0]
        for row in connection.execute(f"DESCRIBE SELECT * FROM {view}").fetchall()
    ]


def _csv_header_and_count(path: Path) -> tuple[list[str], int]:
    """Validate UTF-8/CSV structure and return header plus logical row count."""

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return [], 0
        return header, sum(1 for _ in reader)


def _copy_view_to_csv(
    connection: "duckdb.DuckDBPyConnection",
    *,
    view: str,
    select_list: str,
    order_by: str,
    output_path: Path,
) -> None:
    output_path.unlink(missing_ok=True)
    query = f"SELECT {select_list} FROM {view} ORDER BY {order_by}"
    connection.execute(
        f"""
        COPY ({query}) TO {_sql_literal(str(output_path))} (
            FORMAT CSV,
            HEADER TRUE,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"',
            NULLSTR '',
            DATEFORMAT '{_DATE_FORMAT}',
            TIMESTAMPFORMAT '{_TIMESTAMP_FORMAT}'
        )
        """
    )


def export_tableau_csvs(
    analytics_dir: str | Path,
    exports_dir: str | Path,
    *,
    duckdb_filename: str = ANALYTICS_DUCKDB_FILENAME,
) -> dict:
    """Export deterministic Tableau-ready CSVs from an existing DuckDB file.

    The function fails before opening DuckDB when the expected database file
    does not exist (avoiding DuckDB's create-on-connect behavior), verifies all
    required analytical views, stages and validates every CSV, then atomically
    replaces the published files and writes a minimal deterministic manifest.
    """

    duckdb_path = Path(analytics_dir) / duckdb_filename
    if not duckdb_path.is_file():
        raise ValueError(
            f"DuckDB analytics database not found: {duckdb_path}. "
            "Build it first with `licitaciones-pipeline build-analytics`."
        )

    export_dir = Path(exports_dir) / TABLEAU_EXPORT_SUBDIR
    staged: list[tuple[Path, Path]] = []
    files: dict[str, str] = {}
    counts: dict[str, int] = {}
    manifest_entries: dict[str, dict[str, object]] = {}
    manifest_path = export_dir / TABLEAU_EXPORT_MANIFEST_FILENAME
    manifest_tmp = export_dir / f".{TABLEAU_EXPORT_MANIFEST_FILENAME}.tmp"

    try:
        connection = duckdb.connect(str(duckdb_path), read_only=True)
    except duckdb.Error as exc:
        raise ValueError(f"Cannot open DuckDB analytics database {duckdb_path}: {exc}") from exc

    try:
        existing_views = {
            row[0]
            for row in connection.execute(
                "SELECT view_name FROM duckdb_views() WHERE schema_name = 'main'"
            ).fetchall()
        }
        missing = [view for view in REQUIRED_TABLEAU_VIEWS if view not in existing_views]
        if missing:
            raise ValueError(
                f"DuckDB analytics database {duckdb_path} is missing required "
                f"Tableau view(s): {', '.join(missing)}. Rebuild it with "
                "`licitaciones-pipeline build-analytics`."
            )

        export_dir.mkdir(parents=True, exist_ok=True)
        connection.execute("SET TimeZone='UTC'")

        for view, filename, order_by, select_list in _TABLEAU_EXPORTS:
            expected_header = _view_columns(connection, view)
            view_count = connection.execute(f"SELECT count(*) FROM {view}").fetchone()[0]
            target = export_dir / filename
            staged_path = export_dir / f".{filename}.tmp"
            staged.append((staged_path, target))
            _copy_view_to_csv(
                connection,
                view=view,
                select_list=select_list,
                order_by=order_by,
                output_path=staged_path,
            )
            actual_header, csv_count = _csv_header_and_count(staged_path)
            if actual_header != expected_header:
                raise ValueError(
                    f"Tableau export {filename} has unexpected headers: "
                    f"actual={actual_header} expected={expected_header}"
                )
            if csv_count != view_count:
                raise ValueError(
                    f"Tableau export {filename} has {csv_count} rows but DuckDB "
                    f"view {view} has {view_count}"
                )
            if view_count and csv_count == 0:
                raise ValueError(
                    f"Tableau export {filename} is empty although view {view} has rows"
                )

            files[view] = str(target)
            counts[view] = view_count
            manifest_entries[filename] = {
                "view": view,
                "row_count": view_count,
            }

        manifest = {
            "format_version": TABLEAU_EXPORT_FORMAT_VERSION,
            "source_duckdb": str(duckdb_path.resolve()),
            "files": manifest_entries,
        }
        manifest_tmp.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for staged_path, target in staged:
            staged_path.replace(target)
        manifest_tmp.replace(manifest_path)
    except duckdb.Error as exc:
        raise ValueError(f"Tableau export from {duckdb_path} failed: {exc}") from exc
    finally:
        connection.close()
        for staged_path, _ in staged:
            staged_path.unlink(missing_ok=True)
        manifest_tmp.unlink(missing_ok=True)

    return {
        "export_dir": str(export_dir),
        "files": files,
        "counts": counts,
        "manifest": str(manifest_path),
    }
