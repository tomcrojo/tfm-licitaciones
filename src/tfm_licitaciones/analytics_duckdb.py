"""DuckDB analytical layer over the Gold principal dataset.

Boundary::

    Gold open_opportunities Parquet
        -> DuckDB views (this module; SQL versioned under ``sql/duckdb/``)
        -> future Tableau exports

Gold remains the source of truth. This layer only *reads* Gold Parquet and
the existing CPV reference dimension through views: it never redefines
current-state or open/actionable semantics, never touches Silver, never
invents identity and never duplicates Spark business logic. Rebuilding from
the same Gold produces the same semantics because every object in the
``.duckdb`` file is a deterministic view.

The build is a full rebuild: an existing database file is removed so stale
views cannot survive. Portability limitation: DuckDB persists view
definitions with the literal paths resolved at build time, so moving the
repository (or the Gold output) requires rerunning ``build-analytics``; the
command is the documented reproducible rebuild (see ``docs/analytics.md``).
"""

from __future__ import annotations

import duckdb
from pathlib import Path
from string import Template

from .config import project_root
from .gold_contract import GOLD_OPEN_OPPORTUNITIES_FIELDS


ANALYTICS_DUCKDB_FILENAME = "licitaciones.duckdb"
GOLD_OPEN_OPPORTUNITIES_DIRNAME = "open_opportunities"
CPV_CODES_FILENAME = "cpv_codes.parquet"

SQL_DIR = project_root() / "sql" / "duckdb"

# Ordered SQL steps. Step 000 has two variants: the real dimension view or a
# typed empty stub (documented fallback policy, see 000_dim_cpv_missing.sql).
_DIM_CPV_SQL = "000_dim_cpv.sql"
_DIM_CPV_MISSING_SQL = "000_dim_cpv_missing.sql"

ANALYTICS_VIEWS = (
    "dim_cpv",
    "open_opportunities",
    "opportunity_cpv",
    "buyer_summary",
    "cpv_summary",
    "opportunities_monthly",
    "opportunities_without_publication_date",
)

# Engine-neutral logical types (gold_contract.FieldSpec) mapped to the
# DuckDB types read_parquet exposes for the Spark-written Gold Parquet.
# Parquet timestamps adjusted to UTC surface as TIMESTAMP WITH TIME ZONE
# while non-adjusted ones surface as TIMESTAMP; both preserve the Gold
# instant semantics, so the guard accepts either.
_TIMESTAMP_TYPES = ("TIMESTAMP", "TIMESTAMP WITH TIME ZONE")

_LOGICAL_TO_DUCKDB_TYPES = {
    "string": ("VARCHAR",),
    "array<string>": ("VARCHAR[]",),
    "decimal(20,2)": ("DECIMAL(20,2)",),
    "date": ("DATE",),
    "timestamp": _TIMESTAMP_TYPES,
}


def gold_open_opportunities_glob(gold_dir: str | Path) -> str:
    """Return the Parquet glob of the Gold principal dataset."""

    return str(Path(gold_dir) / GOLD_OPEN_OPPORTUNITIES_DIRNAME / "*.parquet")


def _sql_literal(value: str) -> str:
    """Quote a path for embedding in SQL (single quotes escaped)."""

    return "'" + value.replace("'", "''") + "'"


def _render(sql_template: str, *, gold_glob: str, cpv_dimension: str | None) -> str:
    return Template(sql_template).substitute(
        gold_glob=_sql_literal(gold_glob),
        cpv_dimension=_sql_literal(cpv_dimension or ""),
    )


def _resolve_gold_parquet_files(gold_dir: str | Path) -> list[Path]:
    dataset_dir = Path(gold_dir) / GOLD_OPEN_OPPORTUNITIES_DIRNAME
    files = sorted(dataset_dir.glob("*.parquet"))
    if not files:
        raise ValueError(
            "Gold open_opportunities not found: no *.parquet under "
            f"{dataset_dir}. Build Gold first with "
            "`licitaciones-pipeline build-gold`."
        )
    return files


def _remove_database_files(analytics_path: Path) -> None:
    """Remove the DuckDB file and its write-ahead log if present."""

    analytics_path.unlink(missing_ok=True)
    analytics_path.with_name(analytics_path.name + ".wal").unlink(missing_ok=True)


def _build_dim_cpv(
    connection: "duckdb.DuckDBPyConnection", reference_dir: str | Path | None
) -> dict:
    """Create the CPV dimension view and validate its key when it exists."""

    dimension_path = (
        Path(reference_dir) / CPV_CODES_FILENAME
        if reference_dir is not None
        else None
    )
    if dimension_path is None or not dimension_path.is_file():
        sql_file = SQL_DIR / _DIM_CPV_MISSING_SQL
        info = {"available": False, "path": None}
    else:
        sql_file = SQL_DIR / _DIM_CPV_SQL
        info = {"available": True, "path": str(dimension_path)}
    sql = _render(
        sql_file.read_text(encoding="utf-8"),
        gold_glob="",
        cpv_dimension=str(dimension_path) if dimension_path else None,
    )
    try:
        connection.execute(sql)
    except duckdb.Error as exc:
        raise ValueError(
            f"CPV dimension at {dimension_path} cannot be read as the "
            f"documented contract (cpv_code, label_es, label_en, level, "
            f"parent_code, is_leaf): {exc}"
        ) from exc
    if info["available"]:
        null_keys, duplicates = connection.execute(
            """
            SELECT
                count(*) FILTER (WHERE cpv_code IS NULL),
                count(*) - count(DISTINCT cpv_code)
            FROM dim_cpv
            """
        ).fetchone()
        if null_keys or duplicates:
            raise ValueError(
                f"CPV dimension at {dimension_path} is not a valid join key: "
                f"null_keys={null_keys} duplicate_keys={duplicates}"
            )
        info["dimension_rows"] = connection.execute(
            "SELECT count(*) FROM dim_cpv"
        ).fetchone()[0]
    return info


def _assert_base_view_contract(connection: "duckdb.DuckDBPyConnection") -> None:
    """Validate base view column names, order and types against Gold."""

    described = connection.execute("DESCRIBE SELECT * FROM open_opportunities").fetchall()
    actual = [(row[0], row[1]) for row in described]
    expected_names = [field.name for field in GOLD_OPEN_OPPORTUNITIES_FIELDS]
    if [name for name, _ in actual] != expected_names:
        raise ValueError(
            "open_opportunities view columns drifted from the Gold contract: "
            f"actual={[name for name, _ in actual]} expected={expected_names}"
        )
    for (name, duckdb_type), field in zip(actual, GOLD_OPEN_OPPORTUNITIES_FIELDS):
        accepted = _LOGICAL_TO_DUCKDB_TYPES[field.logical_type]
        if duckdb_type not in accepted:
            raise ValueError(
                f"open_opportunities.{name} has DuckDB type {duckdb_type}; "
                f"the Gold contract expects one of {list(accepted)}"
            )


def _assert_reconciliations(
    connection: "duckdb.DuckDBPyConnection",
    gold_glob: str,
) -> dict:
    """Run Gold <-> DuckDB consistency guards and return view counts."""

    def scalar(query: str) -> int:
        return connection.execute(query).fetchone()[0]

    gold_rows = scalar(f"SELECT count(*) FROM read_parquet({_sql_literal(gold_glob)})")
    base_rows = scalar("SELECT count(*) FROM open_opportunities")
    if base_rows != gold_rows:
        raise ValueError(
            f"open_opportunities view has {base_rows} rows but Gold Parquet "
            f"has {gold_rows}"
        )

    null_procedures = scalar(
        "SELECT count(*) FROM open_opportunities WHERE procedure_id IS NULL"
    )
    duplicate_procedures = scalar(
        "SELECT count(*) - count(DISTINCT procedure_id) FROM open_opportunities"
    )
    if null_procedures or duplicate_procedures:
        raise ValueError(
            "procedure_id is not a valid base grain: "
            f"null={null_procedures} duplicates={duplicate_procedures}"
        )

    cpv_occurrences = scalar(
        "SELECT coalesce(sum(len(cpv_codes)), 0) FROM open_opportunities"
    )
    cpv_rows = scalar("SELECT count(*) FROM opportunity_cpv")
    if cpv_rows != cpv_occurrences:
        raise ValueError(
            f"opportunity_cpv has {cpv_rows} rows but Gold publishes "
            f"{cpv_occurrences} CPV occurrences"
        )

    buyer_total = scalar("SELECT coalesce(sum(opportunities_count), 0) FROM buyer_summary")
    if buyer_total != gold_rows:
        raise ValueError(
            f"buyer_summary accounts for {buyer_total} opportunities but "
            f"Gold has {gold_rows}"
        )

    dated_rows = scalar(
        "SELECT count(*) FROM open_opportunities WHERE publication_date IS NOT NULL"
    )
    monthly_total = scalar(
        "SELECT coalesce(sum(opportunities_count), 0) FROM opportunities_monthly"
    )
    undated_total = scalar(
        "SELECT coalesce(sum(opportunities_count), 0) "
        "FROM opportunities_without_publication_date"
    )
    if monthly_total + undated_total != gold_rows or monthly_total != dated_rows:
        raise ValueError(
            "monthly views do not reconcile with Gold: "
            f"monthly={monthly_total} undated={undated_total} "
            f"dated_gold={dated_rows} gold={gold_rows}"
        )

    return {
        "open_opportunities": base_rows,
        "opportunity_cpv": cpv_rows,
        "cpv_matched": scalar(
            "SELECT count(*) FROM opportunity_cpv WHERE cpv_matched"
        ),
        "cpv_unmatched": scalar(
            "SELECT count(*) FROM opportunity_cpv WHERE NOT cpv_matched"
        ),
        "buyer_summary_rows": scalar("SELECT count(*) FROM buyer_summary"),
        "cpv_summary_rows": scalar("SELECT count(*) FROM cpv_summary"),
        "opportunities_monthly_rows": scalar(
            "SELECT count(*) FROM opportunities_monthly"
        ),
        "opportunities_without_publication_date": undated_total,
    }


def build_analytics_duckdb(
    gold_dir: str | Path,
    analytics_dir: str | Path,
    *,
    reference_dir: str | Path | None = None,
    duckdb_filename: str = ANALYTICS_DUCKDB_FILENAME,
) -> dict:
    """Build the reproducible DuckDB analytical file from Gold Parquet.

    Validates that the Gold ``open_opportunities`` dataset exists, recreates
    ``<analytics_dir>/<duckdb_filename>`` from scratch and creates the
    versioned views in ``sql/duckdb/``. Fails loudly on schema drift,
    broken dimensions or any Gold <-> DuckDB reconciliation mismatch.
    """

    gold_glob = gold_open_opportunities_glob(gold_dir)
    _resolve_gold_parquet_files(gold_dir)

    analytics_path = Path(analytics_dir) / duckdb_filename
    analytics_path.parent.mkdir(parents=True, exist_ok=True)
    _remove_database_files(analytics_path)

    connection = duckdb.connect(str(analytics_path))
    try:
        cpv_info = _build_dim_cpv(connection, reference_dir)
        for sql_file in sorted(SQL_DIR.glob("[0-9][0-9][0-9]_*.sql")):
            if sql_file.name in {_DIM_CPV_SQL, _DIM_CPV_MISSING_SQL}:
                continue
            sql = _render(
                sql_file.read_text(encoding="utf-8"),
                gold_glob=gold_glob,
                cpv_dimension=None,
            )
            connection.execute(sql)
        _assert_base_view_contract(connection)
        counts = _assert_reconciliations(connection, gold_glob)
    except duckdb.Error as exc:
        connection.close()
        # A failed build must not leave an unverified artifact behind.
        _remove_database_files(analytics_path)
        raise ValueError(
            f"Gold Parquet at {gold_glob} cannot satisfy the analytical "
            f"views (schema drift or unreadable dataset): {exc}"
        ) from exc
    except Exception:
        connection.close()
        # A failed build must not leave an unverified artifact behind.
        _remove_database_files(analytics_path)
        raise
    else:
        connection.close()

    return {
        "duckdb_path": str(analytics_path),
        "duckdb_version": duckdb.__version__,
        "views": list(ANALYTICS_VIEWS),
        "gold_glob": gold_glob,
        "cpv_dimension": cpv_info,
        "counts": counts,
    }
