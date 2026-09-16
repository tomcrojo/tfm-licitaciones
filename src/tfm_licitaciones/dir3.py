"""DIR3 organisational-unit reference dimension from the official PAe XLSX distributions.

The Portal de Administración Electrónica publishes one XLSX distribution per
administration scope (AGE, CCAA, EELL, Universidades, Otras Instituciones y
Justicia). Every file keeps an empty first row and column, the real header in
the second row, and repeats the name column ``C_DNM_UD_ORGANICA`` four times,
so columns are resolved by position relative to unique anchor columns instead
of by name. See ``docs/dir3-reference.md`` for the measured per-scope layout.
"""

from __future__ import annotations

import hashlib
import logging
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import HTTPCookieProcessor, Request, build_opener

import polars as pl

from .io import write_json, write_parquet

_XLSX_MAGIC = b"PK\x03\x04"
_HEADER_ROW = 1  # zero-based index of the header row inside the first sheet

#: Unique columns present (possibly with extra scope columns in between) in
#: every official units distribution. Name columns are resolved positionally.
_ANCHOR_COLUMNS = (
    "C_ID_UD_ORGANICA",
    "C_ID_NIVEL_ADMON",
    "C_ID_TIPO_ENT_PUBLICA",
    "N_NIVEL_JERARQUICO",
    "C_ID_DEP_UD_SUPERIOR",
    "C_ID_DEP_UD_PRINCIPAL",
    "C_ID_ESTADO",
    "D_VIG_ALTA_OFICIAL",
    "NIF_CIF",
)

# Column headers allowed directly after each code column carrying its name.
_NAME_COLUMN_PREFIXES = ("C_DNM_UD_ORGANICA", "EDP.C_DNM_UD_ORGANICA")

_CODE_PATTERN = re.compile(r"^[A-Z][A-Z0-9]\d{7}$")  # 9 characters, as measured in all official distributions
_STATUS_VALUES = frozenset({"V", "E", "A", "T"})
_DATE_PATTERN = re.compile(r"^\d{2}/\d{2}/\d{2}$")  # DIR3 publishes dd/mm/yy with no official century rule
_HIERARCHY_PATTERN = re.compile(r"^\d+$")

#: Canonical typed schema of the organisational-unit dimension. Column names
#: keep the official source semantics: ``C_ID_TIPO_ENT_PUBLICA`` is the public
#: entity type, ``C_ID_DEP_UD_PRINCIPAL`` the principal (root) unit and
#: ``D_VIG_ALTA_OFICIAL`` the official validity start, preserved verbatim as a
#: two-digit-year string because DIR3 publishes no century-resolution rule.
DIR3_UNITS_SCHEMA = pl.Schema(
    {
        "dir3_code": pl.String,
        "name": pl.String,
        "administration_scope": pl.String,
        "public_entity_type": pl.String,
        "hierarchy_level": pl.Int16,
        "parent_dir3_code": pl.String,
        "principal_dir3_code": pl.String,
        "status": pl.String,
        "official_valid_from_raw": pl.String,
        "nif_cif": pl.String,
    }
)

ADMINISTRATION_SCOPES = ("AGE", "CCAA", "EELL", "UNIVERSIDADES", "OTRAS_INSTITUCIONES", "JUSTICIA")


#: Official host of the DIR3 distributions, used for provenance records.
DIR3_OFFICIAL_BASE = "https://administracionelectronica.gob.es"


@dataclass(frozen=True)
class Dir3UnitSource:
    """One official units distribution and its administration-scope metadata."""

    scope: str
    nivel_admin: str
    filename: str
    official_name: str
    url_path: str
    id_elemento: str

    @property
    def url(self) -> str:
        return f"{DIR3_OFFICIAL_BASE}{self.url_path}?idIniciativa=238&idElemento={self.id_elemento}"


#: Registry of the six organisational-unit distributions (in scope order).
DIR3_UNIT_SOURCES = tuple(
    Dir3UnitSource(scope, level, filename, official, path, elemento)
    for scope, level, filename, official, path, elemento in (
        (
            "AGE",
            "1",
            "dir3-unidades-age.xlsx",
            "Listado Unidades AGE.xlsx",
            "/ctt/resources/Soluciones/238/Descargas/Listado%20Unidades%20AGE.xlsx",
            "2741",
        ),
        (
            "CCAA",
            "2",
            "dir3-unidades-ccaa.xlsx",
            "Listado Unidades CCAA.xlsx",
            "/ctt/resources/Soluciones/238/Descargas/Listado%20Unidades%20CCAA.xlsx",
            "2742",
        ),
        (
            "EELL",
            "3",
            "dir3-unidades-eell.xlsx",
            "Listado Unidades EELL.xlsx",
            "/ctt/resources/Soluciones/238/Descargas/Listado%20Unidades%20EELL.xlsx",
            "2744",
        ),
        (
            "UNIVERSIDADES",
            "4",
            "dir3-unidades-universidades.xlsx",
            "Listado Unidades Universidades.xlsx",
            "/ctt/resources/Soluciones/238/Descargas/Listado%20Unidades%20Universidades.xlsx",
            "2808",
        ),
        (
            "OTRAS_INSTITUCIONES",
            "5",
            "dir3-unidades-otras-instituciones.xlsx",
            "Listado Unidades Institucionales.xlsx",
            "/ctt/resources/Soluciones/238/Descargas/Listado%20Unidades%20Institucionales.xlsx",
            "2810",
        ),
        (
            "JUSTICIA",
            "6",
            "dir3-unidades-justicia.xlsx",
            "Listado-unidades-organicas-Justicia.xlsx",
            "/ctt/resources/Soluciones/238/Descargas/Listado-unidades-organicas-Justicia.xlsx",
            "7326",
        ),
    )
)

DIR3_SOURCES_BY_SCOPE = {source.scope: source for source in DIR3_UNIT_SOURCES}


def _build_opener():
    """Build the cookie-aware opener used for the PAe downloads."""

    return build_opener(HTTPCookieProcessor(CookieJar()))


def _load_unit_sheet(path: Path) -> pl.DataFrame:
    """Read the first sheet of one official distribution with typed string cells."""

    try:
        frame = pl.read_excel(source=str(path), engine="calamine", read_options={"header_row": _HEADER_ROW})
    except Exception as exc:  # noqa: BLE001 - surfaced as a single clear error
        raise RuntimeError(f"No se pudo leer la hoja de unidades de {path}: {exc}") from exc
    missing = [column for column in _ANCHOR_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Faltan columnas DIR3 obligatorias en {path}: {', '.join(missing)}")
    return frame


def _is_valid_unit_file(path: Path) -> bool:
    """Check that a path holds a readable distribution with the expected header."""

    if not path.is_file() or path.stat().st_size < 1024:
        return False
    try:
        _load_unit_sheet(path)
    except (RuntimeError, ValueError):
        return False
    return True


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_source(
    source: Dir3UnitSource,
    destination: Path,
    *,
    base_url: str,
    timeout: int,
    attempts: int,
    opener,
    logger: logging.Logger,
) -> None:
    """Download one distribution to a temporary file, validate it and commit it."""

    url = base_url.rstrip("/") + source.url_path + f"?idIniciativa=238&idElemento={source.id_elemento}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, attempts + 1):
        temporary = Path(tempfile.NamedTemporaryFile(dir=destination.parent, delete=False, suffix=".part").name)
        try:
            request = Request(url, headers={"User-Agent": "tfm-licitaciones/0.2", "Accept": "*/*"})
            with opener.open(request, timeout=timeout) as response, temporary.open("wb") as output:
                while True:
                    chunk = response.read(1 << 20)
                    if not chunk:
                        break
                    output.write(chunk)
            with temporary.open("rb") as handle:
                head = handle.read(4)
            if temporary.stat().st_size < 1024 or head != _XLSX_MAGIC:
                raise ValueError("la respuesta no es un XLSX (posible challenge JavaScript de F5/TSPD)")
            try:
                _load_unit_sheet(temporary)
            except (RuntimeError, ValueError) as exc:
                raise ValueError(f"el XLSX descargado no tiene la estructura esperada: {exc}") from exc
            temporary.replace(destination)
            logger.info("DIR3 descargado %s -> %s", url, destination)
            return
        except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
            if attempt == attempts:
                raise RuntimeError(
                    f"Descarga fallida de {source.scope} ({attempts} intentos): {url}. "
                    f"Descarga manual: abre la URL en un navegador, guarda el fichero como "
                    f"'{destination}' y vuelve a ejecutar."
                ) from exc
            wait_seconds = min(2 ** (attempt - 1), 8)
            logger.warning("Intento DIR3 %d/%d fallido para %s: %s", attempt, attempts, url, exc)
            time.sleep(wait_seconds)
        finally:
            temporary.unlink(missing_ok=True)


def fetch_dir3_units(raw_dir: Path, config: dict[str, Any], logger: logging.Logger | None = None) -> list[Path]:
    """Download the six official unit distributions with deterministic names.

    Existing valid files are left untouched so the operation is idempotent, and
    the call fails if any scope is still missing after the download attempts.
    """

    source_config = config["sources"]["dir3"]
    log = logger or logging.getLogger(__name__)
    if not source_config.get("enabled", False):
        log.info("DIR3 deshabilitado en configuración, no se descarga nada")
        return []
    base_url = source_config["base_url"]
    timeout = int(source_config.get("timeout_seconds", 120))
    attempts = int(source_config.get("retry_attempts", 3))
    opener = _build_opener()
    directory = raw_dir / "dir3"
    saved: list[Path] = []
    missing: list[str] = []
    for source in DIR3_UNIT_SOURCES:
        destination = directory / source.filename
        if _is_valid_unit_file(destination):
            log.info("DIR3 %s ya presente y válido, se omite", source.scope)
            saved.append(destination)
            continue
        try:
            _download_source(source, destination, base_url=base_url, timeout=timeout, attempts=attempts, opener=opener, logger=log)
            saved.append(destination)
        except RuntimeError as exc:
            log.error("%s", exc)
            missing.append(f"{source.scope}: {exc}")
    if missing:
        raise RuntimeError(
            "Ficheros DIR3 ausentes o inválidos tras la descarga:\n- "
            + "\n- ".join(missing)
            + "\nDescarga manual: abre la URL oficial de cada ámbito en un navegador, guarda el "
            "fichero en data/raw/dir3/ con su nombre determinista y vuelve a ejecutar."
        )
    return saved


def _normalize_frame(
    frame: pl.DataFrame,
    source: Dir3UnitSource,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Validate and map one raw sheet with native expressions.

    Returns the canonical rows plus located rejections; the rejection reason
    keeps the documented precedence order. Cell text is normalized exactly
    like the previous row loop: stripped strings, blank cells as null.
    """

    columns = frame.columns
    code_index = columns.index("C_ID_UD_ORGANICA")
    name_column = columns[code_index + 1]
    if not any(name_column.startswith(prefix) for prefix in _NAME_COLUMN_PREFIXES):
        raise ValueError(
            f"Se esperaba una columna de denominación tras C_ID_UD_ORGANICA en {source.official_name}, "
            f"encontrado: {name_column!r}"
        )

    def _text(column: str) -> pl.Expr:
        return pl.col(column).cast(pl.String).str.strip_chars()

    code = _text("C_ID_UD_ORGANICA")
    name = _text(name_column)
    nivel = _text("C_ID_NIVEL_ADMON")
    hierarchy = _text("N_NIVEL_JERARQUICO")
    parent = _text("C_ID_DEP_UD_SUPERIOR")
    principal = _text("C_ID_DEP_UD_PRINCIPAL")
    status = _text("C_ID_ESTADO")
    valid_from = _text("D_VIG_ALTA_OFICIAL")
    nif = _text("NIF_CIF")
    empty = lambda expr: expr.is_null() | (expr == "")  # noqa: E731 - readability

    valid_from_shape = valid_from.str.contains(_DATE_PATTERN.pattern).fill_null(True)
    reason = (
        pl.when(empty(code)).then(pl.lit("missing_dir3_code"))
        .when(~code.str.contains(_CODE_PATTERN.pattern).fill_null(False)).then(pl.lit("invalid_dir3_code_format"))
        .when(empty(name)).then(pl.lit("missing_name"))
        .when(empty(status)).then(pl.lit("missing_status"))
        .when(~status.is_in(_STATUS_VALUES)).then(pl.lit("invalid_status"))
        .when(nivel.is_null() | (nivel != pl.lit(source.nivel_admin))).then(pl.lit("nivel_admin_mismatch"))
        .when(empty(hierarchy)).then(pl.lit("missing_hierarchy_level"))
        .when(~hierarchy.str.contains(_HIERARCHY_PATTERN.pattern).fill_null(False)).then(pl.lit("invalid_hierarchy_level"))
        .when(empty(parent)).then(pl.lit("missing_parent_dir3_code"))
        .when(~parent.str.contains(_CODE_PATTERN.pattern).fill_null(False)).then(pl.lit("invalid_parent_dir3_code"))
        .when(empty(principal)).then(pl.lit("missing_principal_dir3_code"))
        .when(~principal.str.contains(_CODE_PATTERN.pattern).fill_null(False)).then(pl.lit("invalid_principal_dir3_code"))
        .when(valid_from.is_not_null() & ~valid_from_shape).then(pl.lit("invalid_valid_from_date"))
        .otherwise(pl.lit(None, dtype=pl.String))
        .alias("rejection_reason")
    )

    typed = frame.with_columns(reason)
    accepted = (
        typed.filter(pl.col("rejection_reason").is_null())
        .select(
            dir3_code=code,
            name=name,
            administration_scope=pl.lit(source.scope),
            public_entity_type=_text("C_ID_TIPO_ENT_PUBLICA"),
            hierarchy_level=hierarchy.cast(pl.Int16, strict=False),
            parent_dir3_code=parent,
            principal_dir3_code=principal,
            status=status,
            official_valid_from_raw=valid_from,
            nif_cif=pl.when(empty(nif)).then(pl.lit(None, dtype=pl.String)).otherwise(nif.str.to_uppercase()),
        )
        .cast(DIR3_UNITS_SCHEMA)
    )
    rejected = typed.filter(pl.col("rejection_reason").is_not_null()).select(
        dir3_code=code.fill_null(""),
        rejection_reason=pl.col("rejection_reason"),
    )
    return accepted, rejected


def normalize_dir3_units(path: Path, scope: str) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    """Normalize one official distribution into canonical rows plus rejections."""

    source = DIR3_SOURCES_BY_SCOPE.get(scope)
    if source is None:
        raise ValueError(f"Ámbito DIR3 desconocido: {scope!r}")
    frame = _load_unit_sheet(path)
    accepted, rejected = _normalize_frame(frame, source)
    return accepted, rejected.to_dicts()


def build_dir3_units(raw_dir: Path) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Build the whole dimension from the six local distributions.

    Returns the deterministically ordered frame and a provenance manifest with
    per-source checksums, row accounting and hierarchy-resolution metrics.
    """

    directory = raw_dir / "dir3"
    accepted_frames: list[pl.DataFrame] = []
    rejections: list[dict[str, Any]] = []
    sources_manifest: list[dict[str, Any]] = []
    for source in DIR3_UNIT_SOURCES:
        path = directory / source.filename
        frame, rejected = normalize_dir3_units(path, source.scope)
        accepted_frames.append(frame)
        rejections.extend({**row, "administration_scope": source.scope} for row in rejected)
        reasons: dict[str, int] = {}
        for row in rejected:
            reasons[row["rejection_reason"]] = reasons.get(row["rejection_reason"], 0) + 1
        sources_manifest.append(
            {
                "scope": source.scope,
                "official_name": source.official_name,
                "url": source.url,
                "filename": source.filename,
                "sha256": _sha256(path),
                "parsed": frame.height + len(rejected),
                "accepted": frame.height,
                "rejected": len(rejected),
                "rejection_reasons": reasons,
            }
        )
    units = (
        pl.concat(accepted_frames).sort("dir3_code").cast(DIR3_UNITS_SCHEMA)
        if accepted_frames
        else pl.DataFrame(schema=DIR3_UNITS_SCHEMA)
    )
    duplicates = units.group_by("dir3_code").len().filter(pl.col("len") > 1)
    if duplicates.height:
        codes = duplicates.get_column("dir3_code").sort().to_list()[:10]
        raise ValueError(f"Códigos DIR3 duplicados entre ámbitos ({duplicates.height}): {', '.join(codes)}")
    known_codes = set(units.get_column("dir3_code"))
    parents = units.get_column("parent_dir3_code")
    parents_unresolved = parents.filter(~parents.is_in(pl.Series(sorted(known_codes)))).n_unique()
    manifest = {
        "dimension": "dir3_units",
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),  # transformation time, not retrieval time
        "row_count": units.height,
        "unique_dir3_codes": len(known_codes),
        "parents_unresolved": parents_unresolved,
        "status_counts": units.get_column("status").value_counts().sort("status").to_dicts(),
        "administration_scope_counts": units.get_column("administration_scope")
        .value_counts()
        .sort("administration_scope")
        .to_dicts(),
        "sources": sources_manifest,
    }
    return units, {"manifest": manifest, "rejections": rejections}


def persist_dir3_units(
    units: pl.DataFrame,
    build: dict[str, Any],
    reference_dir: Path,
) -> dict[str, Path]:
    """Persist the dimension Parquet and its provenance manifest."""

    output_dir = reference_dir / "dir3"
    outputs = {
        "units": output_dir / "dir3_units.parquet",
        "manifest": output_dir / "build_manifest.json",
    }
    write_parquet(outputs["units"], units)
    write_json(outputs["manifest"], build["manifest"])
    return outputs
