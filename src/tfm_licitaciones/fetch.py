"""Optional live adapters for the official TED, BOE and OpenPLACSP endpoints."""

from __future__ import annotations

import json
import logging
import ssl
import tempfile
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .io import write_jsonl
from .raw_provenance import (
    RawProvenanceError,
    load_raw_artifact,
    persist_raw_artifact,
    provenance_path,
)


def build_ted_query(country: str, technology_terms: list[str], start: date, end: date) -> str:
    """Build the TED eForms expert query for one inclusive date window."""

    clauses = [f'FT~"{term}"' for term in technology_terms if term.strip()]
    text_clause = f" AND ({' OR '.join(clauses)})" if clauses else ""
    return f"CY={country}{text_clause} AND PD>={start:%Y%m%d} AND PD<={end:%Y%m%d}"


def split_window(start: date, end: date, max_days: int) -> list[tuple[date, date]]:
    """Split an inclusive date window into bounded, contiguous requests."""

    if end < start:
        raise ValueError("end must not be earlier than start")
    if max_days < 1:
        raise ValueError("max_days must be positive")
    chunks: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=max_days - 1))
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def _request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: int = 30,
    attempts: int = 1,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Request JSON with bounded retries and no third-party dependency."""

    log = logger or logging.getLogger(__name__)
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Accept": "application/json", "User-Agent": "tfm-licitaciones/0.1"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    for attempt in range(1, attempts + 1):
        try:
            request = Request(url, data=body, headers=headers, method=method)
            with urlopen(request, timeout=timeout) as response:
                parsed = json.loads(response.read().decode("utf-8"))
            if not isinstance(parsed, dict):
                raise ValueError(f"Expected JSON object from {url}")
            return parsed
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            if attempt == attempts:
                raise RuntimeError(f"Request failed after {attempts} attempt(s): {url}") from exc
            wait_seconds = min(2 ** (attempt - 1), 8)
            log.warning("Request attempt %d/%d failed for %s: %s", attempt, attempts, url, exc)
            time.sleep(wait_seconds)
    raise AssertionError("unreachable")


def fetch_ted(start: date, end: date, config: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch TED notices page by page, retaining raw source payloads."""

    source = config["sources"]["ted"]
    if not source.get("enabled", False):
        return []
    throttle_seconds = float(source.get("sleep_seconds", 0.5))
    notices: list[dict[str, Any]] = []
    for chunk_start, chunk_end in split_window(start, end, source["max_days_per_request"]):
        query = build_ted_query(source["country"], source["technology_terms"], chunk_start, chunk_end)
        for page in range(1, source["max_pages"] + 1):
            if page > 1:
                time.sleep(throttle_seconds)
            response = _request_json(
                source["base_url"],
                method="POST",
                payload={"query": query, "fields": source["fields"], "page": page, "limit": source["page_size"]},
                timeout=source["timeout_seconds"],
                attempts=source["retry_attempts"],
            )
            page_rows = response.get("notices", []) or []
            for row in page_rows:
                record = dict(row)
                record.update({"_source": "ted", "_window_start": start.isoformat(), "_window_end": end.isoformat()})
                notices.append(record)
            if len(page_rows) < source["page_size"]:
                break
    return notices


def _as_list(value: Any) -> list[Any]:
    """Normalize a BOE node that may be a dict, list or null."""

    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def flatten_boe_sumario(payload: dict[str, Any], day: date) -> list[dict[str, Any]]:
    """Flatten the official BOE sumario nesting into item-level records."""

    items: list[dict[str, Any]] = []
    sumario = (payload.get("data") or {}).get("sumario") or {}
    for diario in _as_list(sumario.get("diario")):
        for section in _as_list(diario.get("seccion")):
            section_name = section.get("nombre") or section.get("titulo") or ""
            for department in _as_list(section.get("departamento")):
                department_name = department.get("nombre") or ""
                for epigraph in _as_list(department.get("epigrafe")):
                    for item in _as_list(epigraph.get("item")):
                        raw_url = item.get("url_pdf")
                        url = raw_url.get("texto") if isinstance(raw_url, dict) else raw_url
                        items.append(
                            {
                                "_source": "boe",
                                "publication_date": day.isoformat(),
                                "section": section_name,
                                "department": department_name,
                                "item_id": item.get("identificador") or item.get("id"),
                                "title": item.get("titulo") or "",
                                "url": url or "",
                            }
                        )
    return items


def fetch_boe(start: date, end: date, config: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch and flatten the BOE daily sumario for an inclusive window."""

    source = config["sources"]["boe"]
    if not source.get("enabled", False):
        return []
    items: list[dict[str, Any]] = []
    current = start
    while current <= end:
        url = f"{source['base_url']}/{current:%Y%m%d}"
        try:
            payload = _request_json(url, timeout=source["timeout_seconds"], attempts=source["retry_attempts"])
        except RuntimeError:
            current += timedelta(days=1)
            continue
        day_items = flatten_boe_sumario(payload, current)
        sections = set(source.get("sections", []))
        items.extend(item for item in day_items if not sections or item["section"] in sections)
        current += timedelta(days=1)
    return items


def fetch_placsp(start: date, end: date, config: dict[str, Any], raw_dir: Path) -> list[Path]:
    """Download the OpenPLACSP monthly bulk zips for the inclusive window.

    Hacienda serves the syndication over a FNMT-RCM chain that is not in the
    Python trust store, so the adapter pins the published root certificate
    from ``ca_bundle``. Each zip is validated before publishing its immutable
    payload and retrieval sidecar. Cached files must have matching evidence.
    """

    source = config["sources"]["placsp"]
    if not source.get("enabled", False):
        return []
    base_url = source["base_url"].rstrip("/")
    ca_bundle = Path(source.get("ca_bundle", "config/certs/fnmt-root-servidores-seguros.pem"))
    if not ca_bundle.is_absolute():
        ca_bundle = Path(config["_project_root"]) / ca_bundle
    context = ssl.create_default_context()
    context.load_verify_locations(cafile=str(ca_bundle))
    log = logging.getLogger(__name__)
    saved: list[Path] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        run_id = f"{cursor:%Y%m}"
        url = f"{base_url}/{source['zip_pattern'].format(run_id=run_id)}"
        destination = raw_dir / "placsp" / f"placsp-{run_id}.zip"
        next_month = date(cursor.year + (cursor.month == 12), cursor.month % 12 + 1, 1)
        valid_zip = _is_valid_zip(destination)
        payload_without_evidence = valid_zip and not provenance_path(destination).exists()
        if valid_zip and not payload_without_evidence:
            evidence = load_raw_artifact(raw_dir, destination)
            if (evidence.source, evidence.partition, evidence.window_start, evidence.window_end) != (
                "placsp", run_id, cursor.isoformat(), (next_month - timedelta(days=1)).isoformat(),
            ):
                raise RawProvenanceError(f"PLACSP partition identity mismatch: {destination}")
            log.info("PLACSP %s already present, skipping", run_id)
            saved.append(destination)
        else:
            try:
                saved_path = _download_zip(
                    url,
                    destination,
                    raw_dir=raw_dir,
                    partition=run_id,
                    window_start=cursor.isoformat(),
                    window_end=(next_month - timedelta(days=1)).isoformat(),
                    context=context,
                    timeout=source.get("timeout_seconds", 120),
                    attempts=source.get("retry_attempts", 3),
                    logger=log,
                )
                saved.append(saved_path)
            except RuntimeError as exc:
                log.warning("PLACSP %s not downloaded: %s", run_id, exc)
        cursor = next_month
    return saved


def _is_valid_zip(path: Path) -> bool:
    """Check that a path is a complete, readable zip archive."""

    if not path.is_file() or path.stat().st_size < 100:
        return False
    try:
        with zipfile.ZipFile(path) as archive:
            return bool(archive.namelist())
    except zipfile.BadZipFile:
        return False


def _download_zip(
    url: str,
    destination: Path,
    *,
    raw_dir: Path,
    partition: str,
    window_start: str,
    window_end: str,
    context: ssl.SSLContext,
    timeout: int,
    attempts: int,
    logger: logging.Logger,
) -> Path:
    """Download one zip with bounded retries, validating it before commit."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, attempts + 1):
        try:
            with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as handle:
                temporary = Path(handle.name)
            try:
                request = Request(url, headers={"User-Agent": "tfm-licitaciones/0.2", "Accept": "application/zip"})
                with urlopen(request, timeout=timeout, context=context) as response, open(temporary, "wb") as output:
                    while True:
                        chunk = response.read(1 << 20)
                        if not chunk:
                            break
                        output.write(chunk)
                retrieved_at = datetime.now(timezone.utc)
                if not _is_valid_zip(temporary):
                    raise ValueError("downloaded file is not a complete zip archive")
                saved = persist_raw_artifact(
                    temporary, destination, raw_dir, source="placsp", retrieved_at=retrieved_at,
                    partition=partition, window_start=window_start, window_end=window_end,
                )
                logger.info("PLACSP downloaded %s -> %s", url, saved)
                return saved
            finally:
                temporary.unlink(missing_ok=True)
        except RawProvenanceError:
            raise
        except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
            if attempt == attempts:
                raise RuntimeError(f"Download failed after {attempts} attempt(s): {url}") from exc
            wait_seconds = min(2 ** (attempt - 1), 8)
            logger.warning("Download attempt %d/%d failed for %s: %s", attempt, attempts, url, exc)
            time.sleep(wait_seconds)


def fetch_raw_batch(
    source: str, start: date, end: date, config: dict[str, Any], raw_dir: Path,
) -> tuple[int, list[Path]]:
    """Capture JSON batch retrieval completion before serialization/persistence.

    TED's existing Raw artifact aggregates a requested window. Its retrieval
    time is completion of that batch, not an individual notice's publication.
    """

    adapter = {"ted": fetch_ted, "boe": fetch_boe}[source]
    rows = adapter(start, end, config)
    retrieved_at = datetime.now(timezone.utc)
    paths = save_raw_batches(
        raw_dir, f"{start:%Y%m%d}-{end:%Y%m%d}", rows if source == "ted" else [],
        rows if source == "boe" else [], retrieved_at=retrieved_at,
        window_start=start.isoformat(), window_end=end.isoformat(),
    )
    return len(rows), paths


def save_raw_batches(
    raw_dir: Path, run_id: str, ted: list[dict[str, Any]], boe: list[dict[str, Any]], *,
    retrieved_at: datetime, window_start: str | None = None, window_end: str | None = None,
) -> list[Path]:
    """Persist batches with explicit retrieval evidence, preserving prior bytes."""

    paths: list[Path] = []
    for source, rows in (("ted", ted), ("boe", boe)):
        if not rows:
            continue
        destination = raw_dir / source / f"{source}-{run_id}.jsonl"
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as handle:
            temporary = Path(handle.name)
        try:
            write_jsonl(temporary, rows)
            paths.append(persist_raw_artifact(
                temporary, destination, raw_dir, source=source, partition=run_id,
                retrieved_at=retrieved_at, window_start=window_start, window_end=window_end,
            ))
        finally:
            temporary.unlink(missing_ok=True)
    return paths
