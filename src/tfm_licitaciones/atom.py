"""Atom/XML adapters for the OpenPLACSP source contract.

PLACSP publishes chained Atom files (RFC 4287) whose entries embed CODICE
fragments: CPV classifications, DIR3 buyer identifiers, budget amounts and
contract-folder status. The parser keeps every structured field that the
silver contract can absorb and returns flat payloads for the bronze layer.
"""

from __future__ import annotations

import lzma
import math
import re
import zipfile
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from .models import TenderRecord
from .normalize import _parse_amount, normalize_placsp

ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
CODICE_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "cbc": "urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2",
    "cac": "urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2",
    "cpe": "urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2",
    "cpex": "urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2",
    "at": "http://purl.org/atompub/tombstones/1.0",
}

_ENTRY_ID_NUMBER = re.compile(r"/(\d+)\s*$")


@dataclass
class AtomBatch:
    """Account for every entry and distinguish unreadable documents."""

    entries: list[dict[str, Any]] = field(default_factory=list)
    rejections: list[dict[str, Any]] = field(default_factory=list)
    tombstone_rows: list[dict[str, Any]] = field(default_factory=list)
    atom_files: int = 0

    @property
    def tombstones(self) -> set[str]:
        """Keep the legacy ref-set API while retaining every located control."""

        return {row["source_record_id"] for row in self.tombstone_rows}

    def require_valid(self) -> None:
        """Legacy adapters fail explicitly instead of silently dropping entries."""

        if self.rejections:
            raise ValueError(f"Atom parsing rejected input: {self.rejections[0]}")


def parse_atom_file(path: Path) -> list[TenderRecord]:
    """Parse a plain Atom feed into normalized records without losing fields."""

    batch = parse_atom_batch(Path(path).read_bytes())
    batch.require_valid()
    return [normalize_placsp(entry["payload"]) for entry in batch.entries]


def parse_placsp_atom(text: str) -> tuple[list[dict[str, Any]], set[str]]:
    """Parse one PLACSP Atom document into entry payloads and tombstone ids.

    Returns the flat entry payloads (bronze-ready, ``_source=placsp``) plus
    the set of entry ids withdrawn by ``at:deleted-entry`` tombstones, which
    later files use to cancel previously published tenders.
    """

    batch = parse_atom_batch(text)
    batch.require_valid()
    return [entry["payload"] for entry in batch.entries], batch.tombstones


def parse_atom_batch(text: str | bytes, source_member: str | None = None) -> AtomBatch:
    """Return accepted entries, located rejections and separate deletion controls.

    A tombstone with a valid ``ref`` is always retained as deletion evidence.
    If it also publishes an unusable ``when`` value, the row remains undated
    and a separate ``invalid_tombstone_when`` control rejection makes that
    loss of ordering information observable to Bronze ingestion metrics.
    """

    batch = AtomBatch(atom_files=1)
    location = {"source_member": source_member, "record_locator": None, "source_record_id": None}
    try:
        root = ElementTree.fromstring(text)
    except (ElementTree.ParseError, LookupError, ValueError):
        batch.rejections.append({**location, "rejection_reason": "invalid_xml", "rejection_scope": "document"})
        return batch
    if root.tag != f"{{{ATOM_NS['atom']}}}feed":
        batch.rejections.append({**location, "rejection_reason": "expected_atom_feed", "rejection_scope": "document"})
        return batch
    for index, node in enumerate(root.findall("at:deleted-entry", CODICE_NS), 1):
        ref = node.attrib.get("ref", "").strip()
        if ref:
            source_deleted_at, invalid_when = _atom_source_instant(node.attrib.get("when"))
            control_location = {
                **location,
                "record_locator": f"deleted-entry:{index}",
                "source_record_id": ref,
            }
            batch.tombstone_rows.append({
                **control_location,
                "source_deleted_at": source_deleted_at,
            })
            if invalid_when:
                batch.rejections.append({
                    **control_location,
                    "rejection_reason": "invalid_tombstone_when",
                    "rejection_scope": "control",
                })
        else:
            batch.rejections.append({**location, "record_locator": f"deleted-entry:{index}",
                                     "rejection_reason": "missing_tombstone_ref", "rejection_scope": "control"})
    for index, entry in enumerate(root.findall("atom:entry", CODICE_NS), 1):
        entry_id = _text(entry, "id")
        location = {"source_member": source_member, "record_locator": f"entry:{index}",
                    "source_record_id": entry_id or None}
        reason = None
        if not _ENTRY_ID_NUMBER.search(entry_id):
            reason = "invalid_atom_id"
        elif not _text(entry, "title"):
            reason = "missing_title"
        if reason:
            batch.rejections.append({**location, "rejection_reason": reason, "rejection_scope": "record"})
        else:
            payload = _parse_entry(entry)
            if any(isinstance(value, float) and not math.isfinite(value) for value in payload.values()):
                batch.rejections.append({**location, "rejection_reason": "non_finite_amount", "rejection_scope": "record"})
                continue
            if source_member is not None:
                payload["_atom_file"] = source_member
            batch.entries.append({**location, "payload": payload})
    return batch


def iter_placsp_zip(path: Path) -> tuple[list[dict[str, Any]], set[str], int]:
    """Parse every Atom member inside one OpenPLACSP monthly zip.

    Returns the concatenated payloads, the merged tombstone set and the
    number of Atom files processed, so provenance can be recorded without
    extracting the archive onto disk.
    """

    batch = parse_placsp_zip_batch(path)
    batch.require_valid()
    return [entry["payload"] for entry in batch.entries], batch.tombstones, batch.atom_files


def parse_placsp_zip_batch(path: Path) -> AtomBatch:
    """Parse members independently so one malformed feed cannot hide the rest."""

    result = AtomBatch()
    location = {"source_member": None, "record_locator": None, "source_record_id": None}
    try:
        with zipfile.ZipFile(path) as archive:
            for member_index, member in enumerate(archive.infolist(), 1):
                if member.is_dir() or not member.filename.lower().endswith(".atom"):
                    continue
                result.atom_files += 1
                try:
                    batch = parse_atom_batch(archive.read(member), member.filename)
                except (zipfile.BadZipFile, RuntimeError, NotImplementedError, zlib.error,
                        OSError, lzma.LZMAError, EOFError):
                    result.rejections.append({**location, "source_member": member.filename,
                                              "source_member_index": member_index,
                                              "rejection_reason": "unreadable_zip_member", "rejection_scope": "document"})
                    continue
                for row in batch.entries + batch.rejections + batch.tombstone_rows:
                    row["source_member_index"] = member_index
                result.entries.extend(batch.entries)
                result.rejections.extend(batch.rejections)
                result.tombstone_rows.extend(batch.tombstone_rows)
    except zipfile.BadZipFile:
        result.rejections.append({**location, "rejection_reason": "invalid_zip", "rejection_scope": "document"})
        return result
    if not result.atom_files:
        result.rejections.append({**location, "rejection_reason": "no_atom_members", "rejection_scope": "document"})
    return result


def _parse_entry(entry: ElementTree.Element) -> dict[str, Any]:
    """Extract the CODICE fields of one Atom entry into a flat payload."""

    entry_id = _text(entry, "id")
    number = _ENTRY_ID_NUMBER.search(entry_id or "")
    title = _text(entry, "title")
    assert number is not None and title, "Entry must be validated before extraction"
    buyer_node = entry.find(".//cac:Party/cac:PartyName/cbc:Name", CODICE_NS)
    dir3 = None
    nif = None
    for identification in entry.findall(".//cac:Party/cac:PartyIdentification/cbc:ID", CODICE_NS):
        scheme = identification.attrib.get("schemeName", "")
        if scheme == "DIR3" and dir3 is None:
            dir3 = (identification.text or "").strip() or None
        if scheme == "NIF" and nif is None:
            nif = (identification.text or "").strip() or None
    cpv_codes = [
        (node.text or "").strip()
        for node in entry.findall(
            ".//cac:RequiredCommodityClassification/cbc:ItemClassificationCode", CODICE_NS
        )
        if (node.text or "").strip()
    ]
    payload: dict[str, Any] = {
        "_source": "placsp",
        "tender_no": number.group(1),
        "atom_id": entry_id.strip(),
        "title": title,
        "summary": _text(entry, "summary"),
        "updated": _text(entry, "updated"),
        "url": _link(entry),
        "contract_folder_id": _first_text(entry, ".//cpe:ContractFolderStatus/cbc:ContractFolderID"),
        "status_code": _first_text(entry, ".//cbc-place-ext:ContractFolderStatusCode", explicit_ns=True),
        "buyer": (buyer_node.text or "").strip() or None if buyer_node is not None else None,
        "buyer_dir3": dir3,
        "buyer_nif": nif,
        "city": _first_text(entry, ".//cac:PostalAddress/cbc:CityName"),
        "nuts_code": _first_text(entry, ".//cac:RealizedLocation/cbc:CountrySubentityCode"),
        "region": _first_text(entry, ".//cac:RealizedLocation/cbc:CountrySubentity"),
        "cpv": cpv_codes,
    }
    for key, xpath in (
        ("amount_tax_exclusive", ".//cac:BudgetAmount/cbc:TaxExclusiveAmount"),
        ("amount_total", ".//cac:BudgetAmount/cbc:TotalAmount"),
        ("amount_estimated_overall", ".//cac:BudgetAmount/cbc:EstimatedOverallContractAmount"),
    ):
        node = entry.find(xpath, CODICE_NS)
        if node is not None and (node.text or "").strip():
            text = node.text.strip()
            payload[key] = _parse_amount(text)
            payload[f"{key}_currency"] = node.attrib.get("currencyID")
            # Retain the published numeric text so Silver can build an exact
            # Decimal instead of reusing the legacy float conversion.
            payload[f"{key}_raw"] = text
    return payload


def _atom_source_instant(value: str | None) -> tuple[datetime | None, bool]:
    """Parse an Atom source instant and report malformed published values.

    The boolean is ``True`` only when a ``when`` attribute was present but
    unusable for authoritative ordering. A missing attribute is legitimate
    undated evidence and is therefore not a control error.
    """

    if value is None:
        return None, False
    text = value.strip()
    if not text:
        return None, True
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (ValueError, OverflowError):
        return None, True
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None, True
    return parsed.astimezone(timezone.utc), False


def _text(entry: ElementTree.Element, local_name: str) -> str:
    """Read and trim one Atom child element."""

    node = entry.find(f"atom:{local_name}", ATOM_NS)
    return (node.text or "").strip() if node is not None else ""


def _link(entry: ElementTree.Element) -> str | None:
    """Read the first Atom link href."""

    node = entry.find("atom:link", ATOM_NS)
    href = node.attrib.get("href") if node is not None else None
    return href.strip() if href else None


def _first_text(entry: ElementTree.Element, xpath: str, explicit_ns: bool = False) -> str | None:
    """Read the first matching CODICE node text, if present."""

    namespaces = {"cbc-place-ext": "urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2"}
    node = entry.find(xpath, namespaces if explicit_ns else CODICE_NS)
    if node is None or not (node.text or "").strip():
        return None
    return node.text.strip()
