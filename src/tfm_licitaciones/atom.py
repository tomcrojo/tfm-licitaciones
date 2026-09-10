"""Atom/XML adapters for the OpenPLACSP source contract.

PLACSP publishes chained Atom files (RFC 4287) whose entries embed CODICE
fragments: CPV classifications, DIR3 buyer identifiers, budget amounts and
contract-folder status. The parser keeps every structured field that the
silver contract can absorb and returns flat payloads for the bronze layer.
"""

from __future__ import annotations

import re
import zipfile
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


def parse_atom_file(path: Path) -> list[TenderRecord]:
    """Parse a plain Atom feed into normalized records without losing fields."""

    payloads, _ = parse_placsp_atom(Path(path).read_text(encoding="utf-8"))
    records = [normalize_placsp(payload) for payload in payloads]
    return [record for record in records if record.title]


def parse_placsp_atom(text: str) -> tuple[list[dict[str, Any]], set[str]]:
    """Parse one PLACSP Atom document into entry payloads and tombstone ids.

    Returns the flat entry payloads (bronze-ready, ``_source=placsp``) plus
    the set of entry ids withdrawn by ``at:deleted-entry`` tombstones, which
    later files use to cancel previously published tenders.
    """

    root = ElementTree.fromstring(text)
    payloads: list[dict[str, Any]] = []
    tombstones = {ref for node in root.findall("at:deleted-entry", CODICE_NS) if (ref := node.attrib.get("ref"))}
    for entry in root.findall("atom:entry", CODICE_NS):
        payload = _parse_entry(entry)
        if payload is not None:
            payloads.append(payload)
    return payloads, tombstones


def iter_placsp_zip(path: Path) -> tuple[list[dict[str, Any]], set[str], int]:
    """Parse every Atom member inside one OpenPLACSP monthly zip.

    Returns the concatenated payloads, the merged tombstone set and the
    number of Atom files processed, so provenance can be recorded without
    extracting the archive onto disk.
    """

    payloads: list[dict[str, Any]] = []
    tombstones: set[str] = set()
    atoms = 0
    with zipfile.ZipFile(path) as archive:
        for name in archive.namelist():
            if not name.lower().endswith(".atom"):
                continue
            atoms += 1
            text = archive.read(name).decode("utf-8", errors="replace")
            batch, deleted = parse_placsp_atom(text)
            for payload in batch:
                payload["_atom_file"] = name
            payloads.extend(batch)
            tombstones |= deleted
    return payloads, tombstones, atoms


def _parse_entry(entry: ElementTree.Element) -> dict[str, Any] | None:
    """Extract the CODICE fields of one Atom entry into a flat payload."""

    entry_id = _text(entry, "id")
    number = _ENTRY_ID_NUMBER.search(entry_id or "")
    title = _text(entry, "title")
    if not number or not title:
        return None
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
            payload[key] = _parse_amount(node.text)
            payload[f"{key}_currency"] = node.attrib.get("currencyID")
    return payload


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
