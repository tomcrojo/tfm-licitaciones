"""Source-specific normalization into the common tender contract."""

from __future__ import annotations

from datetime import date
from typing import Any

from .models import TenderRecord


def _first_text(value: Any, language: str = "spa") -> str:
    """Extract a useful string from TED's localized dict/list structures."""

    if value is None:
        return ""
    if isinstance(value, dict):
        preferred = value.get(language)
        if preferred is not None:
            return _first_text(preferred, language)
        for candidate in value.values():
            text = _first_text(candidate, language)
            if text:
                return text
        return ""
    if isinstance(value, list):
        for candidate in value:
            text = _first_text(candidate, language)
            if text:
                return text
        return ""
    return str(value).strip()


def _parse_date(value: Any) -> date | None:
    """Parse an ISO-like source date, ignoring time and timezone suffixes."""

    text = _first_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _normalize_amount_text(value: Any) -> str | None:
    """Normalize decimal/thousands separators, or None when not parseable text."""

    text = _first_text(value).replace(" ", "")
    if not text:
        return None
    if "," in text and "." in text:
        decimal_separator = "," if text.rfind(",") > text.rfind(".") else "."
        thousands_separator = "." if decimal_separator == "," else ","
        return text.replace(thousands_separator, "").replace(decimal_separator, ".")
    if "," in text:
        return text.replace(",", ".")
    if text.count(".") == 1:
        return text
    return text.replace(".", "")


def _parse_amount(value: Any) -> float | None:
    """Parse a simple numeric amount while leaving ambiguous values null."""

    normalized = _normalize_amount_text(value)
    if normalized is None:
        return None
    try:
        amount = float(normalized)
    except ValueError:
        return None
    return amount if amount >= 0 else None


def _url_from_links(links: Any) -> str:
    """Extract a stable notice URL from TED's per-language links payload."""

    if not isinstance(links, dict):
        return ""
    html = links.get("html")
    if not isinstance(html, dict):
        return ""
    for language in ("ENG", "SPA", "DEU", "FRA"):
        if html.get(language):
            return str(html[language]).strip()
    return str(next(iter(html.values()), "")).strip()


def normalize_ted(raw: dict[str, Any]) -> TenderRecord:
    """Normalize one TED eForms record."""

    title = _first_text(raw.get("TI") or raw.get("title"))
    summary = _first_text(raw.get("description-glo") or raw.get("description"))
    buyer = _first_text(raw.get("buyer-name") or raw.get("buyer")) or None
    amount = _parse_amount(
        raw.get("estimated-value-lot") or raw.get("framework-maximum-value-lot") or raw.get("amount")
    )
    cpv_codes = _as_code_list(raw.get("PC") or raw.get("cpv"))
    return TenderRecord(
        tender_id=_first_text(raw.get("ND") or raw.get("notice_id")),
        source="ted",
        title=title,
        summary=summary,
        buyer=buyer,
        published_date=_parse_date(raw.get("PD") or raw.get("publication_date") or raw.get("publication-date")),
        amount=amount,
        # The TED Search API normalizes value fields to EUR, so the currency
        # is inferred and kept explicit for downstream aggregation.
        currency=_first_text(raw.get("currency")) or ("EUR" if amount is not None else None),
        country=_first_text(raw.get("CY") or raw.get("country")) or None,
        url=_first_text(raw.get("url")) or _url_from_links(raw.get("links")) or None,
        cpv_main=cpv_codes[0] if cpv_codes else None,
        raw=raw,
    )


def normalize_placsp(payload: dict[str, Any]) -> TenderRecord:
    """Normalize one OpenPLACSP Atom/CODICE payload."""

    return TenderRecord(
        tender_id=_first_text(payload.get("tender_no") or payload.get("atom_id")),
        source="placsp",
        title=_first_text(payload.get("title")),
        summary=_first_text(payload.get("summary")),
        buyer=_first_text(payload.get("buyer")) or None,
        published_date=_parse_date(payload.get("updated")),
        amount=payload.get("amount_tax_exclusive"),
        currency=payload.get("amount_tax_exclusive_currency"),
        country="ES",
        url=_first_text(payload.get("url")) or None,
        cpv_main=(_as_code_list(payload.get("cpv")) or [None])[0],
        buyer_id=_first_text(payload.get("buyer_dir3")) or None,
        region=_first_text(payload.get("region")) or None,
        status=_first_text(payload.get("status_code")) or None,
        raw={key: value for key, value in payload.items() if key != "_source"},
    )


def _as_code_list(value: Any) -> list[str]:
    """Flatten a list/scalar of CPV codes while preserving order."""

    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    codes: list[str] = []
    for item in items:
        text = _first_text(item)
        if text and text not in codes:
            codes.append(text)
    return codes


def normalize_boe(raw: dict[str, Any]) -> TenderRecord:
    """Normalize one BOE item as contextual procurement evidence."""

    return TenderRecord(
        tender_id=_first_text(raw.get("item_id") or raw.get("identificador")),
        source="boe",
        title=_first_text(raw.get("title") or raw.get("titulo")),
        summary=_first_text(raw.get("summary")),
        buyer=_first_text(raw.get("buyer") or raw.get("department")) or None,
        published_date=_parse_date(raw.get("publication_date")),
        country="ES",
        url=_first_text(raw.get("url")) or None,
        raw=raw,
    )


def normalize_record(raw: dict[str, Any]) -> TenderRecord:
    """Dispatch a raw record to its source adapter."""

    source = str(raw.get("_source") or raw.get("source") or "").lower()
    if source == "ted":
        return normalize_ted(raw)
    if source == "boe":
        return normalize_boe(raw)
    if source == "placsp":
        return normalize_placsp(raw)
    raise ValueError(f"Unsupported source: {source or '<missing>'}")
