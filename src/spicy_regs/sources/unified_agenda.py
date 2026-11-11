"""Normalize SpicyDocs' Unified Agenda observations for the SpicyRegs table.

SpicyDocs fetches and checks each edition's XML. This adapter retains the
published table's field choices and whitespace rules. A failed edition raises
before any of its rows are yielded; earlier completed editions remain yielded.
"""

from __future__ import annotations

import re
from dataclasses import asdict
from collections.abc import Callable, Iterator, Sequence
from typing import TYPE_CHECKING

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

if TYPE_CHECKING:
    from spicy_docs.sources.unified_agenda.records import UnifiedAgendaField, UnifiedAgendaRecordObservation

#: The newest edition when this was written; ``newest_edition`` moves past it.
DEFAULT_EDITION = "202510"

#: The publisher's page naming each edition's XML file. It is the publisher's own
#: statement of what exists: an unpublished edition's export answers 200 with an
#: HTML page, so the export cannot be probed. The page omits Spring 2018 (201804),
#: which the export still serves, so it moves the newest edition, never the series.
EDITION_REPORT_URL = "https://www.reginfo.gov/public/do/eAgendaXmlReport"
_LISTED_EDITION = re.compile(r"REGINFO_RIN_DATA_((?:19|20)[0-9]{2}(?:04|10))\.xml")


def newest_edition(*, transport: httpx.BaseTransport | None = None) -> str:
    """The newest edition the publisher's report page names, never older than ``DEFAULT_EDITION``.

    A page that cannot be read, or names no edition, keeps ``DEFAULT_EDITION`` and says so in the
    run log: the run still refreshes the edition it knows.
    """
    try:
        with httpx.Client(transport=transport, timeout=60, follow_redirects=True) as client:
            response = client.get(EDITION_REPORT_URL)
            response.raise_for_status()
    except httpx.HTTPError as error:
        logger.warning(
            "Unified Agenda: edition report unreadable ({}); newest edition stays {}", error, DEFAULT_EDITION
        )
        return DEFAULT_EDITION
    listed = _LISTED_EDITION.findall(response.text)
    if not listed:
        logger.warning("Unified Agenda: edition report names no edition; newest edition stays {}", DEFAULT_EDITION)
        return DEFAULT_EDITION
    return max(DEFAULT_EDITION, *listed)


class UnifiedAgendaReader(Reader):
    """Read explicit editions; the default selects the held Fall 2025 edition."""

    def __init__(
        self,
        *,
        editions: tuple[str, ...] | None = None,
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
        on_source: Callable[[dict], object] | None = None,
        evidence=None,
    ) -> None:
        self.editions = editions or (DEFAULT_EDITION,)
        self.verbose = verbose
        self.transport = transport
        self.on_source = on_source
        #: A ``CaptureEvidence``: each edition file is retained as read, before it is parsed.
        self.evidence = evidence

    def iter_records(self) -> Iterator[dict]:
        # Base CLI/MCP installations can import source names without the optional
        # owner wheel. Fetching an edition requires the source-readers extra.
        try:
            from spicy_docs.sources.unified_agenda import (
                UnifiedAgendaAcquirer,
                UnifiedAgendaBudget,
                UnifiedAgendaEdition,
            )
        except ModuleNotFoundError as error:
            if error.name == "spicy_docs":
                raise RuntimeError(
                    "Unified Agenda requires spicy-regs[source-readers]. "
                    "Run `uv sync --frozen` in a SpicyRegs checkout."
                ) from None
            raise

        budget = UnifiedAgendaBudget(
            max_requests=5,
            max_bytes=64 * 1024**2,
            timeout_seconds=300,
            min_request_interval_seconds=1,
        )
        with UnifiedAgendaAcquirer(budget=budget, transport=self.transport) as source:
            for edition in self.editions:
                acquired = source.acquire_edition(UnifiedAgendaEdition(edition))
                if self.evidence is not None:
                    self.evidence.capture(acquired.capture, stage="unified-agenda-edition", edition=edition)
                rows = normalize_retained_edition(acquired.capture.body, edition=edition, on_source=self.on_source)
                logger.info("Unified Agenda: edition {} yielded {:,} records", edition, len(rows))
                yield from rows


def normalize_retained_edition(
    body: bytes, *, edition: str, on_source: Callable[[dict], object] | None = None
) -> list[dict]:
    """Read an exact retained edition offline using the maintained source checks.

    The table keeps its strip-only normalization. Historical repair is shared
    with acquisition, qualified by the full original digest, and receipted.
    ``related_rins`` is None when the record states no ``RELATED_RIN_LIST``;
    each entry's RIN and relation are stripped, never shape-checked, so
    self-references, repeats and cut-off values ("Related t") stay as stated.
    SpicyDocs' projection collapses internal whitespace instead; the two agree
    on all 28,720 related RINs of the 60 retained editions only because none
    holds an internal whitespace run.
    No rows or success receipt escape a failed whole-edition scan.
    """
    import hashlib

    from spicy_docs.sources.unified_agenda import (
        UnifiedAgendaEdition,
        unified_agenda_xml_locator,
        validate_unified_agenda_xml,
    )
    from spicy_docs.sources.unified_agenda.records import scan_unified_agenda_records
    from spicy_docs.sources.unified_agenda.repair import prepare_unified_agenda_xml

    selected = UnifiedAgendaEdition(edition)
    metadata = validate_unified_agenda_xml(body, edition=selected, final_url=unified_agenda_xml_locator(selected))
    readable, repair = prepare_unified_agenda_xml(body, publication_id=metadata.publication_id)
    rows: list[dict] = []
    scan_unified_agenda_records(
        readable, on_record=lambda record: rows.append(_normalize(record, metadata.publication_id))
    )
    if on_source is not None:
        on_source(
            {
                "edition": metadata.publication_id,
                "file_stem": edition,
                "input_sha256": hashlib.sha256(body).hexdigest(),
                "input_bytes": len(body),
                "records": len(rows),
                "repair": asdict(repair) if repair is not None else None,
            }
        )
    return rows


def _find(fields: Sequence[UnifiedAgendaField], *path: str) -> UnifiedAgendaField | None:
    # ElementTree.find("parent/child") skips parents without a matching child.
    for field in fields:
        if field.element.tag == path[0]:
            if len(path) == 1:
                return field
            match = _find(field.children, *path[1:])
            if match is not None:
                return match
    return None


def _text(fields: Sequence[UnifiedAgendaField], *path: str) -> str | None:
    field = _find(fields, *path)
    value = field.leading_text.strip() if field is not None else ""
    return value or None


def _items(fields: Sequence[UnifiedAgendaField], container: str, name: str) -> Iterator[UnifiedAgendaField]:
    # ElementTree.findall("container/item") read every matching container in
    # the old adapter. Repeated source containers must keep that ordering.
    for field in fields:
        if field.element.tag == container:
            yield from (child for child in field.children if child.element.tag == name)


def _normalize(record: UnifiedAgendaRecordObservation, edition: str) -> dict:
    """Shape one observed record into the published row, preferring agency ``ACRONYM`` over ``CODE``."""
    fields = record.fields
    agency = _find(fields, "AGENCY")
    agency_fields = agency.children if agency is not None else ()
    timetable = [
        {
            "action": _text(entry.children, "TTBL_ACTION"),
            "date": _text(entry.children, "TTBL_DATE"),
            "fr_citation": _text(entry.children, "FR_CITATION"),
        }
        for entry in _items(fields, "TIMETABLE_LIST", "TIMETABLE")
    ]
    return {
        "rin": _text(fields, "RIN"),
        "agency_code": _text(agency_fields, "ACRONYM") or _text(agency_fields, "CODE"),
        "agency_name": _text(agency_fields, "NAME"),
        "title": _text(fields, "RULE_TITLE"),
        "abstract": _text(fields, "ABSTRACT"),
        "priority_category": _text(fields, "PRIORITY_CATEGORY"),
        "rin_status": _text(fields, "RIN_STATUS"),
        "rule_stage": _text(fields, "RULE_STAGE"),
        "major": _text(fields, "MAJOR"),
        "publication_id": _text(fields, "PUBLICATION", "PUBLICATION_ID"),
        "agenda_edition": edition,
        "cfr_references": [text for field in _items(fields, "CFR_LIST", "CFR") if (text := field.leading_text.strip())],
        "legal_authority": [
            text
            for field in _items(fields, "LEGAL_AUTHORITY_LIST", "LEGAL_AUTHORITY")
            if (text := field.leading_text.strip())
        ],
        "timetable": timetable,
        "related_rins": [
            {"rin": _text(entry.children, "RIN"), "rin_relation": _text(entry.children, "RIN_RELATION")}
            for entry in _items(fields, "RELATED_RIN_LIST", "RELATED_RIN")
        ]
        if _find(fields, "RELATED_RIN_LIST") is not None
        else None,
    }
