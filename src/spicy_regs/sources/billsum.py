"""Read CRS bill summaries in bulk: one GovInfo ``BILLSUM`` folder zip per Congress and bill type.

The keyless route is ``bulkdata/BILLSUM/{congress}/{type}/BILLSUM-{congress}-{type}.zip``, one zip per
publisher folder, holding one XML file per bill named ``BILLSUM-{congress}{type}{number}.xml``. The folder
also answers a keyless JSON listing (``bulkdata/json/BILLSUM/{congress}/{type}``) naming every file it holds
with its own stamp and size, the zip included. It is the same listing shape the BILLSTATUS and BILLS folders
answer, so it is read by spicy-docs' shared ``read_folder_listing`` and compared by ``zip_entry_unchanged``.

Why the bill family reads it: BILLSTATUS is the family's summary source, and GovInfo does not regenerate a
BILLSTATUS file when CRS publishes a summary for a bill of an ended Congress. On 2026-11-09 the BILLSUM zips of
the 117th-119th held 84 summaries the BILLSTATUS-built ``bill_publisher_summaries`` did not (82 of the 118th,
all introduced-stage, their files dated February to November 2026; 2 of the 117th), and agreed on every other
(bill, summary version) pair: 21,253 of the 117th, 14,208 of the 118th and 6,464 of the 119th. 118-hr-4291's
BILLSTATUS file was last regenerated 2025-02-04; BILLSUM states its introduced summary with
``update-date="2026-10-26"``.

What a BILLSUM ``<summary>`` states, per the publisher's user guide
(https://github.com/usgpo/bulk-data/blob/main/Bills-Summary-XML-User-Guide.md): a ``summary-id`` ending in the
two-digit summary version code (``id118hr4291v00``), an ``update-date`` with day precision and no time,
``<action-date>``, ``<action-desc>`` and ``<summary-text>``. It states no ``lastSummaryUpdateDate`` and no
schema version. Every value is kept as the publisher spelled it; no time is supplied for the day.

The text is BILLSUM's own rendition, not BILLSTATUS's bytes: for summaries both hold, BILLSUM sometimes
carries earlier unmarked text or an empty later-stage text. An empty or blank ``<summary-text>`` is refused
here, as BILLSTATUS's reader refuses a summary with no text. BILLSUM begins with the 113th Congress
(``BILLSUM_FLOOR``; ``bulkdata/json/BILLSUM`` lists 113 on).

The zip is read offline by :func:`read_billsum_archive`, so a retained zip replays without the client.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import TYPE_CHECKING
from xml.etree.ElementTree import Element

from spicy_docs.reading.xml import parse_xml
from spicy_docs.reading.zip_archive import archive_members, open_archive, read_member
from spicy_docs.releases.format import MAX_EVIDENCE_BYTES
from spicy_docs.sources.congress.bill_acquisition import BillSourceUnavailableError
from spicy_docs.sources.congress.bill_status import BillIdentity, BillSourceError
from spicy_docs.sources.congress.bulk_status import (
    ARCHIVE_MEDIA_TYPES,
    DEFAULT_MAX_ENTRIES,
    DEFAULT_MAX_LISTING_BYTES,
    DEFAULT_MAX_TOTAL_BYTES,
    JSON_LISTING_MEDIA_TYPES,
    MAX_BULK_STATUS_BYTES,
    BulkArchiveBudget,
    BulkListingEntry,
    read_folder_listing,
)
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.source_acquirer import SourceAcquirer, named_challenge, utc_now

if TYPE_CHECKING:
    import httpx

BILLSUM_BULKDATA = "https://www.govinfo.gov/bulkdata/BILLSUM"
BILLSUM_LISTING_JSON = "https://www.govinfo.gov/bulkdata/json/BILLSUM"

#: The oldest Congress the BILLSUM bulk collection holds (``bulkdata/json/BILLSUM`` lists 113 on, checked
#: 2026-11-09). An older Congress's summaries are BILLSTATUS's alone.
BILLSUM_FLOOR = 113

#: The literal a BILLSUM row carries in ``bill_publisher_summaries.source_schema_version``. That column names
#: the source format that gives a row's clocks their meaning; BILLSUM states no version of its own, so the
#: row names the collection. A BILLSTATUS row states its BILLSTATUS version (``1.0.0``, ``3.0.0``), never this.
BILLSUM_SCHEMA_CONTEXT = "BILLSUM"

_LABEL = "BILLSUM archive"
_LISTING_LABEL = "BILLSUM bulk listing"
# Bounded digit runs, as in spicy-docs' bulk readers: four digits cover any Congress, seven any bill number.
_TYPES = "hconres|sconres|hjres|sjres|hres|sres|hr|s"
_MEMBER_NAME = re.compile(rf"BILLSUM-([1-9][0-9]{{0,3}})({_TYPES})([1-9][0-9]{{0,6}})\.xml")
_SUMMARY_ID = re.compile(rf"id([1-9][0-9]{{0,3}})({_TYPES})([1-9][0-9]{{0,6}})v([0-9]{{2}})")
#: One BILLSUM file is one bill's summaries: the largest measured 2026-11-09 (117th-119th) is well under this.
MAX_MEMBER_BYTES = MAX_EVIDENCE_BYTES


def _folder(congress: int, bill_type: str) -> str:
    """``{congress}/{type}``, with the Congress and type proved by ``BillIdentity``."""
    BillIdentity(congress, bill_type, 1)
    if congress < BILLSUM_FLOOR:
        raise BillSourceError(f"BILLSUM bulk begins with the {BILLSUM_FLOOR}th Congress")
    return f"{congress}/{bill_type}"


def billsum_locator(congress: int, bill_type: str) -> str:
    """One zip per Congress and bill type, in the publisher's lowercase folder names."""
    return f"{BILLSUM_BULKDATA}/{_folder(congress, bill_type)}/BILLSUM-{congress}-{bill_type}.zip"


def billsum_listing_locator(congress: int, bill_type: str) -> str:
    """The same folder's keyless JSON listing: every file it holds, the zip included."""
    return f"{BILLSUM_LISTING_JSON}/{_folder(congress, bill_type)}"


@dataclass(frozen=True, slots=True)
class BillSumListing:
    """One folder's listing: every entry and the folder's own zip entry."""

    congress: int
    bill_type: str
    entries: tuple[BulkListingEntry, ...]
    zip_entry: BulkListingEntry
    folder_modified: datetime | None


def read_billsum_listing(
    body: bytes, *, congress: int, bill_type: str, max_bytes: int = DEFAULT_MAX_LISTING_BYTES
) -> BillSumListing:
    """Read one retained folder listing offline; an entry another folder's ``link`` names refuses the whole read."""
    locator = billsum_locator(congress, bill_type)
    entries, zip_entry, folder_modified = read_folder_listing(
        body,
        folder_url=f"{BILLSUM_BULKDATA}/{_folder(congress, bill_type)}",
        zip_name=locator.rsplit("/", 1)[-1],
        label=_LISTING_LABEL,
        max_bytes=max_bytes,
    )
    return BillSumListing(congress, bill_type, entries, zip_entry, folder_modified)


@dataclass(frozen=True, slots=True)
class BillSumSummary:
    """One ``<summary>`` of a BILLSUM file, every value as the publisher spelled it."""

    identity: BillIdentity
    version_code: str
    update_date: str
    action_date: str | None
    action_desc: str | None
    text: str


@dataclass(frozen=True, slots=True)
class BillSumMember:
    """One archive entry, in the publisher's order: its summaries, or why the file was refused.

    ``refused_summaries`` counts the summaries of a read file refused one by one (an empty text, an unreadable
    ``summary-id``); ``refusal`` is set only when the whole file could not be read, and then ``summaries`` is
    empty.
    """

    name: str
    identity: BillIdentity | None
    byte_size: int
    sha256: str
    summaries: tuple[BillSumSummary, ...]
    refused_summaries: tuple[str, ...]
    refusal: str | None


@dataclass(frozen=True, slots=True)
class BillSumArchive:
    """Every entry of one folder's zip, read or refused."""

    congress: int
    bill_type: str
    members: tuple[BillSumMember, ...]

    @property
    def summary_count(self) -> int:
        return sum(len(member.summaries) for member in self.members)

    @property
    def refused_count(self) -> int:
        return sum(member.refusal is not None for member in self.members)


def _member_identity(name: str, *, congress: int, bill_type: str) -> BillIdentity:
    match = _MEMBER_NAME.fullmatch(name)
    if match is None:
        raise BillSourceError("BILLSUM archive entry name does not state a bill")
    identity = BillIdentity(int(match[1]), match[2], int(match[3]))
    if (identity.congress, identity.bill_type) != (congress, bill_type):
        raise BillSourceError("BILLSUM archive entry belongs to another Congress or bill type")
    return identity


def _leaf(parent: Element, name: str) -> str | None:
    elements = parent.findall(name)
    if len(elements) > 1:
        raise BillSourceError(f"BILLSUM summary states {name} more than once")
    if not elements:
        return None
    if len(elements[0]):
        raise BillSourceError(f"BILLSUM summary {name} must be a text leaf")
    return elements[0].text or ""


def _summary(element: Element, identity: BillIdentity) -> BillSumSummary:
    match = _SUMMARY_ID.fullmatch(element.get("summary-id") or "")
    if match is None:
        raise BillSourceError("BILLSUM summary-id does not state a bill and summary version")
    if BillIdentity(int(match[1]), match[2], int(match[3])) != identity:
        raise BillSourceError("BILLSUM summary-id names another bill than its file")
    update_date = element.get("update-date")
    if not update_date:
        raise BillSourceError("BILLSUM summary states no update-date")
    text = _leaf(element, "summary-text")
    if text is None or not text.strip():
        raise BillSourceError("BILLSUM summary states no text")
    return BillSumSummary(
        identity=identity,
        version_code=match[4],
        update_date=update_date,
        action_date=_leaf(element, "action-date"),
        action_desc=_leaf(element, "action-desc"),
        text=text,
    )


def read_billsum_file(body: bytes, *, identity: BillIdentity) -> tuple[tuple[BillSumSummary, ...], tuple[str, ...]]:
    """One BILLSUM file's summaries, and the reasons any single summary was refused.

    The file's ``<item>`` must name the bill its file name does; a file naming another refuses whole.
    """
    root = parse_xml(body, max_bytes=MAX_MEMBER_BYTES, error_type=BillSourceError, label="BILLSUM XML")
    if root.tag != "BillSummaries":
        raise BillSourceError("BILLSUM XML root is not BillSummaries")
    summaries: list[BillSumSummary] = []
    refused: list[str] = []
    for item in root.findall("item"):
        try:
            stated = BillIdentity(
                int(item.get("congress") or ""), item.get("measure-type") or "", int(item.get("measure-number") or "")
            )
        except (TypeError, ValueError) as error:
            raise BillSourceError("BILLSUM item does not state its bill") from error
        if stated != identity:
            raise BillSourceError("BILLSUM item names another bill than its file")
        for element in item.findall("summary"):
            try:
                summaries.append(_summary(element, identity))
            except BillSourceError as error:
                refused.append(str(error))
    return tuple(summaries), tuple(refused)


def read_billsum_archive(
    capture: CapturedBodyResponse,
    *,
    congress: int,
    bill_type: str,
    max_bytes: int = MAX_BULK_STATUS_BYTES,
    max_entries: int = DEFAULT_MAX_ENTRIES,
    max_entry_bytes: int = MAX_MEMBER_BYTES,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
) -> BillSumArchive:
    """Read one retained folder zip offline. Archive-wide bounds refuse the zip; one bad file is its own refusal."""
    locator = billsum_locator(congress, bill_type)
    if (capture.requested_url, capture.resolved_url) != (locator, locator):
        raise BillSourceError("BILLSUM archive capture is not the listed folder's zip")
    members: list[BillSumMember] = []
    with open_archive(
        capture.body,
        max_bytes=max_bytes,
        max_entries=max_entries,
        max_entry_bytes=max_entry_bytes,
        max_total_bytes=max_total_bytes,
        error_type=BillSourceError,
        label=_LABEL,
    ) as archive:
        for info in archive_members(archive, max_entries=max_entries, error_type=BillSourceError, label=_LABEL):
            name = info.filename.rsplit("/", 1)[-1]
            data = read_member(archive, info, max_bytes=max_entry_bytes, error_type=BillSourceError, label=_LABEL)
            identity: BillIdentity | None = None
            summaries: tuple[BillSumSummary, ...] = ()
            refused: tuple[str, ...] = ()
            refusal: str | None = None
            try:
                identity = _member_identity(name, congress=congress, bill_type=bill_type)
                summaries, refused = read_billsum_file(data, identity=identity)
            except BillSourceError as error:
                refusal = str(error)
            members.append(
                BillSumMember(
                    name=info.filename,
                    identity=identity,
                    byte_size=len(data),
                    sha256="sha256:" + hashlib.sha256(data).hexdigest(),
                    summaries=summaries,
                    refused_summaries=refused,
                    refusal=refusal,
                )
            )
    return BillSumArchive(congress, bill_type, tuple(members))


@dataclass(frozen=True, slots=True)
class BillSumListingAcquisition:
    listing: BillSumListing
    capture: CapturedBodyResponse
    request_count: int
    budget: BulkArchiveBudget


@dataclass(frozen=True, slots=True)
class BillSumAcquisition:
    archive: BillSumArchive
    capture: CapturedBodyResponse
    request_count: int
    budget: BulkArchiveBudget


class BillSumAcquirer(SourceAcquirer):
    """Keyless capture of one BILLSUM folder's listing, then, if the caller wants it, its zip.

    Two calls, as for the BILLS folders: whether the zip moved since a retained entry is the listing's to
    answer, and the caller decides.
    """

    def __init__(
        self,
        *,
        budget: BulkArchiveBudget,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if not isinstance(budget, BulkArchiveBudget):
            raise TypeError("budget must be a BulkArchiveBudget")
        self._budget = budget
        super().__init__(
            max_requests=budget.max_requests,
            timeout_seconds=budget.timeout_seconds,
            min_request_interval_seconds=budget.min_request_interval_seconds,
            user_agent="spicy-regs-billsum/1.0",
            label="BILLSUM bulk",
            error_type=BillSourceError,
            context_key="billsum_acquisition",
            transport=transport,
            clock=clock,
            keyless=True,
        )

    @property
    def budget(self) -> BulkArchiveBudget:
        return self._budget

    def list_folder(self, congress: int, bill_type: str) -> BillSumListingAcquisition:
        """Capture the folder's listing, bounded like the BILLSTATUS listings; a 404 means that folder."""
        budget = self.budget
        url = billsum_listing_locator(congress, bill_type)
        with named_challenge(url, error_type=BillSourceError, context_key=self.context_key):
            listing, capture = self.capture_validated(
                url,
                media_types=JSON_LISTING_MEDIA_TYPES,
                parse=lambda response, limit: read_billsum_listing(
                    response.body, congress=congress, bill_type=bill_type, max_bytes=limit
                ),
                max_bytes=min(budget.max_bytes, DEFAULT_MAX_LISTING_BYTES),
                unavailable=BillSourceUnavailableError,
                context={
                    "operation": "billsum-listing",
                    "selection": {"congress": congress, "billType": bill_type},
                    "budget": asdict(budget),
                },
                request_headers={"Accept": "application/json"},
            )
        return BillSumListingAcquisition(listing, capture, self.request_count, budget)

    def acquire(self, listing: BillSumListing) -> BillSumAcquisition:
        """Capture the listed folder's zip once and read every member."""
        if not isinstance(listing, BillSumListing):
            raise TypeError("listing must be a BillSumListing")
        budget = self.budget
        url = billsum_locator(listing.congress, listing.bill_type)
        with named_challenge(url, error_type=BillSourceError, context_key=self.context_key):
            archive, capture = self.capture_validated(
                url,
                media_types=ARCHIVE_MEDIA_TYPES,
                parse=lambda response, limit: read_billsum_archive(
                    response,
                    congress=listing.congress,
                    bill_type=listing.bill_type,
                    max_bytes=limit,
                    max_entries=budget.max_entries,
                    max_entry_bytes=min(budget.max_entry_bytes, MAX_MEMBER_BYTES),
                    max_total_bytes=budget.max_total_bytes,
                ),
                max_bytes=budget.max_bytes,
                unavailable=BillSourceUnavailableError,
                context={
                    "operation": "billsum-archive",
                    "selection": {"congress": listing.congress, "billType": listing.bill_type},
                    "budget": asdict(budget),
                },
            )
        return BillSumAcquisition(archive, capture, self.request_count, budget)


__all__ = [
    "BILLSUM_BULKDATA",
    "BILLSUM_FLOOR",
    "BILLSUM_LISTING_JSON",
    "BILLSUM_SCHEMA_CONTEXT",
    "BillSumAcquirer",
    "BillSumAcquisition",
    "BillSumArchive",
    "BillSumListing",
    "BillSumListingAcquisition",
    "BillSumMember",
    "BillSumSummary",
    "billsum_listing_locator",
    "billsum_locator",
    "read_billsum_archive",
    "read_billsum_file",
    "read_billsum_listing",
]
