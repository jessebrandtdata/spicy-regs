"""Reports, letters, hearing cover links and their own acquisition checkpoints.

MODS cover links and the CBO letter rule use bytes already fetched. Agenda
acquisition is deferred: this pass has no verified meeting-to-jacket join or
House repository locator. Pending reads precede discovery under the same cap;
completed empty covers are checkpointed without inventing a link row. A report
is one row per published part and dated package revision: every part is read
or none, and that revision's part rows are replaced as a set. The current
checkpoint stays keyed by package.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from collections.abc import Callable, Collection, Iterator
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from functools import cache
from importlib.metadata import version
from pathlib import Path
from typing import Any, NamedTuple, Protocol

import httpx
from loguru import logger
from spicy_docs.extraction.body_text import BodyText, body_text
from spicy_docs.extraction.model import PageContent, PageResult, TextBlock
from spicy_docs.interpretation.cbo_estimates import read_cbo_estimate, recital_bill_id
from spicy_docs.interpretation.hearing_bill_links import cover_links
from spicy_docs.schemas.hearing_bill_link_tables import shape_hearing_bill_link
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.schemas.committee_report_tables import (
    CHAMBER_BY_DOCUMENT_TYPE,
    shape_committee_report,
    shape_committee_report_cbo_letter,
    shape_hearing_transcript,
    shape_report_section,
)
from spicy_docs.schemas.tables import natural_key
from spicy_docs.sources.agency_reports.report_blocks import parse_agency_blocks
from spicy_docs.sources.govinfo.bodies import (
    GovInfoBodySourceError,
    GovInfoPartsRefusedError,
    ModsBill,
    parse_package_id,
    publisher_body_status,
)
from spicy_docs.sources.govinfo.body_acquisition import (
    GovInfoBodyAcquirer,
    GovInfoBodyBudget,
    GovInfoFormatNotOfferedError,
    GovInfoPartsOverBudgetError,
)
from spicy_docs.reading.refusals import RefusedResponse
from spicy_docs.reading.paged_json import PagedJsonSourceError, PagedJsonUnavailableError
from spicy_docs.schemas.tables import text
from spicy_docs.sources.congress.listing import LIST_ROUTES, hearing_detail_event_id, list_route_url
from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader, collection_url
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.govinfo_revisions import latest_read, revision_instant
from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key, listing_reader
from spicy_regs.phase_log import annotate
from spicy_regs.transforms.table_merge import (
    merge_contract_table,
    merge_table,
    published_table,
    set_column,
    write_in_place,
)
from spicy_regs.transforms.committee_report_reads import (
    DETAIL_AWAITED,
    DETAIL_GRACE_DAYS,
    DETAIL_REFUSED,
    DETAIL_UNAVAILABLE,
    READ_COLUMNS,
    READS_TABLE,
    REFUSED_FINAL,
    RULE_VERSIONS,
    SIZE_BASIS,
    awaited,
    prior_reads,
    refusal_rule,
    settled,
)
from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError
from spicy_regs.sources.retained import RetainedGovInfoBodyAcquirer, RetainedGovInfoDiscoveryReader


class PackageDiscoverySource(Protocol):
    """What this transform needs of a GovInfo discovery reader."""

    def packages(self, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


class PackageBodySource(Protocol):
    """What this transform needs of a GovInfo package-body acquirer.

    ``acquire_parts`` reads a report, every part its record states or none;
    ``acquire`` reads a hearing, whose record states no parts. The rendition
    order is the acquirer's sealed default
    (``sources.govinfo.bodies.BODY_PREFERENCE`` — XML, HTML, text, then PDF);
    ``prefer`` is passed only as ``("pdf",)``, to read the PDF a part offers
    in place of the publisher's placeholder text.
    """

    def acquire(self, package_id: str, *, max_bytes: int | None = ..., prefer: tuple[str, ...] = ...) -> Any: ...

    def acquire_parts(self, package_id: str, *, max_bytes: int | None = ..., prefer: tuple[str, ...] = ...) -> tuple[Any, ...]: ...


class HearingDetailSource(Protocol):
    """What this transform needs of a Congress.gov listing reader: the ``hearing-detail`` route."""

    def records(self, route: Any, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


DISCOVERY_BUDGET = PagedJsonBudget(
    max_requests=500,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: Per package: summary and MODS, then one body per part (``2 + P``), paced,
#: with every retry drawn from the same count. Eight gives a two-part report,
#: the most any of the 145 retained CRPT packages states (spicy-docs
#: ``docs/tables.md``), four requests of retry headroom, and reads up to six
#: parts; a record stating seven or more is refused before any body request
#: (``GovInfoPartsOverBudgetError``), every run, until this grows.
BODY_BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=24 * 1024 * 1024,  # MAX_EVIDENCE_BYTES
    max_metadata_bytes=4 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=0.34,
)
#: The bounds a final refusal (:func:`_refusal_is_final`) is recorded under; raising either reads it again.
REFUSAL_BOUNDS = f"body={BODY_BUDGET.max_body_bytes};requests={BODY_BUDGET.max_requests}"

#: Cold-start window only: with no prior table there is no watermark, and
#: thirty days back covers a daily cron with a wide margin for an outage.
DEFAULT_WINDOW_DAYS = 30

#: Re-ask this far back from the stored watermark, so a package modified after
#: the previous run's cutoff is still seen.
OVERLAP_HOURS = 24

#: ~3 requests each at ~3/s: 200 packages is ~3.5 minutes per collection.
MAX_PACKAGES_PER_RUN = 200

MAX_PAGES = 40


def _since(prior_file: Path | None = None, window_days: int = DEFAULT_WINDOW_DAYS,
           *, evidence: CaptureEvidence | None = None, now: datetime | None = None) -> str:
    """Last-modified floor from the selected prior's successful discovery, never imported rows.

    ``prior_file`` is retained for callers; its historical contents cannot establish
    a live cursor. Without qualified discovery state the declared cold window applies.
    """
    raw = os.environ.get("COMMITTEE_REPORTS_SINCE", "").strip()
    if raw:
        return raw if raw.endswith("Z") else f"{raw}T00:00:00Z"
    state = (evidence.inherited_event("committee-discovery") or {}) if evidence is not None else {}
    cutoff = datetime.fromisoformat(state["cursor"]) if state.get("cursor") else None
    start = (cutoff - timedelta(hours=OVERLAP_HOURS)) if cutoff else (now or datetime.now(UTC)) - timedelta(days=window_days)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ")


def _package_ids(reader: PackageDiscoverySource, collection: str, since: str,
                 evidence: CaptureEvidence | None = None, *, until: str | None = None) -> dict[str, str | None]:
    """Enumerate within the page cap; use the package grammar before spending body requests."""
    found = {}
    unsupported = 0
    for page in reader.packages(collection_url(collection, since, until), max_pages=MAX_PAGES):
        if evidence:
            evidence.capture(page.capture, stage=collection + ":listing")
        for record in page.records:
            package_id = record.get("packageId")
            try:
                identity = parse_package_id(package_id)
            except GovInfoBodySourceError:
                unsupported += 1
                continue
            if identity.collection == collection:
                found[str(package_id)] = text(record.get("lastModified"))
    logger.info("{}: {} supported packages in window, {} unsupported IDs", collection, len(found), unsupported)
    return found


#: The ``publisher_body_status`` a read acts on: the text rendition is the
#: publisher's own notice that the text is only in the PDF.
PLACEHOLDER = "publisher_placeholder"


class ReadBody(NamedTuple):
    """One fetched body, its one text derivation, and what that text states.

    ``completeness`` (``publisher_body_status``) and ``text_sha256`` each scan
    the whole text, so they are taken once here, not by every consumer.
    """

    body: Any
    derived: BodyText
    completeness: str
    text_sha256: str


def _read(body: Any) -> ReadBody:
    """The text derivation for one fetched body's own rendition, with its status and digest."""
    # No extractor argument: ``body_text``'s default (``DocumentExtractor(NativeText())``,
    # PyMuPDF) is the pipeline ``gpo_normalize`` was derived on — the GPO
    # gutter-numbered layout only comes through on PyMuPDF's own line
    # adjacency, not pypdf's (measured
    # ``docs/research/gpo-normalizer-vs-upstream-2026-09-19.md`` in spicy-docs).
    derived = body_text(body)
    return ReadBody(body, derived, publisher_body_status(derived.text, rendition=derived.rendition),
                    "sha256:" + hashlib.sha256(derived.text.encode("utf-8")).hexdigest())


def _part_key(body: Any, package_id: str) -> str:
    """The part a body is: its report part's id, or the package id for a hearing, whose record states none."""
    return package_id if body.part is None else body.part.part_id


def _retain(evidence: CaptureEvidence, bodies: Collection[Any], stage: str) -> None:
    """Retain one acquisition's summary and MODS once, then each body; every part repeats the first two."""
    first = next(iter(bodies))
    for capture in (first.summary_capture, first.mods_capture, *(body.body_capture for body in bodies)):
        evidence.capture(capture, stage=stage)


@cache
def _extraction_versions() -> dict[str, str]:
    """What a PDF's text is re-derived with from its retained bytes; read only once a PDF was extracted."""
    return {name: version(name) for name in ("spicy-docs", "pymupdf")}


def _read_bodies(acquirer: PackageBodySource, package_id: str, collection: str,
                 evidence: CaptureEvidence | None = None) -> list[ReadBody]:
    """Each fetched body of one package with the one text derivation for its rendition.

    A report is every part its record states, read by ``acquire_parts``: one
    summary and one MODS, then one body per part, all or nothing, so a
    package's part rows are never half replaced. A hearing's record states no
    parts, so it is one ``acquire``. Takes the narrow Protocol rather than the
    concrete acquirer, the way the bill family's ``_version_captures`` does:
    what this transform depends on is the facts ``body_text`` reads off a
    fetched body, and the rendition it was fetched in is the body's own, never
    this caller's guess.

    A part whose text is the publisher's placeholder is read again from the
    PDF its record offers, under that PDF's own digest; the placeholder's
    capture is kept beside it. A part offering no PDF keeps its placeholder,
    marked, since nothing more is published, and so does one whose PDF every
    later run would be refused in the same way (:func:`_refusal_is_final`),
    so the package is not re-read forever; a transient failure refuses it. The journal records digests and
    counts, never text: the bytes are retained, and a PDF's text is re-derived
    from them with the recorded versions.
    """
    bodies = acquirer.acquire_parts(package_id) if collection == "CRPT" else (acquirer.acquire(package_id),)
    if evidence:
        _retain(evidence, bodies, package_id + ":used")
    stamps = {body.summary.last_modified for body in bodies}
    if len(stamps) != 1:
        raise ValueError(f"{package_id}: report parts disagree on publisher last_modified")
    revision_instant(next(iter(stamps)))
    reads = [_read(body) for body in bodies]
    keys = [_part_key(read.body, package_id) for read in reads]
    placeholders = {key: read for key, read in zip(keys, reads, strict=True) if read.completeness == PLACEHOLDER}
    offered = [key for key, read in placeholders.items()
               if read.body.format != "pdf" and "pdf" in read.body.offered_formats]
    if evidence:
        for key, read in placeholders.items():
            evidence.event("body-placeholder", package_id=package_id, part_id=key,
                           source_sha256=read.body.body_capture.sha256, text_sha256=read.text_sha256,
                           text_chars=len(read.derived.text), offered_formats=list(read.body.offered_formats))
    if offered:
        # A report's parts are read all or none, so every part is asked for as
        # PDF and only the placeholder parts take theirs. A failure refuses the
        # package, which stays pending until a read succeeds.
        try:
            pdfs = (acquirer.acquire_parts(package_id, prefer=("pdf",)) if collection == "CRPT"
                    else (acquirer.acquire(package_id, prefer=("pdf",)),))
        except GovInfoBodySourceError as error:
            if not _refusal_is_final(error):
                raise
            logger.warning("{}: offered PDF refused for good; placeholder kept for {}: {}", package_id, offered, error)
            if evidence is not None:
                evidence.refusal(error, stage=package_id + ":pdf-fallback")
            return reads
        by_part = {_part_key(body, package_id): body for body in pdfs}
        if missing := [key for key in offered if key not in by_part]:
            raise ValueError(f"PDF acquisition did not return placeholder parts {missing}")
        if evidence:
            _retain(evidence, [by_part[key] for key in offered], package_id + ":pdf-fallback")
        reads = [_read(by_part[key]) if key in offered else read for key, read in zip(keys, reads, strict=True)]
        if any(read.body.summary.last_modified not in stamps for read in reads):
            raise ValueError(f"{package_id}: publisher revision changed during PDF fallback")
    if evidence:
        for key, read in zip(keys, reads, strict=True):
            if read.derived.pages is not None:
                replaced = placeholders[key].body.body_capture.sha256 if key in offered else None
                evidence.event("body-text", package_id=package_id, part_id=key,
                               source_sha256=read.body.body_capture.sha256, replaces_sha256=replaced,
                               text_sha256=read.text_sha256, text_chars=len(read.derived.text),
                               page_count=len(read.derived.pages), derivation=read.derived.derivation,
                               extraction_versions=_extraction_versions(), cleanup=asdict(read.derived.record),
                               body_completeness=read.completeness)
    return reads


def _refusal_is_final(error: Exception) -> bool:
    """Whether a later run would get the same refusal: the publisher's record and this run's bounds decide it.

    A rendition a part does not offer, a report whose parts exceed the request
    budget, a record stating its parts in a shape the reader refuses
    (CRPT-119hrpt853 states two parts as part 1), and a body past the byte bound
    (the transport's ``response-byte-limit`` refusal) repeat until the publisher
    changes the package, which moves its ``last_modified``, the reader changes,
    which moves the rule, or the bound grows past it (:func:`_refusal_basis`).
    Anything else may be transient.
    """
    if isinstance(error, (GovInfoFormatNotOfferedError, GovInfoPartsOverBudgetError, GovInfoPartsRefusedError)):
        return True
    refused = getattr(error, "refused_response", None)
    return isinstance(refused, RefusedResponse) and refused.unavailable_reason == "response-byte-limit"


def _refusal_basis(error: Exception) -> str:
    """What a final refusal rests on: the size its response stated, else the bounds that refused it.

    A body past the byte bound states its ``Content-Length`` (the 1946 parts'
    312,487,940 and 373,504,330 bytes), so it is final on that size against the
    bound, not on the bound's value; a record stating too many parts or parts the
    reader refuses, no rendition taken, or an unstated length rests on :data:`REFUSAL_BOUNDS`.
    """
    refused = getattr(error, "refused_response", None)
    if isinstance(refused, RefusedResponse) and refused.stated_byte_size is not None:
        return f"{SIZE_BASIS}{refused.stated_byte_size}"
    return REFUSAL_BOUNDS


def _settled(row: dict, collection: str, modified: str | None = None) -> bool:
    return settled(row, collection, modified, bounds=REFUSAL_BOUNDS, max_body_bytes=BODY_BUDGET.max_body_bytes)


def _section_input(derived: BodyText) -> str | tuple[PageResult, ...]:
    """What ``parse_agency_blocks`` reads: the pages, where the rendition states them, so a section cites its pages.

    ``BodyText.text`` is its pages joined by one ``\\n``, the join
    ``parse_agency_blocks`` makes, so a section's offsets are the same either
    way. Page ``N`` is the PDF's ``N``-th page, as extraction numbers it.
    """
    if derived.pages is None:
        return derived.text
    return tuple(PageResult({"page": number}, PageContent((TextBlock(page),), ()))
                 for number, page in enumerate(derived.pages, start=1))


#: The one MODS ``context`` that states a package is *about* a bill, as
#: opposed to mentioning it (``OTHER``, ``COVER``, ``BODY``, or none). The
#: rule itself is ``PackageModsIdentity.primary_bill``'s; this names what the
#: mention count leaves out.
PRIMARY_BILL_CONTEXT = "PRIMARY"


def _bill_key(package_id: str, bill: ModsBill) -> str | None:
    """The bill's natural key, or None when the publisher's type is outside the vocabulary.

    ``normalized_bill_type`` is spicy-docs' own lower-casing of the MODS
    ``type``, checked against ``BILL_TYPES``; a ``<bill>`` spelled in a type
    that vocabulary lacks is logged and not linked, rather than published as a
    key with a hole in it.
    """
    if bill.normalized_bill_type is None:
        logger.warning("{}: MODS bill type {!r} is not a supported bill type; not linked", package_id, bill.bill_type)
        return None
    # ``int`` on purpose: the wheel keeps the publisher's digits verbatim
    # (``isdecimal()`` is guaranteed), while ``congress_bills.bill_id`` is
    # spelled from an int, so a zero-padded MODS number must collapse to the
    # key it can join on.
    return natural_key(bill.congress, bill.normalized_bill_type, int(bill.number))


#: What one hearing-detail request can refuse with, short of a credential
#: refusal, which propagates: the reader's own refusals (``404`` included), a
#: transport failure after its retries, and a record not shaped as expected.
_HEARING_REFUSALS = (PagedJsonSourceError, httpx.HTTPError, ConnectionError, ValueError, TypeError, KeyError)


#: The chambers a hearing-detail request tries, in order, by GovInfo document
#: type. Congress.gov files some joint hearings under a chamber: GovInfo's
#: CHRG-119jhrg61392 (Joint Economic Committee) answers 404 under ``joint`` and
#: 200 under ``senate`` (probed 2026-11-09), while CHRG-119jhrg60491 answers
#: under ``joint``. Any other type asks its own chamber only.
_DETAIL_CHAMBERS = {"jhrg": ("joint", "senate", "house")}


def _event_id(hearings: HearingDetailSource, package: Any,
              evidence: CaptureEvidence | None = None) -> tuple[str | None, str]:
    """``(event_id, outcome)`` for one CHRG package from its Congress.gov hearing detail.

    ``outcome`` is ``meeting``, ``no_meeting``, ``unavailable`` or ``refused``,
    so the run log can count them apart. ``unavailable`` is Congress.gov's
    404/410 for the exact jacket on every chamber route the document type
    names (:data:`_DETAIL_CHAMBERS`); the caller decides from the package's age
    whether that is final (:func:`_unavailable_rule`). ``refused`` is anything
    else. The detail must name the jacket and Congress the package id spells,
    or it is refused rather than read.
    """
    identity = package.identity
    # The hearing detail route wants the chamber's word; spicy-docs maps each document type to it.
    chamber = CHAMBER_BY_DOCUMENT_TYPE.get(str(identity.document_type))
    route = LIST_ROUTES["hearing-detail"]
    chambers = _DETAIL_CHAMBERS.get(str(identity.document_type), (chamber,))
    try:
        if chamber is None:
            raise ValueError(f"document type {identity.document_type!r} names no chamber")
        for index, each in enumerate(chambers):
            url = list_route_url(route, congress=identity.congress, chamber=each, number=int(identity.number), limit=1)
            try:
                page = next(iter(hearings.records(route, url, max_pages=1)))
                break
            except PagedJsonUnavailableError:
                if index == len(chambers) - 1:
                    raise
        if evidence:
            evidence.capture(page.capture, stage=identity.package_id + ":hearing-detail")
        # spicy-docs reads the answer as the hearing asked for (one record, its jacket and Congress) (X5).
        event_id = hearing_detail_event_id(page.records, congress=identity.congress, jacket=identity.number)
    except CredentialRefusedError:
        raise
    except _HEARING_REFUSALS as error:
        if evidence:
            evidence.refusal(error, stage=identity.package_id + ":hearing-detail")
        outcome = "unavailable" if isinstance(error, PagedJsonUnavailableError) else "refused"
        logger.warning("CHRG: {} hearing detail {}: {}", identity.package_id, outcome,
                       scrub_credential(str(error), evidence.credential if evidence else ""))
        return None, outcome
    return event_id, "meeting" if event_id is not None else "no_meeting"


#: Which detail answer a checkpoint keeps when a package's bodies disagree (higher wins).
_DETAIL_RANK = {
    refusal_rule("CHRG", DETAIL_UNAVAILABLE): 0,
    refusal_rule("CHRG", DETAIL_AWAITED): 1,
    RULE_VERSIONS["CHRG"]: 2,
}


def _today() -> date:
    """The run's UTC date; the one clock the hearing-detail grace window reads."""
    return datetime.now(UTC).date()


def _unavailable_rule(package: Any, collection: str) -> str:
    """The checkpoint rule for a hearing whose detail Congress.gov does not have: awaited, then settled.

    Age runs from the package's GovInfo ``dateIssued`` (the summary's), else
    the date of its ``last_modified``. Within :data:`DETAIL_GRACE_DAYS` it is
    asked again every run but is not pending; past it, it is settled.
    """
    stamp = package.summary.date_issued or package.summary.last_modified or ""
    try:
        issued = date.fromisoformat(stamp[:10])
    except ValueError:
        issued = None
    within = issued is not None and (_today() - issued).days <= DETAIL_GRACE_DAYS
    return refusal_rule(collection, DETAIL_AWAITED if within else DETAIL_UNAVAILABLE)


def _retry_order(row: dict, modified: str | None) -> tuple[bool, str]:
    """Where a package goes in the cap: never attempted at this stamp first, then retries, oldest attempt first.

    A fixed order let the same first 200 unsettled hearings take the cap every
    night (run 42558022791). A package new to the listing, changed since its
    checkpoint, or deferred by the cap last run (its checkpoint still
    ``pending``) has not been attempted at this stamp; a refusal has, at its
    ``observed_at``, and goes behind every older one. Ties keep their order.
    """
    retry = row.get("outcome") not in (None, "pending") and row.get("last_modified") == modified
    return retry, (row.get("observed_at") or "") if retry else ""


def _refuse_unsafe_reread(reread: Collection[str], checkpointed: set[str], reads: dict[str, dict]) -> None:
    """Refuse, before any source request, a named re-read that would publish state no read established.

    An id without a prior checkpoint (a typo, or no prior at all) would publish
    a refused checkpoint that every scheduled run retries; a prior row without
    one would publish the pending checkpoint :func:`prior_reads` invents for it,
    with no clock, since only discovery reads it.
    """
    if unknown := sorted(set(reread) - checkpointed):
        raise ValueError(f"reread names packages with no prior checkpoint: {unknown}")
    if unchecked := sorted(set(reads) - checkpointed):
        raise ValueError(f"reread needs every prior row checkpointed; run discovery first: {unchecked}")


def _link_committee_codes(links: Path, transcripts: Path) -> int:
    """Give each link its hearing's first committee code as ``hearing_transcripts`` states it; how many moved.

    spicy-docs' link rule publishes the MODS ``authorityId`` as the record spells it (CHRG-119hhrg61487 states
    ``HSAS00``), while its transcript shaper lower-cases each code the way ``committees.system_code`` keys it, root
    first and then each granule. Taking the shaper's first code keeps one statement of a hearing's committee and
    no second copy of its fold, on every merged row, so a held link needs no re-read. A link whose hearing row
    predates ``committee_system_codes_json`` (NULL) keeps its own code. O(links + hearings).
    """
    import pyarrow.parquet as pq

    stated = pq.read_table(transcripts, columns=["package_id", "last_modified", "committee_system_codes_json"]).to_pylist()
    first = {(row["package_id"], row["last_modified"]): next(iter(json.loads(row["committee_system_codes_json"])), None)
             for row in stated if row["committee_system_codes_json"] is not None}
    table = pq.read_table(links)
    held = table["committee_system_code"].to_pylist()
    codes = [first.get((package_id, modified), code) for package_id, modified, code in zip(
        table["package_id"].to_pylist(), table["last_modified"].to_pylist(), held)]
    moved = sum(new != old for new, old in zip(codes, held))
    if moved:
        write_in_place(set_column(table, "committee_system_code", codes), links)
    return moved


def build_committee_reports(
    output_dir: Path,
    *,
    reader: PackageDiscoverySource | None = None,
    acquirer: PackageBodySource | None = None,
    hearings: HearingDetailSource | None = None,
    max_packages: int = MAX_PACKAGES_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
    evidence: CaptureEvidence | None = None,
    reread: Collection[str] = (),
    coverage: dict | None = None,
) -> tuple[Path, ...]:
    """Build five contract tables and the acquisition checkpoint, with one owner.

    ``reread`` names hearings to read again whatever their checkpoints say, and
    nothing else: discovery is skipped, so every other row and checkpoint is
    carried from the prior unchanged. It re-establishes the named hearings'
    capture evidence without moving the rest of the family, and refuses
    (see :func:`_refuse_unsafe_reread`) rather than publish a state only a
    discovery run can settle.
    """
    if outside := sorted(key for key in reread if not key.startswith("CHRG-")):
        # A report's re-read can move the ``committee_reports`` watermark past
        # packages changed since the last listing, which this run never asks for.
        raise ValueError(f"reread takes hearings only; a report would move the discovery watermark: {outside}")
    if reader is None or acquirer is None or hearings is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Committee reports need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = reader or (RetainedGovInfoDiscoveryReader(budget=DISCOVERY_BUDGET, api_key=api_key, evidence=evidence)
                            if evidence else GovInfoDiscoveryReader(budget=DISCOVERY_BUDGET, api_key=api_key))
        if evidence:
            evidence.credential = api_key
        acquirer = acquirer or (RetainedGovInfoBodyAcquirer(budget=BODY_BUDGET, api_key=api_key, evidence=evidence)
                               if evidence else GovInfoBodyAcquirer(budget=BODY_BUDGET, api_key=api_key))
        hearings = hearings or listing_reader(api_key, evidence=evidence)

    prior_files = {
        name: published_table(output_dir, name, download_prior) for name in ("committee_reports", "hearing_transcripts")
    }
    checkpoint = published_table(output_dir, READS_TABLE, download_prior)
    reads = prior_reads(checkpoint, prior_files)
    previous_reads = dict(reads)
    if reread:
        _refuse_unsafe_reread(reread, set(prior_reads(checkpoint, {})), reads)
    discovered_at = datetime.now(UTC)
    since = _since(evidence=evidence, now=discovered_at)
    raw_until = os.environ.get("COMMITTEE_REPORTS_UNTIL", "").strip()
    until = (raw_until if raw_until.endswith("Z") else f"{raw_until}T00:00:00Z") if raw_until else None
    logger.info("Committee reports: {}; cap {} per collection; agenda cap 0",
                f"re-reading only {sorted(reread)}" if reread else f"modified since {since}", max_packages)
    observed_at = datetime.now(UTC).isoformat()
    if evidence:
        evidence.event("selection", since=None if reread else since, reread=sorted(reread),
                       max_packages_per_collection=max_packages, max_pages=MAX_PAGES,
                       agenda_cap=0, checkpoint_observed_at=observed_at)
    link_rows: list[dict] = []
    evaluated_hearings: set[tuple[str, str]] = set()
    evaluated_reports: set[tuple[str, str]] = set()
    report_rows: list[dict] = []
    letter_rows: list[dict] = []
    section_rows: list[dict] = []
    hearing_rows: list[dict] = []
    renditions: Counter[str] = Counter()
    completeness: Counter[str] = Counter()
    mentions: Counter[str] = Counter()
    meetings: Counter[str] = Counter()
    refused = unchanged = linked = deferred = listed_count = 0

    for collection in ("CRPT", "CHRG"):
        if reread:
            listed = {}
            pending = {key: reads.get(key, {}).get("last_modified") for key in reread
                       if key.startswith(collection + "-")}
        else:
            try:
                listed = _package_ids(reader, collection, since, evidence, until=until)
            except Exception as error:
                if evidence:
                    evidence.refusal(error, stage=collection + ":listing")
                raise
            pending = {key: row.get("last_modified") for key, row in reads.items()
                       if key.startswith(collection + "-") and not _settled(row, collection)}
            pending.update({key: modified for key, modified in listed.items()
                            if not _settled(reads.get(key, {}), collection, modified)})
            pending = dict(sorted(pending.items(), key=lambda item: _retry_order(reads.get(item[0], {}), item[1])))
        listed_count += len(listed)
        deferred += max(0, len(pending) - max_packages)
        unchanged += len(listed) - sum(key in pending for key in listed)
        if evidence:
            evidence.event("package-selection", collection=collection, listed=listed,
                           selected=list(pending)[:max_packages], deferred=list(pending)[max_packages:],
                           unchanged=[key for key in listed if key not in pending],
                           refused_final=sorted(key for key, row in reads.items() if key.startswith(collection + "-")
                                                and row.get("outcome") == REFUSED_FINAL and key not in pending))
        for package_id, modified in pending.items():
            reads[package_id] = {"package_id": package_id, "last_modified": modified,
                                 "outcome": "pending", "rule_version": RULE_VERSIONS[collection],
                                 "observed_at": observed_at}
        logger.info("{}: {} pending; {} deferred by package cap", collection, len(pending), max(0, len(pending) - max_packages))
        for package_id in list(pending)[:max_packages]:
            state = reads[package_id]
            try:
                bodies = _read_bodies(acquirer, package_id, collection, evidence)
            except (CredentialRefusedError, SourceEvidenceError):
                raise
            except Exception as error:  # noqa: BLE001 — retained for the next run
                if evidence:
                    evidence.refusal(error, stage=package_id)
                refused += 1
                if _refusal_is_final(error):
                    # Not asked again until its stamp, the rule or the bounds move.
                    state.update(outcome=REFUSED_FINAL, rule_version=refusal_rule(collection, _refusal_basis(error)))
                else:
                    state["outcome"] = "refused"
                logger.warning("{}: {} {}: {}", collection, package_id, state["outcome"].replace("_", " "),
                               scrub_credential(str(error), evidence.credential if evidence else ""))
                if evidence:
                    evidence.event("package-outcome", **state)
                continue
            modified = bodies[0].body.summary.last_modified
            # A placeholder whose part offers no PDF is complete: nothing more
            # is published, its row says so, and a new stamp or rule re-reads it.
            state.update(last_modified=modified, outcome="complete")
            for body, derived, status, text_sha256 in bodies:
                renditions[derived.rendition] += 1
                completeness[status] += 1
                # A report part states its own bills; a multi-part package's
                # root states none (CRPT-119hrpt455, -119hrpt494). A hearing
                # has no part, so its bills are the package's.
                stated = body.part if body.part is not None else body.mods
                primary = stated.primary_bill
                bill = None if primary is None else _bill_key(package_id, primary)
                linked += bill is not None
                mentions.update(entry.context for entry in stated.bills if entry.context != PRIMARY_BILL_CONTEXT)
                common = {"page_count": None if derived.pages is None else len(derived.pages),
                          "text_sha256": text_sha256, "body_completeness": status,
                          "text_derivation": derived.derivation}
                if collection == "CHRG":
                    event_id, outcome = _event_id(hearings, body, evidence)
                    meetings[outcome] += 1
                    # A package's bodies may answer differently: a retried refusal
                    # outranks a detail still awaited, which outranks a settled one.
                    rule = (RULE_VERSIONS[collection] if outcome == "refused"
                            else _unavailable_rule(body, collection) if outcome == "unavailable" else None)
                    held = _DETAIL_RANK[state["rule_version"]] if state["outcome"] == DETAIL_REFUSED else -1
                    if rule is not None and _DETAIL_RANK[rule] > held:
                        state.update(outcome=DETAIL_REFUSED, rule_version=rule)
                    hearing_rows.append(shape_hearing_transcript(body, event_id=event_id, **common))
                    link_rows.extend(shape_hearing_bill_link(link, last_modified=modified)
                                     for link in cover_links(body.mods, event_id=event_id))
                else:
                    estimate = read_cbo_estimate(derived.text)
                    report_rows.append(shape_committee_report(
                        body, bill_id=bill, estimate=estimate,
                        recital_bill_id=recital_bill_id(estimate, body.identity.congress), **common))
                    letter_rows.extend(shape_committee_report_cbo_letter(
                        letter, package_id=package_id, part_id=body.part.part_id, seq=seq, last_modified=modified
                    ) for seq, letter in enumerate(estimate.letters))
                    section_rows.extend(shape_report_section(
                        block, package_id=package_id, part_id=body.part.part_id, seq=seq, last_modified=modified
                    ) for seq, block in enumerate(parse_agency_blocks(_section_input(derived))))
            (evaluated_hearings if collection == "CHRG" else evaluated_reports).add((package_id, modified))
            if evidence:
                evidence.event("package-outcome", **state)

    completed_hearings = {key for key, _modified in evaluated_hearings}
    if unfinished := sorted(key for key in reread if key not in completed_hearings or reads[key]["outcome"] != "complete"):
        # A refused named read would publish a checkpoint every later run
        # retries, and a refused hearing detail would drop the prior event_id.
        raise RuntimeError(f"reread did not complete, so nothing is merged: {unfinished}")
    logger.info(
        "Committee reports: {:,} reports, {:,} CBO letters, {:,} sections, {:,} hearings, {:,} already held, {:,} refused",
        len(report_rows),
        len(letter_rows),
        len(section_rows),
        len(hearing_rows),
        unchanged,
        refused,
    )
    logger.info(
        "Committee reports: {:,} report parts and hearings name a PRIMARY bill; other mentions by MODS context — {}",
        linked,
        dict(mentions),
    )
    if meetings:
        # The three outcomes of the hearing-detail read, kept apart: a NULL
        # event_id is either a detail that names no meeting or one not read.
        logger.info("Committee reports: hearing details by outcome — {}", dict(meetings))
    if renditions:
        # The run's tally of what each row states in format, text_derivation
        # and body_completeness: a placeholder kept for want of a PDF shows here.
        logger.info("Committee reports: renditions read — {}; body completeness — {}",
                    dict(renditions), dict(completeness))
    logger.info("Committee reports: {} cover links; agenda deferred (no verified meeting-to-jacket join)", len(link_rows))
    # Replace only the successfully read package revision. A removed part or
    # child stays in its earlier revision; failed and unread scopes stay whole.
    # Legacy report rows predate part_id and represent the package's one part.
    # Keep that established backfill alongside the publisher revision key.
    replaced = {"committee_reports": evaluated_reports, "report_sections": evaluated_reports,
                "committee_report_cbo_letters": evaluated_reports,
                "hearing_bill_links": evaluated_hearings, "hearing_transcripts": evaluated_hearings}
    paths = {name: merge_contract_table(output_dir, name, rows, download_prior=download_prior,
                                        replace_parents=(("package_id", "last_modified"), replaced[name]),
                                        backfill_prior={"part_id": "package_id"} if name in
                                        ("committee_reports", "report_sections", "committee_report_cbo_letters") else None,
                                        prior_present=(prior_files[name] is not None) if name in prior_files else None)
             for name, rows in (("committee_reports", report_rows), ("report_sections", section_rows),
                                ("hearing_transcripts", hearing_rows), ("hearing_bill_links", link_rows),
                                ("committee_report_cbo_letters", letter_rows))}
    relinked = _link_committee_codes(paths["hearing_bill_links"], paths["hearing_transcripts"])
    logger.info("Committee reports: {:,} hearing links take their hearing's committee code", relinked)
    reads = {key: latest_read(previous_reads.get(key, {}), row) for key, row in reads.items()}
    read_path = merge_table(output_dir, name=READS_TABLE, columns=READ_COLUMNS,
                                         identity=("package_id",), version_column="observed_at", rows=reads.values(),
                                         remote_key=f"{READS_TABLE}.parquet", download_prior=download_prior,
                                         prior_present=checkpoint is not None)
    waiting = sorted(key for key, row in reads.items() if awaited(row, key.split("-", 1)[0]))
    if waiting:
        annotate("warning", "Committee reports: hearing details awaited",
                 f"{len(waiting)} hearing(s) not yet on Congress.gov, asked again until {DETAIL_GRACE_DAYS} days "
                 f"after GovInfo issued them: {', '.join(waiting[:20])}{' ...' if len(waiting) > 20 else ''}")
    status = {"discovery": "not_requested" if reread else "complete", "since": None if reread else since,
              "until": until, "listed": listed_count, "unchanged": unchanged, "refused": refused, "deferred": deferred,
              "pending": sum(not _settled(row, key.split("-", 1)[0]) and not awaited(row, key.split("-", 1)[0])
                             for key, row in reads.items()),
              "awaiting_detail": len(waiting),
              "table_generation": "prepared", "atomic_snapshot": False, "whole_history_complete": False}
    if coverage is not None:
        coverage.update(status)
    if evidence is not None:
        evidence.event("acquisition-coverage", **status)
        state = evidence.inherited_event("committee-discovery") or {}
        explicit = bool(os.environ.get("COMMITTEE_REPORTS_SINCE", "").strip()) or until is not None
        evidence.event("committee-discovery",
                       cursor=discovered_at.isoformat() if not reread and not explicit else state.get("cursor"),
                       since=status["since"], mode="reread" if reread else "explicit-window" if explicit else "refresh")
    return (*paths.values(), read_path)
