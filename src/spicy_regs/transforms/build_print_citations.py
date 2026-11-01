"""Build activity reports, budget volumes, bill actions and citations in one pass.

Publishes ``house_activity_reports``, ``budget_volumes``,
``bill_committee_actions`` and ``document_citations`` (in that order) from
GovInfo's CRPT and BUDGET collections. SpicyDocs owns selection, rendition
preference, interpretation and row shapes; this host owns the issue-date
window, caps, prior-output resume and publication.

The whole published window is enumerated each run so unsuccessful packages
remain eligible, and held packages are revisited when a reading input changes
(:func:`_processing_versions`); an unchanged source with successful matching
checkpoints across all its outputs costs no body requests. Outstanding packages
are read most-needed first (:func:`_queue`), and collections share the package
cap round-robin so neither starves on a cold start. Print families read the
package's PDF-first order because their columns state pages; bodies are read whole and
``read_depth`` reports the publisher's stated extent against pages actually
extracted. The two keyless chamber rosters supply the committee vocabulary — a
roster failure leaves its names unresolved, while access refusals abort.
Senate expenditure granules use a separate acquisition pass
(:mod:`spicy_regs.transforms.build_senate_expenditures`).
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple, Protocol

import httpx
from loguru import logger
from spicy_docs.extraction import BODY_TEXT_DERIVATION_VERSION
from spicy_docs.extraction.body_text import BodyText, body_text
from spicy_docs.interpretation.bill_actions import (
    PRINT_ACTION_RULE_SET_VERSION,
    PRINT_ACTION_VOCABULARY_VERSION,
    find_bill_actions,
)
from spicy_docs.interpretation.citations import (
    CITATION_RULE_SET_VERSION,
    CITATION_RULES,
    CITATION_RULES_BY_NAME,
    COMMITTEE_CHAMBERS,
    committee_vocabulary,
    find_citations,
)
from spicy_docs.reading.paged_json import PagedJsonBudget, PagedJsonSourceError
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.schemas.bill_action_tables import shape_bill_committee_action
from spicy_docs.schemas.budget_volume_tables import (
    BUDGET_VOLUME,
    budget_index_stated_keys,
    shape_budget_volume,
)
from spicy_docs.schemas.committee_report_tables import CHAMBER_BY_DOCUMENT_TYPE
from spicy_docs.schemas.document_citation_tables import (
    GOVINFO_PACKAGE,
    activity_report_stated_keys,
    document_provenance,
    shape_activity_report,
    shape_document_citation,
)
from spicy_docs.sources.congress.committee_rosters import (
    CommitteeRosterAcquirer,
    CommitteeRosterBudget,
    CommitteeRosterError,
)
from spicy_docs.sources.govinfo.activity_reports import (
    ACTIVITY_REPORT_RULE_VERSION,
    COVERED_CONGRESS_RULE_VERSION,
    covered_congress,
    is_activity_report,
    names_activity,
)
from spicy_docs.sources.govinfo.bodies import MEASURED_BUDGET_PARTS, PRINT_BODY_PREFERENCE, parse_package_id
from spicy_docs.sources.govinfo.body_acquisition import (
    GovInfoBodyAcquirer,
    GovInfoBodyBudget,
    GovInfoFormatNotOfferedError,
)
from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader, published_url
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import current_congress
from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints
from spicy_regs.transforms.table_merge import merge_contract_table, published_table

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

#: The four tables this transform publishes, in the order ``build`` returns
#: them. ``document_citations`` is last because it is fed by both families.
ACTIVITY_REPORTS = "house_activity_reports"
BUDGET_VOLUMES = "budget_volumes"
BILL_ACTIONS = "bill_committee_actions"
CITATIONS = "document_citations"
CHECKPOINT_NAMESPACE = "print-citations"

#: The two collections, with the package-id prefix each row must actually
#: carry. The prefix check is not redundant with the request: a collection
#: walk serves neighbouring-collection rows (3 of 3,000 CRPT-scoped ids,
#: spicy-docs 2026-09-19).
CRPT = "CRPT"
BUDGET = "BUDGET"

#: The tables one package's read writes, its parent first. A parent checkpoint
#: alone can overtake failed child publication, so every table a read affects
#: carries the same successful-read checkpoint.
_OUTPUTS: dict[str, tuple[str, ...]] = {
    CRPT: (ACTIVITY_REPORTS, BILL_ACTIONS, CITATIONS),
    BUDGET: (BUDGET_VOLUMES, CITATIONS),
}

#: The issue-date floor a cold run walks from. **This is a scope, not a
#: coverage claim.** GovInfo's CRPT and CDOC collections reach 1817, and
#: whether the activity-report title rule finds anything before the 118th is
#: unmeasured — spicy-docs' own register calls one early-window ``published``
#: walk "the cheapest unanswered question in the document", and no CRPT
#: artifact outside the 118th and 119th is retained anywhere in that corpus.
#: 2023-01-01 reaches the 118th Congress's activity reports (issued January
#: 2025) and the FY2026/FY2027 budget volumes. Widen it with
#: ``PRINT_CITATIONS_SINCE`` and the walk goes back as far as it is pointed;
#: the answer is then a measurement someone made rather than a default.
DEFAULT_ISSUE_FLOOR = "2023-01-01"

#: Packages fetched per run, across both collections. Two keyed requests each
#: (summary and MODS; the body is keyless), so 40 packages is ~80 keyed plus a
#: handful of list pages — comfortably inside the share of the 4,000-per-hour
#: GovInfo ceiling left by the 3,680 worst hour D1 measured across the existing
#: rollups, and this rollup's cron shares an hour with none of them. It is a
#: catch-up bound, not a steady-state one: both families together publish
#: fewer than 30 packages a Congress.
MAX_PACKAGES_PER_RUN = 40

#: List pages per collection walk. At 1,000 rows a page this clears every
#: window either collection can produce (291 CDOC rows and 71 CRPT rows in the
#: measured windows) with room for a decade of backfill.
MAX_PAGES = 20

DISCOVERY_BUDGET = PagedJsonBudget(
    max_requests=500,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: Three requests per package (summary, MODS, body), paced. ``max_body_bytes``
#: is 24 MiB because the Budget Appendix is 19.4 MB — the largest body either
#: family publishes — and a volume above the bound is refused whole rather than
#: read short, then retried next run.
BODY_BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=24 * 1024 * 1024,
    max_metadata_bytes=8 * 1024 * 1024,
    timeout_seconds=300.0,
    min_request_interval_seconds=0.34,
)

#: The two keyless roster files. ``max_bytes`` matches what
#: ``build_committee_rosters`` allows the larger of the two.
ROSTER_BUDGET = CommitteeRosterBudget(
    max_requests=6,
    max_bytes=4 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

def _report_chamber(package_id: str) -> str | None:
    """Whose committee wrote a CRPT activity report: ``house`` or ``senate``, or ``None`` when its id states neither.

    Read off the package id's own document-type code through spicy-docs'
    ``CHAMBER_BY_DOCUMENT_TYPE`` (``hrpt`` House; ``srpt`` and ``erpt``
    Senate), never assumed: until 2026-09-26 it was the constant ``"house"``,
    while the title rule admits Senate reports (the window holds 14). The
    chamber decides two things -- which chamber's committee a name both hold
    means when the print names no chamber before it (``Committee on the
    Judiciary`` in CRPT-118srpt11 is ``ssju00``; ``Senate Committee on ...``
    is the Senate's in any report), and
    a hearing's or markup's action code, which follows the committee that
    acted. A package whose id states neither is refused before any request.
    """
    try:
        document_type = parse_package_id(package_id).document_type
    except ValueError:
        return None
    chamber = CHAMBER_BY_DOCUMENT_TYPE.get(document_type or "")
    return chamber if chamber in COMMITTEE_CHAMBERS else None


#: What one package's acquisition and reading can refuse with, short of a
#: credential refusal, which propagates and aborts the run.
_PACKAGE_REFUSALS = (
    PagedJsonSourceError,
    httpx.HTTPError,
    ConnectionError,
    ValueError,
    TypeError,
    KeyError,
    OSError,
)


class PackageDiscoverySource(Protocol):
    """What this transform needs of a GovInfo discovery reader."""

    def packages(self, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


class PackageBodySource(Protocol):
    """What this transform needs of a GovInfo package-body acquirer.

    ``prefer`` is in the signature because this transform passes one — see
    :data:`PRINT_BODY_PREFERENCE`, the one place it names a rendition.
    """

    def acquire(self, package_id: str, *, prefer: Sequence[str] = ..., max_bytes: int | None = ...) -> Any: ...


class RosterSource(Protocol):
    """What this transform needs of the chamber roster acquirer."""

    def acquire_house(self, *, congress: int, session: int | None = ..., max_bytes: int | None = ...) -> Any: ...

    def acquire_senate(self, *, max_bytes: int | None = ...) -> Any: ...


def _issue_floor() -> str:
    """The issue date a run walks from. ``PRINT_CITATIONS_SINCE`` overrides."""
    raw = os.environ.get("PRINT_CITATIONS_SINCE", "").strip()
    return raw or DEFAULT_ISSUE_FLOOR


def _today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _now() -> str:
    """When a package was read or left unread, as its checkpoint records it; ISO 8601 UTC sorts as text."""
    return datetime.now(UTC).isoformat(timespec="microseconds")


class _Held(NamedTuple):
    """One published parent row's read state: the source timestamp, text digest and citation rule set it was read under."""

    last_modified: str | None
    text_sha256: str | None
    rule_set_version: str | None


def _held_packages(prior_file: Path | None) -> dict[str, _Held]:
    """Each published parent row's read state, including legacy rows to reread."""
    if prior_file is None:
        return {}
    import duckdb

    return {
        package_id: _Held(modified, text_sha256, rule_set_version)
        for package_id, modified, text_sha256, rule_set_version in duckdb.sql(
            f"SELECT package_id, last_modified, text_sha256, rule_set_version FROM read_parquet('{prior_file}')"
        ).fetchall()
    }


def _processing_versions(vocabularies: Mapping[str, Mapping[str, tuple[tuple[str, str], ...]]]) -> dict[str, str]:
    """Identify every input that decides a print's published rows, even when no finding exists.

    **Named inputs, not the installed SpicyDocs' whole-code digest.** Over the
    eight builds vendored through round 5 (0.53.0, printing, four FEC, laws,
    votes), all with one citation rule set, that digest took seven values and
    this key three, then digesting the derivation packages itself
    (``round5/impl-C/w52_key_across_wheels.out``). Each move of
    the digest left every held print outstanding, and with a capped queue the
    same first packages were re-read every day while 20 activity reports stayed
    at bill rule 004 (round 5, W5-2). What the digest stood for is named
    instead, so a build that moves none of it re-reads nothing:

    * the citation rule set, and the table each rule names -- ``target_table``
      is written into every citation row but is not in the rule-set digest,
      so a relabel would otherwise never reach a held row;
    * the body-text derivation: spicy-docs' ``BODY_TEXT_DERIVATION_VERSION``
      (it moves when a rendition's text can change, its digest pinned beside
      it there), PyMuPDF's release and the rendition order;
    * each output's contract columns and types, so a column a contract adds is
      filled on held prints;
    * for reports, the covered-Congress and action rules, the chamber map and
      the committee vocabulary the rosters gave this run.

    What this cannot see: a SpicyDocs change to row shaping that keeps every
    contract's columns and moves no rule version. Such a change reaches a held
    print when its publisher modifies it or one of these inputs moves; one that
    must reach every print moves a rule version. The release string is not an
    input either (a version-only release, 0.39.2 against 0.39.1, re-reads
    nothing). The parent tables' public ``rule_set_version`` keeps its
    citation-only meaning, and :func:`_queue` reads it to order the re-reads.
    """
    def contracts(collection: str) -> dict[str, Any]:
        return {name: [list(TABLE_CONTRACTS[name].columns), dict(TABLE_CONTRACTS[name].types)]
                for name in _OUTPUTS[collection]}

    common = {
        "body_text_derivation": BODY_TEXT_DERIVATION_VERSION,
        "pdf_reader_release": version("PyMuPDF"),
        "citation_rules": CITATION_RULE_SET_VERSION,
        "citation_rule_tables": sorted((rule.name, rule.target_table) for rule in CITATION_RULES),
        "body_preference": PRINT_BODY_PREFERENCE,
    }
    inputs = {
        BUDGET: common | {"contracts": contracts(BUDGET)},
        CRPT: common | {
            "contracts": contracts(CRPT),
            "covered_congress_rules": COVERED_CONGRESS_RULE_VERSION,
            "action_rules": PRINT_ACTION_RULE_SET_VERSION,
            "action_vocabulary": PRINT_ACTION_VOCABULARY_VERSION,
            "committee_chamber_by_document_type": sorted(CHAMBER_BY_DOCUMENT_TYPE.items()),
            "committee_vocabulary": {
                reading: {chamber: sorted(vocabulary) for chamber, vocabulary in by_chamber.items()}
                for reading, by_chamber in vocabularies.items()
            },
        },
    }
    return {
        collection: hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()
        for collection, values in inputs.items()
    }


def _superseded_readings(
    states: Iterable[Mapping[str, Any]], *, source_sha256: str, last_modified: str | None, current: str
) -> set[str]:
    """Earlier text digests that this read replaces because its source bytes did not change.

    ``body_version_id`` keeps a publisher's revisions apart: offsets are evidence for one text, so the
    findings on an earlier revision stay beside the current ones. A new digest from the same bytes is not a
    revision. It is this code reading the same print differently (derivation 003 moved one glyph on 41 of 42
    House activity reports and would have repeated every finding under a second digest), so the new reading
    takes the old one's place. Only the reading a checkpoint names is replaced: a digest from an
    earlier revision of the source stays.

    Bytes decide where the checkpoint recorded them. One written before ``source_sha256`` has only the
    publisher's ``lastModified``, so an equal stamp stands in for that one read. The stamp errs mostly towards
    keeping both: it moves without the bytes moving. Over 32 retained runs, 71 packages read repeatedly, no
    body digest changed at all, so a change hidden behind an equal stamp is unobserved rather than disproved.
    O(outputs).
    """
    superseded = set()
    for state in states:
        earlier = state.get("text_sha256")
        if not earlier or earlier == current:
            continue
        held_source = state.get("source_sha256")
        unchanged = (held_source == source_sha256 if held_source is not None
                     else last_modified is not None and state.get("last_modified") == last_modified)
        if unchanged:
            superseded.add(earlier)
    return superseded


def _collection(package_id: str) -> str | None:
    """The collection spicy-docs' package-id grammar places this id in, or None when it refuses the id."""
    try:
        return parse_package_id(package_id).collection
    except ValueError:
        return None


def _listed(
    reader: PackageDiscoverySource,
    collection: str,
    since: str,
    keep: Callable[[str, str], bool],
) -> list[tuple[str, str | None]]:
    """``(package_id, last_modified)`` for every row of one collection window this run accepts.

    The whole window, every run — see the module docstring. The id grammar,
    not the request, says which collection a row belongs to: a row whose id it
    refuses is dropped by name and counted, and one it places in another
    collection is dropped; neither is fetched.
    """
    accepted: list[tuple[str, str | None]] = []
    walked = refused_id = activity_named = 0
    url = published_url(since, _today(), collections=[collection], page_size=1000)
    for page in reader.packages(url, max_pages=MAX_PAGES):
        for record in page.records:
            walked += 1
            package_id = str(record.get("packageId") or "")
            title = str(record.get("title") or "")
            activity_named += int(names_activity(title))
            try:
                identity = parse_package_id(package_id)
            except ValueError as error:
                # A neighbouring collection's id inside this collection's walk.
                refused_id += 1
                logger.warning("{}: {} is not a {} package id: {}", collection, package_id, collection, error)
                continue
            if identity.collection == collection and keep(package_id, title):
                accepted.append((package_id, record.get("lastModified")))
    logger.info(
        "{}: {:,} rows walked since {}, {:,} accepted, {:,} refused by id grammar",
        collection,
        walked,
        since,
        len(accepted),
        refused_id,
    )
    if collection == CRPT:
        logger.info("CRPT title rule {}: {} titles name activity, {} accepted",
                    ACTIVITY_REPORT_RULE_VERSION, activity_named, len(accepted))
    return accepted


def _roster_vocabulary(rosters: RosterSource, congress: int) -> dict[str, dict[str, tuple[tuple[str, str], ...]]]:
    """The committee names ``find_citations`` resolves against, from both chamber files, by chamber and reading.

    ``report`` is read for each chamber's print: every name either file
    states, a name both chambers hold given to that chamber. ``named`` is each
    chamber's own committees alone, for a name the print qualifies with its
    chamber (``Senate Committee on Armed Services``). A file that refuses
    leaves its chamber out, which makes ``committees_unresolved`` larger and
    invents nothing. Both routes are keyless, so neither refusal is a
    credential refusal.
    """
    house: list[Any] = []
    senate: list[Any] = []
    for label, call, into in (
        ("House", lambda: rosters.acquire_house(congress=congress), house),
        ("Senate", rosters.acquire_senate, senate),
    ):
        try:
            into.append(call().roster)
        except CredentialRefusedError:
            raise
        except (CommitteeRosterError, httpx.HTTPError, ConnectionError) as error:
            logger.warning(
                "Print citations: {} roster not established, its committees stay unresolved: {}",
                label,
                scrub_credential(str(error), ""),
            )
    vocabularies = {
        reading: {
            chamber: committee_vocabulary(house=house, senate=senate, chamber=chamber, own_only=reading == "named")
            for chamber in COMMITTEE_CHAMBERS
        }
        for reading in ("report", "named")
    }
    logger.info("Print citations: committee vocabulary holds {:,} names", len(vocabularies["report"]["house"]))
    return vocabularies


def _read_body(acquirer: PackageBodySource, package_id: str) -> tuple[Any, BodyText]:
    """One package's fetched body and the one text derivation for its rendition.

    No extractor argument, for the reason ``build_committee_reports`` states:
    ``body_text``'s default is the pipeline ``gpo_normalize`` was derived on,
    and the GPO gutter-numbered layout only comes through on PyMuPDF's line
    adjacency. Table detection stays off — nothing published here is a ruled
    cell, and it would cost three to eight times the per-page time.
    """
    package = acquirer.acquire(package_id, prefer=PRINT_BODY_PREFERENCE)
    return package, body_text(package)


#: The two collections this pass walks, with the table each fills and the rule
#: that says a listing row belongs to it. Named once so the enumeration, the
#: scheduling and the row lists cannot disagree about which families exist.
_FAMILIES: tuple[tuple[str, str, str, Callable[[str, str], bool]], ...] = (
    (CRPT, ACTIVITY_REPORTS, "activity", is_activity_report),
    # A BUDGET title says nothing useful ("Appendix", "Mid-Session Review"), so
    # the collection ``_listed`` reads from the id's grammar is the whole rule.
    (BUDGET, BUDGET_VOLUMES, "budget", lambda _package_id, _title: True),
)


def _read_identity(state: Mapping[str, Any] | None) -> tuple[Any, Any, Any]:
    """What a checkpoint says was read: source timestamp, text digest and processing version.

    The read and refusal times beside them order the queue and never decide
    whether a package is complete.
    """
    state = state or {}
    return state.get("last_modified"), state.get("text_sha256"), state.get("processing_version")


def _queue(pending: Iterable[str], held: Mapping[str, _Held], states: Mapping[str, Mapping[str, Any]]) -> list[str]:
    """One collection's outstanding packages, the one most in need of a read first.

    1. A package its last scheduled attempt left unread (a refusal, or a
       volume whose root offers no preferred body) goes behind every other,
       oldest attempt first: a print the publisher keeps refusing never holds
       the head of a capped queue, and is still retried whenever the cap
       leaves room.
    2. Then a package whose published row was read under a citation rule set
       other than the installed one, or that has no published row: its rows are
       wrong or missing, not merely due. Round 5 (W5-2) found 20 activity
       reports held under bill rule 004 behind 20 at the installed set, which a
       cap of 20 re-read every day.
    3. Then the least recently read; a checkpoint from before read times were
       kept counts as never read.

    Ties keep the enumeration order (held table, then listing). ``O(n log n)``
    in one collection's outstanding packages (81 across both, 2026-10-03).
    """

    def need(package_id: str) -> tuple[str, bool, str]:
        state = states.get(package_id) or {}
        row = held.get(package_id)
        return (
            str(state.get("unread_at") or ""),
            row is not None and row.rule_set_version == CITATION_RULE_SET_VERSION,
            str(state.get("read_at") or ""),
        )

    return sorted(pending, key=need)


def _schedule(outstanding: Mapping[str, Sequence[str]], cap: int) -> list[tuple[str, str]]:
    """Round-robin the collections' outstanding packages into one capped list.

    **Why not simply walk one collection and then the other.** The cap is
    shared, so walking CRPT first spends all of it on CRPT whenever CRPT has
    more outstanding packages than the cap — and the run then publishes an
    *empty* ``budget_volumes``, which is worse than a partial table because a
    consumer cannot tell "this family published nothing" from "this family has
    nothing". Round-robin fetches the same total, makes progress on both
    families every run, and a family with more outstanding work keeps the
    slack once the other is exhausted. ``O(cap)`` in time; the order within a
    collection is :func:`_queue`'s, untouched.
    """
    queues = {collection: list(packages) for collection, packages in outstanding.items() if packages}
    scheduled: list[tuple[str, str]] = []
    while queues and len(scheduled) < cap:
        for collection in list(queues):
            if len(scheduled) >= cap:
                break
            scheduled.append((collection, queues[collection].pop(0)))
            if not queues[collection]:
                del queues[collection]
    remaining = sum(len(packages) for packages in outstanding.values()) - len(scheduled)
    if remaining:
        # The windows were enumerated whole; only the fetch stopped. The next
        # run enumerates them again and picks up what this one left.
        logger.warning(
            "Print citations: per-run cap of {:,} packages reached — {:,} outstanding packages are retried next run",
            cap,
            remaining,
        )
    logger.info(
        "Print citations: scheduled {}",
        dict(Counter(collection for collection, _ in scheduled)) or "nothing — every package is published",
    )
    return scheduled


def build_print_citations(
    output_dir: Path,
    *,
    reader: PackageDiscoverySource | None = None,
    acquirer: PackageBodySource | None = None,
    rosters: RosterSource | None = None,
    max_packages: int = MAX_PACKAGES_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
    evidence: CaptureEvidence | None = None,
) -> tuple[Path, Path, Path, Path]:
    """Build the two document tables, the bill-action table and the shared citation link table."""
    if reader is None or acquirer is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Print citations need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        if evidence is not None:
            evidence.credential = api_key
        reader = reader or GovInfoDiscoveryReader(
            budget=DISCOVERY_BUDGET, api_key=api_key,
            transport=None if evidence is None else evidence.transport(stage="print-discovery", max_bytes=DISCOVERY_BUDGET.max_page_bytes),
        )
        acquirer = acquirer or GovInfoBodyAcquirer(
            budget=BODY_BUDGET, api_key=api_key,
            transport=None if evidence is None else evidence.transport(
                stage="print-package", max_bytes=max(BODY_BUDGET.max_body_bytes, BODY_BUDGET.max_metadata_bytes)),
        )
    rosters = rosters or CommitteeRosterAcquirer(
        budget=ROSTER_BUDGET,
        transport=None if evidence is None else evidence.transport(stage="print-rosters", max_bytes=ROSTER_BUDGET.max_bytes),
    )

    # All four priors are asked for once, up front. The two document tables
    # need theirs for the held-package check anyway; the two derived tables ask
    # only so ``merge_contract_table`` is told whether a prior exists and does
    # not repeat a download that already returned 404.
    priors = {
        name: published_table(output_dir, name, download_prior)
        for name in (ACTIVITY_REPORTS, BUDGET_VOLUMES, BILL_ACTIONS, CITATIONS)
    }
    have_prior = {name: path is not None for name, path in priors.items()}
    held = {name: _held_packages(priors[name]) for name in (ACTIVITY_REPORTS, BUDGET_VOLUMES)}
    checkpoints = {
        name: {row["package_id"]: row for row in read_checkpoints(path, CHECKPOINT_NAMESPACE)
               if isinstance(row.get("package_id"), str)}
        for name, path in priors.items()
    }
    since = _issue_floor()
    logger.info("BUDGET: {} measured parts admitted by the package grammar", len(MEASURED_BUDGET_PARTS))

    vocabularies = _roster_vocabulary(rosters, current_congress())
    processing_versions = _processing_versions(vocabularies)
    evaluated: dict[str, set[tuple[str, str]]] = {CRPT: set(), BUDGET: set()}

    def complete(package_id: str, collection: str, last_modified: str | None) -> bool:
        published = held[_OUTPUTS[collection][0]].get(package_id)
        if last_modified is None or published is None or published.last_modified != last_modified:
            return False
        expected = (last_modified, published.text_sha256, processing_versions[collection])
        return all(_read_identity(checkpoints[name].get(package_id)) == expected for name in _OUTPUTS[collection])

    def left_unread(collection: str, package_id: str) -> None:
        # Kept beside whatever the last successful read recorded, which this
        # does not complete: the package stays outstanding, behind the others.
        at = _now()
        for name in _OUTPUTS[collection]:
            checkpoints[name][package_id] = {**checkpoints[name].get(package_id, {"package_id": package_id}),
                                             "unread_at": at}

    activity_rows: list[dict] = []
    budget_rows: list[dict] = []
    action_rows: list[dict] = []
    citation_rows: list[dict] = []
    refusals: Counter[str] = Counter()
    kinds: Counter[str] = Counter()
    covered_by: Counter[str] = Counter()
    filed_in_another_congress = 0
    unchanged = fetched = capped = root_not_offered = 0
    pages_read = 0

    # Enumerate both windows first, drop what is already published, then
    # interleave — see :func:`_schedule` for why the order is not "CRPT, then
    # whatever is left".
    outstanding: dict[str, list[str]] = {}
    for collection, table, _label, keep in _FAMILIES:
        # Legacy outputs have no checkpoint and need one reread. Stale held
        # packages remain eligible even when the discovery window omits them.
        known = {package_id: row.last_modified for package_id, row in held[table].items()}
        for name in _OUTPUTS[collection]:
            for package_id, state in checkpoints[name].items():
                if _collection(package_id) == collection:
                    known.setdefault(package_id, state.get("last_modified"))
        pending = {package_id: modified for package_id, modified in known.items()
                   if not complete(package_id, collection, modified)}
        listed = _listed(reader, collection, since, keep)
        pending.update({package_id: modified for package_id, modified in listed
                        if not complete(package_id, collection, modified)})
        outstanding[collection] = _queue(pending, held[table], checkpoints[table])
        unchanged += sum(package_id not in pending for package_id, _modified in listed)

    scheduled = _schedule(outstanding, max_packages)
    rows_for = {CRPT: activity_rows, BUDGET: budget_rows}

    for collection, package_id in scheduled:
        rows = rows_for[collection]
        chamber = _report_chamber(package_id) if collection == CRPT else None
        if collection == CRPT and chamber is None:
            refusals["NoStatedChamber"] += 1
            logger.warning("CRPT: {} states no House or Senate chamber in its id; refused before any request", package_id)
            left_unread(collection, package_id)
            continue
        try:
            package, derived = _read_body(acquirer, package_id)
        except CredentialRefusedError:
            raise
        except GovInfoFormatNotOfferedError as error:
            if collection != BUDGET:
                raise
            root_not_offered += 1
            logger.info("BUDGET: {} publisher offers no preferred package-root body: {}; no granule attempted",
                        package_id, scrub_credential(str(error), ""))
            left_unread(collection, package_id)
            continue
        except _PACKAGE_REFUSALS as error:
            # Counted by reason, not just counted: forty refusals sharing
            # one reason is a defect here, and forty different ones are the
            # publisher.
            reason = type(error).__name__
            refusals[reason] += 1
            logger.warning("{}: {} refused ({}): {}", collection, package_id, reason, scrub_credential(str(error), ""))
            left_unread(collection, package_id)
            continue
        fetched += 1

        # A report's bills belong to the Congress the report says it covers,
        # never the package id's: a Senate report is filed in the next
        # Congress, and its MODS stamps its bills with that one too
        # (CRPT-118srpt99's H.R. 5376 is the 117th's). A BUDGET package, or a
        # report stating no Congress, leaves each printed bill key
        # congress-free and unresolved rather than stamped with a guess.
        covered = covered_congress(package.summary.title, derived.pages) if collection == CRPT else None
        findings = find_citations(
            derived.text,
            pages=derived.pages,
            congress=None if covered is None else covered.congress,
            committees=vocabularies["report"][chamber] if chamber is not None else (),
            chamber_committees=vocabularies["named"] if chamber is not None else None,
        )
        for finding in findings:
            kinds[finding.kind] += 1

        if covered is not None:  # an activity report; only CRPT is read for the Congress it covers
            covered_by[str(covered.source)] += 1
            filed_in_another_congress += int(covered.congress not in (None, package.identity.congress))
            if covered.congress is None:
                logger.warning("CRPT: {} states no single Congress it covers ({} {}); its bills stay unresolved",
                               package_id, covered.source, covered.stated)
            stated = activity_report_stated_keys(package.mods, covered)
            document_kind = GOVINFO_PACKAGE
            rows.append(
                shape_activity_report(
                    package.summary,
                    package.mods,
                    derived,
                    findings,
                    covered=covered,
                    rule_set_version=CITATION_RULE_SET_VERSION,
                )
            )
        else:
            # Bills are dropped from the comparison, not answered `false`:
            # a budget volume has no Congress, so no comparison against the
            # MODS's `119-hr-7806` is possible from the print's `HR7806`.
            stated = budget_index_stated_keys(package.mods)
            document_kind = BUDGET_VOLUME
            rows.append(
                shape_budget_volume(
                    package.summary, package.mods, derived, findings, rule_set_version=CITATION_RULE_SET_VERSION
                )
            )

        provenance = document_provenance(derived, document_key=package_id, document_kind=document_kind)
        citation_rows.extend(shape_document_citation(f, provenance, stated_by_index=stated) for f in findings)

        if collection == CRPT:
            reading = find_bill_actions(derived.text, findings, committee_chamber=chamber)
            action_rows.extend(
                shape_bill_committee_action(
                    action,
                    provenance,
                    citation_rule_version=CITATION_RULES_BY_NAME["bill_number"].version,
                )
                for action in reading.findings
            )

        # Offsets are evidence for one normalized text. Correct findings for that text, and keep the
        # findings on an earlier revision of the source; see :func:`_superseded_readings`. This one
        # scope drives every table that keeps a row per text digest.
        source_sha256 = package.body_capture.sha256
        if provenance.text_sha256 is not None:
            evaluated[collection].add((package_id, provenance.text_sha256))
            superseded = _superseded_readings(
                (checkpoints[name].get(package_id, {}) for name in _OUTPUTS[collection]),
                source_sha256=source_sha256, last_modified=package.summary.last_modified,
                current=provenance.text_sha256,
            )
            evaluated[collection].update((package_id, digest) for digest in superseded)
            if superseded and evidence is not None:
                # The public rows hold one reading; this and the prior generation keep the trail.
                evidence.event("print-reading-replaced", package_id=package_id, source_sha256=source_sha256,
                               replaced_text_sha256=sorted(superseded), text_sha256=provenance.text_sha256)
        state = {"package_id": package_id, "last_modified": package.summary.last_modified,
                 "source_sha256": source_sha256, "text_sha256": provenance.text_sha256,
                 "processing_version": processing_versions[collection], "read_at": _now()}
        for name in _OUTPUTS[collection]:
            checkpoints[name][package_id] = state

        depth = len(derived.pages) if derived.pages is not None else 0
        pages_read += depth
        stated_pages = package.summary.pages
        if stated_pages is not None and str(stated_pages).isdecimal() and depth < int(stated_pages):
            capped += 1

    logger.info(
        "Print citations: {:,} activity reports, {:,} budget volumes, {:,} citation rows, {:,} action rows"
        " from {:,} packages and {:,} pages; {:,} already held, {:,} refused",
        len(activity_rows),
        len(budget_rows),
        len(citation_rows),
        len(action_rows),
        fetched,
        pages_read,
        unchanged,
        sum(refusals.values()),
    )
    logger.info("BUDGET: {} publisher package-root format answers (not failures)", root_not_offered)
    if covered_by:
        logger.info("CRPT: covered Congress read from {} (rule {}); {} reports cover a Congress other than their filing one",
                    dict(covered_by), COVERED_CONGRESS_RULE_VERSION, filed_in_another_congress)
    if capped:
        # Every count on a capped document is a floor. Nothing here caps pages,
        # so this can only mean the body the publisher served was shorter than
        # the extent it states — which is the publisher's disagreement with
        # itself and worth seeing.
        logger.warning("Print citations: {:,} packages read fewer pages than the publisher states", capped)
    if refusals:
        logger.info("Print citations: refusals by reason — {}", dict(refusals))
    if kinds:
        # Which citation kinds the prints actually stated. The contract has no
        # column for the per-run distribution, so this is where it is said.
        logger.info("Print citations: citation findings by kind — {}", dict(kinds))

    metadata = {
        name: checkpoint_metadata(priors[name], CHECKPOINT_NAMESPACE, list(states.values()))
        for name, states in checkpoints.items()
    }
    return (
        merge_contract_table(
            output_dir,
            ACTIVITY_REPORTS,
            activity_rows,
            download_prior=download_prior,
            prior_present=have_prior[ACTIVITY_REPORTS],
            parquet_metadata=metadata[ACTIVITY_REPORTS],
        ),
        merge_contract_table(
            output_dir,
            BUDGET_VOLUMES,
            budget_rows,
            download_prior=download_prior,
            prior_present=have_prior[BUDGET_VOLUMES],
            parquet_metadata=metadata[BUDGET_VOLUMES],
        ),
        merge_contract_table(
            output_dir,
            BILL_ACTIONS,
            action_rows,
            download_prior=download_prior,
            prior_present=have_prior[BILL_ACTIONS],
            replace_parents=(("document_key", "text_sha256"), evaluated[CRPT]),
            parquet_metadata=metadata[BILL_ACTIONS],
        ),
        merge_contract_table(
            output_dir,
            CITATIONS,
            citation_rows,
            download_prior=download_prior,
            prior_present=have_prior[CITATIONS],
            replace_parents=(
                ("document_kind", "document_key", "text_sha256"),
                {(GOVINFO_PACKAGE, *key) for key in evaluated[CRPT]}
                | {(BUDGET_VOLUME, *key) for key in evaluated[BUDGET]},
            ),
            parquet_metadata=metadata[CITATIONS],
        ),
    )
