"""Build the bill family through spicy-docs, retaining this rollup's own resume state.

BILLSTATUS supplies the CBO index and explicit per-bill outcomes, and lists
each bill's printings; their bodies, sections and comparisons are the body
pass's (``bill_family_bodies``), by each printing's own state. The per-package
route and model processing keep their separate declared caps.
"""

from __future__ import annotations

import functools
import json
import os
from collections import Counter
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from itertools import groupby
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, Self

import httpx
from loguru import logger
from spicy_docs.interpretation.bill_family import (
    BillFamilyCapture,
    BillFamilyTables,
    BillSummarizer,
    BillVersionCapture,
    DiffSummarizer,
    EngineStamp,
    FamilyRefusal,
    SectionClassifier,
    installed_engine_stamp,
    build_bill_printings,
)
from spicy_docs.interpretation.bill_family import build_bill_family as build_family
from spicy_docs.interpretation.bill_stage import infer_stage, infer_stage_from_action, listed_enrolled_text, signed_date
from spicy_docs.interpretation.bill_summaries import summarize_bill, summarize_diff
from spicy_docs.interpretation.gemini_call import DEFAULT_MODEL, model_call
from spicy_docs.interpretation.section_classification import classify_sections
from spicy_docs.interpretation.vote_matching import (
    VoteMatchError,
    VoteReference,
    read_vote_key,
    recorded_vote_references,
)
from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.schemas.activity_events import activity_events, snapshot_from_rows
from spicy_docs.schemas.bill_version_tables import BILL_VERSION_SOURCES
from spicy_docs.schemas.tables import bill_congress
from spicy_docs.schemas.tables import bill_id as bill_key
from spicy_docs.schemas.tables import text
from spicy_docs.sources.congress.bill_status import (
    BillAction,
    BillIdentity,
    BillLaw,
    BillSourceError,
    BillSponsor,
    BillStatus,
    BillTextVersion,
)
from spicy_docs.sources.congress.bill_versions import (
    VersionCodeError,
    bill_text_package_id,
    consecutive_pairs,
    printing_order,
    printing_version_code,
)
from spicy_docs.sources.congress.bulk_status import (
    BulkListingEntry,
    BulkStatusAcquirer,
    bulk_status_locator,
)
from spicy_docs.sources.congress.bulk_bills import BulkBillsAcquirer
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.generations import spicy_docs_code
from spicy_regs.legislative_documents import printing_id
from spicy_regs.policy_migrations import PriorReplayRequired
from spicy_regs.source_evidence import SourceEvidenceError
from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import (
    API_KEY_ENV_VARS,
    _resolve_api_key,
    bill_detail,
    listing_reader,
)
from spicy_regs.transforms.bill_family_cbo import (
    cost_estimate_publication,
    law_bills_from,
    prior_citations,
    read_cbo_feeds,
)
from spicy_regs.transforms.bill_family_bodies import (
    ACQUIRED_SOURCE,
    BODY_BUDGET,
    LISTED_SOURCE,
    MAX_VERSION_FETCHES,
    PACKAGE_REFUSALS_KEY,
    TEXT_REFUSALS_KEY,
    BodyPass,
    BulkBillsSource,
    PackageBodySource,
    body_kind,
    plan_work,
    refusal_metadata,
    refusal_state,
    version_from_row,
    supported_package,
)
from spicy_regs.transforms.congress_scope import (
    BULK_STATUS_FLOOR,
    DEFAULT_BILL_TYPES,
    bill_types_from_env,
    bulk_status_budget,
    congresses_from_env,
    default_congresses,
)
from spicy_regs.transforms.model_call import resolve_gemini_key
from spicy_regs.sources.publication import parquet_scan
from spicy_regs.transforms.table_merge import (
    Partitioning,
    ReplacementScope,
    download_prior_members,
    merge_contract_table,
    merge_partitioned_table,
    merge_table,
    published_members,
    published_table,
    set_column,
    write_in_place,
)

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

#: The DeltaTrack commit the vendored wheel was built from. ``installed_engine_stamp``
#: reads a revision out of ``direct_url.json``, which only a git install writes;
#: this repository vendors wheels on purpose, so the stamp it returns has an
#: empty revision. The commit is a fact of the vendoring, recorded in
#: ``vendor/README.md`` beside the wheel's SHA-256, and is supplied here rather
#: than left blank — ``section_diffs.engine_revision`` is the column that says
#: which engine produced a diff.
DELTATRACK_REVISION = "c636448ba08d55bba7cb8c884aad0f5ac1ccf2f6"

#: How many distinct refusal reasons a run's log names before it stops and says
#: how many it did not. Distinct reasons, not refusals: a prompt every bill
#: refuses is one reason and one line. The cap is only against a reason that
#: embeds a publisher string and so varies per row.
REFUSAL_REASONS_LOGGED = 20

#: The per-folder listing entries this rollup retains between runs, so the next
#: run can prove a BILLSTATUS zip has not moved without downloading it. It is
#: processing state, not a contract table: nothing in ``spicy_docs.schemas``
#: shapes it, and the dictionary documents it as this rollup's own.
ARCHIVES_TABLE = "bill_family_archives"

#: The four fields ``BulkStatusAcquirer.acquire`` actually compares (``name``
#: and ``link`` to prove the entry describes *this* folder's zip, ``modified_at``
#: and ``size`` to prove it has not moved), the publisher's own stamp string the
#: instant was read from, the folder the entry belongs to, and when it was seen.
ARCHIVE_COLUMNS: tuple[str, ...] = (
    "name",
    "link",
    "formatted_last_modified_time",
    "modified_at",
    "size",
    "congress",
    "bill_type",
    "observed_at",
)

#: One entry per folder, and the folder is what a run looks it up by.
ARCHIVE_IDENTITY: tuple[str, ...] = ("congress", "bill_type")

# Qualifies the folder's status: every member parsed and every rebuilt bill
# shaped, or its refusal recorded (``STATUS_REFUSALS_KEY``). A source listing
# stamp alone never proves that this work completed. v3 required the dated
# cosponsor reader and no refusal at all; v4 admits a recorded refusal, so a
# folder holding one document no reader can shape is not downloaded every run.
# A v3 scope is a v4 one too (it had no refusal), so both are read. Earlier
# completion cannot qualify this output, even when the publisher archive stamp
# is unchanged.
ARCHIVE_COMPLETION_KEY = "spicy_regs.bill_family.completed_archive_scopes.v4"
ARCHIVE_COMPLETION_KEYS_READ = (ARCHIVE_COMPLETION_KEY, "spicy_regs.bill_family.completed_archive_scopes.v3")

#: The status reads that settled nothing, by bill: a document the reader refused, a shaped bill whose rows leave its
#: status in doubt (its ``version_count`` against its distinct printings, or no ``congress_bills`` row), and a bill a
#: BILLSTATUS read published that its folder's zip no longer holds. Each is ``{"folder", "zip", "reader",
#: "refusal"}``: the zip entry it was found in (``modified_at|size``) and the reader that found it. Such a bill does
#: not reopen its folder while the folder still holds that zip and that reader runs, since reading the same bytes with
#: the same code settles nothing; a moved zip or another reader reads it again. Before this record, each one reopened
#: its folder on every run: 26 bills in ten folders of the 108th-111th and 117th (a printing listed twice, or one the
#: publisher lists without a type), measured on the live generation of 2026-09-29. Parquet metadata on the archives
#: table, in the shape ``refusal_state`` reads, beside the package refusals it mirrors.
STATUS_REFUSALS_KEY = "spicy_regs.bill_family.status_refusals.v1"

#: The refusal a status read records for a bill its folder's zip no longer holds.
DROPPED_REFUSAL = "dropped: the folder's zip holds no document for this bill"

#: Which reader read each bill's status, in the same metadata: ``{reader: {"119-hr": "1-7120,7122"}}``, each
#: bill's number under the :func:`status_reader` of its last status read. A bill's rows are what that reader
#: derived from its BILLSTATUS, so an unchanged publisher stamp skips a bill only while its recorded reader is
#: the running one: a SpicyDocs release that changes the code re-reads every bill of a Congress on that
#: Congress's next run, and a version-only release re-reads none. A re-read does not replace every older
#: value: ``congress_bills`` merges column-wise, so a value the new reader leaves NULL keeps the old one; a bill
#: whose cosponsor rows are refused keeps its prior rows, and CBO rows are replaced only on a listed outcome;
#: and the pre-108th detail backfill records no reader. A bill with no reader recorded, every bill before this
#: key, is read once. Per bill, not per folder, so a run that reaches part of a folder records what it read.
#: Numbers are stored as runs: the 172,996 bills the BILLSTATUS reads published by 2026-09-28 fall in 126 runs
#: over 96 folders, where one id each is ~2 MB. Here, not on ``congress_bills``: that table is SpicyDocs' public
#: contract, the ``laws`` join rewrites it without metadata, and every query reads its footer; this one is the
#: rollup's own resume state. The key's ``.v1`` is the record's format, not the reader.
STATUS_READERS_KEY = "spicy_regs.bill_family.status_readers.v1"

#: This rollup's own status-pass rule, in the recorded reader beside SpicyDocs' code as the committee-report and
#: Table III readers carry theirs. Bump it when a change here alters what a status read publishes (the shaping
#: around ``build_family``, the vote references, the listed printings), and every bill is read once more.
#: status-v2: CRS summaries gained both native clocks and their schema context (2853bad1).
STATUS_READER_RULE = "status-v2"

#: The table a replay-required prior (``policy_migrations``) can rebuild here: every row derives from the bill's
#: BILLSTATUS read, so re-reading every bulk folder whole restates all of them from the publisher's XML.
REPLAYED_FROM_STATUS = "bill_publisher_summaries"


def status_reader() -> str:
    """The reader a bill's status is recorded under: :data:`STATUS_READER_RULE` and :func:`spicy_docs_code`."""
    return f"{STATUS_READER_RULE};code={spicy_docs_code()}"


#: The roll calls each bill's own actions record, one row per ``recordedVotes``
#: entry. Not a contract table — nothing in ``spicy_docs.schemas`` shapes it —
#: but a published output all the same, because the votes rollup reads it.
VOTE_REFERENCES_TABLE = "bill_vote_references"

#: The six sealed fields of a recorded vote (``vote_matching.RECORDED_VOTE_FIELDS``)
#: as the reference carries them, the action they sat on, the seventh field
#: the guide documents and the publisher may resume sending, and the capture.
VOTE_REFERENCE_COLUMNS: tuple[str, ...] = (
    "bill_id",
    "chamber",
    "congress",
    "session",
    "roll_number",
    "action_index",
    "url",
    "date",
    "full_action_name",
    "observed_at",
)

#: One row per recorded vote per action. The publisher attaches the same roll
#: call to each floor action it settled (measured: a passage vote sits on both
#: "On passage" and "Motion to reconsider"), so the action is part of the key.
#: ``url``, ``date`` and ``full_action_name`` are values, not key parts:
#: ``full_action_name`` is NULL on every entry measured, and a NULL key part
#: would have ``merge_table`` drop the row rather than publish it. The position
#: is the publisher's newest-first list position, so it moves as actions are
#: added: a re-read bill's rows replace its prior ones whole, and a bill's rows
#: come from its newest capture only (``_latest_capture_only``).
VOTE_REFERENCE_IDENTITY: tuple[str, ...] = ("bill_id", "chamber", "congress", "session", "roll_number", "action_index")

#: The oldest Congress the ``bill`` list route reaches (measured:
#: ``bill/{congress}?format=json&limit=1`` answers a declared count for every
#: Congress from the 82nd on — the A11 receipt's ``declared-counts.json``).
#: Naming an older one refuses the run rather than walking into an empty
#: success that would read as absence.
LIST_ROUTE_FLOOR = 82

#: Backstop against a runaway walk of one ``bill/{congress}/{type}`` unit, not an
#: expected limit: the whole ~430k-bill archive was about 1,930 pages at 223 rows
#: a page (the retired list writer's measure, 2026-08), so one unit is a small
#: fraction of this. Hitting it raises ``PagedJsonSourceError``
#: from the spicy-docs reader; it is never a quiet stop.
LIST_WALK_MAX_PAGES = 2_500

#: The backfill's own retained state, beside ``bill_family_archives``. One row
#: per bill the backfill has attempted: filled (``refusal`` NULL, the
#: ``congress_bills`` row is published) or refused (``refusal`` names the
#: error class). That is the CRS summaries resume pattern this repository's
#: AGENTS.md points at — resume skips a success and retries a failure, except a
#: settled one (:data:`SETTLED_REFUSALS`: refused under this reader, at this
#: list stamp) — and, per ``(congress, bill_type)`` walked, the route's
#: declared total against what was walked.
BACKFILLS_TABLE = "bill_family_backfills"
BACKFILL_WALKS_TABLE = "bill_family_backfill_walks"

#: The stamp a state row keeps is the list record's own
#: ``updateDateIncludingText`` exactly as that walk saw it — compared as a
#: string, because both walks read the same route shape, and an old Congress's
#: list records truncate the instant to a date while its detail records do not.
#: ``refusal`` is the error's class name only, never its message: a class name
#: cannot carry a credential or a publisher's text, and the run log has the
#: scrubbed message.
BACKFILL_COLUMNS: tuple[str, ...] = (
    "congress",
    "bill_type",
    "number",
    "list_update_date_including_text",
    "refusal",
    "observed_at",
)

#: One row per attempted bill; the bill is what a walk looks it up by.
BACKFILL_IDENTITY: tuple[str, ...] = ("congress", "bill_type", "number")

#: One row per ``(congress, bill_type)`` walked — the same unit as
#: ``bill_family_archives``, because ``bill/{congress}/{type}`` declares its
#: own count, so the scope's ``BILL_FAMILY_BILL_TYPES`` narrows the walk the
#: way it narrows the zips and each unit settles on its own declared total.
#: Rewritten each walk (fresh ``observed_at`` wins the merge); a settled unit
#: is not walked and keeps its prior row.
BACKFILL_WALK_COLUMNS: tuple[str, ...] = (
    "congress",
    "bill_type",
    "declared_count",
    "records_walked",
    "pages_walked",
    "list_completed",
    "unwalkable_count",
    "repeated_count",
    "backfilled_count",
    "observed_at",
)

BACKFILL_WALK_IDENTITY: tuple[str, ...] = ("congress", "bill_type")

#: The ``congress_bills`` columns a backfilled row NULLs after ``shape_bill``
#: because the detail record cannot substantiate them. The counts are stated
#: by the record only as sub-route ``count`` values — the items are the
#: sub-route's own requests, which the backfill does not walk — and a
#: published zero where the publisher declared a positive count would be a
#: false statement, not a smaller one (``cosponsor_count`` is the exception:
#: the record does state ``cosponsors.count``, and ``_backfill_bill_row``
#: publishes that). ``stage`` is the shaper's default rung ("introduced")
#: reached because the status carried no actions to classify, which is not a
#: finding about the bill — the A11 sample row "Became Public Law" and would
#: have said introduced. ``signed_date_rule`` would state
#: ``public_law_without_became_law_action``, a rule whose count the stage
#: module documents as the measure of whether a fallback is needed; on the
#: detail route no action was examined, so the rule did not run on the bill.
#: ``schema_version`` has no detail counterpart at all, so its NULL is also the
#: provenance marker: a row for a Congress below the bulk floor (where no
#: BILLSTATUS exists) with these columns NULL came from the API detail route,
#: and the dictionary prose pins that reading beside the column.
BACKFILL_UNSUBSTANTIATED: tuple[str, ...] = (
    "schema_version",
    "action_count",
    "committee_count",
    "version_count",
    "subject_count",
    "subjects_json",
    "related_bill_count",
    "related_bills_json",
    "stage",
    "signed_date_rule",
)

#: The ``url_source`` only the retired list writer stated (``run-rollup-congress-bills``,
#: retired by plan A1, decision 31). Its run of 2026-09-23 replaced 3,044 BILLSTATUS
#: ``update_date`` instants with the list route's same-day dates and 3,095 congress.gov
#: page URLs with API resource URLs (receipt ``drift-audit-2026-09-23/bill-family.json``)
#: but left ``update_date_including_text``, the stamp the unchanged-bill skip compares,
#: alone — so a bill carrying this label is re-read rather than skipped. The re-read
#: states its own url and label, so the set drains in one run over the bill's Congress.
RETIRED_LIST_URL_SOURCE = "congress_api_list"

#: The three tables ``public_activity_events`` compares between runs.
SNAPSHOT_TABLES = ("congress_bills", "bill_versions", "bill_summaries")

#: ``snapshot_from_rows`` names its arguments after the contracts, except
#: for bills, which it calls ``bills`` rather than ``congress_bills``.
_SNAPSHOT_ARG = {
    "congress_bills": "bills",
    "bill_versions": "bill_versions",
    "bill_summaries": "bill_summaries",
}

#: Provider-owned shapes, including the CBO publication index.
FAMILY_TABLES: tuple[tuple[str, str], ...] = (
    ("congress_bills", "bills"),
    ("bill_actions", "bill_actions"),
    ("bill_cosponsors", "bill_cosponsors"),
    ("bill_committees", "bill_committees"),
    # spicy-docs 0.54.0 (round 4, B4): what each committee did and when, from the same committee walk.
    ("bill_committee_activities", "bill_committee_activities"),
    ("bill_publisher_summaries", "bill_publisher_summaries"),
    ("bill_versions", "bill_versions"),
    ("bill_sections", "bill_sections"),
    ("section_diffs", "section_diffs"),
    ("section_diff_items", "section_diff_items"),
    ("financial_changes", "financial_changes"),
    ("section_classifications", "section_classifications"),
    ("bill_summaries", "bill_summaries"),
    ("diff_summaries", "diff_summaries"),
    ("cbo_cost_estimates", "cbo_cost_estimates"),
    # spicy-docs 0.54.0: every item of each CBO feed read, whether or not it names a bill.
    ("cbo_feed_items", "cbo_feed_items"),
)

#: Contracts stored one file per partition (multi-file design §4.2): ``bill_sections`` by the Congress its
#: ``bill_id`` names, derived by the contract's own ``bill_congress``. A nightly run then rewrites the sitting
#: Congress's file, and publication copies the rest. The rollup declares these to ``build_generation``.
PARTITIONED: Mapping[str, Partitioning] = {"bill_sections": Partitioning("congress", "bill_id", bill_congress)}


class BulkStatusSource(Protocol):
    """What this transform needs of a BILLSTATUS archive acquirer.

    Structural on purpose: the real ``BulkStatusAcquirer`` satisfies it, and so
    does the fixture-backed stub in ``tests/test_bill_family.py``. Naming the
    concrete class here would make the hermetic test unrepresentable.
    """

    def acquire(self, congress: int, bill_type: str, *, unchanged_since: BulkListingEntry | None = ...) -> Any: ...

    def list_archives(self, congress: int, bill_type: str) -> Any: ...


class ListBackfillSource(Protocol):
    """What the pre-BILLSTATUS backfill needs of the ``bill`` route.

    Structural on purpose: :class:`CongressListBackfill` satisfies it with
    spicy-docs' reader over the seam ``sources.congress_bills`` owns, and the
    hermetic test stubs it with retained pages. ``pages`` yields the reader's
    page objects (``records``, ``declared_count``, a capture carrying
    ``observed_at``) for one ``(congress, bill_type)`` to the walk's terminal
    page or refusal; ``detail`` returns one bill's detail mapping beside the
    instant it was captured at.
    """

    def __enter__(self) -> Self: ...

    def __exit__(self, *_error: object) -> None: ...

    def pages(self, congress: int, bill_type: str) -> Iterator[Any]: ...

    def detail(self, identity: BillIdentity) -> tuple[Mapping[str, Any], str]: ...


class CongressListBackfill:
    """The production :class:`ListBackfillSource`: one reader instance for the run.

    ``listing_reader`` and ``bill_detail`` are the only constructor and the
    only single-request primitive this repository has for spicy-docs'
    ``CongressListingReader`` on the bill route (see
    ``sources.congress_bills``); this class composes them into the two calls
    the backfill loop makes, so the backfill is not a second reader of the
    route. The reader's budget is per request (see ``listing_reader``); the
    run's bound is the transform's shared ``max_version_fetches`` cap, which
    charges every page and every detail.
    """

    def __init__(
        self, api_key: str, transport: httpx.BaseTransport | None = None, *, evidence: CaptureEvidence | None = None
    ) -> None:
        self._reader = listing_reader(api_key, transport, evidence=evidence)
        self._evidence = evidence

    def __enter__(self) -> Self:
        self._reader.__enter__()
        return self

    def __exit__(self, *_error: object) -> None:
        self._reader.__exit__(*_error)

    def pages(self, congress: int, bill_type: str) -> Iterator[Any]:
        from spicy_docs.sources.congress.listing import bill_list_url

        return self._reader.bills(bill_list_url(congress=congress, bill_type=bill_type), max_pages=LIST_WALK_MAX_PAGES)

    def detail(self, identity: BillIdentity) -> tuple[Mapping[str, Any], str]:
        """One bill's detail; with evidence, its capture (or refusal) is retained, not only list pages."""
        try:
            record, capture = bill_detail(self._reader, identity)
        except Exception as error:
            if self._evidence is not None:
                self._evidence.refusal(error, stage="bill-detail")
            raise
        if self._evidence is not None:
            self._evidence.capture(capture, stage="bill-detail")
        return record, str(capture.observed_at)


def engine_stamp() -> EngineStamp:
    """The installed engine's stamp, with the vendored commit filled in."""
    stamp = installed_engine_stamp()
    return stamp if stamp.revision else EngineStamp(stamp.name, stamp.version, DELTATRACK_REVISION)


def _listed_captures(status: Any, held: Collection[str] = ()) -> list[BillVersionCapture]:
    """A metadata-only capture for each listed printing with no processed body: its pending state.

    The publisher's own facts about a printing (type, date, offered formats)
    are real and belong in ``bill_versions`` before any body is read, and the
    NULL body columns say what is missing; the body pass
    (``bill_family_bodies``) reads the body by that state, not by rebuilding
    the bill. A printing already held is left to its published body row.

    Bills before the 113th Congress offer no XML at all (measured:
    ``spicy-docs/docs/research/pdf-only-corpus-2026-09-19.md``); the body pass
    reads their PDF, whose ``GpoCleanupRecord`` fills the version row's
    ``cleanup_*`` columns, and a pair such a printing belongs to is refused by
    name through the provider's ``FamilyRefusal`` path.
    """
    captures: list[BillVersionCapture] = []
    for version in _ordered_printings(status):
        version_code = printing_version_code(version)
        if version_code in held:
            continue
        package_id = supported_package(status.identity, version)
        captures.append(
            BillVersionCapture(
                version=version,
                version_code=version_code,
                source=LISTED_SOURCE,
                package_id=package_id,
            )
        )
    return captures


def _printings(status: Any) -> list[tuple[str, str | None]]:
    """A status's typed printings as the ``(version_code, date)`` pairs the provider orders.

    The code is the provider's ``printing_version_code``: a numbered reprint the
    publisher addresses by package (``BILLS-119hr6644eas2``) is its own printing,
    not a second one under its stage's slug.
    """
    typed = (version for version in status.text_versions if version.type)
    return [(printing_version_code(version), getattr(version, "date", None)) for version in typed]


def _row_printing_code(row: Mapping[str, Any]) -> str:
    """The code a published ``bill_versions`` row's own label and package give its printing today."""
    version = BillTextVersion(type=row["label"], date=row["version_date"], formats=(), package_id=row["package_id"])
    try:
        return printing_version_code(version)
    except VersionCodeError:
        return row["version_code"]


def _ordered_printings(status: Any) -> list[Any]:
    """A status's typed printings in the provider's own ``printing_order``, the order its diffs pair in."""
    typed = [version for version in status.text_versions if version.type]
    return [typed[index] for index in printing_order(_printings(status))]


def _code_pairs(printings: Sequence[tuple[str, str | None]]) -> set[tuple[str, str]]:
    """The ``(earlier, later)`` version codes the provider's ``consecutive_pairs`` compares."""
    return {(printings[older][0], printings[newer][0]) for older, newer in consecutive_pairs(printings)}


@dataclass(frozen=True, slots=True)
class PriorIndex:
    """Published status stamps, processed bodies and completed XML pairs, each looked up by bill.

    ``pending_bills`` are bills whose *status* must be read again; bills with
    body or comparison work are ``body_bills``, which the body pass reaches
    without rebuilding them.
    """

    bill_text_dates: Mapping[str, str | None]
    #: Version codes with a published processed body, by bill. Keyed so a
    #: lookup costs one bill's codes: the scan it replaces ran over every held
    #: printing once per archive member, O(bills x printings) a run (3.6 s over
    #: 18,924 bills and 2,394 held printings at 55671b43, and growing with both).
    printings: Mapping[str, Collection[str]] = field(default_factory=dict)
    #: Version codes with a complete published XML body and section rows, by bill.
    xml_printings: Mapping[str, Collection[str]] = field(default_factory=dict)
    pairs: Collection[tuple[str, str, str]] = field(default_factory=frozenset)
    pending_bills: Collection[str] = field(default_factory=frozenset)
    #: Every published ``section_diffs`` key, by bill, less the bill: what a
    #: rebuild checks against the pairs its status establishes, retiring the rest.
    published_pairs: Mapping[str, Collection[tuple[str, str, str, str]]] = field(default_factory=dict)
    #: Published ``bill_versions`` rows, by bill, keyed under a code their own
    #: package now names another printing: ``(version_code, source, printing
    #: code)``. Before spicy-docs 0.37.0 a numbered reprint took its stage's
    #: slug, so 119-hr-6644's congress row ``engrossed-amendment-senate``
    #: carries ``eas2``'s package and date.
    miskeyed_rows: Mapping[str, Collection[tuple[str, str, str]]] = field(default_factory=dict)
    #: Bills with a listed, addressable printing that has no processed body, a
    #: consecutive XML pair with no complete published comparison, or a
    #: published comparison no consecutive pair accounts for.
    body_bills: Collection[str] = field(default_factory=frozenset)
    #: ``(bill_id, version_code)`` listed under both sources: a ``congress``
    #: placeholder left beside the processed ``govinfo`` body that stands for it.
    shadowed: Collection[tuple[str, str]] = field(default_factory=frozenset)
    #: The reader that last read each bill's status (``STATUS_READERS_KEY``).
    status_readers: Mapping[str, str] = field(default_factory=dict)
    #: Scoped rows whose stored BILLS target contradicts the publisher's law listing.
    invalid_targets: Mapping[tuple[str, str, str], BillVersionCapture] = field(default_factory=dict)

    def held_codes(self, bill_id: str) -> set[str]:
        """The version codes this bill has a published processed body for."""
        return set(self.printings.get(bill_id, ()))

    def xml_codes(self, bill_id: str) -> set[str]:
        """The version codes this bill has a complete published XML body and section rows for."""
        return set(self.xml_printings.get(bill_id, ()))


def _download_prior(
    output_dir: Path, download_prior: Callable[[str, Path], bool]
) -> tuple[dict[str, Path | None], PriorReplayRequired | None]:
    """Fetch each single-file prior table this run reads before merging, to the path the merge reuses in place.

    ``bill_sections`` is stored one file per Congress and comes through ``published_members`` instead. A
    replay-required ``REPLAYED_FROM_STATUS`` prior is absent here and returned, so the run can rebuild it whole.
    """
    paths: dict[str, Path | None] = {}
    replay = None
    for name in (
        *SNAPSHOT_TABLES,
        "bill_actions",
        "section_diffs",
        "section_diff_items",
        "cbo_cost_estimates",
        "bill_cosponsors",
        REPLAYED_FROM_STATUS,
        ARCHIVES_TABLE,
        BACKFILLS_TABLE,
        BACKFILL_WALKS_TABLE,
    ):
        try:
            paths[name] = published_table(output_dir, name, download_prior)
        except PriorReplayRequired as required:
            if name != REPLAYED_FROM_STATUS:
                raise
            paths[name], replay = None, required
    return paths, replay


def _require_replay_scope(
    replay: PriorReplayRequired, status_bills: Mapping[tuple[str, str], set[str]],
    congresses: Sequence[int], bill_types: Sequence[str],
) -> None:
    """A replay rebuilds every prior row, so the run must read every folder a prior BILLSTATUS read published."""
    published = {int(congress) for congress, _ in status_bills}
    newest = max([*published, *congresses, *default_congresses()])
    wanted = sorted({*published, *range(BULK_STATUS_FLOOR, newest + 1)})
    missing = sorted(set(wanted) - set(congresses))
    types = sorted(set(DEFAULT_BILL_TYPES) - set(bill_types))
    if missing or types:
        raise ValueError(
            f"{replay}. This run covers Congresses {list(congresses)} and bill types {list(bill_types)}; a replay "
            f"must re-read every bulk folder ({wanted[0]}-{wanted[-1]}, all eight bill types) or it would drop "
            f"the rows it cannot restate. Run once: {replay_dispatch(wanted[0], wanted[-1])}"
        )


def replay_dispatch(first: int, last: int) -> str:
    """The one dispatch that replays every bulk folder: the refusal names it, the tests derive it from here."""
    return f"gh workflow run rollup-bill-family.yml -R mikewolfd/spicy-regs -f congresses={first}-{last}"


def _prior_index(
    paths: Mapping[str, Path | None],
    sections: Sequence[Path] | None = None,
    *,
    in_scope: Callable[[str], bool] = lambda _bill: True,
) -> PriorIndex:
    """Read the skip lookups out of the prior tables without materialising them.

    ``sections`` are the prior ``bill_sections`` files: its members, or the one file it was before it was split.
    ``in_scope`` bounds target repairs and names the bills this run's status pass reaches.
    """
    import duckdb

    outcomes: dict[str, str | None] = {}
    version_counts: dict[str, int | None] = {}
    bills_path = paths.get("congress_bills")
    wants_versions = bills_path is not None and _has_columns(bills_path, ("bill_id", "version_count"))
    # A publisher stamp qualifies a skip only for a row both the CBO reader and the cosponsor
    # occurrence reader wrote, and only while both child tables are held: a row either reader
    # did not write cannot be skipped, even when the publisher's stamp is unchanged. Nor can a
    # row another reader, or none recorded, wrote (``STATUS_READERS_KEY``).
    readers, reader = _held_readers(paths.get(ARCHIVES_TABLE)), status_reader()
    wants_text = (
        bills_path is not None
        and paths.get("cbo_cost_estimates") is not None
        and paths.get("bill_cosponsors") is not None
        and _has_columns(
            bills_path, ("bill_id", "update_date_including_text", "cbo_cost_estimates_outcome", "cosponsors_outcome")
        )
    )
    if wants_versions and wants_text:
        # One scan serves both lookups where the prior used two: the text-dates
        # subset is split out of the same rows instead of re-reading the file.
        rows = duckdb.sql(
            f"SELECT bill_id, version_count, update_date_including_text, "
            f"cbo_cost_estimates_outcome IS NOT NULL AND cosponsors_outcome IS NOT NULL "
            f"FROM read_parquet('{bills_path}')"
        ).fetchall()
        version_counts = {bill: _int_or_none(count) for bill, count, _text, _read in rows}
        outcomes = {bill: text for bill, _count, text, read in rows if read}
    elif wants_versions:
        version_counts = {
            bill: _int_or_none(count)
            for bill, count in duckdb.sql(f"SELECT bill_id, version_count FROM read_parquet('{bills_path}')").fetchall()
        }
    elif wants_text:
        outcomes = dict(
            duckdb.sql(
                f"SELECT bill_id, update_date_including_text FROM read_parquet('{bills_path}') "
                "WHERE cbo_cost_estimates_outcome IS NOT NULL AND cosponsors_outcome IS NOT NULL"
            ).fetchall()
        )
    text_dates = {bill: text for bill, text in outcomes.items() if readers.get(bill) == reader}
    if stale := sum(1 for bill in outcomes if bill not in text_dates and in_scope(bill)):
        logger.info(
            "Bill family: {:,} prior bills in scope were read by another reader or by none recorded, so are read again",
            stale,
        )

    printings: dict[str, set[str]] = {}
    listed: dict[str, set[str]] = {}
    addressable: dict[str, set[str]] = {}
    sources: dict[tuple[str, str], set[str]] = {}
    dates: dict[tuple[str, str], str] = {}
    xml_printings: dict[str, set[str]] = {}
    section_counts = {}
    if sections and not _has_columns(sections, ("bill_id", "version_code", "source")):
        sections = None
    if sections:
        section_counts = {
            (bill, code): count
            for bill, code, count in duckdb.sql(
                f"SELECT bill_id, version_code, count(*) FROM {_scan(sections)} "
                "WHERE source = 'govinfo' GROUP BY bill_id, version_code"
            ).fetchall()
        }
    versions_path = paths.get("bill_versions")
    needed = (
        "bill_id",
        "version_code",
        "source",
        "sha256",
        "byte_size",
        "format_name",
        "content_type",
        "section_count",
        "cleanup_json",
        "version_date",
        "label",
        "package_id",
    )
    miskeyed: dict[str, set[tuple[str, str, str]]] = {}
    invalid_targets: dict[tuple[str, str, str], BillVersionCapture] = {}
    versions_qualified = versions_path is not None and _has_columns(versions_path, needed)
    if versions_qualified:
        offered = "offered_formats_json" if _has_columns(versions_path, ("offered_formats_json",)) else "NULL"
        for row in duckdb.sql(
            f"SELECT {', '.join(needed)}, {offered} AS offered_formats_json FROM read_parquet('{versions_path}')"
        ).to_arrow_table().to_pylist():
            invalid = (
                row["version_code"] in {"public-law", "private-law"}
                and row["label"] in {"Public Law", "Private Law"}
                and (row["package_id"] or "").startswith("BILLS-")
                and row["source"] in {LISTED_SOURCE, ACQUIRED_SOURCE}
                and in_scope(row["bill_id"])
            )
            if invalid:
                version = version_from_row(row)
                # Consult the same source policy as new acquisition. Keep the offered
                # law facts; regenerate every body-derived field through its shaper.
                if bill_text_package_id(BillIdentity.from_bill_id(row["bill_id"]), version) is not None:
                    raise ValueError("law listing unexpectedly selects a BILLS acquisition target")
                invalid_targets[(row["bill_id"], row["version_code"], row["source"])] = BillVersionCapture(
                    version=version, version_code=row["version_code"], source=LISTED_SOURCE,
                    package_id=None,
                )
            if row["source"] in {LISTED_SOURCE, ACQUIRED_SOURCE}:
                listed.setdefault(row["bill_id"], set()).add(row["version_code"])
                sources.setdefault((row["bill_id"], row["version_code"]), set()).add(row["source"])
                dates[(row["bill_id"], row["version_code"])] = row["version_date"] or ""
                if row["package_id"] and not invalid:
                    addressable.setdefault(row["bill_id"], set()).add(row["version_code"])
                if row["label"] and row["package_id"] and (code := _row_printing_code(row)) != row["version_code"]:
                    miskeyed.setdefault(row["bill_id"], set()).add((row["version_code"], row["source"], code))
            if invalid or row["source"] != ACQUIRED_SOURCE or not row["sha256"] or not row["byte_size"]:
                continue
            bill, code = row["bill_id"], row["version_code"]
            kind = body_kind(row["content_type"], row["format_name"])
            if kind == "xml":
                count = row["section_count"]
                if not sections or count is None or not count.isdigit():
                    continue
                if int(count) != section_counts.get((bill, code), 0):
                    continue
                xml_printings.setdefault(bill, set()).add(code)
            elif kind == "pdf" and row["cleanup_json"] is None:
                continue
            elif kind is None:
                continue
            printings.setdefault(bill, set()).add(code)
    pairs: set[tuple[str, str, str]] = set()
    published: dict[str, set[tuple[str, str, str, str]]] = {}
    diffs_path = paths.get("section_diffs")
    items_path = paths.get("section_diff_items")
    pair_identity = TABLE_CONTRACTS["section_diffs"].identity
    if (
        diffs_path is not None
        and items_path is not None
        and _has_columns(diffs_path, (*pair_identity, "item_count"))
        and _has_columns(items_path, (*pair_identity, "seq"))
    ):
        columns = ", ".join(pair_identity)
        child_counts = {
            tuple(row[:-1]): row[-1]
            for row in duckdb.sql(
                f"SELECT {columns}, count(*) FROM read_parquet('{items_path}') GROUP BY {columns}"
            ).fetchall()
        }
        for row in duckdb.sql(f"SELECT {columns}, item_count FROM read_parquet('{diffs_path}')").fetchall():
            key, count = tuple(row[:-1]), _int_or_none(row[-1])
            published.setdefault(key[0], set()).add(key[1:])
            if (
                key[2] == ACQUIRED_SOURCE
                and key[4] == ACQUIRED_SOURCE
                and count is not None
                and count >= 0
                and count == child_counts.get(key, 0)
            ):
                pairs.add((key[0], key[1], key[3]))
    logger.info(
        "Bill family: {:,} prior bills qualify for a stamp skip; {:,} processed printings",
        len(text_dates),
        sum(len(codes) for codes in printings.values()),
    )
    # A body is the body pass's to read, by the printing's own state; the
    # status is read again only when a status fact is in doubt.
    body_bills = {bill for bill, codes in addressable.items() if not codes <= printings.get(bill, set())}
    # The surviving version rows cannot establish which source rows were lost.
    # Source counts are independent of acquisition paths: congress/govinfo
    # duplicates count once, and uploaded/PDF-twin rows never fill a source gap.
    pending_bills = {
        bill for bill in text_dates if not versions_qualified or version_counts.get(bill) != len(listed.get(bill, ()))
    }
    # A mis-keyed row reopens its bill: the rebuild retires it (see ``miskeyed_rows``).
    pending_bills.update(miskeyed)
    for bill, codes in listed.items():
        established = _code_pairs([(code, dates[(bill, code)]) for code in sorted(codes)])
        xml = xml_printings.get(bill, set())
        # A missing comparison, and a published one no established neighbour
        # pair accounts for -- backward, or skipping a printing -- are both the
        # body pass's: it computes the first and retires the second.
        if any(
            older in xml and newer in xml and (bill, older, newer) not in pairs for older, newer in established
        ) or any((key[0], key[2]) not in established for key in published.get(bill, ())):
            body_bills.add(bill)
    shadowed = {
        (bill, code)
        for (bill, code), stated in sources.items()
        if stated == {LISTED_SOURCE, ACQUIRED_SOURCE} and code in printings.get(bill, ())
    }
    body_bills.update(bill for bill, _code in shadowed)
    if bills_path is not None and _has_columns(bills_path, ("bill_id", "url", "url_source")):
        # Pre-label rows can carry the same API URL without the retired
        # writer's marker. Body completion does not qualify that URL: reread
        # its status once so the native or inherited link gets its provenance.
        unqualified_urls = {
            bill
            for (bill,) in duckdb.sql(
                f"SELECT bill_id FROM read_parquet('{bills_path}') "
                f"WHERE url_source = '{RETIRED_LIST_URL_SOURCE}' OR (url IS NOT NULL AND url_source IS NULL)"
            ).fetchall()
        }
        if unqualified_urls:
            logger.info(
                "Bill family: {:,} bills have retired or unlabelled URL provenance; those in scope are re-read",
                len(unqualified_urls),
            )
        pending_bills.update(unqualified_urls)
    return PriorIndex(
        text_dates, printings, xml_printings, pairs, pending_bills, published, miskeyed, body_bills, shadowed, readers,
        invalid_targets,
    )


def _target_repair_rows(index: PriorIndex, fresh: Sequence[Mapping[str, Any]], engine: EngineStamp) -> BillFamilyTables:
    """Re-shape only invalid held listings not already refreshed from BILLSTATUS."""
    refreshed = {(row["bill_id"], row["version_code"]) for row in fresh}
    captures: dict[tuple[str, str], BillVersionCapture] = {}
    for (bill, code, source), capture in index.invalid_targets.items():
        if (bill, code) not in refreshed and ((bill, code) not in captures or source == LISTED_SOURCE):
            captures[(bill, code)] = capture
    return BillFamilyTables.concat([
        build_bill_printings(BillIdentity.from_bill_id(bill), (capture,), engine=engine, diff=False)
        for (bill, _code), capture in captures.items()
    ])


def _incident_pairs(paths: Mapping[str, Path | None], parents: Collection[tuple[str, str, str]]) -> set[tuple[str, ...]]:
    """Only comparisons that name a withdrawn printing, including orphaned derived rows."""
    import duckdb

    if not parents:
        return set()
    retired = set(parents)
    identity = TABLE_CONTRACTS["section_diffs"].identity
    pairs: set[tuple[str, ...]] = set()
    for name in ("section_diffs", "section_diff_items", "financial_changes", "diff_summaries"):
        path = paths.get(name)
        if path is None or not _has_columns(path, identity):
            continue
        for key in duckdb.sql(f"SELECT DISTINCT {', '.join(identity)} FROM read_parquet('{path}')").fetchall():
            if (key[0], key[1], key[2]) in retired or (key[0], key[3], key[4]) in retired:
                pairs.add(key)
    return pairs


def _held_readers(path: Path | None) -> dict[str, str]:
    """``bill_id`` -> the reader that last read its status, from the archives table's metadata.

    An absent or unreadable record holds no reader, so every bill's status is read again: never a skip.
    """
    import pyarrow.parquet as pq

    raw = None if path is None else (pq.read_schema(path).metadata or {}).get(STATUS_READERS_KEY.encode())
    if not raw:
        return {}
    try:
        return {
            f"{folder}-{number}": reader
            for reader, folders in json.loads(raw).items()
            for folder, runs in folders.items()
            for first, _, last in (run.partition("-") for run in runs.split(","))
            for number in range(int(first), int(last or first) + 1)
        }
    except (ValueError, TypeError, AttributeError):
        logger.warning("Bill family: the status reader record is unreadable; every bill's status is read again")
        return {}


def _readers_metadata(readers: Mapping[str, str]) -> str:
    """The record :func:`_held_readers` reads: each reader's bill numbers by folder, as ``first-last`` runs."""
    numbers: dict[str, dict[str, list[int]]] = {}
    for bill, reader in readers.items():
        folder, _, number = bill.rpartition("-")
        if number.isdecimal():  # a BILLSTATUS identity's number always is; any other bill is read again
            numbers.setdefault(reader, {}).setdefault(folder, []).append(int(number))
    record: dict[str, dict[str, str]] = {}
    for reader, folders in sorted(numbers.items()):
        for folder, held in sorted(folders.items()):
            runs: list[list[int]] = []
            for number in sorted(held):
                if runs and number == runs[-1][1] + 1:
                    runs[-1][1] = number
                else:
                    runs.append([number, number])
            record.setdefault(reader, {})[folder] = ",".join(
                str(first) if first == last else f"{first}-{last}" for first, last in runs
            )
    return json.dumps(record, separators=(",", ":"))


#: The reason a fresh row repeating a key is refused. The merge keeps one row
#: per contract identity, so a repeat would publish as one and drop the other
#: without a word -- what ``bill_sections`` did before spicy-docs 0.35.0 keyed
#: it on ``seq`` (119-hr-5334 enrolled: two divisions' ``Sec. 1``; 119-hr-9022
#: reported: two paragraphs under one heading).
REPEATED_KEY_REASON = "repeats the key of a row this run already produced"


def _refuse_repeated_keys(tables: BillFamilyTables) -> BillFamilyTables:
    """Keep each contract key's first row and refuse the rest by name, before any merge sees them.

    The provider refuses a repeat within one bill's pass; this covers the run,
    so no merge collapses two produced rows into one. One pass over every row.
    """
    refusals: list[FamilyRefusal] = []
    kept: dict[str, tuple[dict, ...]] = {}
    for contract, attr in FAMILY_TABLES:
        identity = TABLE_CONTRACTS[contract].identity
        seen: set[tuple[str | None, ...]] = set()
        rows = []
        for row in getattr(tables, attr):
            key = tuple(row[column] for column in identity)
            if key in seen:
                refusals.append(FamilyRefusal(contract, tuple(value or "" for value in key), REPEATED_KEY_REASON))
                continue
            seen.add(key)
            rows.append(row)
        kept[attr] = tuple(rows)
    if not refusals:
        return tables
    return replace(tables, **kept, refusals=(*tables.refusals, *refusals))


def _complete_child_scopes(
    parent: str,
    parents: Sequence[Mapping[str, Any]],
    children: Sequence[Mapping[str, Any]],
    count_column: str,
) -> set[tuple[str, ...]]:
    """Successfully shaped child scopes, including explicitly empty results."""
    identity = TABLE_CONTRACTS[parent].identity
    counts = Counter(tuple(row[column] for column in identity) for row in children)
    complete = set()
    for row in parents:
        key = tuple(row[column] for column in identity)
        count = _int_or_none(row[count_column])
        if all(isinstance(value, str) for value in key) and count is not None and count >= 0 and count == counts[key]:
            complete.add(key)
    return complete


def _scan(source: Path | Sequence[Path]) -> str:
    """A prior table's file, or a split table's members, as one relation; Hive partitioning off.

    A member's ``congress=<N>`` directory would otherwise add a second, integer ``congress`` beside the stored one.
    """
    return parquet_scan([str(source)] if isinstance(source, Path) else [str(path) for path in source])


def _has_columns(source: Path | Sequence[Path], needed: Sequence[str]) -> bool:
    """Whether a prior table predates the columns a lookup needs."""
    import duckdb

    present = {str(row[0]) for row in duckdb.sql(f"DESCRIBE SELECT * FROM {_scan(source)}").fetchall()}
    missing = [column for column in needed if column not in present]
    if missing:
        label = source.name if isinstance(source, Path) else source[0].parent.name
        logger.info("Bill family: prior {} lacks {} — treating as cold for that lookup", label, missing)
    return not missing


def _archive_row(entry: BulkListingEntry, *, congress: int, bill_type: str, observed_at: str | None) -> dict:
    """One ``bill_family_archives`` row: the folder's own zip entry as this run saw it."""
    return {
        "name": text(entry.name),
        "link": text(entry.link),
        "formatted_last_modified_time": text(entry.formatted_last_modified_time),
        "modified_at": text(entry.modified_at.isoformat()),
        "size": text(entry.size),
        "congress": text(congress),
        "bill_type": text(bill_type),
        "observed_at": text(observed_at),
    }


def _vote_reference_row(reference: VoteReference, *, full_action_name: str | None, observed_at: str | None) -> dict:
    """One ``bill_vote_references`` row from one reference the bill's own actions carry."""
    return {
        "bill_id": bill_key(reference.bill),
        "chamber": text(reference.vote.chamber),
        "congress": text(reference.vote.congress),
        "session": text(reference.vote.session),
        "roll_number": text(reference.vote.roll_number),
        "action_index": text(reference.action_index),
        "url": text(reference.url),
        "date": text(reference.date),
        "full_action_name": text(full_action_name),
        "observed_at": text(observed_at),
    }


def _full_action_name(action: Any, reference: VoteReference) -> str | None:
    """The seventh, optional field of the entry ``reference`` was read from.

    ``VoteReference`` carries the six sealed fields and not this one, so the
    entry is found again on its action by the same key the reference states.
    """
    for entry in getattr(action, "recorded_votes", ()):
        try:
            if read_vote_key(entry) == reference.vote:
                return getattr(entry, "full_action_name", None)
        except VoteMatchError:  # an entry the reader refused has no name to give
            continue
    return None


def _latest_capture_only(path: Path) -> int:
    """Drop each bill's reference rows from any capture but its newest; return how many went.

    Congress.gov lists a bill's actions newest-first, so ``action_index`` moves with every action added and an
    earlier capture's row no longer names its action. A bill this run read is replaced at the merge
    (``replace_parents``); this sweeps the bills a run does not read again, whose rows from two captures survived
    before that scope existed (33 rows over 9 bills in the live table on 2026-10-03). One capture per bill per run,
    so the newest ``observed_at`` is the bill's current statement.
    """
    import duckdb

    source = f"read_parquet('{path}')"
    superseded = "observed_at IS DISTINCT FROM max(observed_at) OVER (PARTITION BY bill_id)"
    counted = duckdb.sql(f"SELECT count(*) FROM (SELECT 1 FROM {source} QUALIFY {superseded})").fetchone()
    swept = int(counted[0]) if counted else 0
    if swept:
        staged = path.with_name(f".{path.name}.partial")
        duckdb.sql(
            f"COPY (SELECT * FROM {source} QUALIFY NOT ({superseded}) "
            f"ORDER BY observed_at DESC, {', '.join(VOTE_REFERENCE_IDENTITY)}) "
            f"TO '{staged}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)"
        )
        staged.replace(path)
    return swept


def vote_reference_rows(status: Any, *, observed_at: str | None) -> tuple[list[dict], int]:
    """Every recorded vote on one bill's actions as rows, and how many entries were refused.

    A refused entry — one missing any of the six sealed fields — costs that
    entry, not the bill; the count is what the run log reports.
    """
    references = recorded_vote_references(status.identity, status.actions)
    rows = [
        _vote_reference_row(
            reference,
            full_action_name=_full_action_name(status.actions[reference.action_index], reference)
            if reference.action_index is not None
            else None,
            observed_at=observed_at,
        )
        for reference in references.references
    ]
    return rows, len(references.refusals)


def _comparison_entry(row: Mapping[str, Any]) -> BulkListingEntry | None:
    """Rebuild, from a retained row, the entry ``acquire`` compares this folder's listing against.

    ``acquire`` reads four of :class:`BulkListingEntry`'s eleven fields:
    ``name`` and ``link``, which must name this folder's own zip or the skip is
    refused outright rather than risk comparing a different file, and
    ``modified_at`` and ``size``, which must match for the zip to be skipped.
    Those four are what this table retains. The other six the publisher states
    — display label, just-file-name, folder flag, mime type, file extension and
    formatted size — are not retained and are left empty here rather than
    guessed at: this is a comparison value, not a republication of the
    listing. ``modified_at`` round-trips through ISO 8601 rather than being
    re-parsed out of ``formatted_last_modified_time``, so the publisher's own
    ``DD-Mon-YYYY HH:MM`` rule stays spicy-docs' and is never restated here;
    the stamp string travels beside it as the evidence the instant came from.

    A row missing any of the four is no comparison value at all, and returning
    ``None`` means the next ``acquire`` downloads the zip the way a cold run
    does.
    """
    stated = [row.get(column) for column in ("name", "link", "modified_at", "size")]
    if any(value is None for value in stated):
        return None
    name, link, modified_at, size = stated
    try:
        instant = datetime.fromisoformat(str(modified_at))
        byte_size = int(str(size))
    except ValueError:
        logger.warning("Bill family: retained archive row for {} is unreadable — its zip will be downloaded", link)
        return None
    return BulkListingEntry(
        name=str(name),
        display_label="",
        just_file_name="",
        link=str(link),
        folder=False,
        formatted_last_modified_time=str(row.get("formatted_last_modified_time") or ""),
        modified_at=instant,
        mime_type=None,
        file_extension=None,
        formatted_size=None,
        size=byte_size,
    )


def _held_archives(path: Path | None) -> dict[tuple[str, str], BulkListingEntry]:
    """``(congress, bill_type)`` -> the zip entry the last run retained for that folder."""
    if path is None or not _has_columns(path, ARCHIVE_COLUMNS):
        return {}
    import duckdb
    import pyarrow.parquet as pq

    metadata = pq.read_schema(path).metadata or {}
    raw = next((metadata[key.encode()] for key in ARCHIVE_COMPLETION_KEYS_READ if key.encode() in metadata), None)
    try:
        completed = set(tuple(scope) for scope in json.loads(raw)) if raw else set()
    except (ValueError, TypeError):
        completed = set()
    columns = ", ".join(ARCHIVE_COLUMNS)
    held: dict[tuple[str, str], BulkListingEntry] = {}
    for row in duckdb.sql(f"SELECT {columns} FROM read_parquet('{path}')").to_arrow_table().to_pylist():
        entry = _comparison_entry(row)
        scope = (str(row["congress"]), str(row["bill_type"]))
        if entry is not None and row["congress"] and row["bill_type"] and scope in completed:
            held[scope] = entry
    logger.info("Bill family: {} folder listing entries retained from the last run", len(held))
    return held


def _comparable_entry(held: BulkListingEntry | None, congress: int, bill_type: str) -> BulkListingEntry | None:
    """The retained entry, but only while it still names this folder's own zip.

    ``acquire`` refuses an entry whose ``name`` or ``link`` differs from the
    folder's zip entry — it describes a different file, so comparing its stamp
    would risk a false skip — and it refuses it *after* reading the listing.
    Those two fields are not listing facts, though: ``bulk_status_locator``
    spells the zip's address, and ``read_bulk_listing`` proves every entry
    against that same locator before returning one. So a stale row is
    recognised here for no requests at all, and the folder falls back to a cold
    download that pays one listing read — the one ``_retained_entry`` makes to
    replace the row — instead of that plus the refused call's own.

    Checked rather than caught for the same reason: the refusal carries no
    listing to reuse, and retrying around it would ask the publisher twice for
    something this run can already answer.
    """
    if held is None:
        return None
    locator = bulk_status_locator(congress, bill_type)
    # The comparison ``acquire`` makes, against the address it would compare to.
    if (held.name, held.link) == (locator.rsplit("/", 1)[-1], locator):
        return held
    logger.warning(
        "Bill family: {} {} retained entry names {} rather than this folder's own zip — downloading it",
        congress,
        bill_type,
        held.name,
    )
    return None


def _acquire_archive(acquirer: BulkStatusSource, congress: int, bill_type: str, held: BulkListingEntry | None) -> Any:
    """One folder's archive, skipping the zip when the retained entry still describes it."""
    entry = _comparable_entry(held, congress, bill_type)
    if entry is None:
        return acquirer.acquire(congress, bill_type)
    return acquirer.acquire(congress, bill_type, unchanged_since=entry)


def _retained_entry(acquirer: BulkStatusSource, acquisition: Any, congress: int, bill_type: str) -> dict | None:
    """The row to retain for this folder, so the next run can skip its zip.

    ``acquire`` reads the folder listing only when it was given something to
    compare against, so a folder seen for the first time comes back with no
    ``listing_entry`` and the listing is asked for here — one small request,
    once per folder, and without it the next run would have nothing to prove
    the zip unchanged with and would download it again.
    """
    entry, capture = acquisition.listing_entry, acquisition.listing_capture
    if entry is None or capture is None:
        try:
            listing = acquirer.list_archives(congress, bill_type)
        except SourceEvidenceError:
            raise
        except Exception as error:  # noqa: BLE001 — no entry means no skip next run, not a failed run
            logger.warning(
                "Bill family: {} {} listing refused, so the next run cannot skip its zip: {}",
                congress,
                bill_type,
                scrub_credential(str(error), ""),
            )
            return None
        entry, capture = listing.listing.zip_entry, listing.capture
    return _archive_row(entry, congress=congress, bill_type=bill_type, observed_at=capture.observed_at)


def _held_actions(actions: Path, bill_ids: Collection[str] | None = None) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    """Each bill's published ``bill_actions`` rows, for ``bill_ids`` (all when None), as the stage rule reads them.

    The rows come as they are published, in publisher order (``action_index``), newest first, as ``bill_family``
    passes a BILLSTATUS list: ``infer_stage`` reads published rows, and their ``source_system_name`` is what a
    chamber-aware rule reads (a Senate bill "Held at the desk." by the House is in the other chamber). Each row also
    states its ``action_index`` and its ``file_row_number`` in ``actions``. Streamed a batch at a time, so the
    table is never one Python list. A bill with no published action yields nothing: no rule read it.
    """
    import duckdb

    if not _has_columns(actions, ("bill_id", "action_index", "action_text", "action_date")):
        return
    names = ("action_index", "file_row_number", *(c for c in _STAGE_READ_COLUMNS if _has_columns(actions, (c,))))
    with duckdb.connect() as con:
        only = ""
        if bill_ids is not None:
            con.execute("CREATE TEMP TABLE wanted AS SELECT UNNEST(?::VARCHAR[]) AS bill_id", [sorted(bill_ids)])
            only = "SEMI JOIN wanted w ON t.bill_id = w.bill_id"
        cursor = con.execute(
            f"SELECT t.bill_id, {', '.join(f't.{name}' for name in names)} "
            f"FROM read_parquet('{actions}', file_row_number = true) t {only} "
            "ORDER BY t.bill_id, TRY_CAST(t.action_index AS INTEGER)"
        )
        def stream() -> Iterator[tuple[Any, ...]]:
            while batch := cursor.fetchmany(50_000):
                yield from batch

        for bill, group in groupby(stream(), key=lambda row: row[0]):
            yield bill, [dict(zip(names, values)) for _, *values in group]


def rederived_stages(
    actions: Path | None, bill_ids: Collection[str] | None = None, versions: Path | None = None
) -> dict[str, str]:
    """Each bill's stage as the running rule reads its published ``bill_actions``, for ``bill_ids`` (all when None).

    With the bill's own type and its listed enrolled text, as ``bill_family`` reads a BILLSTATUS document, so an
    unchanged bill re-derives the stage this run publishes, and no rule change surfaces as a ``stage_changed`` event.
    A key the bill grammar refuses (16 of the 6th-42nd Congresses) is read without its type.
    """
    if actions is None:
        return {}
    listed = _listed_versions(versions, bill_ids)
    return {
        bill: infer_stage(rows, newest_first=True, bill_type=_bill_type(bill), text_versions=listed.get(bill, ())).stage
        for bill, rows in _held_actions(actions, bill_ids)
    }


def _listed_versions(versions: Path | None, bill_ids: Collection[str] | None = None) -> dict[str, list[dict[str, Any]]]:
    """Each bill's published ``bill_versions`` rows that list an enrolled text, as the stage rule reads them.

    spicy-docs' ``listed_enrolled_text`` decides which rows (an "Enrolled Bill" label from the BILLSTATUS list:
    ``source`` congress or govinfo); only those are kept, 4,990 of 226,033 rows on 2026-10-04. Each row is passed
    with its ``source``, and the rule's ``ValueError`` for a row that states none of the contract's four is the
    tripwire, never caught: a table read as listing nothing puts a rebuilt bill back to `passed_both`.

    The native layout states no ``source``. Its ``printing_id`` is a digest of the bill, the version code and the
    source, the row's key and no source (the rule refuses it as one), so here a row's source is the one whose key it
    carries, and a key no source gives is passed as no source, for the rule to refuse. No table (a cold start) lists
    nothing. One columnar read, O(rows).
    """
    if versions is None:
        return {}
    import duckdb

    wanted = None if bill_ids is None else frozenset(bill_ids)
    sources = sorted(BILL_VERSION_SOURCES)
    listed: dict[str, list[dict[str, Any]]] = {}
    with duckdb.connect() as con:
        present = {str(row[0]) for row in con.execute(f"DESCRIBE SELECT * FROM {_scan(versions)}").fetchall()}
        native = "source" not in present and "printing_id" in present
        columns = "version_code, printing_id" if native else "NULL, source"
        rows = con.execute(f"SELECT bill_id, label, {columns} FROM {_scan(versions)}").fetchall()
        for bill, label, code, source in rows:
            if wanted is not None and bill not in wanted:
                continue
            if native:
                key = source
                source = next((stated for stated in sources if printing_id(bill, code, stated) == key), None)
            row = {"label": label, "source": source}
            if listed_enrolled_text([row]) is not None:
                listed.setdefault(bill, []).append(row)
    return listed


#: The published ``bill_actions`` columns the stage rule reads, by their published names.
_STAGE_READ_COLUMNS = ("action_text", "action_code", "action_type", "action_date", "action_time", "source_system_name")

#: The columns a bill's stage and signing findings fill, by the finding's field, as ``shape_bill`` fills them; the
#: first three are also what ``shape_bill_action`` publishes for one action's own finding.
_STAGE_COLUMNS = {
    "stage": "stage",
    "stage_rule": "rule",
    "stage_matcher": "matcher",
    "stage_action_index": "action_index",
    "stage_action_date": "action_date",
    "stage_source_text": "source_text",
}
_SIGNING_COLUMNS = {
    "signed_date": "signed_date",
    "signed_date_rule": "rule",
    "signed_date_action_index": "action_index",
    "signed_date_action_code": "action_code",
}
_ACTION_STAGE_COLUMNS = ("stage", "stage_rule", "stage_matcher")


def restage_held_rows(bills: Path, actions: Path, versions: Path | None = None) -> tuple[int, int]:
    """Read every merged bill's stage and signing date, and every merged action's stage, again by the running rule.

    A row carries the rule of the run that last read its bill, and a bill is read again only when its BILLSTATUS
    moves, so a rule change reached a Congress only when a dispatch re-read it (round 6's `cleared` stage, a mention
    read as no step and a private law's signing date: every Congress from the 108th was re-staged by hand on
    2026-10-03). The rules read nothing the stored actions do not hold, so each run re-reads them all with
    spicy-docs' own functions, for no publisher request: a bill this run read comes back as it was shaped, and a held
    one moves with the rule. The stage reads the bill's published ``bill_versions`` rows as its listed text versions
    (an enrolled text listed raises `passed_both` to `cleared`, spicy-docs 0.57.0). The signing date reads the row's
    own ``law_type`` and ``public_law_number`` as its law entry (a private law states no number). A bill with no
    stored action keeps its row: no rule read one (the 82nd-107th detail-route rows). O(actions + versions), streamed
    in publisher order; a table is read whole and rewritten only when a row moved. Returns how many bill rows and
    action rows moved.
    """
    import pyarrow.parquet as pq

    per_action = pq.read_table(actions, columns=list(_ACTION_STAGE_COLUMNS)).to_pydict()
    per_bill = pq.read_table(bills, columns=["bill_id", "law_type", "public_law_number", *_STAGE_COLUMNS,
                                             *_SIGNING_COLUMNS]).to_pydict()
    place = {bill: index for index, bill in enumerate(per_bill.pop("bill_id"))}
    law_types, numbers = per_bill.pop("law_type"), per_bill.pop("public_law_number")
    listed = _listed_versions(versions)
    moved_bills = moved_actions = 0
    # One reading per distinct thing the action rule reads (no date: only a bill's stage orders by date): 214,670
    # of the 931,774 published actions on 2026-10-04, which took the whole pass from 37.8 s to 28.6 s.
    readings: dict[tuple[Any, ...], dict[str, Any]] = {}
    for bill, rows in _held_actions(actions):
        bill_type = _bill_type(bill)
        for row in rows:
            read = (bill_type, *(row.get(c) for c in ("action_code", "action_text", "action_type", "source_system_name")))
            if (values := readings.get(read)) is None:
                finding = infer_stage_from_action(row, bill_type=bill_type)
                values = readings[read] = {c: getattr(finding, _STAGE_COLUMNS[c]) for c in _ACTION_STAGE_COLUMNS}
            moved_actions += _put(per_action, row["file_row_number"], values)
        index = place.get(bill)
        if index is None:
            continue
        laws = [{"type": law_types[index], "number": numbers[index] or ""}] if law_types[index] else []
        stage = infer_stage(rows, newest_first=True, bill_type=bill_type, text_versions=listed.get(bill, ()))
        signing = signed_date({"laws": laws, "actions": rows})
        values = {column: getattr(stage, name) for column, name in _STAGE_COLUMNS.items()}
        values |= {column: getattr(signing, name) for column, name in _SIGNING_COLUMNS.items()}
        # A finding's action_index is a place in ``rows``; each row states its own.
        for column in ("stage_action_index", "signed_date_action_index"):
            values[column] = None if values[column] is None else rows[values[column]]["action_index"]
        moved_bills += _put(per_bill, index, {column: text(value) for column, value in values.items()})
    for path, moved, columns in ((bills, moved_bills, per_bill), (actions, moved_actions, per_action)):
        if moved:
            table = pq.read_table(path)
            for column, values in columns.items():
                table = set_column(table, column, values)
            write_in_place(table, path)
    return moved_bills, moved_actions


def _put(columns: dict[str, list[Any]], index: int, values: Mapping[str, Any]) -> bool:
    """Set row ``index`` of ``columns`` to ``values``; whether any value moved."""
    moved = any(columns[column][index] != value for column, value in values.items())
    for column, value in values.items():
        columns[column][index] = value
    return moved


def _bill_type(bill_id: str) -> str | None:
    """The type a ``bill_id`` names, through its owner's grammar; None for a key it refuses."""
    try:
        return BillIdentity.from_bill_id(bill_id).bill_type
    except BillSourceError:
        return None


def _prior_snapshot(paths: Mapping[str, Path | None], bill_ids: Collection[str]) -> Any:
    """The prior rows for *this run's* bills only, as an event snapshot, each prior stage read by the running rule.

    Filtered in DuckDB rather than in Python: activity events only ever compare
    bills this run rebuilt, so pulling the whole published table into a list to
    compare a few thousand of them is work with no answer attached to it.

    A stage rule that changes moves the published stage of bills whose actions did not (spicy-docs 0.51.0 moved
    154,374), and a ``stage_changed`` event for each would state a rule change as a legislative one. So each prior
    bill's stage is re-derived from its published actions under the running rule (owner decision 2026-09-28, over a
    blanket switch): a bill whose actions did not change compares equal, and one whose actions did emits the move
    the running rule reads. A bill with no published action keeps its published stage.
    """
    import duckdb

    rows: dict[str, list[dict]] = {name: [] for name in SNAPSHOT_TABLES}
    if not bill_ids:
        return snapshot_from_rows(**{_SNAPSHOT_ARG[name]: rows[name] for name in SNAPSHOT_TABLES})

    wanted = duckdb.sql("SELECT UNNEST(?::VARCHAR[]) AS bill_id", params=[sorted(bill_ids)])  # noqa: F841 — referenced by name in the queries below
    for name in SNAPSHOT_TABLES:
        path = paths.get(name)
        if path is None:
            continue
        columns = TABLE_CONTRACTS[name].columns
        present = {str(r[0]) for r in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
        select = ", ".join(f"t.{c}" if c in present else f"CAST(NULL AS VARCHAR) AS {c}" for c in columns)
        rows[name] = (
            duckdb.sql(f"SELECT {select} FROM read_parquet('{path}') t SEMI JOIN wanted w ON t.bill_id = w.bill_id")
            .to_arrow_table()
            .to_pylist()
        )
        logger.info("Bill family: prior {} contributes {:,} rows for this run's bills", name, len(rows[name]))
    stages = rederived_stages(paths.get("bill_actions"), bill_ids, paths.get("bill_versions"))
    moved = 0
    for row in rows["congress_bills"]:
        stage = stages.get(row["bill_id"])
        if stage is not None and stage != row["stage"]:
            row["stage"] = stage
            moved += 1
    if moved:
        logger.info("Bill family: {:,} prior stages move under the running rule alone, so emit no event", moved)
    return snapshot_from_rows(**{_SNAPSHOT_ARG[name]: rows[name] for name in SNAPSHOT_TABLES})


# --------------------------------------------------------------------------- #
# The pre-BILLSTATUS backfill: the 82nd-107th from the API ``bill`` route.
# --------------------------------------------------------------------------- #

#: A state key: ``(congress, bill_type, number)`` as the state table spells them.
type BackfillKey = tuple[str, str, str]


def _detail_str(value: Any) -> str | None:
    """A detail field as text: ``None`` stays ``None``, a number spells as the publisher wrote it.

    The reader parses JSON numbers as decimals, so ``str`` keeps ``2185`` from
    ever becoming ``2185.0`` on the way into a VARCHAR column.
    """
    return None if value is None else str(value)


def backfill_status(identity: BillIdentity, record: Mapping[str, Any]) -> BillStatus:
    """One bill's status from its detail record: what the route states, nothing else.

    ``latestAction`` (date and text only — the coded fields live in the
    actions sub-route, not the record), ``laws`` and ``sponsors`` arrive in
    the record itself; the actions, committees, titles, subjects, summaries
    and text versions arrive only as sub-route counts, so those status fields
    come back empty rather than invented, and the shaper's derived zeros and
    defaults are NULLed downstream (:data:`BACKFILL_UNSUBSTANTIATED`).
    ``schema_version`` names the BILLSTATUS schema and the detail route states
    none, so it is empty here and NULL in the published row.
    """
    latest = record.get("latestAction")
    latest_action = None
    if isinstance(latest, Mapping) and (latest.get("actionDate") is not None or latest.get("text") is not None):
        latest_action = BillAction(
            text=_detail_str(latest.get("text")),
            action_date=_detail_str(latest.get("actionDate")),
            action_time=_detail_str(latest.get("actionTime")),
            action_code=None,
            action_type=None,
            source_system_code=None,
            source_system_name=None,
        )
    title = _detail_str(record.get("title"))
    if not title:
        raise ValueError(f"{identity.congress}/{identity.bill_type}/{identity.number} detail record states no title")
    policy_area = record.get("policyArea")
    return BillStatus(
        identity=identity,
        schema_version="",
        title=title,
        origin_chamber=_detail_str(record.get("originChamber")),
        introduced_date=_detail_str(record.get("introducedDate")),
        update_date=_detail_str(record.get("updateDate")),
        update_date_including_text=_detail_str(record.get("updateDateIncludingText")),
        legislation_url=_detail_str(record.get("legislationUrl")),
        latest_action=latest_action,
        policy_area=_detail_str(policy_area.get("name")) if isinstance(policy_area, Mapping) else None,
        subjects=(),
        summaries=(),
        actions=(),
        sponsors=tuple(
            BillSponsor(_detail_str(entry.get("bioguideId")), _detail_str(entry.get("fullName")))
            for entry in record.get("sponsors") or ()
            if isinstance(entry, Mapping)
        ),
        text_versions=(),
        laws=tuple(
            BillLaw(_detail_str(entry.get("number")), _detail_str(entry.get("type")))
            for entry in record.get("laws") or ()
            if isinstance(entry, Mapping)
        ),
    )


def _backfill_identity(congress: int, bill_type: str, record: Mapping[str, Any]) -> BillIdentity | None:
    """The identity a list record names, or ``None`` when it names nothing this walk can address.

    A record whose type is not the one walked, or whose number is not a
    positive integer, cannot address a detail request from this walk at all;
    it is counted as unwalkable on the walk row rather than retried, since no
    identity exists to retry.
    """
    try:
        if str(record["type"]).lower() != bill_type:
            return None
        return BillIdentity(congress=congress, bill_type=bill_type, number=int(str(record["number"])))
    except (KeyError, TypeError, ValueError, BillSourceError):
        return None


def _sub_route_count(record: Mapping[str, Any], name: str) -> str | None:
    """A sub-route's stated ``count``, or ``None`` when the record states no such sub-route."""
    value = record.get(name)
    count = value.get("count") if isinstance(value, Mapping) else None
    return _detail_str(count) if isinstance(count, int) and not isinstance(count, bool) else None


def _backfill_bill_row(row: Any, record: Mapping[str, Any]) -> Any:
    """NULL what this row cannot substantiate, then prove it still satisfies the contract.

    :data:`BACKFILL_UNSUBSTANTIATED` names the columns. ``cosponsor_count`` is
    the one count the record *does* state — ``cosponsors`` is its own
    sub-route with a ``count`` — so it is published from there; never from
    the shaper's ``len(sponsors) - 1``, which on the detail route is the
    sponsor list and has nothing to do with cosponsors (measured: the A11
    receipt's 107/hr/3162 has one sponsor and ``cosponsors.count`` 1).
    """
    adjusted: dict[str, Any] = dict(row)
    for column in BACKFILL_UNSUBSTANTIATED:
        adjusted[column] = None
    adjusted["cosponsor_count"] = _sub_route_count(record, "cosponsors")
    # The shaper labels any stated url as BILLSTATUS's; this row's came from the detail record.
    adjusted["url_source"] = "congress_api" if adjusted.get("url") else None
    return TABLE_CONTRACTS["congress_bills"].checked(adjusted)


@dataclass(slots=True)
class BackfillState:
    """What the state table says about every bill the backfill has attempted, kept live through a run.

    ``filled`` maps a key to the list stamp it was filled under — a matching
    stamp costs no request; ``refused`` maps a key to the stamp its record
    carried when the attempt failed, and is what the next run retries first.
    A key is in one or the other, never both.
    """

    filled: dict[BackfillKey, str] = field(default_factory=dict)
    refused: dict[BackfillKey, str] = field(default_factory=dict)
    #: The error class each refusal recorded (the state table's ``refusal``).
    reasons: dict[BackfillKey, str] = field(default_factory=dict)

    def count(self, congress: str, bill_type: str) -> tuple[int, int]:
        """``(filled, refused)`` for one walk unit."""
        prefix = (congress, bill_type)
        return (
            sum(1 for key in self.filled if key[:2] == prefix),
            sum(1 for key in self.refused if key[:2] == prefix),
        )


def _held_backfills(path: Path | None) -> BackfillState:
    """The state table as the last run left it."""
    state = BackfillState()
    if path is None or not _has_columns(path, BACKFILL_COLUMNS):
        return state
    import duckdb

    columns = ", ".join(BACKFILL_COLUMNS)
    for row in duckdb.sql(f"SELECT {columns} FROM read_parquet('{path}')").to_arrow_table().to_pylist():
        key = (str(row["congress"]), str(row["bill_type"]), str(row["number"]))
        stamp = str(row["list_update_date_including_text"] or "")
        if row["refusal"] is None:
            state.filled[key] = stamp
        else:
            state.refused[key] = stamp
            state.reasons[key] = str(row["refusal"])
    logger.info(
        "Bill family backfill: {:,} filled and {:,} refused bills retained from the last run",
        len(state.filled),
        len(state.refused),
    )
    return state


def _prior_walks(path: Path | None) -> dict[tuple[str, str], dict[str, Any]]:
    """Per walk unit, the record the last walk left: declared total, completion, unwalkable records."""
    if path is None or not _has_columns(path, BACKFILL_WALK_COLUMNS):
        return {}
    import duckdb

    columns = ", ".join(BACKFILL_WALK_COLUMNS)
    return {
        (str(row["congress"]), str(row["bill_type"])): row
        for row in duckdb.sql(f"SELECT {columns} FROM read_parquet('{path}')").to_arrow_table().to_pylist()
    }


def _int_or_none(value: Any) -> int | None:
    """Parse a value's text as an int, or None when it does not spell one."""
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _settled(walk: Mapping[str, Any] | None, state: BackfillState, congress: str, bill_type: str) -> bool:
    """Whether a walk unit needs no list requests: walked complete, and every entry accounted for.

    Accounted for means filled, refused (and so retried directly, without a
    walk), unwalkable, or a repeat of an entry already counted — the
    publisher's ``updateDate desc`` order repeats a bill across a page
    boundary when its stamp ties (measured on the 92nd: ``hr 4634`` twice,
    identical, in a walk whose declared and observed counts agreed at 767),
    so the declared total counts entries, not bills, and so does this. A unit
    that never reached its terminal page, or whose declared total is unknown,
    is walked again — pages charged to the cap — because only a walk can find
    the entries it did not reach.
    """
    if walk is None or str(walk.get("list_completed")) != "true":
        return False
    declared = _int_or_none(walk.get("declared_count"))
    if declared is None:
        return False
    filled, refused = state.count(congress, bill_type)
    accounted = (_int_or_none(walk.get("unwalkable_count")) or 0) + (_int_or_none(walk.get("repeated_count")) or 0)
    return filled + refused + accounted >= declared


@dataclass(frozen=True, slots=True)
class _BackfillOutcome:
    """What one backfill pass over the named pre-floor Congresses produced."""

    families: list[BillFamilyTables]
    touched: set[str]
    state_rows: list[dict[str, Any]]
    walk_rows: list[dict[str, Any]]
    refused: int


#: The refusals a backfilled bill's own record earned: the detail route answered and the record could not be shaped
#: (no title, no ``congress_bills`` row). Recorded with the reader, they are not asked again every run; every other
#: class -- a transport failure, a route refusal -- is.
SETTLED_REFUSALS = frozenset({"ValueError", "TypeError"})


class _CapReached(Exception):
    """Internal: the run's shared fetch budget ran out mid-backfill."""


def _state_row(key: BackfillKey, stamp: str, refusal: str | None, observed_at: str) -> dict[str, Any]:
    congress, bill_type, number = key
    return {
        "congress": text(congress),
        "bill_type": text(bill_type),
        "number": text(number),
        "list_update_date_including_text": text(stamp),
        "refusal": text(refusal),
        "observed_at": text(observed_at),
    }


class _Backfill:
    """One run's backfill pass: the retry of retained refusals, then the walks, under one cap.

    A class rather than a closure so the three things every attempt touches —
    the live state, the shared budget and the rows to publish — are named
    once. ``attempt`` is the single place a detail request is made and its
    outcome recorded, for a retried refusal and a walked record alike.
    """

    def __init__(
        self,
        source: ListBackfillSource,
        state: BackfillState,
        remaining: list[int],
        *,
        engine: EngineStamp,
        classify: SectionClassifier | None,
        summarize: BillSummarizer | None,
        summarize_diff: DiffSummarizer | None,
        readers: dict[str, str] | None = None,
        reader: str = "",
        law_bills: Mapping[str, str] | None = None,
    ) -> None:
        self.source = source
        self.state = state
        self.remaining = remaining
        self.build = functools.partial(
            build_family,
            engine=engine,
            classify=classify,
            summarize=summarize,
            summarize_diff=summarize_diff,
            law_bills=law_bills,
        )
        #: Each filled bill's reader (``STATUS_REFUSALS_KEY``'s sibling, ``STATUS_READERS_KEY``), updated in place.
        self.readers = {} if readers is None else readers
        self.reader = reader
        self.families: list[BillFamilyTables] = []
        self.touched: set[str] = set()
        self.state_rows: list[dict[str, Any]] = []
        self.walk_rows: list[dict[str, Any]] = []
        self.refused = 0
        #: Every identity this run has requested, whatever the outcome: a bill
        #: is attempted at most once a run, so a refusal retried at the start
        #: is not asked for again when its unit's walk reaches it.
        self.attempted: set[BackfillKey] = set()

    def charge(self) -> None:
        """One request against the run's cap; the cap reached ends the pass where it stands."""
        if self.remaining[0] <= 0:
            raise _CapReached
        self.remaining[0] -= 1

    def attempt(self, identity: BillIdentity, stamp: str) -> bool:
        """One detail request for one bill: a row on success, a refused state row otherwise.

        A ``401``/``403`` propagates and aborts the run. A refusal by the
        reader, a transport failure after its retries (``ConnectionError`` is
        what spicy-docs raises then) or an ``httpx`` error is that one bill's
        gap: recorded as refused, retried first next run, never a row with
        invented values.
        """
        key: BackfillKey = (str(identity.congress), identity.bill_type, str(identity.number))
        self.charge()
        self.attempted.add(key)
        try:
            detail, detail_at = self.source.detail(identity)
            status = backfill_status(identity, detail)
            built = self.build(BillFamilyCapture(status=status, versions=(), observed_at=detail_at))
            if not built.bills:
                raise ValueError("the shaper produced no bills row")
            row = _backfill_bill_row(built.bills[0], detail)
        except CredentialRefusedError:
            raise
        except (PagedJsonSourceError, httpx.HTTPError, ConnectionError, ValueError, TypeError) as error:
            self.refused += 1
            logger.warning(
                "Bill family backfill: {} {} {} refused: {}", key[0], key[1], key[2], scrub_credential(str(error), "")
            )
            self.state.filled.pop(key, None)
            self.state.refused[key] = stamp
            self.state.reasons[key] = type(error).__name__
            self.readers[bill_key(identity)] = self.reader
            self.state_rows.append(_state_row(key, stamp, type(error).__name__, _detected_at()))
            return False
        self.families.append(replace(built, bills=(row,)))
        self.touched.add(bill_key(status.identity))
        self.readers[bill_key(status.identity)] = self.reader
        self.state.refused.pop(key, None)
        self.state.filled[key] = stamp
        self.state_rows.append(_state_row(key, stamp, None, detail_at))
        return True

    def settled_refusal(self, key: BackfillKey, stamp: str | None = None) -> bool:
        """Whether a retained refusal would recur: this reader shaped the record and refused it, at this stamp.

        The detail route answered, and the reader or the shaper refused what it said (:data:`SETTLED_REFUSALS`), so
        the same record under the same code refuses again; it is asked again when its list stamp moves or another
        reader runs. A transport or route failure is not settled: it is retried next run.
        """
        return (
            self.state.reasons.get(key) in SETTLED_REFUSALS
            and self.readers.get(f"{key[0]}-{key[1]}-{key[2]}") == self.reader
            and (stamp is None or self.state.refused.get(key) == stamp)
        )

    def retry_refusals(self, congresses: Sequence[int], bill_types: Sequence[str]) -> None:
        """Every retained refusal in scope, newest Congress first, one request each and no walk; a settled one waits."""
        in_scope = {str(congress) for congress in congresses}
        for key in sorted(self.state.refused, key=lambda key: (-int(key[0]), key[1], int(key[2]))):
            if key[0] not in in_scope or key[1] not in bill_types or self.settled_refusal(key):
                continue
            identity = BillIdentity(congress=int(key[0]), bill_type=key[1], number=int(key[2]))
            self.attempt(identity, self.state.refused[key])

    def reread_stale(self, congresses: Sequence[int], bill_types: Sequence[str]) -> None:
        """Every filled bill in scope another reader, or none, read: one detail request each, newest Congress first.

        A SpicyDocs release that changes the code re-reads each backfilled bill once, as the status pass re-reads a
        BILLSTATUS one; the shared cap spreads the re-reads over runs, and a bill re-read stops being stale.
        """
        in_scope = {str(congress) for congress in congresses}
        for key in sorted(self.state.filled, key=lambda key: (-int(key[0]), key[1], int(key[2]))):
            if key[0] not in in_scope or key[1] not in bill_types or key in self.attempted:
                continue
            if self.readers.get(f"{key[0]}-{key[1]}-{key[2]}") == self.reader:
                continue
            identity = BillIdentity(congress=int(key[0]), bill_type=key[1], number=int(key[2]))
            self.attempt(identity, self.state.filled[key])

    def walk(self, congress: int, bill_type: str) -> bool:
        """One walk unit's pages, each charged, each record filled unless its retained stamp matches.

        Returns whether the walk reached the route's terminal page. A walk
        that refuses (a changed declared count, a repeated continuation)
        propagates and fails the run loudly: a walk asked for and not
        received must never read as a completed one — which is also why the
        walk row states the route's declared total beside what this run
        actually walked.
        """
        declared: int | None = None
        walked = pages = unwalkable = repeated = 0
        completed = False
        observed_at = ""
        seen: set[BackfillKey] = set()
        pages_iter = iter(self.source.pages(congress, bill_type))
        try:
            while True:
                self.charge()
                page = next(pages_iter, None)
                if page is None:
                    self.remaining[0] += 1  # the terminal page was already paid for
                    completed = True
                    break
                pages += 1
                observed_at = str(getattr(page.capture, "observed_at", "") or "")
                if declared is None and page.declared_count is not None:
                    declared = int(page.declared_count)
                for record in page.records:
                    walked += 1
                    identity = _backfill_identity(congress, bill_type, record)
                    if identity is None:
                        unwalkable += 1
                        logger.warning(
                            "Bill family backfill: {} {} list record names no walkable bill", congress, bill_type
                        )
                        continue
                    stamp = _detail_str(record.get("updateDateIncludingText")) or ""
                    key: BackfillKey = (str(congress), bill_type, str(identity.number))
                    if key in seen:
                        repeated += 1
                        continue
                    seen.add(key)
                    if self.state.filled.get(key) == stamp or key in self.attempted or self.settled_refusal(key, stamp):
                        continue
                    self.attempt(identity, stamp)
        finally:
            filled, _ = self.state.count(str(congress), bill_type)
            self.walk_rows.append(
                {
                    "congress": text(congress),
                    "bill_type": text(bill_type),
                    "declared_count": text(declared),
                    "records_walked": text(walked),
                    "pages_walked": text(pages),
                    "list_completed": text(completed),
                    "unwalkable_count": text(unwalkable),
                    "repeated_count": text(repeated),
                    "backfilled_count": text(filled),
                    "observed_at": text(observed_at),
                }
            )
            logger.info(
                "Bill family backfill: {} {} — declared {}, walked {:,} on {} page(s), {:,} filled in all",
                congress,
                bill_type,
                "?" if declared is None else f"{declared:,}",
                walked,
                pages,
                filled,
            )
        return completed


def _run_backfill(
    list_source: ListBackfillSource,
    congresses: Sequence[int],
    bill_types: Sequence[str],
    *,
    state: BackfillState,
    prior_walks: Mapping[tuple[str, str], Mapping[str, Any]],
    remaining: list[int],
    engine: EngineStamp,
    classify: SectionClassifier | None,
    summarize: BillSummarizer | None,
    summarize_diff: DiffSummarizer | None,
    readers: dict[str, str] | None = None,
    reader: str = "",
    law_bills: Mapping[str, str] | None = None,
) -> _BackfillOutcome:
    """Retry last run's refusals, re-read the bills another reader filled, then walk every unsettled unit.

    The budget is the rollup's own per-run cap (``remaining``, shared with the
    bulk printings) and every request — a retried detail, a list page, a
    walked detail — is charged to it. When it runs out the pass stops where
    it stands: the walk row records how far that unit got, and the next run
    resumes on the retained state. Resume is the state table: a retained
    refusal is retried directly, without a walk, unless it is settled (the
    record was shaped and refused by this reader at this list stamp, which
    would only refuse again); a bill whose state row still matches its list
    stamp and whose reader is the running one costs no request, and one
    another reader filled is re-read once, within the cap; a unit walked
    complete with every record filled, refused or unwalkable is not walked
    again (a transport refusal is retried, one request a run, so a gapped unit
    costs one request per gap rather than a page walk). Only an unsettled unit
    pays for its pages again, because only a walk can reach the records it
    did not.
    """
    pass_ = _Backfill(
        list_source,
        state,
        remaining,
        engine=engine,
        classify=classify,
        summarize=summarize,
        summarize_diff=summarize_diff,
        readers=readers,
        reader=reader,
        law_bills=law_bills,
    )
    with list_source:
        try:
            pass_.retry_refusals(congresses, bill_types)
            pass_.reread_stale(congresses, bill_types)
            for congress in congresses:
                for bill_type in bill_types:
                    if _settled(prior_walks.get((str(congress), bill_type)), state, str(congress), bill_type):
                        logger.info(
                            "Bill family backfill: {} {} walked complete and every record accounted for — no list requests",
                            congress,
                            bill_type,
                        )
                        continue
                    pass_.walk(congress, bill_type)
        except _CapReached:
            logger.warning(
                "Bill family backfill: the per-run cap was reached — the next run resumes on the retained state: "
                "refusals retried first, filled bills skipped by their stamp, unsettled units walked again"
            )
    return _BackfillOutcome(
        families=pass_.families,
        touched=pass_.touched,
        state_rows=pass_.state_rows,
        walk_rows=pass_.walk_rows,
        refused=pass_.refused,
    )


def build_bill_family(
    output_dir: Path,
    *,
    bulk_acquirer: BulkStatusSource | None = None,
    body_acquirer: PackageBodySource | None = None,
    bills_acquirer: BulkBillsSource | None = None,
    list_source: ListBackfillSource | None = None,
    cbo_acquirer: Any = None,
    max_version_fetches: int = MAX_VERSION_FETCHES,
    download_prior: Callable[[str, Path], bool] = r2.download,
    download_members: Callable[[str, Path], Sequence[Path]] = download_prior_members,
    evidence: CaptureEvidence | None = None,
    receipt_build=None,
    publication_package_ids: tuple[str, ...] = (),
    publication_metadata_acquirer: Any = None,
) -> tuple[Path, ...]:
    """Build bill-family outputs, with a directory for each partitioned table.

    ``download_prior`` fetches a single-file prior; ``download_members`` fetches every partitioned member.
    """
    if receipt_build is not None:
        return receipt_build.run(build_bill_family, output_dir, **{
            key: value for key, value in locals().items() if key not in {"output_dir", "receipt_build"}
        })
    if isinstance(max_version_fetches, bool) or not isinstance(max_version_fetches, int) or max_version_fetches < 0:
        raise ValueError("max_version_fetches must be a nonnegative integer; zero disables acquisition")
    congresses = congresses_from_env()
    bill_types = bill_types_from_env()
    logger.info("Bill family: Congresses {}, bill types {}", congresses, bill_types)

    # One scope input, two routes: BILLSTATUS bulk serves nothing below its
    # floor and the ``bill`` list route nothing below its own, so the named
    # Congresses split on the publisher's fact, and the backfill walks newest
    # first. A name neither route reaches refuses the run rather than walking
    # an empty success that would read as absence.
    bulk_congresses = [congress for congress in congresses if congress >= BULK_STATUS_FLOOR]
    backfill_congresses = sorted((congress for congress in congresses if congress < BULK_STATUS_FLOOR), reverse=True)
    unreachable = [congress for congress in backfill_congresses if congress < LIST_ROUTE_FLOOR]
    if unreachable:
        raise ValueError(
            f"BILL_FAMILY_CONGRESSES names {unreachable}: the bill list route reaches no older Congress than "
            f"the {LIST_ROUTE_FLOOR}nd (LIST_ROUTE_FLOOR; measured: the A11 receipt's declared-counts.json). "
            "Nothing below it can be walked, and an empty walk would read as absence."
        )

    api_key = _resolve_api_key()
    if evidence is not None:
        evidence.credential = api_key or ""
        evidence.event(
            "bill-family-selection",
            congresses=list(congresses),
            bill_types=list(bill_types),
            max_version_fetches=max_version_fetches,
        )
    bulk_budget = bulk_status_budget()
    bulk_acquirer = bulk_acquirer or BulkStatusAcquirer(
        budget=bulk_budget,
        transport=None
        if evidence is None
        else evidence.transport(stage="bill-status-bulk", max_bytes=bulk_budget.max_bytes),
    )
    # Bill text in bulk is keyless, so it runs whether or not a key is present;
    # the per-package route it leaves the rest to is keyed and capped.
    bills_acquirer = bills_acquirer or BulkBillsAcquirer(
        budget=bulk_budget,
        transport=None
        if evidence is None
        else evidence.transport(stage="bill-text-bulk", max_bytes=bulk_budget.max_bytes),
    )
    if body_acquirer is None and max_version_fetches > 0:
        if api_key:
            body_acquirer = GovInfoBodyAcquirer(
                budget=BODY_BUDGET,
                api_key=api_key,
                transport=None
                if evidence is None
                else evidence.transport(
                    stage="bill-package", max_bytes=max(BODY_BUDGET.max_body_bytes, BODY_BUDGET.max_metadata_bytes)
                ),
            )
        else:
            logger.warning(
                "Bill family: no api.data.gov key ({}) — bill text before the 113th Congress is not read",
                ", ".join(API_KEY_ENV_VARS),
            )

    # The model seams, wired only when a key is present. Nothing wraps them:
    # since spicy-docs 0.22.0 `build_bill_family` runs all three inside its own
    # guard and files a `FamilyRefusal` naming the reader's message, while a
    # credential refusal and a transport failure still abort. A wrapper here
    # could only re-file one of those as the other, which is what the 0.21.3
    # one had to do.
    gemini_key = resolve_gemini_key()
    classify = summarize = summarize_diff_call = None
    if gemini_key:
        from spicy_docs.extraction.gemini import GeminiClient

        model = os.environ.get("GEMINI_MODEL", DEFAULT_MODEL)
        call = model_call(GeminiClient(api_key=gemini_key))
        classify = functools.partial(classify_sections, call=call, model=model)
        summarize = functools.partial(summarize_bill, call=call, model=model)
        summarize_diff_call = functools.partial(summarize_diff, call=call, model=model)
        logger.info("Bill family: model seams wired ({})", model)
    else:
        logger.info("Bill family: no Gemini key — section_classifications, bill_summaries, diff_summaries stay empty")

    stamp = engine_stamp()
    logger.info("Bill family: engine {} {} @ {}", stamp.name, stamp.version, stamp.revision or "(unknown)")

    # 1. What the last run already published, so this one can skip it. A replay-required summary prior is
    # asked for first: a run whose scope cannot replay it refuses before restoring anything else.
    try:
        published_table(output_dir, REPLAYED_FROM_STATUS, download_prior)
    except PriorReplayRequired as required:
        _require_replay_scope(required, {}, bulk_congresses, bill_types)
    prior_paths, replay = _download_prior(output_dir, download_prior)
    _require_spelled_digests(prior_paths)
    split_priors = {name: published_members(output_dir, name, download_members) for name in PARTITIONED}
    index = _prior_index(
        prior_paths, split_priors["bill_sections"], in_scope=lambda bill: _in_scope(bill, congresses, bill_types)
    )
    if index.invalid_targets:
        for name in ("financial_changes", "diff_summaries"):
            prior_paths[name] = published_table(output_dir, name, download_prior)
    invalid_pairs = _incident_pairs(prior_paths, index.invalid_targets)
    # The published ``laws`` table names each public law's bill, best-effort, for the CBO titles led by a law.
    law_bills = law_bills_from(published_table(output_dir, "laws", download_prior))
    # Every bill's reader, carried forward, and this run's code for each status it shapes.
    readers, reader = dict(index.status_readers), status_reader()
    held_archives = _held_archives(prior_paths.get(ARCHIVES_TABLE))
    status_refusals = _held_refusals(prior_paths.get(ARCHIVES_TABLE), STATUS_REFUSALS_KEY)
    # A folder whose refusals another reader recorded is read again, so a release retries them.
    for record in status_refusals.values():
        if record.get("reader") != reader:
            congress_, _, bill_type_ = str(record.get("folder", "")).partition("-")
            held_archives.pop((congress_, bill_type_), None)
    # An unchanged archive still needs a read for a status row in doubt: one a
    # BILLSTATUS read wrote (it states a schema version) before this rollup
    # recorded CBO outcomes, or one the prior index reopens. A row no BILLSTATUS
    # read wrote does not reopen the folder, whatever else is said of it: a
    # full read of the zip rebuilds every bill it holds, so such a row is one
    # it does not hold -- the 119th's six H.R. numbers reserved for the Speaker
    # and the Minority Leader, listed by the retired list writer with an API
    # URL -- and no read of the zip can qualify it; the zip moves when it starts
    # to hold the bill. Nor does a bill this reader already found refused in, or
    # dropped from, the very zip the folder still holds (``STATUS_REFUSALS_KEY``):
    # reading the same bytes with the same code cannot settle it. Bodies and
    # comparisons are the body pass's.
    bills_prior = prior_paths.get("congress_bills")
    # The bills a BILLSTATUS read published, by folder: a read of the folder's zip that holds none of one drops it.
    status_bills: dict[tuple[str, str], set[str]] = {}
    # A prior without the frozen identity columns names no folder to reopen.
    if bills_prior is not None and _has_columns(bills_prior, ("bill_id", "congress", "bill_type")):
        import duckdb

        # A narrow projection replaces materialising every prior row into Python
        # dicts: only the columns the prune decides on are read.
        stated = _has_columns(bills_prior, ("schema_version",))
        version = "schema_version" if stated else "'unknown'"
        for bill_id, congress, bill_type, schema_version in duckdb.sql(
            f"SELECT bill_id, congress, bill_type, {version} FROM read_parquet('{bills_prior}')"
        ).fetchall():
            if schema_version is not None:
                status_bills.setdefault((congress, bill_type), set()).add(bill_id)
            if (
                schema_version is not None
                and (bill_id in index.pending_bills or bill_id not in index.bill_text_dates)
                and not _settled_refusal(status_refusals.get(bill_id), reader, held_archives.get((congress, bill_type)))
            ):
                held_archives.pop((congress, bill_type), None)

    if replay is not None:
        _require_replay_scope(replay, status_bills, bulk_congresses, bill_types)
        logger.warning("Bill family: {} — re-reading every bulk folder and bill to rebuild it", replay)
        if evidence is not None:
            evidence.event("prior-replay", dataset=replay.dataset, policy_version=replay.policy_version,
                           congresses=list(bulk_congresses))
    if prior_paths.get("bill_versions") is None or bills_prior is None or replay is not None:
        held_archives.clear()
    completed_archives = set(held_archives)
    visited_archives: set[tuple[str, str]] = set()

    # 2. Status: rebuild each bill whose BILLSTATUS moved, listing its new
    # printings as pending; bodies are step 2c's, by each printing's own state.
    remaining = [max_version_fetches]
    families: list[BillFamilyTables] = []
    touched: set[str] = set()
    # Published printings keyed under another printing's code.
    retired_versions: set[tuple[str, ...]] = set(index.invalid_targets)
    archive_rows: list[dict] = []
    vote_rows: list[dict] = []
    # The bills whose status this run read whole: a read status is the complete statement of its actions'
    # recorded votes, so their prior reference rows are replaced at the merge, including when it states none.
    votes_restated: set[str] = set()
    # Each parsed bill's own report citations, which its CBO feed rows carry.
    citations: dict[BillIdentity, tuple[str, ...]] = {}
    bills = unchanged = skipped = archives_skipped = votes_refused = 0
    for congress in bulk_congresses:
        for bill_type in bill_types:
            scope = (str(congress), bill_type)
            visited_archives.add(scope)
            acquisition = _acquire_archive(
                bulk_acquirer, congress, bill_type, held_archives.get((str(congress), bill_type))
            )
            retained = _retained_entry(bulk_acquirer, acquisition, congress, bill_type)
            if acquisition.skipped_unchanged:
                if retained is not None:
                    archive_rows.append(retained)
                # Only qualified completed scopes enter this skip. The source
                # listing then proves their BILLSTATUS archive has not moved.
                archives_skipped += 1
                logger.info(
                    "Bill family: {} {} — zip unchanged since the last run, not downloaded", congress, bill_type
                )
                continue
            archive = acquisition.archive
            observed_at = acquisition.capture.observed_at
            logger.info(
                "Bill family: {} {} — {:,} parsed, {:,} refused by the reader",
                congress,
                bill_type,
                archive.parsed_count,
                archive.refused_count,
            )
            skipped += archive.refused_count
            completed_archives.discard(scope)
            folder = f"{congress}-{bill_type}"
            read_zip = None if retained is None else _zip_stamp(retained["modified_at"], retained["size"])
            # This read restates every refusal and drop of the folder.
            for key in [key for key, record in status_refusals.items() if record.get("folder") == folder]:
                del status_refusals[key]
            refusal = functools.partial(_status_refusal, folder=folder, zip_stamp=read_zip, reader=reader)
            held_bills: set[str] = set()
            for member in archive.members:
                if member.identity is not None:
                    held_bills.add(bill_key(member.identity))
                if member.status is None:
                    name = bill_key(member.identity) if member.identity is not None else f"{folder}/{member.name}"
                    status_refusals[name] = refusal(scrub_credential(member.refusal or "refused by the reader", ""))
                    continue
                identifier = bill_key(member.status.identity)
                citations[member.status.identity] = tuple(member.status.report_citations)
                # A matching publisher stamp skips the bill: its printings'
                # bodies and comparisons are the body pass's, not a reason to
                # read its status again.
                published = index.bill_text_dates.get(identifier)
                if (
                    replay is None
                    and published is not None
                    and published == member.status.update_date_including_text
                    and identifier not in index.pending_bills
                ):
                    unchanged += 1
                    continue
                codes = {code for code, _ in _printings(member.status)}
                capture = BillFamilyCapture(
                    status=member.status,
                    versions=tuple(_listed_captures(member.status, index.held_codes(identifier))),
                    observed_at=observed_at,
                )
                # No documents here, so nothing to compare or model: diff=False
                # keeps the provider from refusing every pair of placeholders.
                tables = build_family(capture, engine=stamp, diff=False, law_bills=law_bills)
                # A row whose package belongs to another listed printing
                # describes that printing under the wrong key: retire it.
                retired_versions.update(
                    (identifier, code, source)
                    for code, source, printing in index.miskeyed_rows.get(identifier, ())
                    if printing in codes
                )
                if tables.bills:
                    readers[identifier] = reader
                if doubt := _status_doubt(tables, codes):
                    status_refusals[identifier] = refusal(doubt)
                families.append(tables)
                rows, refused = vote_reference_rows(member.status, observed_at=observed_at)
                vote_rows.extend(rows)
                votes_refused += refused
                votes_restated.add(identifier)
                touched.add(identifier)
                bills += 1
            for dropped in status_bills.get(scope, set()) - held_bills:
                status_refusals[dropped] = refusal(DROPPED_REFUSAL)
            # Every visited folder's prior row is replaced, so each is written
            # back: the row records the zip that was read; completion is the
            # metadata list's alone. Gating the row on completion emptied the
            # table whenever a refusal left every visited folder unfinished. A
            # folder completes with its refusals and drops recorded: they are
            # read again when the zip moves or another reader runs, not every run.
            if retained is not None:
                archive_rows.append(retained)
                completed_archives.add(scope)

    # 2b. Bodies, sections and comparisons, by each printing's own state:
    # the printings this run listed and every earlier run's still pending.
    fresh_versions = [row for tables in families for row in tables.bill_versions]
    repairs = _target_repair_rows(index, fresh_versions, stamp)
    families.append(repairs)
    fresh_versions.extend(repairs.bill_versions)
    body = _run_body_pass(
        prior_paths,
        index,
        fresh_versions,
        retired_versions,
        congresses=congresses,
        bill_types=bill_types,
        bills_acquirer=bills_acquirer,
        body_acquirer=body_acquirer,
        remaining=remaining,
        engine=stamp,
        classify=classify,
        summarize=summarize,
        summarize_diff=summarize_diff_call,
        fresh_bills=[row for tables in families for row in tables.bills],
        refusals=_held_refusals(prior_paths.get(ARCHIVES_TABLE), TEXT_REFUSALS_KEY),
        package_refusals=_held_refusals(prior_paths.get(ARCHIVES_TABLE), PACKAGE_REFUSALS_KEY),
    )

    # 2c. The pre-BILLSTATUS backfill: same scope input, the route the
    # publisher serves below the bulk floor, the same per-run cap.
    backfill_state: list[dict[str, Any]] = []
    backfill_walk_state: list[dict[str, Any]] = []
    if backfill_congresses:
        source = list_source
        if source is None and api_key:
            source = CongressListBackfill(api_key, evidence=evidence)
        if source is not None:
            outcome = _run_backfill(
                source,
                backfill_congresses,
                bill_types,
                state=_held_backfills(prior_paths.get(BACKFILLS_TABLE)),
                prior_walks=_prior_walks(prior_paths.get(BACKFILL_WALKS_TABLE)),
                remaining=remaining,
                engine=stamp,
                classify=classify,
                summarize=summarize,
                summarize_diff=summarize_diff_call,
                readers=readers,
                reader=reader,
                law_bills=law_bills,
            )
            families.extend(outcome.families)
            touched.update(outcome.touched)
            backfill_state = outcome.state_rows
            backfill_walk_state = outcome.walk_rows
            if outcome.refused:
                logger.warning(
                    "Bill family backfill: {:,} detail attempts refused (retried first next run)", outcome.refused
                )
        else:
            logger.warning(
                "Bill family backfill: Congresses {} named below the bulk floor but no api.data.gov key ({}) — "
                "publishing no backfill rows",
                backfill_congresses,
                ", ".join(API_KEY_ENV_VARS),
            )

    # 2d. CBO's own feed for each scoped Congress, once, for the estimates BILLSTATUS does not state.
    feed = read_cbo_feeds(
        bulk_congresses,
        citations={**prior_citations(prior_paths.get("cbo_cost_estimates"), bulk_congresses), **citations},
        law_bills=law_bills,
        acquirer=cbo_acquirer,
        evidence=evidence,
    )
    folded = _refuse_repeated_keys(BillFamilyTables.concat([*families, *body.families]))
    folded = replace(folded, refusals=(*folded.refusals, *feed.refusals))
    # A placeholder a processed body now stands for goes, whether this run's
    # status pass listed it or an earlier run did: one row per printing.
    superseded = body.superseded
    if superseded:
        folded = replace(
            folded,
            bill_versions=tuple(
                row
                for row in folded.bill_versions
                if (row["bill_id"], row["version_code"], row["source"]) not in superseded
            ),
        )
    # One pass over every refusal both passes produced — `concat` carries them,
    # so the backfill's families are counted here too, which the per-bill count
    # this replaces never reached. Keyed by the reason as well as the table: a
    # `FamilyRefusal` carries the reason so someone can act on it, and a count
    # alone throws that away. The C1 defect read as "the model tables are
    # short" until the message said which keys the answer had left out.
    # Distinct reasons rather than one line per refusal keeps it bounded: a
    # systematically refused prompt is one line, not one per bill.
    refusals: Counter[str] = Counter(refusal.table for refusal in folded.refusals)
    reasons: Counter[tuple[str, str]] = Counter(
        (refusal.table, scrub_credential(refusal.reason, gemini_key or "")) for refusal in folded.refusals
    )
    logger.info(
        "Bill family: {:,} bills rebuilt, {:,} unchanged and skipped, {:,} capped fetches (per-package printings "
        "and backfill details) of {:,} allowed",
        bills,
        unchanged,
        max_version_fetches - remaining[0],
        max_version_fetches,
    )
    logger.info(
        "Bill family: bodies — {:,} printings read ({:,} from {:,} BILLS zips, {:,} zips not needed, {:,} listings; "
        "{:,} per-package fetches), {:,} bills built",
        body.captured,
        body.bulk_read,
        body.zips,
        body.zips_not_needed,
        body.listings,
        body.fetched,
        len(body.families),
    )
    if remaining[0] == 0:
        logger.warning(
            "Bill family: the per-run cap was reached — the next run resumes on what this one did not reach: "
            "pending printings and XML pairs remain pending; backfilled bills use their retained state"
        )
    if archives_skipped:
        logger.info(
            "Bill family: {:,} of {:,} folder zips proved unchanged and were not downloaded",
            archives_skipped,
            len(bulk_congresses) * len(bill_types),
        )
    if skipped:
        logger.warning("Bill family: {:,} archive entries the reader refused", skipped)
    if refusals:
        logger.warning("Bill family: refusals by table — {}", dict(refusals))
        for (table, reason), count in reasons.most_common(REFUSAL_REASONS_LOGGED):
            logger.warning("Bill family: {} refused {:,}x — {}", table, count, reason)
        if len(reasons) > REFUSAL_REASONS_LOGGED:
            logger.warning(
                "Bill family: {:,} further distinct refusal reasons not listed", len(reasons) - REFUSAL_REASONS_LOGGED
            )
    logger.info("Bill family: {:,} recorded-vote references on this run's bills", len(vote_rows))
    if votes_refused:
        logger.warning("Bill family: {:,} recordedVotes entries refused for a missing sealed field", votes_refused)

    # 3. The thirteenth table: what changed since the last published run,
    # compared over the bills whose status this run read — the rest cannot
    # have changed. A body the body pass read for another bill is not a
    # version added: the printing was added when its status listed it.
    prior = _prior_snapshot(prior_paths, touched)
    current = snapshot_from_rows(
        bills=folded.bills,
        bill_versions=[row for row in folded.bill_versions if row["bill_id"] in touched],
        bill_summaries=[row for row in folded.bill_summaries if row["bill_id"] in touched],
    )
    events = activity_events(prior, current, detected_at=_detected_at())
    logger.info("Bill family: {:,} activity events", len(events))

    # 4. Publish. Every table goes through the one merge helper. Each prior was
    # either downloaded above or found absent, so the merge is told rather than
    # left to retry a download that already failed.
    section_scopes = _complete_child_scopes(
        "bill_versions", folded.bill_versions, folded.bill_sections, "section_count"
    )
    item_scopes = _complete_child_scopes("section_diffs", folded.section_diffs, folded.section_diff_items, "item_count")

    pair_identity = TABLE_CONTRACTS["section_diffs"].identity
    retired_pairs = body.retired_pairs | invalid_pairs
    if retired_pairs:
        logger.warning("Bill family: retiring {:,} published comparisons of non-neighbours", len(retired_pairs))
    if retired_versions:
        logger.warning("Bill family: retiring {:,} published printings keyed as another", len(retired_versions))
    if superseded:
        logger.info("Bill family: retiring {:,} placeholders a processed body now stands for", len(superseded))
    # A refused per-package read is not a read: the tree-less body row the same bytes published before goes, and
    # the listing stands alone (113-hr-1067's and 113-hr-1068's public laws kept both).
    if body.unread:
        logger.info("Bill family: withdrawing {:,} body rows a refused read left without a tree", len(body.unread))
    version_identity = TABLE_CONTRACTS["bill_versions"].identity
    # A bill whose cosponsor rows were refused keeps its prior rows. One set, so the
    # scope below costs O(bills + refusals) rather than a refusal scan per bill.
    refused_cosponsors = {ref.identity[0] for ref in folded.refusals if ref.table == "bill_cosponsors"}
    # A bill whose CBO outcome was listed replaces its BILLSTATUS rows; a feed read replaces its Congress's feed rows.
    estimates, estimate_scope = cost_estimate_publication(
        folded.cbo_cost_estimates,
        feed,
        reread={
            identifier
            for row in folded.bills
            if (identifier := row["bill_id"]) is not None
            and row.get("cbo_cost_estimates_outcome")
            in {"populated", "requested-empty:absent", "requested-empty:present-and-empty"}
        },
        prior=prior_paths.get("cbo_cost_estimates"),
    )
    folded = replace(folded, cbo_cost_estimates=tuple(estimates), cbo_feed_items=feed.items)
    # Each scope names parents whose prior child rows this run replaces --
    # emptying them where the run publishes none.
    replaced: dict[str, ReplacementScope] = {
        "bill_cosponsors": (
            "bill_id",
            {
                identifier
                for row in folded.bills
                if (identifier := row["bill_id"]) is not None
                and row.get("cosponsors_outcome") in {"absent", "empty", "populated"}
                and identifier not in refused_cosponsors
            },
        ),
        "cbo_cost_estimates": estimate_scope,
        # A feed is read whole, so its Congress's items replace that Congress's prior items; an unread feed's stand.
        "cbo_feed_items": ("congress", {str(congress) for congress in feed.congresses}),
        "bill_versions": (version_identity, retired_versions | superseded | body.unread),
        "bill_sections": (version_identity, section_scopes | retired_versions | body.unread),
        "bill_summaries": (version_identity, set(index.invalid_targets)),
        "section_classifications": (version_identity, set(index.invalid_targets)),
        "section_diffs": (pair_identity, retired_pairs),
        "section_diff_items": (pair_identity, item_scopes | retired_pairs),
        "financial_changes": (pair_identity, retired_pairs),
        "diff_summaries": (pair_identity, retired_pairs),
    }

    def publish(contract: str, rows: Any) -> Path:
        if contract in PARTITIONED:
            table = TABLE_CONTRACTS[contract]
            return merge_partitioned_table(
                output_dir,
                name=contract,
                columns=table.columns,
                identity=table.identity,
                version_column=table.version_column,
                rows=rows,
                partitioning=PARTITIONED[contract],
                prior=split_priors[contract],
                replace_parents=replaced.get(contract),
            )
        return merge_contract_table(
            output_dir,
            contract,
            rows,
            download_prior=download_prior,
            prior_present=(prior_paths.get(contract) is not None) if contract in prior_paths else None,
            replace_parents=replaced.get(contract),
        )

    paths = [publish(contract, getattr(folded, attr)) for contract, attr in FAMILY_TABLES]
    merged = dict(zip((contract for contract, _ in FAMILY_TABLES), paths, strict=True))
    if publication_package_ids:
        from spicy_regs.transforms.bill_publication import refresh_bill_publication

        metadata_source = publication_metadata_acquirer or body_acquirer
        if metadata_source is None:
            raise ValueError("Bill publication refresh requires a metadata acquirer")
        refresh_bill_publication(merged["bill_versions"], package_ids=publication_package_ids,
                                 metadata_source=metadata_source, evidence=evidence)
    restaged = restage_held_rows(merged["congress_bills"], merged["bill_actions"], merged["bill_versions"])
    logger.info("Bill family: the running stage rule moves {:,} held bill rows and {:,} action rows", *restaged)
    if evidence is not None:
        evidence.event("held-rows-restaged", bills=restaged[0], actions=restaged[1])
    paths.append(publish("public_activity_events", events))
    # The last two outputs are this repository's own tables, not contracts, so
    # they go through the same merge helper one level down.
    paths.append(
        merge_table(
            output_dir,
            name=ARCHIVES_TABLE,
            columns=ARCHIVE_COLUMNS,
            identity=ARCHIVE_IDENTITY,
            version_column="observed_at",
            rows=archive_rows,
            remote_key=f"{ARCHIVES_TABLE}.parquet",
            download_prior=download_prior,
            prior_present=prior_paths.get(ARCHIVES_TABLE) is not None,
            replace_parents=(ARCHIVE_IDENTITY, visited_archives),
            parquet_metadata={
                ARCHIVE_COMPLETION_KEY: json.dumps(sorted(completed_archives)),
                STATUS_READERS_KEY: _readers_metadata(readers),
                TEXT_REFUSALS_KEY: refusal_metadata(body.refusals),
                PACKAGE_REFUSALS_KEY: refusal_metadata(body.package_refusals),
                STATUS_REFUSALS_KEY: refusal_metadata(status_refusals),
            },
        )
    )
    references = merge_table(
        output_dir,
        name=VOTE_REFERENCES_TABLE,
        columns=VOTE_REFERENCE_COLUMNS,
        identity=VOTE_REFERENCE_IDENTITY,
        version_column="observed_at",
        rows=vote_rows,
        remote_key=f"{VOTE_REFERENCES_TABLE}.parquet",
        download_prior=download_prior,
        replace_parents=("bill_id", votes_restated),
    )
    paths.append(references)
    swept = _latest_capture_only(references)
    if swept:
        logger.info("Bill family: {:,} recorded-vote references from superseded captures swept", swept)
    if evidence is not None:
        evidence.event("vote-references-merged", restated_bills=len(votes_restated), rows=len(vote_rows), swept=swept)
    # The backfill's own retained state, beside the archives table: what was
    # filled under which list stamp, and, per Congress walked, the route's
    # declared total against what was reached. Both merge like the archives —
    # this run's rows win on a repeated identity, and an empty run republishes
    # the prior state untouched.
    paths.append(
        merge_table(
            output_dir,
            name=BACKFILLS_TABLE,
            columns=BACKFILL_COLUMNS,
            identity=BACKFILL_IDENTITY,
            version_column="observed_at",
            rows=backfill_state,
            remote_key=f"{BACKFILLS_TABLE}.parquet",
            download_prior=download_prior,
            prior_present=prior_paths.get(BACKFILLS_TABLE) is not None,
        )
    )
    paths.append(
        merge_table(
            output_dir,
            name=BACKFILL_WALKS_TABLE,
            columns=BACKFILL_WALK_COLUMNS,
            identity=BACKFILL_WALK_IDENTITY,
            version_column="observed_at",
            rows=backfill_walk_state,
            remote_key=f"{BACKFILL_WALKS_TABLE}.parquet",
            download_prior=download_prior,
            prior_present=prior_paths.get(BACKFILL_WALKS_TABLE) is not None,
        )
    )
    return tuple(paths)


def _held_refusals(path: Path | None, key: str) -> dict[str, dict[str, Any]]:
    """What the last run's body pass refused under ``key``: printings by BILLS zip link, or packages by id."""
    if path is None:
        return {}
    import pyarrow.parquet as pq

    return refusal_state((pq.read_schema(path).metadata or {}).get(key.encode()))


def _zip_stamp(modified_at: object, size: object) -> str:
    """A folder zip's listed instant and size, as its retained archive row spells them."""
    return f"{modified_at}|{size}"


def _status_refusal(refusal: str, *, folder: str, zip_stamp: str | None, reader: str) -> dict[str, str | None]:
    """One ``STATUS_REFUSALS_KEY`` record: what a status read found, in which zip, under which reader."""
    return {"folder": folder, "zip": zip_stamp, "reader": reader, "refusal": refusal}


def _settled_refusal(record: Mapping[str, Any] | None, reader: str, held: BulkListingEntry | None) -> bool:
    """Whether ``record`` says this reader already read the zip its folder still holds, so a read settles nothing."""
    return (
        record is not None
        and held is not None
        and record.get("reader") == reader
        and record.get("zip") == _zip_stamp(held.modified_at.isoformat(), held.size)
    )


def _status_doubt(tables: BillFamilyTables, codes: Collection[str]) -> str | None:
    """Why a shaped bill's status stays in doubt after its read, or ``None``: what the prior index reopens it for.

    No ``congress_bills`` row, or a ``version_count`` its distinct printings do not reach (a printing the publisher
    lists twice, or without a type), which the next run's ``pending_bills`` would read as rows lost.
    """
    if not tables.bills:
        reasons = sorted({refusal.reason for refusal in tables.refusals if refusal.table == "congress_bills"})
        return "no congress_bills row" + (f": {reasons[0]}" if reasons else "")
    count = _int_or_none(tables.bills[0].get("version_count"))
    if count is not None and count != len(codes):
        return f"version_count {count} against {len(codes)} distinct printings"
    return None


#: The bill-family digests spicy-docs 0.52.0 spells ``sha256:`` and compares strictly: ``bill_summaries.content_hash``
#: is what this run's ``summary_generated`` events compare. A prior bare value would emit a regeneration event for
#: every unchanged summary, so the run refuses until the one-time re-spell has run (``prior_repairs``).
_BARE_DIGEST = "[0-9a-f]{64}"


def _require_spelled_digests(paths: Mapping[str, Path | None]) -> None:
    """Refuse a prior ``bill_summaries`` holding a bare-hex ``content_hash``: the re-spell runs before any compare."""
    import duckdb

    path = paths.get("bill_summaries")
    if path is None or not _has_columns(path, ("content_hash",)):
        return
    row = duckdb.sql(
        f"SELECT count(*) FROM read_parquet('{path}') WHERE regexp_full_match(content_hash, '{_BARE_DIGEST}')"
    ).fetchone()
    bare = int(row[0]) if row else 0
    if bare:
        raise ValueError(
            f"bill_summaries holds {bare:,} bare-hex content_hash values, which spicy-docs compares strictly; "
            "run the one-time re-spell first: python -m spicy_regs.pipelines.prior_repairs respell-bill-family-digests"
        )


def _in_scope(bill_id: str, congresses: Collection[int], bill_types: Collection[str]) -> bool:
    identity = BillIdentity.from_bill_id(bill_id)
    return identity.congress in congresses and identity.bill_type in bill_types


def _run_body_pass(
    prior_paths: Mapping[str, Path | None],
    index: PriorIndex,
    fresh_versions: Sequence[Mapping[str, Any]],
    retired_versions: Collection[tuple[str, ...]],
    *,
    congresses: Collection[int],
    bill_types: Collection[str],
    bills_acquirer: BulkBillsSource | None,
    body_acquirer: PackageBodySource | None,
    remaining: list[int],
    engine: EngineStamp,
    classify: SectionClassifier | None,
    summarize: BillSummarizer | None,
    summarize_diff: DiffSummarizer | None,
    fresh_bills: Sequence[Mapping[str, Any]],
    refusals: Mapping[str, Mapping[str, Any]],
    package_refusals: Mapping[str, Mapping[str, Any]],
) -> Any:
    """Plan and run the body pass over the in-scope bills with body or comparison work.

    The candidates are the prior index's ``body_bills`` and the bills this
    run's status pass rebuilt; only their ``bill_versions`` rows are read,
    prior and fresh, the fresh winning, and none a mis-keyed retirement names.
    """
    import duckdb

    candidates = {
        bill
        for bill in {*index.body_bills, *(row["bill_id"] for row in fresh_versions)}
        if _in_scope(bill, congresses, bill_types)
    }
    fresh = {(row["bill_id"], row["version_code"], row["source"]): row for row in fresh_versions}
    columns = (
        "bill_id",
        "version_code",
        "source",
        "label",
        "version_date",
        "package_id",
        "offered_formats_json",
        "sha256",
        "section_count",
    )
    rows: list[Mapping[str, Any]] = []
    path = prior_paths.get("bill_versions")
    if candidates and path is not None and _has_columns(path, columns):
        wanted = duckdb.sql("SELECT UNNEST(?::VARCHAR[]) AS bill_id", params=[sorted(candidates)])  # noqa: F841 — read by name below
        rows = (
            duckdb.sql(
                f"SELECT {', '.join(f't.{column}' for column in columns)} FROM read_parquet('{path}') t "
                "SEMI JOIN wanted w ON t.bill_id = w.bill_id"
            )
            .to_arrow_table()
            .to_pylist()
        )
    retired = set(retired_versions)
    merged = [
        row
        for row in rows
        if (key := (row["bill_id"], row["version_code"], row["source"])) not in fresh and key not in retired
    ]
    merged.extend(row for key, row in fresh.items() if key not in retired and key[0] in candidates)
    work, retired_pairs, redundant = plan_work(
        merged,
        held=index.held_codes,
        xml=index.xml_codes,
        complete_pairs=index.pairs,
        published_pairs=index.published_pairs,
        shadowed=index.shadowed,
    )
    logger.info(
        "Bill family: body pass over {:,} bills with work ({:,} pending printings) of {:,} candidates",
        len(work),
        sum(len(bill.pending) for bill in work),
        len(candidates),
    )
    bill_rows: dict[str, Mapping[str, Any]] = {}
    if summarize is not None and work:
        # A summary reads its bill's title, stage and money-bill kind: this
        # run's row where the status pass wrote one, the published one else.
        bill_rows = {row["bill_id"]: row for row in fresh_bills}
        path = prior_paths.get("congress_bills")
        facts = ("bill_id", "title", "stage", "money_bill_kind")
        unread = sorted({bill.bill_id for bill in work} - bill_rows.keys())
        if unread and path is not None and _has_columns(path, facts):
            wanted = duckdb.sql("SELECT UNNEST(?::VARCHAR[]) AS bill_id", params=[unread])  # noqa: F841 — read by name below
            for row in (
                duckdb.sql(
                    f"SELECT {', '.join(f't.{column}' for column in facts)} FROM read_parquet('{path}') t "
                    "SEMI JOIN wanted w ON t.bill_id = w.bill_id"
                )
                .to_arrow_table()
                .to_pylist()
            ):
                bill_rows[row["bill_id"]] = row
    outcome = BodyPass(
        bills_source=bills_acquirer,
        body_source=body_acquirer,
        remaining=remaining,
        engine=engine,
        classify=classify,
        summarize_diff=summarize_diff,
        refusals=refusals,
        summarize=summarize,
        bill_rows=bill_rows,
        package_refusals=package_refusals,
    ).run(work)
    outcome.retired_pairs |= retired_pairs
    outcome.superseded |= redundant
    return outcome


def _detected_at() -> str:
    """The run instant every event this run detects is stamped with."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
