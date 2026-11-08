"""Transform: build ``committees`` and ``committee_assignments`` from three publishers.

``committees`` is the Congress.gov ``committee`` list route walked whole with
no Congress, every committee of every Congress the publisher records (818
records in four pages, 2026-09-26), one row per ``systemCode``, with the
``committee/{chamber}/{system_code}`` detail folded on where captured under the
per-run cap; ``shape_committee`` refuses a detail whose ``systemCode`` is not
the row's, so a fold never lands on the wrong row. ``committee_assignments`` is
who sits where *today*, from the House Clerk's ``MemberData.xml`` and the
Senate's ``cvc_member_data.xml``: two keyless requests for the current Congress
only, because that is the only Congress those files describe. The House file
states its Congress and the reader proves it against the request; the Senate
file states none, so its rows carry the caller's Congress with
``congress_basis = caller``. A vacancy, and a seated member whose only
assignment is the file's ``<committee rank=""/>`` placeholder, have no row.
Needs an api.data.gov key for the two Congress.gov routes; the chamber files
are keyless.

**The route over-declares.** :func:`walk_route` reads past a single
terminal-page count mismatch (what was served is published, both numbers
logged); every other refusal fails the run.

**Incremental.** The list is re-walked whole every run; per listed committee, a
row already published with its detail and the same list ``update_date`` is left
standing (no request, no fresh row), anything else is asked for newest
``update_date`` first under :data:`MAX_DETAILS_PER_RUN`. When the cap or a
refusal stops the ask, a committee not yet published gets its list row with
``detail_captured = false``, and one already published keeps its prior row — a
fresh list-only row must never overwrite a held detail, because the merge
replaces rows whole. A ``401``/``403`` aborts the run.

**Assignments are snapshots.** Each file captured this run replaces every prior
row for its chamber and Congress (:func:`retire_prior_rows`), so a seat the
file no longer lists is gone on the next capture and an earlier Congress keeps
its last capture. A chamber whose file was not established this run keeps its
prior rows; a keyless ``401``/``403`` (``CommitteeRosterRefusedError``) aborts.

**Earlier Congresses come from the Congressional Directory** (ss-ehci): one
GovInfo edition per Congress, 113-117 (:mod:`spicy_regs.transforms.directory_rosters`),
read once, when the published table holds no ``congress_basis = directory`` row
for that Congress, or again when named in ``COMMITTEE_ROSTERS_DIRECTORY_REREAD``
(comma-separated Congresses), which first retires that Congress's directory
rows. Every response is retained through the run's ``evidence``; every printed
entry that publishes no row is logged and journaled as a disposition. An edition
that cannot be read is retried next run; a ``401``/``403`` aborts.
"""

from __future__ import annotations

import os
from collections import Counter
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, NamedTuple, Protocol

import httpx
from loguru import logger
from spicy_docs.reading.paged_json import PagedJsonBudget, PagedJsonSourceError
from spicy_docs.schemas.roster_tables import shape_committee, shape_house_assignment, shape_senate_assignment
from spicy_docs.schemas.tables import TableContractError, text
from spicy_docs.sources.congress.committee_rosters import (
    CommitteeRosterAcquirer,
    CommitteeRosterBudget,
    CommitteeRosterError,
    CommitteeRosterRefusedError,
)
from spicy_docs.sources.congress.listing import LIST_ROUTES, MAX_LIMIT, CongressListingReader, list_route_url
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyBudget
from spicy_docs.sources.govinfo.directory_rosters import DirectoryRosterError
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import current_congress
from spicy_regs.transforms.congress_walk import ListingSource, PerRunCap, walk_route
from spicy_regs.transforms.directory_rosters import (
    DIRECTORY_EDITIONS,
    DirectorySource,
    GovInfoDirectorySource,
    directory_congresses_pending,
    interpret_edition,
    read_rows,
)
from spicy_regs.transforms.table_merge import merge_contract_table, published_table, retire_prior_rows


class RosterSource(Protocol):
    """What this transform needs of the chamber-file acquirer."""

    def acquire_house(self, *, congress: int, session: int | None = ..., max_bytes: int | None = ...) -> Any: ...

    def acquire_senate(self, *, max_bytes: int | None = ...) -> Any: ...


LIST_BUDGET = PagedJsonBudget(
    max_requests=500,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: Two requests, paced; the House file is ~557 KB, the Senate file ~68 KB
#: (measured 2026-09-19), well under the acquirer's own per-file defaults.
ROSTER_BUDGET = CommitteeRosterBudget(
    max_requests=4,
    max_bytes=4 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=1.0,
)

#: Pages of ``MAX_LIMIT`` for the whole route; its 818 records took four on 2026-09-26.
MAX_PAGES = 10

#: Detail records asked for per run: a whole Congress in one run (~1 minute
#: at the pacing above), bounding a backfill (the 581 committees the unscoped
#: route added to the 119th's 236 take two runs).
MAX_DETAILS_PER_RUN = 300

#: Package metadata (a CDIR package MODS is 2.8-3.0 MB, measured 2026-11-08) and committee granules (7-163 KB).
DIRECTORY_BUDGET = GovInfoBodyBudget(
    max_requests=6,
    max_body_bytes=2 * 1024 * 1024,
    max_metadata_bytes=8 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=1.0,
)
DIRECTORY_DISCOVERY_BUDGET = PagedJsonBudget(
    max_requests=10,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=1.0,
)
DIRECTORY_REREAD_ENV = "COMMITTEE_ROSTERS_DIRECTORY_REREAD"

NAME = "committees"
ASSIGNMENTS = "committee_assignments"
TRUE = "true"


class HeldCommittee(NamedTuple):
    """The prior table's state for one ``system_code``: list ``update_date`` and whether a detail was captured."""

    update_date: str | None
    detail_captured: str | None


def _held_committees(prior_file: Path | None) -> dict[str, HeldCommittee]:
    if prior_file is None:
        return {}
    import duckdb

    rows = duckdb.sql(f"SELECT system_code, update_date, detail_captured FROM read_parquet('{prior_file}')").fetchall()
    return {str(code): HeldCommittee(update_date, captured) for code, update_date, captured in rows}


def _list_committees(reader: ListingSource) -> list[Mapping[str, Any]]:
    """Every committee the route lists, of every Congress, one record per ``systemCode``, newest first.

    Not scoped to a Congress, because the tables that name committees are not:
    ``committee_meetings`` keeps the previous Congress (its three 118th House
    bodies, such as ``hlvc00``, are on no 119th list) and ``bill_committees``
    reaches the 108th with the bill family's backfill (78 codes on no 119th
    list, all on the 108th-117th lists). The unscoped route lists all of them
    and every Congress's list besides, in four pages where the twelve lists of
    the 108th-119th take twelve; a record listed twice keeps the larger
    ``updateDate``, the row the merge would keep too.
    """
    route = LIST_ROUTES["committee"]
    walk = walk_route(reader, route, list_route_url(route, limit=MAX_LIMIT), max_pages=MAX_PAGES,
                      label="Committees: every Congress")
    by_code: dict[str, Mapping[str, Any]] = {}
    for record in walk.records:
        code = text(record.get("systemCode"))
        if not code:
            logger.warning("Committees: a list record states no systemCode; skipped")
            continue
        held = by_code.get(code)
        if held is None or (text(record.get("updateDate")) or "") > (text(held.get("updateDate")) or ""):
            by_code[code] = record
    return sorted(by_code.values(), key=lambda record: text(record.get("updateDate")) or "", reverse=True)


def _detail_url(record: Mapping[str, Any]) -> str | None:
    """The detail route's URL for one list record, or ``None`` when the reader's code grammar cannot spell it.

    SpicyDocs' grammar is the unscoped list's own (every one of its 817 codes, the
    95 Library of Congress name-authority ids such as ``n79043125`` and ``sp2k00``
    among them, whose details all answered, 2026-09-26). A code outside it stays a
    list-only row and spends no share of the per-run cap.
    """
    try:
        return list_route_url(LIST_ROUTES["committee-detail"], chamber=str(record.get("chamber") or "").lower(),
                              system_code=str(record.get("systemCode")))
    except PagedJsonSourceError:
        return None


def _detail(reader: ListingSource, url: str, code: str) -> Mapping[str, Any] | None:
    """One committee's detail record, or ``None`` when the route did not establish one.

    A ``401``/``403`` propagates and aborts the run; a refusal by the reader
    or a transport failure is this committee's gap, retried next run.
    """
    route = LIST_ROUTES["committee-detail"]
    try:
        for page in reader.records(route, url, max_pages=1):
            for detail in page.records:
                return detail
    except (PagedJsonSourceError, httpx.HTTPError, ConnectionError) as error:
        logger.warning("Committees: detail for {} not established: {}", code, scrub_credential(str(error), ""))
    return None


def _committee_rows(
    listed: list[Mapping[str, Any]], held: Mapping[str, HeldCommittee], reader: ListingSource, max_details: int
) -> list[dict]:
    rows: list[dict] = []
    unchanged = deferred = folded = unaddressable = 0
    cap = PerRunCap(max_details, "Committees: details")
    for record in listed:
        code = str(record.get("systemCode"))
        update_date = text(record.get("updateDate"))
        prior = held.get(code)
        if prior is not None and prior.detail_captured == TRUE and prior.update_date == update_date:
            unchanged += 1
            continue
        url = _detail_url(record)
        unaddressable += url is None
        detail = _detail(reader, url, code) if url is not None and cap.take() else None
        try:
            row = shape_committee(record, detail) if detail is not None else None
        except TableContractError as error:
            logger.warning("Committees: detail for {} refused by the contract: {}", code, error)
            row = None
        if row is None:
            if prior is not None:
                # The prior row — its detail included — stands until a run reaches this committee again.
                deferred += 1
                continue
            try:
                row = shape_committee(record)
            except TableContractError as error:
                logger.warning("Committees: list record {} refused by the contract: {}", code, error)
                continue
        else:
            folded += 1
        rows.append(row)
    logger.info(
        "Committees: {:,} listed — {:,} rows this run ({:,} with a detail), {:,} already folded and unchanged,"
        " {:,} held over, {:,} with a code the detail route cannot spell",
        len(listed),
        len(rows),
        folded,
        unchanged,
        deferred,
        unaddressable,
    )
    return rows


def _assignment_rows(rosters: RosterSource, congress: int, prior_file: Path | None) -> list[dict]:
    """Today's seats from both chamber files; a file not established this run leaves its chamber's prior rows."""
    rows: list[dict] = []
    try:
        house = rosters.acquire_house(congress=congress)
    except CommitteeRosterRefusedError:
        raise
    except (CommitteeRosterError, httpx.HTTPError, ConnectionError) as error:
        logger.warning("Committee assignments: House file not established: {}", scrub_credential(str(error), ""))
    else:
        roster = house.roster
        observed_at = house.capture.observed_at
        placeholders = sum(member.placeholder_assignments for member in roster.members)
        kinds: Counter[str] = Counter()
        for member in roster.members:
            if member.vacant:
                continue
            for assignment in member.assignments:
                kinds[assignment.kind] += 1
                rows.append(shape_house_assignment(member, assignment, roster=roster, observed_at=observed_at))
        if prior_file is not None:
            retire_prior_rows(prior_file, congress=str(congress), chamber="house")
        logger.info(
            "Committee assignments: House file states Congress {} session {}, published {} — {:,} seats, {:,} vacant,"
            " {:,} assignments {}, {:,} placeholders",
            roster.congress,
            roster.session,
            roster.publish_date,
            len(roster.members),
            roster.vacancies,
            sum(kinds.values()),
            dict(kinds),
            placeholders,
        )
    try:
        senate = rosters.acquire_senate()
    except CommitteeRosterRefusedError:
        raise
    except (CommitteeRosterError, httpx.HTTPError, ConnectionError) as error:
        logger.warning("Committee assignments: Senate file not established: {}", scrub_credential(str(error), ""))
    else:
        roster = senate.roster
        observed_at = senate.capture.observed_at
        before = len(rows)
        for senator in roster.senators:
            for assignment in senator.committees:
                rows.append(
                    shape_senate_assignment(
                        senator, assignment, congress=congress, roster=roster, observed_at=observed_at
                    )
                )
        if prior_file is not None:
            retire_prior_rows(prior_file, congress=str(congress), chamber="senate")
        logger.info(
            "Committee assignments: Senate file updated {} (states no Congress; rows carry {} from the caller) — {:,} senators, {:,} seats",
            roster.last_update_date,
            congress,
            len(roster.senators),
            len(rows) - before,
        )
    return rows


def _directory_rows(
    output_dir: Path,
    directory: DirectorySource,
    pending: list[int],
    reread: set[int],
    committee_rows: list[dict],
    priors: Mapping[str, Path | None],
    download_prior: Callable[[str, Path], bool],
    evidence,
) -> list[dict]:
    """Seats for each pending Congress from its directory edition; an edition not read keeps its prior rows."""
    by_code = {str(row["system_code"]): row for row in read_rows(priors[NAME])}
    by_code.update({str(row["system_code"]): row for row in committee_rows})
    members = {str(row["bioguide_id"]): row for row in read_rows(published_table(output_dir, "members", download_prior))}
    terms = read_rows(published_table(output_dir, "member_terms", download_prior))
    if not members or not terms:
        logger.warning("Committee assignments: members or member_terms unavailable; directory editions wait a run")
        return []
    rows: list[dict] = []
    for congress in pending:
        package_id = DIRECTORY_EDITIONS[congress]
        try:
            capture = directory.edition(package_id)
        except CredentialRefusedError:
            raise
        except (DirectoryRosterError, PagedJsonSourceError, httpx.HTTPError, ConnectionError, ValueError) as error:
            logger.warning("Committee assignments: {} not read: {}", package_id, scrub_credential(str(error), ""))
            continue
        if capture.edition.congress != str(congress):
            raise DirectoryRosterError(f"{package_id} states Congress {capture.edition.congress}, not {congress}")
        result = interpret_edition(
            capture.edition, capture.granules, committees=by_code.values(), terms=terms, members=members,
            observed_at=capture.observed_at,
        )  # fmt: skip
        prior_assignments = priors[ASSIGNMENTS]
        if congress in reread and prior_assignments is not None:
            retire_prior_rows(prior_assignments, congress=str(congress), congress_basis="directory")
        kinds = Counter(d.kind for d in result.dispositions)
        logger.info(
            "Committee assignments: {} (Congress {}, issued {}) — {:,} seats, dispositions {}",
            package_id, congress, capture.edition.date_issued, len(result.rows), dict(kinds),
        )  # fmt: skip
        for disposition in result.dispositions:
            if disposition.kind not in ("vacancy", "not_a_committee"):
                logger.info("  {} {}:{} {} | {}", disposition.kind, disposition.granule_id, disposition.line,
                            disposition.context, disposition.text)  # fmt: skip
        if evidence is not None:
            evidence.event(
                "directory-dispositions",
                package_id=package_id,
                congress=str(congress),
                seats=len(result.rows),
                dispositions=[
                    {"granule": d.granule_id, "line": d.line, "kind": d.kind, "context": d.context, "text": d.text}
                    for d in result.dispositions
                ],
            )
        rows.extend(result.rows)
    return rows


def _api_key() -> str:
    api_key = _resolve_api_key()
    if not api_key:
        raise RuntimeError(f"Committees need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
    return api_key


def _directory_source(api_key: str, evidence) -> DirectorySource:
    from spicy_regs.sources.retained import RetainedGovInfoBodyAcquirer, RetainedGovInfoDiscoveryReader

    return GovInfoDirectorySource(
        RetainedGovInfoBodyAcquirer(evidence=evidence, budget=DIRECTORY_BUDGET, api_key=api_key),
        RetainedGovInfoDiscoveryReader(evidence=evidence, budget=DIRECTORY_DISCOVERY_BUDGET, api_key=api_key),
    )


def build_committee_rosters(
    output_dir: Path,
    *,
    reader: ListingSource | None = None,
    rosters: RosterSource | None = None,
    directory: DirectorySource | None = None,
    evidence=None,
    max_details: int = MAX_DETAILS_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
    receipt_build=None,
) -> tuple[Path, Path]:
    """Build ``committees.parquet`` and ``committee_assignments.parquet``."""
    if receipt_build is not None:
        return receipt_build.run(build_committee_rosters, output_dir, **{
            key: value for key, value in locals().items() if key not in {"output_dir", "receipt_build"}
        })
    if reader is None:
        reader = CongressListingReader(budget=LIST_BUDGET, api_key=_api_key())
    rosters = rosters or CommitteeRosterAcquirer(budget=ROSTER_BUDGET)

    priors = {name: published_table(output_dir, name, download_prior) for name in (NAME, ASSIGNMENTS)}

    # 1. The enumeration, whole, then the detail fold under its cap.
    listed = _list_committees(reader)
    committee_rows = _committee_rows(listed, _held_committees(priors[NAME]), reader, max_details)

    # 2. Today's seats, for the Congress the files describe.
    assignment_rows = _assignment_rows(rosters, current_congress(), priors[ASSIGNMENTS])

    # 3. Earlier Congresses' seats, once each, from their directory editions.
    reread = {int(c) for c in os.environ.get(DIRECTORY_REREAD_ENV, "").split(",") if c.strip()}
    pending = directory_congresses_pending(priors[ASSIGNMENTS], reread)
    if pending and directory is None and evidence is None:
        logger.info("Committee assignments: no source evidence, so directory editions {} are not read", pending)
    elif pending:
        directory = directory or _directory_source(_api_key(), evidence)
        assignment_rows += _directory_rows(
            output_dir, directory, pending, reread, committee_rows, priors, download_prior, evidence
        )

    return (
        merge_contract_table(output_dir, NAME, committee_rows, prior_present=priors[NAME] is not None),
        merge_contract_table(output_dir, ASSIGNMENTS, assignment_rows, prior_present=priors[ASSIGNMENTS] is not None),
    )
