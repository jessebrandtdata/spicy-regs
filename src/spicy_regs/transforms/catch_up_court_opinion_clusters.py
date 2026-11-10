"""Append completed Search id ranges to a native court prior: one a day, or an explicitly selected one.

Both routes preserve the parent and add a disjoint ID range above it; neither
infers that the parent is a complete bulk export or refreshes existing cluster
values. The explicit route takes the operator's retained evidence for the
prior's source edition and one reviewed range. The daily route
(:func:`daily_catch_up`) reads the edition and the id it reached from the
published generation's own journal, and spends one day's share of the
account's quota on whole id windows above it (docs/court-search-recovery.md).
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Generator, Iterable, Mapping
from contextlib import closing
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import httpx
import pyarrow.compute as pc
import pyarrow.parquet as pq
from loguru import logger
from rulespec_artifacts import publish_directory_no_replace

from spicy_regs.court_receipts import finish_court_output, local_receipt_selection
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
from spicy_regs.etl_receipts import combine_receipts
from spicy_regs.selected_generations import SelectedInputs
from spicy_regs.sources import publication
from spicy_regs.sources.courtlistener import CourtListenerOpinionSearchReader, require_api_token
from spicy_regs.transforms._courtlistener_writer import CourtListenerTableWriter, check_headroom
from spicy_regs.transforms.build_court_bulk_tables import _highest_cluster_id
from spicy_regs.transforms.build_court_opinion_clusters import BATCH_ROWS, _SCHEMA, _shape_search
from spicy_regs.transforms.court_scope import CourtScope, court_jurisdictions

if TYPE_CHECKING:
    from spicy_docs.sources.courtlistener.search import CourtListenerTraversalBudget
    from spicy_regs.source_evidence import CaptureEvidence

DATASET = "court_opinion_clusters"
OUTPUT = DATASET + ".parquet"
FAMILY = "court-opinion-clusters"
CATCH_UP_EVENT = "catchup-selection"

#: Search attempts one daily run may make, answered or not, and what it leaves for the docket job's next run. The
#: arithmetic against the account's limits has one home: "Quota" in docs/court-search-recovery.md.
DAILY_REQUESTS = 105
DOCKET_REQUESTS = 7
#: 48 an hour, so a run paces itself through the hourly window without a refusal.
DAILY_REQUEST_INTERVAL_SECONDS = 75.0
#: How much later than today's run yesterday's may have started (scheduled runs start late, never early) and
#: still be recognised as the requests now leaving the rolling day.
LOCK_STEP_SECONDS = 5400.0
#: Retry-After waits one run may add to its pacing, the backlog request included, before it stops and publishes
#: what it has.
RETRY_WAIT_SECONDS = 1800.0
#: Results on one Search page; the publisher fixes it.
PAGE_SIZE = 20
#: The most pages one id window is sized for. A window is published whole or not at all, so this bounds what a
#: failed one costs, and how long (38 minutes) its membership must hold still while it is walked.
WINDOW_PAGES = 30
#: Ids left unread below the newest id the backlog count proves assigned, where clusters are still being listed.
FRONTIER_MARGIN = 20


class NothingToCatchUp(Exception):
    """The daily run found nothing it may publish today; the published generation stands unchanged."""


class _WindowOverflow(Exception):
    """An id window lists more clusters (``args[0]``) than the pages it was allotted."""


@dataclass
class _Spend:
    """The live Search attempts one day's run has dispatched."""

    attempts: int = 0


def _live_transport() -> httpx.BaseTransport:
    return httpx.HTTPTransport(trust_env=False)


class _CountedTransport(httpx.BaseTransport):
    """One walk's transport. An attempt is counted when it is dispatched: a refused or timed-out request is still
    one the publisher may have counted."""

    def __init__(self, spend: _Spend) -> None:
        self.spend, self.delegate = spend, _live_transport()

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.spend.attempts += 1
        return self.delegate.handle_request(request)

    def close(self) -> None:
        self.delegate.close()


def check_catch_up_directories(prior: Path, output: Path, retained_search: Path | None) -> None:
    """Refuse paths that could write checkpoints or output into the immutable prior."""
    prior = prior.resolve()
    # Include the fixed write locations used by the rollup and direct transform.
    # A disjoint output root can still contain a link into the prior.
    output_targets = (output, *(output / name for name in (
        "source-evidence", ".builds", "generations", "generation-inputs",
        f"generation-inputs/{FAMILY}", ".catchup-pins", ".court-delta", ".court-etl",
    )))
    if any(target.resolve().is_relative_to(prior) for target in output_targets):
        raise ValueError("Court catch-up output must be outside the selected prior")
    if retained_search is not None:
        retained = retained_search.resolve()
        retained_targets = (retained, *(retained / name for name in (
            "run.lock", "selection.json", "checkpoint.json", "not-before.json", "captures",
        )))
        if prior.is_relative_to(retained) or any(target.resolve().is_relative_to(prior) for target in retained_targets):
            raise ValueError("Court retained search must be outside the selected prior and use a separate directory")


def _append_native(prior: Path, delta: Path, destination: Path, *, guard) -> None:
    """Copy exact Arrow values in bounded batches; never choose an overlap winner."""
    schema = pq.read_schema(prior)
    if not schema.equals(SUBJECT_SCHEMAS[DATASET]) or not schema.equals(pq.read_schema(delta)):
        raise ValueError("Court catch-up requires matching native subject schemas")
    ids = pq.read_table(delta, columns=["cluster_id"])["cluster_id"]
    if len(pc.call_function("unique", [ids])) != len(ids) or ids.null_count:
        raise ValueError("Court catch-up delta repeats or omits a cluster identity")
    with pq.ParquetWriter(destination, schema, compression="zstd") as writer:
        for source in (prior, delta):
            with pq.ParquetFile(source) as parquet:
                for batch in parquet.iter_batches(batch_size=BATCH_ROWS):
                    guard()
                    check_headroom(0, path=destination.parent)
                    if source == prior:
                        overlap = pc.call_function(
                            "is_in", [batch.column("cluster_id")], options=pc.SetLookupOptions(value_set=ids)
                        )
                        if pc.call_function("any", [overlap]).as_py():
                            raise ValueError("Court catch-up delta overlaps the selected prior")
                    writer.write_batch(batch, row_group_size=BATCH_ROWS)


def catch_up_court_opinion_clusters(
    output_dir: Path,
    *,
    prior_directory: Path,
    expected_prior: str,
    source_edition: date,
    source_edition_evidence: Path,
    source_edition_sha256: str,
    courts_local_file: Path,
    courts_sha256: str,
    search_through: int,
    search_budget: CourtListenerTraversalBudget,
    evidence: CaptureEvidence,
    retained_search_directory: Path | None = None,
) -> Path:
    """Fetch before prior admission/restoration; return a private complete candidate.

    ``evidence`` must carry the ordinary rollup's captured publication snapshot.
    The caller selects the exact parent and retained source-edition evidence;
    their relationship is an explicit reviewed input, not an edition detector.
    The local courts original must belong to that edition. Both small inputs
    are byte-pinned and retained with the selection. No bulk source is read.

    Prior member hashes and a native ID-only projection precede source access.
    After complete bounded traversal, only fresh rows are converted. Prior rows
    and all receipt values/identities survive unchanged, in prior-then-delta
    order. The returned owned family MUST pass ordinary generation admission
    and publication; this function neither admits nor selects a generation.
    """
    from spicy_docs.sources.courtlistener.search import CourtListenerTraversalBudget

    check_catch_up_directories(prior_directory, output_dir, retained_search_directory)
    if not isinstance(source_edition, date) or not isinstance(search_budget, CourtListenerTraversalBudget):
        raise ValueError("Court catch-up requires an explicit source edition and traversal allocation")
    require_api_token("Court opinion-cluster search catch-up")
    entry, members = _published_prior(evidence)
    if entry["artifactDigest"] != expected_prior:
        raise ValueError("Court catch-up prior differs from the captured publication")
    prior = prior_directory / OUTPUT
    receipts = prior_directory / entry["etlReceipts"]["key"]
    output_dir.mkdir(parents=True, exist_ok=True)
    selected = SelectedInputs(output_dir, output_dir / ".catchup-pins", public_url="")
    for path, member in zip((prior, receipts), members, strict=True):
        if selected.member_pin(path) != {"sha256": member.sha256, "byteSize": member.byte_size}:
            raise ValueError("Court catch-up prior bytes differ from the captured publication")
    for path, digest in ((source_edition_evidence, source_edition_sha256), (courts_local_file, courts_sha256)):
        if selected.member_pin(path)["sha256"] != digest:
            raise ValueError("Court catch-up source edition evidence differs from its byte pin")
    above = _native_ceiling(prior)
    retained_search = None
    if retained_search_directory is not None:
        from spicy_regs.sources.courtlistener_resume import RetainedCourtSearch

        retained_search = RetainedCourtSearch(retained_search_directory, {
            "family": FAMILY,
            "parent": entry,
            "source_edition": source_edition.isoformat(),
            "source_edition_sha256": source_edition_sha256,
            "courts_sha256": courts_sha256,
            "above": above,
            "through": search_through,
        })
    reader = CourtListenerOpinionSearchReader(
        above=above,
        through=search_through,
        traversal_budget=search_budget,
        evidence=evidence,
        retained_search=retained_search,
    )
    evidence.retain_file(
        source_edition_evidence,
        stage="catchup-source-edition",
        source_edition=source_edition.isoformat(),
        prior_generation=expected_prior,
    )
    evidence.retain_file(courts_local_file, stage="catchup-courts-original", source_edition=source_edition.isoformat())
    scope = CourtScope.for_courts(court_jurisdictions(dump_date=source_edition, local_file=courts_local_file))
    selected.check()
    evidence.event(
        CATCH_UP_EVENT,
        parent=expected_prior,
        source_edition=source_edition.isoformat(),
        above=above,
        through=search_through,
        max_requests=search_budget.max_requests,
        max_wait_seconds=str(search_budget.max_wait_seconds),
        coverage=("Preserved parent plus bounded all-status indexed search; unindexed clusters may be absent. "
                  "Bulk coverage inherited; maximum ID is not citation membership proof."),
    )
    return _append_search_rows(
        output_dir, reader.iter_records(), prior=prior, receipts=receipts, selected=selected, scope=scope,
        diagnostics={"source_edition": source_edition.isoformat(), "prior_generation": expected_prior,
                     "above": above, "through": search_through},
    )


def _published_prior(evidence: CaptureEvidence) -> tuple[Mapping, tuple[publication.Member, publication.Member]]:
    """The captured generation's entry and its subject and receipt members; a split or shared family refuses."""
    snapshot = evidence.read_snapshot
    if snapshot is None or evidence.family != FAMILY:
        raise ValueError("Court catch-up requires the captured court publication snapshot")
    owner = publication.table_owner(snapshot, OUTPUT)
    if owner is None or owner[0] != FAMILY:
        raise ValueError("Court catch-up prior differs from the captured publication")
    entry = owner[1]
    if entry.get("etlReceipts", {}).get("datasets") != [DATASET]:
        raise ValueError("Court catch-up requires the selected single-dataset receipt family")
    (subject_member,) = publication.table_members(snapshot, OUTPUT)
    (receipt_member,) = publication.receipt_members(snapshot, dataset=DATASET)
    return entry, (subject_member, receipt_member)


def _native_ceiling(prior: Path) -> int:
    if not pq.read_schema(prior).equals(SUBJECT_SCHEMAS[DATASET]):
        raise ValueError("Court catch-up prior is not the native cluster schema")
    above = _highest_cluster_id([str(prior)])
    if above is None:
        raise ValueError("Court catch-up prior has no numeric cluster ceiling")
    return above


def _append_search_rows(output_dir: Path, results: Iterable[dict], *, prior: Path, receipts: Path,
                        selected: SelectedInputs, scope: CourtScope, diagnostics: dict) -> Path:
    """Convert a completed selection's rows and append them to the prior; return the private complete candidate."""
    staged = output_dir / (".clusters-delta-" + uuid4().hex + ".parquet")
    writer = CourtListenerTableWriter(staged, schema=_SCHEMA, batch_size=BATCH_ROWS)
    try:
        for result in results:
            writer.add(_shape_search(result, scope=scope))
        writer.close()
        selected.check()
    except BaseException:
        writer.abort()
        raise

    generation_id = uuid4().hex
    delta = finish_court_output(
        DATASET,
        staged,
        output_dir / ".court-delta" / generation_id,
        generation_id=generation_id,
        diagnostics={**diagnostics, "search_rows": writer.written},
    )
    delta_receipts, _ = local_receipt_selection(delta)
    selected.member_pin(delta)
    selected.member_pin(delta_receipts)
    bundle = output_dir / ".court-etl" / uuid4().hex
    bundle.parent.mkdir(exist_ok=True)
    with TemporaryDirectory(prefix=".court-append-", dir=bundle.parent) as temporary:
        complete = Path(temporary) / "bundle"
        complete.mkdir()
        _append_native(prior, delta, complete / OUTPUT, guard=selected.check)
        combine_receipts([receipts, delta_receipts], complete / "etl_receipts.parquet")
        selected.check()
        (complete / "court-family.json").write_text(
            json.dumps({"generation_id": generation_id, "datasets": [DATASET]}) + "\n"
        )
        publish_directory_no_replace(complete, bundle)
    return bundle / OUTPUT


def held_selection(evidence: CaptureEvidence | None) -> tuple[date, int] | None:
    """The export edition the published table holds and the cluster id it is complete through, by its own journal.

    A catch-up and a bulk rebuild each journal one; a generation that states neither (or no generation) is ``None``,
    and only a bulk rebuild can establish what it holds.
    """
    if evidence is None:
        return None
    # Named literally: a conversion hands on the events it finds read back this way (native_conversion.INHERITED_EVENTS).
    stated = evidence.inherited_event("catchup-selection") or evidence.inherited_event("export-selection")
    try:
        if stated is None or type(stated["through"]) is not int:
            return None
        return date.fromisoformat(stated["source_edition"]), stated["through"]
    except (KeyError, TypeError, ValueError):
        return None


def _spent_stop(error: BaseException) -> bool:
    """Whether a walk stopped because the day ran out, as designed, and not on a fault.

    That is a Retry-After the run could not wait for, the last allowed attempt, or less waiting time left than
    one pacing gap. Whole windows read before it are published and the run succeeds.
    """
    allocation = getattr(error, "traversal_allocation", None)
    return bool(allocation) and (
        allocation["retryAfterSeconds"] is not None
        or allocation["requestCount"] >= allocation["maxRequests"]
        or allocation["maxWaitSeconds"] - allocation["waitedSeconds"] < DAILY_REQUEST_INTERVAL_SECONDS
    )


def _walk(above: int, through: int | None, *, pages: int, attempts: int, seconds: float, spend: _Spend,
          evidence: CaptureEvidence, max_records: int | None = None) -> tuple[list[dict], int | None]:
    """One whole selection's records and its declared count, in at most ``attempts`` attempts.

    A selection that lists more than ``pages`` pages overflows after its first; ``max_records`` reads a prefix.
    """
    from spicy_docs.sources.courtlistener.search import CourtListenerTraversalBudget

    reader = CourtListenerOpinionSearchReader(
        above=above, through=through, max_records=max_records, evidence=evidence,
        transport=_CountedTransport(spend),
        traversal_budget=CourtListenerTraversalBudget(attempts, max(seconds, 0.0)),
        min_request_interval_seconds=DAILY_REQUEST_INTERVAL_SECONDS,
    )
    records: list[dict] = []
    with closing(cast(Generator[dict, None, None], reader.iter_records())) as walk:
        for record in walk:
            if max_records is None and (reader.declared_count or 0) > PAGE_SIZE * pages:
                raise _WindowOverflow(reader.declared_count)
            records.append(record)
    return records, reader.declared_count


def daily_catch_up(output_dir: Path, *, evidence: CaptureEvidence, source_edition: date, above: int,
                   max_requests: int | None = None) -> tuple[Path, BaseException | None]:
    """Append one day's whole id windows above ``above``; return the candidate and any fault it built around.

    ``above`` and ``source_edition`` are what the published generation's journal states (:func:`held_selection`);
    ``max_requests`` lowers the day's attempts below ``DAILY_REQUESTS``. The order keeps every stop cheap: the
    account's usage (a free read) decides how many attempts today may have; one Search request then states the
    backlog; only then is the prior downloaded and checked against ``above``.

    The run walks id windows upward. Search orders by filing date, so only a window walked to its end joins the
    table. A window of ``W`` ids lists at most ``W`` clusters, so one sized to the pages left always completes;
    a window that lists nothing doubles the next, and one that then lists too much is halved. No window reaches
    past ``above + backlog - FRONTIER_MARGIN``: that many clusters above ``above`` prove every id up to
    ``above + backlog`` assigned, so no cluster created during the walk can join a window and change its count,
    and a window that lists nothing has shown its ids hold nothing listed. The table is therefore complete
    through the end of its last whole window, for what Search listed when each was read, and the next run
    resumes there; the newest clusters wait for a later day.

    ``NothingToCatchUp`` leaves everything as published. Running out of quota, attempts or waiting time ends the
    day without a fault. A fault after some windows completed is returned beside the candidate, so the caller
    publishes them and still reports it.
    """
    from spicy_regs.sources import courtlistener_resume as quota

    wanted = DAILY_REQUESTS if max_requests is None else max_requests
    if type(wanted) is not int or not 2 <= wanted <= DAILY_REQUESTS:
        raise ValueError(f"Court daily catch-up takes 2 to {DAILY_REQUESTS} requests, not {wanted!r}")
    token = require_api_token("Court opinion-cluster search catch-up")
    entry, members = _published_prior(evidence)
    public_url = os.getenv("R2_PUBLIC_URL")
    if not public_url:
        raise ValueError("Court daily catch-up reads its prior from R2_PUBLIC_URL")
    allowed, not_before, usage = quota.daily_allowance(
        quota.read_usage(token, None), wanted, request_interval=DAILY_REQUEST_INTERVAL_SECONDS,
        reserve=DOCKET_REQUESTS, lock_step_seconds=LOCK_STEP_SECONDS,
    )
    evidence.event("quota-preflight", stage="courtlistener-quota", requested_live=wanted,
                   allowed_live=allowed, not_before=not_before.isoformat() if not_before else None, windows=usage)
    logger.info("Opinion clusters: quota allows {} of {} requests today{}; usage {}", allowed, wanted,
                f", the first at {not_before.isoformat()}" if not_before else "", usage)
    if allowed < 2:  # One request states the backlog; a window needs at least one more.
        raise NothingToCatchUp(f"CourtListener quota leaves {allowed} Search requests today")
    if not_before is not None:
        time.sleep(max((not_before - quota._now()).total_seconds(), 0.0))

    spend = _Spend()
    started = time.monotonic()
    try:
        _, backlog = _walk(above, None, pages=1, attempts=allowed, seconds=RETRY_WAIT_SECONDS, spend=spend,
                           evidence=evidence, max_records=1)
    except Exception as error:
        if _spent_stop(error):
            raise NothingToCatchUp(f"CourtListener refused the backlog request: {error}") from error
        raise
    last = time.monotonic()
    retry_wait = max(RETRY_WAIT_SECONDS - (last - started), 0.0)
    backlog = backlog or 0
    limit = above + backlog - FRONTIER_MARGIN
    logger.info("Opinion clusters: {:,} clusters listed above id {:,} ({:,} pages), so ids to {:,} may be read; "
                "{} requests left today", backlog, above, -(-backlog // PAGE_SIZE), max(limit, above),
                allowed - spend.attempts)
    if limit <= above:
        raise NothingToCatchUp(f"Search lists {backlog:,} clusters above id {above:,}; the newest "
                               f"{FRONTIER_MARGIN} wait for later ones")

    directory = output_dir / ".catchup-prior"
    directory.mkdir(parents=True, exist_ok=True)
    selected = SelectedInputs(output_dir, output_dir / ".catchup-pins", public_url="")
    prior, receipts = directory / OUTPUT, directory / entry["etlReceipts"]["key"]
    for path, member in zip((prior, receipts), members, strict=True):
        check_headroom(member.byte_size or 0, path=directory)
        downloaded = publication.fetch_member(public_url, member, path)
        if isinstance(downloaded, bool):
            raise ValueError("Court catch-up prior is not a pinned published member")
        selected.adopt_download(path, downloaded)
    if _native_ceiling(prior) > above:
        raise ValueError("Court published table holds a cluster above the id its journal says it reached")
    # Refuse now, not after the day's requests: the appended copy and the generation's copy each need this much.
    check_headroom(2 * sum(member.byte_size or 0 for member in members), path=directory)
    scope = CourtScope.for_courts(court_jurisdictions(dump_date=source_edition))

    records: list[dict] = []
    fault: BaseException | None = None
    deadline = time.monotonic() + (allowed - spend.attempts) * DAILY_REQUEST_INTERVAL_SECONDS + retry_wait
    floor, widened, walked = above, 0, 0
    while spend.attempts < allowed and floor < limit:
        pages = min(allowed - spend.attempts, WINDOW_PAGES)
        span = min(max(widened, PAGE_SIZE * pages), limit - floor)
        # Each walk paces its own requests; its first follows the previous walk's last by the same interval.
        time.sleep(max(DAILY_REQUEST_INTERVAL_SECONDS - (time.monotonic() - last), 0.0))
        try:
            found, _ = _walk(floor, floor + span, pages=pages, attempts=allowed - spend.attempts,
                             seconds=deadline - time.monotonic(), spend=spend, evidence=evidence)
        except _WindowOverflow as overflow:
            last = time.monotonic()
            if span <= PAGE_SIZE * pages:  # More clusters than ids: the count cannot be walked or trusted.
                fault = ValueError(f"CourtListener lists {overflow.args[0]:,} clusters in the {span:,} ids "
                                   f"above {floor:,}")
                break
            widened = span // 2
            continue
        except Exception as error:
            fault = error
            break
        last = time.monotonic()
        walked += 1
        records += found
        widened = 0 if found else 2 * span
        floor += span
    if fault is not None:
        logger.warning("Opinion clusters: stopped after {} whole windows ({:,} clusters, {} requests): {}",
                       walked, len(records), spend.attempts, fault)
        if _spent_stop(fault):
            fault = None
    if not records:
        if fault is not None:
            raise fault
        if floor == limit:
            # Every id the count proves assigned was read and lists nothing. Saying "nothing to do" would ask
            # the same requests every day; only a published generation can move the floor.
            raise ValueError(
                f"Search lists {backlog:,} clusters above id {above:,} but none in ids {above + 1:,} to {limit:,}, "
                "which that count proves assigned. Nothing was published; the next run asks the same until the "
                "backlog outgrows the gap"
            )
        raise NothingToCatchUp(f"The day's {spend.attempts} requests ended before a window above id {above:,} "
                               "listed a cluster")

    evidence.event(
        CATCH_UP_EVENT,
        parent=entry["artifactDigest"],
        source_edition=source_edition.isoformat(),
        above=above,
        through=floor,
        declared_backlog=backlog,
        clusters=len(records),
        windows=walked,
        requests=spend.attempts,
        max_requests=allowed,
        coverage=("Preserved parent plus whole all-status indexed id windows; unindexed clusters may be absent. "
                  "Complete through `through` as indexed when read; `declared_backlog` less `clusters` remained "
                  "above it. Bulk coverage inherited; maximum ID is not citation membership proof."),
    )
    logger.info("Opinion clusters: {:,} clusters in {} windows and {} requests, through id {:,}; about {:,} remain",
                len(records), walked, spend.attempts, floor, max(backlog - len(records), 0))
    candidate = _append_search_rows(
        output_dir, records, prior=prior, receipts=receipts, selected=selected, scope=scope,
        diagnostics={"source_edition": source_edition.isoformat(), "prior_generation": entry["artifactDigest"],
                     "above": above, "through": floor},
    )
    # The candidate holds every prior row; generation admission needs the disk more than a second copy.
    prior.unlink()
    receipts.unlink()
    return candidate, fault
