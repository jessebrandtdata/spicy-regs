"""Transform: build ``bill_subjects.parquet`` — the Library of Congress subject assignment per bill.

Closes the seam the retired list writer left: list-level ``congress_bills``
rows carry no subject assignment (one ``policy_area`` from a ~33-term
controlled list, plus any number of legislative subjects). A sibling table rather than extra columns, because the two artifacts
have different writers and fetch economics; it publishes on its own cron, keyed
by the same ``bill_id`` as ``congress_bills``, joined with a one-line ``LEFT JOIN``.

**The Congress picks the route.** From the 108th (``BULK_STATUS_FLOOR``) the
assignment is BILLSTATUS's, read without a per-bill request:

* a bill whose ``congress_bills`` row the bill family filled from BILLSTATUS
  (``schema_version`` set) is copied from that row on every run, and its row
  here is rewritten only when the copy differs, so a policy area CRS assigns
  after the first run arrives with the family's next read;
* a bill in a folder (Congress and bill type) the family has read, whose own
  row is still list-level, waits for the family's next run rather than
  downloading the zip the family is about to read — while its Congress is the
  current or previous one. In an older Congress the family's read is settled,
  so such a bill is one whose file the reader refused, and it goes to the API;
* a bill in a folder the family has not read comes from that folder's bulk zip
  through spicy-docs, one request per folder, at most
  :data:`MAX_FOLDERS_PER_RUN` folders a run. A bill whose file the reader
  refuses (the superseded 1.0.0 schema) goes to the API instead.

Below the 108th only Congress.gov's ``/subjects`` holds it: one keyed request
per bill, at most :data:`MAX_API_BILLS_PER_RUN` a run; without a key those
bills stay pending.

**One deadline for the network.** No folder read and no API request attempt —
first page, later page or retry — starts :data:`DEADLINE_SECONDS` or more after
the run began; what it would have answered is left for the next run. One
already started runs to its own bound: a folder read to its budget, an API
request to its 60-second timeout.

**Incremental and resumable.** Best-effort prior from R2; apart from the
family's rows, a bill is pending while the prior has no row for it; newest
Congress first. Only an answer becomes a row: a failed folder, an API timeout,
or a bill missing from the current or previous Congress's zip (a folder that
may simply lag) writes nothing, so the bill is asked again next run. A bill an
older Congress's zip does not list, or the API answers 404 for, gets a row with
a null ``policy_area`` and its carrier, so it is not asked again. Prior and new
rows merge on ``bill_id``, preferring the fresh one. A bill in
:data:`RECORDED_REFUSALS` is asked once a week, last, and named in every run's log.
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, Protocol

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.bill_subjects import (
    API_FIRST_CONGRESS,
    CARRIER_BULKDATA,
    BillSubjects,
    BillSubjectsFetcher,
    FetchCounts,
    assignment,
)
from spicy_regs.sources.congress_bills import _resolve_api_key
from spicy_regs.transforms.congress_scope import BULK_STATUS_FLOOR, bulk_status_budget, current_congress

if TYPE_CHECKING:
    from spicy_docs.sources.congress.bulk_status import BulkStatusArchive

    from spicy_regs.source_evidence import CaptureEvidence

OUTPUT = "bill_subjects.parquet"
BILLS_INPUT = "congress_bills.parquet"

#: Congress.gov bills per run: what the documented 5,000-requests-an-hour
#: budget allows one run at the fastest pacing (``DELAY_SECONDS``), with room
#: left for retries. Measured 2026-09-23, a bill costs 1.36 s, so
#: :data:`DEADLINE_SECONDS` is the bound a run meets first (~880 bills).
MAX_API_BILLS_PER_RUN = 2_000

#: Seconds from the start of a run after which no folder read and no API
#: request attempt starts. The reusable rollup workflow kills a job at 30
#: minutes and a killed job publishes nothing; 20 minutes leaves the merge and
#: the upload their room.
DEADLINE_SECONDS = 20 * 60

#: BILLSTATUS folder zips per run. Measured 2026-09-23 through
#: ``bulk_status_budget``: the 96 folders of Congresses 108-119 read in 169 s, the
#: largest (118 hr, 35.5 MB, 10,564 members) in 11 s at 343 MB peak RSS. 32
#: folders is four Congresses, about a minute; the 108th-117th backfill is three
#: runs.
MAX_FOLDERS_PER_RUN = 32

#: Bills Congress.gov answers every night with a list the fetcher refuses, so they
#: never get a row and would be asked first on every run. Observed in runs
#: 42111825704 through 42524722104 (2026-11-05..08): with the API slow, timeouts
#: on these three alone took ~13 of the 20-minute budget. They are asked only on
#: :data:`REFUSAL_RETRY_WEEKDAY`, after every other API bill, and each run logs
#: them. Remove an entry when its refusal reason no longer holds.
RECORDED_REFUSALS: dict[str, str] = {
    "107-s-2127": "page 1 is empty with 0 of 1 subjects read",
    "106-hr-3194": "1497 subjects stated, more than the 4 pages of 250 the walk reads",
    "106-hr-4577": "1407 subjects stated, more than the 4 pages of 250 the walk reads",
}

#: UTC weekday (Monday is 0) on which recorded refusals are asked again.
REFUSAL_RETRY_WEEKDAY = 6

#: The published schema: all VARCHAR, keyed by ``bill_id``, joinable straight to
#: ``congress_bills``. The subject list is a JSON string so the table stays flat
#: and portable, matching ``lobbying_filings``' JSON-valued columns.
COLUMNS = (
    "bill_id",
    "policy_area",
    "subjects_json",
    "subject_count",
    "carrier",
    "enriched_at",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])

#: Reads one folder's zip into its archive: the real acquirer, or a test's.
FolderReader = Callable[[int, str], "BulkStatusArchive"]


class PendingBill(NamedTuple):
    """One bill this run may answer, with what the bill family and the prior table hold for it."""

    bill_id: str
    congress: int
    bill_type: str
    bill_number: str
    from_family: bool
    family_folder: bool
    policy_area: str | None
    subjects_json: str | None
    #: The prior row's ``(policy_area, subjects_json, subject_count, carrier)``, or None.
    prior: tuple[str | None, str | None, str | None, str | None] | None


class SubjectsFetcher(Protocol):
    """What the API route needs from a fetcher: the real one, or a test's."""

    def subjects_for(self, congress: str, bill_type: str, bill_number: str) -> BillSubjects | None: ...
    def close(self) -> None: ...


def _shape(bill_id: str, policy_area: str | None, subjects: tuple[str | None, ...], carrier: str, now: str) -> dict:
    """Map one fetched assignment onto the published column shape."""
    return {
        "bill_id": bill_id,
        "policy_area": policy_area,
        "subjects_json": json.dumps(list(subjects)),
        "subject_count": str(len(subjects)),
        "carrier": carrier,
        "enriched_at": now,
    }


def _pending_bills(bills_file: Path, prior_file: Path, *, have_prior: bool) -> list[PendingBill]:
    """Bills this run may answer, newest Congress first, with the family's and the prior's fields.

    Every bill the family filled from BILLSTATUS, so a changed assignment is
    copied again, plus every other bill the prior table has no row for. Bills
    below the API's floor are never selected; the request could only 404.
    """
    import duckdb

    prior = (
        f"SELECT bill_id, policy_area, subjects_json, subject_count, carrier FROM read_parquet('{prior_file}')"
        if have_prior
        else "SELECT NULL::VARCHAR AS bill_id, NULL::VARCHAR AS policy_area, NULL::VARCHAR AS subjects_json, "
        "NULL::VARCHAR AS subject_count, NULL::VARCHAR AS carrier WHERE FALSE"
    )
    rows = duckdb.sql(
        f"""
        WITH bill AS (
            SELECT *, TRY_CAST(congress AS INTEGER) AS congress_number, lower(bill_type) AS folder_type
            FROM read_parquet('{bills_file}')
            WHERE bill_id IS NOT NULL AND bill_type IS NOT NULL AND bill_number IS NOT NULL
        ),
        family_folder AS (
            SELECT DISTINCT congress_number, folder_type FROM bill WHERE schema_version IS NOT NULL
        ),
        prior AS ({prior})
        SELECT bill.bill_id, bill.congress_number, bill.folder_type, bill.bill_number,
               bill.schema_version IS NOT NULL, family.congress_number IS NOT NULL,
               bill.policy_area, bill.subjects_json,
               prior.bill_id IS NOT NULL, prior.policy_area, prior.subjects_json, prior.subject_count, prior.carrier
        FROM bill
        LEFT JOIN family_folder AS family
          ON family.congress_number = bill.congress_number AND family.folder_type = bill.folder_type
        LEFT JOIN prior ON prior.bill_id = bill.bill_id
        WHERE bill.congress_number >= {API_FIRST_CONGRESS}
          AND (prior.bill_id IS NULL OR (bill.congress_number >= {BULK_STATUS_FLOOR} AND bill.schema_version IS NOT NULL))
        ORDER BY bill.congress_number DESC, bill.bill_id
        """
    ).fetchall()
    return [
        PendingBill(str(a), int(b), str(c), str(d), bool(e), bool(f), g, h, (j, k, m, n) if i else None)
        for a, b, c, d, e, f, g, h, i, j, k, m, n in rows
    ]


def _read_folders(
    folders: dict[tuple[int, str], list[PendingBill]],
    read_folder: FolderReader,
    *,
    recent: set[int],
    past_deadline: Callable[[], bool],
) -> tuple[list[tuple[str, BillSubjects | None]], list[PendingBill]]:
    """Answer each folder's pending bills from its one zip; return the answers and the bills for the API.

    A bill whose file the reader refuses (the superseded 1.0.0 schema: 13
    members of the 96 folders on 2026-09-23, 113-hr-4200 and 115-hr-3354 among
    them) goes to the API, and so does one missing from a zip holding a member
    whose name the reader could not read. A bill absent from a zip whose every
    member was named, or a folder the publisher does not publish, is not held —
    except in the ``recent`` Congresses, whose folders can lag the bill list, so
    there it is no answer and is asked again next run. A folder not started
    before the deadline is left, with its bills, for the next run.
    """
    from spicy_docs.sources.congress.bill_acquisition import BillSourceUnavailableError
    from spicy_docs.sources.congress.bill_status import BillSourceError
    from spicy_docs.transport.credentials import CredentialRefusedError

    answers: list[tuple[str, BillSubjects | None]] = []
    to_api: list[PendingBill] = []
    not_held = BillSubjects(None, (), CARRIER_BULKDATA, held=False)
    for done, ((congress, bill_type), bills) in enumerate(folders.items()):
        if past_deadline():
            logger.warning(
                "Bill subjects: run deadline reached; {:,} of {:,} folders left for the next run",
                len(folders) - done,
                len(folders),
            )
            break
        missing = None if congress in recent else not_held
        try:
            archive = read_folder(congress, bill_type)
        except CredentialRefusedError:
            raise
        except BillSourceUnavailableError:
            logger.warning("Bill subjects: BILLSTATUS {} {} folder is not published", congress, bill_type)
            answers.extend((bill.bill_id, missing) for bill in bills)
            continue
        except (BillSourceError, httpx.HTTPError, ConnectionError) as error:
            logger.warning(
                "Bill subjects: BILLSTATUS {} {} folder not read: {}", congress, bill_type, type(error).__name__
            )
            answers.extend((bill.bill_id, None) for bill in bills)
            continue
        statuses = {m.identity.number: m.status for m in archive.members if m.identity is not None}
        every_member_named = all(m.identity is not None for m in archive.members)
        for bill in bills:
            number = int(bill.bill_number) if bill.bill_number.isdigit() else None
            status = statuses.get(number) if number is not None else None
            if status is not None:
                answers.append((bill.bill_id, assignment(status.policy_area, status.subjects, CARRIER_BULKDATA)))
            elif number in statuses or (number is not None and not every_member_named):
                to_api.append(bill)
            else:
                answers.append((bill.bill_id, missing if number is not None else None))
        logger.info(
            "Bill subjects: BILLSTATUS {} {} read for {:,} pending bills ({:,} members, {:,} refused by the reader)",
            congress,
            bill_type,
            len(bills),
            len(archive.members),
            archive.refused_count,
        )
        del archive, statuses  # one folder's parsed members at a time, not two
    return answers, to_api


@contextmanager
def _folder_reader(injected: FolderReader | None, evidence: CaptureEvidence | None = None) -> Iterator[FolderReader]:
    """The injected reader, or the keyless bulk route on the bill family's per-zip budget."""
    if injected is not None:
        yield injected
        return
    from spicy_docs.sources.congress.bulk_status import BulkStatusAcquirer

    budget = bulk_status_budget()
    with BulkStatusAcquirer(
        budget=budget,
        transport=None if evidence is None else evidence.transport(stage="subject-bulk", max_bytes=budget.max_bytes),
    ) as acquirer:

        def read(congress: int, bill_type: str) -> BulkStatusArchive:
            archive = acquirer.acquire(congress, bill_type).archive
            assert archive is not None  # only an ``unchanged_since`` skip returns none, and none is passed
            return archive

        yield read


def _adopt_local_output(out_file: Path, prior_file: Path) -> bool:
    """Treat an artifact left by a previous local run as this run's prior."""
    if not out_file.exists():
        return False
    out_file.replace(prior_file)
    logger.info("Bill subjects: adopting the local {} as this run's prior", out_file.name)
    return True


def _log_counts(counts: FetchCounts, elapsed: float) -> None:
    """Report the run the way the repo's other incremental transforms do."""
    logger.info(
        "Bill subjects: answered={:,} with_policy_area={:,} subjects_only={:,} unassigned={:,} "
        "not_held={:,} failed={:,} in {:.0f}s",
        counts.answered,
        counts.with_policy_area,
        counts.subjects_only,
        counts.unassigned,
        counts.not_held,
        counts.failed,
        elapsed,
    )
    for name, n in sorted(counts.policy_areas.items(), key=lambda kv: (-kv[1], kv[0]))[:10]:
        logger.info("Bill subjects:   {:>6,}  {}", n, name)


def _log_coverage(out_file: Path, bills_file: Path) -> None:
    """State enriched/total against the bill table, in named numbers."""
    import duckdb

    row = duckdb.sql(
        f"""
        SELECT
            (SELECT count(*) FROM read_parquet('{bills_file}')) AS bills,
            (SELECT count(*) FROM read_parquet('{out_file}')) AS rows,
            (SELECT count(*) FROM read_parquet('{out_file}') WHERE policy_area IS NOT NULL) AS with_area
        """
    ).fetchone()
    if not row:
        return
    bills, rows, with_area = row
    share = (100.0 * with_area / bills) if bills else 0.0
    logger.info(
        "Bill subjects: {:,} rows, {:,} with a policy area, against {:,} bills ({:.1f}% of the archive)",
        rows,
        with_area,
        bills,
        share,
    )


def enrich_bill_subjects(
    output_dir: Path,
    *,
    max_bills: int | None = None,
    max_folders: int = MAX_FOLDERS_PER_RUN,
    deadline_seconds: float = DEADLINE_SECONDS,
    fetcher: SubjectsFetcher | None = None,
    read_folder: FolderReader | None = None,
    clock: Callable[[], float] = time.monotonic,
    evidence: CaptureEvidence | None = None,
    download_prior=r2.download,
    receipt_build=None,
) -> Path:
    """Build ``bill_subjects.parquet`` (bounded, resumable enrichment pass).

    ``max_bills`` caps the Congress.gov bills asked this run (default
    :data:`MAX_API_BILLS_PER_RUN`) and ``max_folders`` the BILLSTATUS zips read.
    No folder read and no API request attempt (page or retry) starts once
    ``deadline_seconds`` have passed on ``clock`` since the run began. Copying
    the bill family's own rows costs no request and is not capped.
    """
    if receipt_build is not None:
        return receipt_build.run(enrich_bill_subjects, output_dir, **{
            key: value for key, value in locals().items() if key not in {"output_dir", "receipt_build"}
        })
    import duckdb

    started = clock()
    deadline = started + deadline_seconds

    def past_deadline() -> bool:
        return clock() >= deadline

    out_file = output_dir / OUTPUT
    bills_file = output_dir / BILLS_INPUT
    prior_file = output_dir / "_bill_subjects_prior.parquet"
    max_bills = MAX_API_BILLS_PER_RUN if max_bills is None else max_bills

    if not bills_file.exists():
        raise RuntimeError(f"Bill subjects: {BILLS_INPUT} not found in {output_dir} — prime it from R2 first")

    # 1. Pull the prior table (best effort — absence just means a fresh backfill).
    #    A backfill spread over many runs must never lose ground, so a local
    #    artifact from a previous run counts as a prior in its own right: without
    #    this, an R2-less re-run would silently overwrite everything enriched so
    #    far and start again from the top of the archive.
    have_prior = prior_file.exists() or download_prior(OUTPUT, prior_file) or _adopt_local_output(out_file, prior_file)
    if have_prior:
        logger.info("Bill subjects: merging against prior table {}", prior_file)
    else:
        logger.info("Bill subjects: no prior table found — starting the backfill")

    # 2. Route each pending bill by its Congress, newest first.
    recent = {current_congress(), current_congress() - 1}
    family: list[PendingBill] = []
    folders: dict[tuple[int, str], list[PendingBill]] = defaultdict(list)
    refused: list[PendingBill] = []
    api: list[PendingBill] = []
    waiting = 0
    for bill in _pending_bills(bills_file, prior_file, have_prior=have_prior):
        if bill.congress < BULK_STATUS_FLOOR:
            api.append(bill)
        elif bill.from_family:
            family.append(bill)
        elif bill.family_folder and bill.congress in recent:
            waiting += 1
        elif bill.family_folder:
            refused.append(bill)  # a settled family read skipped it: the reader refused its file
        else:
            folders[(bill.congress, bill.bill_type)].append(bill)
    read = dict(list(folders.items())[:max_folders])
    logger.info(
        "Bill subjects: {:,} family bills, {:,} folders to read ({:,} bills; {:,} folders past the cap), "
        "{:,} bills waiting on the family, {:,} refused by its settled read, {:,} bills below the {}th Congress",
        len(family),
        len(read),
        sum(map(len, read.values())),
        len(folders) - len(read),
        waiting,
        len(refused),
        len(api),
        BULK_STATUS_FLOOR,
    )

    # 3. Answer. Only an answer becomes a row; a failure leaves the bill for next run.
    now = datetime.now(UTC).isoformat(timespec="seconds")
    answers: list[tuple[str, BillSubjects | None]] = []
    unchanged = 0
    for bill in family:
        if bill.subjects_json is None:
            # A family row that states no subject list is not an answer of "none".
            answers.append((bill.bill_id, None))
            continue
        result = assignment(bill.policy_area, json.loads(bill.subjects_json), CARRIER_BULKDATA)
        row = _shape(bill.bill_id, result.policy_area, result.subjects, result.carrier, now)
        if bill.prior == (row["policy_area"], row["subjects_json"], row["subject_count"], row["carrier"]):
            unchanged += 1  # the prior row, and its enriched_at, stand
        else:
            answers.append((bill.bill_id, result))
    logger.info("Bill subjects: {:,} family bills changed, {:,} unchanged", len(family) - unchanged, unchanged)
    if read:
        with _folder_reader(read_folder, evidence) as reader:
            folder_answers, refused_in_zip = _read_folders(read, reader, recent=recent, past_deadline=past_deadline)
        answers += folder_answers
        refused += refused_in_zip
    if refused:
        logger.info("Bill subjects: {:,} bills whose BILLSTATUS file was refused go to the API", len(refused))
    api = refused + api
    recorded = [bill for bill in api if bill.bill_id in RECORDED_REFUSALS]
    if recorded:
        api = [bill for bill in api if bill.bill_id not in RECORDED_REFUSALS]
        retry = datetime.fromisoformat(now).weekday() == REFUSAL_RETRY_WEEKDAY
        logger.info(
            "Bill subjects: {:,} recorded refusals {}: {}",
            len(recorded),
            "asked after every other API bill" if retry else f"wait for weekday {REFUSAL_RETRY_WEEKDAY} (UTC)",
            ", ".join(f"{bill.bill_id} ({RECORDED_REFUSALS[bill.bill_id]})" for bill in recorded),
        )
        if retry:
            api += recorded
    api = api[:max_bills]
    if api and fetcher is None and not _resolve_api_key():
        logger.warning("Bill subjects: {:,} bills wait for an api.data.gov key", len(api))
        api = []
    if api:
        context = nullcontext(fetcher) if fetcher is not None else BillSubjectsFetcher(
            deadline=deadline, clock=clock, evidence=evidence,
        )
        with context as api_fetcher:
            for seen, bill in enumerate(api):
                if past_deadline():
                    logger.warning(
                        "Bill subjects: run deadline ({:.0f}s) reached after {:,} API bills; {:,} left for the next run",
                        deadline_seconds,
                        seen,
                        len(api) - seen,
                    )
                    break
                answers.append(
                    (bill.bill_id, api_fetcher.subjects_for(str(bill.congress), bill.bill_type, bill.bill_number))
                )
                if (seen + 1) % 250 == 0:
                    logger.info("Bill subjects: {:,}/{:,} API bills fetched...", seen + 1, len(api))
    counts = FetchCounts()
    rows = []
    for bill_id, result in answers:
        counts.record(result)
        if result is not None:
            rows.append(_shape(bill_id, result.policy_area, result.subjects, result.carrier, now))
    _log_counts(counts, clock() - started)
    if evidence is not None:
        evidence.event("bill-subject-selection", max_bills=max_bills, max_folders=max_folders,
                       deadline_seconds=deadline_seconds, answered=len(rows), waiting=waiting,
                       recorded_refusals=len(recorded),
                       folders_past_cap=len(folders) - len(read))

    # 4. Merge prior + new, dedup on bill_id preferring the fresh row.
    new_file = output_dir / "_bill_subjects_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    if have_prior:
        union = (
            f"SELECT {cols}, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY bill_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE bill_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY enriched_at DESC, bill_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    _log_coverage(out_file, bills_file)
    return out_file
