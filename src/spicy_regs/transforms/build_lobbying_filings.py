"""Transform: build ``lobbying_filings.parquet`` from the Senate LDA REST API.

Produces native domain tables and separate ETL receipts. ``lobbying_filings`` is keyed by
``filing_uuid``, with ordered native activity and entity arrays.
``lobbying_activities`` holds one row per activity a filing reports, keyed
``(filing_uuid, activity_index)``. ``lobbying_activity_lobbyists`` holds one row
per lobbyist an activity names, keyed ``(filing_uuid, activity_index,
lobbyist_index)`` (owner decision 47). An index is the position in the filing's
own list: LDA never edits a filing, since an amendment is a new filing, so the
positions are stable. The lobbyists were dropped before, so history is read
once, with the shape that keeps them.

Incremental by design (mirrors ``build_federal_register``): a full re-fetch of
the multi-million-row LDA archive every run would be wasteful *and* would trip
the R2 catastrophic-shrink guard on any short run. Instead we:

1. Best-effort download the prior ``lobbying_filings.parquet`` from R2.
2. Fetch only filings posted since its max ``dt_posted`` (minus a short overlap
   to catch late-posted / amended filings).
3. Write receipts for the fetched window, then merge the native tables on their
   keys, preferring freshly fetched rows and retaining unchanged prior receipts.

With no prior table (first run) step 2 becomes a full backfill. A legacy prior
is converted once with the fetched window; a native prior is admitted and
merged without restoring all source rows. The reader is functional keyless,
so this runs in CI with or without ``LDA_API_KEY``.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import os
import shutil
from collections.abc import Iterable, Iterator, Mapping
from contextlib import ExitStack
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.parquet_rows import str_or_none
from spicy_regs.sources import r2
from spicy_regs.transforms.government_receipts import (
    BUILD_METADATA, _digest, has_native_prior, internal_prior, receipt_builder, selected_native_prior,
)
from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS
from spicy_regs.transforms.table_merge import merge_local_prior

if TYPE_CHECKING:
    from spicy_docs.sources.lda import LdaFilingsReader

    from spicy_regs.source_evidence import CaptureEvidence

OUTPUT = "lobbying_filings.parquet"

# Re-scan this many days before the last stored dt_posted on each run, so
# filings posted/amended after the watermark are picked up.
OVERLAP_DAYS = 7
MAX_WINDOW_DAYS = 30

#: Requests per page: one attempt plus the six retries the deleted local
#: reader made per request; keyless requests are rate-limited more
#: aggressively, so 429s are expected and retried within this per-page budget
#: (spicy-docs' retry policy).
MAX_REQUESTS_PER_PAGE = 7

#: Pacing between page requests. Keyless, lda.gov served 15 pages back to back and then
#: answered "Request was throttled. Expected available in 44 seconds." until the per-page
#: retries ran out (2026-09-23), so an unpaced keyless walk cannot finish; 15 a minute is
#: what it measurably allows. A key raises the limit.
KEYLESS_INTERVAL_SECONDS = 4.0
KEYED_INTERVAL_SECONDS = 0.5

#: The whole archive at page size 25 is ~79,082 pages (spicy-docs' SR07
#: measurement, 2026-09-21); this bound preserves a cold-start full backfill
#: with headroom while the walk still refuses past it rather than ending
#: silently.
MAX_PAGES = 100_000


class LobbyingFilingsError(ValueError):
    """The requested LDA scope could not be read completely."""


def _bounded_until(since: date | None, until: date | None, *, today: date | None = None) -> date | None:
    """Return a deterministic end date no more than one catch-up window ahead."""
    if since is None:
        return until
    if until is not None and until < since:
        raise ValueError(f"LDA until date {until} precedes since date {since}")
    return min(until or today or date.today(), since + timedelta(days=MAX_WINDOW_DAYS))


def _filings_url(*, since: date | None, until: date | None, filing_year: int | None) -> str:
    """The one filings query: a year or a posted-date window, oldest first.

    Pagination requires a data filter (page/ordering do not count), so a cold
    start with no filter holds at today's existing upper-bound meaning without
    guessing the first archive year or dropping historical filings — the same
    filtered-request semantics the deleted local reader carried.
    """
    from spicy_docs.sources.lda import filings_url

    if filing_year is None and since is None and until is None:
        until = date.today()
    return filings_url(
        filing_year=filing_year,
        posted_after=since.isoformat() if since is not None else None,
        posted_before=until.isoformat() if until is not None else None,
        ordering="dt_posted",
    )


#: The processing-only key carrying when the page holding a filing was captured; no table column reads it except a
#: profile's ``captured_at``.
CAPTURED_AT = "_captured_at"


def _identified(record: Mapping[str, Any], *, captured_at: str | datetime) -> dict:
    """A copy of one raw filing, refused unless it carries a nonempty ``filing_uuid``, the merge identity.

    The copy carries its page's capture time (UTC) under ``CAPTURED_AT``.
    """
    filing = dict(record)
    identity = filing.get("filing_uuid")
    if not isinstance(identity, str) or not identity.strip():
        raise LobbyingFilingsError("LDA filings page has a row without a nonempty filing_uuid")
    moment = datetime.fromisoformat(captured_at) if isinstance(captured_at, str) else captured_at
    filing[CAPTURED_AT] = moment.astimezone(timezone.utc)
    return filing


def _iter_filings(reader: LdaFilingsReader, url: str, *, max_records: int | None,
                  max_passes: int | None = None) -> Iterator[dict]:
    """Yield raw filing dicts from spicy-docs' page walk: the whole query settled, or a ``max_records`` prefix.

    Every row must carry a nonempty ``filing_uuid`` — the merge identity — so a
    malformed row refuses instead of landing a NULL key in the published table.
    The owner's walk already refuses a changed declared count, an observed
    total disagreeing with it, or a page bound reached before the terminal
    page; a ``max_records`` stop is an explicit prefix of that walk and claims
    nothing about the rest.

    Agreeing with the declared count is not completeness. Offset paging over
    filings tied on ``dt_posted`` repeats one filing and skips another while
    serving exactly the declared total: the retained 1999 walk served its
    declared 31,498 records but 29,399 distinct filings, and lda.gov draws the
    order inside a tie afresh per request, so repeat walks skip different
    filings (ss-hqoi probe, 2026-10-30). A whole walk therefore goes through
    spicy-docs' ``pool_walks`` keyed by ``filing_uuid``: a clean walk (every
    walk since 2007 retained so far) settles in one pass, a tied one is walked
    again and pooled until it holds the declared total, and one still short
    after ``max_passes`` (default ``DEFAULT_POOL_PASSES``) raises
    ``IncompleteWalkError`` rather than publishing a short year. A 1999-2006
    year, whose filings are almost all posted on day-precision ties, lost 1.9%
    (2000) to 6.7% (1999) of its filings in one walk. The owner capped the
    1999-2006 re-walk at four passes (2026-10-31, ss-hqoi), the default; a year
    still short after four refuses and is reported, not published short. An
    unset ``LDA_POOL_PASSES`` means the default; zero or less is refused. One
    walk's filings are held in memory, as the whole-window read always was.
    """
    from dataclasses import replace

    from spicy_docs.reading.paged_json import DEFAULT_POOL_PASSES, WalkPass, pool_walks

    def pages():
        for page in reader.filings(url, max_pages=MAX_PAGES):
            observed = page.capture.observed_at
            yield replace(page, records=tuple(_identified(record, captured_at=observed) for record in page.records))

    if max_records is not None:
        prefix = (filing for page in pages() for filing in page.records)
        yield from itertools.islice(prefix, max_records)
        return
    pooled = pool_walks(lambda _: WalkPass.from_pages(pages(), label="LDA filings"),
                        key=lambda filing: filing["filing_uuid"], max_passes=DEFAULT_POOL_PASSES if max_passes is None else max_passes,
                        label="LDA filings")
    if pooled.passes > 1:
        logger.warning("LDA: {} walks of {} pooled to its declared {:,} filings", pooled.passes, url, pooled.declared)
    yield from cast("tuple[dict, ...]", pooled.records)  # the records _identified copied


# Literal processing columns, owned by the family's source shapes (one list, not a second hand-kept copy). The
# receipt writer converts the final output to the native subject schema.
COLUMNS = LEGACY_COLUMNS["lobbying_filings"]
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])
ACTIVITIES_OUTPUT = "lobbying_activities.parquet"
ACTIVITY_COLUMNS = LEGACY_COLUMNS["lobbying_activities"]
LOBBYISTS_OUTPUT = "lobbying_activity_lobbyists.parquet"
LOBBYIST_COLUMNS = LEGACY_COLUMNS["lobbying_activity_lobbyists"]
REGISTRANTS_OUTPUT = "lobbying_registrants.parquet"
REGISTRANT_COLUMNS = LEGACY_COLUMNS["lobbying_registrants"]
CLIENTS_OUTPUT = "lobbying_clients.parquet"
CLIENT_COLUMNS = LEGACY_COLUMNS["lobbying_clients"]
#: Shaped rows held per table before a Parquet write: a whole history year or the full retained archive streams.
_FLUSH_ROWS = 20_000
NATIVE_ORDERS = {
    "lobbying_filings": ("dt_posted DESC", "filing_uuid"),
    "lobbying_activities": ("filing_uuid", "activity_index"),
    "lobbying_activity_lobbyists": ("filing_uuid", "activity_index", "lobbyist_index"),
    "lobbying_registrants": ("registrant_id",),
    "lobbying_clients": ("client_id",),
}


def _source_list(value: object, field: str) -> list | None:
    """Keep null, empty and positional entries; refuse unsupported source shapes."""
    if value is None:
        return None
    if not isinstance(value, list) or any(item is not None and not isinstance(item, dict) for item in value):
        raise LobbyingFilingsError(f"{field} must be an array of objects or nulls")
    return value


def _entities(activity: dict) -> list | None:
    values = _source_list(activity.get("government_entities"), "government_entities")
    return None if values is None else [None if value is None else {"id": value.get("id"), "name": value.get("name")} for value in values]


def _activities(filing: dict) -> list | None:
    """Keep activity descriptions at their source positions, including nulls."""
    values = _source_list(filing.get("lobbying_activities"), "lobbying_activities")
    return None if values is None else [None if act is None else {
        "general_issue_code": act.get("general_issue_code"),
        "general_issue_code_display": act.get("general_issue_code_display"),
        "description": act.get("description"),
    } for act in values]


def _government_entities(filing: dict) -> list | None:
    """Known entity occurrences in activity order; child rows retain per-activity null lists."""
    activities = _source_list(filing.get("lobbying_activities"), "lobbying_activities")
    if activities is None:
        return None
    return [entity for act in activities if act is not None for entity in (_entities(act) or [])]


def _shape(filing: dict) -> dict:
    """Map one raw LDA filing onto the published column shape."""
    registrant = filing.get("registrant") or {}
    client = filing.get("client") or {}
    return {
        "filing_uuid": filing.get("filing_uuid"),
        "filing_type": filing.get("filing_type"),
        "filing_year": str_or_none(filing.get("filing_year")),
        "filing_period": filing.get("filing_period"),
        "dt_posted": filing.get("dt_posted"),
        "registrant_name": registrant.get("name") if isinstance(registrant, dict) else None,
        "registrant_id": str_or_none(registrant.get("id")) if isinstance(registrant, dict) else None,
        "client_name": client.get("name") if isinstance(client, dict) else None,
        # LDA's global ``client.id``, the lobbying_clients key. The source's ``client.client_id`` is the registrant's
        # own number for the client (lobbying_clients.registrant_client_number) and must not stand in for it.
        "client_id": str_or_none(client.get("id")) if isinstance(client, dict) else None,
        "income": filing.get("income"),
        "expenses": filing.get("expenses"),
        "lobbying_activities_json": json.dumps(_activities(filing)),
        "government_entities_json": json.dumps(_government_entities(filing)),
        "url": filing.get("filing_document_url"),
        "filing_type_display": filing.get("filing_type_display"),
        "filing_period_display": filing.get("filing_period_display"),
        "filing_document_content_type": filing.get("filing_document_content_type"),
        "expenses_method": filing.get("expenses_method"),
        "expenses_method_display": filing.get("expenses_method_display"),
        "termination_date": filing.get("termination_date"),
        "posted_by_name": filing.get("posted_by_name"),
        "registrant_address_1": filing.get("registrant_address_1"),
        "registrant_address_2": filing.get("registrant_address_2"),
        "registrant_city": filing.get("registrant_city"),
        "registrant_state": filing.get("registrant_state"),
        "registrant_zip": filing.get("registrant_zip"),
        "registrant_country": filing.get("registrant_country"),
        "registrant_ppb_country": filing.get("registrant_ppb_country"),
        "registrant_different_address": str_or_none(filing.get("registrant_different_address")),
    }


def _activity_rows(filing: dict) -> list[dict]:
    """One row per activity the filing reports, at its position in the filing's own list."""
    rows = []
    for index, act in enumerate(_source_list(filing.get("lobbying_activities"), "lobbying_activities") or []):
        act = act or {}
        entities = _entities(act)
        rows.append({
            "filing_uuid": filing.get("filing_uuid"),
            "activity_index": str(index),
            "general_issue_code": act.get("general_issue_code"),
            "general_issue_code_display": act.get("general_issue_code_display"),
            "description": act.get("description"),
            "foreign_entity_issues": act.get("foreign_entity_issues"),
            "government_entities_json": json.dumps(entities),
        })
    return rows


def _lobbyist_rows(filing: dict) -> list[dict]:
    """One row per lobbyist an activity names, at its positions in the filing's lists."""
    rows = []
    for index, act in enumerate(_source_list(filing.get("lobbying_activities"), "lobbying_activities") or []):
        act = act or {}
        for position, entry in enumerate(_source_list(act.get("lobbyists"), "lobbyists") or []):
            entry = entry or {}
            person = entry.get("lobbyist")
            if not isinstance(person, dict):
                person = {}
            rows.append({
                "filing_uuid": filing.get("filing_uuid"),
                "activity_index": str(index),
                "lobbyist_index": str(position),
                "lobbyist_id": str_or_none(person.get("id")),
                "prefix": person.get("prefix"),
                "first_name": person.get("first_name"),
                "nickname": person.get("nickname"),
                "middle_name": person.get("middle_name"),
                "last_name": person.get("last_name"),
                "suffix": person.get("suffix"),
                "covered_position": entry.get("covered_position"),
                "new": str_or_none(entry.get("new")),
            })
    return rows


def _prior_max_dt_posted(prior_file: Path) -> date | None:
    """Largest ``dt_posted`` date in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(dt_posted) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def _stage_prior(output_dir: Path, key: str, target: Path) -> bool:
    """Reuse the last completed local build before an older staged or remote prior.

    ``internal_prior`` checks the selected subject and receipt hashes after this
    copy. The staged pair stays intact while the producer writes the next window.
    """
    local = output_dir / key
    metadata_path = output_dir / BUILD_METADATA
    metadata = json.loads(metadata_path.read_text()) if metadata_path.exists() else {}
    expected = metadata.get("subjects", {}).get(key.removesuffix(".parquet"), {}).get("sha256")
    if expected is not None and local.exists() and _digest(local) == expected:
        shutil.copyfile(local, target)
        return True
    # A failed producer may have replaced the public path with its raw window;
    # keep the prior staged before that attempt until a completed build pins it.
    return target.exists() or r2.download(key, target)


def _embedded(filing: dict, field: str) -> dict | None:
    value = filing.get(field)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise LobbyingFilingsError(f"{field} must be an object")
    return value if value.get("id") is not None else None


def _registrant_row(filing: dict) -> dict | None:
    """The registrant record the filing embeds, as captured with it; ``contact_*`` stay out pending the owner."""
    registrant = _embedded(filing, "registrant")
    if registrant is None:
        return None
    # Every other column is the record's own key, under its source name.
    return {
        **{key: registrant.get(key) for key in REGISTRANT_COLUMNS},
        "registrant_id": str_or_none(registrant["id"]),
        "house_registrant_id": str_or_none(registrant.get("house_registrant_id")),
        "captured_at": filing[CAPTURED_AT].isoformat(),
        "captured_filing_uuid": filing["filing_uuid"],
    }


def _client_row(filing: dict) -> dict | None:
    """The client record the filing embeds, as captured with it, beside the filing's registrant."""
    client = _embedded(filing, "client")
    if client is None:
        return None
    registrant = _embedded(filing, "registrant")
    # Every other column is the record's own key, under its source name. The source's ``client_id`` is the
    # registrant's own number for the client; LDA's global ``id`` is the table's ``client_id``, as on filings.
    return {
        **{key: client.get(key) for key in CLIENT_COLUMNS},
        "client_id": str_or_none(client["id"]),
        "registrant_client_number": str_or_none(client.get("client_id")),
        "registrant_id": None if registrant is None else str_or_none(registrant["id"]),
        "client_government_entity": str_or_none(client.get("client_government_entity")),
        "client_self_select": str_or_none(client.get("client_self_select")),
        "captured_at": filing[CAPTURED_AT].isoformat(),
        "captured_filing_uuid": filing["filing_uuid"],
    }


_NEVER = datetime.min.replace(tzinfo=timezone.utc)


def _profile_rank(row: dict) -> tuple[datetime, datetime]:
    """The publisher's later ``dt_updated`` first (clients carry none), then the later capture.

    The publisher's own version outranks capture time because 1,190 of the 80,523 retained pages carry only a
    capture day (ss-hqoi manifest bases). A nightly or year window needs no rank against its prior: LDA embeds its current record in
    every filing it serves, so whatever a window fetches is the newest state and the native merge keeps it.
    """
    updated = row.get("dt_updated")
    return (datetime.fromisoformat(updated) if updated else _NEVER, datetime.fromisoformat(row["captured_at"]))


_TABLES = (
    (OUTPUT, COLUMNS, lambda filing: [_shape(filing)]),
    (ACTIVITIES_OUTPUT, ACTIVITY_COLUMNS, _activity_rows),
    (LOBBYISTS_OUTPUT, LOBBYIST_COLUMNS, _lobbyist_rows),
)
#: One row per LDA id, the latest observation of it in this read (``_profile_rank``; an equal rank keeps the first
#: seen). At most one row per registrant and client is held: 14,881 and 129,462 over the whole archive.
_PROFILES = (
    (REGISTRANTS_OUTPUT, REGISTRANT_COLUMNS, _registrant_row),
    (CLIENTS_OUTPUT, CLIENT_COLUMNS, _client_row),
)


def _write_fresh(filings: Iterable[dict], output_dir: Path) -> dict[str, Path]:
    """Shape ``filings`` into one source-shaped ``_<table>_new.parquet`` per table, a bounded batch at a time.

    A filing's activity and lobbyist rows are shaped from the same record as the filing row, so children always come
    from the capture their filing came from.
    """
    paths = {key: output_dir / f"_{key.removesuffix('.parquet')}_new.parquet" for key, _, _ in (*_TABLES, *_PROFILES)}
    schemas = {key: pa.schema([(c, pa.string()) for c in columns]) for key, columns, _ in (*_TABLES, *_PROFILES)}
    pending: dict[str, list[dict]] = {key: [] for key, _, _ in _TABLES}
    latest: dict[str, dict[str, dict]] = {key: {} for key, _, _ in _PROFILES}
    counts = dict.fromkeys(paths, 0)
    try:
        with ExitStack() as stack:
            writers = {key: stack.enter_context(pq.ParquetWriter(paths[key], schemas[key], compression="zstd"))
                       for key in paths}

            def flush(key: str) -> None:
                writers[key].write_table(pa.Table.from_pylist(pending[key], schema=schemas[key]))
                counts[key] += len(pending[key])
                pending[key].clear()

            for filing in filings:
                for key, _, rows in _TABLES:
                    pending[key].extend(rows(filing))
                    if len(pending[key]) >= _FLUSH_ROWS:
                        flush(key)
                for key, columns, profile in _PROFILES:
                    row = profile(filing)
                    if row is not None:
                        held = latest[key].get(row[columns[0]])
                        if held is None or _profile_rank(row) > _profile_rank(held):
                            latest[key][row[columns[0]]] = row
            for key, _, _ in _PROFILES:
                pending[key] = list(latest.pop(key).values())
            for key in paths:
                if pending[key]:
                    flush(key)
    except BaseException:
        # A refused walk leaves no partial window a later step could mistake for a complete fetch.
        for path in paths.values():
            path.unlink(missing_ok=True)
        raise
    logger.info("LDA: shaped {:,} filings ({:,} activities, {:,} lobbyist rows, {:,} registrants, {:,} clients)",
                *(counts[key] for key in paths))
    return paths


def _merge_fresh(output_dir: Path, fresh: dict[str, Path], *, priors: dict[str, tuple[Path, bool]] | None,
                 memory_limit: str) -> tuple[Path, ...]:
    """Deduplicate and order each fresh table; a legacy prior joins here, a native one in the receipt builder.

    ``priors`` None is a full replacement: no table stages or selects a prior.
    """
    import duckdb

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")
    shapes: dict[str, tuple[str | tuple[str, ...], str]] = {
        OUTPUT: ("filing_uuid", "dt_posted DESC, filing_uuid"),
        ACTIVITIES_OUTPUT: (("filing_uuid", "activity_index"), "filing_uuid, CAST(activity_index AS INTEGER)"),
        LOBBYISTS_OUTPUT: (("filing_uuid", "activity_index", "lobbyist_index"),
                           "filing_uuid, CAST(activity_index AS INTEGER), CAST(lobbyist_index AS INTEGER)"),
        REGISTRANTS_OUTPUT: ("registrant_id", "CAST(registrant_id AS BIGINT)"),
        CLIENTS_OUTPUT: ("client_id", "CAST(client_id AS BIGINT)"),
    }
    outputs = []
    try:
        for key, columns, _ in (*_TABLES, *_PROFILES):
            identity, order_by = shapes[key]
            dataset = key.removesuffix(".parquet")
            table_prior, table_has_prior = output_dir / f"_{dataset}_prior.parquet", False
            if priors is not None and key in priors:
                table_prior, table_has_prior = priors[key]
            elif priors is not None:
                selected = selected_native_prior(dataset)
                table_has_prior = selected is not None or _stage_prior(output_dir, key, table_prior)
                table_prior = selected or table_prior
                if table_has_prior:
                    table_prior = internal_prior(dataset, table_prior, keep_native=True)
            new_file = fresh[key]
            out = output_dir / key
            # A selected native pair stays immutable until the receipt builder upserts
            # these fetched rows. Only a legacy first run needs the full source merge.
            native_prior = has_native_prior(dataset)
            merge_local_prior(con, columns=columns, identity=identity, order_by=order_by,
                              prior_file=table_prior if table_has_prior and not native_prior else None,
                              new_file=new_file, out_file=out)
            # Housekeeping: drop scratch files so they aren't mistaken for outputs.
            new_file.unlink(missing_ok=True)
            if table_has_prior and not native_prior:
                table_prior.unlink(missing_ok=True)
            outputs.append(out)
            logger.info("{}: {:,} rows", key, pq.ParquetFile(out).metadata.num_rows)
    finally:
        con.close()
    return tuple(outputs)


@receipt_builder(native_orders=NATIVE_ORDERS)
def build_lobbying_filings(
    output_dir: Path,
    *,
    evidence: CaptureEvidence | None = None,
    since: date | None = None,
    until: date | None = None,
    filing_year: int | None = None,
    max_records: int | None = None,
    max_passes: int | None = None,
) -> tuple[Path, ...]:
    """Build the filings, activities, lobbyists, registrants and clients tables, each merged with its prior table."""
    prior_file = output_dir / "_lda_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    selected = selected_native_prior("lobbying_filings")
    have_prior = selected is not None or _stage_prior(output_dir, OUTPUT, prior_file)
    prior_file = selected or prior_file
    if have_prior:
        prior_file = internal_prior("lobbying_filings", prior_file, keep_native=True)
    if have_prior:
        logger.info("LDA: merging against prior table {}", prior_file)
    else:
        logger.info("LDA: no prior table found — full backfill")

    # 2. Decide the fetch window start. A filing year is read whole: the
    # posted-date watermark would narrow a history year to its last days.
    if since is None and filing_year is None:
        prior_max = _prior_max_dt_posted(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else None
    # A stale watermark must not create an ever-growing request that repeatedly
    # hits the 30-minute CI timeout. Walk toward today in bounded windows; the
    # overlap makes each successive merge resilient to late/amended filings.
    until = _bounded_until(since, until)
    logger.info("LDA: fetching filings posted {} through {}", since or "the beginning", until or "today")

    # 3. Fetch + shape into "new rows" parquets. spicy-docs' LDA reader owns
    # the requests, retries and walk refusals; an optional LDA_API_KEY raises
    # the keyless rate limit.
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.sources.lda import LdaFilingsReader

    url = _filings_url(since=since, until=until, filing_year=filing_year)
    api_key = os.environ.get("LDA_API_KEY", "").strip() or None
    budget = PagedJsonBudget(
        max_requests=MAX_REQUESTS_PER_PAGE,
        max_page_bytes=16 * 1024 * 1024,
        timeout_seconds=60.0,
        min_request_interval_seconds=KEYED_INTERVAL_SECONDS if api_key else KEYLESS_INTERVAL_SECONDS,
    )
    transport = None
    if evidence is not None:
        evidence.credential = api_key or ""
        evidence.event("selection", stage="lobbying", url=url, max_records=max_records)
        transport = evidence.transport(stage="lobbying-response", max_bytes=budget.max_page_bytes)
    with LdaFilingsReader(budget=budget, api_key=api_key, transport=transport) as reader:
        fresh = _write_fresh(_iter_filings(reader, url, max_records=max_records, max_passes=max_passes), output_dir)

    # 4. Dedup the fetched rows. Only a legacy prior joins here; the receipt
    # builder merges a native prior after converting this window.
    # The prior grows with every backfilled year and the dedup window reads all of it: at 4GB the 2013 run
    # (36356691399, 36362663551) ran out of memory in merge_local_prior. Hosted runners have 16 GB; leave room
    # for Python and the OS, and let DuckDB spill the rest to temp_directory.
    return _merge_fresh(output_dir, fresh, priors={OUTPUT: (prior_file, have_prior)}, memory_limit="12GB")


def read_retained_manifest(manifest: Path) -> list[tuple[datetime, str, int]]:
    """The pinned retained-page manifest as ``(captured_at, sha256, bytes)``, newest capture first.

    A tab-separated file with a header naming at least ``sha256``, ``bytes`` and ``captured_at`` (an ISO-8601 time
    with its offset). Which pages belong to the family and when each was captured is decided by
    ``scripts/lobbying_retained_manifest.py`` and pinned by digest; this reader only refuses a manifest it cannot order exactly.
    """
    import csv

    pages = []
    with manifest.open(newline="") as stream:
        for row in csv.DictReader(stream, delimiter="\t"):
            captured = datetime.fromisoformat(row["captured_at"])
            if captured.tzinfo is None:
                raise LobbyingFilingsError(f"Retained page {row['sha256']} has a capture time without an offset")
            pages.append((captured, row["sha256"].removeprefix("sha256:"), int(row["bytes"])))
    if not pages:
        raise LobbyingFilingsError("Retained-page manifest lists no pages")
    if len({sha for _, sha, _ in pages}) != len(pages):
        raise LobbyingFilingsError("Retained-page manifest repeats a page")
    # Newest first; equal capture times fall to the larger digest, an arbitrary but repeatable order.
    return sorted(pages, reverse=True)


#: Retained pages read at once. Each read is mostly the archive's descriptor-relative opens (about 15 per page,
#: 19 of 39 s for 2,013 pages read one at a time, 2026-10-30); a bounded window overlaps them and holds at most this
#: many pages (about 110 KB each) in memory.
_READ_AHEAD = 32


def _read_retained_page(store, sha256: str, size: int) -> list:
    """One retained page's records, its original bytes hashed as read and refused on any size or digest difference.

    The bytes decode as the live walk decodes a response (``load_decimal_json``), so a rebuilt row reads exactly
    what the page served.
    """
    from spicy_docs.reading.json_input import load_decimal_json
    from spicy_docs.sources.lda import FILINGS_KEY

    hasher, chunks, read = hashlib.sha256(), [], 0
    with store.open(sha256) as stream:
        while chunk := stream.read(1 << 20):
            hasher.update(chunk)
            chunks.append(chunk)
            read += len(chunk)
    if read != size or hasher.hexdigest() != sha256:
        raise LobbyingFilingsError(f"Retained page {sha256} differs from its pinned bytes")
    page: Any = load_decimal_json(b"".join(chunks), source=f"LDA retained page {sha256}",
                                  error_type=LobbyingFilingsError)
    records = page.get(FILINGS_KEY) if isinstance(page, dict) else None
    if not isinstance(records, list):
        raise LobbyingFilingsError(f"Retained page {sha256} has no {FILINGS_KEY} list")
    return records


def _iter_retained_filings(pages: list[tuple[datetime, str, int]], store) -> Iterator[dict]:
    """Yield each filing once, from its latest retained capture: pages newest first, the first ``filing_uuid`` seen wins.

    Pages are read ``_READ_AHEAD`` at a time and consumed in manifest order, so which capture wins never depends on
    which read finished first. The one read of each page is also its verification.
    """
    from concurrent.futures import ThreadPoolExecutor

    seen: set[str] = set()
    # Filings first seen at the current capture time: a later page at the same time that repeats one is a tie the
    # manifest's digest order broke, counted rather than assumed absent (1,168 pages share one day-only time).
    tied_run: tuple[datetime | None, set[str]] = (None, set())
    ties = 0
    with ThreadPoolExecutor(max_workers=_READ_AHEAD) as pool:
        for window in itertools.batched(pages, _READ_AHEAD):
            batch = pool.map(lambda page: _read_retained_page(store, page[1], page[2]), window)
            for (captured_at, _, _), records in zip(window, batch, strict=True):
                if tied_run[0] != captured_at:
                    tied_run = (captured_at, set())
                for record in records:
                    filing = _identified(record, captured_at=captured_at)
                    if filing["filing_uuid"] not in seen:
                        seen.add(filing["filing_uuid"])
                        tied_run[1].add(filing["filing_uuid"])
                        yield filing
                    elif filing["filing_uuid"] in tied_run[1]:
                        ties += 1
    logger.info("LDA retained: {:,} filings from {:,} pages; {:,} repeat occurrences tied on capture time with the "
                "kept one, which the digest order chose", len(seen), len(pages), ties)


# No native_orders: a rebuild selects no prior to merge onto, with or without R2_PUBLIC_URL.
@receipt_builder
def rebuild_lobbying_filings(
    output_dir: Path,
    *,
    manifest: Path,
    manifest_sha256: str,
    evidence: CaptureEvidence | None = None,
    store=None,
    memory_limit: str = "4GB",
) -> tuple[Path, ...]:
    """Rebuild the whole family from retained LDA pages: the latest capture of each filing, no prior, no request.

    The first generation under a new policy is this full replacement (``POLICY_VERSIONS``): every filing, its
    activities and its lobbyists come from one capture, the newest the archive holds. ``memory_limit`` bounds the
    dedup connection; the default keeps a local run under 8 GB.
    """
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    if _digest(manifest) != "sha256:" + manifest_sha256.removeprefix("sha256:"):
        raise LobbyingFilingsError("Retained-page manifest differs from its pinned digest")
    pages = read_retained_manifest(manifest)
    if evidence is not None:
        evidence.retain_file(manifest, stage="lobbying-retained-manifest")
        evidence.event("selection", stage="lobbying", mode="retained", manifest_sha256=_digest(manifest),
                       pages=len(pages))
    store = store or CentralRawArchiveStore(create=False)
    fresh = _write_fresh(_iter_retained_filings(pages, store), output_dir)
    return _merge_fresh(output_dir, fresh, priors=None, memory_limit=memory_limit)
