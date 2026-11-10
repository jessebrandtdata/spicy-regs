"""Transform: build ``federal_register.parquet`` from the FR REST API.

Produces SpicyDocs' public projection (``FEDERAL_REGISTER_COLUMNS``, all
VARCHAR, through ``project_federal_register_document``) plus the derived
``rin``: one owner for the column list and the value mapping, not a local
copy (consolidation plan B2). ``topics_json`` is the publisher's ``topics``;
a row published before that column existed reads NULL until it is fetched
again, and a full re-read (``since`` at the epoch) refills every row.

Incremental by design, in two steps (ss-vse3). ``build_federal_register``
reads the documents published since the prior table's newest
``publication_date`` minus ``OVERLAP_DAYS`` (the FR epoch with no prior) and
keeps one row per ``(document_number, publication_date)``: a document read
twice is one observation, and conflicting observations within one run refuse
publication rather than choose a row. ``merge_federal_register`` maps them
(``regulations_shape``) into the published table the captured index pins: a
fresh row replaces the row of its dated key, so corrections republished under
the same number on another date stay distinct rows, and every other row is
carried as published. A carried row keeps the mapping of the run that last read
it, except ``rin``, which the merge derives on every row; any other mapping
change reaches old rows only through a full re-read (``FR_SINCE=1994``). A
document the mapping refuses fails the run: there is no receipt to refuse it
into.

``rin`` is the one column not fetched: the first Regulation Identifier Number
in ``regulation_id_numbers_json``, retained as a compatibility projection — an
empty array and NULL both give NULL — while a consumer wanting every RIN of a
multi-RIN document unnests the array (see docs/regulatory-rins.md).
``modify_date`` is not exposed by the REST API, so fresh rows carry NULL for it
while carried rows keep whatever the prior table had.

``source_sha256`` is the row's pointer to its evidence: the SHA-256 of the
retained API page it was last read from, spelled as the run's journal spells
it (``sha256:<hex>``; the bytes are ``source-evidence/blobs/sha256/<hex>``).
A row no run has read since the column was added holds NULL.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2

OUTPUT = "federal_register.parquet"

#: The column naming the retained API page a row was last read from; appended after the contract's columns.
POINTER = "source_sha256"

#: Rows per row group of the published table, as the receipt writer left it (507 groups for 1,012,333 rows on
#: 2026-11-09): readers fetch by range, so the layout is part of what is published.
ROW_GROUP_ROWS = 2000

# The oldest documents the API serves. A backfill with no prior table starts here.
FR_EPOCH = date(1994, 1, 1)

# Re-scan this many days before the last stored publication_date on each run, so documents added or corrected after
# their nominal publication date are picked up, and so regulations_dot_gov_comments_count is the Register's latest
# relay rather than its publication-week one: the Register re-checks regulations.gov until about the comment period's
# close. Measured 2026-10-03 (round6/impl-W/m2/overlap_days.out): since 2016 a period closes a median 32 days after
# publication (90th percentile 62, 99th about 91; 487 of 96,442 later than 120); of the March 2024 documents the
# Register relayed a count for, 64 of 325 were last checked within 7 days of publication and 269 within 120. About
# 12 more keyless requests a day.
OVERLAP_DAYS = 120


def build_federal_register_layer1(manifest: Path, archive_root: Path, output_dir: Path, *, max_records: int = 10000) -> Path:
    """Build a bounded typed source slice offline; never reads or publishes R2.

    The manifest selects retained pages by URL and digest. The maintained
    SpicyDocs traversal validates their field request, dates and inventory.
    This explicit candidate route is separate from the scheduled public table.
    """
    from spicy_regs.transforms.federal_register_layer1 import build

    return build(manifest, archive_root, output_dir, max_records=max_records)


def layer1_main() -> None:
    """Installed offline entry point for a retained Federal Register slice."""
    import argparse

    parser = argparse.ArgumentParser(description="Build a typed Federal Register slice from retained originals only")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--archive-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--max-records", type=int, default=10000)
    args = parser.parse_args()
    build_federal_register_layer1(args.manifest, args.archive_root, args.output_dir, max_records=args.max_records)


def published_columns() -> tuple[str, ...]:
    """The built width: SpicyDocs' ``federal_register`` contract (the projection plus the derived ``rin``), then
    this builder's page pointer.

    A function, not a constant, because base installs import this module
    without the source-readers extra that carries SpicyDocs.
    """
    from spicy_docs.schemas.federal_register import FEDERAL_REGISTER

    return (*FEDERAL_REGISTER.columns, POINTER)

#: ``rin`` as a projection of the array, in each shape the table takes: the night's text rows and the published
#: table's native list, where the merge derives it for every row. ``[]`` and NULL both give NULL: a join key is
#: present or it is not.
_RIN_SQL = "json_extract_string(regulation_id_numbers_json, '$[0]')"
_PUBLISHED_RIN_SQL = "regulation_id_numbers[1]"


def _prior_max_publication_date(prior_file: Path) -> date | None:
    """Largest ``publication_date`` in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(publication_date) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


@contextmanager
def _federal_register_evidence(output_dir: Path, evidence):
    """Standalone acquisition owns its journal; scheduled builds use their run's owner."""
    from spicy_regs.source_evidence import CaptureEvidence

    owned = evidence is None
    evidence = evidence if evidence is not None else CaptureEvidence(output_dir, "federal-register")
    try:
        yield evidence
    except BaseException as error:
        if owned:
            evidence.finish(error)
        raise
    else:
        if owned:
            evidence.finish()


def _live_federal_register_documents(since: date, *, output_dir: Path, evidence=None) -> Iterator[tuple[str, dict]]:
    """Yield ``(page SHA-256, raw FR document)`` for documents published in ``[since, today]`` through the SpicyDocs owner.

    The owner acquires exact API pages with its cap-safe date-window traversal:
    a window whose ``count`` reaches the 10,000-result cap is split, and a
    single day that still reads capped refuses the run rather than publish a
    hole. Pages the owner declares included carry the raw API results unchanged;
    capped probe pages carry none (their halves do), so those are skipped here.

    One traversal: the merge below deduplicates repeated observations and
    refuses conflicting ones itself, so the owner's two-traversal
    reconciliation would double the request budget for a contract this
    transform already checks.
    """
    try:
        from spicy_docs.sources.federal_register import native as federal_register
        from spicy_docs.transport.acquisition import federal_register_fetcher
    except ModuleNotFoundError as error:
        if error.name == "spicy_docs":
            raise RuntimeError(
                "The Federal Register acquisition requires spicy-regs[source-readers]. "
                "Run `uv sync --frozen` in a SpicyRegs checkout."
            ) from None
        raise
    scope = {
        "publishedFrom": since.isoformat(),
        "publishedThrough": date.today().isoformat(),
    }
    with _federal_register_evidence(output_dir, evidence) as evidence, federal_register_fetcher(None) as fetch:
        def retained_fetch(url):
            body = fetch(url)
            # The fetcher exposes only request URL and bytes. Preserve the exact
            # response before parsing; final URL and HTTP capture time stay unknown.
            evidence.retain_bytes(body, raw_source=True, stage="api-page", source_url=url, requested_url=url,
                                  covered_period={"requested_from": scope["publishedFrom"],
                                                  "requested_through": scope["publishedThrough"]})
            return body

        window = None
        pages = []
        for page in federal_register.iter_federal_register_pages(retained_fetch, query_scope=scope, traversals=1):
            response = federal_register.parse_page_response(page.response_bytes)
            digest = "sha256:" + hashlib.sha256(page.response_bytes).hexdigest()
            evidence.declare_used_inputs(
                [{"location": page.request_key, "sha256": digest,
                  "bytes": len(page.response_bytes), "asset_kind": "json-body"}],
                scope_id=f"{evidence.family}:successful-source-files", complete=False,
            )
            # A later page's key is the source's next_page_url, which is not a window request and does not parse as one.
            if page.window_page_index == 0:
                window = federal_register.federal_register_request_window(page.request_key)
            included = federal_register.federal_register_records_included(response, query_scope=scope,
                                                                          page_window=window)
            # Each page by the digest its rows point at (source_sha256); a capped probe's records are its halves'.
            pages.append({"sha256": digest, "records": len(response["results"]) if included else 0})
            if not included:
                continue
            for document in response["results"]:
                yield digest, document
        evidence.event("federal-register-pages", requested_from=scope["publishedFrom"],
                       requested_through=scope["publishedThrough"], pages=pages)


def build_federal_register(
    output_dir: Path,
    *,
    since: date = FR_EPOCH,
    documents: Callable[[date], Iterable[dict]] | None = None,
    evidence=None,
) -> Path:
    """This run's documents published since ``since`` (every one by default), projected, checked, one per dated key.

    ``documents`` is the raw API records published since a date; the default is SpicyDocs' Federal Register
    acquisition, and a hermetic test passes its own. Only the default names a retained page, so only its rows carry
    ``source_sha256``. The prior table is merged in ``merge_federal_register``, at the published columns.
    """
    import duckdb
    from spicy_docs.schemas.federal_register import FEDERAL_REGISTER_COLUMNS, project_federal_register_document

    out_file = output_dir / OUTPUT
    schema = pa.schema([(c, pa.string()) for c in (*FEDERAL_REGISTER_COLUMNS, POINTER)])
    logger.info("FR: fetching documents published since {}", since)
    if documents is not None:
        rows = [project_federal_register_document(doc) for doc in documents(since)]
    else:
        rows = [project_federal_register_document(doc) | {POINTER: page}
                for page, doc in _live_federal_register_documents(since, output_dir=output_dir, evidence=evidence)]
    new_file = output_dir / "_fr_new.parquet"
    table = pa.Table.from_pylist(rows, schema=schema) if rows else schema.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("FR: fetched {:,} documents this run", len(rows))

    # The SpicyDocs public-table primary key (projection 1.1). The columns and their literal values stay unchanged;
    # this is not an IRI normalization or an assertion that two dated printings are one matter.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")
    # The pointer rides with each row but is no part of the record: one document read from two pages is one
    # observation, so the conflict check below compares the projection alone.
    cols = ", ".join(FEDERAL_REGISTER_COLUMNS)
    try:
        con.execute(f"CREATE TEMP TABLE _fresh AS SELECT {cols}, {POINTER}, file_row_number AS _row "
                    f"FROM read_parquet('{new_file}', file_row_number=true)")
        invalid = con.execute(
            """SELECT document_number, publication_date FROM _fresh
                WHERE document_number IS NULL OR trim(document_number) = ''
                   OR publication_date IS NULL
                   OR try_cast(publication_date AS DATE) IS NULL
                   OR cast(try_cast(publication_date AS DATE) AS VARCHAR) <> publication_date
                LIMIT 1"""
        ).fetchone()
        if invalid:
            raise ValueError(f"Federal Register record lacks a complete canonical identity: {invalid!r}")
        conflict = con.execute(
            f"""SELECT document_number, publication_date FROM _fresh
                GROUP BY document_number, publication_date
                HAVING count(DISTINCT ({cols})) > 1 LIMIT 1"""
        ).fetchone()
        if conflict:
            raise ValueError(f"Federal Register conflicting observations of the same number/date: {conflict!r}")
        merged_file = output_dir / "_fr_merged.parquet"
        # Written in the contract's published order; SpicyDocs 0.66.6 lists ``rin`` before its newest fields.
        published = published_columns()
        if sorted(published) != sorted((*FEDERAL_REGISTER_COLUMNS, "rin", POINTER)):
            raise ValueError("Federal Register contract differs from its projection plus the derived rin")
        selected = ", ".join(f"{_RIN_SQL} AS rin" if column == "rin" else column for column in published)
        # One row per dated key, the first page's: a document on two pages is one observation (checked above).
        con.execute(
            f"""
        COPY (
            SELECT {selected} FROM _fresh
            QUALIFY row_number() OVER (PARTITION BY document_number, publication_date ORDER BY _row) = 1
            ORDER BY publication_date DESC, document_number
        ) TO '{merged_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
            """
        )
        merged_file.replace(out_file)
    finally:
        con.close()
    new_file.unlink(missing_ok=True)
    logger.info("Federal Register: {:,} documents observed", pq.ParquetFile(out_file).metadata.num_rows)
    return out_file


def published_prior(key: str, path: Path) -> bool:
    """Download the table the captured index pins for ``key``; ``False`` when no family publishes it.

    Never a bare-key object: the receipt route read only selected generations, and so does this one.
    """
    from os import getenv

    from spicy_regs.sources import publication

    public_url = getenv("R2_PUBLIC_URL")
    if not public_url or publication.table_owner(publication.current_index(public_url), key) is None:
        return False
    return r2.download(key, path)


def merge_federal_register(
    output_dir: Path,
    *,
    since: date | None = None,
    documents: Callable[[date], Iterable[dict]] | None = None,
    download_prior: Callable[[str, Path], bool] = published_prior,
    evidence=None,
) -> Path:
    """The published table: this run's observations, mapped once, over the pinned prior public table (ss-vse3).

    ``build_federal_register`` reads and checks the run's documents; they are mapped to the published columns
    here (``regulations_shape``) and replace the prior table's rows of the same ``(document_number,
    publication_date)``. Every other prior row is carried as published, never read back through a receipt or
    rewritten one row at a time: on 2026-11-09 that restore and rewrite were 1,542 of the job's 1,764 s for 233
    changed rows. ``rin`` is derived again on every row. Order is the table's: date descending, then number; row
    groups stay at ``ROW_GROUP_ROWS``.

    A prior without one of the columns that left receipts (``PROMOTED_LAST``) is refused: carried, its rows would
    publish NULL for values only its receipts held. A prior that differs otherwise (a column the contract added or
    dropped since, a type it changed) is read back to the text shape and mapped as the night's rows are, so a column
    it lacks reads NULL until a re-read fills it. The merge is checked by count, one row per key and every prior key
    kept, and the counts go to the run's journal beside the pages it read.
    """
    import duckdb

    from spicy_regs.transforms.regulations_shape import IDENTITIES, PROMOTED_LAST, published_as_source, subject_schema

    schema = subject_schema("federal_register")
    key = ", ".join(IDENTITIES["federal_register"])
    prior, out_file = output_dir / "_fr_published.parquet", output_dir / OUTPUT
    scratch = [prior, output_dir / "_fr_prior_text.parquet", output_dir / "_fr_prior.parquet",
               output_dir / "_fr_observed.parquet", output_dir / "_fr_fresh.parquet"]
    have_prior = prior.exists() or download_prior(OUTPUT, prior)
    if not have_prior and out_file.exists():
        # A local run beside the table it last built: that table is its prior.
        out_file.replace(prior)
        have_prior = True
    if have_prior and not (held := pq.read_schema(prior)).equals(schema, check_metadata=False):
        if missing := [name for name in PROMOTED_LAST["federal_register"] if name not in held.names]:
            raise ValueError(
                f"The published federal_register lacks {missing}, which a generation before 2026-11-10 held only in "
                "its receipts: carried, its rows would publish NULL for them. Point the family back at a generation "
                "that has them (publication.restore_family; every one since 2026-11-10 does) before merging at the "
                "public table")
        logger.info("FR: the prior differs from the published columns; mapping its {:,} rows again",
                    pq.ParquetFile(prior).metadata.num_rows)
        prior = _mapped(published_as_source("federal_register", prior, scratch[1]), scratch[2], schema)
    if since is None:
        prior_max = _prior_max_publication_date(prior) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else FR_EPOCH
    # The night's observations alone (no prior to merge), moved off the table's name before the merge writes it.
    observed = build_federal_register(output_dir, since=since, documents=documents, evidence=evidence
                                      ).replace(scratch[3])
    fresh = _mapped(observed, scratch[4], schema)
    read_rows = pq.ParquetFile(fresh).metadata.num_rows
    names = ", ".join(f"{_PUBLISHED_RIN_SQL} AS rin" if name == "rin" else f'"{name}"' for name in schema.names)
    merged = output_dir / "_fr_merged.parquet"
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    with duckdb.connect() as con:
        con.execute("SET memory_limit='4GB'")
        con.execute("SET threads=2")
        con.execute(f"SET temp_directory='{spill_dir}'")
        con.execute(f"CREATE VIEW fresh AS SELECT * FROM read_parquet('{fresh}')")
        carried = (f" UNION ALL BY NAME SELECT * FROM read_parquet('{prior}') ANTI JOIN fresh USING ({key})"
                   if have_prior else "")
        rows = con.execute(f"SELECT {names} FROM (SELECT * FROM fresh{carried}) "
                           "ORDER BY publication_date DESC, document_number")
        with pq.ParquetWriter(merged, schema, compression="zstd") as writer:
            for batch in rows.to_arrow_reader(50_000):
                writer.write_table(pa.Table.from_batches([batch]).cast(schema), row_group_size=ROW_GROUP_ROWS)
        [(total, keys)] = con.execute(f"SELECT count(*), count(DISTINCT ({key})) FROM read_parquet('{merged}')"
                                      ).fetchall()
        prior_rows = pq.ParquetFile(prior).metadata.num_rows if have_prior else 0
        [(added,)] = (con.execute(f"SELECT count(*) FROM fresh ANTI JOIN read_parquet('{prior}') USING ({key})")
                      .fetchall() if have_prior else [(read_rows,)])
    counts = {"prior_rows": prior_rows, "read_rows": read_rows, "new_rows": added, "rows": total}
    if total != keys or total != prior_rows + added:
        raise ValueError(f"Federal Register merge lost or repeated a record: {counts}, {keys} keys")
    merged.replace(out_file)
    for path in scratch:
        path.unlink(missing_ok=True)
    if evidence is not None:
        evidence.event("federal-register-merge", **counts)
    logger.info("Federal Register: {:,} rows ({:,} read, {:,} new)", total, read_rows, added)
    return out_file


def _mapped(text: Path, destination: Path, schema: pa.Schema) -> Path:
    """``text``'s rows, in the text shape the builder writes, mapped to the published columns a batch at a time.

    One mapper for the night's rows and a prior read back from another width; a row it refuses fails the run.
    """
    from spicy_regs.transforms.regulations_shape import shape_records

    with pq.ParquetWriter(destination, schema, compression="zstd") as writer:
        for batch in pq.ParquetFile(text).iter_batches(batch_size=5000):
            writer.write_table(pa.Table.from_pylist(shape_records("federal_register", batch.to_pylist()),
                                                    schema=schema))
    return destination
