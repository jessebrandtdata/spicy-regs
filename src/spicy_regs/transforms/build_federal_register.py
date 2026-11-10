"""Transform: build ``federal_register.parquet`` from the FR REST API.

Produces SpicyDocs' public projection (``FEDERAL_REGISTER_COLUMNS``, all
VARCHAR, through ``project_federal_register_document``) plus the derived
``rin``: one owner for the column list and the value mapping, not a local
copy (consolidation plan B2). ``topics_json`` is the publisher's ``topics``;
a row published before that column existed reads NULL until it is fetched
again, and a full re-read (``since`` at the epoch) refills every row.

Incremental by design: best-effort prior from R2, fetch documents published
since its max ``publication_date`` minus a short overlap, then dedup on
``(document_number, publication_date)`` preferring a fresh observation of that
same dated record, so corrections republished under the same number on another
date survive as distinct rows. With no prior table it is a full backfill from
the FR epoch; the overlap cannot recover old records already lost by a
number-only merge — those take an explicit historical replay. Conflicting
observations within one input generation refuse publication rather than
choosing an arbitrary row.

``rin`` is the one column not fetched: the first Regulation Identifier Number
in ``regulation_id_numbers_json``, retained as a compatibility projection — an
empty array and NULL both give NULL — while a consumer wanting every RIN of a
multi-RIN document unnests the array (see docs/regulatory-rins.md).
``modify_date`` is not exposed by the REST API, so fresh rows carry NULL for it
while the merge preserves whatever the prior table had.

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

#: ``rin`` as a projection of the array, for every row of the merged table.
#: ``[]`` and NULL both give NULL: a join key is present or it is not.
_RIN_SQL = "json_extract_string(regulation_id_numbers_json, '$[0]')"


def _prior_columns(prior_file: Path, columns: tuple[str, ...]) -> str:
    """``columns`` as selected from the prior table; one it predates reads NULL."""
    held = set(pq.read_schema(prior_file).names)
    return ", ".join(c if c in held else f"CAST(NULL AS VARCHAR) AS {c}" for c in columns)


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
            if not federal_register.federal_register_records_included(response, query_scope=scope, page_window=window):
                continue
            for document in response["results"]:
                yield digest, document


def build_federal_register(
    output_dir: Path,
    *,
    since: date | None = None,
    documents: Callable[[date], Iterable[dict]] | None = None,
    download_prior: Callable[[str, Path], bool] = r2.download,
    evidence=None,
) -> Path:
    """Build ``federal_register.parquet`` (incremental merge with the prior table).

    ``documents`` is the raw API records published since a date; the default is
    SpicyDocs' Federal Register acquisition, and a hermetic test passes its own.
    Only the default names a retained page, so only its rows carry ``source_sha256``.
    """
    import duckdb
    from spicy_docs.schemas.federal_register import FEDERAL_REGISTER_COLUMNS, project_federal_register_document

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_fr_prior.parquet"
    schema = pa.schema([(c, pa.string()) for c in (*FEDERAL_REGISTER_COLUMNS, POINTER)])

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or download_prior(OUTPUT, prior_file)
    if have_prior:
        logger.info("FR: merging against prior table {}", prior_file)
    else:
        logger.info("FR: no prior table found — full backfill from {}", FR_EPOCH)

    # 2. Decide the fetch window start.
    if since is None:
        prior_max = _prior_max_publication_date(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else FR_EPOCH
    logger.info("FR: fetching documents published since {}", since)

    # 3. Fetch + shape into a "new rows" parquet.
    if documents is not None:
        rows = [project_federal_register_document(doc) for doc in documents(since)]
    else:
        rows = [project_federal_register_document(doc) | {POINTER: page}
                for page, doc in _live_federal_register_documents(since, output_dir=output_dir, evidence=evidence)]
    new_file = output_dir / "_fr_new.parquet"
    table = pa.Table.from_pylist(rows, schema=schema) if rows else schema.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("FR: fetched {:,} documents this run", len(rows))

    # 4. Apply the SpicyDocs public-table primary key (projection 1.1).
    # The columns and their literal values stay unchanged; this is not an IRI
    # normalization or an assertion that two dated printings are one matter.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    # The projection's columns are what both sides carry; a prior that already
    # has ``rin`` is not read for it, since the projection below recomputes it,
    # and a column the prior predates (``topics_json``) reads NULL.
    # The pointer rides with each row but is no part of the record: one document read from two pages is one
    # observation, so the conflict check below compares the projection alone.
    cols = ", ".join(FEDERAL_REGISTER_COLUMNS)
    carried = f"{cols}, {POINTER}"
    if have_prior:
        union = (
            f"SELECT {_prior_columns(prior_file, (*FEDERAL_REGISTER_COLUMNS, POINTER))}, file_row_number AS _row, 0 AS _src "
            f"FROM read_parquet('{prior_file}', file_row_number=true) "
            f"UNION ALL BY NAME "
            f"SELECT {carried}, file_row_number AS _row, 1 AS _src FROM read_parquet('{new_file}', file_row_number=true)"
        )
    else:
        union = (
            f"SELECT {carried}, file_row_number AS _row, 1 AS _src FROM read_parquet('{new_file}', file_row_number=true)"
        )

    try:
        # Materialize the prior ∪ fresh union once: the checks, the winner
        # window and the final COPY each scan it, and re-scanning the wide
        # abstract/JSON payload columns four times per run was the largest
        # cost in this rollup.
        con.execute(f"CREATE TEMP TABLE _union AS {union}")
        invalid = con.execute(
            """SELECT document_number, publication_date FROM _union
                WHERE document_number IS NULL OR trim(document_number) = ''
                   OR publication_date IS NULL
                   OR try_cast(publication_date AS DATE) IS NULL
                   OR cast(try_cast(publication_date AS DATE) AS VARCHAR) <> publication_date
                LIMIT 1"""
        ).fetchone()
        if invalid:
            raise ValueError(f"Federal Register record lacks a complete canonical identity: {invalid!r}")
        conflict = con.execute(
            f"""SELECT document_number, publication_date, _src FROM _union
                GROUP BY document_number, publication_date, _src
                HAVING count(DISTINCT ({cols})) > 1 LIMIT 1"""
        ).fetchone()
        if conflict:
            raise ValueError(f"Federal Register conflicting observations of the same number/date: {conflict!r}")
        # Rank positions, not the wide abstract/JSON payloads. A window over
        # every payload column exceeds the 4GB limit on the retained public
        # table; selecting positions first keeps the same winner semantics.
        con.execute(
            """CREATE TEMP TABLE winners AS
                SELECT _src, _row FROM (
                    SELECT _src, _row, row_number() OVER (
                        PARTITION BY document_number, publication_date ORDER BY _src DESC, _row
                    ) AS _rn FROM _union
                ) WHERE _rn = 1"""
        )
        merged_file = output_dir / "_fr_merged.parquet"
        # Written in the contract's published order; SpicyDocs 0.66.6 lists ``rin`` before its newest fields.
        published = published_columns()
        if sorted(published) != sorted((*FEDERAL_REGISTER_COLUMNS, "rin", POINTER)):
            raise ValueError("Federal Register contract differs from its projection plus the derived rin")
        selected = ", ".join(f"{_RIN_SQL} AS rin" if column == "rin" else column for column in published)
        con.execute(
            f"""
        COPY (
            SELECT {selected}
            FROM _union records JOIN winners USING (_src, _row)
            ORDER BY publication_date DESC, document_number
        ) TO '{merged_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
            """
        )
        merged_file.replace(out_file)
    finally:
        con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Federal Register: {:,} rows", total)
    return out_file
