"""CourtListener bulk domain tables, each from one whole export, with no per-row receipts (ss-vse3).

Each complete export becomes one table. Rows keep native identifiers, legal descriptions and typed domain values,
and the publisher's record fields beside them (``date_created``, ``date_modified``, ``local_path``, ...), the
export's edition (``dump_date``) and ``source_sha256``, the SHA-256 of the export file the row was read from: the
pointer to the original, which a run with evidence keeps in the shared raw archive with its capture receipt. The
publisher's spelling of a converted value is its canonical spelling (``court_bulk.write_table``); a row spelled
otherwise, or refused, is recorded in the table's exceptions file, kept with the run's journal.

A source must be read completely before its table replaces its predecessor. Cluster-naming tables still refuse
exports ahead of the selected cluster publication. Downloads keep their listed size and ETag checks and the disk
floor; downloaded and pre-existing source bytes remain held.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs import court_bulk
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.court_subjects import SUBJECT_SCHEMAS, normalize_court_row
from spicy_regs.transforms._courtlistener_writer import check_headroom

if TYPE_CHECKING:
    from spicy_docs.sources.courtlistener.listing import BulkObject

    from spicy_regs.source_evidence import CaptureEvidence

#: Directory of retained exports, named as the publisher names them (``citations-2026-06-30.csv.bz2``).
RETAINED_DIR_ENV = "COURTLISTENER_BULK_DIR"
#: Rows per Parquet row group.
BATCH_ROWS = 100_000
#: The published table whose ``cluster_id`` a cluster-naming export must not run ahead of.
CLUSTERS_TABLE = "court_opinion_clusters.parquet"
#: The evidence stage a downloaded export is captured under, and the journal event each table's build leaves.
EXPORT_STAGE = "courtlistener-bulk-export"
TABLE_EVENT = "courtlistener-bulk-table"


@dataclass(frozen=True)
class BulkTable:
    """One publisher table: the export it comes from and the source field behind each column."""

    dataset: str
    output: str
    fields: tuple[tuple[str, str], ...]

    @property
    def input_columns(self) -> tuple[str, ...]:
        return (*(column for column, _ in self.fields), "dump_date")

    @property
    def input_schema(self) -> pa.Schema:
        return pa.schema([(column, pa.string()) for column in self.input_columns])

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(self.schema.names)

    @property
    def schema(self) -> pa.Schema:
        return SUBJECT_SCHEMAS[self.output.removesuffix('.parquet')]

    @property
    def names_clusters(self) -> bool:
        return "cluster_id" in self.columns


CITATIONS = BulkTable(
    "citations",
    "court_citations.parquet",
    (
        ("citation_id", "id"),
        ("cluster_id", "cluster_id"),
        ("volume", "volume"),
        ("reporter", "reporter"),
        ("page", "page"),
        ("citation_type", "type"),
        ("date_created", "date_created"),
        ("date_modified", "date_modified"),
    ),
)
CITATION_MAP = BulkTable(
    "citation-map",
    "court_citation_map.parquet",
    (
        ("citing_opinion_id", "citing_opinion_id"),
        ("cited_opinion_id", "cited_opinion_id"),
        ("depth", "depth"),
    ),
)
PARENTHETICALS = BulkTable(
    "parentheticals",
    "court_parentheticals.parquet",
    (
        ("parenthetical_id", "id"),
        ("described_opinion_id", "described_opinion_id"),
        ("describing_opinion_id", "describing_opinion_id"),
        ("text", "text"),
        ("score", "score"),
        ("group_id", "group_id"),
    ),
)
OPINIONS = BulkTable(
    "opinions",
    "court_opinions.parquet",
    (
        ("opinion_id", "id"),
        ("cluster_id", "cluster_id"),
        ("opinion_type", "type"),
        ("author_id", "author_id"),
        ("author_str", "author_str"),
        ("per_curiam", "per_curiam"),
        ("joined_by_str", "joined_by_str"),
        ("page_count", "page_count"),
        ("sha1", "sha1"),
        ("download_url", "download_url"),
        ("local_path", "local_path"),
        ("extracted_by_ocr", "extracted_by_ocr"),
        ("date_created", "date_created"),
        ("date_modified", "date_modified"),
    ),
)


def latest_common_dump_date(objects: Sequence[BulkObject], datasets: Sequence[str]) -> date:
    """The newest export date that publishes every one of ``datasets``, so one run reads one edition."""
    dated = [{obj.dump_date for obj in objects if obj.dataset == dataset and obj.dump_date} for dataset in datasets]
    common = set.intersection(*dated) if dated else set()
    if not common:
        raise RuntimeError(f"CourtListener bulk: no export date publishes all of {list(datasets)}")
    return max(common)


def _highest_cluster_id(parquet: list[str]) -> int | None:
    import duckdb

    from spicy_regs.sources.publication import parquet_scan

    with duckdb.connect() as con:
        if any(path.startswith("https://") for path in parquet):
            load_public_http(con)
        row = con.execute(f"SELECT max(TRY_CAST(cluster_id AS BIGINT)) FROM {parquet_scan(parquet)}").fetchone()
        return None if row is None else row[0]


def published_cluster_ceiling() -> int | None:
    """The highest cluster id in the published ``court_opinion_clusters``, or ``None`` when none is published.

    Reads that one column over the public URL (about 55 MB for ten million clusters).
    """
    from spicy_regs.public_url import resolve_r2_base_url
    from spicy_regs.sources.publication import load_index, table_members

    base = resolve_r2_base_url()
    members = table_members(load_index(base), CLUSTERS_TABLE)
    return None if members[0].sha256 is None else _highest_cluster_id([f"{base}/{m.path}" for m in members])


def _bucket_url(url: str) -> str:
    from spicy_docs.sources.courtlistener.listing import BULK_BASE_URL

    if not url.startswith(BULK_BASE_URL + "/"):
        raise ValueError("CourtListener exports are read only from the publisher's bulk-data bucket")
    return url


def export_file(published: BulkObject, work_dir: Path, evidence: CaptureEvidence | None = None, *,
                transport=None) -> tuple[Path, str | None]:
    """A local copy of one listed export, and its SHA-256 when this call learned it without another read.

    A retained copy (``COURTLISTENER_BULK_DIR``) must match the listed size; the bytes themselves are checked as they
    decode, by bzip2's block checksums. With ``evidence`` it is retained as the run's original (into the shared raw
    archive, where a clone of the archive's own object adds no copy), so the rows' ``source_sha256`` names a file the
    evidence holds. With ``evidence``, a download goes once into the shared raw archive through the run's evidence
    transport (the per-download receipt) and is read there in place: the archive object is the only copy. Without it,
    a download is bound to the listed ETag and size in the work directory. A download is refused when it would take
    the volume it writes to below the disk floor. ``transport`` is the HTTP transport the evidence wraps (a test's).
    """
    retained_dir = os.environ.get(RETAINED_DIR_ENV)
    if retained_dir:
        retained = Path(retained_dir) / published.filename
        if retained.is_file():
            if retained.stat().st_size != published.size:
                raise RuntimeError(
                    f"CourtListener bulk: retained {retained} is {retained.stat().st_size} bytes, "
                    f"not the listed {published.size}"
                )
            logger.info("CourtListener bulk: using retained {}", retained)
            if evidence is None:
                return retained, None
            held = evidence.retain_file(retained, stage=EXPORT_STAGE, raw_source=True,
                                        original_location=published.url, source_url=published.url,
                                        etag=published.etag)
            return retained, held["sha256"]
    logger.info("CourtListener bulk: downloading {} ({:.2f} GiB)", published.filename, published.size / 2**30)
    if evidence is not None:
        return _retain_export(published, evidence, transport=transport)
    check_headroom(published.size, path=work_dir)
    from spicy_docs.transport.download import BoundedAcquirer

    store = work_dir / "courtlistener-exports"
    with BoundedAcquirer(validate_url=_bucket_url, timeout=120) as acquirer:
        receipt = acquirer.download(
            published.url,
            store=store,
            max_bytes=published.size,
            expected_size=published.size,
            etag=published.etag,
        )
    return store / receipt["blob_path"], None


#: A staged raw-archive write untouched this long has no live writer: a download rewrites its file continuously.
STALE_PENDING_SECONDS = 6 * 3600
_BZIP2 = b"BZh"


def clear_stale_export_downloads(raw_root: Path, *, now: float | None = None) -> list[Path]:
    """Remove the archive's staged writes a killed export download left (bzip2 bytes, untouched for six hours).

    The archive's writer removes its own staged file on any exception, but not when the process is killed; then up
    to 51.5 GiB stays in ``.pending`` with no receipt. Only files that begin as a bzip2 stream, as every CourtListener
    export does, and that nothing has written for ``STALE_PENDING_SECONDS`` are removed, so another writer's file is
    left alone. Returns what it removed.
    """
    import time

    pending = raw_root / ".pending"
    if not pending.is_dir():
        return []
    cutoff = (time.time() if now is None else now) - STALE_PENDING_SECONDS
    removed = []
    for entry in os.scandir(pending):
        if not entry.is_file(follow_symlinks=False) or entry.stat(follow_symlinks=False).st_mtime > cutoff:
            continue
        with open(entry.path, "rb") as stream:
            if stream.read(len(_BZIP2)) != _BZIP2:
                continue
        logger.warning("CourtListener bulk: removing a stale staged export download {} ({:.2f} GiB)",
                       entry.name, entry.stat(follow_symlinks=False).st_size / 2**30)
        os.unlink(entry.path)
        removed.append(Path(entry.path))
    return removed


def _retain_export(published: BulkObject, evidence: CaptureEvidence, *, transport=None) -> tuple[Path, str]:
    """Stream one export into the raw archive (one write, with its capture receipt); return the archive copy."""
    import httpx

    from spicy_regs.pipelines.raw_releases import raw_original

    clear_stale_export_downloads(evidence.store.root)
    check_headroom(published.size, path=evidence.store.root)
    retained = evidence.transport(transport, stage=EXPORT_STAGE, max_bytes=published.size,
                                  response_header_names=("etag",))
    with httpx.Client(transport=retained, timeout=120, follow_redirects=False) as client:
        with client.stream("GET", _bucket_url(published.url), headers={"Accept-Encoding": "identity"}) as response:
            response.raise_for_status()
            if response.headers.get("etag") != published.etag:
                raise RuntimeError(f"CourtListener bulk: {published.filename} is ETag "
                                   f"{response.headers.get('etag')}, not the listed {published.etag}")
            for _ in response.iter_raw():  # the transport has retained every byte before it replays them
                pass
            capture = response.extensions["spicy_docs.completed_capture"]
    if capture.byte_size != published.size:
        raise RuntimeError(f"CourtListener bulk: {published.filename} is {capture.byte_size} bytes, "
                           f"not the listed {published.size}")
    return raw_original(evidence.store, capture.sha256, published.size), capture.sha256


def _file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def build_court_bulk_table(
    table: BulkTable, output_dir: Path, *, local_file: Path, dump_date: date, cluster_ceiling: int | None = None,
    source_sha256: str | None = None, evidence: CaptureEvidence | None = None,
) -> Path:
    """Write ``table.output`` from one whole export; the previous file survives any failure.

    Every row carries the export's edition (``dump_date``) and ``source_sha256``, the SHA-256 of the export file it
    was read from (hashed here once when the caller did not learn it while downloading). With ``cluster_ceiling``,
    a table that names clusters is refused when any is above it. Rows the mapping refuses or that the publisher
    spelled other than canonically are recorded in ``<table>.exceptions.parquet`` (``court_bulk.EXCEPTION_SCHEMA``),
    which ``evidence`` retains with the run's journal; no per-row receipt is written.
    """
    from spicy_docs.sources.courtlistener.local import CourtListenerLocalDump

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / table.output
    dataset = table.output.removesuffix(".parquet")
    staging = output_dir / f".{table.output}.{uuid4().hex}.source.parquet"
    written = output_dir / f".{table.output}.{uuid4().hex}.partial"
    exceptions = output_dir / f"{dataset}.exceptions.parquet"
    dump = CourtListenerLocalDump(local_file, columns=[source for _, source in table.fields], work_dir=output_dir)
    edition = dump_date.isoformat()
    pending: list[pa.RecordBatch] = []

    def flush(writer: pq.ParquetWriter) -> None:
        # A piece of opinion text yields a few thousand metadata rows; gather them into row groups.
        writer.write_table(pa.Table.from_batches(pending, schema=table.input_schema), row_group_size=BATCH_ROWS)
        pending.clear()

    try:
        with pq.ParquetWriter(staging, table.input_schema, compression="zstd") as writer:
            for batch in dump.iter_batches():
                stamped = [*batch.columns, pa.array([edition] * batch.num_rows, pa.string())]
                pending.append(pa.RecordBatch.from_arrays(stamped, schema=table.input_schema))
                if sum(part.num_rows for part in pending) >= BATCH_ROWS:
                    flush(writer)
            if pending:
                flush(writer)
        if not dump.completed:
            raise RuntimeError(f"CourtListener bulk: {local_file.name} was not read to its end")
        if cluster_ceiling is not None and table.names_clusters:
            newest = _highest_cluster_id([str(staging)])
            if newest is not None and newest > cluster_ceiling:
                raise RuntimeError(
                    f"{table.output}: the {edition} export names cluster {newest:,}, above the published "
                    f"{CLUSTERS_TABLE} (to {cluster_ceiling:,}); run run-rollup-court-opinion-clusters first"
                )
        digest = source_sha256 or _file_sha256(local_file)
        exceptions.unlink(missing_ok=True)
        rows, recorded = court_bulk.write_table(
            dataset, staging, written, constants={"source_sha256": digest}, exceptions=exceptions,
            attempt=partial(normalize_court_row, dataset))
        written.replace(out_file)
    finally:
        staging.unlink(missing_ok=True)
        written.unlink(missing_ok=True)
    if evidence is not None:
        evidence.event(TABLE_EVENT, table=dataset, edition=edition, source_sha256=digest, source_rows=dump.rows,
                       rows=rows, exceptions=recorded)
        if recorded:
            evidence.retain_file(exceptions, stage="courtlistener-export-exceptions", table=dataset)
    logger.info(
        "{}: {:,} rows from {} ({:.2f} GB of CSV, {} pieces); {:,} exception records",
        table.output,
        rows,
        local_file.name,
        dump.decompressed_bytes / 1e9,
        dump.pieces,
        recorded,
    )
    return out_file


def build_court_bulk_tables(
    tables: Sequence[BulkTable], output_dir: Path, *, dump_date: date | None = None,
    evidence: CaptureEvidence | None = None,
) -> tuple[Path, ...]:
    """Build one complete edition, a table at a time; each previous table survives until its replacement is whole."""
    from spicy_docs.sources.courtlistener.bulk import find_dump, list_bulk_dumps

    objects = list_bulk_dumps()
    edition = dump_date or latest_common_dump_date(objects, [table.dataset for table in tables])
    ceiling = None
    if any(table.names_clusters for table in tables):
        ceiling = published_cluster_ceiling()
        if ceiling is None:
            raise RuntimeError(f"CourtListener bulk: {CLUSTERS_TABLE} is not published; publish it before its children")
    built = []
    for table in tables:
        published = find_dump(objects, table.dataset, edition)
        if published is None:
            raise RuntimeError(f"CourtListener bulk: no {table.dataset} export for {edition}")
        if evidence is not None:
            evidence.event("courtlistener-export-version", export_url=published.url, etag=published.etag,
                           content_length=published.size, edition=edition.isoformat())
        local_file, digest = export_file(published, output_dir, evidence)
        built.append(
            build_court_bulk_table(
                table, output_dir, local_file=local_file, dump_date=edition, cluster_ceiling=ceiling,
                source_sha256=digest, evidence=evidence,
            )
        )
    return tuple(built)
