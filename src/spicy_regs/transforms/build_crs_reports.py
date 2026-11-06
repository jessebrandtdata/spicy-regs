"""Transform: build ``crs_reports.parquet`` from the Congress.gov REST API.

Produces a native subject table and shared receipts keyed on ``report_id`` and
``update_date``, retaining each captured revision of the report's list metadata.

Incremental by design: best-effort prior from R2, fetch only reports updated
since its max ``update_date`` minus a short overlap, then dedup on
``(report_id, update_date)`` preferring the fresh copy of a repeated revision.
With no prior table it is a full backfill. Scope is deliberately **list-level only** — every column comes from
the ``/crsreport`` list payload, so there are no per-report detail fetches; the
detail endpoint's authors, formats and related materials are left to a later
enrichment pass.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.parquet_rows import str_or_none
from spicy_regs.sources import r2
from spicy_regs.transforms.government_receipts import internal_prior, receipt_builder
from spicy_regs.transforms.government_source_shapes import IDENTITY_FIELDS, GovernmentShapeError
from spicy_regs.sources.crs_reports import CrsReportsReader
from spicy_regs.transforms.table_merge import merge_local_prior

OUTPUT = "crs_reports.parquet"

# Re-scan this many days before the last stored update_date on each run, so
# reports updated after our previous run's cutoff are picked up.
OVERLAP_DAYS = 3

# Literal processing schema, converted to native subjects and retained in receipts.
# The report id and source update date identify a retained revision.
COLUMNS = (
    "report_id",
    "title",
    "report_type",
    "status",
    "published_date",
    "update_date",
    "version",
    "url",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])



def _shape(doc: dict) -> dict:
    """Map one raw Congress.gov CRS report onto the published column shape."""
    return {
        "report_id": doc.get("id"),
        "title": doc.get("title"),
        "report_type": doc.get("contentType"),
        "status": doc.get("status"),
        "published_date": doc.get("publishDate"),
        "update_date": doc.get("updateDate"),
        "version": str_or_none(doc.get("version")),
        "url": doc.get("url"),
    }


def _prior_max_update_date(prior_file: Path) -> date | None:
    """Largest ``update_date`` in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(update_date) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


@receipt_builder
def build_crs_reports(output_dir: Path, *, since: date | None = None, evidence: CaptureEvidence | None = None) -> Path:
    """Build ``crs_reports.parquet`` (incremental merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_crs_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        prior_file = internal_prior("crs_reports", prior_file)
        # Undated prior rows cannot be assigned revision identities. Preserve the
        # prior and refuse before merging, rather than inventing a date.
        if ("update_date" not in pq.read_schema(prior_file).names
                or pq.read_table(prior_file, columns=["update_date"])["update_date"].null_count):
            raise GovernmentShapeError("crs_reports: prior revisions require update_date; prior preserved")
    if have_prior:
        logger.info("CRS reports: merging against prior table {}", prior_file)
    else:
        logger.info("CRS reports: no prior table found — full backfill")

    # 2. Decide the fetch window start.
    if since is None:
        prior_max = _prior_max_update_date(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else None
    logger.info("CRS reports: fetching reports updated since {}", since or "the beginning")

    # 3. Fetch + shape into a "new rows" parquet.
    reader = CrsReportsReader(since=since, evidence=evidence)
    rows = [_shape(doc) for doc in reader.iter_records()]
    new_file = output_dir / "_crs_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("CRS reports: fetched {:,} reports this run", len(rows))

    # 4. Keep distinct dated revisions; a repeated revision uses the fresh copy.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    merge_local_prior(
        con,
        columns=COLUMNS,
        identity=IDENTITY_FIELDS["crs_reports"],
        order_by="update_date DESC, report_id",
        prior_file=prior_file if have_prior else None,
        new_file=new_file,
        out_file=out_file,
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("CRS reports: {:,} rows", total)
    return out_file
