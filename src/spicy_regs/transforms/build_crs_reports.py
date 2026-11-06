"""Transform: build ``crs_reports.parquet`` from the Congress.gov REST API.

Produces a native subject table and shared receipts keyed on ``report_id`` and
``update_date``, retaining each captured revision of the report's list metadata.

Incremental by design: best-effort prior from R2, fetch only reports updated
since its max ``update_date`` minus a short overlap, then dedup on
``(report_id, update_date)`` preferring the fresh copy of a repeated revision.
With no prior table it is a full list backfill. Explicit ``detail_limit`` adds a
bounded enrichment pass for selected current revisions; ordinary refreshes keep
qualified detail and its witnesses at an unchanged native coordinate.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from contextlib import nullcontext
import json

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.parquet_rows import str_or_none
from spicy_regs.sources import r2
from spicy_regs.transforms.government_receipts import internal_prior, receipt_builder
from spicy_regs.transforms.government_source_shapes import GovernmentShapeError
from spicy_regs.sources.crs_reports import CrsReportDetailReader, CrsReportsReader
from spicy_regs.crs_report_detail import DETAIL_COLUMNS, check_detail_replacement, json_text, qualified_detail

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
    *DETAIL_COLUMNS,
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])



def _shape(doc: dict) -> dict:
    """Map one raw Congress.gov CRS report onto the published column shape."""
    return {
        **dict.fromkeys(DETAIL_COLUMNS),
        "detail_state": "not_requested",
        "detail_attempt_state": "not_requested",
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
def build_crs_reports(
    output_dir: Path, *, since: date | None = None, evidence: CaptureEvidence | None = None,
    detail_limit: int = 0, detail_api_key: str | None = None, detail_transport=None, retry_completed: bool = False,
    retained_details: dict[str, tuple[Path, str]] | None = None, refresh_list: bool = True,
) -> Path:
    """Build ``crs_reports.parquet`` (incremental merge with the prior table)."""
    if type(detail_limit) is not int or not 0 <= detail_limit <= 100:
        raise ValueError("detail_limit must be an integer between 0 and 100")

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
    logger.info("CRS reports: list refresh={}, window starts {}", refresh_list, since or "the beginning")

    # 3. Fetch + shape into a "new rows" parquet.
    if not refresh_list and (not have_prior or not detail_limit):
        raise ValueError("A detail-only build requires a selected local prior and positive detail_limit")
    reader = CrsReportsReader(since=since, evidence=evidence)
    rows = [_shape(doc) for doc in reader.iter_records()] if refresh_list else []
    list_rows = len(rows)
    for row in rows:
        witnesses = getattr(reader, "witnesses", {}).get((row["report_id"], row["update_date"]))
        if witnesses:
            row["list_witness_json"] = json_text(witnesses)
    # One bounded table pass owns the source-specific merge. It distinguishes
    # unrequested detail from authoritative explicit null/empty values.
    merged = {(row["report_id"], row["update_date"]): {**dict.fromkeys(COLUMNS), **row}
              for row in pq.read_table(prior_file).to_pylist()} if have_prior else {}
    for row in rows:
        key = (row["report_id"], row["update_date"])
        prior = merged.get(key)
        if prior:
            if prior.get("detail_state") == "ok" and any(prior.get(name) != row.get(name) for name in
                    ("version", "title", "report_type", "status", "published_date")):
                raise GovernmentShapeError("CRS incompatible same-coordinate list metadata; prior preserved")
            for name in DETAIL_COLUMNS:
                row[name] = prior.get(name)
            old_witnesses = json.loads(prior.get("list_witness_json") or "[]")
            fresh_witnesses = getattr(reader, "witnesses", {}).get(key, [])
            for witness in fresh_witnesses:
                if witness not in old_witnesses:
                    old_witnesses.append(witness)
            row["list_witness_json"] = json_text(old_witnesses) if old_witnesses else None
        merged[key] = row
    if detail_limit:
        from spicy_regs.source_evidence import acquisition_evidence

        latest = {}
        for key, row in sorted(merged.items()):
            latest[row["report_id"]] = key
        selected = [merged[key] for key in latest.values() if retry_completed
                    or merged[key].get("detail_state") != "ok" or merged[key].get("detail_attempt_state") != "ok"]
        context = nullcontext(evidence) if evidence is not None else acquisition_evidence("crs-reports", output_dir=output_dir)
        with context as detail_evidence:
            detail_reader = CrsReportDetailReader(evidence=detail_evidence, api_key=detail_api_key,
                                                 transport=detail_transport, retained=retained_details)
            for row in selected[:detail_limit]:
                outcome = detail_reader.read(row)
                if outcome["state"] == "revision_mismatch":
                    detail_evidence.event("crs-detail-revision-mismatch", report_id=row["report_id"],
                                          update_date=row["update_date"], witnesses=outcome["witnesses"])
                    raise GovernmentShapeError("CRS detail revision mismatch; prior preserved")
                if outcome["state"] == "ok":
                    if any(field in outcome["report"] and row.get(column) is not None
                           and outcome["report"][field] != row[column] for column, field in
                           (("title", "title"), ("report_type", "contentType"), ("status", "status"),
                            ("published_date", "publishDate"))):
                        raise GovernmentShapeError("CRS incompatible list/detail metadata; prior preserved")
                    check_detail_replacement(row, outcome["report"])
                    row.update(qualified_detail(outcome["report"], outcome["witnesses"]))
                else:
                    row.update(detail_attempt_state="failed", detail_error=outcome["error"],
                               detail_attempt_witness_json=json_text(outcome["witnesses"]))
    rows = sorted(merged.values(), key=lambda row: (row["update_date"] or "", row["report_id"] or ""), reverse=True)
    new_file = output_dir / "_crs_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("CRS reports: read {:,} list rows; prepared {:,} retained revisions", list_rows, len(rows))

    # Atomically replace only after every source-specific compatibility check.
    new_file.replace(out_file)

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("CRS reports: {:,} rows", total)
    return out_file
