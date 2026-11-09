"""Backfill ``text_content`` from PDF attachments on documents and comments.

This wires the two halves of issue #9 together:

    sources.fetch_pdf_bytes (download)  →  transforms.extract_pdf_text (parse)

over the rows of ``documents.parquet`` / the comments dataset. It is
intentionally a *separate* step from the metadata ETL: that pipeline only moves
JSON and is fast, whereas this downloads potentially many PDFs and is run on
demand / in its own job. The metadata ETL leaves ``text_content`` as ``NULL``;
this fills it.

The enrichment functions take the fetch + extract callables as parameters so
they can be exercised without touching the network.

Documents and comments pack their attachment renditions differently:

  * documents:  ``[{url, format, size}, ...]``                       (flat)
  * comments:   ``[{title, formats: [{url, format, size}]}, ...]``   (nested)

so each has its own URL extractor; both feed the same generic core.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, overload

import polars as pl
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.schemas import RecordType
from spicy_regs.transforms.pdf_text import (
    PAGE_SEPARATOR,
    PdfTextResult,
    PdfTextStatus,
    extract_pdf_text,
)

#: downloads.regulations.gov returns 403 to default/spicy-docs User-Agents; a
#: browser-like UA gets 200. The quirk lives beside the call that needs it.
_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

#: The same cap the local fetch used: a guard against a pathological
#: multi-hundred-MB attachment blowing up a batch run.
_MAX_PDF_BYTES = 100 * 1024 * 1024


#: Each column a text fill writes, and the update column that overrides it when non-null.
TEXT_UPDATES = {
    "text_content": "_new_text",
    "text_extraction_status": "_new_status",
    "pdf_extraction_results_json": "_new_pdf_results",
}


@overload
def with_text_columns(frame: pl.DataFrame) -> pl.DataFrame: ...
@overload
def with_text_columns(frame: pl.LazyFrame) -> pl.LazyFrame: ...
def with_text_columns(frame: Any) -> Any:
    """``frame`` with any missing text-fill column added as null strings."""
    present = frame.collect_schema().names()
    return frame.with_columns(pl.lit(None, dtype=pl.Utf8).alias(c) for c in TEXT_UPDATES if c not in present)


@overload
def apply_text_updates(frame: pl.DataFrame, updates: pl.DataFrame, id_col: str) -> pl.DataFrame: ...
@overload
def apply_text_updates(frame: pl.LazyFrame, updates: pl.LazyFrame, id_col: str) -> pl.LazyFrame: ...
def apply_text_updates(frame: Any, updates: Any, id_col: str) -> Any:
    """Join a fill's ``_new_*`` columns onto ``frame``, keeping its row order; a null update keeps the row's value."""
    return (
        frame.join(updates, on=id_col, how="left", maintain_order="left")
        .with_columns(pl.coalesce([new, column]).alias(column) for column, new in TEXT_UPDATES.items())
        .drop(TEXT_UPDATES.values())
    )


def _attachment_url(url: str) -> str:
    """Accept any credential-free https URL; the fetch follows up to the bounded redirect chain."""
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    if parts.scheme != "https" or parts.username or parts.password:
        raise ValueError("attachment URL must be credential-free https")
    return url


def fetch_pdf_bytes(url: str) -> bytes | None:
    """Bounded attachment fetch through spicy-docs' transport; ``None`` on any refusal.

    Returns ``None`` (rather than raising) on HTTP errors, timeouts, transport
    problems, or a body exceeding the bound, so a batch enrichment run can skip
    the document and keep going — the contract the deleted ``sources/pdf.py``
    copy pinned, now served by the owner's bounded capture.
    """
    from spicy_docs.transport.download import AcquisitionError, BoundedAcquirer

    try:
        with BoundedAcquirer(validate_url=_attachment_url, timeout=30.0) as acquirer:
            capture = acquirer.capture(url, max_bytes=_MAX_PDF_BYTES, extra_headers={"User-Agent": _BROWSER_USER_AGENT})
    except (AcquisitionError, ValueError, OSError):
        return None
    return capture.body


FetchFn = Callable[[str], bytes | None]
ExtractFn = Callable[[bytes], PdfTextResult]
UrlsFn = Callable[[Mapping[str, object]], list[str]]


@dataclass(frozen=True)
class _PdfAttempt:
    """One fetched PDF URL's retained source digest and extraction result."""

    source_sha256: str | None
    extraction: PdfTextResult | None

    @property
    def text(self) -> str | None:
        return None if self.extraction is None else self.extraction.text or None

    @property
    def status(self) -> str:
        return self.extraction.status.value if self.extraction is not None else PdfTextStatus.ERROR.value

    def diagnostic(self, url: str) -> dict[str, object]:
        return {
            "url": url,
            "source_sha256": self.source_sha256,
            "status": self.status,
            "page_count": self.extraction.page_count if self.extraction is not None else None,
            "error": self.extraction.error if self.extraction is not None else "fetch returned no bytes",
        }


def _is_pdf(url: str | None, fmt: str | None) -> bool:
    if not url:
        return False
    return (fmt or "").lower() == "pdf" or url.lower().endswith(".pdf")


def _dedupe(urls: list[str]) -> list[str]:
    """Drop duplicate URLs while preserving first-seen order."""
    return list(dict.fromkeys(urls))


def _opt_str(value: object) -> str | None:
    """Narrow a polars row value (typed ``object``) to ``str | None``."""
    return value if isinstance(value, str) else None


def pdf_urls_for_document(attachments_json: str | None, file_url: str | None) -> list[str]:
    """PDF download URLs for one *document*, in order, de-duplicated.

    Prefers the structured ``attachments_json`` (a flat list of renditions);
    falls back to ``file_url`` when it points at a ``.pdf``. Non-PDF renditions
    (htm, docx, ...) are ignored — extracting those is out of scope.
    """
    urls: list[str] = []
    if attachments_json:
        try:
            for att in json.loads(attachments_json):
                if att is None:
                    continue
                if _is_pdf(att.get("url"), att.get("format")):
                    urls.append(att["url"])
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass
    if not urls and file_url and file_url.lower().endswith(".pdf"):
        urls.append(file_url)
    return _dedupe(urls)


def pdf_urls_for_comment(attachments_json: str | None) -> list[str]:
    """PDF download URLs for one *comment*, in order, de-duplicated.

    Comment attachments nest their renditions: each attachment carries a
    ``formats`` list of ``{url, format, size}``.
    """
    urls: list[str] = []
    if attachments_json:
        try:
            for att in json.loads(attachments_json):
                if att is None:
                    continue
                for fmt in att.get("formats") or []:
                    if fmt is None:
                        continue
                    if _is_pdf(fmt.get("url"), fmt.get("format")):
                        urls.append(fmt["url"])
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass
    return _dedupe(urls)


def _extract_unique(urls: list[str], fetch: FetchFn, extract: ExtractFn, max_workers: int) -> dict[str, _PdfAttempt]:
    """Fetch each URL once and reuse extraction results by source digest.

    Retains each URL's source digest, spelled ``sha256:`` as every published digest is (spicy-docs 0.52.0), and
    complete extraction result. Concurrent
    equal-byte downloads can both extract before the cache is filled; they keep
    the first cached result. The cache lasts one call, so it needs no version key.
    """
    from spicy_docs.schemas.tables import bytes_digest

    by_digest: dict[str, PdfTextResult] = {}
    lock = Lock()

    def _one(url: str) -> tuple[str, _PdfAttempt]:
        data = fetch(url)
        if data is None:
            return url, _PdfAttempt(None, None)
        digest = bytes_digest(data)
        with lock:
            result = by_digest.get(digest)
        if result is None:
            result = extract(data)
            with lock:
                result = by_digest.setdefault(digest, result)
        return url, _PdfAttempt(digest, result)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return dict(executor.map(_one, urls))


def _combine(results: list[_PdfAttempt]) -> tuple[str | None, str]:
    """Combine one row's per-PDF results into ``(text_or_None, status)``.

    A row can have more than one PDF rendition; their texts are concatenated.
    The combined status is ``ok`` if any PDF yielded text, otherwise ``error``
    > ``encrypted`` > ``empty`` in that order of informativeness.
    """
    texts = [result.text for result in results if result.text]
    if texts:
        return PAGE_SEPARATOR.join(texts), PdfTextStatus.OK.value
    statuses = [result.status for result in results]
    for candidate in (PdfTextStatus.ERROR.value, PdfTextStatus.ENCRYPTED.value, PdfTextStatus.EMPTY.value):
        if candidate in statuses:
            return None, candidate
    return None, PdfTextStatus.EMPTY.value


def _pdf_text_updates(
    df: pl.DataFrame,
    *,
    id_col: str,
    url_cols: list[str],
    urls_fn: UrlsFn,
    fetch: FetchFn,
    extract: ExtractFn,
    limit: int | None,
    max_workers: int,
    overwrite: bool,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Fetch + extract PDF text for a frame's candidate rows.

    Returns ``(updates, stats)`` where ``updates`` is an
    ``(id_col, _new_text, _new_status, _new_pdf_results)`` frame of every row processed (including
    empty/encrypted/error, so a re-run skips them). Shared core of both write
    paths: :func:`_enrich_with_pdf_text` joins it onto the whole frame; the
    catalog path upserts exactly these rows. Requires ``text_extraction_status``
    to be present on ``df`` (the caller ensures it or SELECTs it).
    """
    candidates = df.select(id_col, *url_cols, "text_extraction_status").unique(subset=id_col, keep="first")

    work: list[tuple[str, list[str]]] = []
    for row in candidates.iter_rows(named=True):
        row_id = row[id_col]
        if row_id is None:
            continue
        if not overwrite and row["text_extraction_status"] is not None:
            continue
        urls = urls_fn(row)
        if urls:
            work.append((row_id, urls))

    if limit is not None:
        work = work[:limit]

    stats = {"selected": len(work), "ok": 0, "empty": 0, "encrypted": 0, "error": 0}
    update_schema = {id_col: pl.Utf8, "_new_text": pl.Utf8, "_new_status": pl.Utf8, "_new_pdf_results": pl.Utf8}
    empty = pl.DataFrame(schema=update_schema)
    if not work:
        logger.info("No PDF attachments to enrich")
        return empty, stats

    unique_urls = sorted({url for _, urls in work for url in urls})
    logger.info(
        "Enriching {} rows with PDF text: {} distinct URLs ({} workers)...", len(work), len(unique_urls), max_workers
    )
    per_url = _extract_unique(unique_urls, fetch, extract, max_workers)

    ids: list[str] = []
    texts: list[str | None] = []
    statuses: list[str] = []
    diagnostics: list[str] = []
    for row_id, urls in work:
        text, status = _combine([per_url[url] for url in urls])
        ids.append(row_id)
        texts.append(text)
        statuses.append(status)
        diagnostics.append(json.dumps([per_url[url].diagnostic(url) for url in urls]))
        stats[status] = stats.get(status, 0) + 1

    updates = pl.DataFrame(
        {id_col: ids, "_new_text": texts, "_new_status": statuses, "_new_pdf_results": diagnostics},
        schema=update_schema,
    )
    logger.info(
        "PDF enrichment: {} ok, {} empty, {} encrypted, {} error",
        stats["ok"],
        stats["empty"],
        stats["encrypted"],
        stats["error"],
    )
    return updates, stats


def _enrich_with_pdf_text(
    df: pl.DataFrame,
    *,
    id_col: str,
    url_cols: list[str],
    urls_fn: UrlsFn,
    fetch: FetchFn,
    extract: ExtractFn,
    limit: int | None,
    max_workers: int,
    overwrite: bool,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Generic core: fill ``text_content`` / ``text_extraction_status`` for the
    rows of ``df`` whose attachments (via ``urls_fn``) include a PDF.

    Each unique ``id_col`` is processed once. Unless ``overwrite`` is set, rows
    that already have a ``text_extraction_status`` are skipped, so repeated runs
    are incremental.
    """
    df = with_text_columns(df)
    updates, stats = _pdf_text_updates(
        df,
        id_col=id_col,
        url_cols=url_cols,
        urls_fn=urls_fn,
        fetch=fetch,
        extract=extract,
        limit=limit,
        max_workers=max_workers,
        overwrite=overwrite,
    )
    if updates.is_empty():
        return df, stats

    return apply_text_updates(df, updates, id_col), stats


def enrich_documents_with_pdf_text(
    df: pl.DataFrame,
    *,
    fetch: FetchFn = fetch_pdf_bytes,
    extract: ExtractFn = extract_pdf_text,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Fill ``text_content`` / ``text_extraction_status`` for PDF documents."""
    return _enrich_with_pdf_text(
        df,
        id_col="document_id",
        url_cols=["attachments_json", "file_url"],
        urls_fn=lambda row: pdf_urls_for_document(_opt_str(row["attachments_json"]), _opt_str(row["file_url"])),
        fetch=fetch,
        extract=extract,
        limit=limit,
        max_workers=max_workers,
        overwrite=overwrite,
    )


def enrich_comments_with_pdf_text(
    df: pl.DataFrame,
    *,
    fetch: FetchFn = fetch_pdf_bytes,
    extract: ExtractFn = extract_pdf_text,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Fill ``text_content`` / ``text_extraction_status`` for comment PDF attachments."""
    return _enrich_with_pdf_text(
        df,
        id_col="comment_id",
        url_cols=["attachments_json"],
        urls_fn=lambda row: pdf_urls_for_comment(_opt_str(row["attachments_json"])),
        fetch=fetch,
        extract=extract,
        limit=limit,
        max_workers=max_workers,
        overwrite=overwrite,
    )


def _enrich_parquet_file(
    path: Path,
    enrich: Callable[[pl.DataFrame], tuple[pl.DataFrame, dict[str, int]]],
) -> dict[str, int]:
    """Read a Parquet file, enrich it in place, write it back, and return the stats."""
    df = pl.read_parquet(path)
    enriched, stats = enrich(df)
    enriched.write_parquet(path, compression="zstd")
    logger.info("Wrote {} ({} rows)", path, len(enriched))
    return stats


def enrich_documents_parquet(
    documents_path: Path,
    *,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> dict[str, int]:
    """Read ``documents.parquet``, enrich it in place, and write it back."""
    if not documents_path.exists():
        raise FileNotFoundError(f"{documents_path} not found; run the ETL first")
    return _enrich_parquet_file(
        documents_path,
        lambda df: enrich_documents_with_pdf_text(df, limit=limit, max_workers=max_workers, overwrite=overwrite),
    )


def enrich_comments_parquet(
    comments_path: Path,
    *,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> dict[str, int]:
    """Read a single comments Parquet file, enrich it in place, write it back."""
    if not comments_path.exists():
        raise FileNotFoundError(f"{comments_path} not found; run the ETL first")
    return _enrich_parquet_file(
        comments_path,
        lambda df: enrich_comments_with_pdf_text(df, limit=limit, max_workers=max_workers, overwrite=overwrite),
    )


def enrich_comment_partitions(
    partition_dir: Path,
    *,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> dict[str, int]:
    """Enrich every ``agency_code=*/*.parquet`` partition under ``partition_dir``.

    Comments are Hive-partitioned by agency; each partition is enriched and
    rewritten independently. ``limit`` (if given) is the total budget across
    all partitions, so a capped run won't silently process every agency.
    """
    parts = sorted(partition_dir.glob("agency_code=*/*.parquet"))
    if not parts:
        raise FileNotFoundError(f"No comment partitions found under {partition_dir}")

    totals = {"selected": 0, "ok": 0, "empty": 0, "encrypted": 0, "error": 0}
    remaining = limit
    for part in parts:
        if remaining is not None and remaining <= 0:
            logger.info("Reached --limit budget; {} partitions left unprocessed", len(parts) - parts.index(part))
            break
        stats = _enrich_parquet_file(
            part,
            lambda df: enrich_comments_with_pdf_text(df, limit=remaining, max_workers=max_workers, overwrite=overwrite),
        )
        for k, v in stats.items():
            totals[k] += v
        if remaining is not None:
            remaining -= stats["selected"]

    logger.info("Comment partition enrichment totals: {}", totals)
    return totals


def _enrich_comment_agency_in_catalog(
    con,
    record_type: RecordType,
    agency: str,
    *,
    fetch: FetchFn = fetch_pdf_bytes,
    extract: ExtractFn = extract_pdf_text,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> dict[str, int]:
    """PDF-enrich one agency's comment attachments directly in the catalog.

    The catalog counterpart of :func:`enrich_comment_partitions`: selects the
    agency's attachment-bearing comments with no ``text_extraction_status``,
    extracts their PDF text, and upserts the results with the per-agency
    DELETE+INSERT idiom of ``iceberg._merge`` (mirrors the durable derived-text
    backfill). Writing the catalog — the system of record — is what makes the
    fill survive the daily ``publish-comments-mirror`` regeneration. ``con`` is
    injected so this is testable against a local ``reg_catalog``-aliased DuckDB.
    """
    from spicy_regs.sources import iceberg

    from spicy_regs.sources.regulatory_catalog import processing_table

    ag = iceberg._sql_str(agency)
    tbl = processing_table(con, record_type, where=f"agency_code = '{ag}'")
    status_filter = "" if overwrite else "AND (text_extraction_status IS NULL OR text_extraction_status = '')"
    limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
    candidates = con.execute(
        f"""
        SELECT comment_id, docket_id, agency_code, attachments_json, text_extraction_status
        FROM {tbl}
        WHERE agency_code = '{ag}'
          AND attachments_json IS NOT NULL AND TRIM(attachments_json) NOT IN ('', '[]')
          {status_filter}
        {limit_sql}
        """
    ).pl()
    empty_stats = {"selected": 0, "ok": 0, "empty": 0, "encrypted": 0, "error": 0}
    if candidates.is_empty():
        return empty_stats

    updates, stats = _pdf_text_updates(
        candidates,
        id_col="comment_id",
        url_cols=["attachments_json"],
        urls_fn=lambda row: pdf_urls_for_comment(_opt_str(row["attachments_json"])),
        fetch=fetch,
        extract=extract,
        limit=limit,
        max_workers=max_workers,
        overwrite=overwrite,
    )
    if updates.is_empty():
        return stats

    # Durable per-agency upsert into the catalog via the shared helper, which
    # snapshots the affected rows into a self-contained temp table before the
    # INSERT — the invariant that keeps text_extraction_status writes from being
    # dropped on the R2 Data Catalog (see iceberg.upsert_comment_text / PR #117).
    # COALESCE keeps a pre-existing text_content when a non-ok result has no text.
    iceberg.upsert_comment_text(con, record_type, agency, updates)

    logger.info(
        "catalog[{}]: PDF-enriched {} row(s) (ok={}, empty={}, encrypted={}, error={})",
        agency,
        stats["selected"],
        stats["ok"],
        stats["empty"],
        stats["encrypted"],
        stats["error"],
    )
    return stats


def enrich_comments_catalog(
    record_type: RecordType,
    *,
    agencies: list[str] | None = None,
    fetch: FetchFn = fetch_pdf_bytes,
    extract: ExtractFn = extract_pdf_text,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
) -> dict[str, int]:
    """PDF-enrich comment attachments directly in the R2 Data Catalog (durable path).

    Iterates agencies (all in the table, or the given subset) and upserts each
    one's extracted text via :func:`_enrich_comment_agency_in_catalog`, honoring a
    global ``limit`` budget. Writing the catalog means the fill survives the daily
    ``publish-comments-mirror`` regeneration (a mirror-only enrich does not). Run
    ``publish-comments-mirror`` afterward to surface it. Returns aggregate stats.
    """
    from spicy_regs.sources import iceberg

    con = iceberg._connect_for_table(record_type)
    try:
        tbl = iceberg._qualified(record_type)
        if agencies:
            agency_list = [a.strip().upper() for a in agencies if a.strip()]
        else:
            agency_list = [
                r[0]
                for r in con.execute(
                    f"SELECT DISTINCT agency_code FROM {tbl} WHERE agency_code IS NOT NULL ORDER BY 1"
                ).fetchall()
            ]

        totals = {"selected": 0, "ok": 0, "empty": 0, "encrypted": 0, "error": 0}
        remaining = limit
        for agency in agency_list:
            if remaining is not None and remaining <= 0:
                logger.info("Reached --limit budget; {} agency bucket(s) left unprocessed", len(agency_list))
                break
            stats = _enrich_comment_agency_in_catalog(
                con,
                record_type,
                agency,
                fetch=fetch,
                extract=extract,
                limit=remaining,
                max_workers=max_workers,
                overwrite=overwrite,
            )
            for key, value in stats.items():
                totals[key] = totals.get(key, 0) + value
            if remaining is not None:
                remaining -= stats["selected"]

        logger.info("Catalog PDF enrichment totals: {} across {} agency bucket(s)", totals, len(agency_list))
        logger.info("Next: run publish-comments-mirror so the extracted text reaches the public read mirror.")
        return totals
    finally:
        con.close()


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Backfill documents/comments text_content from PDF attachments.")
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument(
        "--target",
        choices=["documents", "comments"],
        default="documents",
        help="Which dataset to enrich (default: documents)",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max rows to process this run")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--overwrite", action="store_true", help="Re-extract rows that already have a status")
    parser.add_argument(
        "--use-iceberg",
        action="store_true",
        help="Comments only: write extracted text into the R2 Data Catalog (durable; survives the "
        "mirror republish). Needs R2_CATALOG_*. Run publish-comments-mirror afterwards.",
    )
    parser.add_argument(
        "--agency",
        default=None,
        help="Comma-separated agency codes to limit the --use-iceberg comment run (default: all)",
    )
    args = parser.parse_args()

    # Durable comment path: write the catalog (the mirror is regenerated from it).
    # Documents are published as a whole-file monolith, not via the catalog, so
    # their in-place enrichment below is already durable — --use-iceberg is
    # comments-only.
    if args.use_iceberg:
        if args.target != "comments":
            parser.error("--use-iceberg applies to --target comments (documents are a monolith, not in the catalog)")
        from spicy_regs.schemas.regulations import RECORD_TYPES

        agencies = [a for a in args.agency.split(",")] if args.agency else None
        enrich_comments_catalog(
            RECORD_TYPES["comments"],
            agencies=agencies,
            limit=args.limit,
            max_workers=args.max_workers,
            overwrite=args.overwrite,
        )
        return

    if args.target == "documents":
        enrich_documents_parquet(
            args.output_dir / "documents.parquet",
            limit=args.limit,
            max_workers=args.max_workers,
            overwrite=args.overwrite,
        )
        return

    # Comments: prefer the Hive-partitioned layout, fall back to the monolithic file.
    partition_dir = args.output_dir / "comments" / "agency"
    if partition_dir.exists():
        enrich_comment_partitions(
            partition_dir,
            limit=args.limit,
            max_workers=args.max_workers,
            overwrite=args.overwrite,
        )
    else:
        enrich_comments_parquet(
            args.output_dir / "comments.parquet",
            limit=args.limit,
            max_workers=args.max_workers,
            overwrite=args.overwrite,
        )


def enrich_receipted_dataset(
    selected,
    destination: Path,
    *,
    generation_id: str,
    fetch: FetchFn = fetch_pdf_bytes,
    extract: ExtractFn = extract_pdf_text,
    limit: int | None = None,
    max_workers: int = 8,
    overwrite: bool = False,
    batch_size: int = 2000,
):
    """Locally enrich a selected native generation, qualifying retry state first.

    The shared receipt join must succeed before fetching any PDF. Each bounded
    batch uses the existing text/status/results update rules, then the subject
    and new receipts are written together. Every input row is carried forward;
    ``limit`` bounds extraction work across batches, not the output population.
    """
    from hashlib import file_digest
    from itertools import islice

    from spicy_regs.etl_receipts import ReceiptContext
    from spicy_regs.schemas import RECORD_TYPES
    from spicy_regs.transforms.regulations_receipts import read_internal, write_records

    if selected.dataset not in ("documents", "comments"):
        raise ValueError("PDF enrichment requires documents or comments")
    if batch_size <= 0 or limit is not None and limit < 0:
        raise ValueError("Invalid PDF batch size or limit")
    witnesses = []
    for path in (*selected.subjects, selected.receipts):
        with path.open("rb") as body:
            digest = file_digest(body, "sha256").hexdigest()
        witnesses.append(
            {
                "source_id": f"{selected.dataset}:{path.name}",
                "source_uri": None,
                "sha256": digest,
                "locator": None,
                "body_version": selected.generation_id,
            }
        )
    enrich = enrich_documents_with_pdf_text if selected.dataset == "documents" else enrich_comments_with_pdf_text
    stats = {"selected": 0, "ok": 0, "empty": 0, "encrypted": 0, "error": 0}

    def records():
        rows = iter(read_internal(selected))
        ordinal = 0
        while batch := list(islice(rows, batch_size)):
            frame = pl.DataFrame(batch, schema=RECORD_TYPES[selected.dataset].schema)
            remaining = None if limit is None else max(0, limit - stats["selected"])
            filled, counts = enrich(
                frame, fetch=fetch, extract=extract, limit=remaining, max_workers=max_workers, overwrite=overwrite
            )
            for key, value in counts.items():
                stats[key] += value
            for row in filled.iter_rows(named=True):
                yield (
                    row,
                    ReceiptContext(
                        generation_id,
                        f"pdf:{ordinal}",
                        "spicy-regs:pdf-receipt-v1",
                        witnesses,
                        {"prior_generation_id": selected.generation_id},
                    ),
                )
                ordinal += 1

    paths = write_records(selected.dataset, records(), destination, prior_receipts=[selected.receipts])
    return paths, stats


if __name__ == "__main__":
    main()
