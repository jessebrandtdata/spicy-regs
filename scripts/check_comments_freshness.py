#!/usr/bin/env python3
"""Verify that the public comments mirror is the verified publication of the current catalog.

The publisher owns content verification: before upload it checks unique IDs, index coverage of the monolith and
agency files and retention of every prior ID, then reads back each public file's SHA-256 into
``comments-publication.json``. This check binds the current state to that receipt and verifies content only where
the binding breaks:

* public: every receipt file must still have its stored version (ETag and size) at the public URL;
* catalog: a pair reached from the receipt's source snapshot through compaction alone holds the published rows.
  Otherwise the raw catalog's IDs and agency/docket/month counts must equal the public index, which fails when the
  catalog advanced past the publication. A deduplicating consumer view cannot establish physical catalog integrity.

Exit 0 when bound or verified, 1 on any mismatch or incomplete check.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from time import monotonic

import duckdb
import httpx

from spicy_regs.comments_health import check_comments
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.pipelines.comments_mirror import RECEIPT_KEY, receipt_files, receipt_source
from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg, r2
from spicy_regs.sources.regulatory_catalog import require_initialized

#: Remote Parquet scans wait on one ranged GET per column chunk; parallel requests, not CPU, set their pace.
SCAN_THREADS = 8


@contextmanager
def _stage(label: str):
    """Keep completed stages visible if the runner later times out."""
    started = monotonic()
    print(f"START {label}", flush=True)
    yield
    print(f"DONE {label}: {monotonic() - started:.1f}s", flush=True)


def read_receipt(base: str) -> dict:
    response = httpx.get(f"{base}/{RECEIPT_KEY}", follow_redirects=True, timeout=60, headers=r2._PUBLIC_HEADERS)
    response.raise_for_status()
    return response.json()


def public_drift(files: dict[str, dict], base: str, *, limit: int) -> list[str]:
    """Receipt files whose public stored version differs; any rewrite or removal changes the ETag."""
    def drift(entry: tuple[str, dict]) -> str | None:
        key, item = entry
        expected = {"etag": item["etag"], "bytes": item["bytes"]}
        actual = r2.public_object_version(f"{base}/{key}")
        return None if actual == expected else f"{key}: receipt {expected}, public {actual}"

    with ThreadPoolExecutor(max_workers=8) as pool:
        return [error for error in pool.map(drift, sorted(files.items())) if error][:limit]


def verify_catalog(base: str, agency: str | None, *, limit: int) -> list[str]:
    """Raw catalog IDs and every agency/docket/month count against the public index."""
    where = " WHERE agency_code = '" + agency.replace("'", "''") + "'" if agency else ""
    index = f"SELECT * FROM read_parquet('{base.replace(chr(39), chr(39) * 2)}/comments_index.parquet'){where}"
    con = iceberg._connect()
    try:
        load_public_http(con)
        con.execute(f"SET memory_limit='4GB'; SET threads={SCAN_THREADS}; SET preserve_insertion_order=false")
        with _stage("catalog: selected native table"):
            require_initialized(con, RECORD_TYPES["comments"])
        return check_comments(con, f"SELECT * FROM {iceberg._qualified(RECORD_TYPES['comments'])}{where}",
                              index, limit=limit)
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--agency", help="Verify this agency's catalog rows even when the snapshot is bound")
    parser.add_argument("--verify-content", action="store_true",
                        help="Verify all catalog rows even when the snapshot is bound")
    parser.add_argument("--surface", choices=("public", "both"), default="both",
                        help="public: bind only the public files (a v1 promotion checks its own catalog)")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    base = resolve_r2_base_url().rstrip("/")
    rt = RECORD_TYPES["comments"]
    try:
        with _stage("public: files match the publication receipt"):
            receipt = read_receipt(base)
            files = receipt_files(receipt)
            if files is None:
                print(f"FAIL public: {RECEIPT_KEY} does not state a valid comments publication", flush=True)
                return 1
            errors = public_drift(files, base, limit=args.limit)
        for error in errors:
            print(f"FAIL public: differs from the publication receipt: {error}", flush=True)
        if errors:
            return 1
        print(f"OK public: {len(files)} files match {RECEIPT_KEY} "
              f"({files['comments.parquet']['rows']:,} rows, sha256 {files['comments.parquet']['sha256'][:12]})",
              flush=True)

        if args.surface == "public":
            return 0
        source = receipt_source(receipt)
        with _stage("catalog: snapshot against the published source"):
            bound = source is not None and iceberg.rows_unchanged_since(rt, source)
        if bound and not (args.agency or args.verify_content):
            print(f"OK catalog: unchanged since published snapshot {source.snapshot_id} (compaction only)", flush=True)
            return 0
        reason = ("requested" if bound else
                  "receipt predates pair snapshots" if source is None else "catalog changed since publication")
        with _stage(f"catalog: IDs and index coverage ({reason})"):
            errors = verify_catalog(base, args.agency, limit=args.limit)
        for error in errors:
            print(f"FAIL catalog: {error}", flush=True)
        if errors:
            return 1
        print(f"OK catalog: unique IDs and complete index coverage{' for ' + args.agency if args.agency else ''}",
              flush=True)
        return 0
    except (duckdb.Error, httpx.HTTPError, ValueError, RuntimeError, OSError) as exc:
        print(f"FAIL verification incomplete: {exc!r}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
