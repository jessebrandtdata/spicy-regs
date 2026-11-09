#!/usr/bin/env python3
"""Verify that the comments users read are the verified publication of the current catalog.

The publisher owns content verification: before upload it checks unique IDs, index coverage of the monolith and
agency files and retention of every prior ID, then reads back each public file's SHA-256 into
``comments-publication.json`` with the catalog pair it exported and the generation it published. This check binds
the current state to that receipt and reads rows only where a binding breaks:

* public: every receipt file must still have its stored version (ETag and size) at its fixed URL;
* generation: ``publication.v2.json``'s comments family, which the website and MCP server read, must be the
  receipt's generation;
* catalog: the pair must be reachable from the receipt's source through compaction alone, under the declared schema.

Otherwise the raw catalog's IDs and agency/docket/month counts must equal both the fixed and the generation index,
which fails when the catalog advanced past the publication. A deduplicating consumer view cannot establish physical
catalog integrity. Counts cannot see a text-only change, so any comments-table commit other than compaction since the
receipt's snapshot fails as unpublished; the counts then print as diagnostics. The receipt table is shared by every
dataset's writers, so any write to it sends the run to the row path (~95 s, 2026-11-09; accepted). A receipt that
names no generation or catalog pair (written before the publisher recorded them; the receipt format number did not
change) binds nothing, so every run reads rows until a comments publication writes one.

Exit 0 when bound or verified, 1 on any mismatch or incomplete check.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from time import monotonic

import duckdb
import httpx

from spicy_regs.pipelines.comments_mirror import receipt_files, receipt_source
from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg, publication

#: One 429 or 5xx from the public host must not fail the night; SpicyDocs' backoff caps each wait.
PUBLIC_ATTEMPTS = 4
INDEX_KEY = "comments_index.parquet"


@contextmanager
def _stage(label: str):
    """Keep completed stages visible if the runner later times out."""
    started = monotonic()
    print(f"START {label}", flush=True)
    yield
    print(f"DONE {label}: {monotonic() - started:.1f}s", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--agency", help="Verify this agency's catalog rows even when everything is bound")
    parser.add_argument("--verify-content", action="store_true",
                        help="Verify all catalog rows even when everything is bound")
    parser.add_argument("--surface", choices=("public", "both"), default="both",
                        help="public: bind only the fixed public files (a v1 promotion checks its own catalog)")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    base = resolve_r2_base_url().rstrip("/")
    rt = RECORD_TYPES["comments"]
    try:
        with _stage("public: files match the publication receipt"):
            loaded = publication.load_comments_publication(base)
            files = receipt_files(loaded["receipt"]) if loaded else None
            if loaded is None or files is None:
                print("FAIL public: comments-publication.json does not state a complete comments publication",
                      flush=True)
                return 1
            errors = publication.public_version_drift({f"{base}/{key}": item for key, item in files.items()},
                                             attempts=PUBLIC_ATTEMPTS)
        for error in errors[:args.limit]:
            print(f"FAIL public: differs from the publication receipt: {error}", flush=True)
        if errors:
            return 1
        receipt = loaded["receipt"]
        print(f"OK public: {len(files)} files match {loaded['receipt_sha256'][:19]} "
              f"({files['comments.parquet']['rows']:,} rows)", flush=True)
        if args.surface == "public":
            return 0

        with _stage("generation: published family against the receipt"):
            index = publication.load_index(base)
            family = index["families"].get("comments")
            # Without a comments family, readers fall back to the fixed files bound above.
            generation_bound = family is None or (isinstance(receipt.get("generation"), dict) and
                                                  receipt["generation"].get("artifactDigest") == family["artifactDigest"])
        source = receipt_source(receipt)
        with _stage("catalog: snapshot against the published source"):
            subject_bound, receipts_bound = ((False, False) if source is None else
                                             iceberg.compacted_since(rt, source, declared_schema=True))
        if generation_bound and subject_bound and receipts_bound and not (args.agency or args.verify_content):
            print(f"OK: generation {family['artifactDigest'][:19] if family else 'none'} and catalog snapshot "
                  f"{source.snapshot_id} (compaction only) are the receipt's", flush=True)
            return 0

        # A text-only writer keeps every ID and count, so counts cannot clear a comments-table commit (below).
        unpublished = source is not None and not subject_bound
        reasons = [reason for reason, applies in (
            ("requested", args.agency or args.verify_content),
            ("receipt names no generation" if "generation" not in receipt else "generation differs from receipt",
             not generation_bound),
            ("receipt predates pair snapshots", source is None),
            ("comments table has unpublished catalog commits", unpublished),
            ("shared etl_receipts table changed since publication", source is not None and subject_bound
             and not receipts_bound)) if applies]
        indexes = {"fixed": [f"{base}/{INDEX_KEY}"]}
        generation = [f"{base}/{member.path}" for member in publication.table_members(index, INDEX_KEY)]
        if generation != indexes["fixed"]:
            indexes["generation"] = generation
        with _stage(f"catalog: IDs and coverage of {', '.join(indexes)} index ({'; '.join(reasons)})"):
            errors = iceberg.check_comments_against_indexes(rt, indexes, agency=args.agency, limit=args.limit)
        for error in errors:
            print(f"FAIL catalog: {error}", flush=True)
        if unpublished:
            print(f"FAIL catalog: unpublished catalog commits: the comments table has commits other than compaction "
                  f"since published snapshot {source.snapshot_id}; republish comments (the counts above are "
                  "diagnostic: a text-only change keeps them equal)", flush=True)
            return 1
        if errors:
            return 1
        print(f"OK catalog: unique IDs and complete coverage of the {' and '.join(indexes)} index"
              f"{' for ' + args.agency if args.agency else ''}", flush=True)
        return 0
    except ValueError as exc:
        print(f"FAIL: {exc}", flush=True)
        return 1
    except (duckdb.Error, httpx.HTTPError, RuntimeError, OSError) as exc:
        print(f"FAIL verification incomplete: {exc!r}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
