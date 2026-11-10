#!/usr/bin/env python3
"""Rebuild the live native comments catalog in the regulations-native-v2 shape from the published generation.

A dry run unless ``--publish``: it reuses byte-verified local subject and receipt files, downloads missing files, pins the catalog
snapshot, proves the live 21 shared columns equal the published subject (per-key SHA-256), prints the plan (rows,
bytes, missing-input disk, spill cap, target namespace) and writes nothing to the catalog. ``--publish`` drops the v1 table and its receipts,
creates the v2 table, bulk-copies rows and receipts, writes one v2 marker and checks schemas, counts and a small sample of all subject/receipt fields.
``--verify-only`` runs just that readback against an already rebuilt catalog. Needs the R2 Data Catalog variables
and ``R2_PUBLIC_URL``. The sample is not full-corpus verification. Spill is capped separately; actual peak disk use is unmeasured.
See ``spicy_regs.sources.regulatory_catalog.rebuild_native``.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import shutil
import sys
import tempfile
from collections.abc import Sequence
from os import getenv
from pathlib import Path

from loguru import logger

from spicy_regs.duckdb_settings import ExportResources, memory_limit
from spicy_regs import etl_bulk
from spicy_regs.local_data import file_signature
from spicy_regs.runtime_bounds import stream_sha256
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg, publication
from spicy_regs.sources import regulatory_catalog as native
from spicy_regs.transforms.regulations_receipts import ReceiptInput

GIB = 2**30
#: The last comments generation published with receipts, pinned in docs/comments-receipt-archive.md (ss-juz8).
LAST_RECEIPT_GENERATION = "sha256:08e42cccdb0a57ff5e9fb7c0e0a825ff578c2e349e308289acaab86f766266a1"


def verified_local_member(member, path):
    """Reuse only an unchanged regular file whose bytes match the publication pin."""
    before = file_signature(path)
    with path.open('rb') as source:
        digest = 'sha256:' + stream_sha256(source)
    if before[2] != member.byte_size or digest != member.sha256 or file_signature(path) != before:
        raise ValueError(f'Local member differs from its publication pin; left untouched: {path}')


@contextmanager
def execution_scratch(work, resources):
    """Keep this standalone command's nested validators on the selected work volume."""
    directory = work / 'temporary'
    directory.mkdir(exist_ok=True)
    previous = tempfile.tempdir
    tempfile.tempdir = str(directory.resolve())
    try:
        with etl_bulk.bulk_limits(threads=resources.threads, memory_limit=resources.memory):
            yield
    finally:
        tempfile.tempdir = previous


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--work", type=Path, required=True, help="Directory for missing published inputs and temporary spill")
    parser.add_argument("--expect-artifact-digest", required=True,
                        help="The comments generation to rebuild from: its artifactDigest, or an 12+ hex-digit prefix of it")
    parser.add_argument("--publish", action="store_true", help="Write the rebuilt catalog; without it nothing is written")
    parser.add_argument("--dry-run", action="store_true", help="The default; refuses to combine with --publish")
    parser.add_argument("--verify-only", action="store_true", help="Only read an already rebuilt catalog back against the generation")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory", default="12GB")
    parser.add_argument("--spill", default="32GiB", help="DuckDB spill cap, not a disk reservation or measured peak")
    parser.add_argument("--subject-file", type=Path, help="Existing published comments member to verify and reuse in place")
    parser.add_argument("--receipts-file", type=Path, help="Existing published receipt member to verify and reuse in place")
    args = parser.parse_args(argv)
    if args.publish and (args.dry_run or args.verify_only):
        parser.error("--publish cannot combine with --dry-run or --verify-only")
    resources = ExportResources(memory=args.memory, threads=args.threads)
    spill = memory_limit(args.spill, "rebuild spill cap")
    base = (getenv("R2_PUBLIC_URL") or "").rstrip("/")
    if not base or not iceberg.is_configured():
        parser.error("R2_PUBLIC_URL and the R2 Data Catalog variables must be set")
    prefix = args.expect_artifact_digest.removeprefix("sha256:")
    if len(prefix) < 12 or any(c not in "0123456789abcdef" for c in prefix):
        parser.error("--expect-artifact-digest needs at least 12 lowercase hex digits")

    record_type = RECORD_TYPES["comments"]
    index = publication.load_index(base)
    owner = publication.table_owner(index, "comments.parquet")
    if owner is not None and "comments" not in owner[1].get("etlReceipts", {}).get("datasets", ()):
        raise SystemExit(
            f"The published comments generation {owner[1]['artifactDigest']} has no receipts: comments publish "
            "receipts-free since ss-juz8. This rebuild copies the live family's subjects and receipts and cannot read "
            f"another generation. The last generation with receipts is the pinned archive {LAST_RECEIPT_GENERATION} "
            "(docs/comments-receipt-archive.md), older than the live table; a seeding mode that writes one receipt "
            "per row of a receipts-free generation is not built.")
    if owner is None or not owner[1]["artifactDigest"].removeprefix("sha256:").startswith(prefix):
        raise SystemExit(f"The published comments generation is not {args.expect_artifact_digest}: "
                         f"{owner[1]['artifactDigest'] if owner else 'no family publishes comments.parquet'}")
    subject = publication.single_member(index, "comments.parquet")
    (receipts,) = publication.receipt_members(index, dataset="comments")
    if any(m.sha256 is None or m.byte_size is None or m.rows is None for m in (subject, receipts)):
        raise SystemExit("The published comments members are not byte-pinned; refusing")
    assert subject.byte_size is not None and receipts.byte_size is not None  # for the type checker, after the refusal
    args.work.mkdir(parents=True, exist_ok=True)
    inputs = args.work / "published"
    paths = {'comments.parquet': args.subject_file or inputs / 'comments.parquet',
             'etl_receipts.parquet': args.receipts_file or inputs / 'etl_receipts.parquet'}
    members = ((subject, 'comments.parquet', args.subject_file), (receipts, 'etl_receipts.parquet', args.receipts_file))
    missing_bytes = 0
    for member, name, explicit in members:
        path = paths[name]
        if path.exists() or path.is_symlink():
            verified_local_member(member, path)
        elif explicit is not None:
            raise ValueError(f'Explicit local member is missing: {path}')
        else:
            assert member.byte_size is not None  # All publication pins were checked above.
            missing_bytes += member.byte_size
    free = shutil.disk_usage(args.work).free
    print(f"Plan: rebuild {native.qualified(record_type)} (namespace {native.namespace()}) from generation "
          f"{owner[1]['artifactDigest']}\n"
          f"  rows {subject.rows:,}; subject {subject.byte_size / GIB:.1f} GiB; receipts {receipts.rows:,} rows, "
          f"{receipts.byte_size / GIB:.1f} GiB\n"
          f"  missing input bytes {missing_bytes:,} ({missing_bytes / GIB:.1f} GiB); free in {args.work}: {free / GIB:.1f} GiB\n"
          f"  configured spill cap {spill} (not reserved); peak disk use is unmeasured\n"
          f"  mode: {'PUBLISH (writes the live catalog)' if args.publish else 'verify-only' if args.verify_only else 'dry run (writes nothing)'}",
          flush=True)
    if free < missing_bytes:
        raise SystemExit("Not enough free disk for the missing published inputs; use a larger --work volume")

    inputs.mkdir(exist_ok=True)
    for member, name, _ in members:
        if not paths[name].exists():
            publication.fetch_member(base, member, paths[name], name)  # Checks bytes against the pin.
    pair = ReceiptInput("comments", (paths["comments.parquet"],), paths["etl_receipts.parquet"], owner[1]["artifactDigest"])

    with execution_scratch(args.work, resources):
        con = iceberg._connect()
        try:
            resources.configure(con, args.work / "spill")
            con.execute('SET max_temp_directory_size = ?', [spill])
            if not args.verify_only:
                state = native.rebuild_state(con, record_type)
                snapshot = iceberg._read_pair_snapshot(con, record_type, namespace=native.namespace()) if state == "v1" else None
                plan = native.rebuild_native(con, record_type, pair, expected_snapshot=snapshot,
                                             expected_prior=pair.subjects[0], dry_run=not args.publish)
                print(f"Checks passed; live state {plan['state']}, {plan['rows']:,} rows"
                      + ("" if args.publish else "; dry run, nothing written"), flush=True)
                if not args.publish:
                    return 0
            result = native.verify_rebuilt(con, record_type, pair)
            print(f"Sampled readback passed (not full-corpus verification): {result}", flush=True)
            logger.info("Next: dispatch check-comments-freshness; dispatch dedupe-comments-catalog with probe_partition=false")
        finally:
            con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
