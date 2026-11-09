"""CourtListener opinion bodies as Markdown: one pass over the opinions export, written id-sorted in id blocks.

The export is in no id order (a median 2,000-record stretch spans 96% of the id range;
``work/court-text-measure-20261030/extrapolate.json``), so a streaming write cannot sort it, and staging the whole
export first is the 73 GB of scratch behind decision 6. So the build has two phases:

1. Read the export once (``CourtListenerLocalDump``), convert each record in a process pool
   (``spicy_docs.sources.courtlistener.opinion_markdown.select_source``) and spill the converted rows, partitioned by
   ``opinion_id // SPILL_WIDTH``, one file per bucket held open for the pass, at zstd 1, each row group about
   ``SPILL_GROUP_BYTES`` of Markdown. O(N) in the export. ``_complete.json`` marks the spill whole.
2. Walk the spill buckets in id order: read one, sort it by numeric id, and hand the sorted table to
   ``court_bulk.write_bundle``, which writes its subject rows and receipts in one pass into the member of its
   ``opinion_block`` (``court_opinion_texts/opinion_block=<k>/part-000000.parquet``), at zstd 12, row groups of at
   most ``ROW_GROUP_TEXT_BYTES`` of Markdown, statistics only on the short columns. O(N log B) for bucket size B.
   Each block's receipts are committed as one part; a failed run resumes at the first uncommitted block.

Peak memory is O(largest spill bucket) in phase 2; peak disk is the whole spill at the end of phase 1, shrinking as
phase 2 deletes each committed block's buckets while the members grow.

Readers compute a row's member from its id: ``opinion_block = opinion_id // OPINION_BLOCK_WIDTH`` (stored as decimal
text, as ``opinion_id`` is), so a point read names one member. The width is chosen from the whole 2026-06-30 export
(``lengths-full.parquet``, scaled by the 2026-10-30 Markdown size model): 76 blocks whose largest is projected at
0.84 GB. Spill buckets are a third of a block so the heaviest one (1.99 GB of source HTML) sorts in memory.

Receipts point at the converted value and never copy it (owner, 2026-10-31): the export's file witness (its
SHA-256), the record's id, ``source_column`` (the rendering converted) and ``source_sha256`` (of that rendering's
value), with the converter's identity in the diagnostics. A rendering the converter refuses becomes a refused
receipt carrying the same pointer, never an empty text. An opinion without text is a row with ``text_source``
'none' and NULL Markdown; ``court_opinion_pdf_extractions`` joins it by ``opinion_id`` where a PDF was extracted.

The run receipt ``court_opinion_texts-run.json`` is written last and marks the build done. A run that stopped
before it resumes on the same command: from the spill if phase 1 finished, at the first uncommitted block in
phase 2, and by re-joining the committed parts after that (each block's routed-row count is kept with its part).

A run bounded by ``max_rows`` reads the export's head, writes a ``bounded`` run receipt beside its output and never
selects or admits a generation.

Measured 2026-10-30 on the export's first 100,000 records (2.36 GB of renderings): 216 s wall, 124 s phase 1 and
92 s phase 2, process tree peak 6.7 GB RSS with 3 workers, 0.60 GB of spill, 75 members of 415 MB in all with
footers of about 0.9 KB per row group (about 440 KB at a full member's ~490 groups). A point read over HTTP takes 4
range requests on the member computed from the id, 6 through the whole member list with ``opinion_block`` filtered,
and 154 cold with only ``opinion_id`` filtered. On the same rows reshuffled, DuckDB's ``COPY ... ORDER BY`` with
``PARTITION_BY`` and ``ROW_GROUP_SIZE_BYTES`` was 8% faster than the spill and sort but wrote members out of id order
(byte-sized row groups need ``preserve_insertion_order=false``) with row groups of up to 54 MB of Markdown, so the
sort stays here. Export-order members with a sorted id index (a key-to-position sidecar, measured then) skip the spill and
sort (1.1 GB peak RSS against 3.7 GB for the comparison's in-memory sort) at the same write time, but are 6% larger
and a point read takes 9 requests.

Re-measured on the same 100,000 rows fed in shuffled 500-row tasks (``work/scripts/phase2_compare.py``): the spill is
209 files of 808 row groups (``write_dataset`` without ``min_rows_per_group``: 11,916 files of 15,419 groups at a
256 open-file limit) for 0.29 GB of RSS over its input. Phase 2 spends 80% of its time in the zstd-12 member writes
(``ParquetWriter.write_table``); handing ``write_bundle`` the sorted table instead of a temporary file saved about 10%
of phase 2 for 0.5 GB more peak RSS (3.4 GB).
"""

from __future__ import annotations

import hashlib
import json
import os
import resource
import shutil
import sys
import time
from collections import Counter, deque
from collections.abc import Callable, Generator, Iterator
from contextlib import closing, nullcontext
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from spicy_docs.sources.courtlistener import opinion_markdown

from spicy_regs import etl_bulk
from spicy_regs.court_receipts import POLICIES, _refusal, _write_bulk_pair, file_witness
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, ReceiptContext
from spicy_regs.transforms.table_merge import Partitioning

DATASET = "court_opinion_texts"
#: Published: the member holding opinion ``N`` is ``opinion_block = N // OPINION_BLOCK_WIDTH``.
OPINION_BLOCK_WIDTH = 150_000
#: Ids per spill bucket, the unit sorted in memory; a divisor of the block width so buckets never straddle a member.
SPILL_WIDTH = 50_000
#: Uncompressed Markdown bytes per subject row group at most.
ROW_GROUP_TEXT_BYTES = 8 << 20
#: Subject columns with Parquet statistics; the Markdown's would only bloat the footer.
STATISTICS = ["opinion_id", "cluster_id", "date_modified", "opinion_block", "text_source", "converter_version"]
#: Export columns read: the identity, the subject's native fields and the renderings ``select_source`` orders.
SOURCE_COLUMNS = ("id", "cluster_id", "date_modified", "html_with_citations", "html", "plain_text")
PARTITIONING = Partitioning("opinion_block", "opinion_id", lambda opinion_id: str(int(opinion_id) // OPINION_BLOCK_WIDTH))
PROCESSOR = "spicy_regs/courts/opinion-texts/1"
#: Markdown bytes buffered per spill bucket before it is written as one row group: the 2026-06-30 export's ids
#: (largest 11,349,507) fill 227 buckets (``work/court-text-measure-20261030/lengths-full.parquet``), so at most
#: about 0.45 GB is buffered. ``pyarrow.dataset.write_dataset`` wrote about 2.4-row groups per 500-row task without
#: ``min_rows_per_group`` and peaked 1.4 GB higher with it (``review-fix/spill-writer-*.json``).
SPILL_GROUP_BYTES = 2 << 20
#: Records per conversion task, so a task's HTML stays in the tens of MB.
_TASK_ROWS = 500
_SPILL_COMPLETE = "_complete.json"
_RUN = f"{DATASET}-run.json"
_REFUSED = "_refused.parquet"
_RECEIPT_PARTS = "_receipts"
_BUCKET = "spill_bucket"
_SPILL_SCHEMA = pa.schema([
    *((name, pa.string()) for name in POLICIES[DATASET].subject_schema.names),
    ("source_column", pa.string()), ("source_sha256", pa.string()), ("dump_date", pa.string()), (_BUCKET, pa.int32()),
])

Converter = Callable[[dict], Any]


@dataclass
class _Converted:
    batch: pa.RecordBatch
    refused: list[dict]
    source_bytes: int
    cpu_seconds: float
    by_source: Counter


def _convert(rows: list[dict], dump_date: str, convert: Converter | None, version: str) -> _Converted:
    """One task: each record's Markdown row, or a refusal pointer for a rendering the converter refuses."""
    started = time.process_time()
    if convert is None:
        convert = opinion_markdown.select_source
    out: dict[str, list] = {name: [] for name in _SPILL_SCHEMA.names}
    refused, source_bytes = [], 0
    for row in rows:
        opinion_id = row["id"]
        try:
            text = convert(row)
        except ValueError as error:  # OpinionMarkdownError is a ValueError
            column = next((c for c in ("html_with_citations", "html", "plain_text") if (row.get(c) or "").strip()), None)
            value = (row.get(column) or "").encode() if column else b""
            refused.append({"opinion_id": opinion_id, "cluster_id": row["cluster_id"], "source_column": column,
                            "source_sha256": "sha256:" + hashlib.sha256(value).hexdigest(), "error": error})
            continue
        column = text.text_source if text.text_source != "none" else None
        value = (row.get(column) or "").encode() if column else b""
        source_bytes += len(value)
        out["opinion_id"].append(opinion_id)
        out["cluster_id"].append(row["cluster_id"])
        out["date_modified"].append(row["date_modified"])
        out["text_source"].append(text.text_source)
        out["markdown"].append(text.markdown if column else None)
        out["converter_version"].append(version)
        out["opinion_block"].append(PARTITIONING.value(opinion_id))
        out["source_column"].append(column)
        out["source_sha256"].append("sha256:" + hashlib.sha256(value).hexdigest() if column else None)
        out["dump_date"].append(dump_date)
        out[_BUCKET].append(int(opinion_id) // SPILL_WIDTH)
    batch = pa.RecordBatch.from_pydict(out, schema=_SPILL_SCHEMA)
    return _Converted(batch, refused, source_bytes, time.process_time() - started, Counter(out["text_source"]))


def byte_segments(limit: int = ROW_GROUP_TEXT_BYTES) -> Callable[[pa.Table], Iterator[pa.Table]]:
    """Cut a sorted batch into consecutive slices of at most ``limit`` Markdown bytes (one row may exceed it)."""

    def segments(table: pa.Table) -> Iterator[pa.Table]:
        sizes = pc.fill_null(pc.call_function("binary_length", [table["markdown"].cast(pa.binary())]), 0).to_pylist()
        start, total = 0, 0
        for index, size in enumerate(sizes):
            if index > start and total + size > limit:
                yield table.slice(start, index - start)
                start, total = index, 0
            total += size
        if start < table.num_rows:
            yield table.slice(start, table.num_rows - start)

    return segments


def _raise_open_files(needed: int) -> None:
    """Lift the soft open-file limit to ``needed`` (the spill holds a file per bucket open), or refuse."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft >= needed:
        return
    if hard != resource.RLIM_INFINITY and hard < needed:
        raise RuntimeError(f"open-file hard limit {hard} is below the {needed} the spill writer keeps open")
    resource.setrlimit(resource.RLIMIT_NOFILE, (needed, hard))


def _member_writer(path: Path) -> pq.ParquetWriter:
    path.parent.mkdir(parents=True, exist_ok=True)
    return pq.ParquetWriter(path, POLICIES[DATASET].subject_schema, compression="zstd", compression_level=12,
                            write_statistics=STATISTICS)


def _rss_bytes() -> int:
    """This process's peak RSS in bytes (``ru_maxrss`` is bytes on macOS, KiB on Linux)."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def build_court_opinion_texts(
    local_file: Path,
    output_dir: Path,
    *,
    dump_date: date,
    generation_id: str | None = None,
    max_rows: int | None = None,
    workers: int = 3,
    convert: Converter | None = None,
    parse_workers: int = 4,
    row_group_text_bytes: int = ROW_GROUP_TEXT_BYTES,
    source_id: str | None = None,
) -> dict:
    """Write ``output_dir/court_opinion_texts/`` and ``output_dir/etl_receipts.parquet``; return the run receipt.

    ``workers`` conversion processes (0: convert in this process); ``convert`` replaces the SpicyDocs converter
    (tests); ``max_rows`` bounds the read to the export's head; ``source_id`` names the export in the witness when
    ``local_file`` is a content-addressed raw object rather than the publisher's file name.

    A rerun into the same directory after a failure resumes: a complete spill (its ``_complete.json`` written last)
    is not read again, phase 2 restarts at the first block whose receipts were not committed, and a run that stopped
    after the last block re-joins the receipts and writes the run receipt. The rerun must name the same export,
    converter and bound; it reuses the first run's generation id. A directory holding a run receipt is done.
    """
    table_dir = output_dir / DATASET
    spill_dir = output_dir / f".{DATASET}-spill"
    receipt_path = output_dir / "etl_receipts.parquet"
    run_path = output_dir / _RUN
    if run_path.exists():
        raise FileExistsError(f"{run_path} exists; build into an empty directory")
    witness = file_witness(local_file, source_id=source_id)
    identity = opinion_markdown.converter_identity() if convert is None else {"converter_version": "test"}
    marker = spill_dir / _SPILL_COMPLETE
    resumed = marker.exists()
    if resumed:
        spill = json.loads(marker.read_text())
        expected = {"witness": witness, "converter": identity, "bounded_rows": max_rows, "dump_date": dump_date.isoformat()}
        if {key: spill[key] for key in expected} != json.loads(json.dumps(expected)):
            raise ValueError(f"{spill_dir} was spilled from another export, converter or bound; remove it to rebuild")
        if generation_id not in (None, spill["generation_id"]):
            raise ValueError(f"{spill_dir} belongs to generation {spill['generation_id']}")
    else:
        for path in (table_dir, spill_dir, receipt_path):
            if path.exists():
                raise FileExistsError(f"{path} exists without a complete spill; build into an empty directory")
        output_dir.mkdir(parents=True, exist_ok=True)
        spill = _spill(local_file, spill_dir, generation_id=generation_id or uuid4().hex, witness=witness,
                       identity=identity, dump_date=dump_date, max_rows=max_rows, workers=workers, convert=convert,
                       parse_workers=parse_workers, work_dir=output_dir)
    phase2 = time.perf_counter()
    diagnostics = _diagnostics(spill)
    routed = _write_members(spill, spill_dir, table_dir, receipt_path, diagnostics, row_group_text_bytes)
    run = {
        "dataset": DATASET, "generation_id": spill["generation_id"], "dump_date": spill["dump_date"],
        "witness": witness, "status": "bounded" if max_rows is not None else "complete",
        "completed_read": spill["completed_read"], **spill["counts"],
        "rows_by_text_source": spill["rows_by_text_source"], "refused_by_text_source": spill["refused_by_text_source"],
        "rows_routed_to_row_function": routed, "spill_bytes": spill["spill_bytes"],
        "spill_row_groups": spill["spill_row_groups"], "spill_files": spill["spill_files"],
        "resumed": resumed, "phase1_seconds": spill["phase1_seconds"],
        "phase2_seconds": round(time.perf_counter() - phase2, 1),
        "max_rss_main_process_bytes": _rss_bytes(), "diagnostics": diagnostics,
    }
    # The run receipt commits the build; the spill (and the resume state in it) goes only after it is in place.
    pending = run_path.with_name(f".{run_path.name}.tmp")
    pending.write_text(json.dumps(run, indent=2, sort_keys=True, default=str) + "\n")
    pending.rename(run_path)
    shutil.rmtree(spill_dir)
    return run


def _diagnostics(spill: dict) -> dict:
    return {"converter": spill["converter"], "opinion_block_width": OPINION_BLOCK_WIDTH,
            "bounded_rows": spill["bounded_rows"]}


def _spill(local_file: Path, spill_dir: Path, *, generation_id: str, witness: dict, identity: dict, dump_date: date,
           max_rows: int | None, workers: int, convert: Converter | None, parse_workers: int, work_dir: Path) -> dict:
    """Phase 1: one pass over the export into id buckets, refusals as receipts; ``_complete.json`` is written last."""
    from spicy_docs.sources.courtlistener.local import CourtListenerLocalDump

    started = time.perf_counter()
    edition = dump_date.isoformat()
    spill: dict[str, Any] = {"generation_id": generation_id, "witness": witness, "converter": identity, "bounded_rows": max_rows,
             "dump_date": edition}
    dump = CourtListenerLocalDump(local_file, columns=list(SOURCE_COLUMNS), parse_workers=parse_workers,
                                  work_dir=work_dir)
    counts = {"rows_read": 0, "refused": 0, "source_bytes": 0, "convert_cpu_seconds": 0.0}
    refusals: list[dict] = []
    by_source: Counter = Counter()

    def tasks() -> Iterator[list[dict]]:
        # Closing the reader's generator early (a bounded run) removes its pieces and stops its threads.
        with closing(cast(Generator[pa.RecordBatch], dump.iter_batches())) as batches:
            for batch in batches:
                rows = batch.to_pylist()
                if max_rows is not None:
                    rows = rows[: max_rows - counts["rows_read"]]
                counts["rows_read"] += len(rows)
                for start in range(0, len(rows), _TASK_ROWS):
                    yield rows[start: start + _TASK_ROWS]
                if max_rows is not None and counts["rows_read"] >= max_rows:
                    return

    def converted(pool: ProcessPoolExecutor | None) -> Iterator[pa.RecordBatch]:
        running: deque[Future] = deque()
        version = identity["converter_version"]

        def tally(result: _Converted) -> pa.RecordBatch:
            refusals.extend(result.refused)
            counts["source_bytes"] += result.source_bytes
            counts["convert_cpu_seconds"] += result.cpu_seconds
            by_source.update(result.by_source)
            return result.batch

        for task in tasks():
            if pool is None:
                yield tally(_convert(task, edition, convert, version))
                continue
            running.append(pool.submit(_convert, task, edition, convert, version))
            if len(running) >= 2 * workers:  # bounded in flight: a task holds tens of MB of HTML
                yield tally(running.popleft().result())
        while running:
            yield tally(running.popleft().result())

    with ProcessPoolExecutor(workers) if workers else nullcontext() as pool:
        _write_spill(converted(pool), spill_dir)
    if max_rows is None and not dump.completed:
        raise RuntimeError(f"{local_file.name} was not read to its end")
    receipts = []
    for ordinal, refusal in enumerate(refusals):
        error = refusal.pop("error")
        context = ReceiptContext(generation_id, f"{DATASET}:convert:{edition}:{ordinal}", PROCESSOR, [witness],
                                 _diagnostics(spill))
        receipts.append(_refusal(POLICIES[DATASET], context, error, {**refusal, "dump_date": edition}))
    # Refusals follow every accepted receipt, as the row writer orders them; they carry the pointer only.
    pq.write_table(pa.Table.from_pylist(receipts, schema=RECEIPT_SCHEMA), spill_dir / _REFUSED, compression="zstd")
    files = list(spill_dir.glob(f"{_BUCKET}=*.parquet"))
    counts["refused"] = len(refusals)
    spill.update(
        completed_read=dump.completed, counts=counts, rows_by_text_source=dict(sorted(by_source.items())),
        refused_by_text_source=dict(sorted(Counter(str(r["source_column"]) for r in refusals).items())),
        spill_bytes=sum(p.stat().st_size for p in files), spill_files=len(files),
        spill_row_groups=sum(pq.ParquetFile(p).metadata.num_row_groups for p in files),
        phase1_seconds=round(time.perf_counter() - started, 1),
    )
    (spill_dir / _SPILL_COMPLETE).write_text(json.dumps(spill, indent=2, sort_keys=True, default=str) + "\n")
    return spill


def _write_spill(batches: Iterator[pa.RecordBatch], spill_dir: Path) -> None:
    """Route converted rows to ``spill_bucket=<k>.parquet``, each bucket's rows buffered to ``SPILL_GROUP_BYTES``.

    Every bucket's writer stays open for the pass (the export is in no id order, so every bucket is written
    throughout), so each bucket is one file. O(N log T) for tasks of T rows.
    """
    spill_dir.mkdir()
    schema = _SPILL_SCHEMA.remove(_SPILL_SCHEMA.get_field_index(_BUCKET))
    writers: dict[int, pq.ParquetWriter] = {}
    buffered: dict[int, tuple[list[pa.Table], int]] = {}

    def flush(bucket: int) -> None:
        tables, _ = buffered.pop(bucket)
        if bucket not in writers:
            _raise_open_files(len(writers) + 256)
            writers[bucket] = pq.ParquetWriter(spill_dir / f"{_BUCKET}={bucket}.parquet", schema,
                                               compression="zstd", compression_level=1)
        writers[bucket].write_table(pa.concat_tables(tables))

    try:
        for batch in batches:
            table = pa.Table.from_batches([batch])
            order = pc.call_function("sort_indices", [table[_BUCKET]])
            keys = table[_BUCKET].take(order).to_pylist()
            sizes = pc.fill_null(pc.call_function("binary_length", [table["markdown"].cast(pa.binary())]), 0)
            sizes = sizes.take(order).to_pylist()
            table = table.drop_columns([_BUCKET])
            start = 0
            for index in range(1, len(keys) + 1):
                if index < len(keys) and keys[index] == keys[start]:
                    continue
                # A copy of the bucket's rows: a slice would pin the whole task's buffers until the bucket flushes.
                tables, size = buffered.get(keys[start], ([], 0))
                tables.append(table.take(order.slice(start, index - start)))
                buffered[keys[start]] = (tables, size + sum(sizes[start:index]))
                if buffered[keys[start]][1] >= SPILL_GROUP_BYTES:
                    flush(keys[start])
                start = index
        for bucket in list(buffered):
            flush(bucket)
    finally:
        for writer in writers.values():
            writer.close()


def _write_members(spill: dict, spill_dir: Path, table_dir: Path, receipt_path: Path, diagnostics: dict,
                   row_group_text_bytes: int) -> int:
    """Phase 2: each block's buckets sorted by numeric id into its member; its receipts committed last, per block.

    A block is done once its receipt part is renamed into place; only then are its buckets deleted. So a failed run
    resumes at the first block without a part, and the spill shrinks as the members grow. Each block's count of rows
    the row function decided is written beside its part before the part is committed, so the total survives a
    resume. The parts are joined into ``receipt_path`` (pointers only, about 156 B a row) once every block is done,
    then the refusals; the join is repeated whole on a resume, so a run stopped mid-join or after it recovers.
    """
    parts = spill_dir / _RECEIPT_PARTS
    parts.mkdir(exist_ok=True)
    blocks: dict[int, list[Path]] = {}
    for bucket in spill_dir.glob(f"{_BUCKET}=*.parquet"):
        number = int(bucket.stem.partition("=")[2])
        blocks.setdefault(number * SPILL_WIDTH // OPINION_BLOCK_WIDTH, []).append(bucket)
    done = {int(p.stem) for p in parts.glob("*.parquet")}
    routed = sum(json.loads((parts / f"{block}.json").read_text())["rows_routed_to_row_function"] for block in done)
    for block in sorted(blocks.keys() | done):
        buckets = sorted(blocks.get(block, []), key=lambda p: int(p.stem.partition("=")[2]))
        part = parts / f"{block}.parquet"
        if block not in done:
            member_path = table_dir / f"opinion_block={block}" / "part-000000.parquet"
            member_path.unlink(missing_ok=True)  # a failed attempt's, never committed
            pending = parts / f".{block}.parquet.tmp"
            block_routed = 0
            with etl_bulk.new_receipt_writer(pending) as receipts, _member_writer(member_path) as member:
                for bucket in buckets:
                    table = pq.read_table(bucket)
                    table = table.take(pc.call_function("sort_indices", [pc.cast(table["opinion_id"], pa.int64())]))
                    block_routed += _write_bulk_pair(
                        DATASET, table, table_dir / ".unused.parquet", pending,
                        generation_id=spill["generation_id"], witnesses=[spill["witness"]], processor=PROCESSOR,
                        diagnostics=diagnostics, attempt_prefix=f"{spill['dump_date']}:{bucket.stem}",
                        receipt_writer=receipts, subject_writer=member, segments=byte_segments(row_group_text_bytes),
                    )
                    del table
            (parts / f"{block}.json").write_text(json.dumps({"rows_routed_to_row_function": block_routed}) + "\n")
            pending.rename(part)
            routed += block_routed
        for bucket in buckets:
            bucket.unlink()
    joined = receipt_path.with_name(f".{receipt_path.name}.tmp")
    with etl_bulk.new_receipt_writer(joined) as receipts:
        for part in [*sorted(parts.glob("*.parquet"), key=lambda p: int(p.stem)), spill_dir / _REFUSED]:
            with pq.ParquetFile(part) as file:
                for group in range(file.num_row_groups):
                    receipts.write_table(file.read_row_group(group))
    joined.rename(receipt_path)
    return routed


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Build court_opinion_texts from a retained opinions export.")
    parser.add_argument("export", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--dump-date", type=date.fromisoformat, required=True)
    parser.add_argument("--max-rows", type=int)
    # Three: with the reader's four parse threads the process tree peaked at 6.8 GB on the 100,000-record head
    # (7.3 GB with four), under the 8 GB budget with headroom.
    parser.add_argument("--workers", type=int, default=min(3, os.cpu_count() or 1))
    parser.add_argument("--source-id", help="the publisher's file name, when the export is a raw archive object")
    args = parser.parse_args(argv)
    run = build_court_opinion_texts(args.export, args.output_dir, dump_date=args.dump_date, max_rows=args.max_rows,
                                    workers=args.workers, source_id=args.source_id)
    print(json.dumps(run, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
