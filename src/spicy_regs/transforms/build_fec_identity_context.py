"""Build retained FEC identity/context subjects and shared ETL receipts locally.

The explicit manifest selects byte-pinned mapped tables or native source rows.
The latter calls maintained mappers. No network, current-record selection or
publication occurs. Each input is hashed once; its file signature guards the read.
"""

from __future__ import annotations

from bisect import bisect_left
from collections import defaultdict
import json
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

from .assemble_fec_query import TypedInput, hashed_input, unchanged_input
from .fec_identity_context_fields import REGISTRY
from .fec_identity_dispatch import prepare_identity_job, map_identity_record
from .fec_identity_receipts import IdentityReceiptWriter


def _may_hold(metadata, group, keep):
    """Whether a row group's collection_id statistics admit any of the sorted selected IDs.

    Only Arrow's writer is trusted: it drops oversized string statistics rather than
    truncating them, so its min/max are exact. Otherwise, or without statistics, decode.
    """
    if not (metadata.created_by or "").startswith("parquet-cpp-arrow"):
        return True
    group = metadata.row_group(group)
    names = [group.column(i).path_in_schema for i in range(group.num_columns)]
    statistics = group.column(names.index("collection_id")).statistics
    if statistics is None or not statistics.has_min_max:
        return True
    first = bisect_left(keep, statistics.min)  # keep is sorted: O(log k) per row group
    return first < len(keep) and keep[first] <= statistics.max


def _input_rows(item, base, *, collections=None):
    """Rows of one selected input, hashed once per run.

    ``collections`` keeps only rows of those collection IDs: row groups whose
    statistics exclude them are skipped, the rest filtered in Arrow before any
    row reaches Python; input order is kept. The file signature taken
    around the one hash refuses an input that changed during mapping.
    """
    path = Path(item["path"])
    if not path.is_absolute():
        path = base / path
    if path.is_symlink() or not path.is_file():
        raise ValueError("FEC input is not the selected regular-file bytes")
    pinned = hashed_input(TypedInput(path, item["sha256"], item["rows"]), "FEC input is not the selected regular-file bytes")
    keep = None if collections is None else sorted(collections)
    count = 0
    with pq.ParquetFile(path) as source:
        if source.metadata.num_rows != item["rows"]:
            raise ValueError("FEC input row membership differs from selection")
        for group in range(source.metadata.num_row_groups):
            if keep is not None and not _may_hold(source.metadata, group, keep):
                count += source.metadata.row_group(group).num_rows
                continue
            for batch in source.iter_batches(batch_size=2048, row_groups=[group], use_threads=False):
                count += batch.num_rows
                if keep is not None:
                    batch = batch.filter(pc.field("collection_id").isin(keep))
                yield from batch.to_pylist()
    if count != item["rows"]:
        raise ValueError("FEC input changed during mapping")
    unchanged_input(pinned, "FEC input changed during mapping")


def _emit_mapping(writer, tables, evidence, input_witness, source_input):
    """Emit mapped rows with their evidence, grouped once: O(rows + evidence) per mapping."""
    emitted = [table for table, rows in tables.items() if rows]
    for table in emitted:
        if table not in REGISTRY:
            raise ValueError(f"Context emitted another owned family: {table}")
    links = defaultdict(list)
    for row in evidence:
        links[row["target_table"], row["target_record_id"]].append(row)
    for table in emitted:
        for row in tables[table]:
            writer.emit(
                table,
                row,
                input_witness=input_witness,
                evidence=links.get((table, row.get("record_id")), []),
                source_input=source_input,
            )
    if "fec_record_evidence" in writer.policies:
        for row in evidence:
            writer.emit("fec_record_evidence", row, input_witness=input_witness)


def build_fec_identity_context(manifest, output_dir):
    """Consume a local version-1 selection, admitting every subject with receipts.

    A mapped input names its exact table. A source input supplies explicit jobs
    (kind plus native entry and optional retained header_row). Context inputs
    select collection IDs and are recorded as receipts only: no table is built
    from receipt context (ss-j4vz B11; the tables that were are rebuilt from their
    originals or retired).
    Unsupported native rows become refusal receipts; corrupt inputs abort the
    directory. No schema or namespace guessing is permitted.
    """
    manifest = Path(manifest)
    original = manifest.read_bytes()
    spec = json.loads(original)
    if set(spec) != {"version", "generation_id", "tables", "inputs"} or spec["version"] != 1:
        raise ValueError("Unsupported FEC identity build manifest")
    if not isinstance(spec["inputs"], list) or not spec["inputs"]:
        raise ValueError("FEC identity build needs explicit input membership")
    if not isinstance(spec["tables"], list) or len(set(spec["tables"])) != len(spec["tables"]):
        raise ValueError("FEC identity output selection repeats a table")
    with IdentityReceiptWriter(output_dir, generation_id=spec["generation_id"], tables=spec["tables"]) as writer:
        for item in spec["inputs"]:
            mode = item["mode"]
            witness = dict(
                source_id=item["table"],
                source_uri=None,
                sha256=item["sha256"],
                locator=None,
                body_version=None,
            )
            if mode == "mapped":
                for row in _input_rows(item, manifest.parent):
                    writer.emit(item["table"], row, input_witness=witness)
            elif mode == "source_records":
                if item["table"] != "fec_source_records":
                    raise ValueError("Native jobs require source records")
                jobs = {}
                for job in item["jobs"]:
                    prepared = prepare_identity_job(
                        job["kind"],
                        job["entry"],
                        source_generation_pin=item["source_generation_pin"],
                        header_row=job.get("header_row"),
                    )
                    if prepared.collection_id in jobs:
                        raise ValueError("Duplicate collection mapping selection")
                    jobs[prepared.collection_id] = prepared
                selected = set()
                for row in _input_rows(item, manifest.parent, collections=jobs):
                    selected.add(row["collection_id"])
                    try:
                        tables, evidence = map_identity_record(row, jobs[row["collection_id"]])
                    except (ValueError, KeyError, TypeError) as error:
                        writer.emit(
                            "fec_source_records",
                            row,
                            input_witness=witness,
                            outcome="refused",
                            diagnostic={"error": str(error), "kind": type(error).__name__},
                        )
                        continue
                    writer.emit("fec_source_records", row, input_witness=witness)
                    _emit_mapping(writer, tables, evidence, witness, None)
                if selected != set(jobs):
                    raise ValueError("Selected collection has no input observations")
            elif mode == "context":
                if item["table"] != "fec_collections":
                    raise ValueError("Context mapping requires collections")
                if "filing_feed" in item:
                    raise ValueError("The filing feed is built from its original, not from context parts")
                selected = set(item["collection_ids"])
                seen = set()
                for row in _input_rows(item, manifest.parent, collections=selected):
                    if row["collection_id"] in seen:
                        raise ValueError("Repeated context collection identity")
                    seen.add(row["collection_id"])
                    writer.emit("fec_collections", row, input_witness=witness)
                if seen != selected:
                    raise ValueError("Selected context membership is incomplete")
            else:
                raise ValueError(f"Unsupported identity build mode: {mode}")
        if manifest.read_bytes() != original:
            raise ValueError("FEC manifest changed during build")
    return tuple(
        Path(output_dir) / name
        for name in [
            *(t + ".parquet" for t in spec["tables"] if not REGISTRY[t]["receipt_only"]),
            "etl_receipts.parquet",
        ]
    )
