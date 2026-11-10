"""Re-read every Mirrulations comment once, keeping everything a later column decision could need (owner, 2026-09-28).

The catalog replaces a row only when its ``modify_date`` moves, and the manifest skips keys already read, so rows
ingested before a field was retained keep it NULL: the four comment-reference columns, ``subtype`` and
``duplicate_comments`` (``comment_fields_write.FILL_COLUMNS``), ``attachments_json`` wherever the row's first read
lacked the attachments, and the submitter's name, organization and category on every row read before the extract
mapped them (2026-06-15). One pass keeps, per object: its key and the GET's ETag and size; the whole thin-table row
through ``COMMENT.extract`` (one spelling), with the body as its SHA-256 and length; and every other stated attribute
as compact JSON (``attributes_json``: the keys the extract does not map, non-null only), which ``comment_attributes``
is built from.

Two phases, so the long read holds no lock and the catalog write holds it briefly:

* ``plan`` + ``read`` touch no catalog. ``plan`` splits the ETL manifest's comment keys into fixed chunks per agency;
  ``read`` fetches each chunk through the ETL's Mirrulations reader and writes one Parquet part per chunk. A part is
  written whole or not at all, so a rerun reads only the chunks that have no part, and a chunk with transport
  failures stays unwritten for the next run. Answers that are not transport failures (unreadable or empty objects)
  are kept in the chunk's journal line. A plan is named by the manifest digest and the record shape, so a changed
  shape is a new plan, never parts of two shapes stitched together.
* ``stage`` writes what ``prepare`` reads of the parts for one fill profile into one checked file a runner fetches
  (:func:`stage`, :func:`upload_staged`); ``prepare`` and ``write`` (``comment_fields_write``) then run under the
  catalog lock.
"""

from __future__ import annotations

import hashlib
import json
import resource as rusage
import shutil
import sys
import time
import zlib
from collections.abc import Iterator
from glob import glob
from pathlib import Path

import duckdb
import polars as pl
from cyclopts import App
from loguru import logger

from spicy_regs.pipelines.comment_fields_write import (
    BATCH_BYTES,
    HOST_COLUMNS,
    WRITE_RESOURCES,
    fetch_staged,
    prepare,
    read_columns,
    sync_journals,
    undo,
    write,
)
from spicy_regs.schemas import COMMENT

#: Keys per chunk: the resume unit, about 17 s of reads at the measured rate.
CHUNK_KEYS = 20_000
#: The part record's shape; a plan carries it, so a new shape is a new plan. s3 (2026-10-03): ``attachments_json`` as
#: SpicyDocs b19b092 extracts it, every attachment the record lists, withheld ones included. The s2 parts (the
#: 2026-09-28 read) hold the rule before it; under s2, ``plan`` on their workdir would add only the keys ingested since
#: and keep the rest, so the owner's full re-read (2026-10-03) would replace nothing. s4 (2026-11-10): as s3, each entry
#: ending with its ``attachment_id`` (SpicyDocs 0.85.0), so a read under it starts a plan of its own beside s3's.
#: :func:`require_record_shape` refuses to plan or read s4 under an extract that drops a withheld attachment or its id.
RECORD_SHAPE = "s4"
_PROBE_ATTACHMENT_ID = "0900006400000001"
#: A record listing one withheld attachment (no ``fileFormats``) with its id, which an s4 part keeps.
_WITHHELD = {"data": {"id": "PROBE-0000-0001", "attributes": {}}, "included": [
    {"id": _PROBE_ATTACHMENT_ID, "type": "attachments",
     "attributes": {"title": "withheld", "fileFormats": None, "restrictReasonType": "Other"}}]}
#: The ``data.attributes`` keys ``COMMENT.extract`` maps into the thin row; every other stated key is kept in
#: ``attributes_json``. ``test_extracted_attributes_are_the_keys_the_extract_reads`` derives this set independently.
EXTRACTED_ATTRIBUTES = frozenset({
    "agencyId", "category", "comment", "commentOn", "commentOnDocumentId", "docketId", "documentType",
    "duplicateComments", "firstName", "lastName", "modifyDate", "organization", "originalDocumentId", "postedDate",
    "receiveDate", "subtype", "title",
})
PART_SCHEMA = {
    "key": pl.Utf8, "etag": pl.Utf8, "size": pl.Int64,
    **{c: t for c, t in COMMENT.schema.items() if c not in (*HOST_COLUMNS, "comment")},
    "comment_sha256": pl.Utf8, "comment_length": pl.Int64, "attributes_json": pl.Utf8,
}


def part_row(keyed) -> dict:
    """One part row for a keyed Mirrulations payload: identity, the thin row, the body's digest, the other attributes."""
    row = COMMENT.extract(keyed.payload)
    body = row["comment"]
    attributes = (keyed.payload.get("data") or {}).get("attributes") or {}
    stated = {key: value for key, value in attributes.items() if key not in EXTRACTED_ATTRIBUTES and value is not None}
    return {
        "key": keyed.key, "etag": keyed.etag, "size": keyed.size,
        **{column: row[column] for column in PART_SCHEMA if column in row and column != "comment"},
        "comment_sha256": None if body is None else hashlib.sha256(body.encode()).hexdigest(),
        "comment_length": None if body is None else len(body),
        "attributes_json": json.dumps(stated, separators=(",", ":"), sort_keys=True, ensure_ascii=False),
    }


def require_record_shape(extract=None) -> None:
    """Refuse an extract that reads another shape than :data:`RECORD_SHAPE`: one that drops a withheld attachment
    (SpicyDocs before b19b092) or its id (before 0.85.0) would write s4 parts holding s2's or s3's ``attachments_json``."""
    listed = json.loads((extract or COMMENT.extract)(_WITHHELD)["attachments_json"] or "null")
    if not listed:
        raise RuntimeError(f"record shape {RECORD_SHAPE} keeps withheld attachments, and this environment's SpicyDocs "
                           "drops them: run from a branch that vendors SpicyDocs with b19b092 (0.54.0 or later)")
    if listed[0].get("attachment_id") != _PROBE_ATTACHMENT_ID:
        raise RuntimeError(f"record shape {RECORD_SHAPE} keeps each attachment's id, and this environment's SpicyDocs "
                           "drops it: run from a branch that vendors SpicyDocs 0.85.0 or later")


def _sql(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def plan(workdir: Path, manifest: Path, *, chunk_keys: int = CHUNK_KEYS) -> Path:
    """Add the manifest's comment keys that no earlier plan holds, as new fixed chunks; return the plan file.

    A plan is immutable and named by the manifest's digest, so a rerun on the same manifest is a no-op and a newer
    manifest adds only the keys ingested since. Chunks are contiguous in key order within an agency directory.
    """
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    plans = workdir / "plan"
    target = plans / f"{digest[:16]}-{RECORD_SHAPE}.parquet"
    if target.exists():
        return target
    plans.mkdir(parents=True, exist_ok=True)
    earlier = sorted(plans.glob(f"*-{RECORD_SHAPE}.parquet"))
    temporary = target.with_suffix(".tmp")
    with duckdb.connect() as con:
        con.execute("SET preserve_insertion_order=false")
        known = f"SELECT key FROM read_parquet([{', '.join(map(_sql, earlier))}])" if earlier else "SELECT NULL::VARCHAR AS key WHERE false"
        con.execute(f"""
            COPY (
                SELECT key, split_part(key, '/', 2) AS agency,
                       (row_number() OVER (PARTITION BY split_part(key, '/', 2) ORDER BY key) - 1) // {int(chunk_keys)} AS chunk
                FROM read_parquet({_sql(manifest)})
                WHERE key LIKE '%/comments/%' AND key LIKE '%.json' AND key NOT IN ({known})
                ORDER BY agency, chunk, key
            ) TO {_sql(temporary)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {int(chunk_keys)})
        """)
        keys, chunks = con.execute(
            f"SELECT count(*), count(DISTINCT (agency, chunk)) FROM read_parquet({_sql(temporary)})").fetchall()[0]
    temporary.replace(target)
    (plans / f"{digest[:16]}-{RECORD_SHAPE}.json").write_text(json.dumps(
        {"manifest": str(manifest), "manifest_sha256": digest, "record_shape": RECORD_SHAPE, "keys": keys,
         "chunks": chunks, "chunk_keys": chunk_keys,
         "earlier_plans": [p.name for p in earlier]}, indent=2) + "\n")
    logger.info("plan {}: {:,} new comment keys in {:,} chunks", target.name, keys, chunks)
    return target


def _chunks(
    workdir: Path, agencies: set[str] | None, shard: tuple[int, int] | None = None,
) -> Iterator[tuple[str, str, int, list[str]]]:
    """Every planned chunk as ``(plan id, agency, chunk, keys)``, in plan, agency and chunk order.

    ``shard`` ``(i, n)`` keeps the chunks whose CRC-32 of ``agency/chunk`` is ``i`` modulo ``n``: chunks, not
    agencies, so the largest agencies spread across every process.
    """
    for plan_file in sorted((workdir / "plan").glob(f"*-{RECORD_SHAPE}.parquet")):
        with duckdb.connect() as con:
            index = con.execute(f"SELECT DISTINCT agency, chunk FROM {_sql(plan_file)} ORDER BY 1, 2").fetchall()
            for agency, chunk in index:
                if agencies is not None and agency not in agencies:
                    continue
                if shard is not None and zlib.crc32(f"{agency}/{chunk}".encode()) % shard[1] != shard[0]:
                    continue
                keys = [row[0] for row in con.execute(
                    f"SELECT key FROM {_sql(plan_file)} WHERE agency = ? AND chunk = ? ORDER BY key", [agency, chunk]
                ).fetchall()]
                yield plan_file.stem, agency, chunk, keys


def part_path(workdir: Path, plan_id: str, agency: str, chunk: int) -> Path:
    return workdir / "parts" / f"agency={agency}" / f"{plan_id}-{chunk:05d}.parquet"


def shape_parts(workdir: Path) -> Path:
    """The glob of this record shape's parts: a workdir that also holds an earlier shape's never mixes them in."""
    return workdir / "parts" / "agency=*" / f"*-{RECORD_SHAPE}-*.parquet"


def _peak_rss_mb() -> float:
    peak = rusage.getrusage(rusage.RUSAGE_SELF).ru_maxrss
    return peak / 2**20 if sys.platform == "darwin" else peak / 2**10


def read(
    workdir: Path, *, workers: int = 64, agencies: list[str] | None = None, max_chunks: int | None = None,
    max_rss_mb: float = 2048, min_free_gb: float = 4, shard: tuple[int, int] | None = None, resource=None,
    progress_name: str = "progress.json",
) -> dict:
    """Read every planned chunk that has no part yet; return this run's counts.

    ``agencies`` restricts the run (a shard); ``resource`` replaces the anonymous S3 resource in tests. The process
    stops cleanly before a chunk when the disk holding ``workdir`` has less than ``min_free_gb`` free, and after the
    chunk that takes its peak RSS past ``max_rss_mb``; a rerun resumes where it stopped.
    """
    from spicy_docs.sources import mirrulations

    from spicy_regs.pipelines.regulations_state import source_record_type

    resource = resource or mirrulations.s3_resource(max_pool_connections=workers)
    source = source_record_type(COMMENT)
    journal = workdir / "journal.jsonl"
    progress = workdir / progress_name
    totals: dict = {"chunks": 0, "keys": 0, "rows": 0, "unresolved": 0, "deferred_chunks": 0, "stopped": None}
    started = time.monotonic()
    wanted = set(agencies) if agencies is not None else None
    for plan_id, agency, chunk, keys in _chunks(workdir, wanted, shard):
        target = part_path(workdir, plan_id, agency, chunk)
        if target.exists():
            continue
        if max_chunks is not None and totals["chunks"] + totals["deferred_chunks"] >= max_chunks:
            break
        free_gb = shutil.disk_usage(workdir).free / 2**30
        if free_gb < min_free_gb:
            totals["stopped"] = f"{free_gb:.1f} GB free, below {min_free_gb:.1f} GB"
            logger.warning("stopping before {} chunk {}: {}", agency, chunk, totals["stopped"])
            break
        begun = time.monotonic()
        reader = mirrulations.MirrulationsReader(
            resource, mirrulations.BUCKET, mirrulations.PREFIX, agency, source,
            key_lister=lambda keys=keys: keys, download_workers=workers,
        )
        rows, refused = [], []
        for keyed in reader.iter_keyed_records():
            try:
                rows.append(part_row(keyed))
            except ValueError as error:  # the extract refuses a malformed value rather than coercing it
                refused.append({"key": keyed.key, "status": "refused", "reason": str(error)[:300]})
        transport = [o for o in reader.unresolved if o.status == mirrulations.STATUS_TRANSPORT]
        record = {"plan": plan_id, "agency": agency, "chunk": chunk, "keys": len(keys), "rows": len(rows),
                  "seconds": round(time.monotonic() - begun, 2),
                  "unresolved": [{"key": o.key, "status": o.status} for o in reader.unresolved] + refused}
        if transport:
            # Unwritten, so the next run reads the whole chunk again; nothing it holds is lost or doubled.
            totals["deferred_chunks"] += 1
            logger.warning("{} chunk {}: {} keys failed in transport; deferred", agency, chunk, len(transport))
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".tmp")
        pl.DataFrame(rows, schema=PART_SCHEMA, orient="row").write_parquet(temporary, compression="zstd")
        temporary.replace(target)
        with journal.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
        totals["chunks"] += 1
        totals["keys"] += len(keys)
        totals["rows"] += len(rows)
        totals["unresolved"] += len(reader.unresolved) + len(refused)
        elapsed = time.monotonic() - started
        progress.write_text(json.dumps({**totals, "seconds": round(elapsed, 1),
                                        "keys_per_second": round(totals["keys"] / elapsed, 1) if elapsed else None,
                                        "last": {k: v for k, v in record.items() if k != "unresolved"},
                                        "peak_rss_mb": round(_peak_rss_mb()),
                                        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
                                       indent=2) + "\n")
        if _peak_rss_mb() > max_rss_mb:
            totals["stopped"] = f"peak RSS {_peak_rss_mb():.0f} MB above {max_rss_mb:.0f} MB"
            logger.warning("stopping after {} chunk {}: {}", agency, chunk, totals["stopped"])
            break
    return totals


def status(workdir: Path) -> dict:
    """Planned and read chunks and keys, from the plans and the parts on disk."""
    with duckdb.connect() as con:
        plans = sorted((workdir / "plan").glob(f"*-{RECORD_SHAPE}.parquet"))
        if not plans:
            return {"planned_keys": 0, "planned_chunks": 0, "read_chunks": 0, "read_keys": 0}
        planned = con.execute(f"""
            SELECT agency, chunk, split_part(filename, '/', -1) AS plan_file, count(*) AS n
            FROM read_parquet([{', '.join(map(_sql, plans))}], filename=true) GROUP BY ALL""").fetchall()
    done = {(path.parent.name.removeprefix("agency="), path.stem) for path in (workdir / "parts").glob("agency=*/*.parquet")}
    read_keys = sum(n for agency, chunk, plan_file, n in planned
                    if (agency, f"{plan_file.removesuffix('.parquet')}-{chunk:05d}") in done)
    return {"planned_keys": sum(row[3] for row in planned), "planned_chunks": len(planned),
            "read_chunks": len(done), "read_keys": read_keys}


#: Words no staged column's name may carry. A part also keeps each copy's other stated attributes (``attributes_json``:
#: email, phone, fax, address), its ETag and size and its body's digest; a staged read sits in the data bucket and
#: holds only the object key and columns ``comments`` publishes (:func:`read_columns`).
PRIVATE_WORDS = ("attribute", "email", "phone", "fax", "address", "etag", "size", "sha256", "length", "body")


def _column_totals(con, source: str, columns: list[str]) -> dict:
    """DuckDB's side: rows, distinct keys and ids, blank ids, and each column's non-null count and hash sum."""
    select = ["count(*) AS rows", "count(DISTINCT key) AS distinct_keys",
              "count(DISTINCT comment_id) AS distinct_comment_ids",
              "count(*) FILTER (WHERE comment_id IS NULL OR trim(comment_id) = '') AS blank_comment_ids",
              *(f'count("{c}") AS "nn:{c}"' for c in columns),
              *(f'sum(hash("{c}"))::VARCHAR AS "hash:{c}"' for c in columns)]
    cursor = con.execute(f"SELECT {', '.join(select)} FROM {source}")
    return dict(zip([d[0] for d in cursor.description], cursor.fetchone()))


def _arrow_totals(files: list[str], columns: list[str]) -> dict:
    """Arrow's side, another reader and other kernels: rows, footer rows, and each column's non-null count and
    UTF-8 byte total (a number's sum)."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    totals = {"rows": 0, "footer_rows": 0, **{f"nn:{c}": 0 for c in columns}, **{f"total:{c}": 0 for c in columns}}
    for file in files:
        handle = pq.ParquetFile(file)
        totals["footer_rows"] += handle.metadata.num_rows
        for batch in handle.iter_batches(columns=columns, batch_size=1 << 20):
            totals["rows"] += batch.num_rows
            for c in columns:
                values = batch.column(c)
                totals[f"nn:{c}"] += len(values) - values.null_count
                measured = (values.cast(pa.int64()) if pa.types.is_integer(values.type)
                            else pc.call_function("binary_length", [values]))
                totals[f"total:{c}"] += pc.call_function("sum", [measured]).as_py() or 0
    return totals


def stage(workdir: Path, out: Path, *, profile: str = "all") -> dict:
    """Write the staged fill input ``out/reads.parquet`` for ``profile`` from this record shape's parts, check it
    against them, and write ``reads.parquet.sha256`` and ``staging.json``; return the receipt.

    One row per read copy, a copy stating NULL included (it counts toward the copies' agreement), holding exactly
    :func:`read_columns` of ``profile``, sorted by ``comment_id`` and key. Every check reads the written file back:
    its columns, no private column, and against the parts, by DuckDB (rows, one row per key, no blank id, each
    column's non-null count and hash sum) and by Arrow (rows, footer rows, each column's non-null count and byte
    total). A failed check removes the file. :func:`upload_staged` puts it where a runner fetches it.
    """
    import pyarrow.parquet as pq

    from spicy_regs.sources.publication import file_identity

    columns = list(read_columns(profile))
    parts = sorted(glob(str(shape_parts(workdir))))
    if not parts:
        raise RuntimeError(f"no {RECORD_SHAPE} parts under {workdir / 'parts'}")
    target = out / "reads.parquet"
    if target.exists() or (out / "staging.json").exists():
        raise RuntimeError(f"{out} already holds a staged read; stage into a new directory")
    out.mkdir(parents=True, exist_ok=True)
    source = f"read_parquet([{', '.join(map(_sql, parts))}], hive_partitioning=false)"
    begun = time.monotonic()
    with duckdb.connect() as con:
        WRITE_RESOURCES.configure(con, out / "spill")
        selected = ", ".join(f'"{c}"' for c in columns)
        con.execute(f"COPY (SELECT {selected} FROM {source} ORDER BY comment_id, key) TO {_sql(target)} "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)")
        from_parts, from_file = (_column_totals(con, rel, columns) for rel in (source, f"read_parquet({_sql(target)})"))
    shutil.rmtree(out / "spill", ignore_errors=True)
    arrow_parts, arrow_file = _arrow_totals(parts, columns), _arrow_totals([str(target)], columns)
    names = pq.read_schema(target).names
    checks = {
        "the file holds exactly the profile's read columns, in order": names == columns,
        "no column name carries a private word": not [n for n in names if any(w in n.lower() for w in PRIVATE_WORDS)],
        "one row per object key": from_file["distinct_keys"] == from_file["rows"],
        "no blank comment_id": from_file["blank_comment_ids"] == 0,
        "DuckDB: rows, ids, non-null counts and hash sums equal the parts'": from_file == from_parts,
        "Arrow: rows, footer rows, non-null counts and byte totals equal the parts'": arrow_file == arrow_parts,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        target.unlink()
        raise RuntimeError(f"the staged read failed {failed}; nothing kept")
    identity = file_identity(target)
    sha256 = identity["sha256"].removeprefix("sha256:")
    (out / "reads.parquet.sha256").write_text(f"{sha256}  reads.parquet\n")
    receipt = {
        "purpose": "the fill input `fill-comment-fields prepare --reads` reads (docs/comment-fields-fill.md); delete "
                   "it from the data bucket once the fill is done",
        "profile": profile, "columns": columns, "record_shape": RECORD_SHAPE, "parts_dir": str(workdir / "parts"),
        "part_files": len(parts),
        **{k: from_file[k] for k in ("rows", "distinct_keys", "distinct_comment_ids")},
        "non_null_by_column": {c: from_file[f"nn:{c}"] for c in columns},
        "file": {"name": target.name, "bytes": identity["bytes"], "sha256": sha256, "etag": identity["etag"]},
        "checks": checks, "seconds": round(time.monotonic() - begun, 1),
        "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (out / "staging.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def upload_staged(out: Path, *, client=None) -> dict:
    """Put a staged read and its ``.sha256`` under ``staging/comment-fields-fill-<sha12>/`` in the data bucket, never
    over an object; read it back whole and record the upload in ``staging.json``.

    Refuses a prefix that already holds anything and a file that is not the one ``staging.json`` names.
    """
    from spicy_regs.pipelines.comment_fields_write import _bucket
    from spicy_regs.sources import r2
    from spicy_regs.sources.publication import _put_immutable, file_identity

    receipt = json.loads((out / "staging.json").read_text())
    sha256 = receipt["file"]["sha256"]
    files = {"reads.parquet": out / "reads.parquet", "reads.parquet.sha256": out / "reads.parquet.sha256"}
    if file_identity(files["reads.parquet"])["sha256"] != f"sha256:{sha256}":
        raise RuntimeError(f"{files['reads.parquet']} is not the staged read {sha256}")
    client = client or r2.get_r2_client()
    bucket, prefix = _bucket(), f"staging/comment-fields-fill-{sha256[:12]}"
    held = [item["Key"] for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/")
            for item in page.get("Contents", [])]
    if held:
        raise RuntimeError(f"{prefix}/ already holds {held}; nothing uploaded")
    for name, path in files.items():
        _put_immutable(client, bucket, f"{prefix}/{name}", path, sha256=file_identity(path)["sha256"])
    # The bytes a runner will fetch, read back whole: only these were checked.
    digest, fetched = hashlib.sha256(), client.get_object(Bucket=bucket, Key=f"{prefix}/reads.parquet")
    for block in iter(lambda: fetched["Body"].read(1 << 20), b""):
        digest.update(block)
    if digest.hexdigest() != sha256:
        raise RuntimeError(f"{prefix}/reads.parquet reads back as {digest.hexdigest()}, not {sha256}")
    receipt["upload"] = {"bucket": bucket, "key": f"{prefix}/reads.parquet", "sha256": sha256,
                         "etag": fetched["ETag"],
                         "journals": f"{prefix}/journals", "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (out / "staging.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt["upload"]


def build_attributes(workdir: Path, out: Path | None = None, *, fetch=None) -> dict:
    """Seed ``comment_attributes.parquet`` from the re-read parts; return counts and write refusals beside it.

    Every read copy is projected by SpicyDocs' ``project_comment_attributes`` from its ``attributes_json`` (the keys
    the extract does not map, which are the contract's columns) and merged by the ETL's own copy rule
    (:func:`~spicy_regs.transforms.regulations_attributes.merge_attribute_parts`): newest ``modifyDate``, then the
    smallest digest among tied copies. The parts keep no write time and no canonical record digest, so a tie takes the
    object's ETag (its MD5) as the content digest. Reviewed exclusions get no row, by the ETL's own check
    (``ExcludeReviewedComments.excludes``): the parts keep no payload, so each copy of a reviewed comment is fetched
    again by its key (``fetch``, the anonymous reader by default), and a reviewed comment whose content changed refuses
    the seed. A record the contract refuses loses only its attributes row and is listed in ``attribute_refusals.json``.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_docs.schemas.regulations_attribute_tables import project_comment_attributes
    from spicy_docs.schemas.tables import TableContractError

    from spicy_regs.contract_types import arrow_schema
    from spicy_regs.transforms.regulations_attributes import _with_order_columns, contract, merge_attribute_parts
    from spicy_regs.transforms.reviewed_comments import ExcludeReviewedComments

    table = "comment_attributes"
    out = out or workdir / f"{table}.parquet"
    staging = workdir / "attributes-staging" / table
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    schema = _with_order_columns(arrow_schema(contract(table)))
    exclusion = ExcludeReviewedComments()
    if fetch is None:
        from spicy_docs.sources import mirrulations

        resource = mirrulations.s3_resource()
        fetch = lambda key: mirrulations.download_and_parse(resource, mirrulations.BUCKET, key, lambda d: d)  # noqa: E731
    refused: list[dict] = []
    projected = 0
    for index, part in enumerate(sorted((workdir / "parts").glob("*/*.parquet"))):
        batch = []
        columns = ["key", "etag", "comment_id", "modify_date", "attributes_json"]
        for row in pq.read_table(part, columns=columns).to_pylist():
            if row["comment_id"] in exclusion.decisions and exclusion.excludes(fetch(row["key"])):
                continue
            try:
                projected_row = project_comment_attributes(row["comment_id"], json.loads(row["attributes_json"]))
            except TableContractError as error:
                refused.append({"key": row["key"], "id": row["comment_id"], "reason": str(error)[:300]})
                continue
            batch.append({**projected_row, "_modify_date": row["modify_date"], "_written_at": None,
                          "_record_digest": row["etag"]})
        if batch:
            pq.write_table(pa.Table.from_pylist(batch, schema=schema), staging / f"part-{index:05d}.parquet",
                           compression="zstd")
            projected += len(batch)
    rows = merge_attribute_parts(table, staging, None, out)
    shutil.rmtree(staging.parent, ignore_errors=True)
    receipt = {"projected_copies": projected, "rows": rows, "refused": refused}
    (out.parent / "attribute_refusals.json").write_text(json.dumps({"refused": refused}, indent=2) + "\n")
    return {k: v for k, v in receipt.items() if k != "refused"} | {"refused": len(refused)}


app = App(name="fill-comment-fields", help=__doc__)


@app.command(name="plan")
def plan_command(*, workdir: Path, manifest: Path, chunk_keys: int = CHUNK_KEYS) -> None:
    """Add a manifest's unplanned comment keys as new chunks."""
    require_record_shape()
    plan(workdir, manifest, chunk_keys=chunk_keys)


@app.command(name="read")
def read_command(
    *, workdir: Path, workers: int = 64, shard: int | None = None, shards: int | None = None,
    max_rss_mb: float = 2048, min_free_gb: float = 4,
) -> None:
    """Read the unread chunks, or one ``--shard`` of ``--shards`` (chunks dealt by CRC-32)."""
    if (shard is None) != (shards is None):
        raise ValueError("--shard and --shards go together")
    pair = (shard, shards) if shard is not None and shards is not None else None
    if pair is not None and not 0 <= pair[0] < pair[1]:
        raise ValueError(f"shard {pair[0]} is outside 0..{pair[1] - 1}")
    require_record_shape()
    name = f"progress-{shard}.json" if shard is not None else "progress.json"
    totals = read(workdir, workers=workers, max_rss_mb=max_rss_mb, min_free_gb=min_free_gb, shard=pair,
                  progress_name=name)
    print(json.dumps(totals))
    if totals["deferred_chunks"] or totals["stopped"]:
        raise SystemExit(3)


@app.command(name="stage")
def stage_command(*, workdir: Path, out: Path, profile: str = "all") -> None:
    """Write and check the staged fill input for ``--profile`` (``attachments`` for the 2026-10 re-read) under
    ``--out``, from this record shape's parts (no upload)."""
    print(json.dumps(stage(workdir, out, profile=profile), indent=2))


@app.command(name="upload-staged")
def upload_staged_command(*, out: Path) -> None:
    """Put the staged read under ``--out`` in the data bucket, never over an object, and read it back."""
    from dotenv import load_dotenv

    load_dotenv()
    print(json.dumps(upload_staged(out), indent=2))


@app.command(name="prepare")
def prepare_command(
    *, workdir: Path, docket: str | None = None, agency: str | None = None, reads: Path | None = None,
    profile: str = "all",
) -> None:
    """Compute the fill at the catalog's current snapshot (no write); ``--docket``/``--agency`` scope a pilot.

    ``--reads`` is the staged fill input (runbook); by default this record shape's parts under the workdir.
    ``--profile`` names the fill columns, ``attachments`` for the 2026-10 re-read (runbook).
    """
    from dotenv import load_dotenv

    load_dotenv()
    scope = {k: v for k, v in (("docket_id", docket), ("agency_code", agency)) if v}
    print(json.dumps(prepare(workdir, scope=scope or None, reads=reads or shape_parts(workdir), profile=profile),
                     indent=2))


@app.command(name="write")
def write_command(
    *, workdir: Path, batch_bytes: int = BATCH_BYTES, by_file: bool = True, clear_failure: bool = False,
) -> None:
    """Apply the prepared fill under the catalog lock; ``--no-by-file`` for a scoped pilot."""
    from dotenv import load_dotenv

    load_dotenv()
    print(json.dumps(write(workdir, batch_bytes=batch_bytes, by_file=by_file, clear_failure=clear_failure), indent=2))


@app.command(name="fetch")
def fetch_command(*, workdir: Path, key: str, sha256: str) -> None:
    """Download the staged fill input to ``workdir/reads.parquet``, refusing any other bytes than ``--sha256``'s."""
    from dotenv import load_dotenv

    load_dotenv()
    print(fetch_staged(workdir, key, sha256))


@app.command(name="journals")
def journals_command(*, workdir: Path, prefix: str, push: bool = False) -> None:
    """Pull every run's journal from the data bucket's ``--prefix`` (``--push``: upload this workdir's)."""
    from dotenv import load_dotenv

    load_dotenv()
    print(json.dumps(sync_journals(workdir, prefix, push=push)))


@app.command(name="undo")
def undo_command(*, workdir: Path, batch: str, expected_snapshot: int) -> None:
    """Restore a committed batch from its pre-image as a new checked commit, if the catalog is at ``--expected-snapshot``."""
    from dotenv import load_dotenv

    load_dotenv()
    print(json.dumps(undo(workdir, batch, expected_snapshot=expected_snapshot), indent=2))


@app.command(name="attributes")
def attributes_command(*, workdir: Path) -> None:
    """Seed comment_attributes.parquet from the re-read parts (no upload)."""
    print(json.dumps(build_attributes(workdir), indent=2))


@app.command(name="status")
def status_command(*, workdir: Path) -> None:
    """Planned and read chunks and keys."""
    print(json.dumps(status(workdir), indent=2))


if __name__ == "__main__":
    app()
