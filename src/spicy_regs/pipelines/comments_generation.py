"""Seal the comments mirror and its aggregate index as one generation.

:func:`build_comments_generation` is the route: no ETL receipts since ss-juz8. The remote staging and prepared
publication below belong to the one-shot v1 promotion of 2026-10-10, which published receipts; the mirror refuses
that route now, and they go with it once a receipts-free publication has run.
"""
from dataclasses import asdict
from contextlib import ExitStack
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from uuid import uuid4

import duckdb
import pyarrow.parquet as pq
import pyarrow as pa

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.etl_receipts import ReceiptContext
from spicy_regs.generations import build_generation
from spicy_regs.sources.publication import file_identity
from spicy_regs.transforms.regulations_receipts import policy


def _index_records(path, generation_id, witness):
    ordinal = 0
    with pq.ParquetFile(path) as source:
        for batch in source.iter_batches(batch_size=2000):
            for row in batch.to_pylist():
                yield row, ReceiptContext(generation_id, str(ordinal), "comments-index/1", [witness])
                ordinal += 1


def _matched_subject_batches(parquet, paired, schema, batch_size):
    """Repack preserved rows only after exact ordered agreement with receipt input.

    The declared/readback schema check precedes this safe cast; the cast only
    restores the serializer's structural LIST child names. Alignment handles
    receipt-only attempts and different input row-group boundaries.
    """
    batches = iter(parquet.iter_batches(batch_size=batch_size, use_threads=False))
    held, offset = None, 0
    for subject, receipt in paired:
        if subject is None:
            yield subject, receipt
            continue
        parts, remaining = [], subject.num_rows
        while remaining:
            if held is None or offset == held.num_rows:
                held, offset = next(batches, None), 0
                if held is None:
                    raise ValueError("Retained Comments subject ended before receipt input")
            count = min(remaining, held.num_rows - offset)
            parts.append(held.slice(offset, count))
            offset += count
            remaining -= count
        restored = pa.Table.from_batches(parts).cast(schema, safe=True)
        if not restored.equals(subject, check_metadata=True):
            raise ValueError("Retained Comments values or order differ from receipt input")
        yield restored, receipt
    if (held is not None and offset != held.num_rows) or next(batches, None) is not None:
        raise ValueError("Retained Comments subject has extra rows")


def stage_comments_remote(records, current, *, failures, generation_id, snapshot,
                          output_dir, client, bucket, staging_prefix, max_bytes,
                          previous_url, batch_size=128, retained_subject=None,
                          retained_subject_sha256=None, on_subject=None, on_member=None):
    """Write both large members from one input pass, retaining only narrow index scratch.

    Returned upload receipts remain unpublished and require full generation
    admission. The same splitter, history inheritance, index builder and
    predecessor checks serve local and remote output.
    """
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, _dataset_batches
    from spicy_regs.sources.remote_parquet import (
        ParquetUpload, StoredParquet, remote_parquet_writer, open_pinned_s3, parquet_schema,
    )
    from spicy_regs.sources.iceberg import _build_comments_index
    from spicy_regs.schemas.regulations import RECORD_TYPES
    from spicy_regs.pipelines.comments_mirror import validate_export

    if not staging_prefix or staging_prefix != staging_prefix.strip("/"):
        raise ValueError("Remote Comments needs an explicit attempt staging prefix")
    reuse_subject = retained_subject_sha256 is not None
    existing = client.list_objects_v2(Bucket=bucket, Prefix=staging_prefix + "/", MaxKeys=2)
    if reuse_subject:
        if (retained_subject is None or not isinstance(retained_subject_sha256, str)
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", retained_subject_sha256)
                or retained_subject["key"] != staging_prefix + "/comments.parquet"
                or not 0 < retained_subject["byte_size"] <= max_bytes
                or existing.get("IsTruncated")
                or [item["Key"] for item in existing.get("Contents", [])] != [retained_subject["key"]]
                or client.list_multipart_uploads(
                    Bucket=bucket, Prefix=staging_prefix + "/", MaxUploads=1).get("Uploads")):
            raise ValueError("Known-digest recovery needs exactly its completed subject and no other outputs")
    elif existing.get("Contents") or existing.get("IsTruncated"):
        raise ValueError("Remote Comments needs a new, empty attempt staging prefix")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="comments-validation-", dir=output_dir) as temporary, ExitStack() as preparation:
        scratch = Path(temporary)
        candidate_path = scratch / "candidate.parquet"
        columns = ("comment_id", "agency_code", "docket_id", "posted_date")
        schema = pa.schema([current.subject_schema.field(name) for name in columns])
        receipt_upload = preparation.enter_context(remote_parquet_writer(
            client=client, bucket=bucket, key=staging_prefix + "/etl_receipts.parquet",
            schema=RECEIPT_SCHEMA, max_bytes=max_bytes))
        with ExitStack() as subject_output, \
                pq.ParquetWriter(candidate_path, schema, compression="zstd") as projection_writer:
            projection = ParquetUpload(projection_writer, schema)
            if not reuse_subject:
                subject_upload = subject_output.enter_context(remote_parquet_writer(
                    client=client, bucket=bucket, key=staging_prefix + "/comments.parquet",
                    schema=current.subject_schema, max_bytes=max_bytes))
            with ExitStack() as inputs:
                paired = _dataset_batches(records, current, failures=failures, batch_size=batch_size)
                if retained_subject is not None:
                    head = client.head_object(Bucket=bucket, Key=retained_subject["key"])
                    if (head.get("ETag") != retained_subject["etag"]
                            or head.get("ContentLength") != retained_subject["byte_size"]):
                        raise ValueError("Retained Comments subject version changed")
                    stream = inputs.enter_context(open_pinned_s3(
                        client=client, bucket=bucket, key=retained_subject["key"],
                        etag=retained_subject["etag"]))
                    parquet = inputs.enter_context(pq.ParquetFile(stream))
                    if (not parquet.schema_arrow.equals(parquet_schema(current.subject_schema), check_metadata=True)
                            or parquet.metadata.num_rows != retained_subject["rows"]):
                        raise ValueError("Retained Comments schema or population differs")
                    paired = _matched_subject_batches(parquet, paired, current.subject_schema, batch_size)
                for subject, receipt in paired:
                    if subject is not None:
                        if not reuse_subject:
                            subject_upload.write(subject)
                        # Retained rows have already passed exact ordered comparison.
                        # Share their arrays and coalesce the small replay batches.
                        projection.write(pa.Table.from_arrays([subject[name] for name in columns], schema=schema))
                    receipt_upload.write(receipt)
                if retained_subject is not None:
                    head = client.head_object(Bucket=bucket, Key=retained_subject["key"])
                    if (head.get("ETag") != retained_subject["etag"]
                            or head.get("ContentLength") != retained_subject["byte_size"]):
                        raise ValueError("Retained Comments subject changed during receipt reconstruction")
            if retained_subject is not None:
                # Release the old, potentially large footer before index work.
                del parquet, stream
            del paired
            projection.flush()
        if reuse_subject:
            assert retained_subject is not None  # Validated before staging any output.
            subject = StoredParquet(**retained_subject, sha256=retained_subject_sha256)
        else:
            subject = subject_upload.stored
        assert subject is not None
        if on_subject is not None:
            on_subject(subject)
        if on_member is not None:
            on_member(subject)
        # Keep the staged schema/footer checks without rereading the remote body.
        # Complete generation admission still independently decodes every page.
        with open_pinned_s3(client=client, bucket=bucket, key=subject.key, etag=subject.etag) as stream, \
                pq.ParquetFile(stream) as parquet:
            if not parquet.schema_arrow.equals(parquet_schema(current.subject_schema), check_metadata=True):
                raise ValueError("Staged Comments schema differs")
            if projection.rows != subject.rows or projection.rows != parquet.metadata.num_rows:
                raise ValueError("Staged Comments and index-input populations differ")

        with duckdb.connect() as con:
            ExportResources().configure(con, scratch / "index-spill")
            con.from_parquet(str(candidate_path)).create_view("candidate")
            _build_comments_index(con, RECORD_TYPES["comments"], output_dir,
                                  source_sql="SELECT * FROM candidate")
        index_policy = policy("comments_index")
        witness = {"source_id": "comments", "source_uri": None, "sha256": subject.sha256,
                   "locator": json.dumps(snapshot, sort_keys=True), "body_version": generation_id}
        with remote_parquet_writer(client=client, bucket=bucket, key=staging_prefix + "/comments_index.parquet",
                                   schema=index_policy.subject_schema, max_bytes=max_bytes) as index_upload:
            for index, receipt in _dataset_batches(
                    _index_records(output_dir / "comments_index.parquet", generation_id, witness),
                    index_policy, batch_size=2000):
                assert index is not None
                index_upload.write(index)
                receipt_upload.write(receipt)
        assert index_upload.stored is not None
        if on_member is not None:
            on_member(index_upload.stored)
        # Close the receipts while retaining the projection for the checks below.
        # ExitStack also retains ordinary abort behavior if earlier work fails.
        preparation.close()
        assert receipt_upload.stored is not None
        if on_member is not None:
            on_member(receipt_upload.stored)

        def candidates(con):
            con.from_parquet(str(candidate_path)).create_view("candidate")
            con.from_parquet(str(output_dir / "comments_index.parquet")).create_view("candidate_index")

        predecessor = validate_export(output_dir, previous_url, candidate_builder=candidates)
    return subject, receipt_upload.stored, index_upload.stored, predecessor


def publish_prepared_comments_remote(output_dir, *, client, bucket, reader, snapshot,
                                     prepared_sha256, source_guard):
    """Re-admit pinned staging; check source and predecessor immediately before each pointer attempt."""
    from rulespec_artifacts import ArtifactPin
    from spicy_regs.remote_generations import publish_remote_generation
    from spicy_regs.sources.remote_parquet import StoredParquet
    from spicy_regs.sources import r2
    from spicy_regs.pipelines.comments_mirror import _check_prepared_snapshot
    from spicy_regs.public_url import resolve_r2_base_url

    output_dir = Path(output_dir)
    path = output_dir / "comments-remote-preparation.json"

    def prepared():
        if file_identity(path)["sha256"] != prepared_sha256:
            raise ValueError("Remote preparation record differs from its explicit pin")
        return json.loads(path.read_text())

    record = prepared()
    derivation = record["derivation"]
    if (record["format"] != "comments-remote-preparation/1"
            or record["status"] != "remote-generation-validated-unpublished" or record["published"] is not False
            or record["bucket"] != bucket or record["source"] != asdict(snapshot)
            or derivation["snapshot"] != record["source"] or derivation["rows"] != 26_418_078
            or derivation["current_policy"] != policy("comments").descriptor()):
        raise ValueError("Expected one fully qualified remote Comments preparation")
    directory = Path(record["generationDirectory"])
    if not directory.resolve().is_relative_to(output_dir.resolve()):
        raise ValueError("Remote generation metadata is outside its owned output")
    members = [StoredParquet(**member) for member in record["members"]]
    if {Path(member.key).name for member in members} != {
            "comments.parquet", "comments_index.parquet", "etl_receipts.parquet"}:
        raise ValueError("Remote Comments membership differs")
    base = record["baseUrl"]
    if base != resolve_r2_base_url():
        raise ValueError("Remote preparation names a different public data authority")

    def guard():
        if prepared() != record:
            raise ValueError("Remote preparation changed before publication")
        source_guard()
        _check_prepared_snapshot(reader, snapshot)
        version = r2.public_object_version(base + "/comments.parquet")
        if version is None or version["etag"] != record["predecessor"]["etag"]:
            raise ValueError("Prior Comments mirror changed before publication")

    guard()
    pin = record["artifactPin"]
    return publish_remote_generation(
        directory, client=client, bucket=bucket, staging_prefix=record["stagingPrefix"], members=members,
        prior_index=record["priorIndex"], bounded_receipts=True,
        expected_pin=ArtifactPin(pin["logicalId"], pin["artifactDigest"]), before_pointer=guard)


#: The comments family's datasets, which leave ETL receipts together (ss-juz8): admission requires a receipt member
#: to classify every table beside it (``generations._verify_receipts``), so one family cannot keep receipts for one.
RETIRED_RECEIPT_DATASETS = frozenset({"comments", "comments_index"})


def build_comments_generation(output_dir, result):
    """Seal the exported comments and their index as one family generation, without ETL receipts.

    Each table must have the columns and types its installed policy declares, the check receipt admission made
    before (ss-juz8). The published index is the export's own file, the one the fixed ``comments_index.parquet``
    URL serves. ``catalog_comments`` records the digest of the table read, as every comments generation has.
    """
    from spicy_regs.native_types import described_schema

    comments = result["comments"]
    directory = output_dir / ".comments-generations" / uuid4().hex
    build_generation(directory, family="comments", files=[comments, result["index"]],
                     expected_keys=("comments.parquet", "comments_index.parquet"),
                     schemas={name: described_schema(policy(name).subject_schema) for name in RETIRED_RECEIPT_DATASETS},
                     parents={"catalog_comments": {"sha256": file_identity(comments)["sha256"],
                                                   "byteSize": comments.stat().st_size}})
    return directory
