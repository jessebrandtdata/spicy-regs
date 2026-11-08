"""Derive current public comments from one explicitly pinned v1 catalog pair.

The earlier-policy catalog export runs in a separate interpreter. This module
only reads that local pair and writes private files; it never opens a catalog.
An explicit captured input can defer historical validation to prepare's reader;
it does not manufacture an earlier export-complete seal.
"""

from collections import Counter
from dataclasses import asdict
from hashlib import file_digest
import json
from pathlib import Path
import re
import stat
from tempfile import TemporaryDirectory
from uuid import uuid4
import os
import subprocess
import sys

import duckdb
import pyarrow.parquet as pq

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.etl_bulk import _read_promotion_rows
from spicy_regs.etl_receipts import (
    ReceiptContext, _digest, _rows, comments_receipt_diagnostics, exact_json,
    selected_subject_policy, write_dataset,
)
from spicy_regs.transforms.regulations_receipts import COMMENTS_PROMOTION_PROCESSOR, policy


FIELDS = (
    "comment_reference_values_json", "text_extraction_status",
    "pdf_extraction_results_json",
)
EXPECTED_ROWS = 26_418_078
V1_EXPORT_REVISION = "e9dfd216dc999845ea94e6171afb41e0c9380a10"


def identity(path):
    with path.open("rb") as source:
        digest = file_digest(source, "sha256").hexdigest()
    return {"sha256": digest, "byteSize": path.stat().st_size}


def verify_original(pair):
    marker = json.loads((pair / "export-complete.json").read_text())
    metadata = json.loads((pair / "generation.json").read_text())
    if marker.get("metadata") != metadata or set(marker.get("members", {})) != {
        "comments.parquet", "etl_receipts.parquet"
    }:
        raise ValueError("Earlier export completion marker differs")
    for name, expected in marker["members"].items():
        if identity(pair / name) != expected:
            raise ValueError("Earlier export member changed after its snapshot was sealed")
    return marker


def _regular_signature(path):
    """Keep custody of the same absolute regular file across byte verification."""
    if not path.is_absolute():
        raise ValueError("Captured input must name absolute regular files")
    value = path.lstat()
    if not stat.S_ISREG(value.st_mode):
        raise ValueError("Captured input must name absolute regular files")
    return {"device": value.st_dev, "inode": value.st_ino, "byteSize": value.st_size,
            "mtimeNs": value.st_mtime_ns, "ctimeNs": value.st_ctime_ns}


def _captured_descriptor(path, expected_sha256):
    before = _regular_signature(path)
    descriptor_identity = identity(path)
    if (not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)
            or descriptor_identity["sha256"] != expected_sha256):
        raise ValueError("Captured input descriptor differs from its explicit digest")
    captured = json.loads(path.read_text())
    if _regular_signature(path) != before:
        raise ValueError("Captured input descriptor changed during verification")
    if (set(captured) != {"format", "state", "metadata", "members", "capture_provenance", "validationRevision"}
            or captured["format"] != "comments-captured-pair/1"
            or captured["state"] != "captured-validation-pending"
            or not isinstance(captured["validationRevision"], str)
            or not re.fullmatch(r"[0-9a-f]{40}", captured["validationRevision"])):
        raise ValueError("Expected an explicit captured, validation-pending comments input")
    metadata = captured["metadata"]
    if (set(metadata) != {"dataset", "generation_id", "snapshot", "policy", "captureRevision"}
            or metadata["dataset"] != "comments"
            or not isinstance(metadata["generation_id"], str) or not metadata["generation_id"]
            or metadata["captureRevision"] != V1_EXPORT_REVISION):
        raise ValueError("Captured input lacks its exact historical source and publisher label")
    snapshot = metadata["snapshot"]
    if (set(snapshot) != {"table_uuid", "snapshot_id", "schema_id", "receipts"}
            or set(snapshot["receipts"]) != {"table_uuid", "snapshot_id", "schema_id"}):
        raise ValueError("Captured input must pin both exact catalog table snapshots")
    for selected in (snapshot, snapshot["receipts"]):
        if (not isinstance(selected["table_uuid"], str) or not selected["table_uuid"]
                or any(type(selected[key]) is not int for key in ("snapshot_id", "schema_id"))):
            raise ValueError("Captured catalog table identity differs")
    if set(captured["members"]) != {"comments.parquet", "etl_receipts.parquet"}:
        raise ValueError("Captured pair must name exactly both original members")
    provenance = captured["capture_provenance"]
    if set(provenance) != {"path", "sha256", "byteSize"}:
        raise ValueError("Captured input must identify retained capture provenance")
    provenance_path = Path(provenance["path"])
    provenance_before = _regular_signature(provenance_path)
    if identity(provenance_path) != {key: provenance[key] for key in ("sha256", "byteSize")}:
        raise ValueError("Captured input provenance changed")
    if _regular_signature(provenance_path) != provenance_before:
        raise ValueError("Captured input provenance changed during verification")
    return captured, descriptor_identity


def verify_captured(pair, descriptor, expected_sha256):
    """Verify retained bytes/custody, without asserting historical receipt validity."""
    if (pair / "export-complete.json").exists():
        raise ValueError("A sealed original pair must use the normal sealed-input path")
    captured, descriptor_identity = _captured_descriptor(descriptor, expected_sha256)
    provenance = captured["capture_provenance"]
    members, signatures = {}, {}
    for name, pin in captured["members"].items():
        if set(pin) != {"path", "sha256", "byteSize", "custody"}:
            raise ValueError("Captured member requires exact bytes and custody")
        path = Path(pin["path"])
        if path.absolute() != (pair / name).absolute():
            raise ValueError("Captured member is outside its explicit original pair")
        before = _regular_signature(path)
        if before != pin["custody"]:
            raise ValueError("Captured member custody changed; fresh verification is required")
        expected = {key: pin[key] for key in ("sha256", "byteSize")}
        if (identity(path) != expected or _regular_signature(path) != before):
            raise ValueError("Captured member bytes or custody changed")
        members[name], signatures[name] = expected, before
    if pq.ParquetFile(pair / "comments.parquet").metadata.num_rows != EXPECTED_ROWS:
        raise ValueError("Captured subject is not the full population")
    if pq.ParquetFile(pair / "etl_receipts.parquet").metadata.num_rows != EXPECTED_ROWS + 1:
        raise ValueError("Captured receipts are not the full population")
    capture = {"format": captured["format"], "state": captured["state"],
               "descriptor": {"path": str(descriptor), **descriptor_identity},
               "provenance": provenance, "custody": signatures,
               "validationRevision": captured["validationRevision"]}
    return {"metadata": captured["metadata"], "members": members, "capture": capture}


def _validation_source():
    """Identify the clean maintained executable, separately from capture source."""
    from spicy_regs import etl_bulk, etl_receipts
    root = Path(__file__).resolve().parents[1]
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"],
                               cwd=root, text=True).strip():
        raise ValueError("Current comments validation source has tracked changes")
    members = {"helper": Path(__file__).resolve(), "reader": Path(etl_receipts.__file__).resolve(),
               "bulk_reader": Path(etl_bulk.__file__).resolve()}
    if (members["reader"] != root / "src/spicy_regs/etl_receipts.py"
            or members["bulk_reader"] != root / "src/spicy_regs/etl_bulk.py"):
        raise ValueError("Current comments readers do not belong to the pinned source checkout")
    return {"revision": revision, "members": {name: identity(path) for name, path in members.items()}}


def _pair_snapshot(value):
    """Decode a recorded pair without changing its original IDs."""
    from spicy_regs.sources.iceberg import CatalogPairSnapshot, CatalogSnapshot
    value = dict(value)
    value["receipts"] = CatalogSnapshot(**value["receipts"])
    return CatalogPairSnapshot(**value)


class V1CatalogReader:
    """Explicit earlier-policy metadata reads in an independent interpreter."""

    def __init__(self, checkout):
        self.checkout = Path(checkout).resolve()

    def _read(self, snapshot=None):
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.checkout,
                                           text=True).strip()
        if revision != V1_EXPORT_REVISION:
            raise ValueError("Historical reader revision changed")
        if subprocess.check_output(["git", "status", "--porcelain"], cwd=self.checkout, text=True).strip():
            raise ValueError("Historical reader has local changes")
        code = """
from dataclasses import asdict
from pathlib import Path
import json, sys
from spicy_regs.sources import iceberg
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.transforms.regulations_receipts import COMMENTS_PROMOTION_PROCESSOR, policy
assert policy('comments').policy_version == 'regulations-native-v1'
rt = RECORD_TYPES['comments']
if len(sys.argv) == 2:
    result = asdict(iceberg.catalog_snapshot(rt))
else:
    value = json.loads(sys.argv[2])
    value['receipts'] = iceberg.CatalogSnapshot(**value['receipts'])
    result = iceberg.rows_unchanged_since(rt, iceberg.CatalogPairSnapshot(**value))
Path(sys.argv[1]).write_text(json.dumps(result))
"""
        environment = dict(os.environ, PYTHONPATH=str(self.checkout / "src"))
        arguments = [] if snapshot is None else [json.dumps(snapshot)]
        # Catalog diagnostics may use stdout. Keep the exact JSON result separate
        # and let both diagnostic streams reach the operation log.
        with TemporaryDirectory(prefix="comments-snapshot-") as temporary:
            result = Path(temporary) / "snapshot.json"
            subprocess.run([sys.executable, "-c", code, str(result), *arguments],
                           cwd=self.checkout, env=environment, check=True)
            return json.loads(result.read_text())

    def snapshot(self):
        return _pair_snapshot(self._read())

    def rows_unchanged_since(self, snapshot):
        return self._read(asdict(snapshot)) is True


def prepare(pair: Path, destination: Path, *, captured_input=None, captured_input_sha256=None,
            retained_scan=None, retained_scan_sha256=None, native_only=False, remote_writer=None):
    """Keep the complete earlier pair and derive declared current subjects once."""
    pair, destination = Path(pair), Path(destination)
    if remote_writer is not None and not native_only:
        raise ValueError("Remote preparation requires native-only output")
    if remote_writer is not None and destination.exists():
        raise FileExistsError(destination)
    sealed = (verify_original(pair) if captured_input is None else
              verify_captured(pair, captured_input, captured_input_sha256))
    metadata = sealed["metadata"]
    if metadata.get("dataset") != "comments" or not metadata.get("snapshot"):
        raise ValueError("Exact catalog-pair snapshot metadata is required")
    subjects, receipts = pair / "comments.parquet", pair / "etl_receipts.parquet"
    current = policy("comments")
    earlier = selected_subject_policy(current, [subjects])
    if (earlier.policy_version != "regulations-native-v1"
            or current.policy_version != "regulations-native-v2"):
        raise ValueError("Expected exact declared earlier/current comments policies")
    if native_only and (earlier.dataset != current.dataset or earlier.identity_fields != current.identity_fields
            or earlier.nullable_identity_fields != current.nullable_identity_fields
            or any(earlier.subject_schema.field(key) != current.subject_schema.field(key)
                   for key in current.identity_fields)):
        raise ValueError("Streaming history promotion requires unchanged subject identity fields")
    revision_field = "exportRevision" if captured_input is None else "captureRevision"
    if metadata.get("policy") != earlier.descriptor() or metadata.get(revision_field) != V1_EXPORT_REVISION:
        raise ValueError("Earlier export authority differs from the shipped historical policy")
    validation_source = None
    if captured_input is not None:
        validation_source = _validation_source()
        if validation_source["revision"] != sealed["capture"]["validationRevision"]:
            raise ValueError("Captured input names a different current validation source")
    if pq.ParquetFile(subjects).metadata.num_rows != EXPECTED_ROWS:
        raise ValueError("The complete captured comments population is required")
    source_identity = sealed["members"]["comments.parquet"]
    receipt_identity = sealed["members"]["etl_receipts.parquet"]
    scan = None
    if retained_scan is not None:
        if captured_input is None:
            raise ValueError("Retained scans require the exact captured-input path")
        scan = (Path(retained_scan), retained_scan_sha256, sealed["capture"]["descriptor"])
    generation = "comments-v2-" + uuid4().hex
    available = {name: Counter() for name in FIELDS}
    count = 0

    def context_for(prior, attempt_id, processor):
        # The pinned original pair holds the inputs and historical receipts.
        # Link to those bytes instead of embedding them again in every output.
        diagnostics = comments_receipt_diagnostics(prior)
        return ReceiptContext(
            generation, attempt_id, processor,
            [{"source_id": "selected-comments-v1", "source_uri": str(subjects),
              "sha256": source_identity["sha256"],
              "locator": json.dumps(metadata["snapshot"], sort_keys=True),
              "body_version": metadata["generation_id"]},
             {"source_id": "selected-comments-v1-receipts", "source_uri": str(receipts),
              "sha256": receipt_identity["sha256"], "locator": prior["receipt_id"],
              "body_version": prior["generation_id"]}],
            diagnostics,
        )

    def carried_attempts(path):
        for prior in _rows(path):
            context = context_for(prior, prior["attempt_id"], prior["processor"])
            # Keep the actual outcome, identity and failure facts. The original
            # input remains available through the pinned receipt-file witness.
            carried = dict(prior, generation_id=generation, witnesses=list(context.witnesses),
                           processing_json=exact_json({}), diagnostic_json=exact_json(context.diagnostics))
            carried["receipt_id"] = _digest({key: value for key, value in carried.items() if key != "receipt_id"})
            yield carried

    def records():
        nonlocal count
        # Preserve the bulk failure and its cause instead of silently starting
        # a full-payload SQLite copy for this complete selected population.
        rows = _read_promotion_rows([subjects], [receipts], earlier,
                                   generation_id=metadata["generation_id"], evidence=scan,
                                   include_prior=True)
        for row, prior in rows:
            # Promote the retained values directly. The earlier mapper remains
            # the authority for its normalized subject and its original record.
            for name in FIELDS:
                available[name]["absent" if name not in row else
                                "null" if row[name] is None else "recorded"] += 1
                row.setdefault(name, None)
            if set(row) != current.input_fields:
                raise ValueError("Retained comments fields differ from current policy")
            context = context_for(prior, str(count), COMMENTS_PROMOTION_PROCESSOR)
            yield {name: row[name] for name in current.subject_schema.names}, context
            count += 1

    # Select historical nonaccepted occurrences locally, retaining file order.
    # Admission still validates every output receipt under its exact policy.
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="comments-prior-attempts-", dir=destination.parent) as temporary:
        attempts = Path(temporary) / "attempts.parquet"
        with duckdb.connect() as con:
            ExportResources().configure(con, Path(temporary) / "spill")
            con.execute(
                "COPY (SELECT * EXCLUDE(file_row_number) FROM read_parquet($receipts, "
                "file_row_number=true, hive_partitioning=false) "
                "WHERE outcome<>'accepted' ORDER BY file_row_number) "
                "TO $attempts (FORMAT PARQUET, COMPRESSION ZSTD)",
                {"receipts": str(receipts), "attempts": str(attempts)},
            )
        # Admission still checks the whole subject/receipt pair before exposure.
        # Original input/history stays in the pinned pair, not another rewrite.
        from rulespec_artifacts import publish_directory_no_replace
        staged = Path(temporary) / "fresh"
        # This knob also bounds the wide Python input buffer. Output-only
        # batching would need a separate maintained writer setting.
        failures = carried_attempts(attempts)
        if remote_writer is None:
            subject, receipt = write_dataset(records(), staged, current, batch_size=128, failures=failures)
        else:
            subject, receipt = remote_writer(records(), current, failures=failures,
                                             generation_id=generation, snapshot=metadata["snapshot"])
        if captured_input is not None:
            _captured_descriptor(captured_input, captured_input_sha256)
            # Reuse byte identities verified at entry only while custody stays
            # unchanged through conversion, failure rebinding and history use.
            if (count != EXPECTED_ROWS
                    or (pair / "export-complete.json").exists()
                    or {name: _regular_signature(pair / name) for name in sealed["members"]}
                       != sealed["capture"]["custody"]
                    or _validation_source() != validation_source):
                raise ValueError("Captured inputs or validation source changed during preparation")
        if remote_writer is None:
            if subject is None:
                raise ValueError("Derived Comments population has no accepted subjects")
            publish_directory_no_replace(staged, destination)
            subject, receipt = destination / subject.name, destination / receipt.name
        else:
            # Only derivation metadata is local; remote receipts never masquerade
            # as the canonical local body files or a completed local export.
            destination.mkdir()
    if count != EXPECTED_ROWS:
        raise ValueError("Derived comments population differs from the captured population")
    evidence = {
        "generation_id": generation, "dataset": "comments", "rows": count,
        "snapshot": metadata["snapshot"],
        "original_pair": {"path": str(pair), "metadata": metadata,
                          "subjects": source_identity, "receipts": receipt_identity,
                          "policy": earlier.descriptor()},
        "current_policy": current.descriptor(),
        "recorded_fields": {name: dict(values) for name, values in available.items()},
        "receipt_inputs": "pinned-original-pair",
        "native_catalog_mutated": False,
    }
    if captured_input is not None:
        evidence["original_pair"]["capture"] = sealed["capture"]
        evidence["historical_admission"] = {"status": "validated", "source": validation_source,
            "policy": earlier.descriptor(), "generation_id": metadata["generation_id"],
            "members": sealed["members"], "capture_descriptor": sealed["capture"]["descriptor"]}
        if retained_scan is not None:
            evidence["historical_admission"]["retained_scan"] = {"path": str(retained_scan),
                                                                 "sha256": retained_scan_sha256}
    (destination / "generation.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    return subject, receipt, evidence


def assemble(pair: Path, output: Path, *, captured_input=None, captured_input_sha256=None,
             retained_scan=None, retained_scan_sha256=None, native_only=False):
    """Use existing agency writers and index builder on the one derived artifact."""
    from spicy_regs.sources.iceberg import _build_comments_index
    from spicy_regs.schemas.regulations import RECORD_TYPES
    output = Path(output)
    subject, receipts, evidence = prepare(pair, output / ".catalog-pairs" / "comments",
                                         captured_input=captured_input,
                                         captured_input_sha256=captured_input_sha256,
                                         retained_scan=retained_scan, retained_scan_sha256=retained_scan_sha256,
                                         native_only=native_only)
    resources = ExportResources()
    if native_only:
        # The native generation publishes the monolith and index. Keep one
        # subject body; canonical preparation paths remain unchanged.
        monolith = output / "comments.parquet"
        os.link(subject, monolith)
        partitions = output / "comments" / "agency"
        with TemporaryDirectory(prefix="comments-native-index-", dir=output) as temporary, duckdb.connect() as con:
            resources.configure(con, Path(temporary) / "spill")
            con.from_parquet(str(monolith)).create_view("comments_export")
            index = _build_comments_index(con, RECORD_TYPES["comments"], output,
                                         source_sql="SELECT * FROM comments_export")
    else:
        monolith, index, partitions = _assemble_legacy(subject, output, resources)
    result = {"comments": monolith, "index": index, "partitions": partitions,
              "receipts": receipts, "generation": subject.parent / "generation.json"}
    members = [monolith, index, receipts, result["generation"],
               *sorted(partitions.glob("agency_code=*/part-0.parquet"))]
    manifest = {"format": "comments-prepared-export/1", "source": evidence["snapshot"],
                "derivation": evidence, "members": {
                    path.relative_to(output).as_posix(): identity(path) for path in members}}
    if native_only:
        manifest["purpose"] = "native-generation"
    result["manifest"] = output / "comments-prepared-export.json"
    result["manifest"].write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return result


def _assemble_legacy(subject, output, resources):
    from spicy_regs.sources.iceberg import _build_comments_index
    from spicy_regs.schemas.regulations import RECORD_TYPES
    from spicy_regs.transforms.partition_comments import assemble_comments, sort_comment_agencies, stage_comment_agencies

    with TemporaryDirectory(prefix="comments-derived-sort-", dir=output) as temporary:
        temporary = Path(temporary)
        with duckdb.connect() as con:
            resources.configure(con, temporary / "spill")
            con.from_parquet(str(subject)).create_view("comments_derived")
            stage_comment_agencies(con, "SELECT * FROM comments_derived", temporary / "staging",
                                  resources=resources)
        partitions = sort_comment_agencies(temporary / "staging", output, resources=resources)
        with duckdb.connect() as con:
            resources.configure(con, temporary / "spill")
            monolith = assemble_comments(con, partitions, output, policy("comments").subject_schema.names,
                                        resources=resources)
            con.from_parquet(str(monolith)).create_view("comments_export")
            index = _build_comments_index(con, RECORD_TYPES["comments"], output,
                                         source_sql="SELECT * FROM comments_export")
    return monolith, index, partitions


def _retained_remote_subject(path, expected_sha256, *, bucket, captured, snapshot, staging_prefix):
    """Bind either a digest-unknown repack input or one known completed subject."""
    before = _regular_signature(path)
    descriptor = identity(path)
    if (not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)
            or descriptor["sha256"] != expected_sha256):
        raise ValueError("Retained remote subject descriptor differs from its pin")
    value = json.loads(path.read_text())
    if _regular_signature(path) != before:
        raise ValueError("Retained remote subject descriptor changed during verification")
    if (set(value) != {"format", "bucket", "source", "originalMembers", "subjectPolicy", "subject",
                       "subjectByteDigest"}
            or value["format"] != "comments-remote-subject-recovery/1"
            or value["bucket"] != bucket or value["source"] != asdict(snapshot)
            or value["originalMembers"] != captured["members"]
            or value["subjectPolicy"] != policy("comments").descriptor()):
        raise ValueError("Retained remote subject recovery authority differs")
    digest = value["subjectByteDigest"]
    if digest is not None and (not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)):
        raise ValueError("Retained subject byte digest must be null or its exact recorded SHA256")
    member = value["subject"]
    if (not isinstance(member, dict) or set(member) != {"key", "byte_size", "etag", "rows"}
            or not isinstance(member["key"], str)
            or not re.fullmatch(r"staging/comments/[0-9a-f]{32}/comments\.parquet", member["key"])
            or (member["key"] == staging_prefix + "/comments.parquet") != (digest is not None)
            or type(member["byte_size"]) is not int or not 0 < member["byte_size"] <= 64 * 1024**3
            or type(member["rows"]) is not int or member["rows"] != EXPECTED_ROWS
            or not isinstance(member["etag"], str) or not member["etag"].strip()):
        raise ValueError("Retained subject must match known-digest reuse or distinct digest-unknown repacking")
    return value, {"path": str(path), **descriptor}


def assemble_remote(pair, output, *, client, bucket, staging_prefix, max_bytes,
                    reader, captured_snapshot, captured_input=None, captured_input_sha256=None,
                    retained_scan=None, retained_scan_sha256=None,
                    retained_remote_subject=None, retained_remote_subject_sha256=None):
    """Prepare an unpublished remote generation, with small local derivation/control files."""
    from spicy_regs.sources import publication
    from spicy_regs.remote_generations import prepare_remote_generation
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA
    from spicy_regs.pipelines.comments_generation import stage_comments_remote
    from spicy_regs.pipelines.comments_mirror import _check_prepared_snapshot
    from spicy_regs.public_url import resolve_r2_base_url

    output = Path(output)
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("Partial remote preparation requires explicit recovery; refusing rebuild")
    output.mkdir(parents=True, exist_ok=True)
    base_url = resolve_r2_base_url()
    record = {"format": "comments-remote-preparation/1", "status": "staging-unpublished",
              "bucket": bucket, "stagingPrefix": staging_prefix, "baseUrl": base_url,
              "source": asdict(captured_snapshot), "published": False,
              "priorIndex": publication.current_index(base_url)}
    recovery = recovery_pin = None
    if retained_remote_subject is not None:
        captured, _ = _captured_descriptor(captured_input, captured_input_sha256)
        recovery, recovery_pin = _retained_remote_subject(
            retained_remote_subject, retained_remote_subject_sha256, bucket=bucket, captured=captured,
            snapshot=captured_snapshot, staging_prefix=staging_prefix)
        record["subjectRecovery"] = {
            "descriptor": recovery_pin, "input": recovery,
            "meaning": ("Known completed subject is reused in place; missing receipts still require full ordered "
                        "input comparison and independent generation admission."
                        if recovery["subjectByteDigest"] is not None else
                        "Pinned input bytes have no prior digest; full ordered values must match rebuilt receipt "
                        "input before repacked outputs qualify."),
        }
    result_path = output / "comments-remote-preparation.json"

    def save():
        temporary = result_path.with_suffix(".tmp")
        with temporary.open("w") as stream:
            stream.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(result_path)

    save()
    holder = {}
    try:
        def member_complete(member):
            completed = record.setdefault("completedMembers", {})
            assert isinstance(completed, dict)  # This local record creates this map only here.
            completed[Path(member.key).name] = asdict(member)
            if Path(member.key).name == "comments.parquet":
                record["completedSubject"] = asdict(member)
            record["status"] = "remote-members-unvalidated-unpublished"
            save()  # Each closed upload remains recoverable if a later check fails.

        def writer(records, current, *, failures, generation_id, snapshot):
            subject, receipt, index, predecessor = stage_comments_remote(
                records, current, failures=failures, generation_id=generation_id, snapshot=snapshot,
                output_dir=output, client=client, bucket=bucket, staging_prefix=staging_prefix,
                max_bytes=max_bytes, previous_url=base_url + "/comments.parquet",
                retained_subject=None if recovery is None else recovery["subject"],
                retained_subject_sha256=None if recovery is None else recovery["subjectByteDigest"],
                on_member=member_complete)
            if recovery is not None and _retained_remote_subject(
                    retained_remote_subject, retained_remote_subject_sha256, bucket=bucket, captured=captured,
                    snapshot=captured_snapshot, staging_prefix=staging_prefix) != (recovery, recovery_pin):
                raise ValueError("Retained subject recovery descriptor changed during repacking")
            holder.update(subject=subject, receipt=receipt, index=index)
            record.update(status="remote-members-written-unpublished",
                          members=[asdict(member) for member in (subject, receipt, index)],
                          predecessor={"etag": predecessor.etag, "agencies": sorted(predecessor.agencies)})
            save()
            return subject, receipt

        subject, receipt, evidence = prepare(
            pair, output / ".catalog-pairs" / "comments", native_only=True, remote_writer=writer,
            captured_input=captured_input, captured_input_sha256=captured_input_sha256,
            retained_scan=retained_scan, retained_scan_sha256=retained_scan_sha256)
        if evidence["snapshot"] != asdict(captured_snapshot) or subject.rows != EXPECTED_ROWS:
            raise ValueError("Remote preparation differs from the complete captured source")
        current, index_policy = policy("comments"), policy("comments_index")
        schemas = {}
        with duckdb.connect() as con:
            for name, schema in (("comments", current.subject_schema),
                                 ("comments_index", index_policy.subject_schema), ("etl_receipts", RECEIPT_SCHEMA)):
                con.register("remote_schema", schema.empty_table())
                schemas[name] = [(row[0], row[1]) for row in con.execute("DESCRIBE SELECT * FROM remote_schema").fetchall()]
                con.unregister("remote_schema")
        directory = output / ".comments-generations" / uuid4().hex
        record.update(status="validating-remote-generation-unpublished", derivation=evidence,
                      generationDirectory=str(directory))
        save()
        artifact = prepare_remote_generation(
            directory, family="comments", client=client, bucket=bucket, staging_prefix=staging_prefix,
            members=[subject, holder["index"], receipt],
            expected_keys=("comments.parquet", "comments_index.parquet", "etl_receipts.parquet"),
            schemas=schemas, read_snapshot=record["priorIndex"],
            parents={"catalog_comments": {"sha256": subject.sha256, "byteSize": subject.byte_size}},
            receipt_policies=[current, index_policy], receipt_generation_id=evidence["generation_id"],
            bounded_receipts=True)
        observed = _check_prepared_snapshot(reader, captured_snapshot)
        if captured_input is not None:
            _captured_descriptor(captured_input, captured_input_sha256)
            if ({name: _regular_signature(Path(pair) / name) for name in evidence["original_pair"]["capture"]["custody"]}
                    != evidence["original_pair"]["capture"]["custody"]
                    or _validation_source() != evidence["historical_admission"]["source"]):
                raise ValueError("Captured inputs or validation source changed during remote admission")
        record.update(status="remote-generation-validated-unpublished",
                      observedSnapshot=asdict(observed),
                      artifactPin={"logicalId": artifact.pin.logical_id, "artifactDigest": artifact.pin.artifact_digest})
        save()
        return record
    except BaseException as error:
        record.update(status="failed-unpublished", errorType=type(error).__name__)
        save()
        raise


def export_v1(source_checkout: Path, pair: Path, *, retained_subject=None, retained_receipts=None):
    """Export in the historical interpreter; optionally admit checked retained bodies."""
    source_checkout, pair = Path(source_checkout).resolve(), Path(pair).resolve()
    if retained_receipts is not None and retained_subject is None:
        raise ValueError("Retained receipts require the matching retained subject")
    if (pair / "generation.json").exists():
        if not (pair / "export-complete.json").exists():
            raise ValueError("Earlier export lacks a sealed completion marker")
        return
    if pair.exists():
        raise FileExistsError("An incomplete earlier export requires explicit recovery")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source_checkout,
                                       text=True).strip()
    if revision != V1_EXPORT_REVISION:
        raise ValueError("Earlier export checkout differs from the exact merged revision")
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=source_checkout, text=True).strip():
        raise ValueError("Earlier export checkout has unqualified local changes")
    code = """
from dataclasses import asdict
from pathlib import Path
import json
import hashlib, os
import sys
import pyarrow.parquet as pq
from spicy_regs.sources import iceberg, regulatory_catalog
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.transforms.regulations_receipts import COMMENTS_PROMOTION_PROCESSOR, policy
assert policy('comments').policy_version == 'regulations-native-v1'
rt = RECORD_TYPES['comments']
snapshot = iceberg.catalog_snapshot(rt)
pair = Path(sys.argv[1])
generation = 'catalog-comments-v1-' + str(snapshot.snapshot_id)
held = json.loads(sys.argv[3]) if len(sys.argv) >= 4 else None
held_receipts = json.loads(sys.argv[4]) if len(sys.argv) == 5 else None
if held is not None:
    if set(held) != {'path', 'sha256', 'byteSize', 'snapshot'} or held['snapshot'] != asdict(snapshot):
        raise ValueError('Retained subject does not name the exact selected pair')
    source = Path(held['path'])
    if not source.is_absolute() or source.is_symlink() or not source.is_file():
        raise ValueError('Retained subject must be an absolute regular file')
    with source.open('rb') as original:
        digest = hashlib.file_digest(original, 'sha256').hexdigest()
    if digest != held['sha256'] or source.stat().st_size != held['byteSize']:
        raise ValueError('Retained subject bytes changed')
    if pq.ParquetFile(source).metadata.num_rows != 26418078:
        raise ValueError('Retained subject is not the full population')
if held_receipts is not None:
    if held is None or set(held_receipts) != {'path', 'sha256', 'byteSize', 'snapshot'} or held_receipts['snapshot'] != asdict(snapshot):
        raise ValueError('Retained receipts do not name the exact selected pair')
    receipt_source = Path(held_receipts['path'])
    if not receipt_source.is_absolute() or receipt_source.is_symlink() or not receipt_source.is_file():
        raise ValueError('Retained receipts must be an absolute regular file')
    with receipt_source.open('rb') as original:
        digest = hashlib.file_digest(original, 'sha256').hexdigest()
    if digest != held_receipts['sha256'] or receipt_source.stat().st_size != held_receipts['byteSize']:
        raise ValueError('Retained receipt bytes changed')
    if pq.ParquetFile(receipt_source).metadata.num_rows != 26418079:
        raise ValueError('Retained receipts are not the full population')
con = iceberg._connect()
try:
    if held is None and con.execute('SELECT count(*) FROM ' + regulatory_catalog.qualified(rt)).fetchone()[0] != 26418078:
        raise ValueError('Full native population is required before export')
    if held is None:
        regulatory_catalog.export_pair(con, rt, pair,
            generation_id=generation, snapshot=snapshot)
    else:
        from spicy_regs.duckdb_settings import ExportResources
        # The historical entry point has no subject-reuse option. Keep its
        # receipt selection, rebinding and admission primitives unchanged.
        stage = pair.parent / 'original-v1-recovery-stage'
        stage.mkdir()
        pair.mkdir()
        os.link(source, pair / 'comments.parquet')  # Same filesystem; no copy fallback.
        ExportResources().configure(con, stage / 'spill')
        with regulatory_catalog._transaction(con):
            if not regulatory_catalog.initialized(con, 'comments'):
                raise ValueError('Native catalog has no qualified initialization receipt')
            if iceberg._read_pair_snapshot(con, rt) != snapshot:
                raise RuntimeError('Selected pair changed before receipt recovery')
            # Stream the exact native selection into the maintained rebinder.
            # Avoid COPY's raw intermediate and its second full disk pass.
            # Row batches are targets; the owned process limit still bounds
            # unusually wide rows and the native reader's allocations.
            if held_receipts is not None:
                os.link(receipt_source, pair / 'etl_receipts.parquet')
            else:
                with con.execute(
                    "SELECT * FROM " + regulatory_catalog.receipts_table() + " WHERE dataset='comments'"
                ).to_arrow_reader(batch_size=128) as reader:
                    regulatory_catalog.write_rows(
                        (regulatory_catalog.rebind_receipt(row, generation_id=generation)
                         for batch in reader for row in batch.to_pylist()),
                        pair / 'etl_receipts.parquet', regulatory_catalog.RECEIPT_SCHEMA)
            if pq.ParquetFile(pair / 'etl_receipts.parquet').metadata.num_rows != 26418079:
                raise ValueError('Receipt recovery is not the complete selected population')
            if held_receipts is not None:
                from spicy_regs.etl_receipts import validate_receipt_bundle
                # Complete admission is required; reconstructed processing rows
                # are unused here. Keep only join fields in its temporary index.
                validate_receipt_bundle(
                    {'comments': [pair / 'comments.parquet']},
                    [pair / 'etl_receipts.parquet'], [policy('comments')],
                    generation_id=generation)
            else:
                for _ in regulatory_catalog.read_with_receipts(
                        [pair / 'comments.parquet'], [pair / 'etl_receipts.parquet'],
                        policy('comments'), generation_id=generation):
                    pass
            if iceberg._read_pair_snapshot(con, rt) != snapshot:
                raise RuntimeError('Selected pair changed during receipt recovery')
        (pair / 'generation.json').write_text(json.dumps({
            'generation_id': generation, 'dataset': 'comments', 'snapshot': asdict(snapshot)},
            sort_keys=True) + '\\n')
finally:
    con.close()
if pq.ParquetFile(pair / 'comments.parquet').metadata.num_rows != 26418078:
    raise ValueError('Native export is not the complete captured comments population')
metadata_path = Path(sys.argv[1]) / 'generation.json'
metadata = json.loads(metadata_path.read_text())
metadata.update(policy=policy('comments').descriptor(), exportRevision=sys.argv[2])
metadata_path.write_text(json.dumps(metadata, sort_keys=True) + '\\n')
members = {}
for name in ('comments.parquet', 'etl_receipts.parquet'):
    path = Path(sys.argv[1]) / name
    with path.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    members[name] = {'sha256': digest, 'byteSize': path.stat().st_size}
if held is not None and members['comments.parquet'] != {
        'sha256': held['sha256'], 'byteSize': held['byteSize']}:
    raise ValueError('Retained subject changed during receipt recovery')
if held_receipts is not None and members['etl_receipts.parquet'] != {
        'sha256': held_receipts['sha256'], 'byteSize': held_receipts['byteSize']}:
    raise ValueError('Retained receipts changed during admission')
marker = Path(sys.argv[1]) / 'export-complete.json'
temporary = marker.with_suffix('.tmp')
with temporary.open('w') as output:
    output.write(json.dumps({'metadata': metadata, 'members': members}, sort_keys=True) + '\\n')
    output.flush()
    os.fsync(output.fileno())
temporary.replace(marker)
"""
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(source_checkout / "src")
    arguments = [] if retained_subject is None else [json.dumps(retained_subject)]
    if retained_receipts is not None:
        arguments.append(json.dumps(retained_receipts))
    subprocess.run([sys.executable, "-c", code, str(pair), V1_EXPORT_REVISION, *arguments], cwd=source_checkout,
                   env=environment, check=True)


def encoding(path: Path, member_identity):
    """Read actual final footer evidence; writer targets are not encoding proof."""
    with pq.ParquetFile(path) as parquet:
        groups, codecs = [], set()
        for number in range(parquet.metadata.num_row_groups):
            group = parquet.metadata.row_group(number)
            groups.append({"rows": group.num_rows, "totalByteSize": group.total_byte_size})
            codecs.update(group.column(column).compression for column in range(group.num_columns))
        return {**member_identity, "rows": parquet.metadata.num_rows,
                "physicalSchema": str(parquet.schema), "arrowSchema": str(parquet.schema_arrow),
                "codecs": sorted(codecs), "rowGroups": groups}


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v1-source", type=Path, required=True)
    parser.add_argument("--original-pair", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--base-receipt", type=Path)
    parser.add_argument("--captured-input", type=Path,
                        help="Explicit captured-validation-pending pair descriptor; never an export seal")
    parser.add_argument("--captured-input-sha256", help="Exact digest of that captured input descriptor")
    parser.add_argument("--retained-scan", type=Path,
                        help="Optional exact closed source15 scan evidence; omit for fresh original-pair admission")
    parser.add_argument("--retained-scan-sha256", help="Exact digest of the retained scan evidence")
    parser.add_argument("--native-only", action="store_true", help="Prepare only immutable native publication members")
    parser.add_argument("--remote-staging-prefix", help="New unreferenced R2 prefix for native-only members")
    parser.add_argument("--remote-max-bytes", type=int, default=64 * 1024**3,
                        help="Maximum serialized bytes per remote member, including footer")
    parser.add_argument("--retained-remote-subject", type=Path,
                        help="Explicit pinned completed remote subject to repack while rebuilding missing receipts")
    parser.add_argument("--retained-remote-subject-sha256", help="Exact digest of that recovery descriptor")
    args = parser.parse_args()
    if args.native_only and args.publish:
        raise ValueError("Native-only preparation cannot publish the legacy mirrors")
    if args.remote_staging_prefix is not None and (
            not args.native_only or args.captured_input is None
            or not 0 < args.remote_max_bytes <= 64 * 1024**3):
        raise ValueError("Remote native preparation requires native-only output, captured inputs and bounded output")
    if ((args.retained_remote_subject is None) != (args.retained_remote_subject_sha256 is None)
            or args.retained_remote_subject is not None and args.remote_staging_prefix is None):
        raise ValueError("Retained remote subject requires its descriptor pin and remote native preparation")
    if args.retained_remote_subject is not None:
        args.retained_remote_subject = args.retained_remote_subject.absolute()
    args.v1_source = args.v1_source.resolve()
    args.original_pair = args.original_pair.resolve()
    args.output = args.output.resolve()
    if (args.captured_input is None) != (args.captured_input_sha256 is None):
        raise ValueError("Captured input path and exact descriptor digest are required together")
    if (args.retained_scan is None) != (args.retained_scan_sha256 is None):
        raise ValueError("Retained scan path and exact evidence digest are required together")
    if args.retained_scan is not None:
        args.retained_scan = args.retained_scan.absolute()
    if args.captured_input is not None:
        # Keep the literal path so symlink custody is checked rather than resolved away.
        args.captured_input = args.captured_input.absolute()
    if args.base_receipt is not None:
        args.base_receipt = args.base_receipt.resolve()
    if args.publish and (os.getenv("GITHUB_ACTIONS") != "true"
            or os.getenv("SPICY_REGS_CATALOG_LOCK") != "comments-catalog-write"
            or not os.getenv("GITHUB_WORKFLOW_REF", "").startswith(
                "mikewolfd/spicy-regs/.github/workflows/publish-comments-mirror.yml@")):
        raise RuntimeError("Publish requires the existing locked manual comments workflow")
    def verify_base():
        if args.base_receipt is None:
            if args.publish:
                raise ValueError("Prepared publication requires captured published base versions")
            return
        base = json.loads(args.base_receipt.read_text())
        if base.get("reuse_published_base") is not True or base.get("base_only") is not True:
            raise ValueError("Prepared publication requires an explicit earlier base-only capture")
        subprocess.run([sys.executable, str(Path(__file__).with_name("check_refresh_inputs.py")),
                        "verify", "--receipt", str(args.base_receipt)], check=True)

    verify_base()
    from spicy_regs.pipelines.comments_mirror import _check_prepared_snapshot, _validate_prepared_export
    reader = V1CatalogReader(args.v1_source)
    if args.remote_staging_prefix is not None:
        from spicy_regs.sources import r2
        captured, _ = _captured_descriptor(args.captured_input, args.captured_input_sha256)
        captured_snapshot = _pair_snapshot(captured["metadata"]["snapshot"])
        _check_prepared_snapshot(reader, captured_snapshot)
        assemble_remote(args.original_pair, args.output, client=r2.get_r2_client(),
                        bucket=os.environ.get("R2_BUCKET_NAME", "spicy-regs"),
                        staging_prefix=args.remote_staging_prefix, max_bytes=args.remote_max_bytes,
                        reader=reader, captured_snapshot=captured_snapshot,
                        captured_input=args.captured_input, captured_input_sha256=args.captured_input_sha256,
                        retained_scan=args.retained_scan, retained_scan_sha256=args.retained_scan_sha256,
                        retained_remote_subject=args.retained_remote_subject,
                        retained_remote_subject_sha256=args.retained_remote_subject_sha256)
        return
    observed_snapshot = None
    manifest_path = args.output / "comments-prepared-export.json"
    if manifest_path.exists():
        sealed = (verify_original(args.original_pair) if args.captured_input is None else
                  verify_captured(args.original_pair, args.captured_input, args.captured_input_sha256))
        derivation = json.loads(manifest_path.read_text())["derivation"]
        original = derivation["original_pair"]
        if (original["metadata"] != sealed["metadata"]
                or original["subjects"] != sealed["members"]["comments.parquet"]
                or original["receipts"] != sealed["members"]["etl_receipts.parquet"]):
            raise ValueError("Prepared output names a different original export")
        if args.captured_input is not None:
            admission = derivation.get("historical_admission", {})
            if (original.get("capture") != sealed["capture"]
                    or admission.get("status") != "validated"
                    or admission.get("source", {}).get("revision") != sealed["capture"]["validationRevision"]
                    or admission.get("policy") != original["policy"]
                    or admission.get("generation_id") != sealed["metadata"]["generation_id"]
                    or admission.get("members") != sealed["members"]
                    or admission.get("capture_descriptor") != sealed["capture"]["descriptor"]):
                raise ValueError("Prepared output lacks its exact current historical admission")
        result = {"comments": args.output / "comments.parquet",
                  "index": args.output / "comments_index.parquet",
                  "partitions": args.output / "comments" / "agency",
                  "receipts": args.output / ".catalog-pairs" / "comments" / "etl_receipts.parquet",
                  "generation": args.output / ".catalog-pairs" / "comments" / "generation.json",
                  "manifest": manifest_path}
        captured_snapshot = _pair_snapshot(sealed["metadata"]["snapshot"])
        observed_snapshot = _check_prepared_snapshot(reader, captured_snapshot)
        if _validate_prepared_export(args.output, result, captured_snapshot) != args.native_only:
            raise ValueError("Prepared output purpose differs from the requested operation")
    else:
        if args.output.exists() and any(args.output.iterdir()):
            raise FileExistsError("Partial prepared output requires explicit recovery; refusing rebuild")
        if args.captured_input is None:
            export_v1(args.v1_source, args.original_pair)
        else:
            captured, _ = _captured_descriptor(args.captured_input, args.captured_input_sha256)
            captured_snapshot = _pair_snapshot(captured["metadata"]["snapshot"])
            observed_snapshot = _check_prepared_snapshot(reader, captured_snapshot)
        result = assemble(args.original_pair, args.output, captured_input=args.captured_input,
                          captured_input_sha256=args.captured_input_sha256,
                          retained_scan=args.retained_scan, retained_scan_sha256=args.retained_scan_sha256,
                          native_only=args.native_only)
        if args.captured_input is not None:
            manifest = json.loads(manifest_path.read_text())
            if manifest["source"] != asdict(captured_snapshot):
                raise ValueError("Prepared output names a different captured snapshot")
            observed_snapshot = _check_prepared_snapshot(reader, captured_snapshot)
    manifest = json.loads(manifest_path.read_text())
    proof = {"comments": encoding(result["comments"], manifest["members"]["comments.parquet"]),
             "index": encoding(result["index"], manifest["members"]["comments_index.parquet"]),
             "originalPair": str(args.original_pair), "status": "prepared-unpublished"}
    if observed_snapshot is not None:
        proof["observedSnapshot"] = asdict(observed_snapshot)
    evidence = args.output / "comments-exact-encoding.json"
    evidence.write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n")
    if args.publish:
        verify_base()
        from spicy_regs.pipelines.comments_mirror import publish_comments_mirror
        publish_comments_mirror(args.output, prepared_export=result,
                                prepared_reader=reader, prepared_snapshot=_pair_snapshot(manifest["source"]))
        proof["status"] = "published"
        proof["publication"] = json.loads((args.output / "comments-publication.json").read_text())
        evidence.write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
