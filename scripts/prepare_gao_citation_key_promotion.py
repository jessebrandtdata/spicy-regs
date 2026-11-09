"""Add the GAO main citation key to one exact local native family; never acquire or publish.

Keep literal admitted subjects and raw_record mappings. Government processing
replay expands missing legacy columns to NULL, so it cannot preserve every
historical raw shape exactly for this additive main-field change.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import os
import sqlite3
from uuid import uuid4

import pyarrow.parquet as pq
from rulespec_artifacts import iter_member_descriptors

from scripts.prepare_citation_namespace_promotion import _json
from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs.etl_receipts import (
    ReceiptContext, combine_receipts, decode_exact_json, exact_json, rebind_receipt,
    subject_identity, write_dataset,
)
from spicy_regs.gao_citation_keys import citation_product_id
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.rollups.gao_reports import GaoReportsRollup
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.sources import publication
from spicy_regs.transforms.government_receipts import _rows

FAMILY = "gao-reports"
KEY = "citation_product_id"


def _stage(snapshot, bindings, reports, decisions, output):
    entry = snapshot["families"][FAMILY]
    if bindings.get("selected_artifact") != entry["artifactDigest"]:
        raise ValueError("Custody map differs from the selected GAO artifact")
    items = bindings["bindings"]
    mapped = {item["key"]: item for item in items}
    if len(mapped) != len(items) or set(mapped) != {"artifact.json", "members.json", "etl_receipts.parquet"}:
        raise ValueError("Exact GAO controls and shared receipt bindings are required")
    controls = Path(bindings["prior_controls_directory"])
    paths = {key: Path(item["canonical_path"]) for key, item in mapped.items()}
    paths.update({"gao_reports.parquet": reports, "gao_decisions.parquet": decisions})
    for key in ("artifact.json", "members.json"):
        if paths[key] != controls / key:
            raise ValueError("Control binding differs from its selected directory")
        _, digest = _json(paths[key])
        if paths[key].stat().st_size != mapped[key]["bytes"] or digest.removeprefix("sha256:") != mapped[key]["raw_sha256"]:
            raise ValueError("Control bytes differ from their custody binding")
    members = [m for key in entry["tables"] for m in publication.table_members(snapshot, key)]
    members.extend(publication.receipt_members(snapshot, dataset="gao_reports"))
    if {m.key for m in members} != {"gao_reports.parquet", "gao_decisions.parquet", "etl_receipts.parquet"}:
        raise ValueError("Expected the complete unsplit GAO family")
    for member in members:
        if not isinstance(member.sha256, str):
            raise ValueError("Selected member has no content digest")
        path = paths[member.key]
        if path.is_symlink() or not path.is_file() or path.stat().st_size != member.byte_size:
            raise ValueError("Local member differs from its selected size/type")
        if member.key in mapped and (mapped[member.key]["bytes"] != member.byte_size or
                mapped[member.key]["raw_sha256"] != member.sha256.removeprefix("sha256:")):
            raise ValueError("Receipt custody pin differs from the captured member")
    held = [controls, *paths.values()]
    if any(output.resolve().is_relative_to(p.resolve()) or p.resolve().is_relative_to(output.resolve()) for p in held):
        raise ValueError("Output must be separate from retained inputs and controls")
    output.mkdir(parents=True, exist_ok=False)
    prior = output / "prior"
    prior.mkdir()
    for key, source in paths.items():
        os.link(source, prior / key)
    return prior


def _same_main(before, after, *, added=()):
    with pq.ParquetFile(before) as old, pq.ParquetFile(after) as new:
        names = old.schema_arrow.names
        if [name for name in new.schema_arrow.names if name not in added] != names:
            raise ValueError("Promotion changes original main column order")
        if old.metadata.num_rows != new.metadata.num_rows:
            raise ValueError("Promotion changes main row count")
        for expected, actual in zip(old.iter_batches(batch_size=2000),
                                    new.iter_batches(batch_size=2000, columns=names), strict=True):
            if not expected.equals(actual):
                raise ValueError("Promotion changes original main values or row order")
        return old.metadata.num_rows


def prepare(*, captured_index: Path, member_bindings: Path, reports: Path, decisions: Path,
            output: Path, expected_prior: str, expected_index: str, expected_bindings: str) -> dict:
    snapshot, index_digest = _json(captured_index)
    bindings, bindings_digest = _json(member_bindings)
    if index_digest != expected_index or bindings_digest != expected_bindings:
        raise ValueError("Captured controls differ from their expected digests")
    snapshot = publication.parse_index(json.dumps(snapshot).encode())
    entry = snapshot["families"][FAMILY]
    if entry["artifactDigest"] != expected_prior or set(entry["tables"]) != set(GaoReportsRollup.outputs):
        raise ValueError("The exact complete selected GAO family is required")
    if KEY in dict(entry["tables"]["gao_reports.parquet"]["columns"]):
        raise ValueError("Selected GAO reports already expose the citation key")
    if "etlReceipts" not in entry:
        raise ValueError("This recipe requires the shared GAO receipt")
    prior = _stage(snapshot, bindings, reports, decisions, output)
    with ConversionReadOperation() as operation, ExitStack() as stack:
        old, old_reader, old_source = operation.admit_generation(prior, expected_pin={
            "logicalId": entry["logicalId"], "artifactDigest": expected_prior})
        if publication.generation_entry(old, iter_member_descriptors(old, old_source)) != {
                k: v for k, v in entry.items() if k != "publishedAt"}:
            raise ValueError("Admitted GAO prior differs from its captured complete entry")
        if old.root["spec"]["publicationStatus"] != "complete-family":
            raise ValueError("Partial GAO prior cannot qualify complete-family promotion")
        connection = stack.enter_context(sqlite3.connect(output / "prior-receipts.sqlite"))
        connection.execute("CREATE TABLE receipts (dataset TEXT, record_id TEXT, receipt TEXT, PRIMARY KEY(dataset,record_id))")
        generation = uuid4().hex
        connection.execute("CREATE TABLE attempts (receipt_id TEXT PRIMARY KEY, receipt TEXT)")
        for receipt in _rows(prior / "etl_receipts.parquet"):
            if receipt["outcome"] == "accepted":
                connection.execute("INSERT INTO receipts VALUES (?,?,?)",
                    (receipt["dataset"], receipt["record_id"], exact_json(receipt)))
            else:
                carried = rebind_receipt(receipt, generation_id=generation)
                connection.execute("INSERT INTO attempts VALUES (?,?)", (carried["receipt_id"], exact_json(carried)))
        connection.commit()
        files, shards = [], []
        for policy in GaoReportsRollup.receipt_policies:
            def records(policy=policy):
                for row in old_reader.read_subjects(policy.dataset):
                    identity = subject_identity(policy, row)[0]
                    stored = connection.execute("SELECT receipt FROM receipts WHERE dataset=? AND record_id=?",
                                                (policy.dataset, identity)).fetchone()
                    if stored is None:
                        raise ValueError("Admitted subject has no selected prior receipt")
                    receipt = decode_exact_json(stored[0])
                    carried = rebind_receipt(receipt, generation_id=generation)
                    subject = {name: row[name] for name in old_reader.policies[policy.dataset].subject_schema.names}
                    if policy.dataset == "gao_reports":
                        subject[KEY] = citation_product_id(subject)
                    yield {**subject, **{name: row[name] for name in policy.receipt_fields}}, ReceiptContext(
                        generation, receipt["attempt_id"], policy.policy_version,
                        receipt["witnesses"], decode_exact_json(carried["diagnostic_json"]))
            failures = (rebind_receipt(r, generation_id=generation)
                        for r in _rows(prior / "etl_receipts.parquet")
                        if r["dataset"] == policy.dataset and r["outcome"] != "accepted")
            subject, shard = write_dataset(records(), output / "writer" / policy.dataset, policy, failures=failures)
            assert subject is not None
            # Shared write_dataset requires a subject to validate the new receipt.
            # The final decisions publication carries the original admitted bytes.
            files.append(prior / "gao_decisions.parquet" if policy.dataset == "gao_decisions" else subject)
            shards.append(shard)
        receipt_path = combine_receipts(shards, output / "writer/etl_receipts.parquet")
        evidence = CaptureEvidence(output, FAMILY)
        stack.push(lambda _kind, error, _trace: evidence.finish(error))
        evidence.inherit_admitted(snapshot, old)
        evidence.event("native-promotion", source_acquisition=False,
                       change="Add the existing GAO resolver key to retained main rows; preserve literal originals.")
        writer = GaoReportsRollup(skip_upload=True)
        artifact = build_generation(output / "generation", family=FAMILY, files=files,
            expected_keys=writer.outputs, schemas=writer.generation_schemas(), read_snapshot=snapshot,
            inputs=evidence.inputs(), carried_forward={"gao_decisions.parquet": entry["artifactDigest"]},
            receipt_path=receipt_path, receipt_policies=writer.receipt_policies,
            receipt_generation_id=generation, read_operation=operation)
        _, new_reader, _ = operation.admit_generation(output / "generation", expected_pin=artifact.pin)
        counts = {}
        for dataset in ("gao_reports", "gao_decisions"):
            counts[dataset] = _same_main(prior / (dataset + ".parquet"),
                output / "generation" / (dataset + ".parquet"), added=(KEY,) if dataset == "gao_reports" else ())
            for before, after in zip(old_reader.read_subjects(dataset), new_reader.read_subjects(dataset), strict=True):
                if exact_json(before["raw_record"]) != exact_json(after["raw_record"]):
                    raise ValueError("Promotion changes literal original producer values")
                if dataset == "gao_reports" and after[KEY] != citation_product_id(before):
                    raise ValueError("Main citation key differs from the maintained resolver rule")
        preserved = 0
        for receipt in _rows(output / "generation/etl_receipts.parquet"):
            if receipt["outcome"] == "accepted":
                old_receipt = decode_exact_json(connection.execute(
                    "SELECT receipt FROM receipts WHERE dataset=? AND record_id=?",
                    (receipt["dataset"], receipt["record_id"])).fetchone()[0])
                if receipt["witnesses"] != old_receipt["witnesses"] or receipt["processing_json"] != old_receipt["processing_json"]:
                    raise ValueError("Promotion changes retained witnesses or literal processing fields")
                preserved += 1
            else:
                held = connection.execute("SELECT receipt FROM attempts WHERE receipt_id=?", (receipt["receipt_id"],)).fetchone()
                if held is None or held[0] != exact_json(receipt):
                    raise ValueError("Promotion changes a retained nonaccepted receipt")
                connection.execute("DELETE FROM attempts WHERE receipt_id=?", (receipt["receipt_id"],))
        if connection.execute("SELECT count(*) FROM attempts").fetchone()[0]:
            raise ValueError("Promotion omits retained nonaccepted receipts")
        if preserved != sum(counts.values()) or old_reader.outcome_counts() != new_reader.outcome_counts():
            raise ValueError("Promotion changes accepted subjects or receipt outcome counts")
        old_reader.check()
        new_reader.check()
        assert evidence.artifact is not None
        admitted = verify_evidence(evidence.artifact_dir, expected_pin=evidence.artifact.pin)
        if admitted.root["inputs"] != [r for r in artifact.root["inputs"] if r["role"] == "prior-generation"]:
            raise ValueError("Evidence and generation name different inherited inputs")
        result = {"state": "qualified-local-complete-family-not-published", "family": FAMILY,
            "capturedIndexSha256": index_digest, "bindingMapSha256": bindings_digest,
            "prior": entry, "candidate": {**artifact.pin.as_dict(), "directory": str(output / "generation")},
            "preservedMainRows": counts, "preservedAcceptedReceiptRows": preserved,
            "outcomes": new_reader.outcome_counts(), "sourceEvidence": {**admitted.pin.as_dict(),
                "directory": str(evidence.artifact_dir)}, "sourceAcquisition": False, "remoteGets": 0, "published": False,
            "rawInputHandling": "Literal selected receipt mappings retained; no legacy column expansion or raw rebuild.",
            "carriedMainBytes": ["gao_decisions.parquet"],
            "receiptOrdering": "Dataset logs follow GaoReportsRollup policy order. Accepted rows follow main order; nonaccepted rows retain their dataset order after accepted rows. The exact prior retains the original global receipt order."}
    (output / "preparation.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("captured-index", "member-bindings", "reports", "decisions", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("expected-prior", "expected-index", "expected-bindings"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(**vars(args)), indent=2))


if __name__ == "__main__":
    main()
