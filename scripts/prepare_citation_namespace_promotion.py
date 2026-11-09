"""Prepare a complete citation namespace promotion from an exact local native prior.

This caller never acquires source data, fetches remote members or publishes.
Payload execution requires a separately bounded operation; preparing this code
or supplying a custody map does not qualify a corpus or authorize execution.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from contextlib import ExitStack
from pathlib import Path

import pyarrow.parquet as pq
from rulespec_artifacts import iter_member_descriptors, parse_admitted_json

from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs.generations import build_generation
from spicy_regs.legislative_receipts import RAW
from spicy_regs.pipelines.rollups.print_citations import PrintCitationsRollup
from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
from spicy_regs.selected_generations import remember_generation
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.sources import publication

FAMILY = "print-citations"
CONTROL_LIMIT = 2 * 1024**2


def _json(path: Path) -> tuple[dict, str]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > CONTROL_LIMIT:
        raise ValueError("A bounded regular JSON control file is required")
    with path.open("rb") as stream:
        raw = stream.read(CONTROL_LIMIT + 1)
    if len(raw) > CONTROL_LIMIT:
        raise ValueError("JSON control exceeds its bound")
    value = parse_admitted_json(raw)
    if not isinstance(value, dict):
        raise ValueError("JSON control must be an object")
    return value, "sha256:" + hashlib.sha256(raw).hexdigest()


def _stage_prior(controls: Path, bindings: dict, entry: dict, destination: Path) -> list[dict]:
    """Private hardlink aliases only; never alter a retained input inode."""
    selected_index = publication.empty_index()
    selected_index["families"][FAMILY] = entry
    members = [member for key in entry["tables"]
               for member in publication.table_members(selected_index, key)]
    members.extend(publication.receipt_members(selected_index))
    expected = {member.key: member for member in members}
    selected = [item for item in bindings.get("bindings", ()) if item.get("family") == FAMILY]
    mapped = {item["objectKey"]: item for item in selected}
    if len(mapped) != len(selected) or set(mapped) != set(expected):
        raise ValueError("Exact complete selected member bindings are required")
    destination.mkdir()
    for key in ("artifact.json", "members.json"):
        source = controls / key
        _json(source)
        os.link(source, destination / key)
    aliases = []
    for key, member in expected.items():
        item = mapped[key]
        source = Path(item["canonical_path"])
        if (item.get("artifactDigest") != entry["artifactDigest"]
                or item.get("sha256") != member.sha256 or item.get("bytes") != member.byte_size
                or source.is_symlink() or not source.is_file() or source.stat().st_size != member.byte_size):
            raise ValueError("Local custody binding differs from the selected member")
        if Path(key).name != key:
            raise ValueError("This promotion requires unsplit plain member names")
        os.link(source, destination / key)
        aliases.append({"objectKey": key, "source": str(source.resolve()),
                        "alias": str(destination / key), "sha256": member.sha256,
                        "bytes": member.byte_size})
    return aliases


def _same_main(before: Path, after: Path) -> int:
    """Compare native values/order while allowing only the new namespace column."""
    with pq.ParquetFile(before) as old, pq.ParquetFile(after) as new:
        names = old.schema_arrow.names
        if [name for name in new.schema_arrow.names if name != "target_table"] != names:
            raise ValueError("Promotion changes the original native column order")
        if old.metadata.num_rows != new.metadata.num_rows:
            raise ValueError("Promotion changes the original native row count")
        batches = iter(new.iter_batches(batch_size=2000, columns=names))
        count = 0
        for expected in old.iter_batches(batch_size=2000):
            actual = next(batches, None)
            if actual is None or not expected.equals(actual):
                raise ValueError("Promotion changes original native values or row order")
            count += expected.num_rows
        if next(batches, None) is not None:
            raise ValueError("Promotion adds native rows")
        return count


def prepare(*, captured_index: Path, prior_controls: Path, member_bindings: Path,
            output: Path, expected_prior: str, expected_index: str) -> dict:
    """Replay one complete admitted native family, explicitly retaining its captured prior."""
    snapshot, index_digest = _json(captured_index)
    if index_digest != expected_index:
        raise ValueError("Captured index differs from its expected digest")
    snapshot = publication.parse_index(json.dumps(snapshot).encode())
    entry = snapshot["families"][FAMILY]
    if entry["artifactDigest"] != expected_prior or "etlReceipts" not in entry:
        raise ValueError("An exact selected native citation prior is required")
    expected_keys = set(PrintCitationsRollup.outputs)
    if set(entry["tables"]) != expected_keys:
        raise ValueError("Selected prior does not contain the complete citation family")
    if "target_table" in dict(entry["tables"]["document_citations.parquet"]["columns"]):
        raise ValueError("Selected prior already exposes its citation namespace")
    bindings, bindings_digest = _json(member_bindings)
    held = [prior_controls, captured_index, member_bindings,
            *(Path(item["canonical_path"]) for item in bindings.get("bindings", ())
              if item.get("family") == FAMILY)]
    if any(output.resolve().is_relative_to(path.resolve())
           or path.resolve().is_relative_to(output.resolve()) for path in held):
        raise ValueError("Output must be separate from retained inputs and controls")
    output.mkdir(parents=True, exist_ok=False)
    aliases = _stage_prior(prior_controls, bindings, entry, output / "prior")
    with ConversionReadOperation() as operation, ExitStack() as evidence_run:
        old, old_reader, old_source = operation.admit_generation(output / "prior", expected_pin={
            "logicalId": entry["logicalId"], "artifactDigest": entry["artifactDigest"]})
        described = publication.generation_entry(old, iter_member_descriptors(old, old_source))
        if described != {key: value for key, value in entry.items() if key != "publishedAt"}:
            raise ValueError("Admitted local prior differs from the complete captured entry")
        if old.root["spec"]["publicationStatus"] != "complete-family":
            raise ValueError("A partial prior cannot qualify complete-family promotion")
        if [item["role"] for item in old.root["inputs"]].count("source-evidence") != 1:
            raise ValueError("The exact selected prior source-evidence binding is required")
        selection = output / "selection"
        remember_generation(selection, output / "prior", old)
        prior = SelectedPriors(output / "priors", root=selection, index=None, public_url="")
        originals = {}
        for key in PrintCitationsRollup.source_outputs:
            dataset = key.removesuffix(".parquet")
            source = prior.get(dataset)
            if source is None:
                raise ValueError("Complete original producer input is unavailable")
            originals[dataset] = source
            old_reader.compare_original(dataset, source)
        writer = PrintCitationsRollup(skip_upload=True)
        work = output / "writer"
        work.mkdir()
        private = output / "writer-private"
        private.mkdir()
        writer._primed_receipt_work = work, private, prior

        def retained_tables(_directory, **_kwargs):
            # get() keeps native-prior lineage on the maintained writer's actual selector.
            return tuple(prior.get(key.removesuffix(".parquet"))
                         for key in PrintCitationsRollup.source_outputs)

        files = writer.build_receipts(work, retained_tables)
        evidence = CaptureEvidence(output, FAMILY)
        evidence_run.push(lambda _kind, error, _trace: evidence.finish(error))
        evidence.inherit_admitted(snapshot, old)
        evidence.event("native-promotion", source_acquisition=False,
                       change="Expose the retained literal citation target namespace on main rows.")
        inputs = evidence.inputs()
        artifact = build_generation(output / "generation", family=FAMILY, files=files,
            expected_keys=PrintCitationsRollup.outputs, schemas=writer.generation_schemas(),
            read_snapshot=snapshot, publication_status="complete-family", inputs=inputs,
            receipt_path=work / "etl_receipts.parquet", receipt_policies=writer.receipt_policies,
            receipt_generation_id=writer.receipt_generation_id, adopt_owned_receipt=True,
            owned_output_root=output, read_operation=operation)
        _, new_reader, _ = operation.admit_generation(output / "generation", expected_pin=artifact.pin)
        original_checks = {name: new_reader.compare_original(name, path)
                           for name, path in originals.items()}
        main_counts = {name: _same_main(output / "prior" / (name + ".parquet"),
                                       output / "generation" / (name + ".parquet"))
                       for name in originals if name != "document_citation_reads"}
        namespace_count = 0
        for row in new_reader.read_subjects("document_citations"):
            if row["target_table"] != row[RAW]["target_table"]:
                raise ValueError("Promoted namespace differs from its retained literal value")
            namespace_count += 1
        if namespace_count != main_counts["document_citations"]:
            raise ValueError("Namespace qualification omitted citation rows")
        old_reader.check()
        new_reader.check()
        assert evidence.artifact is not None
        admitted_evidence = verify_evidence(evidence.artifact_dir, expected_pin=evidence.artifact.pin)
        if admitted_evidence.root["inputs"] != [item for item in artifact.root["inputs"]
                                                if item["role"] == "prior-generation"]:
            raise ValueError("Source evidence and generation name different inherited inputs")
        report = {"state": "qualified-local-complete-family-not-published", "family": FAMILY,
                  "capturedIndexSha256": index_digest, "bindingMapSha256": bindings_digest,
                  "prior": entry, "candidate": {"logicalId": artifact.pin.logical_id,
                      "artifactDigest": artifact.pin.artifact_digest,
                      "directory": str((output / "generation").resolve())},
                  "originalChecks": original_checks, "preservedMainRows": main_counts,
                  "literalNamespacesChecked": namespace_count, "inputAliases": aliases,
                  "sourceEvidence": {**admitted_evidence.pin.as_dict(),
                                     "directory": str(evidence.artifact_dir.resolve())},
                  "sourceAcquisition": False, "remoteGets": 0, "published": False,
                  "priorStagingPayloadCopies": 0,
                  "privateWorkingCopies": "The maintained selector copies native subjects and writes restored inputs; the writer retains intermediate subjects and receipts beside the final generation.",
                  "supportingOutcomes": "Regenerated by the maintained writer; original outcomes remain in the exact prior."}
    # Retain failed artifacts without a success seal; only completed checks write this report.
    with (output / "preparation.json").open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--captured-index", required=True, type=Path)
    parser.add_argument("--prior-controls", required=True, type=Path)
    parser.add_argument("--member-bindings", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--expect-prior", required=True)
    parser.add_argument("--expect-index", required=True)
    args = parser.parse_args()
    report = prepare(captured_index=args.captured_index, prior_controls=args.prior_controls,
        member_bindings=args.member_bindings, output=args.output,
        expected_prior=args.expect_prior, expected_index=args.expect_index)
    print(json.dumps({"state": report["state"], "candidate": report["candidate"]}))


if __name__ == "__main__":
    main()
