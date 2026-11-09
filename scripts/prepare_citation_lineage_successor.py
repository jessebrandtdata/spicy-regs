"""Create fresh citation lineage around unchanged, already prepared native members.

No converter, source acquisition or publication runs here. The exact prepared
artifact and new successor both undergo maintained full byte/receipt admission.
Only the selected prior root is read for inheritance, as in normal remote
CaptureEvidence.inherit; its payloads are not re-admitted by this operation.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from rulespec_artifacts import iter_member_descriptors

from scripts.prepare_citation_namespace_promotion import CONTROL_LIMIT, FAMILY, _json
from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs.etl_policy_registry import require_registered_receipts
from spicy_regs.generations import _write_generation_metadata
from spicy_regs.pipelines.rollups.print_citations import PrintCitationsRollup
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.sources import publication


def prepare_successor(*, captured_index: Path, prior_controls: Path, prepared: Path,
                      output: Path, expected_prior: str, expected_prepared: str,
                      expected_index: str) -> dict:
    snapshot, index_digest = _json(captured_index)
    if index_digest != expected_index:
        raise ValueError("Captured index differs from its expected digest")
    snapshot = publication.parse_index(json.dumps(snapshot).encode())
    prior = snapshot["families"][FAMILY]
    if prior["artifactDigest"] != expected_prior:
        raise ValueError("Selected prior differs from its expected pin")
    root, prepared_control_digest = _json(prepared / "artifact.json")
    if root["artifactDigest"] != expected_prepared or root["spec"]["family"] != FAMILY:
        raise ValueError("Prepared candidate differs from its expected pin or family")
    prior_input = {"role": "prior-generation", "logicalId": prior["logicalId"],
                   "artifactDigest": prior["artifactDigest"]}
    if [item for item in root["inputs"] if item["role"] == "prior-generation"] != [prior_input]:
        raise ValueError("Prepared candidate names a different inherited prior")
    spec = root["spec"]
    if (spec["publicationStatus"] != "complete-family"
            or set(spec["tables"]) != set(PrintCitationsRollup.outputs)
            or "target_table" not in dict(spec["tables"]["document_citations.parquet"]["columns"])):
        raise ValueError("A complete prepared native namespace family is required")
    require_registered_receipts(spec["tables"], spec.get("etlReceipts"))
    for path in (captured_index, prior_controls, prepared):
        if output.resolve().is_relative_to(path.resolve()) or path.resolve().is_relative_to(output.resolve()):
            raise ValueError("Successor output must be separate from retained inputs")
    prior_root_path = prior_controls / "artifact.json"
    _, prior_control_digest = _json(prior_root_path)
    with prior_root_path.open("rb") as stream:
        prior_raw = stream.read(CONTROL_LIMIT + 1)
    if len(prior_raw) > CONTROL_LIMIT:
        raise ValueError("Prior root exceeds its control bound")
    publication.family_root(prior_raw, prior)
    if len(root["memberManifests"]) != 1:
        raise ValueError("This citation successor requires its one ordinary member manifest")
    manifest_spec = root["memberManifests"][0]
    if manifest_spec["objectKey"] != "members.json":
        raise ValueError("Unexpected prepared member manifest")
    manifest, manifest_digest = _json(prepared / "members.json")
    if manifest_digest != manifest_spec["sha256"]:
        raise ValueError("Prepared manifest differs from its root pin")
    expected_members = set(spec["tables"]) | {"etl_receipts.parquet"}
    names = [item["objectKey"] for item in manifest["members"]]
    if len(set(names)) != len(names) or set(names) != expected_members:
        raise ValueError("Prepared physical members differ from complete family membership")
    output.mkdir(parents=True, exist_ok=False)
    generation = output / "generation"
    generation.mkdir()
    aliases = []
    # Alias creation changes inode ctime: do it before either live admission.
    for item in manifest["members"]:
        key = item["objectKey"]
        source = prepared / key
        if (Path(key).name != key or source.is_symlink() or not source.is_file()
                or source.stat().st_size != item["byteSize"]):
            raise ValueError("Prepared member requires an exact regular local file")
        os.link(source, generation / key)
        aliases.append({"key": key, "source": str(source.resolve()), "alias": str(generation / key),
                        "sha256": item["sha256"], "bytes": item["byteSize"]})
    evidence = CaptureEvidence(output, FAMILY)
    try:
        with ConversionReadOperation() as operation:
            candidate, reader, source = operation.admit_generation(prepared, expected_pin={
                "logicalId": root["logicalId"], "artifactDigest": expected_prepared})
            members = list(iter_member_descriptors(candidate, source))
            evidence.inherit_pinned_root(snapshot, prior_raw)
            evidence.event("native-lineage-successor", prepared_generation=candidate.pin.as_dict(),
                           source_acquisition=False, conversion_repeated=False,
                           members=[{"key": member.object_key, "sha256": member.sha256,
                                     "bytes": member.byte_size} for member in members])
            inputs = evidence.inputs()
            # This maintained metadata builder is also used by remote_generations;
            # it never supplies admission proof. Full standard admission follows.
            _write_generation_metadata(generation, family=FAMILY, tables=spec["tables"], members=members,
                read_snapshot=snapshot, carried_forward=spec["carriedForward"],
                publication_status=spec["publicationStatus"], inputs=inputs, parents=spec.get("parents"),
                etl_receipts=spec["etlReceipts"], extra_packages=tuple(sorted(set(spec["packages"]) - {
                    "spicy-regs", "spicy-docs", "rulespec-artifacts"})))
            successor, successor_reader, successor_source = operation.admit_generation(generation)
            if list(iter_member_descriptors(successor, successor_source)) != members:
                raise ValueError("Successor changes prepared payload descriptors")
            assert evidence.artifact is not None
            admitted_evidence = verify_evidence(evidence.artifact_dir, expected_pin=evidence.artifact.pin)
            if admitted_evidence.root["inputs"] != [item for item in successor.root["inputs"]
                                                     if item["role"] == "prior-generation"]:
                raise ValueError("Source evidence and generation name different inherited inputs")
            reader.check()
            successor_reader.check()
            report = {"state": "qualified-local-lineage-successor-not-published", "family": FAMILY,
                "capturedIndexSha256": index_digest, "priorRootControlSha256": prior_control_digest,
                "preparedRootControlSha256": prepared_control_digest, "prepared": candidate.pin.as_dict(),
                "candidate": {**successor.pin.as_dict(), "directory": str(generation.resolve())},
                "sourceEvidence": {**admitted_evidence.pin.as_dict(),
                                   "directory": str(evidence.artifact_dir.resolve())},
                "inputAliases": aliases, "payloadCopies": 0, "payloadMembersUnchanged": True,
                "fullNativeAdmissions": 2, "priorPayloadAdmissionRepeated": False,
                "conversionRepeated": False, "originalComparisonsRepeated": False,
                "sourceAcquisition": False, "remoteGets": 0, "published": False}
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    with (output / "preparation.json").open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("captured-index", "prior-controls", "prepared", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    for name in ("expect-prior", "expect-prepared", "expect-index"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    result = prepare_successor(captured_index=args.captured_index, prior_controls=args.prior_controls,
        prepared=args.prepared, output=args.output, expected_prior=args.expect_prior,
        expected_prepared=args.expect_prepared, expected_index=args.expect_index)
    print(json.dumps({"state": result["state"], "candidate": result["candidate"]}))


if __name__ == "__main__":
    main()
