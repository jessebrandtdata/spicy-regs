"""Prepare one additive U.S. Code generation from held, accepted native outputs.

The prior's rows and the accepted candidate's rows are unioned unchanged, in order,
into files the maintained generation builder adopts. The tables publish no per-row
receipts (``receipts_retired``): a prior's receipt member stays in that generation,
which docs/pins/receipt-archive.json keeps. The candidate's selected originals are
retained once into the generation's source evidence and the shared raw archive,
under their publisher URL. This command never publishes; publication must still
use the captured exact prior (``publish-local-generation --expect-prior``). Row
batches do not impose a byte bound; the caller must supervise RSS, output and
elapsed time for the selected inputs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import stat

import pyarrow.parquet as pq
from rulespec_artifacts import ArtifactInput, ArtifactPin

from spicy_regs import generations
from spicy_regs.native_types import described_schema
from spicy_regs.source_evidence import CaptureEvidence, PRIOR_ROLE
from spicy_regs.sources import publication
from spicy_regs.sources.uscode_editions import CAPTURE_STAGE, FAMILY, POLICIES

BATCH_ROWS = 64
METADATA_LIMIT = 8 * 1024**2
ROW_GROUP_ROWS = 8192
ROW_GROUP_BYTES = 64 * 1024**2


def _stat(path: Path) -> tuple:
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError("Held inputs must not use symlink paths")
    info = path.stat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("Held input must be a regular file")
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_nlink


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _json(path: Path, expected: str | None = None, size: int | None = None) -> dict:
    before = _stat(path)
    if before[2] > METADATA_LIMIT or (size is not None and before[2] != size):
        raise ValueError("Accepted metadata byte bound differs")
    raw = path.read_bytes()
    if _stat(path) != before or (expected is not None and "sha256:" + hashlib.sha256(raw).hexdigest() != expected):
        raise ValueError("Accepted metadata pin changed")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("Accepted metadata must be an object")
    return value


def _batches(paths, schema):
    for path in paths:
        with pq.ParquetFile(path) as parquet:
            if not parquet.schema_arrow.equals(schema, check_metadata=True):
                raise ValueError(f"Held schema differs: {path.name}")
            yield from parquet.iter_batches(batch_size=BATCH_ROWS, use_threads=False)


def _union(paths, schema, target, *, identities=None, dataset=None):
    from spicy_regs.fec_observation_segments import coalesced_tables

    # Parquet normalizes list child names. Keep the admitted physical schema,
    # including metadata, while requiring the maintained logical policy type.
    physical = pq.read_schema(paths[0])
    if not physical.equals(schema):
        raise ValueError("Held schema differs from its policy")
    def checked_batches():
        for batch in _batches(paths, physical):
            if identities is not None:
                if dataset is None:
                    raise ValueError("Identity admission requires an explicit dataset")
                policy = POLICIES[dataset]
                columns = [batch.column(batch.schema.get_field_index(key)).to_pylist() for key in policy.identity_fields]
                try:
                    identities.executemany(
                        "INSERT INTO identities VALUES (?, ?)",
                        ((dataset, json.dumps(values)) for values in zip(*columns)),
                    )
                except sqlite3.IntegrityError as error:
                    raise ValueError(f"Overlapping or duplicate U.S. Code identity: {dataset}") from error
            yield batch

    # Bound the output group separately from small read batches. A 64 MiB
    # group also admits the accepted XML record bound; external RSS supervision
    # still owns native allocations and temporary Arrow backing buffers.
    with pq.ParquetWriter(target, physical) as writer:
        for table in coalesced_tables(checked_batches(), physical, check_resources=lambda: None,
                                      row_group_rows=ROW_GROUP_ROWS, max_bytes=ROW_GROUP_BYTES):
            writer.write_table(table, row_group_size=ROW_GROUP_ROWS)


def _check_ordered_union(paths, target, schema):
    """Compare bounded Arrow slices across arbitrary input/output row-group boundaries."""
    physical = pq.read_schema(paths[0])
    if not physical.equals(schema):
        raise ValueError("Held schema differs from its policy")
    expected, actual = iter(_batches(paths, physical)), iter(_batches([target], physical))
    left, right = next(expected, None), next(actual, None)
    while left is not None and right is not None:
        count = min(left.num_rows, right.num_rows)
        if not left.slice(0, count).equals(right.slice(0, count)):
            raise ValueError("Union changed an input value or occurrence order")
        left = next(expected, None) if count == left.num_rows else left.slice(count)
        right = next(actual, None) if count == right.num_rows else right.slice(count)
    if left is not None or right is not None:
        raise ValueError("Union omitted or added input occurrences")


def _publisher_url(source: dict) -> str:
    """Where OLRC serves one selected original; the journal is published, so it names this, never a local path."""
    from spicy_docs.sources.uscode import (
        ReleasePoint, TitleSelection, annual_archive_locator, corpus_xml_locator, title_xml_locator,
    )

    if source["kind"] == "annual":
        return annual_archive_locator(source["year"])
    point = ReleasePoint.from_label(source["release_point"])
    if source["kind"] == "release-point-corpus":
        return corpus_xml_locator(point)
    return title_xml_locator(TitleSelection(point, source["title"]))


def prepare_addition(
    *, prior_directory: Path, candidate_directory: Path, audit_pin: dict,
    read_snapshot: dict, expected_prior: str, output: Path,
) -> dict:
    """Append one accepted selected edition to the complete captured native family.

    ``audit_pin`` is the coordinator's exact path/bytes/sha256 for the accepted
    independent audit. Its ``evidence_sha256`` map must bind the build metadata,
    selection and all candidate subject files. The prior may still carry the
    receipts of the generations before retirement; they are not read or carried.
    Failed working files stay readable; an existing output is never replaced.
    """
    prior_directory, candidate_directory, output = map(Path, (prior_directory, candidate_directory, output))
    if output.exists() or any(output.resolve().is_relative_to(p.resolve()) for p in (prior_directory, candidate_directory)):
        raise ValueError("Additive preparation requires a fresh separate output")
    if any(p.is_symlink() for p in (output, *output.parents)):
        raise ValueError("Output must not use symlink paths")
    publication.parse_index(json.dumps(read_snapshot).encode())
    prior = read_snapshot["families"].get(FAMILY)
    if prior is None or prior["artifactDigest"] != expected_prior:
        raise ValueError("Selected U.S. Code prior differs")
    keys = {name + ".parquet" for name in POLICIES}
    if set(prior["tables"]) != keys or set(prior.get("etlReceipts", {}).get("datasets", POLICIES)) != set(POLICIES):
        raise ValueError("Selected U.S. Code family membership differs")
    old = generations.verify_generation(prior_directory, expected_pin=ArtifactPin(prior["logicalId"], expected_prior))
    if old.root["spec"]["family"] != FAMILY or old.root["spec"]["publicationStatus"] != "complete-family":
        raise ValueError("Held prior is not the complete selected U.S. Code family")
    for key, descriptor in old.root["spec"]["tables"].items():
        if descriptor != {k: v for k, v in prior["tables"][key].items() if k not in {"sha256", "byteSize"}}:
            raise ValueError("Selected prior schema or population differs")
    audit = _json(Path(audit_pin["path"]), audit_pin["sha256"], audit_pin["bytes"])
    if audit.get("verdict") != "APPROVE" or not str(audit.get("status", "")).startswith("verified-complete-selected-"):
        raise ValueError("Candidate requires accepted complete independent audit")
    evidence_pins = audit["evidence_sha256"]
    build_path, selection_path = candidate_directory / "rebuild.json", candidate_directory / "selection.json"
    build = _json(build_path, evidence_pins[str(build_path)])
    selection = _json(selection_path, evidence_pins[str(selection_path)])
    if (build.get("status") != "source-only-built" or build["generation"] != audit["generation"]
            or set(build["outputs"]) != set(POLICIES) or build["baseline_inputs"] != []
            or {n: v["rows"] for n, v in build["outputs"].items()} != audit["rows"]):
        raise ValueError("Candidate build differs from its accepted complete audit")
    if not selection.get("sources") or _digest(selection_path) != audit["selection_sha256"]:
        raise ValueError("Accepted candidate selection differs")
    held, pins = {}, {}
    for name in POLICIES:
        path = candidate_directory / build["outputs"][name]["path"]
        if not path.resolve().is_relative_to(candidate_directory.resolve()):
            raise ValueError("Candidate member escaped its accepted directory")
        held[name] = (prior_directory / (name + ".parquet"), path)
        for member, expected in zip(held[name], (prior["tables"][name + ".parquet"], build["outputs"][name])):
            before = _stat(member)
            if _digest(member) != expected["sha256"] or _stat(member) != before:
                raise ValueError("Held subject differs from its selected or accepted pin")
            if member == path and evidence_pins[str(member)] != expected["sha256"]:
                raise ValueError("Candidate subject is not bound by accepted audit")
            if "byteSize" in expected and before[2] != expected["byteSize"]:
                raise ValueError("Selected subject size differs")
            if not pq.read_schema(member).equals(POLICIES[name].subject_schema):
                raise ValueError(f"Held schema differs from its policy: {name}")
            pins[member] = before
    originals = build["inputs"]
    if [source["sha256"] for source in originals] != [source["sha256"] for source in selection["sources"]]:
        raise ValueError("Candidate build read other originals than its selection")
    output.mkdir(parents=True)
    materialized = output / "union"
    materialized.mkdir()
    with sqlite3.connect(output / "identities.sqlite") as identities:
        identities.execute("CREATE TABLE identities (dataset TEXT, identity TEXT, PRIMARY KEY(dataset, identity))")
        for name, policy in POLICIES.items():
            target = materialized / (name + ".parquet")
            _union(held[name], policy.subject_schema, target, identities=identities, dataset=name)
            _check_ordered_union(held[name], target, policy.subject_schema)
    source_evidence = CaptureEvidence(output, FAMILY)
    source_evidence.read_snapshot = read_snapshot
    source_evidence.prior_input = ArtifactInput(PRIOR_ROLE, prior["logicalId"], expected_prior)
    # The accepted originals, once each: the retained bytes must be the ones the build and the audit read.
    for source in originals:
        retained = source_evidence.retain_file(Path(source["path"]), stage=CAPTURE_STAGE, raw_source=True,
                                               original_location=_publisher_url(source))
        if retained["sha256"] != source["sha256"]:
            raise ValueError("Retained original differs from the accepted selection")
    source_evidence.event("held-native-addition", prior_generation=prior, prior_inputs=old.root["inputs"],
                          accepted_generation=build["generation"],
                          audit={k: audit_pin[k] for k in ("bytes", "sha256")},
                          selection_sha256=audit["selection_sha256"],
                          sources=[{k: v for k, v in source.items() if k != "path"} for source in selection["sources"]],
                          source_reads=len(originals), new_capture_claim=False,
                          evidence_status="Prior rows held unchanged; the addition's originals retained above.")
    files = [materialized / (name + ".parquet") for name in POLICIES]
    adopted = {p.name: (p.stat().st_dev, p.stat().st_ino) for p in files}
    artifact = generations.build_generation(
        output / "generation", family=FAMILY, files=files, expected_keys=sorted(keys),
        schemas={name: described_schema(policy.subject_schema) for name, policy in POLICIES.items()},
        read_snapshot=read_snapshot, inputs=source_evidence.inputs(), owned_output_root=output,
    )
    for path, before in pins.items():
        if _stat(path) != before:
            raise ValueError("Retained input changed during additive preparation")
    for name, identity in adopted.items():
        installed = output / "generation" / name
        if (installed.stat().st_dev, installed.stat().st_ino) != identity:
            raise ValueError("Generation adoption copied a materialized member")
    source_evidence.finish()
    result = {"status": "prepared-additive-uscode-generation", "generation": str(output / "generation"),
              "artifactDigest": artifact.pin.artifact_digest, "capturedPrior": expected_prior,
              "acceptedAddition": build["generation"], "evidence": str(source_evidence.artifact_dir),
              "orderedPopulationEquality": True, "payloadCopies": 0, "sourceReads": len(originals),
              "published": False}
    (output / "prepared.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("prior", "candidate", "audit", "index", "output"):
        parser.add_argument("--" + flag, type=Path, required=True)
    parser.add_argument("--audit-sha256", required=True)
    parser.add_argument("--audit-bytes", type=int, required=True)
    parser.add_argument("--expected-prior", required=True)
    args = parser.parse_args()
    print(json.dumps(prepare_addition(prior_directory=args.prior, candidate_directory=args.candidate,
        audit_pin={"path": str(args.audit), "sha256": args.audit_sha256, "bytes": args.audit_bytes},
        read_snapshot=_json(args.index), expected_prior=args.expected_prior, output=args.output)))


if __name__ == "__main__":
    main()
