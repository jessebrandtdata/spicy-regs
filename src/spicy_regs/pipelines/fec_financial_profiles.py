"""Build ordinary FEC profiles offline from exact selected summary bytes."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence, without_local_paths
from spicy_regs.sources import publication
from spicy_regs.transforms.build_fec_financial_profiles import OUTPUT, PARENT, SCHEMA, TABLE, build_profiles, json_bytes


def build(selection, output_dir):
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh directory for each financial-profile generation")
    index_bytes = Path(selection["publication"]["path"]).read_bytes()
    if "sha256:" + hashlib.sha256(index_bytes).hexdigest() != selection["publication"]["sha256"]:
        raise ValueError("Profile publication control differs from selected digest")
    snapshot = publication.parse_index(index_bytes)
    evidence = CaptureEvidence(output_dir, "fec-financial-profiles")
    try:
        # The selection names where this machine read its inputs; the published copy keeps their digests (ss-81bv).
        evidence.retain_bytes(json_bytes(without_local_paths(selection)), stage="profile-selection")
        evidence.retain_bytes(index_bytes, stage="profile-parent-publication")
        table = output_dir / OUTPUT
        summary = build_profiles(selection, snapshot, table)
        evidence.retain_bytes(json_bytes(summary), stage="profile-population-reconciliation")
        artifact = build_generation(
            output_dir / "generation",
            family="fec-financial-profiles",
            files=(table,),
            expected_keys=(OUTPUT,),
            schemas={TABLE: described_schema(SCHEMA)},
            inputs=evidence.inputs(),
            read_snapshot=snapshot,
            parents={PARENT: publication.table_pin(snapshot, PARENT)},
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    artifact, evidence = build(json.loads(args.selection.read_text()), args.output_dir)
    print(
        json.dumps(
            {
                "directory": str(args.output_dir / "generation"),
                "sourceEvidence": str(evidence),
                "logicalId": artifact.pin.logical_id,
                "artifactDigest": artifact.pin.artifact_digest,
            }
        )
    )


if __name__ == "__main__":
    main()
