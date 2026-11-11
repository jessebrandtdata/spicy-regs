"""Acquire OFAC enhanced exports into the raw archive, then build list history from selected snapshots.

A capture selection names each snapshot as its OFAC scope: the selected list and the request URL, SHA-256,
observation time and size of its ZIP, as the raw acquisition receipt records them. The build admits one
source release per scope from the raw original; releases admitted earlier can be selected instead.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import (
    add_snapshot_build, download_original, inherit_read_index, journal_snapshot_releases, snapshot_inputs,
)
from spicy_regs.source_evidence import CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_ofac import SCHEMAS, build_ofac

STAGE = "ofac-source-archive"
RELEASE_EVENT = "ofac-source-release"


def capture(evidence, selected_lists, *, transport=None) -> list[dict]:
    """Capture each selected list's official export once through ``evidence``; returns their OFAC scopes."""
    from spicy_docs.sources import ofac

    scopes = []
    for selected in selected_lists:
        descriptor = download_original(
            evidence, lambda acquirer, store, selected=selected: ofac.acquire_snapshot(acquirer, selected, store=store),
            stage=STAGE, max_bytes=ofac.MAX_ZIP_BYTES, transport=transport,
            validate_url=lambda url, selected=selected: ofac.validate_download_url(url, selected_list=selected),
        )
        scopes.append(ofac.ofac_scope(descriptor, selected))
    return scopes


def acquire(selected_lists, output_dir: Path, *, transport=None) -> dict:
    """Capture the selected exports into the raw archive; returns the capture selection from the run's receipts."""
    with acquisition_evidence("ofac-sanctions", output_dir=Path(output_dir)) as evidence:
        return {"releases": capture(evidence, selected_lists, transport=transport)}


def build(readers, output_dir: Path, *, archives, read_index: Path | None = None):
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each OFAC generation")
    readers = tuple(readers)
    evidence = CaptureEvidence(output_dir, "ofac-sanctions")
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        retained = set()
        for reader in readers:
            capture = reader.collection_outcome["requestedScope"]["capture"]
            ref = capture["response_sha256"]
            if ref not in retained:
                original = Path(archives[ref])
                if original.stat().st_size != capture["byte_size"]:
                    raise ValueError("OFAC original differs from the selected size")
                # The request URL, not the local path: this journal is published.
                saved = evidence.retain_file(original, stage=STAGE, original_location=capture["request_url"])
                if saved.get("sha256") != ref:
                    raise ValueError("OFAC original differs from the admitted digest")
                retained.add(ref)
        journal_snapshot_releases(evidence, readers, RELEASE_EVENT)
        tables = build_ofac(readers, output_dir)
        artifact = build_generation(
            output_dir / "generation",
            family="ofac-sanctions",
            files=tables,
            expected_keys=tuple(table + ".parquet" for table in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
            read_snapshot=read_snapshot,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    from spicy_docs.sources.ofac_profile import OFAC_PROFILE, iter_retained_pages

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("acquire", help="Capture official exports into the raw archive")
    capture.add_argument("--list", action="append", required=True, choices=("sdn", "consolidated"),
                         dest="lists", help="Selected list, repeatable")
    capture.add_argument("--output-dir", required=True, type=Path)
    capture.add_argument("--captures-out", required=True, type=Path, help="Where to write the capture selection")
    add_snapshot_build(commands)
    args = parser.parse_args()
    if args.command == "acquire":
        selection = acquire(args.lists, args.output_dir)
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"captures": str(args.captures_out), "releases": selection["releases"]}))
        return
    readers, archives = snapshot_inputs(args, OFAC_PROFILE, iter_retained_pages)
    artifact, evidence = build(readers, args.output_dir, archives=archives, read_index=args.read_index)
    print(json.dumps({
        "directory": str(args.output_dir / "generation"),
        "sourceEvidence": str(evidence),
        "logicalId": artifact.pin.logical_id,
        "artifactDigest": artifact.pin.artifact_digest,
    }))


if __name__ == "__main__":
    main()
