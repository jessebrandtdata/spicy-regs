"""Build exact scorecard links from one captured published index; no source reads."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from os import getenv
from uuid import uuid4

from dotenv import load_dotenv

from spicy_regs.scorecards.inputs import stage_member
from spicy_regs.scorecards.etl import ANALYSIS_NAMES, generation_options
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.scorecards.analysis_inputs import (
    PROJECTED_NATIVE_OFFICIAL_TABLES, convert_published_official_input, published_input,
)
from spicy_regs.sources import publication, r2
from spicy_regs.transforms.build_scorecard_analysis import (
    INPUTS, OFFICIAL_TABLES, OUTPUTS, analysis_input_entries, build_scorecard_analysis,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--local-inputs", type=Path, action="append", default=[])
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--convert-published-official-inputs", action="store_true",
                        help="Explicitly convert pinned published official shaped inputs into verified private native bundles")
    args = parser.parse_args(argv)
    if args.output.exists():
        raise ValueError("Use a fresh analysis directory")
    args.output.mkdir(parents=True)
    load_dotenv(args.env_file)
    client = r2.get_r2_client()
    snapshot, _ = publication._stored_index(client, "spicy-regs")
    entries = analysis_input_entries(snapshot, convert_published_official_inputs=args.convert_published_official_inputs)
    project_official = all("etlReceipts" in entries[name] or published_input(entries[name], name)
                           for name in PROJECTED_NATIVE_OFFICIAL_TABLES)
    index_path = args.output / "publication.v2.json"
    index_path.write_text(json.dumps(snapshot, indent=2) + "\n")
    paths, pins, parents = {}, {}, {}
    source_entry = entries["scorecards"]
    source_receipt = None
    source_generation = None
    receipt_inputs = {}
    if "etlReceipts" in source_entry:
        source_generation = source_entry["etlReceipts"]["generationId"]
        receipt_inputs["source-etl-receipts.parquet"] = ("scorecards", source_generation)
    for name in OFFICIAL_TABLES:
        if project_official and name in PROJECTED_NATIVE_OFFICIAL_TABLES:
            continue
        entry = entries[name]
        if "etlReceipts" in entry:
            receipt_inputs[name + "-etl-receipts.parquet"] = (name, entry["etlReceipts"]["generationId"])
    def download(member, target):
        response = client.get_object(Bucket="spicy-regs", Key=member.path)
        count = 0
        with target.open("xb") as output, response["Body"] as body:
            while block := body.read(1024 * 1024):
                count += len(block)
                if count > member.byte_size:
                    raise ValueError("Published object exceeded its pinned byte count")
                output.write(block)
        return True

    for key in (*INPUTS, *receipt_inputs):
        name = key.removesuffix(".parquet")
        if key not in receipt_inputs:
            pins[name] = parents[key] = publication.table_pin(snapshot, key)
        paths[name] = []
        members = (
            publication.receipt_members(snapshot, dataset=receipt_inputs[key][0])
            if key in receipt_inputs
            else publication.table_members(snapshot, key)
        )
        for member in members:
            if member.sha256 is None or member.byte_size is None:
                raise ValueError("Analysis requires immutable published inputs")
            target = args.output / "inputs" / (key if key in receipt_inputs else member.key)
            stage_member(member, target, local_inputs=args.local_inputs, fetch=download)
            paths[name].append(target)
    if source_generation:
        source_receipt = paths.pop("source-etl-receipts")[0]
    official_receipts = {
        name: (paths.pop(key.removesuffix(".parquet"))[0], generation)
        for key, (name, generation) in receipt_inputs.items() if name in OFFICIAL_TABLES
    }
    conversions = []
    conversion_generation = "published-official-conversion:" + uuid4().hex
    for name in OFFICIAL_TABLES:
        if project_official and name in PROJECTED_NATIVE_OFFICIAL_TABLES:
            continue
        if name not in official_receipts and not published_input(entries[name], name):
            selected, proof = convert_published_official_input(
                snapshot, name, paths[name], args.output / "official-conversions" / name,
                generation_id=conversion_generation,
                published_url=getenv("R2_PUBLIC_URL", "https://data.spicygov.ai"),
            )
            paths[name] = list(selected.subjects)
            official_receipts[name] = selected.receipts, selected.generation_id
            conversions.append(proof)
    if conversions:
        (args.output / "published-official-conversion.json").write_text(json.dumps(conversions, indent=2) + "\n")
    files = build_scorecard_analysis(
        args.output,
        input_pins=pins,
        input_paths=paths,
        comparison_index=snapshot,
        source_receipt_path=source_receipt,
        source_generation_id=source_generation,
        official_receipts=official_receipts,
        projected_official_index=snapshot if project_official else None,
    )
    evidence = CaptureEvidence(args.output, "scorecard-analysis")
    evidence.inherit(snapshot, public_url="https://data.spicygov.ai")
    evidence.event("verified-published-inputs", input_pins=parents, source_network_requests=0)
    if conversions:
        evidence.event("verified-published-shaped-conversion", conversions=conversions, source_network_requests=0)
    artifact = build_generation(
        args.output / "generation",
        family="scorecard-analysis",
        files=files,
        expected_keys=OUTPUTS,
        **generation_options(args.output, ANALYSIS_NAMES),
        read_snapshot=snapshot,
        parents=parents,
        inputs=evidence.inputs(),
    )
    # build_generation already returns the fully verified artifact. Preserve
    # that result through preparation instead of repeating its receipt joins.
    verify_evidence(evidence.artifact_dir)
    qualification = json.loads((args.output / "scorecard-analysis-qualification.json").read_bytes())
    report = {
        "status": "prepared_not_published",
        "generation": artifact.pin.as_dict(),
        "generation_directory": str(args.output / "generation"),
        "evidence_directory": str(evidence.artifact_dir),
        "read_snapshot_path": str(index_path),
        "source_network_requests": 0,
        "published_official_conversions": conversions,
        "qualification": qualification,
    }
    (args.output / "preparation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
