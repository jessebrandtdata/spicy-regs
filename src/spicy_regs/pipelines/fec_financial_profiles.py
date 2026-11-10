"""Capture the FEC summary CSVs, and build the financial profiles from one captured pair.

``acquire`` saves each cycle's candidate and committee summary CSVs into the raw archive with their acquisition
receipts. FEC rewrites both files every day under the same URL, so a day not captured cannot be fetched again.
``build`` maps one cycle's captured pair straight from the raw archive (docs/fec-financial-profiles.md).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import READ_INDEX_HELP, download_original, inherit_read_index, raw_original
from spicy_regs.source_evidence import CaptureEvidence, acquisition_evidence
from spicy_regs.sources import publication
from spicy_regs.transforms.build_fec_financial_profiles import (
    MAX_SUMMARY_BYTES, MEASURES, OUTPUT, SCHEMA, TABLE, build_profiles, collection_id, json_bytes, select,
)

FAMILY = "fec-financial-profiles"
STAGE = "fec-summary-capture"
#: Where an append-only index of each published capture run goes, beside the shared evidence blobs.
INDEX_FAMILY = "fec-summaries"


def summary_url(kind: str, cycle: int) -> str:
    """The publisher's address of one cycle's summary CSV, as fec-observations captured it."""
    from spicy_docs.sources.fec.catalog import BUCKET_URL

    if kind not in MEASURES or type(cycle) is not int or cycle % 2:
        raise ValueError("A summary CSV is a candidate or committee file of an even election cycle")
    return f"{BUCKET_URL}bulk-downloads/{cycle}/{kind}_summary_{cycle}.csv"


def capture(evidence, cycles, *, transport=None) -> list[dict]:
    """Download each cycle's two summary CSVs through ``evidence``; returns each one's capture, as its receipt records it."""
    from spicy_docs.sources.fec.assets import validate_original_prefix
    from spicy_docs.sources.fec.catalog import official_url

    captures = []
    for cycle in cycles:
        for kind in MEASURES:
            url = summary_url(kind, cycle)
            descriptor = download_original(
                evidence,
                lambda acquirer, store, url=url: acquirer.download(
                    url, store=store, max_bytes=MAX_SUMMARY_BYTES,
                    validate_prefix=lambda chunk: validate_original_prefix(chunk, url=url)),
                stage=STAGE, max_bytes=MAX_SUMMARY_BYTES, validate_url=official_url, transport=transport,
            )
            captures.append({"entity_kind": kind, "cycle": cycle, "collection_id": collection_id(kind, cycle),
                             **descriptor})
    return captures


def _publish_capture(evidence, captures: list[dict]) -> dict:
    """Seal ``evidence`` and send it, its originals and an index of ``captures`` to R2's ``source-evidence/``.

    For a hosted runner, whose raw archive is scratch: retention never deletes ``source-evidence/``, and
    ``raw_releases.restore_originals`` reads a blob back into a raw archive by its digest.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    from spicy_regs.sources import r2

    r2.require_credentials("FEC summary capture")
    client, bucket = r2.verified_target_client()
    artifact = evidence.seal(outcome="build-complete")
    publication.publish_evidence(client, bucket, evidence.artifact_dir, artifact, archive_root=evidence.store.root)
    index = evidence.directory / "captures-index.parquet"
    rows = [{**{key: str(item[key]) for key in ("collection_id", "request_url", "response_sha256", "observed_at")},
             "byte_size": item["byte_size"], "artifact_digest": artifact.pin.artifact_digest} for item in captures]
    pq.write_table(pa.Table.from_pylist(rows), index, compression="zstd")
    return {"evidence": artifact.pin.artifact_digest,
            "index": publication.publish_evidence_index(client, bucket, INDEX_FAMILY, index)}


def acquire(cycles, output_dir: Path, *, transport=None, publish: bool = False) -> dict:
    """Capture the summary CSVs of ``cycles`` into the raw archive; returns the captures and, published, where."""
    with acquisition_evidence(FAMILY, output_dir=Path(output_dir)) as evidence:
        captures = capture(evidence, cycles, transport=transport)
        published = _publish_capture(evidence, captures) if publish else None
    return {"captures": captures, **({"published": published} if published else {})}


def profiles_table(evidence, captures: list[dict], output_dir: Path) -> Path:
    """Build the profiles of one captured pair, whose originals ``evidence`` holds, into ``output_dir``.

    The selection and the population reconciliation are journaled; the witnesses name the selection's digest.
    """
    selection = select(captures)
    selected = evidence.retain_bytes(json_bytes(selection), stage="profile-selection")
    table = Path(output_dir) / OUTPUT
    summary = build_profiles(selection, evidence.store, table, selection_sha256=selected["sha256"])
    evidence.retain_bytes(json_bytes(summary), stage="profile-population-reconciliation")
    return table


def build(captures: list[dict], output_dir: Path, *, read_index: Path | None = None):
    """Build a generation from one captured pair, citing both originals in the raw archive the capture used."""
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh directory for each financial-profile generation")
    evidence = CaptureEvidence(output_dir, FAMILY)
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        for item in captures:
            original = raw_original(evidence.store, item["response_sha256"], item["byte_size"])
            # The request URL, not the local path: this journal is published.
            saved = evidence.retain_file(original, stage=STAGE, raw_source=True, source_url=item["request_url"],
                                         original_location=item["request_url"], observed_at=item["observed_at"])
            if saved.get("sha256") != item["response_sha256"]:
                raise ValueError("Summary original differs from its captured digest")
        table = profiles_table(evidence, captures, output_dir)
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=(table,),
            expected_keys=(OUTPUT,),
            schemas={TABLE: described_schema(SCHEMA)},
            inputs=evidence.inputs(),
            read_snapshot=read_snapshot,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capturing = commands.add_parser("acquire", help="Save the summary CSVs into the raw archive")
    capturing.add_argument("--cycle", type=int, action="append", required=True, dest="cycles",
                           help="Even election cycle, repeatable: a closed cycle's files still change after it ends")
    capturing.add_argument("--output-dir", type=Path, required=True)
    capturing.add_argument("--captures-out", type=Path, required=True, help="Where to write the capture record")
    capturing.add_argument("--publish-evidence", action="store_true",
                           help="Also send the sealed evidence to R2 (a hosted runner, whose raw archive is scratch)")
    building = commands.add_parser("build", help="Build profiles from one cycle's captured pair")
    building.add_argument("--captures", type=Path, required=True, help="A capture record `acquire` wrote")
    building.add_argument("--cycle", type=int, required=True, help="The cycle whose two captures to build from")
    building.add_argument("--output-dir", type=Path, required=True)
    building.add_argument("--read-index", type=Path, help=READ_INDEX_HELP)
    args = parser.parse_args(argv)
    if args.command == "acquire":
        record = acquire(args.cycles, args.output_dir, publish=args.publish_evidence)
        args.captures_out.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
        print(json.dumps(record))
        return
    captures = [item for item in json.loads(args.captures.read_text())["captures"] if item["cycle"] == args.cycle]
    artifact, evidence = build(captures, args.output_dir, read_index=args.read_index)
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


def build_main():
    """``build-fec-financial-profiles``: the build, under the name it was published with."""
    main(["build", *sys.argv[1:]])


if __name__ == "__main__":
    main()
