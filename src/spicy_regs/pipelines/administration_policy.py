"""Acquire administration-policy archive metadata into the raw archive, then build its generation.

A capture selection names one metadata file per administration: its archive commit, URL, SHA-256, size and
observation time, as the raw acquisition receipt records them. ``acquire`` reads every selected file at one
commit; a file whose bytes equal the published observation keeps that observation, so its statement ids
stay as published. ``build`` runs the retained-collections builder over the raw originals and packages its
candidate as a generation whose source evidence names every selected file.
"""
from __future__ import annotations

import argparse
import json
from importlib.metadata import version
from pathlib import Path

from spicy_regs.generations import build_generation
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.raw_releases import open_raw, raw_original
from spicy_regs.source_evidence import JOURNAL, CaptureEvidence, acquisition_evidence
from spicy_regs.sources import retained_collections

FAMILY = "administration-policy"
STAGE = "administration-policy-metadata"
PUBLISHER = "unitedstates/statements-of-administration-policy"
#: The archive states no licence, so published evidence names each file's URL and digest and carries no body;
#: the bodies are kept in the raw archive. The October 2026 generation's evidence made the same choice.
EVIDENCE_DECISION = "administration-policy-archive-hash-only/1"
STATEMENTS = "administration_policy_statements"


def published_sources(statements: Path) -> dict[str, dict]:
    """The one metadata capture behind each administration's rows of a published statements table."""
    import pyarrow.parquet as pq

    columns = ("administration", "archive_commit", "source_uri", "source_sha256", "observed_at")
    rows = pq.read_table(statements, columns=list(columns)).to_pylist()
    distinct = {tuple(row[column] for column in columns) for row in rows}
    sources = {name: {"commit": commit, "url": url, "sha256": digest, "observed_at": observed}
               for name, commit, url, digest, observed in distinct}
    if len(sources) != len(distinct):
        raise ValueError("Published statements hold more than one capture of an administration")
    return sources


def acquire(commit: str, administrations, output_dir: Path, *, prior_statements: Path | None = None,
            transport=None) -> dict:
    """Capture each administration's metadata at ``commit``; returns the capture selection.

    Every administration of ``prior_statements`` is read too. Unchanged bytes keep their published capture.
    """
    from spicy_docs.sources.administration_policy import MAX_METADATA_BYTES, AdministrationPolicyAcquirer, metadata_url

    published = published_sources(prior_statements) if prior_statements is not None else {}
    names = sorted({*administrations, *published})
    if not names:
        raise ValueError("Select at least one administration or a published statements table")
    with acquisition_evidence(FAMILY, output_dir=Path(output_dir)) as evidence:
        for name in names:
            # One acquirer per file is the owner's request budget; closing it closes its transport.
            retained = evidence.transport(transport, stage=STAGE, max_bytes=MAX_METADATA_BYTES)
            with AdministrationPolicyAcquirer(transport=retained) as acquirer:
                acquirer.acquire_metadata(commit=commit, administration=name)
        events = [json.loads(line) for line in (evidence.artifact_dir / JOURNAL).read_text().splitlines()]
        receipts = {event["requested_url"]: event for event in events
                    if event["event"] == "capture" and event["stage"] == STAGE}
        sources = []
        for name in names:
            receipt = receipts[metadata_url(commit, name)]
            if receipt["content_encoding"] != "identity":
                raise ValueError("Archive metadata was served encoded; its raw bytes are not the YAML file")
            source = {"kind": "administration-policy", "administration": name, "commit": commit,
                      "url": receipt["requested_url"], "sha256": receipt["sha256"], "bytes": receipt["byte_size"],
                      "observed_at": receipt["observed_at"]}
            held = published.get(name)
            if held is not None and held["sha256"] == source["sha256"]:
                evidence.event("administration-policy-unchanged", administration=name, read_url=source["url"],
                               sha256=source["sha256"], published_url=held["url"])
                source |= held
            sources.append(source)
    return {"sources": sources}


def merge_selections(selections) -> dict:
    """Combine capture selections; one capture per administration, never a silent override."""
    sources = {}
    for selection in selections:
        for source in selection["sources"]:
            if sources.setdefault(source["administration"], source) != source:
                raise ValueError("Two captures were selected for one administration")
    return {"sources": [sources[name] for name in sorted(sources)]}


def build_from_captures(selection: dict, output_dir: Path, *, archive_root: Path | None = None):
    """Build the candidate from raw originals and admit it as a generation with its source evidence."""
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists() or (output_dir / "candidate").exists():
        raise ValueError("Use a fresh output directory for each administration-policy generation")
    store = open_raw(archive_root)
    sources = [{**source, "path": str(raw_original(store, source["sha256"], source["bytes"]))}
               for source in selection["sources"]]
    root = CaptureEvidence(output_dir, FAMILY)
    try:
        evidence = root.for_source(PUBLISHER, "hash_only", parser_version=version("spicy-docs"),
                                   policy_decision_id=EVIDENCE_DECISION)
        for source in sources:
            if evidence.retain_file(Path(source["path"]), stage=STAGE)["sha256"] != source["sha256"]:
                raise ValueError("Archive metadata differs from its selected digest")
            evidence.event("administration-policy-source",
                           **{key: value for key, value in source.items() if key != "path"})
        manifest = output_dir / "selection.json"
        manifest.write_text(json.dumps(
            {"format": "spicy-retained-collection-inputs", "version": 1, "sources": sources},
            indent=2, sort_keys=True) + "\n")
        candidate = output_dir / "candidate"
        built = retained_collections.build(manifest, candidate)
        tables = retained_collections.SCHEMAS
        if missing := [table for table in tables if not built["outputs"][table]["path"]]:
            raise ValueError("Candidate wrote no table for: " + ", ".join(missing))
        artifact = build_generation(
            output_dir / "generation", family=FAMILY,
            files=[candidate / built["outputs"][table]["path"] for table in tables],
            expected_keys=[table + ".parquet" for table in tables],
            schemas={table: described_schema(schema) for table, schema in tables.items()},
            receipt_path=candidate / "etl_receipts.parquet",
            receipt_policies=list(retained_collections.POLICIES.values()),
            receipt_generation_id=built["generation"], inputs=root.inputs(),
        )
    except BaseException as error:
        root.finish(error)
        raise
    root.finish()
    return artifact, root.artifact_dir, built


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("acquire", help="Capture archive metadata files into the raw archive")
    capture.add_argument("--commit", required=True, help="Full archive commit SHA to read")
    capture.add_argument("--administration", action="append", default=[], dest="administrations",
                         help="Archive stem such as 47-Trump, repeatable")
    capture.add_argument("--prior-statements", type=Path,
                         help=f"The published {STATEMENTS}.parquet: its administrations are read too, and an "
                              "unchanged file keeps its published capture")
    capture.add_argument("--output-dir", required=True, type=Path)
    capture.add_argument("--captures-out", required=True, type=Path, help="Where to write the capture selection")
    generation = commands.add_parser("build", help="Build a generation from capture selections and raw originals")
    generation.add_argument("--captures", required=True, action="append", type=Path)
    generation.add_argument("--output-dir", required=True, type=Path)
    generation.add_argument("--archive-root", type=Path, help="Raw archive holding the originals (read only)")
    args = parser.parse_args()
    if args.command == "acquire":
        selection = acquire(args.commit, args.administrations, args.output_dir,
                            prior_statements=args.prior_statements)
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"captures": str(args.captures_out), "sources": [
            {key: source[key] for key in ("administration", "commit", "sha256")} for source in selection["sources"]]}))
        return
    selection = merge_selections(json.loads(path.read_text()) for path in args.captures)
    artifact, evidence, built = build_from_captures(selection, args.output_dir, archive_root=args.archive_root)
    print(json.dumps({"directory": str(args.output_dir / "generation"), "sourceEvidence": str(evidence),
                      "logicalId": artifact.pin.logical_id, "artifactDigest": artifact.pin.artifact_digest,
                      "rows": {table: output["rows"] for table, output in built["outputs"].items()}}))


if __name__ == "__main__":
    main()
