"""Publish one qualified candidate through the existing verified family CAS.

The explicit command performs a remote write. An optional checkout pin refuses runtime drift; installed commands record their
verifier implementation separately from the candidate producer. Observe-only
recovers an uncertain publication without uploading or changing a pointer.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from hashlib import sha256
from importlib.metadata import version
import json
from pathlib import Path
import subprocess

from dotenv import load_dotenv
from rulespec_artifacts import ArtifactPin, LocalMemberSource, iter_member_descriptors

from spicy_regs.generations import admit_generation_for_publication, implementation_id, verify_generation
from spicy_regs.sources import publication, r2


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preparation", type=Path, required=True)
    parser.add_argument("--implementation-commit", help="Optional clean checkout revision; omit for installed packages")
    parser.add_argument("--observe-only", action="store_true", help="Verify an already-published candidate without remote writes")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--receipt-only-table", action="append", default=[],
        help="Explicitly move a processing table into its registered receipt-only dataset",
    )
    args = parser.parse_args(argv)
    if args.implementation_commit:
        subprocess.run(
            [
                "git",
                "diff",
                "--exit-code",
                args.implementation_commit,
                "--",
                "src/spicy_regs",
                "pyproject.toml",
                "uv.lock",
                "data_dictionary",
            ],
            check=True,
            capture_output=True,
        )
    if args.output.exists():
        raise ValueError("Use a fresh publication receipt directory")
    args.output.mkdir(parents=True)
    prepared = json.loads(args.preparation.read_bytes())
    if prepared.get("status") != "prepared_not_published":
        raise ValueError("Publication requires a completed scorecard preparation")
    directory = Path(prepared["generation_directory"])
    if args.observe_only:
        artifact = verify_generation(directory)
    else:
        artifact = admit_generation_for_publication(
            directory,
            expected_pin=ArtifactPin(prepared["generation"]["logicalId"], prepared["generation"]["artifactDigest"]),
        )
    if artifact.pin.as_dict() != prepared["generation"]:
        raise ValueError("Prepared generation differs from its qualification pin")
    family = artifact.root["spec"]["family"]
    if family not in {"scorecards", "scorecard-analysis"}:
        raise ValueError("Scorecard commands publish only scorecard source or analysis families")
    prior = publication.parse_index(Path(prepared["read_snapshot_path"]).read_bytes())
    if artifact.root["spec"].get("readSnapshot") != prior:
        raise ValueError("Preparation read snapshot differs from the verified generation")
    load_dotenv(args.env_file)
    client, bucket = r2.verified_target_client()
    before, _ = publication._stored_index(client, bucket)
    (args.output / "before.json").write_text(json.dumps(before, indent=2) + "\n")
    evidence = (Path(prepared["evidence_directory"]),) if prepared.get("evidence_directory") else ()
    if args.observe_only:
        expected = publication.generation_entry(artifact, iter_member_descriptors(artifact, LocalMemberSource(directory)))
        current = before["families"].get(family)
        if current is None or {key: value for key, value in current.items() if key != "publishedAt"} != expected:
            raise ValueError("Candidate is not the currently published family; observation performs no writes")
        after = before
    else:
        after = publication.publish_generation(
            directory, client=client, bucket=bucket, prior_index=prior, evidence_directories=evidence,
            receipt_only_tables=frozenset(args.receipt_only_table),
        )
    observed, _ = publication._stored_index(client, bucket)
    if observed["families"][family] != after["families"][family]:
        raise ValueError("Published family changed before authenticated readback")
    unexpected = [
        name for name, entry in before["families"].items() if name != family and observed["families"].get(name) != entry
    ]
    (args.output / "after.json").write_text(json.dumps(observed, indent=2) + "\n")
    receipt = {
        "status": "published_authenticated_readback",
        "observed_at": datetime.now(UTC).isoformat(),
        "family": family,
        "generation": artifact.pin.as_dict(),
        "family_entry": observed["families"][family],
        "implementation_commit": args.implementation_commit,
        "verifier_implementation_id": implementation_id(),
        "publication_write_requested": not args.observe_only,
        "candidate_packages": artifact.root["spec"].get("packages", {}),
        "provider_version": version("spicy-docs"),
        "preparation_sha256": sha256(args.preparation.read_bytes()).hexdigest(),
        "other_family_changes_during_publication": unexpected,
        "public_index_url": f"{r2.VERIFIED_PUBLIC_URL}/{publication.INDEX_V2_KEY}",
        "source_network_requests": 0,
        "receipt_only_table_migrations": sorted(set(args.receipt_only_table)),
    }
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: receipt[key]
                for key in ("status", "family", "generation", "other_family_changes_during_publication")
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
