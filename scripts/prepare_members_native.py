"""Prepare current native members from the reviewed complete roster captures.

This explicit build preserves the captured published prior as lineage, rather
than converting old rows into new source facts. It performs no publication.
"""

import argparse
from os import getenv
from pathlib import Path
import json

from spicy_regs.congress_tables import as_builder_input
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.rollups.members import MembersRollup
from spicy_regs.sources import publication
from spicy_regs.sources.member_rosters import ReviewedMemberRosters
from spicy_regs.transforms.build_members import BUDGET


class CompleteRosterMembers(MembersRollup):
    """Only this explicit preparation command selects the complete rebuild."""

    def __init__(self, *, roster_owner: ReviewedMemberRosters, **kwargs):
        super().__init__(**kwargs)
        self.roster_owner = roster_owner

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        self.roster_owner.evidence = self.source_evidence
        return self.rebuild_complete_rosters(output_dir, acquirer=self.roster_owner)


def prepare(work: Path, *, expected_prior: str):
    if work.exists():
        raise ValueError("Current member preparation requires a new work directory")
    if (not expected_prior.startswith("sha256:") or len(expected_prior) != 71
            or any(value not in "0123456789abcdef" for value in expected_prior[7:])):
        raise ValueError("Expected member prior must be a complete artifact digest")
    public_url = getenv("R2_PUBLIC_URL")
    if not public_url:
        raise ValueError("Current member preparation requires R2_PUBLIC_URL for its published prior")
    roster_owner = ReviewedMemberRosters(budget=BUDGET)
    with roster_owner, publication.snapshot(public_url) as prior_index:
        prior = prior_index["families"].get("members")
        if prior is None or prior["artifactDigest"] != expected_prior:
            raise ValueError("Published member prior differs from the expected artifact")
        pipeline = CompleteRosterMembers(roster_owner=roster_owner, output_dir=work, skip_upload=True)
        pipeline.run()
    (generation,) = (work / "generations").iterdir()
    artifact = verify_generation(generation)
    if (artifact.root["spec"]["readSnapshot"]["families"].get("members") != prior
            or not any(item["role"] == "prior-generation" and item["artifactDigest"] == expected_prior
                       for item in artifact.root["inputs"])):
        raise ValueError("Prepared member generation must retain the captured published prior")
    restored = {}
    for name in ("members", "member_terms", "member_party_affiliations"):
        # As the next scheduled build reads it: the published table in the builder's spelling (ss-vse3).
        path = as_builder_input(name, [generation / f"{name}.parquet"], work / "readback" / f"{name}.parquet")
        restored[name] = str(path.resolve())
    if pipeline.source_evidence is None:
        raise ValueError("Complete member preparation must retain source evidence")
    result = {
        "format": "spicy-regs-current-members-preparation",
        "version": 1,
        "generation": str(generation.resolve()),
        "generationPin": artifact.pin.as_dict(),
        "sourceRosterSelection": {key: dict(value) for key, value in roster_owner.selection.items()},
        "restoredProcessingInputs": restored,
        "evidence": str(pipeline.source_evidence.artifact_dir.resolve()),
        "capturedPrior": artifact.root["spec"]["readSnapshot"],
        "published": False,
    }
    (work / "members-prepared.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--expected-prior", required=True, help="Published members artifactDigest, including sha256:")
    args = parser.parse_args()
    print(json.dumps(prepare(args.work, expected_prior=args.expected_prior), indent=2))
