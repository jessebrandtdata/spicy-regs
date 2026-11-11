"""Rollup pipeline: members, terms and party-affiliation occurrences.

Related outputs from one pass: the tables are read out of the same two roster
captures, and a legislator's terms are only in hand while the roster is. They
publish no per-row receipts (ss-vse3): a row's evidence is the run's retained
roster captures, which its ``roster``, ``observed_at`` and (for an affiliation)
``input_sha256`` and ``source_path`` name.
"""

from pathlib import Path
from typing import ClassVar
from spicy_docs.sources.legislators import LegislatorsAcquirer

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.subject_receipts import SubjectTableRollup
from spicy_regs.transforms.build_members import build_members


class MembersRollup(SubjectTableRollup):
    """Legislators and their terms, from the @unitedstates community crosswalk."""

    name: ClassVar[str] = "members"
    retain_source_evidence: ClassVar[bool] = True
    added_tables: ClassVar[tuple[str, ...]] = ("member_party_affiliations.parquet",)
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = ("members.parquet", "member_terms.parquet", "member_party_affiliations.parquet")

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return self.build_tables(output_dir, build_members, evidence=self.source_evidence)

    def rebuild_complete_rosters(self, output_dir: Path, *, acquirer: LegislatorsAcquirer | None = None) -> tuple[Path, ...]:
        """Explicit first native build from both reviewed complete source files.

        The ordinary scheduled build merges into the published prior.
        This preparation method reads no old table. The common
        runner retains the captured prior artifact as lineage and publishes
        only after the complete source and native candidate are verified.
        """
        def complete_rosters(directory, **_):
            return build_members(
                directory, evidence=self.source_evidence,
                acquirer=acquirer,
                download_prior=lambda key, target: False,
                declare_complete_inputs=True,
            )

        return self.build_tables(output_dir, complete_rosters)


app = make_rollup_app(MembersRollup)

if __name__ == "__main__":
    app()
