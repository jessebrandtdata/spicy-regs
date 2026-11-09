"""Rollup pipeline: committees and committee_assignments.

Two outputs from one pass: the assignment rows are keyed on the committee
``systemCode`` the list route enumerates, and both chamber files are read
whole in the same run that reads the route. Earlier Congresses' seats come once
from their Congressional Directory editions, whose GovInfo responses are retained
as source evidence; resolving their members reads the members rollup's published
``members`` and ``member_terms``, so this rollup runs after that one.
"""

from pathlib import Path
from typing import Annotated, ClassVar

from cyclopts import App, Parameter
from dotenv import load_dotenv

from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup as RollupPipeline
from spicy_regs.transforms.build_committee_rosters import build_committee_rosters


class CommitteeRostersRollup(RollupPipeline):
    """Committees from Congress.gov (api.data.gov key) and today's seats from the two chamber roster files."""

    name: ClassVar[str] = "committee-rosters"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    soft_inputs: ClassVar[tuple[str, ...]] = ("members.parquet", "member_terms.parquet")
    outputs: ClassVar[tuple[str, ...]] = (
        "committees.parquet",
        "committee_assignments.parquet",
        "committee_directory_entries.parquet",
    )
    # The first generation with this table is a migration; publication refuses any other change of membership.
    added_tables: ClassVar[tuple[str, ...]] = ("committee_directory_entries.parquet",)

    #: ``--senate-paid-ladder``: read today's Senate file through the walled-fetch ladder (Zyte, then Firecrawl;
    #: ``ZYTE_TOKEN`` / ``FIRECRAWL_API_KEY``) because www.senate.gov refuses direct clients (ss-ehci).
    senate_paid_ladder: bool = False

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return self.build_receipts(output_dir, build_committee_rosters, evidence=self.source_evidence,
                                   senate_paid_ladder=self.senate_paid_ladder)  # fmt: skip


# The shared two flags (make_rollup_app's) plus this rollup's one opt-in.
app = App(
    name=f"run-rollup-{CommitteeRostersRollup.name}",
    help=(CommitteeRostersRollup.__doc__ or "").strip().splitlines()[0],
)


@app.default
def main(
    *,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (recommended while vetting)")] = True,
    senate_paid_ladder: Annotated[
        bool, Parameter(help="Read today's Senate file through Zyte, then Firecrawl (opt-in; paid)")
    ] = False,
) -> None:
    load_dotenv()
    rollup = CommitteeRostersRollup(output_dir=output_dir, skip_upload=skip_upload)
    rollup.senate_paid_ladder = senate_paid_ladder
    rollup.run()


if __name__ == "__main__":
    app()
