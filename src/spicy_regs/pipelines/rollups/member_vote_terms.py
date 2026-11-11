"""Rollup pipeline: member_vote_terms.parquet (the term each member vote counts toward).

It publishes no per-row receipts (ss-vse3). A vote the rule gives no term (``unmatched``, ``ambiguous``,
``unresolved_member``, ``undated``) is left out of the table, as the receipt route left it out with a ``rejected``
receipt; the run logs how many and the first keys, and the builder logs every ``term_match`` count. Such a vote is
the ``member_votes`` row with no row here.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.subject_receipts import SubjectTableRollup
from spicy_regs.transforms.build_member_vote_terms import build_member_vote_terms
from spicy_regs.transforms.build_member_vote_terms import INPUTS, OUTPUT


class MemberVoteTermsRollup(SubjectTableRollup):
    """Delivery decision 2's half-open term rule over the published votes, members and terms."""

    name: ClassVar[str] = "member-vote-terms"
    inputs: ClassVar[tuple[str, ...]] = INPUTS
    output: ClassVar[str] = OUTPUT

    def build(self, output_dir: Path) -> Path:
        return self.build_tables(output_dir, build_member_vote_terms)


app = make_rollup_app(MemberVoteTermsRollup)

if __name__ == "__main__":
    app()
