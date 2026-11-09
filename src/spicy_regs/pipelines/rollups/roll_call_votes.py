"""Rollup pipeline: roll_call_votes.parquet and member_votes.parquet.

Two outputs from one pass: the roll calls and the per-member positions come
out of the same House Clerk and Senate LIS walks, so separate rollups would
fetch every roll call twice.

Reads the bill family's published ``bill_vote_references`` best-effort at
merge time, its only linkage source — a ``soft_input``, so a run with no family
output still acquires the native vote identities. A bill link remains NULL when
no recorded reference establishes it. It also reads the published ``members``
crosswalk best-effort after the ``member_votes`` merge, to fill each Senate
row's ``bioguide_id`` through its LIS id and, with ``member_terms`` and
``member_party_affiliations``, each House ``name:`` row's through its printed
label; ``run-rollup-members`` finishes those tables before votes start in the same
refresh workflow, so the captured crosswalk cannot race its own writer. These
reads are not declared ``soft_inputs`` because the declaration test models
separate writer crons rather than dependencies within one workflow. Both publishers
are keyless; senate.gov refuses runners, so a refused Senate read climbs the
walled-fetch ladder (Zyte, Firecrawl) and then reads the Internet Archive's newest
capture (``sources.senate_vote_ladder``). The cron runs an hour after the
family's to reuse any available links.

A chamber whose listing refuses is left out of the run and the other chamber is
published; the run then fails, so the refusal is seen
(``build_roll_call_votes.ChamberListingRefused``).

A dispatch scopes the Congresses (``BILL_FAMILY_CONGRESSES``), the chambers
(``ROLL_CALL_CHAMBERS``) and the per-run cap (``ROLL_CALL_MAX_VOTES``), which is
how the 101st-107th House backfill runs: House only, newest first under the
cap, each run resuming on what the last published.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup as RollupPipeline
from spicy_regs.transforms.build_roll_call_votes import ChamberListingRefused, build_roll_call_votes, max_votes_from_env


class RollCallVotesRollup(RollupPipeline):
    """House and Senate roll calls and member positions with optional bill links."""

    name: ClassVar[str] = "roll-call-votes"
    inputs: ClassVar[tuple[str, ...]] = ()
    soft_inputs: ClassVar[tuple[str, ...]] = ("bill_vote_references.parquet",)
    outputs: ClassVar[tuple[str, ...]] = ("roll_call_votes.parquet", "member_votes.parquet")
    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        if self.source_evidence is not None:
            self.source_evidence.event(
                "vote-read-snapshot", publication=self.source_evidence.read_snapshot,
                generation_id=self.receipt_generation_id,
            )

        def builder(work, *, selected_input, **kwargs):
            try:
                built = build_roll_call_votes(work, selected_input=selected_input, **kwargs)
            except ChamberListingRefused as refused:
                # Seal the successful chamber and its receipts before the run reports the refusal.
                self.deferred_failure = refused
                built = refused.outputs
            if self.source_evidence is not None:
                # The receipt writer's private stage may be cleaned on failure.
                # Keep its exact matched processing inputs in the existing audit.
                for path in built:
                    self.source_evidence.retain_file(path, stage="vote-processing-output", dataset=path.stem)
            return built

        return self.build_receipts(output_dir, builder, max_votes=max_votes_from_env(), evidence=self.source_evidence)



app = make_rollup_app(RollCallVotesRollup)

if __name__ == "__main__":
    app()
