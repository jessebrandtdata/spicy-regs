"""Rollup pipeline: house_activity_reports, budget_volumes, bill_committee_actions and document_citations.

Four outputs from one pass: the two GovInfo collections share an acquirer and
its pacing budget, and the citations and bill actions are read out of a
document body that is only in hand during the pass that fetched it.
``transforms/build_print_citations.py`` states why these four and not some
other grouping, and why ``senate_expenditures`` is a rollup of its own.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.legislative_rollups import LegislativeReceiptRollup, family_policies
from spicy_regs.transforms.build_print_citations import build_print_citations
from spicy_regs.transforms.held_citations import write_citation_reads


class PrintCitationsRollup(LegislativeReceiptRollup):
    """GovInfo activity reports and budget volumes, what their prints cite, and the actions they state (api.data.gov key)."""

    name: ClassVar[str] = "print-citations"
    inputs: ClassVar[tuple[str, ...]] = ()
    receipt_policies = family_policies('house_activity_reports', 'budget_volumes', 'bill_committee_actions', 'document_citations', 'document_citation_reads')
    outputs: ClassVar[tuple[str, ...]] = (
        "house_activity_reports.parquet",
        "budget_volumes.parquet",
        "bill_committee_actions.parquet",
        "document_citations.parquet",
        # The held-field reads the citation table's checkpoints record, rebuilt from the merged table's metadata
        # so a run of this family never drops what the held-citations rollup read.
        "document_citation_reads.parquet",
    )

    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        self.acquisition_status = {}

        def builder(work, **kwargs):
            paths = build_print_citations(work, coverage=self.acquisition_status, **kwargs)
            return (*paths, write_citation_reads(work, paths[3]))

        result = self.build_receipts(output_dir, builder, evidence=self.source_evidence)
        if self.acquisition_status.get("refused") or self.acquisition_status.get("deferred"):
            self.deferred_failure = RuntimeError("Print tables generated with pending acquisition; see acquisition-coverage")
        return result


app = make_rollup_app(PrintCitationsRollup)

if __name__ == "__main__":
    app()
