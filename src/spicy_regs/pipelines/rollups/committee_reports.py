"""Rollup pipeline: committee_reports, committee_report_cbo_letters, report_sections and hearing_transcripts.

Every output from one pass: the two GovInfo collections share an acquirer and
its pacing budget, and the agency blocks are parsed from a report body that is
only in hand during the pass that fetched it.
"""

import os
from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.legislative_rollups import LegislativeReceiptRollup, family_policies
from spicy_regs.transforms.build_committee_reports import build_committee_reports


class CommitteeReportsRollup(LegislativeReceiptRollup):
    """GovInfo committee reports, their agency blocks, and hearing transcripts (api.data.gov key)."""

    name: ClassVar[str] = "committee-reports"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    receipt_policies = family_policies('committee_reports', 'report_sections', 'hearing_transcripts', 'hearing_bill_links', 'committee_report_cbo_letters', 'committee_report_reads')
    outputs: ClassVar[tuple[str, ...]] = (
        "committee_reports.parquet",
        "report_sections.parquet",
        "hearing_transcripts.parquet",
        "hearing_bill_links.parquet",
        "committee_report_cbo_letters.parquet",
        "committee_report_reads.parquet",
    )

    receipt_only_tables = ("committee_report_reads.parquet",)
    # The first generation with this table is a migration; publication refuses any other change of membership.
    added_tables: ClassVar[tuple[str, ...]] = ("committee_report_cbo_letters.parquet",)

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        # Comma-separated package ids to re-read alone; unset is the normal discovery run.
        reread = [key.strip() for key in os.environ.get("COMMITTEE_REPORTS_REREAD", "").split(",") if key.strip()]
        self.acquisition_status = {}
        result = self.build_receipts(output_dir, build_committee_reports, evidence=self.source_evidence,
                                     reread=reread, coverage=self.acquisition_status)
        if self.acquisition_status.get("pending"):
            self.deferred_failure = RuntimeError("Committee tables generated with pending acquisition; see acquisition-coverage")
        return result


app = make_rollup_app(CommitteeReportsRollup)

if __name__ == "__main__":
    app()
