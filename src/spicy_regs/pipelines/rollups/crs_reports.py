"""Rollup pipeline: crs_reports.parquet (Congress.gov CRS report ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_crs_reports``. The
base class still handles the shrink-guarded R2 upload of the single output.
"""

from pathlib import Path

from spicy_regs.env_values import date_env, flag_env
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.government import GovernmentReceiptRollup
from spicy_regs.transforms.build_crs_reports import build_crs_reports


class CrsReportsRollup(GovernmentReceiptRollup):
    """Congressional Research Service reports ingested from the Congress.gov v3 API."""

    name: ClassVar[str] = "crs-reports"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "crs_reports.parquet"

    def build(self, output_dir: Path) -> Path:
        self.acquisition_status = {}
        result = build_crs_reports(output_dir, evidence=self.source_evidence,
                                   receipt_generation_id=self.receipt_generation_id,
                                   since=date_env("CRS_REPORTS_SINCE"),
                                   reconcile=flag_env("CRS_REPORTS_RECONCILE"), coverage=self.acquisition_status)
        if self.acquisition_status.get("detail_failed"):
            self.deferred_failure = RuntimeError("CRS table generated with failed detail acquisition; see acquisition-coverage")
        return result


app = make_rollup_app(CrsReportsRollup)

if __name__ == "__main__":
    app()
