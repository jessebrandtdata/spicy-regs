"""Rollup pipeline: fr_docket_links.parquet (Federal Register ↔ docket links)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup as RollupPipeline
from spicy_regs.transforms.build_fr_docket_links import build_fr_docket_links


class FrDocketLinksRollup(RollupPipeline):
    """FR publications linked to each docket — replaces the docket page's LIKE scan.

    ``federal_register.parquet`` is produced by a separate ingestion path; this rollup reads it from R2 as a base
    input. It is a published input (``published_inputs``): the pinned public table, not a restore of it through
    its receipts. Every column the links carry is public there (ss-vse3), and the restore was 307 of this job's
    937 s (run 42699907697).
    """

    name: ClassVar[str] = "fr-docket-links"
    inputs: ClassVar[tuple[str, ...]] = ("federal_register.parquet",)
    published_inputs: ClassVar[tuple[str, ...]] = inputs
    output: ClassVar[str] = "fr_docket_links.parquet"

    def build(self, output_dir: Path) -> Path:
        return self.build_receipts(output_dir, build_fr_docket_links)


app = make_rollup_app(FrDocketLinksRollup)

if __name__ == "__main__":
    app()
