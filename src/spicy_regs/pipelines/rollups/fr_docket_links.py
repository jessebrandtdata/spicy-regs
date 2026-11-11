"""Rollup pipeline: fr_docket_links.parquet (Federal Register ↔ docket links)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_fr_docket_links import build_fr_docket_links


class FrDocketLinksRollup(RollupPipeline):
    """FR publications linked to each docket — replaces the docket page's LIKE scan.

    ``federal_register.parquet`` is produced by a separate ingestion path; this rollup reads its published table,
    which the generation pins as its parent, and rebuilds every link from it. It publishes no per-row receipts
    (ss-vse3; its installed policy is marked ``receipts_retired``).
    """

    name: ClassVar[str] = "fr-docket-links"
    inputs: ClassVar[tuple[str, ...]] = ("federal_register.parquet",)
    output: ClassVar[str] = "fr_docket_links.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_fr_docket_links(output_dir)


app = make_rollup_app(FrDocketLinksRollup)

if __name__ == "__main__":
    app()
