"""Rollup pipeline: fr_docket_links.parquet (Federal Register ↔ docket links)."""

import shutil
from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup as RollupPipeline
from spicy_regs.sources import r2
from spicy_regs.transforms.build_fr_docket_links import build_fr_docket_links

REGISTER = "federal_register.parquet"


class FrDocketLinksRollup(RollupPipeline):
    """FR publications linked to each docket — replaces the docket page's LIKE scan.

    ``federal_register.parquet`` is produced by a separate ingestion path. This rollup reads its published
    table, pinned by the run's index, not a restore of it through its receipts: every column the links carry is
    public there (ss-vse3), and the restore was 307 of this job's 937 s (run 42699907697). ``inputs`` is empty
    because the receipt route restores every declared input; the fetch in ``build`` records the table as this
    generation's parent all the same.
    """

    name: ClassVar[str] = "fr-docket-links"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "fr_docket_links.parquet"

    def build(self, output_dir: Path) -> Path:
        def from_published_register(work: Path) -> Path:
            local = output_dir / REGISTER  # a local run beside the Register table it just built
            if local.exists():
                shutil.copyfile(local, work / REGISTER)
            elif not r2.download(REGISTER, work / REGISTER):
                raise RuntimeError("fr_docket_links requires the published federal_register table")
            return build_fr_docket_links(work)

        return self.build_receipts(output_dir, from_published_register)


app = make_rollup_app(FrDocketLinksRollup)

if __name__ == "__main__":
    app()
