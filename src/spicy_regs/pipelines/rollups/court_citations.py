"""Rollup pipeline: CourtListener reporter citations, citation map and parentheticals.

Ingests an external source, so ``inputs`` is empty. The three tables come from one quarterly bulk
export (about 0.9 GB compressed together, 102 million rows), which one job downloads and decodes in
well under an hour, so all three are rebuilt from the same edition. Each download goes once into the
run's raw archive with its capture receipt, and publication keeps it as source evidence; the tables
publish no per-row receipts (ss-vse3): each row carries ``source_sha256``, the export file it was read
from. Their opinion ids reach decisions through ``court_opinions``, which is built separately from the
55 GB ``opinions`` export (``run-rollup-court-opinions``).
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_court_bulk_tables import (
    CITATION_MAP,
    CITATIONS,
    PARENTHETICALS,
    build_court_bulk_tables,
)

TABLES = (CITATIONS, CITATION_MAP, PARENTHETICALS)


class CourtCitationsRollup(RollupPipeline):
    """Court citations, the opinion citation map and parentheticals from CourtListener bulk data."""

    name: ClassVar[str] = "court-citations"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = tuple(table.output for table in TABLES)
    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_court_bulk_tables(TABLES, output_dir, evidence=self.source_evidence)


app = make_rollup_app(CourtCitationsRollup)

if __name__ == "__main__":
    app()
