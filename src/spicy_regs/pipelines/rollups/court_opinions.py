"""Rollup pipeline: court_opinions.parquet, the text-free CourtListener opinion index.

Ingests an external source, so ``inputs`` is empty. It maps every opinion to its cluster, which is
what joins ``court_citation_map`` and ``court_parentheticals`` to decisions. Its only source is the
55 GB ``opinions`` export, about 8.6 hours to download at the bucket's rate, so it has no
scheduled workflow: run it on the workstation (docs/court-receipt-archive.md). A download goes once into
the shared raw archive with its capture receipt and is read there; ``COURTLISTENER_BULK_DIR`` names a
directory holding the export under its publisher filename instead. The table publishes no per-row
receipts (ss-vse3): each row carries ``source_sha256``, the export file it was read from.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_court_bulk_tables import OPINIONS, build_court_bulk_tables


class CourtOpinionsRollup(RollupPipeline):
    """Every CourtListener opinion's cluster, type and author, without its text."""

    name: ClassVar[str] = "court-opinions"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = OPINIONS.output
    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> Path:
        (built,) = build_court_bulk_tables((OPINIONS,), output_dir, evidence=self.source_evidence)
        return built


app = make_rollup_app(CourtOpinionsRollup)

if __name__ == "__main__":
    app()
