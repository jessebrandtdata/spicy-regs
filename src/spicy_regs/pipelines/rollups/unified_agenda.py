"""Rollup pipeline: unified_agenda.parquet (reginfo.gov Unified Agenda ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_unified_agenda``.
The base class still handles the shrink-guarded R2 upload of the single output.

It publishes no per-row receipts (ss-vse3; its installed policy is marked
``receipts_retired``): the prior is the published table
(``RegulationsTableRollup``), and each edition file the run reads is retained
as source evidence, with the editions' manifest in the run's journal. The
receipts of the generations before the switch stay where they are
(``docs/pins/receipt-archive.json``).
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.regulations_tables import RegulationsTableRollup
from spicy_regs.transforms.build_unified_agenda import build_unified_agenda


class UnifiedAgendaRollup(RegulationsTableRollup):
    """Unified Agenda entries ingested from reginfo.gov (no API key)."""

    name: ClassVar[str] = "unified-agenda"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "unified_agenda.parquet"

    def build(self, output_dir: Path) -> Path:
        return self.build_table(output_dir, build_unified_agenda, evidence=self.source_evidence)


app = make_rollup_app(UnifiedAgendaRollup)

if __name__ == "__main__":
    app()
