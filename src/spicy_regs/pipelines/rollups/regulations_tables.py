"""A regulations ingest that merges at its published table, with no per-row receipts (ss-vse3).

The Unified Agenda and CFR sections builders keep the text shape they have always written (``SOURCE_COLUMNS``) and
their own merge with the prior table. What changed is where the prior comes from and what is published: the prior
is the table the captured index pins, read back into that text shape (``regulations_shape.published_as_source``),
never restored through receipts; the builder's table is mapped once to the published columns
(``regulations_shape.mapped_table``) in the builder's order, ``ROW_GROUP_ROWS`` rows per group, and no receipt is
written. A column the receipts alone held is either public or derived: both tables' ``url`` is a fixed template of
their identity (measured equal on every row of the 2026-11-10 receipts), so the builders still write it and the
mapper leaves it out, as it always did.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable
from pathlib import Path

from spicy_regs.pipelines.rollups.base import RollupPipeline


class RegulationsTableRollup(RollupPipeline):
    """One regulations table whose installed policy is marked ``receipts_retired``."""

    def build_table(self, output_dir: Path, builder: Callable[..., Path], *,
                    download_prior: Callable[[str, Path], bool] | None = None, **kwargs) -> Path:
        """Run ``builder`` on the published prior read back as its text shape, and map its table for publication."""
        from spicy_regs.phase_log import phase_log
        from spicy_regs.subject_catalog import retired_receipt_datasets
        from spicy_regs.transforms.regulations_shape import ROW_GROUP_ROWS, mapped_table, published_as_source
        from spicy_regs.transforms.table_merge import published_prior

        dataset = self.output.removesuffix(".parquet")
        if dataset not in retired_receipt_datasets():
            raise ValueError(f"{type(self).__name__}: {dataset} still publishes receipts; mark its policy "
                             "receipts_retired before it publishes without them")
        work = output_dir / f".{dataset}-text"
        work.mkdir(parents=True, exist_ok=True)
        download = download_prior or published_prior

        def prior(key: str, target: Path) -> bool:
            published = work / f"_published_{key}"
            if not download(key, published):
                return False
            with phase_log("prior-published-input", dataset=dataset):
                published_as_source(dataset, published, target)
            published.unlink()
            return True

        text = builder(work, download_prior=prior, **kwargs)
        with phase_log("public-table", dataset=dataset):
            table = mapped_table(dataset, text, output_dir / self.output, row_group_rows=ROW_GROUP_ROWS)
        shutil.rmtree(work)
        return table
