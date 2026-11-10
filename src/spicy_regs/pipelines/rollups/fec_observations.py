"""Build verified retained FEC tables in a new local generation directory."""

import hashlib
from pathlib import Path
from typing import ClassVar
import shutil

import pyarrow.parquet as pq

from cyclopts import App

from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.native_types import described_schema
from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter, dataset_policy
from spicy_regs.transforms.build_fec_observations import OUTPUTS, build_fec_observations


class FecObservationsRollup(RollupPipeline):
    """Build explicitly selected retained FEC observations and metadata companions."""

    name: ClassVar[str] = "fec-observations"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = OUTPUTS
    retain_source_evidence: ClassVar[bool] = True
    receipt_policies = tuple(dataset_policy(key.removesuffix(".parquet")) for key in OUTPUTS)
    receipt_only_tables = tuple(p.dataset + ".parquet" for p in receipt_policies if p.receipt_only)

    def generation_schemas(self):
        return {p.dataset: described_schema(p.subject_schema) for p in self.receipt_policies if not p.receipt_only}


    def __init__(self, *, manifest: Path, output_dir: Path | None = None, skip_upload: bool = True):
        super().__init__(output_dir=output_dir, skip_upload=skip_upload)
        self.manifest = manifest

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        # A selection names where its builder's code lived (a git+file: implementation id), so its published copy
        # is cut of local folders; ``local_sha256`` is the exact manifest's (ss-81bv).
        retained = (
            self.source_evidence.retain_control(
                self.manifest,
                stage="retained-fec-selection",
                coverage="Explicit retained selection; input captures retain their original observation dates.",
            )
            if self.source_evidence
            else None
        )
        work = output_dir / ".fec-observations" / self.receipt_generation_id
        work.mkdir(parents=True)
        outputs = build_fec_observations(self.manifest, work / "source")
        native = work / "native"
        with IdentityReceiptWriter(native, generation_id=self.receipt_generation_id,
                                   tables=[p.dataset for p in self.receipt_policies]) as writer:
            for path in outputs:
                with path.open("rb") as stream:
                    digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
                witness = dict(source_id=path.stem, source_uri=None, sha256=digest,
                               locator=None, body_version=None)
                with pq.ParquetFile(path) as source:
                    for batch in source.iter_batches(batch_size=512):
                        for row in batch.to_pylist():
                            writer.emit(path.stem, row, input_witness=witness)
        if retained:
            with self.manifest.open("rb") as stream:
                digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
            if digest != retained["local_sha256"]:
                raise ValueError("FEC input manifest changed while building")
        shutil.copyfile(native / "etl_receipts.parquet", output_dir / "etl_receipts.parquet")
        return tuple(native / (p.dataset + ".parquet") for p in self.receipt_policies if not p.receipt_only)


app = App(help=__doc__)


@app.default
def main(*, manifest: Path, output_dir: Path | None = None, skip_upload: bool = True) -> None:
    """Read explicit input selection; write companions, scopes and relationships."""
    FecObservationsRollup(manifest=manifest, output_dir=output_dir, skip_upload=skip_upload).run()


if __name__ == "__main__":
    app()
