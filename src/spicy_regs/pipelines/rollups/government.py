"""Government-source declarations for the shared rollup generation writer."""

from spicy_regs.etl_receipts import ReceiptContext, combine_receipts, failure_receipt, write_dataset
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.sources.publication import file_identity
from spicy_regs.transforms.government_receipts import POLICIES
from spicy_regs.transforms.government_source_shapes import SUBJECT_SCHEMAS


class GovernmentReceiptRollup(RollupPipeline):
    def run(self, *, read_operation=None):
        try:
            return super().run(read_operation=read_operation)
        except Exception as error:
            # The shared runner seals CaptureEvidence first. Name that actual
            # artifact as the failed attempt's witness, preserving its redaction
            # and credential controls; never copy an arbitrary exception message.
            evidence = self.source_evidence
            if evidence is not None and evidence.artifact is not None:
                pin = evidence.artifact.pin
                directory = evidence.directory / "etl-failure"
                paths = []
                for policy in self.receipt_policies:
                    context = ReceiptContext(
                        self.receipt_generation_id,
                        policy.dataset + ":run-failed",
                        policy.policy_version,
                        [
                            {
                                "source_id": pin.logical_id,
                                "source_uri": None,
                                "sha256": file_identity(evidence.artifact_dir / "artifact.json")["sha256"],
                                "locator": "journal.jsonl",
                                "body_version": pin.artifact_digest,
                            }
                        ],
                        {"stage": "rollup", "error_type": type(error).__name__},
                    )
                    receipt = failure_receipt(policy, context, outcome="error", raw_fields={"raw_record": None})
                    _, path = write_dataset([], directory / policy.dataset, policy, failures=[receipt])
                    paths.append(path)
                combine_receipts(paths, directory / "etl_receipts.parquet")
            raise

    def generation_schemas(self):
        schemas = super().generation_schemas()
        for key in self.outputs or (self.output,):
            dataset = key.removesuffix(".parquet")
            schemas[dataset] = described_schema(SUBJECT_SCHEMAS[dataset])
        return schemas

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        keys = cls.outputs or (cls.output,)
        if not all(isinstance(key, str) for key in keys):
            raise TypeError("Government rollups must declare their output table names")
        cls.receipt_policies = tuple(POLICIES[key.removesuffix(".parquet")] for key in keys if isinstance(key, str))
