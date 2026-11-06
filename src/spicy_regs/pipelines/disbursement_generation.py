"""Use the ordinary generation builder for retained congressional spending tables."""

from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence


def build_tables(manifest, output_dir, *, family, tables, schemas, partitions, archive_root, builder):
    output_dir = Path(output_dir).resolve()
    if (output_dir / "generation").exists() or any((output_dir / name).exists() for name in tables):
        raise ValueError("Use a fresh output directory for a disbursement table generation")
    evidence = CaptureEvidence(output_dir, family, archive_root=archive_root)
    try:
        paths = builder(manifest, output_dir, evidence=evidence)
        artifact = build_generation(
            output_dir / "generation",
            family=family,
            files=paths,
            expected_keys=tuple(name + ".parquet" for name in tables),
            schemas={name: described_schema(schema) for name, schema in schemas.items()},
            partitioned={name + ".parquet": partitions for name in tables},
            inputs=evidence.inputs(),
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir
