"""Build local House or Senate tables from retained complete PDF page readings."""

import argparse
import json
from pathlib import Path

from spicy_regs.pipelines.disbursement_generation import build_tables


def build(manifest: Path, output_dir: Path):
    reading = json.loads(manifest.read_text())
    if reading.get("reading_profile", "ledger") != "ledger":
        from spicy_regs.schemas.disbursement_summaries import PARTITION_COLUMNS, family_tables
        from spicy_regs.transforms.build_disbursement_summaries import build_disbursement_summaries

        family, tables, schemas = family_tables(reading["chamber"], reading["reading_profile"])
        return build_tables(
            manifest,
            output_dir,
            family=family,
            tables=tables,
            schemas=schemas,
            partitions=PARTITION_COLUMNS,
            archive_root=Path(reading["archive_root"]),
            builder=build_disbursement_summaries,
        )
    if reading["chamber"] == "senate":
        from spicy_regs.schemas import senate_disbursements as schema
        from spicy_regs.transforms.build_senate_disbursements import build_senate_disbursements as builder
    elif reading["chamber"] == "house":
        from spicy_regs.schemas import house_disbursement_pages as schema
        from spicy_regs.transforms.build_house_disbursement_pages import build_house_disbursement_pages as builder
    else:
        raise ValueError("Unsupported congressional disbursement chamber")
    return build_tables(
        manifest,
        output_dir,
        family=schema.FAMILY,
        tables=schema.TABLES,
        schemas=schema.SCHEMAS,
        partitions=schema.PARTITION_COLUMNS,
        archive_root=Path(reading["archive_root"]),
        builder=builder,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reading", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact, evidence = build(args.reading, args.output)
    print(
        json.dumps(
            {
                "directory": str(args.output / "generation"),
                "source_evidence": str(evidence),
                "logical_id": artifact.pin.logical_id,
                "artifact_digest": artifact.pin.artifact_digest,
            }
        )
    )


if __name__ == "__main__":
    main()
