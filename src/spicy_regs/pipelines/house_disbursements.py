"""Build a local House ledger/summary generation from a complete pinned normalization."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.pipelines.disbursement_generation import build_tables
from spicy_regs.schemas.house_disbursements import FAMILY, PARTITION_COLUMNS, SCHEMAS, TABLES
from spicy_regs.transforms.build_house_disbursements import build_house_disbursements


def build(manifest: Path, output_dir: Path):
    """Create an ordinary admitted local generation; publication remains a separate operation."""
    normalization = json.loads(manifest.read_text())
    acquisition = json.loads(Path(normalization["acquisition"]).read_text())
    return build_tables(
        manifest,
        output_dir,
        family=FAMILY,
        tables=tuple(TABLES.values()),
        schemas=SCHEMAS,
        partitions=PARTITION_COLUMNS,
        archive_root=Path(acquisition["archive_root"]),
        builder=build_house_disbursements,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--normalization", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact, evidence = build(args.normalization, args.output)
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
