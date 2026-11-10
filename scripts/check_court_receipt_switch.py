"""Compare a receipts-free court build with the live generation it replaces, before the first publish (ss-vse3).

    uv run --frozen --extra comments-reader python scripts/check_court_receipt_switch.py \
        --family court-citations --generation "$WORK/citations/generations/<digest>" --scratch "$WORK/live"

For each table: the receipt route's columns must be the live table (schema, rows both ways, key at every position;
``receipt_switch.table_differences``), and every promoted column must equal its copy in the live receipts
(``receipt_switch.receipt_copy_differences``, read by range from the public bucket). The live tables are downloaded
into ``--scratch`` (1.0 GB for court-citations, 0.41 GB for court-opinions); the receipts are not. Exits 1 on any
difference. Only meaningful when the build read the same edition the live generation holds.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

LIVE = "https://data.spicygov.ai"
TEXT = "{v}->>'$[1]'"
COPIES = {
    "court_citations": {"date_created": TEXT, "date_modified": TEXT, "dump_date": TEXT},
    "court_citation_map": {"dump_date": TEXT},
    "court_parentheticals": {"dump_date": TEXT},
    "court_opinions": {"date_created": TEXT, "date_modified": TEXT, "dump_date": TEXT, "local_path": TEXT,
                       "extracted_by_ocr": "CASE {v}->>'$[1]' WHEN 't' THEN true WHEN 'f' THEN false END"},
}


def main(argv=None) -> int:
    import httpx
    import pyarrow.parquet as pq

    from spicy_regs.etl_receipts import DatasetPolicy
    from spicy_regs.receipt_switch import receipt_copy_differences, table_differences
    from spicy_regs.sources import publication
    from spicy_regs.subject_catalog import descriptor_of

    parser = argparse.ArgumentParser(description="Compare a receipts-free court build with the live generation.")
    parser.add_argument("--family", required=True, choices=["court-citations", "court-opinions"])
    parser.add_argument("--generation", required=True, type=Path, help="the receipts-free build's generation directory")
    parser.add_argument("--scratch", required=True, type=Path)
    args = parser.parse_args(argv)
    entry = publication.current_index(LIVE)["families"][args.family]
    if "etlReceipts" not in entry:
        raise SystemExit(f"{args.family} already publishes without receipts; nothing to compare")
    _, root = publication.load_family_root(LIVE, entry)
    policies = {p["dataset"]: DatasetPolicy.from_descriptor(descriptor_of(p))
                for p in root["spec"]["etlReceipts"]["policies"]}
    receipts = f"{LIVE}/{entry['prefix']}/{entry['etlReceipts']['key']}"
    args.scratch.mkdir(parents=True, exist_ok=True)
    report, failed = {"live": entry["artifactDigest"]}, False
    for dataset, policy in sorted(policies.items()):
        live = args.scratch / f"{dataset}.parquet"
        if not live.exists():
            with httpx.stream("GET", f"{LIVE}/{entry['prefix']}/{dataset}.parquet", timeout=120) as response:
                response.raise_for_status()
                with live.open("wb") as out:
                    for chunk in response.iter_bytes(1 << 20):
                        out.write(chunk)
        built = args.generation / f"{dataset}.parquet"
        projected = args.scratch / f"{dataset}.projected.parquet"
        pq.write_table(pq.read_table(built, columns=policy.subject_schema.names), projected)
        key = list(policy.identity_fields)
        tables = table_differences(projected, live, key=key)
        copies = receipt_copy_differences(built, receipts, dataset=dataset, key=key, columns=COPIES[dataset])
        failed |= bool(tables) or any(value for name, value in copies.items() if name != "receipts")
        report[dataset] = {"tables": tables, "receipt_copies": copies}
    print(json.dumps(report, indent=1))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
