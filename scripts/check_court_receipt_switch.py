"""Compare a receipts-free court build with the live generation it replaces, before the first publish (ss-vse3).

    uv run --frozen --extra comments-reader python scripts/check_court_receipt_switch.py \
        --family court-citations --generation "$WORK/citations/generations/<digest>" --scratch "$WORK/live"

For each table: the receipt route's columns must be the live table (schema, rows both ways, key at every position;
``receipt_switch.table_differences``), and every promoted column must equal its copy in the live receipts
(``receipt_switch.receipt_copy_differences``, read by range from the public bucket). The live tables are downloaded
into ``--scratch`` (1.0 GB for court-citations, 0.41 GB for court-opinions) and checked against their pins (an
interrupted download is never reused); the receipts are read by range, not downloaded. DuckDB is held to 4 GB.
Exits 1 on any difference. Only meaningful when the build read the same edition the live generation holds.
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


def _live_table(entry, dataset: str, scratch: Path) -> Path:
    """The live table in ``scratch``, checked against its pin; an interrupted download is never reused."""
    import hashlib

    import httpx

    (member,) = [m for m in entry["tables"].items() if m[0] == f"{dataset}.parquet"]
    pin = member[1]
    live = scratch / f"{dataset}.parquet"

    def pinned(path: Path) -> bool:
        if not path.is_file() or path.stat().st_size != pin["byteSize"]:
            return False
        with path.open("rb") as stream:
            return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest() == pin["sha256"]

    if pinned(live):
        return live
    partial = scratch / f".{dataset}.parquet.partial"
    with httpx.stream("GET", f"{LIVE}/{entry['prefix']}/{dataset}.parquet", timeout=120) as response:
        response.raise_for_status()
        with partial.open("wb") as out:
            for chunk in response.iter_bytes(1 << 20):
                out.write(chunk)
    if not pinned(partial):
        raise SystemExit(f"{dataset}: the downloaded live table differs from its pin")
    partial.replace(live)
    return live


def main(argv=None) -> int:
    import duckdb

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
        live = _live_table(entry, dataset, args.scratch)
        built = args.generation / f"{dataset}.parquet"
        projected = args.scratch / f"{dataset}.projected.parquet"
        # Streamed, not read whole: the citation map is 78 million rows.
        names = ", ".join('"' + name.replace('"', '""') + '"' for name in policy.subject_schema.names)
        with duckdb.connect(config={"memory_limit": "2GB", "threads": "4"}) as con:
            con.execute(f"COPY (SELECT {names} FROM read_parquet('{built}')) TO '{projected}' "
                        "(FORMAT PARQUET, COMPRESSION ZSTD)")
        key = list(policy.identity_fields)
        tables = table_differences(projected, live, key=key)
        copies = receipt_copy_differences(built, receipts, dataset=dataset, key=key, columns=COPIES[dataset])
        failed |= bool(tables) or any(value for name, value in copies.items() if name != "receipts")
        report[dataset] = {"tables": tables, "receipt_copies": copies}
    print(json.dumps(report, indent=1))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
