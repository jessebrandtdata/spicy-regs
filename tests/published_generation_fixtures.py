"""One family laid out under a local root as the bucket lays out a generation, with its publication index entry."""

from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import RECEIPT_SCHEMA

GENERATION = "generation-under-test"


def _digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _columns(path: Path) -> list[list[str]]:
    with duckdb.connect() as con:
        return [[row[0], row[1]] for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()]


def published(root: Path, family: str, subjects: dict[str, Path], receipts: pa.Table, *, row_group: int = 2000,
              generation: str = GENERATION) -> dict:
    """One family's index entry, its files laid out under ``root`` as the bucket lays out a generation."""
    digest = hashlib.sha256(family.encode()).hexdigest()
    prefix = f"generations/{family}/{digest}"
    (root / prefix).mkdir(parents=True)
    tables = {}
    for table, source in subjects.items():
        target = root / prefix / f"{table}.parquet"
        target.write_bytes(source.read_bytes())
        tables[f"{table}.parquet"] = {"sha256": _digest(target), "byteSize": target.stat().st_size,
                                      "rows": pq.ParquetFile(target).metadata.num_rows, "columns": _columns(target)}
    member = root / prefix / "etl_receipts.parquet"
    pq.write_table(receipts.cast(RECEIPT_SCHEMA), member, row_group_size=row_group, compression="zstd")
    return {
        "prefix": prefix, "logicalId": f"urn:test:{family}", "artifactDigest": "sha256:" + digest, "tables": tables,
        "etlReceipts": {"key": "etl_receipts.parquet", "sha256": _digest(member), "byteSize": member.stat().st_size,
                        "rows": receipts.num_rows, "columns": _columns(member), "generationId": generation,
                        "datasets": sorted(set(receipts.column("dataset").to_pylist()))},
    }


def index_of(families: dict[str, dict]) -> dict:
    return {"format": "spicy-regs-publication", "version": 2, "families": families}
