"""Write typed House ledger and summary partitions from pinned normalized CSVs.

The source owner performs all repairs and normalization. This adapter preserves
every source record and its audit, types exact decimals and dates, and keeps
detail and year-to-date/quarter-to-date summary rows in separate tables.
"""

from __future__ import annotations

import csv
import json
import re
from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows
from spicy_regs.schemas.house_disbursements import DETAIL_FIELDS, SCHEMAS, SUMMARY_FIELDS, TABLES
from spicy_regs.pipelines.congressional_disbursements import _digest, _read_acquisition, _verify_normalized

CSV_METADATA = (
    "source_sha256",
    "source_record_number",
    "source_line_start",
    "source_line_end",
    "report_year",
    "report_quarter",
    "row_kind",
    "issue_codes",
    "correction_rules",
    "correction_confidence",
)
_MONEY = re.compile(r"-?[0-9]+\.[0-9]{2}\Z")


def _typed(value: str, dtype: pa.DataType):
    if value == "":
        return None
    if pa.types.is_date32(dtype):
        return date.fromisoformat(value)
    if pa.types.is_decimal(dtype):
        if not _MONEY.fullmatch(value):
            raise ValueError("Normalized money must be an exact decimal with two places")
        return Decimal(value)
    return value


def _rows(source: dict, entry: dict, receipt: dict, csv_path: Path, audit_path: Path):
    fields = DETAIL_FIELDS if source["kind"] == "detail" else SUMMARY_FIELDS
    with csv_path.open(encoding="utf-8", newline="") as stream, audit_path.open(encoding="utf-8") as audits:
        reader = csv.DictReader(stream)
        if reader.fieldnames != [*CSV_METADATA, *(name for name, _ in fields)]:
            raise ValueError("Normalized House CSV columns differ from the source-owned layout")
        audit_line = next(audits, None)
        audit = json.loads(audit_line) if audit_line is not None else None
        count = 0
        for number, row in enumerate(reader, 2):
            if (
                row["source_sha256"] != source["source_sha256"]
                or row["source_record_number"] != str(number)
                or row["report_year"] != source["period_start"][:4]
                or int(row["report_quarter"]) != (date.fromisoformat(source["period_start"]).month - 1) // 3 + 1
            ):
                raise ValueError("Normalized House source identity, membership or reporting period differs")
            if audit is not None and audit["record_number"] < number:
                raise ValueError("House audit has a repeated or unmatched source record")
            matched = audit is not None and audit["record_number"] == number
            if matched != bool(row["issue_codes"] or row["correction_rules"]):
                raise ValueError("House flags/corrections and source audit membership differ")
            if matched and (
                audit["source_sha256"] != source["source_sha256"]
                or any(audit["values"][name] != (row[name] or None) for name, _ in fields)
            ):
                raise ValueError("House audit values differ from normalized CSV values")
            audit_json = None
            if matched:
                assert audit_line is not None
                audit_json = audit_line.rstrip("\n")
            yield {
                "source_file_id": source["id"],
                "source_url": source["url"],
                "source_sha256": row["source_sha256"],
                **{
                    key: int(row[key])
                    for key in (
                        "source_record_number",
                        "source_line_start",
                        "source_line_end",
                        "report_year",
                        "report_quarter",
                    )
                },
                "report_period_start": date.fromisoformat(source["period_start"]),
                "report_period_end": date.fromisoformat(source["period_end"]),
                "row_kind": row["row_kind"],
                **{
                    key: row[key].split(";") if row[key] else []
                    for key in ("issue_codes", "correction_rules", "correction_confidence")
                },
                "source_encoding": receipt["source_encoding"],
                "source_layout": receipt["source_layout"],
                "normalization_version": receipt["normalization_version"],
                "normalization_receipt_sha256": entry["receipt_sha256"],
                "audit_json": audit_json,
                **{name: _typed(row[name], dtype) for name, dtype in fields},
            }
            count += 1
            if matched:
                audit_line = next(audits, None)
                audit = json.loads(audit_line) if audit_line is not None else None
        if audit is not None or count != receipt["rows"] or count != entry["rows"]:
            raise ValueError("Normalized House row or audit population differs from its receipt")


def build_house_disbursements(manifest_path: Path, output_dir: Path, *, evidence):
    """Convert the complete selected CSV scope through the existing bounded Arrow writer."""
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    manifest = json.loads(manifest_path.read_text())
    if (
        manifest.get("schema") != "congressional-disbursement-normalization/1"
        or not manifest.get("complete")
        or manifest.get("pending")
    ):
        raise ValueError("House table conversion requires a complete selected normalization")
    acquisition_path = Path(manifest["acquisition"])
    if _digest(acquisition_path) != manifest["acquisition_sha256"]:
        raise ValueError("House acquisition pin differs")
    acquisition = _read_acquisition(acquisition_path)
    sources = {source["id"]: source for source in acquisition["files"]}
    if (
        len(manifest["files"]) != len(sources)
        or {entry["id"] for entry in manifest["files"]} != set(sources)
        or any(source["chamber"] != "house" or source["format"] != "csv" for source in sources.values())
    ):
        raise ValueError("House normalization membership differs from its selected CSV sources")
    if {source["kind"] for source in sources.values()} != set(TABLES):
        raise ValueError("House family requires both selected detail and summary sources")
    store = CentralRawArchiveStore(Path(acquisition["archive_root"]), create=False)
    output_dir.mkdir(parents=True, exist_ok=True)
    evidence.retain_file(manifest_path, stage="house-normalization-manifest")
    evidence.retain_file(acquisition_path, stage="house-acquisition-manifest")
    evidence.retain_file(Path(acquisition["selection"]), stage="house-source-selection")
    used, emitted = [], set()
    for entry in manifest["files"]:
        source = sources[entry["id"]]
        if entry["status"] != "normalized" or entry["source_sha256"] != source["source_sha256"]:
            raise ValueError("House file lacks a matching normalized outcome")
        _verify_normalized(entry)
        receipt_path = Path(entry["receipt"])
        receipt = json.loads(receipt_path.read_text())
        if (
            receipt["source_sha256"] != source["source_sha256"]
            or receipt["kind"] != source["kind"]
            or (receipt["report_period_start"], receipt["report_period_end"])
            != (source["period_start"], source["period_end"])
        ):
            raise ValueError("House receipt source selection differs")
        paths = {"audit" if name.endswith(".audit.jsonl") else "csv": Path(name) for name in receipt["artifacts"]}
        if set(paths) != {"csv", "audit"} or len(receipt["artifacts"]) != 2:
            raise ValueError("House normalization must pin exactly its CSV and audit")
        table = TABLES[source["kind"]]
        start = date.fromisoformat(source["period_start"])
        partition = (table, start.year, (start.month - 1) // 3 + 1)
        if partition in emitted:
            raise ValueError("Multiple originals selected for one House quarter/kind; resolve revisions explicitly")
        emitted.add(partition)
        target = (
            output_dir
            / table
            / f"report_year={partition[1]}"
            / f"report_quarter={partition[2]}"
            / "part-000000.parquet"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        write_rows(_rows(source, entry, receipt, paths["csv"], paths["audit"]), target, SCHEMAS[table])
        for path in (paths["csv"], paths["audit"]):
            captured = evidence.retain_file(path, stage="house-normalized-input", source_id=source["url"])
            if captured["sha256"] != receipt["artifacts"][str(path)]["sha256"]:
                raise ValueError("House normalized input changed during conversion")
        evidence.retain_file(receipt_path, stage="house-normalization-receipt", source_id=source["url"])
        blob = store.describe_blob(source["source_sha256"], source["source_byte_size"])
        raw_path = store.root / blob["object"]
        if blob["encoding"] != "raw":
            raise ValueError("House source selection requires original raw archive bytes")
        evidence.retain_file(
            raw_path,
            stage="house-csv-original",
            raw_source=True,
            source_id=source["url"],
            source_url=source["url"],
            acquisition_evidence=source["evidence_directory"],
        )
        used.append(
            {
                "source_id": source["url"],
                "source_version": source["source_sha256"],
                "version_kind": "sha256",
                "asset_kind": "csv",
                "sha256": source["source_sha256"],
                "bytes": source["source_byte_size"],
                "location": str(raw_path.resolve()),
            }
        )
    evidence.selected_inputs(
        {
            "format": "spicy-selected-inputs",
            "version": 1,
            "complete": True,
            "scope": {"id": "house-csv:" + _digest(manifest_path), "finite": True},
            "used": used,
            "available": used,
            "expected": used,
        }
    )
    return tuple(output_dir / table for table in TABLES.values())
