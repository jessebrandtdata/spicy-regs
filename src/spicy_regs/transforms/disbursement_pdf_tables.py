"""Common validation and evidence lifecycle for typed congressional PDF readings."""

import gzip
import json
from collections import Counter
from datetime import date
from decimal import Decimal
from pathlib import Path

from spicy_regs.parquet_rows import write_rows
from spicy_regs.pipelines.congressional_disbursement_pages import (
    pin,
    retained_implementation,
    selected_sources,
    source_implementation,
    verify_pin,
)
from spicy_regs.pipelines.disbursement_page_profiles import record_kind, validate_page_content


def json_value(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")) if value is not None else None


def money(value):
    return exact_decimal(value, 2)


def exact_decimal(value, scale):
    if value is None:
        return None
    amount = Decimal(value)
    if not amount.is_finite() or format(amount, f".{scale}f") != value:
        raise ValueError("Disbursement PDF value differs from its exact declared decimal scale")
    return amount


def source_columns(source, entry, page):
    start = date.fromisoformat(source["period_start"])
    return {
        "source_file_id": source["id"],
        "source_url": source["url"],
        "source_sha256": source["source_sha256"],
        "package_id": source.get("package_id"),
        "source_page": page["source_page"],
        "printed_page": page.get("printed_page", page.get("context", {}).get("printed_page")),
        "report_year": start.year,
        "report_start_month": start.month,
        "report_period_start": start,
        "report_period_end": date.fromisoformat(source["period_end"]),
        "reader_version": page["rule"],
        "reading_sha256": entry["output"]["sha256"],
    }


def page_columns(page):
    unassigned = [row for row in page["lines"] if row["role"] == "unassigned_text"]
    return {
        "page_status": page["status"],
        "native_text": page["text"],
        "native_word_count": len(page["words"]),
        "image_count": page["image_count"],
        "record_count": len(page["records"]),
        "unassigned_line_count": len(unassigned),
        "unassigned_lines_json": json_value(unassigned),
        "outside_ledger_word_indices": page["outside_ledger_word_indices"],
        "issue_codes": page["issues"],
    }


def pages(source, entry):
    counts, kinds = Counter(), Counter()
    pages = records = repairs = 0
    selected = entry.get("selected_pages")
    profile = entry.get("reading_profile", "ledger")
    with gzip.open(verify_pin(entry["output"]), "rt", encoding="utf-8") as stream:
        for line in stream:
            page = json.loads(line)
            expected_page = selected[pages] if selected is not None and pages < len(selected) else pages + 1
            pages += 1
            if (
                page["source_sha256"] != source["source_sha256"]
                or page["source_page"] != expected_page
                or page["page"] != expected_page
                or (selected is not None and pages > len(selected))
            ):
                raise ValueError("Disbursement PDF page reading differs from its selected complete original")
            if page["physical_pages"] != entry.get("physical_pages", entry["pages"]):
                raise ValueError("Disbursement PDF original physical page count differs from its manifest")
            validate_page_content(page, profile)
            counts[page["status"]] += 1
            kinds.update(record_kind(row) for row in page["records"])
            records += len(page["records"])
            repairs += sum(len(row.get("repairs", [])) for row in page["records"])
            yield page
    if (
        (pages, records, repairs) != (entry["pages"], entry["records"], entry["repairs"])
        or dict(counts) != entry["page_statuses"]
        or dict(kinds) != entry["row_kinds"]
    ):
        raise ValueError("Disbursement PDF page, record, role or repair population differs from its manifest")


def build_pdf_tables(
    manifest_path: Path, output_dir: Path, *, evidence, chamber, tables, schemas, rows, profile="ledger"
):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    manifest = json.loads(manifest_path.read_text())
    if (
        manifest.get("schema") != "congressional-disbursement-pdf-reading/1"
        or manifest.get("chamber") != chamber
        or manifest.get("reading_profile", "ledger") != profile
        or not manifest.get("complete")
        or manifest.get("pending")
    ):
        raise ValueError("Disbursement PDF tables require a complete selected PDF page reading")
    selection = verify_pin(manifest["selection"])
    sources = {source["id"]: source for source in selected_sources(selection, chamber)}
    entries = manifest["files"]
    if (
        len(entries) != len(sources)
        or {entry["id"] for entry in entries} != set(sources)
        or manifest["selected"] != list(sources)
    ):
        raise ValueError("Disbursement PDF reading membership differs from its pinned PDF selection")
    if (
        sum(entry["pages"] for entry in entries) != manifest["pages"]
        or sum(entry["records"] for entry in entries) != manifest["records"]
    ):
        raise ValueError("Disbursement PDF manifest totals differ from selected source outcomes")
    store = CentralRawArchiveStore(Path(manifest["archive_root"]), create=False)
    evidence.retain_file(manifest_path, stage=chamber + "-page-reading-manifest")
    evidence.retain_file(selection, stage=chamber + "-pdf-selection")
    retained_files = retained_implementation(manifest)
    for artifact in retained_files.values():
        evidence.retain_file(verify_pin(artifact), stage=chamber + "-reader-implementation")
    used, partitions = [], set()
    for entry in entries:
        source = sources[entry["id"]]
        if entry.get("reading_profile", "ledger") != profile or entry.get("selected_pages") != source.get(
            "reading_pages"
        ):
            raise ValueError("Disbursement PDF profile or selected page scope differs")
        if entry.get("source_implementation") != source_implementation(
            source, manifest["implementation"], retained_files=retained_files
        ):
            raise ValueError("Disbursement PDF source-specific reading implementation differs")
        if entry["status"] != "read" or (entry["source_sha256"], entry["source_byte_size"]) != (
            source["source_sha256"],
            source["source_byte_size"],
        ):
            raise ValueError("Disbursement PDF file has no matching complete page reading")
        start = date.fromisoformat(source["period_start"])
        partition = (start.year, start.month)
        if partition in partitions:
            raise ValueError(
                "Multiple Disbursement PDF originals share a report period; qualify full/part coverage first"
            )
        partitions.add(partition)
        for table in tables:
            destination = (
                output_dir
                / table
                / f"report_year={start.year}"
                / f"report_start_month={start.month}"
                / "part-000000.parquet"
            )
            destination.parent.mkdir(parents=True, exist_ok=True)
            write_rows(rows(source, entry, table), destination, schemas[table])
        captured = evidence.retain_file(
            verify_pin(entry["output"]), stage=chamber + "-page-reading", source_id=source["url"]
        )
        if captured["sha256"] != entry["output"]["sha256"]:
            raise ValueError("Disbursement PDF reading changed during table conversion")
        blob = store.describe_blob(source["source_sha256"], source["source_byte_size"])
        if blob["encoding"] != "raw":
            raise ValueError("Disbursement PDF selection requires retained original PDF bytes")
        raw_path = store.root / blob["object"]
        evidence.retain_file(
            raw_path,
            stage=chamber + "-pdf-original",
            raw_source=True,
            source_id=source["url"],
            source_url=source["url"],
            acquisition_evidence=source.get("evidence_directory"),
        )
        used.append(
            {
                "source_id": source["url"],
                "source_version": source["source_sha256"],
                "version_kind": "sha256",
                "asset_kind": "pdf",
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
            "scope": {"id": chamber + "-pdf:" + pin(manifest_path)["sha256"], "finite": True},
            "used": used,
            "available": used,
            "expected": used,
        }
    )
    return tuple(output_dir / table for table in tables)
