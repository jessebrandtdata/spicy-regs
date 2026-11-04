"""Bounded typed Federal Register candidates from immutable retained API pages.

The reader owns field classification, source identity and traversal. This module
owns only physical types and child rows. Originals remain the evidence for exact
source spelling; dates and mixed numeric/text CFR values have one query column.
"""

from __future__ import annotations

from collections import Counter
from datetime import date
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DATES = frozenset({"publication_date", "effective_on", "comments_close_on", "signing_date"})
INTEGERS = frozenset({"start_page", "end_page", "volume", "executive_order_number"})
TEXT_LISTS = frozenset({"agency_names", "corrections", "docket_ids", "regulation_id_numbers", "topics"})
JSON_LISTS = frozenset({"agencies", "cfr_references"})
OBJECTS = frozenset({"president", "regulations_dot_gov_info"})
TEXT = frozenset({"abstract", "action", "body_html_url", "correction_of", "disposition_notes",
                  "executive_order_notes", "full_text_xml_url", "html_url", "pdf_url", "subtype", "title", "type"})
VERSION = "federal-register-layer1/1"
LINK_FIELDS = [("source_record_id", pa.string()), ("document_number", pa.string()),
               ("publication_date", pa.date32()), ("source_sha256", pa.string()), ("source_locator", pa.string())]


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _date(value: str | None) -> date | None:
    if value is None:
        return None
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError(f"Noncanonical Federal Register date: {value!r}")
    return parsed


def document_schema() -> pa.Schema:
    from spicy_docs.sources.federal_register.native import DOCUMENT_FIELDS

    accounted = DATES | INTEGERS | TEXT_LISTS | JSON_LISTS | OBJECTS | TEXT | {"document_number", "significant"}
    if accounted != DOCUMENT_FIELDS:
        raise ValueError("Federal Register reader fields differ from the typed mapping")
    fields = dict(LINK_FIELDS)
    for name in sorted(DOCUMENT_FIELDS - fields.keys()):
        fields[name] = (pa.date32() if name in DATES else pa.int64() if name in INTEGERS else
                        pa.bool_() if name == "significant" else pa.list_(pa.string())
                        if name in TEXT_LISTS | JSON_LISTS else pa.string())
    fields["member_names"] = pa.list_(pa.string())
    return pa.schema(fields)


def normalized_document(record: dict, sha256: str, locator: str) -> dict:
    from spicy_docs.sources.federal_register.native import classify_document, federal_register_source_record_id

    classified = classify_document(record)
    row = dict(classified)
    for name in DATES:
        row[name] = _date(record.get(name))
    number = record.get("executive_order_number")
    if number is not None:
        if not number.isascii() or not number.isdecimal():
            raise ValueError(f"Unsupported Federal Register executive_order_number: {number!r}")
        row["executive_order_number"] = int(number)
    for name in JSON_LISTS:
        values = record.get(name)
        row[name] = None if values is None else [None if item is None else _json(item) for item in values]
    for name in OBJECTS:
        row[name] = None if record.get(name) is None else _json(record[name])
    return {**row, "source_record_id": federal_register_source_record_id(classified), "source_sha256": sha256,
            "source_locator": locator, "member_names": list(record)}


def _children(record: dict, parent: dict, member: str) -> list[dict]:
    rows = []
    for position, value in enumerate(record.get(member) or []):
        fields = {}
        for name, item in value.items():
            numeric = name in ({"id", "parent_id"} if member == "agencies" else {"title"})
            fields[name] = (None if item is None else int(item)) if numeric else (
                None if item is None else str(item))
        rows.append({**{name: parent[name] for name, _ in LINK_FIELDS}, **fields, "position": position,
                     "member_names": list(value), "source_locator": parent["source_locator"] + f"/{member}/{position}"})
    return rows


def build(manifest_path: Path, archive_root: Path, output_dir: Path, *, max_records: int = 10000) -> Path:
    """Validate a finite held traversal, then write one source table and ordered children.

    Identical dated records collapse; conflicting observations refuse. Every
    distinct input URL is required exactly once, and every selected page must be
    consumed. A failed validation writes no candidate output directory.
    """
    from spicy_docs.sources.federal_register import native
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    if not 1 <= max_records <= 10000:
        raise ValueError("A Federal Register candidate requires max_records between 1 and 10000")
    if output_dir.exists():
        raise FileExistsError(output_dir)
    schema = document_schema()
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    pages = manifest["pages"]
    if not isinstance(pages, list) or not 1 <= len(pages) <= 100:
        raise ValueError("A Federal Register candidate requires 1 to 100 retained pages")
    by_url = {page["url"]: page for page in pages}
    if len(by_url) != len(pages):
        raise ValueError("Repeated Federal Register request cursor in retained manifest")
    archive = CentralRawArchiveStore(archive_root, create=False)
    used: dict[str, dict] = {}

    def fetch(url: str) -> bytes:
        if url not in by_url:
            raise ValueError(f"Required Federal Register page is not retained in this manifest: {url}")
        item = by_url[url]
        with archive.open(item["sha256"]) as stream:
            raw = stream.read(native.MAX_PAGE_BYTES + 1)
        if "sha256:" + hashlib.sha256(raw).hexdigest() != item["sha256"]:
            raise ValueError("Retained Federal Register original digest differs")
        used[url] = {**item, "bytes": len(raw)}
        return raw

    rows, agencies, cfr, candidates = [], [], [], []
    identities: dict[str, str] = {}
    members: Counter = Counter()
    nulls: Counter = Counter()
    populated: Counter = Counter()
    duplicate_records = 0
    observed_records = 0
    window = None
    for page in native.iter_federal_register_pages(fetch, query_scope=manifest["query_scope"], traversals=1):
        response = native.parse_page_response(page.response_bytes)
        if page.source_cursor is None:
            window = native.federal_register_request_window(page.request_key)
        # The owner's capped probes contain no admitted records; split leaf windows follow them.
        if response["count"] >= native.RESULT_CAP:
            continue
        for position, record in enumerate(response["results"]):
            observed_records += 1
            if observed_records > max_records:
                raise ValueError("Federal Register candidate exceeds max_records")
            if set(record) != native.DOCUMENT_FIELDS:
                raise ValueError("Federal Register record members differ from the requested field set")
            if window is not None and not window.published_from.isoformat() <= record["publication_date"] <= window.published_through.isoformat():
                raise ValueError("Federal Register record is outside its requested window")
            if manifest["query_scope"].get("presidentialDocumentType") and record.get("subtype") != "Executive Order":
                raise ValueError("Federal Register record differs from the executive-order predicate")
            sha256 = used[page.request_key]["sha256"]
            row = normalized_document(record, sha256, f"/results/{position}")
            identity, fingerprint = row["source_record_id"], json.dumps(record, sort_keys=True)
            if identity in identities:
                if identities[identity] != fingerprint:
                    raise ValueError(f"Conflicting Federal Register observations for {identity}")
                duplicate_records += 1
                continue
            identities[identity] = fingerprint
            rows.append(row)
            members.update(record.keys())
            nulls.update(name for name, value in record.items() if value is None)
            populated.update(name for name, value in record.items() if value is not None and value != [] and value != {})
            agencies.extend(_children(record, row, "agencies"))
            cfr.extend(_children(record, row, "cfr_references"))
            from spicy_regs.ontology.federal_register import linked_docket_ids
            for label_position, label in enumerate(record.get("docket_ids") or []):
                for candidate_position, docket in enumerate(linked_docket_ids(label)):
                    candidates.append({**{name: row[name] for name, _ in LINK_FIELDS}, "label_position": label_position,
                                       "candidate_position": candidate_position, "label": label, "docket_id": docket})
    if set(used) != set(by_url):
        raise ValueError("Retained Federal Register manifest contains unconsumed pages")
    children_common = [*LINK_FIELDS, ("position", pa.int32()), ("member_names", pa.list_(pa.string()))]
    tables = {
        "federal_register_documents": (rows, schema),
        "federal_register_agencies": (agencies, pa.schema([*children_common, ("id", pa.int64()), ("parent_id", pa.int64()),
            *[(name, pa.string()) for name in ("json_url", "name", "raw_name", "slug", "url")]])),
        "federal_register_cfr_references": (cfr, pa.schema([*children_common,
            ("title", pa.int64()), *[(name, pa.string()) for name in ("part", "chapter", "citation_url")]])),
        "federal_register_docket_candidates": (candidates, pa.schema([*LINK_FIELDS, ("label_position", pa.int32()),
            ("candidate_position", pa.int32()), ("label", pa.string()), ("docket_id", pa.string())])),
    }
    # Convert before creating the output, so invalid physical values cannot leave a successful-looking candidate.
    converted = {name: pa.Table.from_pylist(data, schema=table_schema) for name, (data, table_schema) in tables.items()}
    output_dir.mkdir(parents=True)
    for name, table in converted.items():
        pq.write_table(table.replace_schema_metadata({"layer1.version": VERSION}), output_dir / f"{name}.parquet", compression="zstd")
    receipt = {"version": VERSION, "reader_schema": native.SCHEMA_VERSION, "reader_schema_digest": native.source_schema_digest(),
               "input_manifest_sha256": "sha256:" + hashlib.sha256(manifest_bytes).hexdigest(),
               "requested_fields": sorted(native.DOCUMENT_FIELDS), "query_scope": manifest["query_scope"],
               "observed_records": observed_records, "duplicate_records": duplicate_records, "rows": len(rows),
               "coverage": {name: {"present": members[name], "null": nulls[name], "populated": populated[name],
                                    "absent": len(rows) - members[name], "unrequested": False} for name in sorted(native.DOCUMENT_FIELDS)},
               "tables": {name: {"rows": len(data), "inputs": list(used.values())} for name, (data, _) in tables.items()}}
    (output_dir / "inputs.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return output_dir / "federal_register_documents.parquet"
