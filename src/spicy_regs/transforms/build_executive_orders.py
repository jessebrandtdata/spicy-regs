"""One observation per executive order the Federal Register API serves, from an admitted all-years release.

The release's scope is every publication date the API serves (``FR_EPOCH`` through the day it was read) under
the executive-order predicate. That scope is the table's coverage; it is written once, in the Parquet file's
key-value metadata under ``COVERAGE_KEY``, never per row.

A row's pointers (``POINTERS``) name where its metadata was read: the release, the API page and its locator.
They move only when the publisher's record or the selected body result changes, so an order the publisher has
not touched keeps the row it was published with, byte for byte.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows
from spicy_regs.transforms.build_federal_register import FR_EPOCH

TABLE = "executive_order_observations"
OUTPUT = TABLE + ".parquet"
HISTORY_FIELDS = ("president", "disposition_notes", "executive_order_notes")
#: Where a row's metadata was read. Kept from the published row while the publisher's record and the body result
#: are unchanged; ``source_sha256`` is the locator's page digest, the raw original's address.
POINTERS = ("source_release_logical_id", "source_release_digest", "metadata_locator_json", "source_sha256")
#: Parquet key-value metadata holding the release's requested scope: what "every order the API serves" meant.
COVERAGE_KEY = "spicy_regs.executive_orders.coverage"
SCHEMA = pa.schema(
    [
        ("publication_year", pa.int32()),
        ("executive_order_number", pa.string()),
        ("document_number", pa.string()),
        ("publication_date", pa.date32()),
        ("signing_date", pa.date32()),
        ("title", pa.string()),
        ("president_identifier", pa.string()),
        ("president_name", pa.string()),
        ("president_json", pa.string()),
        ("disposition_notes", pa.string()),
        ("executive_order_notes", pa.string()),
        ("metadata_field_states_json", pa.string()),
        ("body_status", pa.string()),
        ("body_text", pa.string()),
        ("body_sha256", pa.string()),
        ("text_sha256", pa.string()),
        ("body_format", pa.string()),
        ("body_url", pa.string()),
        ("body_observed_at", pa.string()),
        ("text_derivation", pa.string()),
        ("text_derivation_version", pa.string()),
        ("text_cleanup_json", pa.string()),
        ("body_result_json", pa.string()),
        ("offered_renditions_json", pa.string()),
        ("source_record_id", pa.string()),
        ("source_release_logical_id", pa.string()),
        ("source_release_digest", pa.string()),
        ("metadata_locator_json", pa.string()),
        ("raw_metadata_json", pa.string()),
        ("source_sha256", pa.string()),
    ]
)


def json_value(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def record_pin(record) -> str:
    """The content pin of one publisher record: SpicyDocs' canonical record digest."""
    from spicy_docs.sources.federal_register.native import source_record_digest

    return source_record_digest(record)


def _date(value):
    if value is None:
        return None
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("Executive order date must use its exact canonical spelling")
    return parsed


def require_all_orders(outcome) -> dict:
    """The release's scope, refused unless it is every publication date the API serves and complete."""
    scope, count = outcome.get("requestedScope"), outcome.get("publishedRecordCount")
    if (
        not isinstance(scope, dict)
        or set(scope) != {"publishedFrom", "publishedThrough", "presidentialDocumentType"}
        or scope["publishedFrom"] != FR_EPOCH.isoformat()
        or scope["presidentialDocumentType"] != "executive_order"
        or not isinstance(scope["publishedThrough"], str)
        or _date(scope["publishedThrough"]) < FR_EPOCH
        or type(count) is not int
        or count < 0
        or outcome.get("failedRecordCount") != 0
        or outcome.get("discardedObservationCount") != 0
        or outcome.get("discoveredRecordCount") != count
    ):
        raise ValueError("Executive orders require the complete admitted population of every year the API serves")
    return scope


def _body_values(result):
    from spicy_docs.extraction.body_text import body_text
    from spicy_docs.extraction.derivation import BODY_TEXT_DERIVATION_VERSION

    values = {
        key: None
        for key in (
            "body_text",
            "body_sha256",
            "text_sha256",
            "body_format",
            "body_url",
            "body_observed_at",
            "text_derivation",
            "text_derivation_version",
            "text_cleanup_json",
        )
    }
    acquisition = result.acquisition
    if acquisition is None:
        return values
    capture = acquisition.body
    if acquisition.format == "xml" and acquisition.identity.document_type != "PRESDOCU":
        raise ValueError("Executive-order XML must be a presidential document")
    # The source reader validates the exact rendition first. Its HTML spelling
    # differs from the shared extractor's; publisher text may itself be an HTML pre.
    media_type = (capture.content_type or "").split(";", 1)[0].strip().lower()
    rendition = (
        "htm"
        if acquisition.format == "html" or (acquisition.format == "txt" and media_type == "text/html")
        else acquisition.format
    )
    derived = body_text(
        SimpleNamespace(
            format=rendition,
            body=SimpleNamespace(media_type=media_type or None, byte_size=capture.byte_size),
            body_capture=capture,
        )
    )
    if not derived.text.strip():
        raise ValueError("Validated executive-order rendition produced no searchable text")
    return {
        "body_text": derived.text,
        "body_sha256": capture.sha256,
        "text_sha256": "sha256:" + hashlib.sha256(derived.text.encode()).hexdigest(),
        "body_format": acquisition.format,
        "body_url": capture.resolved_url,
        "body_observed_at": capture.observed_at,
        "text_derivation": derived.derivation,
        "text_derivation_version": BODY_TEXT_DERIVATION_VERSION,
        "text_cleanup_json": json_value(asdict(derived.record)),
    }


def build_executive_orders(reader, output_dir: Path, *, body_paths, load_body, evidence, prior=None):
    """Read admitted metadata and explicit local body results; never acquire a body.

    ``prior`` maps the published rows by ``source_record_id``. An order whose publisher record (``record_pin``) and
    body result are unchanged keeps that row's ``POINTERS``, read back from ``evidence.store`` and retained again,
    so the generation's evidence holds every page a row points to.
    """
    from spicy_docs.sources.federal_register.native import MAX_PAGE_BYTES, federal_register_source_record_id

    outcome = reader.collection_outcome
    scope = require_all_orders(outcome)
    count = outcome["publishedRecordCount"]
    prior = prior or {}
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    def retain_page(ref, *, published):
        """Retain the API page a row points to once: the release's own, or a published row's from the raw archive."""
        if published:
            with evidence.store.open(ref) as stream:
                body = stream.read(MAX_PAGE_BYTES + 1)
        else:
            body = reader.read_evidence(ref, max_bytes=MAX_PAGE_BYTES)
        saved = evidence.retain_bytes(body, raw_source=True, stage="executive-order-metadata", evidence_blob_ref=ref)
        if saved.get("sha256") != ref:
            raise ValueError("Executive-order metadata page differs from the digest its row points to")

    def rows():
        seen, retained = set(), set()
        for item, locator in zip(reader.iter_records(), reader.iter_record_evidence(), strict=True):
            record = item["record"]
            identity = federal_register_source_record_id(record)
            published = _date(record["publication_date"])
            number = record.get("executive_order_number")
            if (
                item["sourceRecordId"] != identity
                or locator["sourceRecordId"] != identity
                or identity in seen
                or published is None
                or record.get("type") != "Presidential Document"
                or record.get("subtype") != "Executive Order"
                or not isinstance(number, str)
                or not number.isascii()
                or not number.isdecimal()
            ):
                raise ValueError("Executive-order record differs from the selected identity or type")
            seen.add(identity)
            if identity not in body_paths:
                raise ValueError("Executive-order body result is missing; omissions are not successful coverage")
            body_path = body_paths[identity]
            result = load_body(body_path, record=record)
            failure = result.manifest.get("failure")
            if isinstance(failure, dict) and failure.get("credential_refused"):
                from spicy_docs.transport.credentials import CredentialRefusedError

                # A retry may replace the path after the provider has read the refusal.
                evidence.retain_bytes(
                    result.manifest_bytes, stage="executive-order-body-result", source_record_id=identity,
                    original_location=str(Path(body_path).absolute()),
                )
                evidence.event(
                    "refusal", stage="executive-order-body", source_record_id=identity,
                    error_type="CredentialRefusedError", reason_code="access_refused",
                    status_code=failure.get("status_code"),
                    body_retained=False, credential_refused=True, message=None, response=None,
                )
                raise CredentialRefusedError("Retained source access refusal; explicit retry is required")
            for capture in result.captures:
                evidence.capture(capture, stage="executive-order-body")
            if result.refused_response is not None:
                from spicy_docs.reading.refusals import attach_refused_response

                error = ValueError("Retained executive-order body acquisition failed")
                attach_refused_response(error, result.refused_response)
                evidence.refusal(error, stage="executive-order-body")
            evidence.retain_bytes(
                json_value(result.manifest).encode(), stage="executive-order-body-result", source_record_id=identity
            )
            body = _body_values(result)
            for name in ("disposition_notes", "executive_order_notes"):
                if record.get(name) is not None and not isinstance(record[name], str):
                    raise ValueError("Executive-order history note is not literal text")
            row = {
                "publication_year": published.year,
                "executive_order_number": number,
                "document_number": record["document_number"],
                "publication_date": published,
                "signing_date": _date(record.get("signing_date")),
                "title": record.get("title"),
                "president_identifier": (record.get("president") or {}).get("identifier"),
                "president_name": (record.get("president") or {}).get("name"),
                "president_json": json_value(record["president"]) if record.get("president") is not None else None,
                "disposition_notes": record.get("disposition_notes"),
                "executive_order_notes": record.get("executive_order_notes"),
                "metadata_field_states_json": json_value(
                    {
                        name: "missing"
                        if name not in record
                        else "null"
                        if record[name] is None
                        else "empty"
                        if record[name] in ("", {}, [])
                        else "stated"
                        for name in HISTORY_FIELDS
                    }
                ),
                "body_status": result.outcome,
                **body,
                "body_result_json": json_value(result.manifest),
                "offered_renditions_json": json_value(
                    {
                        name: record[name]
                        for name in ("html_url", "pdf_url", "body_html_url", "full_text_xml_url")
                        if name in record
                    }
                ),
                "source_record_id": identity,
                "source_release_logical_id": reader.pin.logical_id,
                "source_release_digest": reader.pin.artifact_digest,
                "metadata_locator_json": json_value(locator),
                "raw_metadata_json": json_value(record),
            }
            held = prior.get(identity)
            carried = (
                held is not None
                and held["body_result_json"] == row["body_result_json"]
                and record_pin(json.loads(held["raw_metadata_json"])) == record_pin(record)
            )
            if carried:
                row.update({name: held[name] for name in POINTERS[:3]})
            row["source_sha256"] = json.loads(row["metadata_locator_json"])["evidenceBlobRef"]
            if row["source_sha256"] not in retained:
                retain_page(row["source_sha256"], published=carried)
                retained.add(row["source_sha256"])
            yield row
        if len(seen) != count or set(body_paths) != seen:
            raise ValueError("Executive-order count or body-result membership differs from the admitted release")

    evidence.event(
        "executive-order-source-release",
        source_logical_id=reader.pin.logical_id,
        source_artifact_digest=reader.pin.artifact_digest,
        collection_outcome=outcome,
    )
    coverage = json_value({**scope, "sourceReleaseDigest": reader.pin.artifact_digest})
    return write_rows(rows(), output_dir / OUTPUT, SCHEMA.with_metadata({COVERAGE_KEY: coverage}), batch_size=32)
