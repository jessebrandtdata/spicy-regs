"""Capture explicitly selected CRS files and derive text from their exact originals.

Metadata revisions keep their existing report/update identity. These are file
subjects: a numbered PDF is version-bound; offered mutable HTML is an unbound
observation. The central archive owns originals and immutable receipts. Local
shared-receipt bundles own the completed body and text recovery phases.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from importlib.metadata import version
from pathlib import Path
from uuid import uuid4
from typing import TYPE_CHECKING

import pyarrow as pa

from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, read_with_receipts, write_dataset
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.pdf_text import PYPDF_VERSION, extract_pdf_text

if TYPE_CHECKING:
    from spicy_docs.sources.congress.crs_files import CrsFileSelection, CrsHtmlSelection, CrsReportSelection

DATASET = "crs_report_files"
FAMILY = "crs-report-files"
KEY = DATASET + ".parquet"
TEXT_PROCESSOR = "spicy-regs-crs-file-text/1"
_TEXT = pa.string()
SCHEMA = pa.schema([
    *[(name, _TEXT) for name in (
        "report_id", "metadata_update_date", "metadata_source_sha256", "rendition", "source_family",
        "publisher_version", "version_binding", "source_url", "resolved_url", "source_sha256",
        "body_state", "capture_id", "captured_at", "capture_receipt_key", "capture_receipt_sha256",
        "association_receipt_key", "association_receipt_sha256", "association_kind", "error",
        "provenance_basis", "original_receipts_json",
        "content_identity_state", "content_report_id", "content_publisher_version",
        "qualified_publisher_version",
        "file_validation_basis",
        "text_content", "text_state", "text_sha256", "text_input_sha256", "text_processor",
        "text_processor_sha256", "provider_version",
    )],
    ("source_bytes", pa.int64()), ("page_count", pa.int64()), ("capture_receipt_entry", pa.int64()),
])
POLICY = DatasetPolicy(DATASET, SCHEMA,
    ("report_id", "metadata_update_date", "rendition", "source_url", "association_receipt_sha256"),
    (), policy_version="crs-report-files/1")


def _sha(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def text_processor_pin() -> str:
    """Pin local rules, the maintained byte readers, and their actual backend."""
    from spicy_docs.extraction import pypdf
    from spicy_docs.reading import markup
    from spicy_regs.transforms import pdf_text

    digest = hashlib.sha256()
    for path in (Path(__file__), Path(pdf_text.__file__), Path(markup.__file__), Path(pypdf.__file__)):
        digest.update(path.name.encode() + b"\0" + path.read_bytes())
    digest.update(version("pypdf").encode())
    return "sha256:" + digest.hexdigest()


def extract_text(body: bytes, rendition: str) -> dict:
    """Embedded PDF text or literal HTML text nodes; no OCR or version inference."""
    if rendition == "pdf":
        result = extract_pdf_text(body)
        return {"text_content": result.text if result.ok else None, "text_state": result.status.value,
                "page_count": result.page_count, "error": result.error}
    from spicy_docs.reading.markup import read_html_events

    ignored, parts = [], []
    for event in read_html_events(body).events:
        if event.kind == "start" and event.name in {"script", "style", "title"}:
            ignored.append(event.name)
        elif event.kind == "end" and ignored and event.name == ignored[-1]:
            ignored.pop()
        elif event.kind == "text" and event.text and not ignored:
            parts.append(event.text)
    text = " ".join(" ".join(parts).split())
    return {"text_content": text or None, "text_state": "ok" if text else "empty", "page_count": None,
            "error": None}


def _check_content_identity(row: dict) -> None:
    """Compare an explicit printed CRS footer only; an absent footer stays unknown."""
    row["content_identity_state"] = "unknown"
    if row["rendition"] != "pdf" or not row.get("text_content"):
        return
    statements = set(re.findall(r"\b([A-Z]{1,4}[0-9]{1,6}|[0-9]{2}-[0-9]{2,4})\s*[·•|]\s*"
                               r"VERSION\s+([0-9]{1,4})\s*[·•|]\s*UPDATED\b", row["text_content"]))
    if not statements:
        return
    if len(statements) == 1:
        row["content_report_id"], row["content_publisher_version"] = next(iter(statements))
    if statements == {(row["report_id"], row["publisher_version"])}:
        row["content_identity_state"] = "matching-explicit-footer"
        row["qualified_publisher_version"] = row["publisher_version"]
    else:
        row.update(content_identity_state="mismatching-explicit-footer", body_state="content_mismatch",
                   qualified_publisher_version=None,
                   text_state="identity_mismatch", text_content=None,
                   error="Explicit printed CRS report/version footer differs from selected numbered file")


def _witnesses(row: Mapping) -> list[dict]:
    files = [{"source_id": row["report_id"], "source_uri": row["source_url"],
              "sha256": row["source_sha256"], "locator": None,
              "body_version": row.get("publisher_version")}] if row.get("source_sha256") else []
    return [*files, {"source_id": "congress.gov:crs-detail", "source_uri": None,
                    "sha256": row["metadata_source_sha256"], "locator": "/CRSReport", "body_version": None}]


def _replace_association(rows: list[dict], row: dict, *, max_rows: int) -> None:
    """One insertion rule for successful files and refused file associations."""
    key = tuple(row[name] for name in POLICY.identity_fields)
    for index, held in enumerate(rows):
        if tuple(held[name] for name in POLICY.identity_fields) == key:
            rows[index] = row
            return
    if len(rows) >= max_rows:
        raise ValueError("CRS merged file subjects exceed the declared row bound")
    rows.append(row)


def _write(rows: Sequence[dict], directory: Path, generation_id: str) -> Path:
    subjects, _ = write_dataset(
        ((row, ReceiptContext(generation_id, str(index), TEXT_PROCESSOR, _witnesses(row)))
         for index, row in enumerate(rows)), directory, POLICY, generation_id=generation_id)
    assert subjects is not None
    return subjects


def _read(bundle: Path, *, max_bytes: int, max_rows: int) -> list[dict]:
    """Admit the whole bounded subject/receipt pair before using any completion."""
    import pyarrow.parquet as pq

    subjects, receipts = bundle / KEY, bundle / "etl_receipts.parquet"
    if sum(path.stat().st_size for path in (subjects, receipts)) > max_bytes:
        raise ValueError("CRS prior/phase subject and receipt bytes exceed the declared bound")
    if pq.read_metadata(subjects).num_rows > max_rows:
        raise ValueError("CRS prior/phase rows exceed the declared bound")
    decoded = 0
    for path in (subjects, receipts):
        metadata = pq.read_metadata(path)
        decoded += sum(metadata.row_group(i).column(j).total_uncompressed_size
                       for i in range(metadata.num_row_groups) for j in range(metadata.num_columns))
    if decoded > max_bytes:
        raise ValueError("CRS prior/phase decoded subject and receipt bytes exceed the declared bound")
    generations = pq.read_table(receipts, columns=["generation_id"])["generation_id"].unique().to_pylist()
    if len(generations) != 1:
        raise ValueError("CRS file bundle needs one receipt generation")
    return list(read_with_receipts([subjects], [receipts], POLICY, generation_id=generations[0]))


def _receipt(store, key: str, digest: str) -> dict:
    """Read one explicit archive receipt, never enumerate the raw corpus."""
    from spicy_docs.storage.raw_archive import iter_acquisition_receipts

    if key != f"acquisitions/sha256/{digest.removeprefix('sha256:')[:2]}/{digest.removeprefix('sha256:')}":
        raise ValueError("CRS archive receipt key differs from its digest")
    [(_, document)] = list(iter_acquisition_receipts(store.root, receipt_keys=[key], verify_objects=False))
    return document


def retained_file_pin(catalog_row: Mapping) -> dict:
    """Translate the catalog's one-based receipt ordinal to a zero-based file index.

    The exact receipt key/digest stays unchanged; this creates no HTTP facts.
    """
    facts = json.loads(catalog_row["data"]) if isinstance(catalog_row["data"], str) else catalog_row["data"]
    mapping, context = facts.get("mapping") or {}, facts.get("receipt_context") or {}
    reference = mapping.get("receipt_evidence_ref") or {}
    url = context.get("source_url") or facts.get("source_url") or facts.get("receipt_source_url")
    if (catalog_row.get("status") != "held"
            or not reference.get("manifest", "").startswith("acquisitions/sha256/") or not url):
        raise ValueError("CRS retained input requires a held occurrence with exact receipt and source URL")
    ordinal = reference.get("entry")
    if type(ordinal) is not int or ordinal < 1:
        raise ValueError("CRS catalog receipt entry must be a positive one-based ordinal")
    from spicy_docs.sources.congress.crs_files import crs_file_selection, crs_html_selection

    if url.endswith(".pdf"):
        crs_file_selection(url)
    else:
        crs_html_selection(url)
    return {"source_url": url, "resolved_url": None, "source_sha256": catalog_row["sha256"],
            "source_bytes": catalog_row["bytes"], "capture_id": facts.get("capture_id"),
            "captured_at": mapping.get("acquired_at"), "capture_receipt_key": reference["manifest"],
            "capture_receipt_sha256": reference["sha256"], "capture_receipt_entry": ordinal - 1,
            "provenance_basis": "retained-source-reconciliation",
            "original_receipts_json": json.dumps(context.get("original_receipts"), sort_keys=True)}


def _body(row: Mapping, store, max_bytes: int) -> bytes:
    """A recorded success cannot hide a missing or corrupt original or receipt."""
    size = row["source_bytes"]
    if type(size) is not int or not 0 < size <= max_bytes:
        raise ValueError("CRS retained body exceeds the declared bound")
    receipt = _receipt(store, row["capture_receipt_key"], row["capture_receipt_sha256"])
    entry = row.get("capture_receipt_entry")
    files = receipt["files"] if entry is None else [receipt["files"][entry]]
    if not any(file["sha256"].removeprefix("sha256:") == row["source_sha256"].removeprefix("sha256:")
               and file["bytes"] == size and (file.get("source_url") or
                   (file.get("receipt_context") or {}).get("source_url")) == row["source_url"] for file in files):
        raise ValueError("CRS original is absent from its pinned capture receipt")
    with store.open(row["source_sha256"]) as stream:
        body = stream.read(max_bytes + 1)
    if len(body) != size or _sha(body) != row["source_sha256"]:
        raise ValueError("CRS retained original differs from its digest/size")
    if row.get("association_receipt_key"):
        association = _receipt(store, row["association_receipt_key"], row["association_receipt_sha256"])["receipt"]
        if any(association.get(key) != row[key] for key in (
                "report_id", "metadata_update_date", "metadata_source_sha256", "rendition", "publisher_version",
                "source_url", "source_sha256", "capture_id", "captured_at")):
            raise ValueError("CRS file association differs from its exact receipt")
    return body


def _association(evidence, row: dict, *, kind: str) -> None:
    """Keep metadata association time separate from the original network capture."""
    fields = {key: row[key] for key in (
        "report_id", "metadata_update_date", "metadata_source_sha256", "rendition", "publisher_version",
        "version_binding", "source_url", "source_sha256", "capture_id", "captured_at",
        "capture_receipt_key", "capture_receipt_sha256",
    )}
    result = evidence.store.record_receipt(files=[{
        "sha256": row["source_sha256"], "bytes": row["source_bytes"], "family": "crs-reports",
        "dataset_id": "family:crs-reports", "native_id": row["report_id"],
        "source_version": row["publisher_version"], "source_url": row["source_url"],
        "role": "source-body",
    }], receipt={"kind": "crs-file-metadata-association", "association_kind": kind, **fields})
    row.update(association_kind=kind, association_receipt_key=result["object"],
               association_receipt_sha256="sha256:" + result["sha256"])
    evidence.event("crs-file-association", **fields, association_kind=kind,
                   association_receipt_key=result["object"], association_receipt_sha256=row["association_receipt_sha256"])


def _qualified_report(report: Mapping) -> tuple[CrsReportSelection, str]:
    from spicy_docs.sources.congress.crs_files import crs_report_selection

    if report.get("detail_state") != "ok" or not report.get("update_date"):
        raise ValueError("CRS file selection requires a qualified dated metadata revision")
    witnesses = json.loads(report.get("detail_witness_json") or "[]")
    if not witnesses or not witnesses[-1].get("sha256"):
        raise ValueError("CRS file selection requires its exact detail original witness")
    formats = report.get("formats", report.get("formats_json"))
    selection = crs_report_selection(json.loads(formats) if isinstance(formats, str) else formats)
    if selection.pdf.report_id != report.get("report_id") or str(selection.pdf.version) != str(report.get("version")):
        raise ValueError("CRS stated file differs from selected report/version")
    return selection, witnesses[-1]["sha256"]


def _check_metadata_original(evidence, report: Mapping, selection, digest: str) -> None:
    from spicy_docs.sources.congress.crs_files import crs_report_selection
    from spicy_docs.sources.congress.crs_summaries import read_report

    with evidence.store.open(digest) as stream:
        body = stream.read(2 * 1024 * 1024 + 1)
    if len(body) > 2 * 1024 * 1024 or _sha(body) != digest:
        raise ValueError("CRS metadata original differs from its digest/byte bound")
    native = read_report(body, report["report_id"], update_date=report["update_date"], version=report["version"])
    formats = native.get("formats")
    if not isinstance(formats, list):
        raise ValueError("CRS metadata original must state a formats array")
    if crs_report_selection(formats) != selection:
        raise ValueError("CRS selected file locators differ from the pinned metadata original")
    evidence.retain_bytes(body, stage="crs-file-metadata-input", raw_source=True,
                          dataset_id="family:crs-reports", native_id=report["report_id"],
                          source_version=str(report["version"]), metadata_update_date=report["update_date"])


def _recover_captures(output_dir: Path, *, max_bytes: int, max_rows: int) -> dict:
    """Read bounded owner journals once to reconcile captures before body-phase commit."""
    paths = sorted((output_dir / "source-evidence").glob("*/artifact/journal.jsonl"))
    if len(paths) > max_rows or sum(path.stat().st_size for path in paths) > max_bytes:
        raise ValueError("CRS recovery journals exceed the declared prior-state bound")
    matches = {}
    events = 0
    for path in paths:
        with path.open() as stream:
            for line in stream:
                events += 1
                if events > max_rows:
                    raise ValueError("CRS recovery events exceed the declared prior-state bound")
                event = json.loads(line)
                if (event.get("event") == "capture" and event.get("stage") == "crs-file"
                        and event.get("response_complete") is True and event.get("status_code") == 200):
                    key = (event["requested_url"], event["metadata_update_date"])
                    if key in matches and matches[key]["sha256"] != event["sha256"]:
                        raise ValueError("CRS interrupted capture recovery has conflicting bodies")
                    matches[key] = event
    return matches


def _capture_pin(evidence, capture: Mapping) -> dict:
    """Pin the real observation unchanged; a reference receipt is not another download."""
    original = evidence.store.record_receipt(files=[{
        "sha256": capture["sha256"], "bytes": capture["byte_size"], "family": "crs-reports",
        "dataset_id": "family:crs-reports", "native_id": capture.get("native_id"),
        "source_version": capture.get("source_version"), "source_url": capture["requested_url"],
        "acquired_at": capture["observed_at"], "response_complete": True,
    }], receipt={"kind": "crs-file-capture-reference", **capture})
    return {"source_url": capture["requested_url"], "body_state": "ok", "source_sha256": capture["sha256"],
            "source_bytes": capture["byte_size"], "capture_id": capture["capture_id"],
            "captured_at": capture["observed_at"], "resolved_url": capture["resolved_url"],
            "capture_receipt_key": original["object"], "capture_receipt_sha256": "sha256:" + original["sha256"],
            "capture_receipt_entry": 0, "provenance_basis": "network-capture"}


def _check_phase(row: Mapping, *, report: Mapping, digest: str, rendition: str, url: str) -> None:
    if any(row.get(key) != value for key, value in {
        "report_id": report["report_id"], "metadata_update_date": report["update_date"],
        "metadata_source_sha256": digest, "rendition": rendition, "source_url": url,
    }.items()):
        raise ValueError("CRS phase differs from the selected metadata/file identity")


def _validate_file(row: dict, body: bytes, locator: CrsFileSelection | CrsHtmlSelection, max_bytes: int) -> None:
    from spicy_docs.reading.pdf_bytes import check_pdf_bytes
    from spicy_docs.sources.congress.crs_files import CrsFileSelection, CrsFileSourceError, read_crs_html, read_crs_pdf

    final_url = row["resolved_url"]
    if final_url is not None:
        if isinstance(locator, CrsFileSelection):
            read_crs_pdf(body, locator, final_url=final_url, max_bytes=max_bytes)
            row["qualified_publisher_version"] = row["publisher_version"]
        else:
            read_crs_html(body, locator, final_url=final_url, max_bytes=max_bytes)
        row["file_validation_basis"] = "observed-final-url-and-native-shape"
    else:
        # Historical receipts support a source association, not an observed
        # redirect destination. PDF shape alone cannot qualify their edition.
        if isinstance(locator, CrsFileSelection):
            check_pdf_bytes(body, error_type=CrsFileSourceError, label="Retained CRS PDF")
        else:
            # Reuse the maintained native HTML identity reader against the
            # receipt's expected locator; this supplies no HTTP observation.
            read_crs_html(body, locator, final_url=row["source_url"], max_bytes=max_bytes)
        row["qualified_publisher_version"] = None
        row["file_validation_basis"] = "retained-source-association-and-native-shape"


def build_crs_report_files(
    output_dir: Path, reports: Sequence[Mapping], *, include_html: bool = False,
    prior_bundle: Path | None = None, retained_files: Mapping[str, Mapping] | None = None,
    transport=None, allow_network: bool = False, max_reports: int = 10, max_requests: int = 20,
    max_file_bytes: int = 8 * 1024 * 1024, max_total_bytes: int = 64 * 1024 * 1024,
    max_prior_bytes: int = 128 * 1024 * 1024, max_prior_rows: int = 10000,
    generation_id: str | None = None,
) -> dict:
    """Prepare a local candidate; explicit reports and offered URLs bound the work.

    ``retained_files`` pins existing archive bodies using this table's capture
    fields. ``prior_bundle`` carries file subjects/receipts, never metadata
    history. Persistent phase bundles beneath ``output_dir`` survive failed text
    extraction or generation admission. The caller owns that directory until
    candidate acceptance; central originals are never cleanup candidates.
    """
    from spicy_docs.sources.congress.crs_files import (
        CrsFileAcquirer, CrsFileBudget, CrsFileSelection, CrsFileSourceError, CrsFileUnavailableError,
        crs_file_locator, crs_html_locator,
    )
    from spicy_docs.transport.captured import attached_capture
    from spicy_docs.reading.refusals import RefusedResponse

    limits = (max_reports, max_requests, max_file_bytes, max_total_bytes, max_prior_bytes, max_prior_rows)
    if any(type(value) is not int or value <= 0 for value in limits):
        raise ValueError("CRS file bounds must be positive integers")
    if not reports or len(reports) > max_reports:
        raise ValueError("CRS report cohort exceeds its declared bound")
    selected = [(report, *_qualified_report(report)) for report in reports]
    coordinates = [(report["report_id"], report["update_date"]) for report, _, _ in selected]
    if len(set(coordinates)) != len(coordinates):
        raise ValueError("CRS report cohort repeats a metadata revision")
    if version("pypdf") != PYPDF_VERSION:
        raise ValueError("CRS text extraction requires the pinned pypdf backend")
    output_dir.mkdir(parents=True, exist_ok=True)
    phases = output_dir / ".crs-file-phases"
    phases.mkdir(exist_ok=True)
    rows = _read(prior_bundle, max_bytes=max_prior_bytes, max_rows=max_prior_rows) if prior_bundle else []
    generation_id = generation_id or uuid4().hex
    processor_pin = text_processor_pin()
    evidence = CaptureEvidence(output_dir, FAMILY)
    budget = CrsFileBudget(max_requests=1, max_bytes=max_file_bytes, timeout_seconds=60,
                           min_request_interval_seconds=0)
    requests = decoded_bytes = downloaded_bytes = 0
    reused_bodies = reused_text = 0
    recovered_captures = None
    try:
        with CrsFileAcquirer(budget=budget, transport=transport) as acquirer:
            for report, selection, metadata_digest in selected:
                _check_metadata_original(evidence, report, selection, metadata_digest)
                renditions: list[tuple[str, CrsFileSelection | CrsHtmlSelection, str]] = [
                    ("pdf", selection.pdf, crs_file_locator(selection.pdf))]
                if include_html and selection.html is not None:
                    renditions.append(("html", selection.html, crs_html_locator(selection.html)))
                for rendition, locator, url in renditions:
                    identity = [report["report_id"], report["update_date"], rendition, url, metadata_digest]
                    phase_key = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
                    body_phase = phases / (phase_key + "-body")
                    text_phase = phases / (phase_key + "-" + processor_pin.removeprefix("sha256:"))
                    row = None
                    if body_phase.exists():
                        [row] = _read(body_phase, max_bytes=max_prior_bytes, max_rows=1)
                        _check_phase(row, report=report, digest=metadata_digest, rendition=rendition, url=url)
                    if row is None:
                        matches = [held for held in rows if held["source_url"] == url and held["body_state"] == "ok"
                                   and (rendition == "pdf" or held["metadata_update_date"] == report["update_date"])]
                        digests = {held["source_sha256"] for held in matches}
                        if len(digests) > 1:
                            raise ValueError("CRS same-locator body reuse is ambiguous")
                        held = matches[-1] if matches else (retained_files or {}).get(url)
                        if not held:
                            if recovered_captures is None:
                                recovered_captures = _recover_captures(output_dir,
                                    max_bytes=max_prior_bytes, max_rows=max_prior_rows)
                            recovered = recovered_captures.get((url, report["update_date"]))
                            held = _capture_pin(evidence, recovered) if recovered else None
                        row = dict.fromkeys(SCHEMA.names)
                        row.update(report_id=report["report_id"], metadata_update_date=report["update_date"],
                                   metadata_source_sha256=metadata_digest, rendition=rendition,
                                   source_family=locator.family, source_url=url,
                                   publisher_version=str(locator.version) if isinstance(locator, CrsFileSelection) else None,
                                   qualified_publisher_version=str(locator.version) if isinstance(locator, CrsFileSelection) else None,
                                   version_binding="numbered-pdf-locator" if rendition == "pdf" else "unbound-current-observation",
                                   text_state="not_requested", provider_version=version("spicy-docs"))
                        if held:
                            if held.get("source_url") != url:
                                raise ValueError("CRS retained pin differs from the stated source URL")
                            body = _body(held, evidence.store, max_file_bytes)
                            for name in ("source_sha256", "source_bytes", "capture_id", "captured_at", "resolved_url",
                                         "capture_receipt_key", "capture_receipt_sha256", "capture_receipt_entry",
                                         "provenance_basis", "original_receipts_json"):
                                row[name] = held.get(name)
                            row["body_state"] = "ok"
                            reused_bodies += 1
                            same_metadata = all(held.get(key) == row[key] for key in (
                                "report_id", "metadata_update_date", "metadata_source_sha256", "rendition", "source_url"))
                            if same_metadata:
                                row.update({key: held[key] for key in (
                                    "association_kind", "association_receipt_key", "association_receipt_sha256")})
                            association_kind = None if same_metadata else "retained-original-reuse"
                        else:
                            if not allow_network:
                                raise ValueError("CRS original is not retained; explicit bounded network acquisition is required")
                            if requests >= max_requests or decoded_bytes + max_file_bytes > max_total_bytes:
                                raise ValueError("CRS whole-run request/decoded-byte budget exhausted")
                            requests += 1
                            try:
                                if isinstance(locator, CrsFileSelection):
                                    acquired = acquirer.acquire_report(selection, version=locator.version)
                                else:
                                    acquired = acquirer.acquire_report_html(locator)
                                if acquired.selection != locator:
                                    raise ValueError("CRS provider did not return the selected file")
                            except CrsFileSourceError as error:
                                evidence.refusal(error, stage="crs-file")
                                capture = attached_capture(error)
                                refused = getattr(error, "refused_response", None)
                                refused_body = (capture.body if capture is not None else refused.response_bytes
                                                if isinstance(refused, RefusedResponse) else None)
                                refused_bytes = len(refused_body) if refused_body is not None else 0
                                decoded_bytes += refused_bytes
                                downloaded_bytes += refused_bytes
                                row.update(body_state="unavailable" if isinstance(error, CrsFileUnavailableError) else "refused",
                                           qualified_publisher_version=None, error=str(error))
                                # Refusals remain explicit subjects; no original digest is invented.
                                refusal = evidence.store.record_receipt(files=[], receipt={
                                    "kind": "crs-file-refusal", "selection": identity, "error": str(error),
                                    "body_state": row["body_state"]})
                                row.update(association_receipt_key=refusal["object"],
                                           association_receipt_sha256="sha256:" + refusal["sha256"])
                                _replace_association(rows, row, max_rows=max_prior_rows)
                                continue
                            capture = acquired.capture
                            body = capture.body
                            downloaded_bytes += len(body)
                            retained = evidence.capture(capture, stage="crs-file", dataset_id="family:crs-reports",
                                native_id=row["report_id"], source_version=row["publisher_version"],
                                metadata_update_date=report["update_date"])
                            row.update(_capture_pin(evidence, retained))
                            association_kind = "captured-original"
                        _validate_file(row, body, locator, max_file_bytes)
                        if association_kind is not None:
                            _association(evidence, row, kind=association_kind)
                        _write([row], body_phase, generation_id)
                    else:
                        body = _body(row, evidence.store, max_file_bytes)
                        _validate_file(row, body, locator, max_file_bytes)
                        reused_bodies += 1
                    decoded_bytes += len(body)
                    if decoded_bytes > max_total_bytes:
                        raise ValueError("CRS whole-run decoded-byte budget exhausted")
                    if text_phase.exists():
                        [completed] = _read(text_phase, max_bytes=max_prior_bytes, max_rows=1)
                        _check_phase(completed, report=report, digest=metadata_digest, rendition=rendition, url=url)
                        if completed["association_receipt_sha256"] != row["association_receipt_sha256"]:
                            raise ValueError("CRS text completion differs from its file association")
                        if completed["text_state"] not in {"ok", "empty", "identity_mismatch"}:
                            raise ValueError("CRS text phase does not establish a completed extraction")
                        if completed["text_input_sha256"] != row["source_sha256"] or completed["text_processor_sha256"] != processor_pin:
                            raise ValueError("CRS text completion differs from its input/processor")
                        if completed["text_content"] is not None and _sha(completed["text_content"].encode()) != completed["text_sha256"]:
                            raise ValueError("CRS completed text differs from its pinned digest")
                        row = completed
                        reused_text += 1
                    else:
                        completed = next((held for held in rows if held["source_sha256"] == row["source_sha256"]
                                          and held["rendition"] == rendition
                                          and held["text_processor_sha256"] == processor_pin
                                          and held["text_state"] in {"ok", "empty"}), None)
                        if completed:
                            row.update({key: completed[key] for key in ("text_content", "text_state", "page_count", "error")})
                            reused_text += 1
                        else:
                            row.update(extract_text(body, rendition))
                        _check_content_identity(row)
                        row.update(text_input_sha256=row["source_sha256"], text_processor=TEXT_PROCESSOR,
                                   text_processor_sha256=processor_pin,
                                   text_sha256=_sha(row["text_content"].encode()) if row["text_content"] is not None else None)
                        if row["text_state"] in {"ok", "empty", "identity_mismatch"}:
                            _write([row], text_phase, generation_id)
                    # Replace only the exact association; distinct numbered files
                    # and metadata observations remain independently queryable.
                    _replace_association(rows, row, max_rows=max_prior_rows)
        evidence.finish()
    except BaseException as error:
        evidence.finish(error)
        raise
    bundle = output_dir / ("bundle-" + generation_id)
    subject = _write(rows, bundle, generation_id)
    generation = output_dir / ("generation-" + generation_id)
    artifact = build_generation(generation, family=FAMILY, files=[subject], expected_keys=[KEY],
        inputs=evidence.inputs(), receipt_path=bundle / "etl_receipts.parquet", receipt_policies=[POLICY],
        receipt_generation_id=generation_id, publication_status="local-partial")
    verify_generation(generation, expected_pin=artifact.pin)
    return {"bundle": str(bundle), "generation_directory": str(generation), "candidate_pin": artifact.pin.as_dict(),
            "rows": len(rows), "requests": requests, "downloaded_bytes": downloaded_bytes,
            "decoded_body_bytes": decoded_bytes,
            "reused_bodies": reused_bodies, "reused_text": reused_text,
            "evidence_directory": str(evidence.artifact_dir), "status": "verified_local_candidate_not_published"}


def main() -> None:
    """Read an explicit qualified selection and prepare a local, bounded candidate."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True,
                        help="JSON with reports and optional retained_files capture pins")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prior-bundle", type=Path)
    parser.add_argument("--include-html", action="store_true")
    parser.add_argument("--allow-network", action="store_true")
    for name, default in (("max-reports", 10), ("max-requests", 20), ("max-file-bytes", 8 * 1024 * 1024),
                          ("max-total-bytes", 64 * 1024 * 1024), ("max-prior-bytes", 128 * 1024 * 1024),
                          ("max-prior-rows", 10000)):
        parser.add_argument("--" + name, type=int, default=default)
    args = parser.parse_args()
    if args.selection.stat().st_size > 2 * 1024 * 1024:
        parser.error("selection exceeds 2 MiB")
    selection = json.loads(args.selection.read_bytes())
    kwargs = vars(args)
    kwargs.pop("selection")
    output = kwargs.pop("output")
    result = build_crs_report_files(output, selection["reports"], retained_files=selection.get("retained_files"), **kwargs)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
