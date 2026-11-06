"""Register retained disbursement originals in the shared raw-file inventory."""

from __future__ import annotations

from datetime import date
from urllib.parse import urlsplit


def record_original(store, source: dict, *, selection: dict, origin: dict | None = None) -> dict:
    """Add an immutable dataset association without changing original bytes.

    ``selection`` pins the caller-verified source selection. An optional ``origin``
    is an already verified archive file record, so its acquisition facts survive
    a metadata backfill. Without it, acquisition time and HTTP status stay unknown.
    The original URL identifies the full report or part; the content hash names
    this retained version. Neither a successful capture nor this receipt claims
    that parsing, OCR or complete historical coverage has succeeded.
    """
    from spicy_docs.sources.disbursements_reports import disbursements_report_locator
    from spicy_docs.sources.house_disbursements import house_disbursements_csv_url

    chamber, kind, file_format = source["chamber"], source["kind"], source["format"]
    if chamber not in {"house", "senate"}:
        raise ValueError("Expected a House or Senate original")
    if file_format == "pdf":
        locator = disbursements_report_locator(source["url"])
        if kind not in {"full", "part"} or locator.chamber not in {None, chamber}:
            raise ValueError("Report identity conflicts with its source locator")
    elif file_format == "csv" and chamber == "house" and kind in {"detail", "summary"}:
        house_disbursements_csv_url(source["url"])
    else:
        raise ValueError("Expected a disbursement CSV or PDF original")
    start, end = source.get("period_start"), source.get("period_end")
    if start:
        date.fromisoformat(start)
    if end:
        date.fromisoformat(end)
    if start and end and end < start:
        raise ValueError("Report period is reversed")
    if source.get("status") not in {"captured", "retained", "captured-unparsed"}:
        raise ValueError("An unavailable source cannot become a retained original")
    sha, size = source["source_sha256"], source["source_byte_size"]
    original = dict(origin or {})
    if origin is not None and (
        str(original.get("sha256", "")).removeprefix("sha256:") != sha.removeprefix("sha256:")
        or original.get("bytes") != size
        or source["url"]
        not in {original.get("source_url"), original.get("requested_url"), original.get("receipt_source_url")}
    ):
        raise ValueError("Original acquisition facts do not match this source")
    family = "house-disbursements" if chamber == "house" else "senate-expenditures"
    original.update(
        sha256=sha,
        bytes=size,
        dataset_id="family:" + family,
        family=family,
        native_id=source["url"],
        source_version=sha,
        source_url=original.get("source_url") or source["url"],
        name=urlsplit(source["url"]).path.rsplit("/", 1)[-1],
        role="source-body",
    )
    if start and end:
        original["covered_period"] = {
            "kind": "report-period",
            "label": start + "/" + end,
            "basis": "Pinned source selection and its retained publisher listing or report-period evidence",
        }
    return store.record_receipt(
        files=[original],
        receipt={
            "event": "source-inventory",
            "mapping_rule": "congressional-disbursement-original/1",
            "selection": selection,
            "source_id": source["id"],
            "source_format": file_format,
            "report_kind": kind,
            "part_number": source.get("part_number"),
            "package_id": source.get("package_id"),
            "period_start": start,
            "period_end": end,
            "source_index": source.get("source_index"),
            "period_evidence": source.get("period_evidence"),
            "acquisition_evidence_directory": source.get("evidence_directory"),
            "acquisition_outcome": source["status"],
            "limits": "Original-file custody and dataset association only; parsing, OCR, mirror availability and source completeness are separate.",
        },
    )
