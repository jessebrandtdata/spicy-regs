"""Build-time interpretation of held FEC context; no source acquisition or SQL."""

from datetime import datetime, timezone
import re

import pyarrow as pa

QUERY_SHAPE_VERSION = "fec-query-shape-v1"
API_CONTROL_FIELDS = (
    [
        (n, pa.string())
        for n in ("response_field", "response_field_role", "value_status", "api_version", "parsing_status")
    ]
    + [(n, pa.int64()) for n in ("page", "per_page", "reported_count", "reported_pages", "observed_count")]
    + [("is_count_exact", pa.bool_())]
)


def nonnegative_integer(value):
    """Preserve exact integer meaning and reject Python's bool-is-int coercion."""
    return value if type(value) is int and 0 <= value <= 9223372036854775807 else None


def api_control_values(native):
    field, value = native["field"], native.get("value")
    row = dict.fromkeys(n for n, _ in API_CONTROL_FIELDS)
    row.update(
        response_field=field,
        response_field_role="result-container" if field == "results" else "response-control",
        value_status="source_null" if value is None else "source_empty" if value in ("", []) else "reported",
        parsing_status="unsupported_field",
    )
    if field == "api_version":
        row["api_version"] = value if isinstance(value, str) else None
        row["parsing_status"] = "parsed" if isinstance(value, str) else "unsupported_value"
    elif field == "results":
        row["observed_count"] = len(value) if isinstance(value, list) else None
        row["parsing_status"] = "parsed" if isinstance(value, list) else "unsupported_value"
    elif field == "pagination":
        if isinstance(value, dict):
            for name, source in (
                ("page", "page"),
                ("per_page", "per_page"),
                ("reported_count", "count"),
                ("reported_pages", "pages"),
            ):
                row[name] = nonnegative_integer(value.get(source))
            row["is_count_exact"] = value.get("is_count_exact") if type(value.get("is_count_exact")) is bool else None
        row["parsing_status"] = (
            "parsed"
            if all(
                row[n] is not None for n in ("page", "per_page", "reported_count", "reported_pages", "is_count_exact")
            )
            else "invalid_or_missing_pagination"
        )
    return row


def exact_date(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{2}/[0-9]{2}/[0-9]{4}", value):
        return None
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError:
        return None


def feed_timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Z][a-z]{2}, [0-9]{2} [A-Z][a-z]{2} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT", value
    ):
        return None
    try:
        return datetime.strptime(value, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def filing_announcement(description, published):
    fields = {
        "reported_filer_id": "CommitteeId",
        "filing_id": "FilingId",
        "form_type": "FormType",
        "coverage_start_raw": "CoverageFrom",
        "coverage_end_raw": "CoverageThrough",
        "report_type": "ReportType",
    }
    row, repeated = {}, False
    for name, label in fields.items():
        values = re.findall(r"(?:^|[|*])\s*" + label + r":\s*([^|*]*)", description or "")
        repeated |= len(values) > 1
        row[name] = values[0].strip() or None if len(values) == 1 else None
    filer = row["reported_filer_id"] or ""
    row["committee_id"] = filer if re.fullmatch(r"C[0-9]{8}", filer) else None
    row["candidate_id"] = filer if re.fullmatch(r"(?:[HS][0-9][A-Z]{2}[0-9]{5}|P[0-9]{8})", filer) else None
    row["source_label_status"] = (
        "committee_id"
        if row["committee_id"]
        else "candidate_id_under_committee_label"
        if row["candidate_id"]
        else "missing"
        if not filer
        else "unsupported_identifier"
    )
    row["coverage_start_date"] = exact_date(row["coverage_start_raw"])
    row["coverage_end_date"] = exact_date(row["coverage_end_raw"])
    row["published_at"] = feed_timestamp(published)
    row["filing_link_status"] = "source_assertion_not_qualified_filing"
    row["parsing_status"] = (
        "repeated_labels"
        if repeated
        else "missing_identifiers"
        if not filer or not row["filing_id"]
        else "invalid_identifiers"
        if not (row["committee_id"] or row["candidate_id"]) or not re.fullmatch(r"[0-9]+", row["filing_id"])
        else "invalid_date"
        if any(
            row[raw] is not None and row[typed] is None
            for raw, typed in (("coverage_start_raw", "coverage_start_date"), ("coverage_end_raw", "coverage_end_date"))
        )
        or published is not None
        and row["published_at"] is None
        else "parsed"
    )
    return row


def source_page_body(title, text, url):
    """Known held-text boundaries only; unsupported captures remain diagnostics.

    A whole error or interstitial title is a failed page, read by SpicyDocs' one shared classifier:
    the four whole titles this check always held, plus whole interstitial titles such as Cloudflare's.
    """
    from spicy_docs.reading.error_pages import error_page_title

    if error_page_title(title or "") is not None:
        return None, "failed_page"
    if not text or not text.strip():
        return None, "empty_page"
    parts = text.split("Federal Election Commission | United States of America")
    footer = "About\nCareers\nPress\nContact\nPrivacy and security policy"
    if len(parts) == 3 and footer in parts[2]:
        return parts[2].split(footer, 1)[0].strip() or None, "body_extracted"
    if (url or "").startswith("https://docquery.fec.gov/") and title in {"FEC FORM 3", "FEC FORM 99"}:
        marker = "\n" + title + "\n"
        if marker in text and "\nGenerated " in text and "Federal Election Commission (800) 424-9530" in text:
            return title + "\n" + text.split(marker, 1)[1].split("\nGenerated ", 1)[0], "body_extracted"
    return None, "unsupported_body_boundaries"

