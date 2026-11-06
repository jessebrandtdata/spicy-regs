"""Explicit domain shapes for government-source datasets.

The source shapers and incremental mergers use the literal source forms. This
module converts their final rows to queryable native values. Original conversion
inputs and acquisition/processing fields belong to the shared ETL receipts.
No network access or publication occurs here.
"""

from __future__ import annotations

import hashlib
import json
import re
from urllib.parse import urlsplit
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Mapping

import pyarrow as pa

from spicy_regs.gao_citation_keys import citation_product_id

RECEIPT_FIELDS = {
    "crs_reports": ("url",),
    "fcc_filings": ("filing_url", "native_fields_sha256", "pdf_extraction_results_json"),
    "gao_decisions": (),
    # How a letter value and a product-page value were read (rule, bytes, spans, each blank's reason) is processing:
    # the values are the subject columns, and the readings stay with the row's receipt.
    "gao_reports": ("major_rule_letter_json", "product_page_json"),
    "lobbying_filings": ("url",),
    "lobbying_registrants": ("url",),
    "lobbying_clients": ("url",),
    "usaspending_recipients": ("observed_at", "source_capture_sha256"),
}

INTEGER_FIELDS = {
    "crs_reports": ("version",),
    "fcc_filings": ("total_page_count",),
    "lobbying_filings": ("filing_year",),
    "lobbying_activities": ("activity_index",),
    "lobbying_activity_lobbyists": ("activity_index", "lobbyist_index"),
    "lobbying_foreign_entities": ("foreign_entity_index",),
    "lobbying_affiliated_organizations": ("affiliated_organization_index",),
    "lobbying_conviction_disclosures": ("conviction_disclosure_index",),
}

BOOLEAN_FIELDS = {
    "fcc_filings": ("express_comment",),
    "gao_recommendations": ("priority", "listed_open"),
    "lobbying_activity_lobbyists": ("new",),
    "lobbying_filings": ("registrant_different_address",),
    "lobbying_clients": ("client_government_entity", "client_self_select"),
}

MONEY_FIELDS = {
    "lobbying_filings": ("income", "expenses"),
    # ownership_percentage is a percent, not money; it shares the exact DECIMAL(38,6) reading (every retained value is
    # a two-place decimal or null, 2026-10-31 scan of all 1,965,568 filings).
    "lobbying_foreign_entities": ("contribution", "ownership_percentage"),
    "usaspending_recipients": ("total_award_amount",),
}

DATE_FIELDS = {
    "gao_reports": ("published_date",),
    "gao_decisions": ("decision_date",),
    "lobbying_filings": ("termination_date",),
    "lobbying_clients": ("effective_date",),
    "lobbying_conviction_disclosures": ("date",),
    # first_seen and last_seen say when the open-recommendations export first and last listed the row: with
    # listed_open, they are how a reader dates a recommendation's closure (owner decision, 2026-10-05).
    "gao_recommendations": ("publication_date", "first_seen", "last_seen"),
    "sam_entities": ("registration_date", "registration_expiration_date"),
}

TIMESTAMP_FIELDS = {
    "crs_reports": ("published_date", "update_date"),
    "lobbying_filings": ("dt_posted",),
    "lobbying_registrants": ("dt_updated", "captured_at"),
    "lobbying_clients": ("captured_at",),
}

IDENTITY_FIELDS = {
    "crs_reports": ("report_id", "update_date"),
    "fcc_filings": ("id_submission",),
    "fcc_proceedings": ("name",),
    "gao_reports": ("report_id",),
    "gao_decisions": ("decision_number", "url"),
    "gao_recommendations": ("recommendation_id",),
    "lobbying_filings": ("filing_uuid",),
    "lobbying_activities": ("filing_uuid", "activity_index"),
    "lobbying_activity_lobbyists": ("filing_uuid", "activity_index", "lobbyist_index"),
    "lobbying_foreign_entities": ("filing_uuid", "foreign_entity_index"),
    "lobbying_affiliated_organizations": ("filing_uuid", "affiliated_organization_index"),
    "lobbying_conviction_disclosures": ("filing_uuid", "conviction_disclosure_index"),
    "lobbying_registrants": ("registrant_id",),
    "lobbying_clients": ("client_id",),
    "sam_entities": ("uei", "entity_eft_indicator"),
    "usaspending_recipients": ("recipient_id",),
}

LEGACY_COLUMNS = {
    "crs_reports": ("report_id", "title", "report_type", "status", "published_date", "update_date", "version", "url"),
    "fcc_filings": (
        "id_submission",
        "proceeding_names_json",
        "submission_type",
        "express_comment",
        "date_received",
        "date_submission",
        "date_disseminated",
        "filing_status",
        "viewing_status",
        "exparte_or_late_filed",
        "filers_json",
        "authors_json",
        "lawfirms_json",
        "bureaus_json",
        "text_data",
        "total_page_count",
        "documents_json",
        "filing_url",
        "native_fields_json",
        "native_fields_sha256",
        "pdf_extraction_results_json",
    ),
    "fcc_proceedings": (
        "name",
        "id_proceeding",
        "description",
        "bureau_code",
        "bureau_name",
        "rulemaking_or_docket",
        "filing_status",
        "date_created",
        "date_closed",
        "filed_by",
        "date_nprm",
        "date_reporting_and_order",
        "rule_section",
    ),
    "gao_decisions": (
        "decision_number",
        "b_numbers_json",
        "decision_type",
        "title",
        "decision_date",
        "topics_json",
        "url",
        "listing_page",
        "source",
    ),
    "gao_recommendations": (
        "recommendation_id",
        "report_id",
        "publication_number",
        "publication_title",
        "publication_date",
        "director_name",
        "agency",
        "recommendation",
        "recommendation_kind",
        "recommendation_number",
        "status",
        "priority",
        "comments",
        "topics",
        "first_seen",
        "last_seen",
        "listed_open",
        "status_as_of",
    ),
    "gao_reports": (
        "report_id",
        "title",
        "report_type",
        "published_date",
        "abstract",
        "agencies_json",
        "topics_json",
        "url",
        "source",
        "product_type",
        "report_number",
        # From the CetiAlphaFive/gao R package (2026-09-29), for the reports it lists; a GAO product-page read fills
        # the two counts, the page count and the subject terms where no route states them. NULL where none does.
        "requester_type",
        "requester_committees_json",
        "requester_members_json",
        "recommendation_count",
        "matters_for_congress_count",
        "page_count",
        "subject_terms_json",
        # What a Federal Agency Major Rule Report's letter states of the rule it is on (owner decision 2026-10-04):
        # the agency clause as printed, every RIN and every Federal Register citation. The two lists are not pairs.
        "major_rule_agency",
        "major_rule_rins_json",
        "major_rule_fr_citations_json",
        "major_rule_letter_json",
        "product_page_json",
    ),
    "lobbying_activities": (
        "filing_uuid",
        "activity_index",
        "general_issue_code",
        "general_issue_code_display",
        "description",
        "foreign_entity_issues",
        "government_entities_json",
    ),
    "lobbying_activity_lobbyists": (
        "filing_uuid",
        "activity_index",
        "lobbyist_index",
        "lobbyist_id",
        "prefix",
        "first_name",
        "nickname",
        "middle_name",
        "last_name",
        "suffix",
        "covered_position",
        "new",
    ),
    # The filing's other source lists (owner round 8 Q1, 2026-10-31), one row per entry at its position in the
    # filing's own list, under the source's own field names.
    "lobbying_foreign_entities": (
        "filing_uuid",
        "foreign_entity_index",
        "name",
        "contribution",
        "ownership_percentage",
        "address",
        "city",
        "state",
        "state_display",
        "country",
        "country_display",
        "ppb_city",
        "ppb_state",
        "ppb_state_display",
        "ppb_country",
        "ppb_country_display",
    ),
    "lobbying_affiliated_organizations": (
        "filing_uuid",
        "affiliated_organization_index",
        "name",
        "url",
        "address_1",
        "address_2",
        "address_3",
        "address_4",
        "city",
        "state",
        "state_display",
        "zip",
        "country",
        "country_display",
        "ppb_city",
        "ppb_state",
        "ppb_state_display",
        "ppb_country",
        "ppb_country_display",
    ),
    # The disclosed lobbyist is named as on lobbying_activity_lobbyists.
    "lobbying_conviction_disclosures": (
        "filing_uuid",
        "conviction_disclosure_index",
        "lobbyist_id",
        "prefix",
        "first_name",
        "nickname",
        "middle_name",
        "last_name",
        "suffix",
        "date",
        "description",
    ),
    "lobbying_filings": (
        "filing_uuid",
        "filing_type",
        "filing_year",
        "filing_period",
        "dt_posted",
        "registrant_name",
        "registrant_id",
        "client_name",
        "client_id",
        "income",
        "expenses",
        "lobbying_activities_json",
        "government_entities_json",
        "url",
        # government-sources/2: the filing's own fields the first mapper dropped, under their source names. The
        # registrant address is as filed on this filing; registrant_country is the publisher's display name here.
        "filing_type_display",
        "filing_period_display",
        "filing_document_content_type",
        "expenses_method",
        "expenses_method_display",
        "termination_date",
        "posted_by_name",
        "registrant_address_1",
        "registrant_address_2",
        "registrant_city",
        "registrant_state",
        "registrant_zip",
        "registrant_country",
        "registrant_ppb_country",
        "registrant_different_address",
    ),
    # The registrant and client records LDA embeds in each filing, one row per LDA id as last captured
    # (owner decision Q12, 2026-10-30). The publisher serves its current record, not the one filed.
    "lobbying_registrants": (
        "registrant_id",
        "house_registrant_id",
        "name",
        "description",
        "address_1",
        "address_2",
        "address_3",
        "address_4",
        "city",
        "state",
        "state_display",
        "zip",
        "country",
        "country_display",
        "ppb_country",
        "ppb_country_display",
        "contact_telephone",
        "dt_updated",
        "url",
        "captured_at",
        "captured_filing_uuid",
    ),
    "lobbying_clients": (
        "client_id",
        "registrant_client_number",
        "registrant_id",
        "name",
        "general_description",
        "client_government_entity",
        "client_self_select",
        "state",
        "state_display",
        "country",
        "country_display",
        "ppb_state",
        "ppb_state_display",
        "ppb_country",
        "ppb_country_display",
        "effective_date",
        "url",
        "captured_at",
        "captured_filing_uuid",
    ),
    "sam_entities": (
        "uei",
        "entity_eft_indicator",
        "cage_code",
        "legal_business_name",
        "dba_name",
        "entity_structure_desc",
        "entity_type_desc",
        "profit_structure_desc",
        "state",
        "city",
        "zip_code",
        "congressional_district",
        "primary_naics",
        "registration_status",
        "registration_date",
        "registration_expiration_date",
        "exclusion_status_flag",
        "purpose_of_registration_desc",
        "entity_url",
    ),
    "usaspending_recipients": (
        "recipient_id",
        "uei",
        "duns",
        "name",
        "recipient_level",
        "total_award_amount",
        "observed_at",
        "source_capture_sha256",
    ),
}

# Additional held GAO editions rename the release day and retain the actual
# decision day and the publisher's status. Accept both release-day spellings;
# the original spelling remains a receipt conversion input.
LEGACY_COLUMNS["gao_decisions"] += (
    "released_date",
    "decided_date",
    "decision_status",
    "outcome",
    "outcome_rule",
    "b_numbers_truncated",
)
RECEIPT_FIELDS["gao_decisions"] += ("outcome_rule",)
# Whether GAO's listing cut the row's number list is a fact about the decision as held, which a reader needs
# beside b_numbers; it is not how the row was processed (owner decision, 2026-10-04).
BOOLEAN_FIELDS["gao_decisions"] = ("b_numbers_truncated",)
DATE_FIELDS["gao_decisions"] = ("released_date", "decided_date")

# FCC native observations carry more than the older name-only arrays. IDs remain
# text: ECFS serves both strings and integers, and identifiers are not measures.
FCC_ARRAYS = {
    "proceedings": (
        ("name", "name"),
        ("id_proceeding", "id_proceeding"),
        ("bureau_code", "bureau_code"),
        ("bureau_name", "bureau_name"),
        ("created_date", "created_date"),
        ("date_closed", "date_closed"),
        ("description", "description"),
        ("description_display", "description_display"),
        ("sunshine_start_date", "sunshine_start_date"),
        ("sunshine_end_date", "sunshine_end_date"),
        ("filing_status", "filingStatus"),
    ),
    "filers": (("name", "name"),),
    "authors": (("name", "name"),),
    "lawfirms": (("name", "name"),),
    "bureaus": (("name", "name"), ("code", "code"), ("edocs_bureau_code", "edocs_bureau_code")),
    "documents": (("filename", "filename"), ("description", "description"), ("src", "src")),
}
FCC_LEGACY_ARRAYS = {
    "proceeding_names_json": "proceedings",
    "filers_json": "filers",
    "authors_json": "authors",
    "lawfirms_json": "lawfirms",
    "bureaus_json": "bureaus",
    "documents_json": "documents",
}
FCC_EXTRACTION_TYPE = pa.list_(pa.struct([
    ("attempt_ordinal", pa.int64()), ("url", pa.string()),
    ("offered_ordinals", pa.list_(pa.int64())), ("url_status", pa.string()),
    ("source_sha256", pa.string()), ("digest_status", pa.string()),
    ("status", pa.string()), ("page_count", pa.int64()), ("error", pa.string()),
    ("capture_status", pa.string()), ("text_access_status", pa.string()),
]))
STRUCT_ARRAYS = {
    "government_entities_json": ("id", "name"),
    "lobbying_activities_json": ("general_issue_code", "general_issue_code_display", "description"),
}


class GovernmentShapeError(ValueError):
    """A source value cannot be represented without losing its meaning."""


def _list_type(fields: tuple[str, ...]) -> pa.DataType:
    return pa.list_(pa.struct([(field, pa.string()) for field in fields]))


def subject_schema(dataset: str) -> pa.Schema:
    """The family-owned main fields, including selected FCC result observations."""
    fields = []
    for name in LEGACY_COLUMNS[dataset]:
        if dataset == "gao_decisions" and name == "decision_date":
            continue
        if name in RECEIPT_FIELDS.get(dataset, ()) or name == "native_fields_json":
            continue
        target, dtype = name, pa.string()
        if dataset == "fcc_filings" and name in FCC_LEGACY_ARRAYS:
            target = FCC_LEGACY_ARRAYS[name]
            dtype = _list_type(tuple(field for field, _ in FCC_ARRAYS[target]))
        elif name in STRUCT_ARRAYS:
            target, dtype = name.removesuffix("_json"), _list_type(STRUCT_ARRAYS[name])
        elif name.endswith("_json"):
            target, dtype = name.removesuffix("_json"), pa.list_(pa.string())
        elif name in INTEGER_FIELDS.get(dataset, ()) or name in (
            "recommendation_count",
            "matters_for_congress_count",
            "page_count",
        ):
            dtype = pa.int64()
        elif name in BOOLEAN_FIELDS.get(dataset, ()):
            dtype = pa.bool_()
        elif name in MONEY_FIELDS.get(dataset, ()):
            dtype = pa.decimal128(38, 6)
        elif name in DATE_FIELDS.get(dataset, ()):
            dtype = pa.date32()
        elif name in TIMESTAMP_FIELDS.get(dataset, ()):
            dtype = pa.timestamp("us", tz="UTC")
        fields.append((target, dtype))
    if dataset == "fcc_filings":
        fields.extend([("generation_id", pa.string()), ("extraction_results", FCC_EXTRACTION_TYPE)])
    if dataset == "gao_reports":
        fields.append(("citation_product_id", pa.string()))
    schema = pa.schema(fields, metadata={b"government_etl_policy": b"1"})
    if dataset == "crs_reports":
        at = schema.get_field_index("update_date")
        schema = schema.set(at, schema.field(at).with_nullable(False))
    return schema


SUBJECT_SCHEMAS = {dataset: subject_schema(dataset) for dataset in LEGACY_COLUMNS}

#: Text columns published with edge whitespace stripped and a blank (empty or whitespace-only) value as NULL. The
#: receipt's ``raw_record`` keeps the value as written. fcc_proceedings ``rule_section`` (owner, 2026-10-31): FCC staff
#: type it freely, so ECFS writes a single space for "none" (643 of 21,705 dockets, walk 2026-10-08) and pads some
#: values (`` Part 76``); ``government_bulk`` states the same rule in SQL.
STRIPPED_TEXT = {"fcc_proceedings": frozenset({"rule_section"})}
#: The whitespace stripped: ASCII only, the set DuckDB's ``trim`` is given, so both writers agree on every value.
EDGE_WHITESPACE = " \t\n\v\f\r"


def _text(value: Any, *, identifier: bool = False) -> str | None:
    if value is None or isinstance(value, str):
        return value
    if identifier and type(value) is int:
        return str(value)
    raise GovernmentShapeError(f"Expected text, got {type(value).__name__}")


def _integer(value: Any) -> int | None:
    if value is None:
        return None
    if type(value) is int:
        number = value
    elif isinstance(value, str) and re.fullmatch(r"-?[0-9]+", value):
        number = int(value)
    else:
        raise GovernmentShapeError("Expected a literal whole number")
    if not -(2**63) <= number < 2**63:
        raise GovernmentShapeError("Whole number exceeds BIGINT")
    return number


def _boolean(value: Any) -> bool | None:
    if value is None or type(value) is bool:
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value in ("true", "True", "1", "false", "False", "0"):
        return value in ("true", "True", "1")
    raise GovernmentShapeError("Unsupported source boolean spelling")


def _money(value: Any) -> Decimal | None:
    if value is None:
        return None
    if not isinstance(value, str | Decimal | int) or isinstance(value, bool):
        raise GovernmentShapeError("Money requires an exact decimal input, never a binary float")
    try:
        amount = Decimal(value)
        with localcontext() as context:
            context.prec = 50
            scaled = amount.quantize(Decimal("0.000001"))
        if not amount.is_finite() or scaled != amount or abs(amount) >= Decimal(10) ** 32:
            raise GovernmentShapeError("Money cannot fit DECIMAL(38,6) without rounding")
        return amount
    except InvalidOperation as error:
        raise GovernmentShapeError("Invalid exact decimal amount") from error


def _json(value: Any, kind: type) -> Any:
    if value is None:
        return None
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (ValueError, TypeError) as error:
        raise GovernmentShapeError("Invalid source JSON") from error
    if parsed is None:
        return None
    if not isinstance(parsed, kind):
        raise GovernmentShapeError(f"Expected {kind.__name__} source JSON")
    return parsed


def _structs(values: Any, fields: tuple[tuple[str, str], ...]) -> list | None:
    if values is None:
        return None
    if not isinstance(values, list):
        raise GovernmentShapeError("Expected a source array")
    result = []
    for value in values:
        if value is None:
            result.append(None)
        elif isinstance(value, dict):
            result.append(
                {name: _text(value.get(source), identifier=name in ("id", "id_proceeding")) for name, source in fields}
            )
        else:
            raise GovernmentShapeError("Unsupported array element; raw value belongs in refusal receipt")
    return result


def list_value(name: str, source: Any) -> list | None:
    """The native list column ``name`` from its source ``<name>_json`` text (the one rule, also run by the bulk writer)."""
    old = name + "_json"
    values = _json(source, list)
    if old in STRUCT_ARRAYS:
        return _structs(values, tuple((k, k) for k in STRUCT_ARRAYS[old]))
    return None if values is None else [_text(v) for v in values]


def _fcc_arrays(row: Mapping[str, Any]) -> dict[str, Any]:
    literal = row.get("native_fields_json")
    native = _json(literal, dict)
    if literal is not None and row.get("native_fields_sha256") is not None:
        digest = "sha256:" + hashlib.sha256(literal.encode()).hexdigest()
        if digest != row["native_fields_sha256"]:
            raise GovernmentShapeError("FCC native fields differ from their retained digest")
    result = {}
    for old, field in FCC_LEGACY_ARRAYS.items():
        if native is not None:
            values = native.get(field)
        else:
            values = _json(row.get(old), list)
            if field != "documents" and values is not None:
                values = [None if name is None else {"name": _text(name)} for name in values]
        result[field] = _structs(values, FCC_ARRAYS[field])
    return result


def _fcc_extractions(row: Mapping[str, Any], documents: list | None) -> list | None:
    """Retain the latest recorded result order, without inventing capture or text custody.

    A repeated offered URL supplies several offered positions, not several
    extraction attempts. The attempt ordinal is its position in the retained
    result array, not a lifetime attempt count or an acquisition timestamp.
    """
    results = _json(row.get("pdf_extraction_results_json"), list)
    if results is None:
        return None
    offered: dict[str, list[int]] = {}
    for ordinal, document in enumerate(documents or []):
        if document is not None and document.get("src") is not None:
            offered.setdefault(document["src"], []).append(ordinal)
    mapped = []
    for ordinal, result in enumerate(results):
        if result is None:
            mapped.append(None)
            continue
        if not isinstance(result, dict):
            raise GovernmentShapeError("FCC extraction result must be an object or null")
        url = _text(result.get("url"))
        try:
            parts = urlsplit(url) if url else None
            usable = bool(isinstance(url, str) and parts and parts.scheme == "https" and parts.hostname
                          and not parts.username and not parts.password
                          and not re.search(r"\s", url))
        except ValueError:
            usable = False
        digest = _text(result.get("source_sha256"))
        valid_digest = bool(digest and re.fullmatch(r"(?:sha256:)?[0-9a-f]{64}", digest))
        mapped.append({
            "attempt_ordinal": ordinal, "url": url, "offered_ordinals": offered.get(url, []) if isinstance(url, str) else [],
            "url_status": "usable" if usable else "missing" if url is None else "invalid",
            "source_sha256": digest,
            "digest_status": "recorded" if valid_digest else "missing" if digest is None else "invalid",
            "status": _text(result.get("status")), "page_count": _integer(result.get("page_count")),
            "error": _text(result.get("error")),
            "capture_status": "unverified" if digest is not None else "not_recorded",
            "text_access_status": "unverified",
        })
    return mapped


def map_subject(dataset: str, row: Mapping[str, Any], *, generation_id: str | None = None) -> dict[str, Any]:
    """Map one declared legacy row, refusing unclassified top-level fields and lossy coercions.

    The caller retains the entire original row as a receipt input before invoking
    this mapper, including failed inputs. No malformed value silently becomes an
    empty array, zero amount, or successful subject.
    """
    unknown = set(row) - set(LEGACY_COLUMNS[dataset])
    if unknown:
        raise GovernmentShapeError(f"{dataset}: unclassified fields {sorted(unknown)}")
    if dataset == "gao_decisions":
        if row.get("released_date") is not None and row.get("decision_date") is not None:
            if row["released_date"] != row["decision_date"]:
                raise GovernmentShapeError("Conflicting GAO release-day spellings")
        row = dict(row)
        if row.get("released_date") is None:
            row["released_date"] = row.get("decision_date")
    fcc = _fcc_arrays(row) if dataset == "fcc_filings" else {}
    if dataset == "fcc_filings":
        fcc.update(generation_id=generation_id, extraction_results=_fcc_extractions(row, fcc.get("documents")))
    subject: dict[str, Any] = {}
    for field in SUBJECT_SCHEMAS[dataset]:
        name = field.name
        value = row.get(name)
        if name in fcc:
            value = fcc[name]
        elif pa.types.is_list(field.type):
            value = list_value(name, row.get(name + "_json"))
        elif pa.types.is_integer(field.type):
            value = _integer(value)
        elif pa.types.is_boolean(field.type):
            value = _boolean(value)
        elif pa.types.is_decimal(field.type):
            value = _money(value)
        elif pa.types.is_date(field.type) and value is not None:
            try:
                value = date.fromisoformat(value) if isinstance(value, str) else value
                if not isinstance(value, date) or isinstance(value, datetime):
                    raise ValueError("Expected an ISO day")
            except ValueError as error:
                raise GovernmentShapeError(f"{name}: invalid calendar date") from error
        elif pa.types.is_timestamp(field.type) and value is not None:
            try:
                value = datetime.fromisoformat(value) if isinstance(value, str) else value
                if not isinstance(value, datetime) or value.utcoffset() is None:
                    raise ValueError("Expected an offset timestamp")
                value = value.astimezone(timezone.utc)
            except ValueError as error:
                raise GovernmentShapeError(f"{name}: invalid offset timestamp") from error
        elif pa.types.is_string(field.type):
            value = _text(value)
            if value is not None and name in STRIPPED_TEXT.get(dataset, ()):
                value = value.strip(EDGE_WHITESPACE) or None
        subject[name] = value
    if subject.get(IDENTITY_FIELDS[dataset][0]) in (None, ""):
        raise GovernmentShapeError(f"{dataset}: missing stable identity")
    if dataset == "gao_reports":
        subject["citation_product_id"] = citation_product_id(subject)
    return subject
