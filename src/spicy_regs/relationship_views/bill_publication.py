"""Native printing dates with source pins and deliberately limited publication meaning."""

from .sql_views import SQLView


def publication_dates(_pins):
    return """SELECT v.bill_id, v.version_code, v.printing_id, v.package_id, v.body_version_id,
        v.version_date AS action_version_date, v.publication_evidence_status,
        d.ordinality AS date_ordinal, d.value.field AS native_field, d.value.value AS native_value,
        d.value.state AS native_state, d.value.precision AS native_precision,
        d.value.attributes AS native_attributes, d.value.locator AS evidence_locator,
        d.value.source_sha256 AS evidence_sha256, d.value.source_url AS evidence_url,
        d.value.observed_at AS metadata_observed_at,
        'Issue, update and ingest facts do not prove the publication time of these body bytes or first online availability.'
            AS publication_limit
        FROM bill_versions v LEFT JOIN UNNEST(v.publication_dates) WITH ORDINALITY AS d(value, ordinality) ON TRUE"""


BILL_PUBLICATION_VIEWS = (SQLView(
    "bill_text_publication_dates",
    {"bill_versions": ("bill_id", "version_code", "printing_id", "package_id", "body_version_id", "version_date",
                       "publication_evidence_status", "publication_dates")},
    publication_dates,
    "Each printing's native issue/update/ingest date and exact metadata evidence. Missing enrichment remains visible.",
    ("bill_id", "version_code", "printing_id", "date_ordinal"), rule_version="bill-publication-evidence/1",
    column_descriptions={
        "action_version_date": "Congress textVersions date for the action-stage printing; enrolled empty remains empty.",
        "date_ordinal": "One-based position in retained date observations, preserving repeated values.",
        "native_field": "Publisher field path in SUMMARY or MODS.",
        "native_value": "Literal publisher date text; no instant or timezone is invented.",
        "native_state": "Missing, null, empty or a stated value.",
        "native_precision": "Lexical year, month, day or second precision; other syntax remains source_lexical.",
        "native_attributes": "Every native MODS date attribute, in source order.",
        "evidence_locator": "JSON field or element-child path within the pinned original.",
        "evidence_sha256": "Digest of the retained original metadata response.",
        "evidence_url": "Metadata response locator, independently checked against the exact printing.",
        "metadata_observed_at": "Capture observation of this metadata response, separate from the body observation.",
        "publication_limit": "Limits of these source facts for body publication or availability claims.",
    },
),)
