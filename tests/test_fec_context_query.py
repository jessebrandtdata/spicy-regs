"""Build-time context interpretation and thin serving navigation."""

from datetime import timezone

import duckdb
import pytest

from spicy_regs.relationship_views.fec_context_query import FEC_CONTEXT_QUERY_VIEWS
from spicy_regs.relationship_views.sql_views import install_sql_views
from spicy_regs.transforms.fec_context_shape import (
    api_control_values,
    filing_announcement,
    nonnegative_integer,
    source_page_body,
)

SPECS = {s.name: s for s in FEC_CONTEXT_QUERY_VIEWS}


@pytest.fixture
def db():
    with duckdb.connect() as connection:
        yield connection


def install(db, table, data, *names):
    specs = [SPECS[n] for n in names]
    columns = sorted(set().union(*(s.required[table] for s in specs)))
    numeric = {"page", "per_page", "reported_count", "reported_pages", "observed_count"}
    types = {c: "BIGINT" if c in numeric else "BOOLEAN" if c == "is_count_exact" else "VARCHAR" for c in columns}
    db.execute(f"CREATE TABLE {table} ({', '.join(c + ' ' + types[c] for c in columns)})")
    for row in data:
        db.execute(f"INSERT INTO {table} VALUES ({','.join('?' for _ in columns)})", [row.get(c) for c in columns])
    result = install_sql_views(db, [table], specs)
    assert all(v["status"] == "available" for v in result.values())


def rows(db, view):
    result = db.execute("SELECT * FROM " + view)
    return [dict(zip([c[0] for c in result.description], row)) for row in result.fetchall()]


def controls():
    return [
        dict(
            collection_id="c",
            source_sha256="digest",
            source_namespace="api",
            source_url="https://example/",
            record_id=str(i),
            query_completeness="not-asserted",
            **api_control_values(dict(field=field, value=value)),
        )
        for i, (field, value) in enumerate(
            (
                ("api_version", "1.0"),
                ("pagination", dict(page=1, per_page=100, count=301, pages=4, is_count_exact=True)),
                ("results", [{"name": "Ada"}]),
            )
        )
    ]


def test_response_grouping_preserves_counts_without_results(db):
    data = controls()
    install(db, "fec_api_response_controls", data, "fec_api_responses")
    (row,) = rows(db, "fec_api_responses")
    assert row["observed_count"] == 1 and row["reported_count"] == 301
    assert row["is_count_exact"] is True and row["parsing_status"] == "parsed"
    assert row["control_row_count"] == 3
    assert all("value_json" not in r for r in data)


def test_conflicting_controls_never_choose_arbitrary_value(db):
    data = controls()
    data.append(dict(data[1], record_id="conflict", page=2))
    install(db, "fec_api_response_controls", data, "fec_api_responses")
    (row,) = rows(db, "fec_api_responses")
    assert row["page"] is None and row["parsing_status"] == "conflicting_fields"
    assert row["repeated_field_rows"] == 1


@pytest.mark.parametrize("value", [1.5, -1, True, "2", None, 2**64])
def test_build_counts_reject_coercion(value):
    assert nonnegative_integer(value) is None
    native = dict(page=1, per_page=100, count=value, pages=1, is_count_exact=True)
    shaped = api_control_values(dict(field="pagination", value=native))
    assert shaped["reported_count"] is None
    assert shaped["parsing_status"] == "invalid_or_missing_pagination"


@pytest.mark.parametrize("value", [1, "true", None])
def test_build_boolean_requires_boolean(value):
    native = dict(page=1, per_page=100, count=1, pages=1, is_count_exact=value)
    shaped = api_control_values(dict(field="pagination", value=native))
    assert shaped["is_count_exact"] is None and shaped["parsing_status"] == "invalid_or_missing_pagination"


def test_build_feed_retains_candidate_label_mismatch_and_exact_dates():
    description = "CommitteeId: H6KY04171 | FilingId: 2011570 | CoverageFrom: 08/01/2026 | ReportType: MONTHLY"
    row = filing_announcement(description, "Fri, 11 Sep 2026 18:08:19 GMT")
    assert row["candidate_id"] == "H6KY04171" and row["committee_id"] is None
    assert row["source_label_status"] == "candidate_id_under_committee_label"
    assert row["published_at"].astimezone(timezone.utc).isoformat() == "2026-09-11T18:08:19+00:00"
    assert row["coverage_start_date"].isoformat() == "2026-08-01"
    assert row["filing_link_status"] == "source_assertion_not_qualified_filing"
    repeated = filing_announcement(description + " | FilingId: 2", None)
    assert repeated["filing_id"] is None and repeated["parsing_status"] == "repeated_labels"


@pytest.mark.parametrize(
    ("coverage", "published", "bad_column"),
    [
        ("08/01/26", "Fri, 11 Sep 2026 18:08:19 GMT", "coverage_start_date"),
        ("08/01/2026", "not a timestamp", "published_at"),
        ("08/01/2026", "Fri, 11 Sep 26 18:08:19 GMT", "published_at"),
    ],
)
def test_build_feed_exposes_invalid_dates(coverage, published, bad_column):
    row = filing_announcement(f"CommitteeId: C00381806 | FilingId: 2011570 | CoverageFrom: {coverage}", published)
    assert row[bad_column] is None and row["parsing_status"] == "invalid_date"


def test_build_pages_remove_template_and_keep_failure_dispositions():
    masthead = "Federal Election Commission | United States of America"
    text = f"Browser warning\n{masthead}\nMenu\n{masthead}\nCourt decision\nAbout\nCareers\nPress\nContact\nPrivacy and security policy"
    assert source_page_body("Court cases", text, "https://www.fec.gov/") == ("Court decision", "body_extracted")
    assert source_page_body("Server error", text, "https://www.fec.gov/") == (None, "failed_page")
    # The shared classifier also names an interstitial title FEC's collection reader refused.
    assert source_page_body("Just a moment...", text, "https://www.fec.gov/") == (None, "failed_page")
    assert source_page_body("Unknown", "Uncertain text boundaries", None) == (None, "unsupported_body_boundaries")
    assert source_page_body("Empty", "\n\t ", None) == (None, "empty_page")


def test_child_meetings_preserve_order_repetition_and_range(db):
    install(
        db,
        "fec_research_meeting_observations",
        [
            dict(
                record_id="range",
                dates_json='["2025-01-01","2025-01-03"]',
                date_status="source_date_range",
                links_json="[]",
            ),
            dict(
                record_id="list",
                dates_json='["2025-01-01","2025-01-01",null]',
                date_status="source_listed_dates",
                links_json='[{"url":"https://example/","source_fact_index":4},null,{"url":"https://example/"}]',
            ),
        ],
        "fec_meeting_dates",
        "fec_meeting_links",
    )
    dates = rows(db, "fec_meeting_dates")
    assert len(dates) == 5
    assert sorted((r["source_ordinal"], r["date_role"]) for r in dates if r["record_id"] == "range") == [
        (0, "range_start"),
        (1, "range_end"),
    ]
    assert len(rows(db, "fec_meeting_links")) == 3


def test_no_primary_mirror_views_and_installation_does_not_scan(db):
    assert set(SPECS) == {"fec_api_responses", "fec_meeting_dates", "fec_meeting_links"}
    spec = SPECS["fec_meeting_dates"]
    db.execute(
        "CREATE VIEW fec_research_meeting_observations AS SELECT "
        + ", ".join(
            "CAST(error('source scanned') AS VARCHAR) AS " + c
            for c in spec.required["fec_research_meeting_observations"]
        )
    )
    assert install_sql_views(db, ["fec_research_meeting_observations"], [spec])[spec.name]["status"] == "available"
