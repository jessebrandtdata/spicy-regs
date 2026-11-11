"""Tests for the data dictionary generator/checker.

Hermetic (no network): everything here runs against the in-code schema
(``RECORD_TYPES`` + ``DERIVED_SCHEMAS``) and the committed
``data_dictionary/descriptions.yaml``. The live-data reconciliation
(``check --source r2``) is exercised in the docs deploy workflow, not here.
"""

from __future__ import annotations

import json
from datetime import date

import pytest

from spicy_regs import data_dictionary as dd
from spicy_regs.schemas.regulations import RECORD_TYPES


def test_source_catalog_policy_descriptions_keep_selected_scope_and_native_fields():
    from spicy_regs.etl_policy_registry import installed_policies

    names = {
        "administration_policy_statements", "administration_policy_bill_links",
        "federal_register_issues", "ecfr_snapshots", "ecfr_snapshot_sections",
        "table3_historical_records", "uscode_editions", "uscode_edition_structure", "uscode_edition_sections",
    }
    descriptions, policies = dd.load_descriptions(), installed_policies()
    for name in names:
        entry, policy = descriptions[name], policies[name]
        assert set(entry["columns"]) == set(policy.subject_schema.names)
        assert entry["identity_columns"] == list(policy.identity_fields)
        assert entry["coverage"].startswith("Sampled.")
        assert dd.coverage_prose_errors(name, entry["coverage"]) == []
    assert "uscode_edition_sections" in policies


def test_retained_native_policies_regenerate_receipt_declarations():
    from spicy_regs.sources.retained_native_collections import POLICIES
    from scripts.update_etl_policy_declarations import family_policies

    selected = family_policies(frozenset(POLICIES))
    assert set(selected) == set(POLICIES)
    for name, policy in POLICIES.items():
        assert selected[name].descriptor() == policy.descriptor()
        assert not policy.receipt_fields


def test_generation_preserves_qualified_legacy_fec_meanings_and_processing_tables():
    import json

    legacy = json.loads((dd.REPO_ROOT / "data_dictionary" / "fec_legacy_column_descriptions.json").read_text())
    descriptions = dd.load_descriptions()
    generated = dd.build_mcp_metadata(descriptions, dd.expected_schemas())
    for table, meanings in legacy["tables"].items():
        assert generated[table]["legacy_column_descriptions"] == meanings
    for table in ("fec_collection_selection", "fec_filing_definition_evidence",
                  "fec_filing_header_associations", "fec_record_evidence"):
        assert generated[table]["columns"]
    current = descriptions["fec_receipts"]["columns"]["filing_association_target_generation_pin"]
    historical = legacy["tables"]["fec_receipts"]["filing_association_target_generation_pin"]
    assert "self" in current and current != historical


def test_record_types_covered_by_expected_schemas():
    """Every core table exposes only its declared native subject fields."""
    expected = dd.expected_schemas()
    for name in RECORD_TYPES:
        assert name in expected
        assert [c for c, _ in expected[name]] == dd.subject_policies()[name].subject_schema.names



def test_native_field_prose_preserves_current_table_meaning(tmp_path):
    """Generated native columns cannot replace newer curated table descriptions."""
    import yaml

    document = yaml.safe_load(dd.DEFAULT_DESCRIPTIONS.read_text())
    entry = document["tables"]["law_sections"]
    entry["summary"] = "The current law section meaning."
    entry["coverage"] = "Derived: successfully read law sections only."
    path = tmp_path / "descriptions.yaml"
    path.write_text(yaml.safe_dump(document))

    described = dd.load_descriptions(path)["law_sections"]
    # The summary is the written one: nothing about receipts is appended to it (owner decision, 2026-10-05).
    assert described["summary"] == entry["summary"]
    assert described["coverage"] == entry["coverage"]
    assert described["subject"] == "congress"
    assert described["identity_columns"] == ["law_id", "seq"]
    assert set(described["columns"]) == set(dd.subject_policies()["law_sections"].subject_schema.names)


def test_no_summary_carries_the_paragraph_once_appended_to_every_policy_table():
    for table, entry in dd.load_descriptions().items():
        assert "stored separately in etl_receipts" not in entry["summary"], table


def test_all_tables_have_a_schema():
    expected = dd.expected_schemas()
    assert set(expected) == set(dd.TABLES)
    for cols in expected.values():
        assert cols, "schema should be non-empty"


def test_descriptions_match_schema():
    """The shipped descriptions.yaml must fully cover the in-code schema."""
    descriptions = dd.load_descriptions()
    errors = dd.check_descriptions(dd.expected_schemas(), descriptions)
    assert errors == [], "\n".join(errors)


def test_check_detects_missing_description():
    descriptions = dd.load_descriptions()
    # Drop a real column's description -> should be flagged.
    broken = {t: {**v, "columns": dict(v.get("columns") or {})} for t, v in descriptions.items()}
    broken["dockets"]["columns"].pop("rin")
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("rin" in e for e in errors)


def test_check_detects_orphan_description():
    descriptions = dd.load_descriptions()
    broken = {t: {**v, "columns": dict(v.get("columns") or {})} for t, v in descriptions.items()}
    broken["dockets"]["columns"]["not_a_real_column"] = "x"
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("not_a_real_column" in e for e in errors)


def test_check_detects_missing_summary():
    descriptions = dd.load_descriptions()
    broken = {t: {**v} for t, v in descriptions.items()}
    broken["comments"] = {**broken["comments"], "summary": ""}
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("summary" in e for e in errors)


def test_generate_writes_a_page_per_table(tmp_path):
    descriptions = dd.load_descriptions()
    written = dd.generate(descriptions, dd.expected_schemas(), out_dir=tmp_path)
    assert {p.stem for p in written} == set(dd.TABLES)
    dockets = (tmp_path / "dockets.md").read_text(encoding="utf-8")
    assert "# `dockets`" in dockets
    assert "`docket_id`" in dockets
    # The primary key is flagged.
    assert "🔑" in dockets


def test_committed_pages_are_up_to_date(tmp_path):
    """The committed docs/tables/*.md must equal a fresh generation."""
    descriptions = dd.load_descriptions()
    dd.generate(descriptions, dd.expected_schemas(), out_dir=tmp_path)
    for table in dd.TABLES:
        committed = dd.DEFAULT_DOCS_TABLES_DIR / f"{table}.md"
        assert committed.exists(), f"missing committed page for {table}"
        fresh = (tmp_path / f"{table}.md").read_text(encoding="utf-8")
        assert committed.read_text(encoding="utf-8") == fresh, (
            f"{table}.md is stale; run 'uv run spicy-regs-dict generate'"
        )


def test_mcp_queryable_subset_of_tables():
    assert dd.MCP_QUERYABLE <= set(dd.TABLES)


def test_mcp_server_tables_match_dictionary():
    """The MCP server must expose exactly the dictionary's published tables."""
    from spicy_regs import mcp_server

    assert set(mcp_server.TABLES) == set(dd.TABLES)


def test_mcp_queryable_matches_mcp_server():
    """The docs' 'queryable via MCP' flag must track what the server serves."""
    from spicy_regs import mcp_server

    assert dd.MCP_QUERYABLE == set(mcp_server.TABLES)


def test_every_table_has_a_display_label():
    """The catalog names a class by this label, so no table may go unnamed."""
    descriptions = dd.load_descriptions()
    labels = dd.table_labels(descriptions)
    assert set(labels) == set(dd.TABLES)
    unnamed = [t for t, label in labels.items() if not label.strip()]
    assert not unnamed, f"tables with no display label: {unnamed}"


def test_check_detects_a_missing_label():
    descriptions = dd.load_descriptions()
    broken = {t: dict(entry) for t, entry in descriptions.items()}
    broken["congress_bills"].pop("label")
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("missing a 'label'" in e for e in errors)


def test_generated_page_carries_the_label():
    descriptions = dd.load_descriptions()
    page = dd.DEFAULT_DOCS_TABLES_DIR / "congress_bills.md"
    assert f"**{descriptions['congress_bills']['label']}**" in page.read_text()


def test_every_table_has_a_coverage_statement():
    """The boundary sentence a user reads comes from here, so none may be blank."""
    descriptions = dd.load_descriptions()
    coverage = dd.table_coverage(descriptions)
    assert set(coverage) == set(dd.TABLES)
    missing = [t for t, e in coverage.items() if not e["coverage"].strip()]
    assert not missing, f"tables with no coverage statement: {missing}"


def test_every_coverage_statement_names_its_kind():
    """A range, a window and a derived table are different claims; each says which."""
    bad = [t for t, e in dd.table_coverage(dd.load_descriptions()).items() if dd.coverage_kind(e["coverage"]) is None]
    assert not bad, f"coverage statements that do not open by naming their kind: {bad}"


def test_kind_is_readable_without_parsing_the_sentence():
    """The consumer branches on `kind`, never on the prose's first token.

    The prose opens with the kind followed by a period or a comma depending on
    the phrasing, so a whitespace split returns "Derived." or "Derived," and
    matches neither. This asserts the consumer's access pattern rather than the
    author's, which is why the earlier startswith test passed while a consumer
    would have failed.
    """
    document = dd.build_catalog(dd.load_descriptions(), dd.expected_schemas())
    for entry in document["classes"]:
        assert entry["kind"] in set(dd.COVERAGE_KINDS.values()), entry["table"]
        assert dd.coverage_kind(entry["coverage"]) == entry["kind"], entry["table"]
    assert {e["kind"] for e in document["classes"]} == set(dd.COVERAGE_KINDS.values())


def test_check_detects_a_missing_coverage_statement():
    descriptions = dd.load_descriptions()
    broken = {t: dict(entry) for t, entry in descriptions.items()}
    broken["gao_reports"].pop("coverage")
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("missing a 'coverage'" in e for e in errors)


def test_check_detects_an_empty_data_quality_note():
    descriptions = dd.load_descriptions()
    broken = {t: dict(entry) for t, entry in descriptions.items()}
    broken["documents"]["data_quality"] = "   "
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("empty 'data_quality'" in e for e in errors)


@pytest.mark.parametrize(
    ("prose", "refusal"),
    [
        ("Derived. `snapshot_91b19da7…` holds the periods.", "a snapshot id"),
        ("Window. Published as generation 9c87b600aa7b5847.", "a digest or generation pin"),
        ("Derived. Built from members generation `017366cc…`.", "a digest or generation pin"),
        ("Sampled. Local output only; not uploaded.", "a publication-status claim"),
        ("Window. The candidate is not yet published.", "a publication-status claim"),
        ("Window. Measured 319,501 rows on the candidate.", "a row count"),
        ("True range. 1,009,005 dated records published 1994 on.", "a row count"),
        ("Sampled. 13,717,161 provider records in selected collections.", "a row count"),
    ],
)
def test_coverage_prose_refuses_facts_that_decay_with_each_publication(prose, refusal):
    """Counts, snapshots, pins and publication claims go stale at the next publish; the server states them live."""
    errors = dd.coverage_prose_errors("gao_reports", prose)
    assert len(errors) == 1 and refusal in errors[0], errors


@pytest.mark.parametrize(
    "prose",
    [
        "Window: filings ECFS received on or after 2026-08-24, extended by a daily run.",
        "True range. Filing years 2010 to date, each read whole; spicy-regs `3bc0a97` built it.",
        "Sampled. One row per retained collection; rows from different cycles share the table.",
    ],
)
def test_coverage_prose_keeps_scope_dates_rules_and_commit_references(prose):
    assert dd.coverage_prose_errors("gao_reports", prose) == []


def test_a_coverage_exception_allows_exactly_its_phrase(monkeypatch):
    monkeypatch.setitem(dd.COVERAGE_PROSE_EXCEPTIONS, "gao_reports", {"about 3,693 rows": "caveat size"})
    assert dd.coverage_prose_errors("gao_reports", "Window. Names unread on about 3,693 rows.") == []
    errors = dd.coverage_prose_errors("gao_reports", "Window. Names unread on about 3,693 rows; 43 rows held.")
    assert len(errors) == 1 and "'43 rows'" in errors[0]


def test_a_coverage_exception_that_no_longer_matches_is_an_error(monkeypatch):
    """A rewritten note cannot leave a stale permission behind."""
    monkeypatch.setitem(dd.COVERAGE_PROSE_EXCEPTIONS, "gao_reports", {"about 3,693 rows": "caveat size"})
    errors = dd.coverage_prose_errors("gao_reports", "Window. Reports the feed has listed.")
    assert errors and "no longer matches" in errors[0]


def test_check_refuses_a_row_count_in_committed_style_prose():
    descriptions = dd.load_descriptions()
    broken = {t: dict(entry) for t, entry in descriptions.items()}
    broken["gao_reports"]["coverage"] = "Window. 43 rows on the measurement date."
    errors = dd.check_descriptions(dd.expected_schemas(), broken)
    assert any("[gao_reports] coverage states a row count" in e for e in errors)


def test_generated_page_carries_coverage_and_data_quality():
    page = (dd.DEFAULT_DOCS_TABLES_DIR / "documents.md").read_text()
    assert "**Coverage.**" in page
    assert "**Data quality.**" in page


def test_catalog_declares_every_published_class():
    """The consumer cannot import this package, so the file is the interface."""
    descriptions = dd.load_descriptions()
    document = dd.build_catalog(descriptions, dd.expected_schemas())
    assert document["format_version"] == dd.CATALOG_FORMAT_VERSION
    assert [c["table"] for c in document["classes"]] == list(dd.TABLES)
    for entry in document["classes"]:
        assert entry["label"].strip(), entry["table"]
        assert entry["coverage"].strip(), entry["table"]
        assert entry["columns"], entry["table"]


def test_catalog_does_not_claim_searchability():
    """Searchable is the serving side's fact; a catalog certifying itself is not a check."""
    document = dd.build_catalog(dd.load_descriptions(), dd.expected_schemas())
    blob = json.dumps(document).lower()
    for claim in ("queryable", "searchable", "indexed", "mcp_queryable"):
        assert claim not in blob, f"catalog must not assert {claim!r}"


def test_committed_catalog_is_up_to_date():
    """Same discipline as the table pages: the committed file equals a fresh build."""
    fresh = dd.build_catalog(dd.load_descriptions(), dd.expected_schemas())
    committed = json.loads(dd.DEFAULT_CATALOG_PATH.read_text(encoding="utf-8"))
    assert committed == fresh, "catalog.json is stale; run 'uv run spicy-regs-dict catalog'"


def test_bundled_mcp_metadata_matches_dictionary_and_provider():
    """The base MCP wheel carries the same meaning as the generated docs."""
    fresh = dd.build_mcp_metadata(dd.load_descriptions(), dd.expected_schemas())
    committed = json.loads(dd.DEFAULT_MCP_METADATA_PATH.read_text(encoding="utf-8"))
    assert committed == fresh, "table_metadata.json is stale; run 'uv run spicy-regs-dict generate'"
    assert committed["dockets"]["identity_columns"] == ["docket_id"]
    assert committed["member_votes"]["identity_columns"] == list(dd.subject_policies()["member_votes"].identity_fields)
    assert committed["member_votes"]["grain"] == dd.contract_grain("member_votes")


def test_check_rejects_undeclared_identity_column():
    descriptions = dd.load_descriptions()
    descriptions["fec_committees"]["identity_columns"] = ["missing_column"]
    assert any("identity_columns" in error for error in dd.check_descriptions(dd.expected_schemas(), descriptions))


def test_catalog_digest_sidecar_matches_the_committed_bytes():
    """A vendored contract is pinned by digest, so the sidecar must track the file.

    The digest is a sidecar rather than a field inside the document on purpose:
    a digest carried by the thing it certifies proves nothing.
    """
    import hashlib

    payload = dd.DEFAULT_CATALOG_PATH.read_bytes()
    recorded = dd.DEFAULT_CATALOG_DIGEST_PATH.read_text(encoding="utf-8").split()[0]
    assert hashlib.sha256(payload).hexdigest() == recorded, (
        "catalog.json.sha256 is stale; run 'uv run spicy-regs-dict catalog'"
    )


def test_catalog_bytes_are_the_one_canonical_form():
    """One document, one byte string, or the digest pin is meaningless."""
    document = dd.build_catalog(dd.load_descriptions(), dd.expected_schemas())
    assert dd.catalog_bytes(document) == dd.catalog_bytes(document)
    assert dd.catalog_bytes(document) == dd.DEFAULT_CATALOG_PATH.read_bytes()


def test_every_coverage_statement_carries_a_measurement_date():
    """A statement with no date cannot be told from a fresh one by a reader."""
    coverage = dd.table_coverage(dd.load_descriptions())
    assert set(coverage) == set(dd.TABLES)
    for table, entry in coverage.items():
        assert entry["measured_on"].strip(), table
        date.fromisoformat(entry["measured_on"])


def test_check_detects_a_missing_or_malformed_measurement_date():
    descriptions = dd.load_descriptions()
    missing = {t: dict(e) for t, e in descriptions.items()}
    missing["gao_reports"].pop("measured_on")
    assert any("missing 'measured_on'" in e for e in dd.check_descriptions(dd.expected_schemas(), missing))

    malformed = {t: dict(e) for t, e in descriptions.items()}
    malformed["gao_reports"]["measured_on"] = "last Tuesday"
    assert any("not an ISO date" in e for e in dd.check_descriptions(dd.expected_schemas(), malformed))


def test_unreachable_source_is_not_reported_as_a_pass(monkeypatch):
    """The gate must tell an outage from drift; collapsing them is why it could not fail."""
    import httpx
    from spicy_regs.sources import publication

    def unreachable(*_args, **_kwargs):
        raise httpx.ConnectError("fixture publisher is unavailable")

    monkeypatch.setattr(publication, "load_index", unreachable)
    parser = dd.build_parser()
    args = parser.parse_args(["check", "--source", "r2", "--base", "https://nonexistent.invalid"])
    assert args.func(args) == dd.EXIT_SOURCE_UNREACHABLE
    assert dd.EXIT_SOURCE_UNREACHABLE not in (0, 1)


def test_discovery_continues_after_an_unreadable_table(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    monkeypatch.setattr(dd, 'TABLES', ('missing', 'present'))
    pq.write_table(pa.table({'id': ['one']}), tmp_path / 'present.parquet')
    with pytest.raises(dd.SchemaDiscoveryError) as error:
        dd.discover_schemas('local', str(tmp_path))
    assert error.value.schemas == {'present': [('id', 'VARCHAR')]}
    assert '[missing]' in str(error.value)


@pytest.fixture
def dictionary_publication(monkeypatch):
    """Pin live-check metadata while the unit cases control schema discovery.

    Exercise the real index parser, stable-selection check and published row
    counts. Only the remote byte reader is replaced; unknown routes fail.
    """
    from spicy_regs.sources import publication

    base = "https://dictionary.test"
    digest = "a" * 64
    index = {**publication.empty_index(), "families": {"dockets": {
        "prefix": f"generations/dockets/{digest}",
        "logicalId": "urn:spicy:test:dictionary-dockets",
        "artifactDigest": f"sha256:{digest}",
        "tables": {"dockets.parquet": {
            "sha256": "sha256:" + "b" * 64, "byteSize": 0, "rows": 1,
            "columns": dd.expected_schemas()["dockets"],
        }},
    }}}
    raw = json.dumps(index).encode()

    def read(url, **_kwargs):
        if url == f"{base}/{publication.INDEX_V2_KEY}":
            return raw
        if url == f"{base}/{publication.SNAPSHOT_POINTER}":
            return None
        pytest.fail(f"Unexpected publication fixture URL: {url}")

    monkeypatch.setenv("R2_PUBLIC_URL", base)
    monkeypatch.setattr(publication, "_bounded_get", read)
    return publication.parse_index(raw)


def test_live_check_accepts_supported_but_unpublished_tables(monkeypatch, dictionary_publication):
    schemas = dd.expected_schemas()
    monkeypatch.setattr(dd, 'discover_schemas', lambda *_, **_kwargs: {'dockets': schemas['dockets']})
    args = dd.build_parser().parse_args(['check', '--source', 'r2'])
    assert args.func(args) == 0


def test_unknown_published_table_is_schema_drift(monkeypatch, dictionary_publication):
    monkeypatch.setattr(dd, 'discover_schemas', lambda *_, **_kwargs: {'unknown_published': [('id', 'VARCHAR')]})
    args = dd.build_parser().parse_args(['check', '--source', 'r2'])
    assert args.func(args) == 1


def test_incomplete_live_check_is_not_a_pass_after_other_tables_match(monkeypatch, dictionary_publication):
    def partial(*_, **_kwargs):
        raise dd.SchemaDiscoveryError({'dockets': dd.expected_schemas()['dockets']}, ['[documents] unreachable'])
    monkeypatch.setattr(dd, 'discover_schemas', partial)
    args = dd.build_parser().parse_args(['check', '--source', 'r2'])
    assert args.func(args) == dd.EXIT_SOURCE_UNREACHABLE


# --------------------------------------------------------------------------- #
# The receipts page: one page explains receipts, and each statement on it is held to the writer.
# --------------------------------------------------------------------------- #
def test_the_receipts_page_is_the_guide_around_the_declared_columns():
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA

    page = (dd.DEFAULT_DOCS_TABLES_DIR / "etl_receipts.md").read_text(encoding="utf-8")
    assert page == dd.render_receipts_page(dd.receipt_metadata()), "etl_receipts.md is stale; run the generator"
    for heading in ("How to find the receipt for a row", "How to read a value out of a receipt",
                    "What a receipt contains", "What a receipt does not tell you"):
        assert f"\n## {heading}\n" in page
    assert dd.COLUMNS_MARK not in page
    assert all(f"| `{name}` |" in page for name in RECEIPT_SCHEMA.names)
    served = json.loads(dd.DEFAULT_MCP_METADATA_PATH.read_text(encoding="utf-8"))["etl_receipts"]["guide"]
    assert dd.COLUMNS_MARK not in served and "## How to find the receipt for a row" in served


def test_the_receipts_page_states_what_the_writer_does():
    """Each query and each warning on the page is run against receipts the real writer produced."""
    import re
    from datetime import datetime, timezone

    import duckdb
    import pyarrow as pa

    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, ReceiptContext, split_record, subject_identity

    guide = dd.RECEIPT_GUIDE.read_text(encoding="utf-8")
    context = ReceiptContext("build-1", "attempt-1", "test/1", [
        {"source_id": "s", "source_uri": "https://example.test/u", "sha256": "0" * 64, "locator": None, "body_version": None}])

    def written(table: str, rows: list[dict]) -> tuple[pa.Table, list[dict]]:
        policy = dd.subject_policies()[table]
        blank = dict.fromkeys(policy.subject_schema.names)
        pairs = [split_record(policy, {**blank, **row}, context) for row in rows]
        return pa.Table.from_pylist([subject for subject, _ in pairs], schema=policy.subject_schema), [r for _, r in pairs]

    link = "https://api.congress.gov/v3/crsreport/"
    reports, report_receipts = written("crs_reports", [
        {"report_id": "R48641", "update_date": datetime(2026, 7, 17, 22, 38, 55, tzinfo=timezone.utc),
         "title": "First", "raw_record": {"title": 'He said "no"', "url": link + "R48641"}},
        {"report_id": "IN12713", "update_date": datetime(2026, 7, 18, 12, 0, 0, 123456, tzinfo=timezone.utc),
         "title": "Second", "raw_record": {"url": link + "IN12713", "version": 2}},
    ])
    _, committee_receipts = written("fec_committee_history", [
        {"committee_id": "C00097238", "cycle": cycle, "conversion_inputs": {}, "conversion_diagnostics": {},
         "source_input": None} for cycle in (2012, 2014)])
    con = duckdb.connect()
    con.register("crs_reports", reports)
    con.register("etl_receipts", pa.Table.from_pylist(report_receipts + committee_receipts, schema=RECEIPT_SCHEMA))

    queries = re.findall(r"```sql\n(.*?)```", guide, re.DOTALL)
    find, join, read, read_whole = (con.execute(sql).fetchall() for sql in queries)
    assert [row[1] for row in find] == ["crs_reports"] and len(join) == 2
    assert read == [(link + "R48641",)] and read_whole == [('He said "no"',)]
    (look,) = re.findall(r"`(SELECT processing_json [^`]+)`", guide)
    assert len(con.execute(look).fetchall()) == 1

    # The two-column key the page spells is the writer's, and the short form it warns about does return both cycles.
    (two_column_key,) = re.findall(r"```text\n(.*?)\n```", guide, re.DOTALL)
    policy = dd.subject_policies()["fec_committee_history"]
    assert two_column_key == subject_identity(policy, {"committee_id": "C00097238", "cycle": 2012})[2]
    count = "SELECT count(*) FROM etl_receipts WHERE dataset = 'fec_committee_history' AND "
    assert con.execute(count + "identity_json = ?", [two_column_key]).fetchone() == (1,)
    assert con.execute(count + """contains(identity_json, '"C00097238"')""").fetchone() == (2,)

    # The pattern's stated limits: a quote cuts the value short, and an absent or non-text field reads as empty text.
    pattern = """SELECT regexp_extract(processing_json, '\\["{}",\\["str","([^"]*)"', 1) FROM etl_receipts WHERE {}"""
    first, second = "contains(identity_json, '\"R48641\"')", "contains(identity_json, '\"IN12713\"')"
    assert con.execute(pattern.format("title", first)).fetchone() == ("He said \\",)
    assert con.execute(pattern.format("no_such_field", first)).fetchone() == ("",)
    assert con.execute(pattern.format("version", second)).fetchone() == ("",)


# --------------------------------------------------------------------------- #
# A table points at its own receipt in one sentence, and only once the index shows it with receipts.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "sentence",
    [
        "Each report's address on the Congress.gov API is kept in its receipt; see etl_receipts.",
        "The filing's page on fcc.gov is kept in the filing's receipt, not in this table.",
        "Every row comes from `fec-bulk-lobbyist`; the source namespace is kept in the receipt.",
        "Whether the source reported a value is in the row's receipt, not in this table.",
        "Native lists retain ordered business values; processing evidence lives in the shared ETL receipts.",
        "Source irregularities and extraction outcomes remain in the law_sections ETL receipts.",
        "The locator identifies the source record among the fec_source_records receipts.",
        "A typo in the range cannot be detected; the receipt keeps the date as filed.",
        "Document filenames as offered. Source URLs and acquisition evidence remain in receipts.",
        "`SELECT processing_json FROM etl_receipts WHERE dataset = 'fcc_filings'`.",
    ],
)
def test_a_sentence_that_sends_a_reader_to_a_receipt_is_read_as_a_pointer(sentence):
    """Each is a wording main carried on 2026-10-05, most of them on tables that had published no receipts."""
    assert dd.receipt_pointers({"summary": sentence}) == ["summary"]


@pytest.mark.parametrize(
    "sentence",
    [
        "An account transfer is not automatically a receipt to the reporting committee.",
        "Source-specific meaning of amount, such as a reported receipt, payment, balance or aggregate.",
        "A retained original filing contributes only its receipt records.",
        "The day FEC received the filing is in the receipt date the source states.",
        "Time to final then runs from the receipt notice.",
        "Receipt: fork-execution-2026-09-21/discovery-utc/MANUAL-AUDIT.md.",
        "See `press-release-qualification/` and `press-link-repair/` in the fork execution receipts.",
        "The comment re-read filled it: on receipt sha256:77a08369… it is set on most rows.",
    ],
)
def test_money_received_and_evidence_folders_are_not_pointers(sentence):
    """`receipt` has two other meanings in this dictionary, and the rule is about neither."""
    assert dd.receipt_pointers({"summary": sentence}) == []


def test_a_table_points_at_its_receipt_in_at_most_one_sentence():
    entry = {"summary": "One row per report. Its API address is kept in its receipt; see etl_receipts.",
             "columns": {"report_id": "CRS report id."}}
    assert dd.receipt_pointer_errors("crs_reports", entry) == []
    entry["columns"]["report_id"] += " The former `url` column is kept in the report's receipt."
    errors = dd.receipt_pointer_errors("crs_reports", entry)
    assert len(errors) == 1 and "in 2 sentences (summary, columns.report_id)" in errors[0], errors
    broken = {table: dict(value) for table, value in dd.load_descriptions().items()}
    broken["crs_reports"] = {**broken["crs_reports"], "data_quality": "Dated 2026-10-05: more is kept in the receipt."}
    assert any(e.startswith("[crs_reports] points readers to its receipt in 2 sentences")
               for e in dd.check_descriptions(dd.expected_schemas(), broken))


def test_a_pointer_is_refused_until_the_index_shows_the_table_with_receipts():
    """lobbying_filings told readers to query a receipt while its family had published none (2026-10-05)."""
    descriptions = {
        "lobbying_filings": {"columns": {"filing_uuid": "The filing's page is kept in its receipt; see etl_receipts."}},
        "crs_reports": {"summary": "Each report's API address is kept in its receipt; see etl_receipts."},
        "dockets": {"summary": "One row per docket."},
    }

    def with_receipts(*families: str) -> set[str]:
        entries: dict[str, dict] = {"dockets": {"tables": {"dockets.parquet": {}}}}
        entries |= {name: {"etlReceipts": {"datasets": [name.replace("-", "_")]}} for name in families}
        return dd.published_receipt_datasets({"families": entries})

    published = ("lobbying_filings", "crs_reports", "dockets", "not_a_dictionary_table")
    errors = dd.receipt_index_errors(descriptions, published, with_receipts("crs-reports"))
    assert len(errors) == 1 and errors[0].startswith("[lobbying_filings] columns.filing_uuid points"), errors
    assert dd.receipt_index_errors(descriptions, published, with_receipts("crs-reports", "lobbying-filings")) == []
    # It follows actual publication, as the kind does: a publisher that holds none of these tables is not judged.
    assert dd.receipt_index_errors(descriptions, (), set()) == []


def test_a_view_points_at_a_receipt_in_at_most_one_sentence_too():
    """A view's summary is a table note as well, and describe_table serves it; the dictionary check does not read it."""
    import importlib
    import pkgutil

    import spicy_regs.relationship_views as package

    specs = {}
    for info in pkgutil.iter_modules(package.__path__):
        module = importlib.import_module(f"{package.__name__}.{info.name}")
        for value in vars(module).values():
            for item in value if isinstance(value, (tuple, list)) else (value,):
                if hasattr(item, "meaning") and hasattr(item, "name"):
                    specs[id(item)] = item
    # Three views that pointed at a receipt on 2026-10-05, in three modules: the search must reach each of them.
    assert {"house_communication_rins", "org_identity_candidates", "fcc_filing_artifacts"} <= {s.name for s in specs.values()}
    pointing = set()
    for spec in specs.values():
        columns = dict(getattr(spec, "column_descriptions", None) or {})
        columns |= {name: text for name, _, text in getattr(spec, "details", ())}
        fields = dd.receipt_pointers({"summary": spec.meaning, "columns": columns})
        assert len(fields) <= 1, (spec.name, fields)
        pointing |= {spec.name} if fields else set()
    # Offered document URLs are now main fields. Native observation evidence
    # still points once to retained receipts; artifacts need no receipt key.
    assert pointing == {"fcc_native_observations"}


# --------------------------------------------------------------------------- #
# The contract-hosted tables: prose comes from spicy-docs, not from this repo.
# --------------------------------------------------------------------------- #
def test_every_hosted_column_has_prose():
    """All four hundred-odd columns carry a sentence, read from the contract."""
    descriptions = dd.load_descriptions()
    schemas = dd.expected_schemas()
    for table in set(dd.CONTRACT_TABLES) & set(dd.TABLES):
        columns = descriptions[table]["columns"]
        assert set(columns) == {c for c, _ in schemas[table]}, table
        blank = [name for name, text in columns.items() if not (text or "").strip()]
        assert not blank, f"{table}: columns with no description: {blank}"


def test_hosted_prose_is_the_contract_prose_not_a_copy():
    """Wheel prose remains authoritative except explicitly enriched values and native count types."""
    from spicy_docs.schemas import TABLE_CONTRACTS

    descriptions = dd.load_descriptions()
    for table in set(dd.CONTRACT_TABLES) & set(dd.TABLES):
        source = dict(TABLE_CONTRACTS[table].descriptions)
        if table in {"house_communications", "native_legal_reference_reads"}:
            enriched = dd.contract_column_prose(table)
            fields = ({"record_package_id", "record_granule_id", "record_entry_text"}
                      if table == "house_communications" else {"source_bytes", "occurrence_count"})
            assert {field for field in source if source[field] != enriched[field]} == fields
            source.update({field: enriched[field] for field in fields})
        policy = dd.subject_policies().get(table)
        for field, prose in descriptions[table]["columns"].items():
            if field in source and (policy is None or str(policy.subject_schema.field(field).type) in {"string", "bool", "int64", "int32"}):
                assert prose == dd.native_spelling(source[field], table), (table, field)


def test_hosted_entries_do_not_list_columns_inline():
    """The marker and an inline list would be two sources that can disagree."""
    import yaml

    raw = yaml.safe_load(dd.DEFAULT_DESCRIPTIONS.read_text(encoding="utf-8"))["tables"]
    for table in dd.CONTRACT_TABLES:
        assert raw[table].get("columns_from") == dd.COLUMNS_FROM_SPICY_DOCS, table
        assert "columns" not in raw[table], f"{table} lists columns inline as well as by marker"


def test_an_inline_column_list_beside_the_marker_is_refused(tmp_path):
    """Declaring both must fail loudly rather than silently preferring one."""
    import yaml

    path = tmp_path / "descriptions.yaml"
    path.write_text(
        yaml.safe_dump(
            {"tables": {"members": {"columns_from": "spicy_docs", "columns": {"bioguide_id": "x"}}}},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="columns_from"):
        dd.load_descriptions(path)


def test_hosted_schemas_follow_the_contract_types_in_contract_order():
    """VARCHAR unless the contract types a column (decision 67), spelled as DuckDB describes the file."""
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.contract_types import DESCRIBED

    for table, columns in dd.contract_schemas().items():
        contract = TABLE_CONTRACTS[table]
        assert columns == [(c, DESCRIBED[contract.column_type(c)]) for c in contract.columns], table
    typed = dict(dd.contract_schemas()["document_attributes"])
    assert (typed["page_count"], typed["receive_date"], typed["topics"]) == (
        "INTEGER", "TIMESTAMP WITH TIME ZONE", "VARCHAR[]")


# --------------------------------------------------------------------------- #
# The materialized rulemaking dataset: builders' own schemas, served from the snapshot pointer.
# --------------------------------------------------------------------------- #
RULEMAKING_IDENTITY = {
    "rule_targets": ["rule_target_id"],
    "proceedings": ["proceeding_id"],
    "regulatory_agenda_items": ["agenda_item_id"],
    "agenda_item_proceedings": ["relationship_id"],
    "comment_periods": ["comment_period_id"],
    "rulemaking_lifecycles": ["proceeding_id"],
    "lifecycle_events": ["lifecycle_event_id"],
    "agency_lifecycle_stats": ["agency_code", "stratum"],
}


def test_each_rulemaking_table_names_its_identity_and_where_it_is_published():
    assert tuple(RULEMAKING_IDENTITY) == dd.RULEMAKING_TABLES
    metadata = json.loads(dd.DEFAULT_MCP_METADATA_PATH.read_text(encoding="utf-8"))
    for table, identity in RULEMAKING_IDENTITY.items():
        entry = metadata[table]
        assert (entry["identity_columns"], entry["kind"]) == (identity, "derived"), table
        assert entry["grain"] and entry["label"] and entry["measured_on"], table
        page = (dd.DEFAULT_DOCS_TABLES_DIR / f"{table}.md").read_text(encoding="utf-8")
        assert "in the snapshot that `materialized/rulemaking/latest.json` names" in page, table
        assert "**MCP `query_sql` support:** Configured" in page, table


def test_a_pointer_that_names_no_publication_is_unreadable_not_drift(monkeypatch, dictionary_publication):
    from spicy_regs.sources.publication import PublicationError

    def refuse(*_, **_kwargs):
        raise PublicationError("Invalid rulemaking snapshot")

    monkeypatch.setattr(dd, "discover_schemas", refuse)
    args = dd.build_parser().parse_args(["check", "--source", "r2"])
    assert args.func(args) == dd.EXIT_SOURCE_UNREACHABLE


def test_live_check_holds_declared_types_to_the_published_file(monkeypatch, dictionary_publication, capsys):
    """A DATE column published as VARCHAR keeps its name, so only the type comparison can see it."""
    declared = dd.expected_schemas()["rulemaking_lifecycles"]
    drifted = [(column, "VARCHAR" if column == "proposal_date" else kind) for column, kind in declared]
    args = dd.build_parser().parse_args(["check", "--source", "r2"])
    monkeypatch.setattr(dd, "discover_schemas", lambda *_, **_kwargs: {"rulemaking_lifecycles": drifted})
    assert args.func(args) == 1
    assert "[rulemaking_lifecycles.proposal_date] in-code type DATE but live parquet VARCHAR" in capsys.readouterr().err
    monkeypatch.setattr(dd, "discover_schemas", lambda *_, **_kwargs: {"rulemaking_lifecycles": declared})
    assert args.func(args) == 0


def test_native_list_names_are_respelled_within_their_own_table():
    assert dd.native_spelling('committees_json', 'house_communications') == 'committees'
    assert dd.native_spelling('committees_json', 'nominations') == 'committees_json'


@pytest.mark.parametrize("receipts", [False, True, None])
def test_selected_dictionary_uses_explicit_receiptless_owner_only(receipts):
    entry: dict = {"tables": {"bill_subjects.parquet": {}}}
    if receipts is not False:
        entry["etlReceipts"] = {"datasets": ["bill_subjects"]} if receipts else None
    index = {"families": {"bill-subjects": entry}}
    descriptions, schemas = dd.selected_dictionary_definitions(index, ["bill_subjects"], dd.DEFAULT_DESCRIPTIONS)
    raw = receipts is False
    assert schemas["bill_subjects"] == dd.expected_schemas(native=not raw)["bill_subjects"]
    assert descriptions["bill_subjects"]["columns"] == dd.load_descriptions(native=not raw)["bill_subjects"]["columns"]
    assert ("subjects_json" in dict(schemas["bill_subjects"])) == raw


def test_unknown_selected_owner_keeps_strict_native_declarations():
    descriptions, schemas = dd.selected_dictionary_definitions({"families": {}}, ["bill_subjects"], dd.DEFAULT_DESCRIPTIONS)
    assert schemas == dd.expected_schemas()
    assert descriptions == dd.load_descriptions()


def test_live_selected_legacy_check_accepts_declared_shape_but_refuses_real_drift(monkeypatch, capsys):
    from spicy_regs.sources import publication

    index = {"families": {"bill-subjects": {"tables": {"bill_subjects.parquet": {}}}}}
    monkeypatch.setattr(publication, "load_index", lambda _: index)
    monkeypatch.setattr(dd, "published_row_counts", lambda *_, **_kwargs: {})
    declared = dd.expected_schemas(native=False)["bill_subjects"]
    monkeypatch.setattr(dd, "discover_schemas", lambda *_, **_kwargs: {"bill_subjects": declared})
    args = dd.build_parser().parse_args(["check", "--source", "r2"])
    assert args.func(args) == 0
    capsys.readouterr()
    drift = [(name, "BIGINT" if name == "subjects_json" else kind) for name, kind in declared]
    monkeypatch.setattr(dd, "discover_schemas", lambda *_, **_kwargs: {"bill_subjects": drift})
    assert args.func(args) == 1
    assert "[bill_subjects.subjects_json] in-code type VARCHAR but live parquet BIGINT" in capsys.readouterr().err


def test_live_dictionary_changed_selection_is_incomplete(monkeypatch, capsys):
    from spicy_regs.sources import publication

    indexes = iter([{"families": {}}, {"families": {"changed": {"tables": {}}}}])
    monkeypatch.setattr(publication, "load_index", lambda _: next(indexes))
    monkeypatch.setattr(dd, "discover_schemas", lambda *_, **_kwargs: {"dockets": dd.expected_schemas()["dockets"]})
    args = dd.build_parser().parse_args(["check", "--source", "r2"])
    assert args.func(args) == dd.EXIT_SOURCE_UNREACHABLE
    assert "Publication selection changed" in capsys.readouterr().err



def test_published_urls_use_captured_index_without_reloading(monkeypatch):
    from spicy_regs.sources import publication

    def unexpected(*args, **kwargs):
        pytest.fail("Captured index must select URLs without loading another index")

    monkeypatch.setattr(publication, "load_index", unexpected)
    monkeypatch.setattr(publication, "load_rulemaking_snapshot", lambda _: None)
    assert publication.published_urls("https://example.test", index=publication.empty_index()) == {}


def test_live_checker_passes_captured_selection_to_discovery(monkeypatch, dictionary_publication):
    from spicy_regs.sources import publication

    selected = publication.empty_index()
    monkeypatch.setattr(publication, "load_index", lambda _: selected)

    def discover(source, base, *, index):
        assert source == "r2" and index is selected
        return {"dockets": dd.expected_schemas()["dockets"]}

    monkeypatch.setattr(dd, "discover_schemas", discover)
    args = dd.build_parser().parse_args(["check", "--source", "r2"])
    assert args.func(args) == 0


def test_retained_court_legacy_declarations_preserve_native_and_refuse_drift(monkeypatch, capsys):
    from copy import deepcopy
    from pathlib import Path
    from spicy_regs.sources import publication

    from spicy_regs.subject_catalog import retired_receipt_datasets

    fixture = json.loads((Path(__file__).parent / "fixtures" /
                          "court_selected_legacy_dictionary.json").read_text())
    index = fixture["index"]
    every = {table: [tuple(field) for field in fields] for table, fields in fixture["schemas"].items()}
    # A table whose receipts are retired (ss-vse3) publishes none, so a generation without them is its current
    # format, held to its installed schema: the citation tables' legacy shapes are no longer a selectable format.
    retired = set(every) & retired_receipt_datasets()
    assert retired == {"court_citations", "court_citation_map", "court_parentheticals"}
    selected = dd.selected_dictionary_definitions(index, every, dd.DEFAULT_DESCRIPTIONS)[1]
    assert {table: selected[table] for table in retired} == {table: dd.expected_schemas()[table] for table in retired}
    live = {table: fields for table, fields in every.items() if table not in retired}
    native = {table: dd.expected_schemas()[table] for table in live}
    monkeypatch.setattr(publication, "load_index", lambda _: index)
    monkeypatch.setattr(dd, "published_row_counts", lambda *_, **_kwargs: {})
    monkeypatch.setattr(dd, "discover_schemas", lambda *_, **_kwargs: live)
    args = dd.build_parser().parse_args(["check", "--source", "r2"])
    assert args.func(args) == 0
    capsys.readouterr()
    selected = dd.selected_dictionary_definitions(index, live, dd.DEFAULT_DESCRIPTIONS)[1]
    assert {table: selected[table] for table in live} == live
    assert all("raw_source_record" not in dict(fields) for fields in live.values())

    live["court_opinion_clusters"].append(("unexpected_publisher_field", "VARCHAR"))
    assert args.func(args) == 1
    assert "unexpected_publisher_field" in capsys.readouterr().err
    live["court_opinion_clusters"].pop()
    live["court_opinion_clusters"][0] = ("cluster_id", "BIGINT")
    assert args.func(args) == 1
    assert "in-code type VARCHAR but live parquet BIGINT" in capsys.readouterr().err

    for routed in ({"families": {}}, deepcopy(index)):
        for owner in routed["families"].values():
            owner["etlReceipts"] = {"datasets": []}
        schemas = dd.selected_dictionary_definitions(routed, live, dd.DEFAULT_DESCRIPTIONS)[1]
        assert {table: schemas[table] for table in live} == native
        assert dd.check_schema_drift({table: schemas[table] for table in live}, live)
    assert {table: dd.expected_schemas()[table] for table in live} == native


def test_retained_fec_historical_definitions_keep_native_and_strict_drift():
    from copy import deepcopy
    from pathlib import Path

    fixture = json.loads((Path(__file__).parent / "fixtures" /
                          "fec_selected_legacy_dictionary.json").read_text())
    index = fixture["index"]
    live = {key.removesuffix(".parquet"): [tuple(field) for field in table["columns"]]
            for family in index["families"].values() for key, table in family["tables"].items()}
    native_schemas, native_prose = dd.expected_schemas(), dd.load_descriptions()
    descriptions, schemas = dd.selected_dictionary_definitions(index, live, dd.DEFAULT_DESCRIPTIONS)
    assert {table: schemas[table] for table in live} == live
    assert dd.check_descriptions(live, {table: descriptions[table] for table in live}) == []
    assert dd.check_schema_drift({table: schemas[table] for table in live}, live) == []

    drifted = deepcopy(live)
    drifted["fec_collections"][0] = ("collection_id", "BIGINT")
    drifted["fec_collections"].append(("unknown_version_field", "VARCHAR"))
    drifted["fec_collections"].remove(("record_count", "VARCHAR"))
    errors = dd.check_schema_drift({table: schemas[table] for table in live}, drifted)
    assert any("in-code type VARCHAR but live parquet BIGINT" in error for error in errors)
    assert any("unknown_version_field" in error for error in errors)
    assert any("record_count" in error for error in errors)

    for receipts in ({}, None):
        routed = deepcopy(index)
        for owner in routed["families"].values():
            owner["etlReceipts"] = receipts
        prose, selected = dd.selected_dictionary_definitions(routed, live, dd.DEFAULT_DESCRIPTIONS)
        assert selected == native_schemas
        assert prose == native_prose
        # Retired tables (ss-j4vz B10) have no native declaration; only a receipt-free owner reads them.
        # Retired (B10, Q2) or renamed (Q1) since that generation (ss-j4vz).
        retired = {"fec_research_document_observations", "fec_retained_csv_observations",
                   "fec_research_response_outcomes", "fec_research_context_dispositions",
                   "fec_research_filing_feed_items", "fec_research_source_pages"}
        assert live.keys() - selected.keys() == retired
        current = {table: columns for table, columns in live.items() if table not in retired}
        assert dd.check_schema_drift({table: selected[table] for table in current}, current)
    prose, selected = dd.selected_dictionary_definitions({"families": {}}, live, dd.DEFAULT_DESCRIPTIONS)
    assert selected == native_schemas and prose == native_prose
