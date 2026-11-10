#!/usr/bin/env python3
"""Generate and validate the Spicy Regs data dictionary.

The data dictionary has two layers:

* **Schema (source of truth, in code).** Column names and types come from
  :data:`spicy_regs.schemas.regulations.RECORD_TYPES` for the core tables, the
  spicy-docs contracts for the hosted tables, a builder's own column list or
  Arrow schema where it has one, and :data:`DERIVED_SCHEMAS` below for the
  rollups whose SQL is their only declaration. This keeps generation
  deterministic and offline.
* **Descriptions (curated prose).** Human descriptions live in
  ``data_dictionary/descriptions.yaml``, keyed by table and column.

``spicy-regs-dict check`` reconciles the two so they can't silently drift, and
``spicy-regs-dict generate`` renders one Markdown page per table for the MkDocs
site under ``docs/tables/``. ``generate`` also bundles the output ledger's
per-table audits (``output_ledger``), the declared cross-table joins
(``table_joins``) and what each table's receipts carry (``receipt_field_declarations``)
for the MCP server, and ``check`` refuses a stale copy of any.

Usage::

    uv run spicy-regs-dict check                 # offline: descriptions vs in-code schema
    uv run spicy-regs-dict check --source r2     # also reconcile in-code schema vs live R2 parquet
    uv run spicy-regs-dict generate              # (re)write docs/tables/*.md
"""

from __future__ import annotations

from spicy_regs.subject_catalog import subject_tables, policies as subject_policies
from spicy_regs.disbursement_summary_tables import TABLES as DISBURSEMENT_SUMMARY_TABLES

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Iterable
from datetime import date
from functools import lru_cache
import tempfile
from pathlib import Path

import duckdb
from dotenv import load_dotenv

from spicy_regs import output_ledger, table_joins
from spicy_regs.dictionary_resources import INPUTS as DICTIONARY_INPUTS
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.schemas.regulations import COMMENT_MIRROR_COLUMNS, DOCUMENT_FAMILY_COLUMNS, RECORD_TYPES
from spicy_regs.sources.publication import SNAPSHOT_POINTER, PublicationError, parquet_scan

# Repo layout anchors (this file lives at src/spicy_regs/data_dictionary.py).
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DESCRIPTIONS = DICTIONARY_INPUTS / "descriptions.yaml"
DEFAULT_DOCS_TABLES_DIR = REPO_ROOT / "docs" / "tables"
DEFAULT_CATALOG_PATH = REPO_ROOT / "data_dictionary" / "catalog.json"
DEFAULT_CATALOG_DIGEST_PATH = REPO_ROOT / "data_dictionary" / "catalog.json.sha256"
DEFAULT_MCP_METADATA_PATH = Path(__file__).with_name("table_metadata.json")

#: Bumped when the catalog document's shape changes, so a reader can refuse a
#: shape it does not know rather than guess at a missing field.
CATALOG_FORMAT_VERSION = 4

#: The six kinds a coverage statement can be, as machine-readable tokens,
#: keyed by the prose prefix so the two cannot disagree.
#:
#: "Sampled" is the weakest population claim: the table is filled from a
#: bounded slice of the publisher's archive chosen by a per-run cap, not by a
#: date window, so neither an end-to-end range nor a window describes it. The
#: bill-family and roll-call tables open this way until a full run has been
#: measured, because calling a capped first pass a "Window" would state a
#: density it does not have. A capped table whose cap has caught up is
#: re-kinded only after the output ledger records the publisher's declared
#: count beside what is held (house_communications, 2026-10-03).
#:
#: "Empty" is a table that is declared and published with no rows, by owner
#: decision or because its producer is not enabled. The 2026-10-02 blind test
#: found six such tables reading "Sampled" or "Not a range", so a count over
#: them read as a thin sample rather than as zero by construction.
COVERAGE_KINDS: dict[str, str] = {
    "True range": "true_range",
    "Window": "window",
    "Sampled": "sampled",
    "Derived": "derived",
    "Not a range": "not_a_range",
    "Empty": "empty",
}


#: What each table is about, so a reader who does not know table names can find the rulemaking tables
#: among the rest (owner decision 8, 2026-10-03: two personas could not, one of them a non-SQL user). One
#: word per table, stated in descriptions.yaml and carried into table_metadata.json; a relationship view
#: takes its base table's. GAO and CRS are oversight; SAM, USAspending, the President's budget and the
#: Senate's expenditure reports are spending; reference holds the legal texts other subjects cite (the CFR,
#: the U.S. Code and eCFR authority notes).
SUBJECTS: tuple[str, ...] = (
    "rulemaking", "congress", "campaign_finance", "lobbying", "courts", "oversight", "spending", "reference",
)


def coverage_kind(coverage: str) -> str | None:
    """Return the machine-readable kind for a coverage statement, or None.

    Matches on the prose prefix, not the first whitespace-delimited token: the
    token carries a trailing period or comma depending on the sentence
    ("Derived." and "Derived," both occur), so a consumer splitting on a space
    would match neither.
    """
    text = coverage.strip()
    for prefix, kind in COVERAGE_KINDS.items():
        if text.startswith(prefix):
            return kind
    return None


#: What coverage prose may not state, each named by what it is. Coverage prose
#: is pinned to no publication, so a count, snapshot id or pin written there
#: goes stale at the next publish and nothing notices: the 2026-09-28 blind
#: persona test found 13 of 56 stated row counts off by 1.5x to 30x, every
#: rulemaking note naming a replaced snapshot, and 25 notes calling live tables
#: unpublished. The 2026-10-02 test found the same smell one noun over: counts
#: in units the row-count rule did not name (registrations, documents, a
#: per-year rate), and 19 statements restating their own ``measured_on`` inside
#: the sentence, which readers took for the live end bound of a table rebuilt
#: daily. The live row count is describe_table's publication field, and a
#: measurement tied to a pin belongs in the output ledger, which names its pin.
COVERAGE_PROSE_REFUSALS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("a snapshot id", re.compile(r"snapshot_[0-9a-f]{6,}")),
    ("a digest or generation pin", re.compile(r"\b[0-9a-f]{16,}\b|\b[0-9a-f]{8,}…")),
    ("a publication-status claim", re.compile(r"not uploaded|not yet published", re.IGNORECASE)),
    ("a row count", re.compile(r"\d[\d,]*(?:\s+[A-Za-z-]+){0,2}\s+(?:rows|records)\b", re.IGNORECASE)),
    # A thousands-separated number in any other unit. A row count is left to the
    # rule above so it is reported once, and a partial number inside a longer one
    # (the "1,009" of "1,009,005") is not a match.
    ("a count", re.compile(
        r"(?<![\d,])\b\d{1,3}(?:,\d{3})+\b(?!,\d)(?!(?:\s+[A-Za-z-]+){0,2}\s+(?:rows|records)\b)", re.IGNORECASE
    )),
)

#: What these rules still cannot see: a number spelled with a suffix or in words
#: ("26.6M", "~20.7K", "two-thirds"), a share ("87%", "0.08%"), a count under
#: a thousand ("43 laws"), a dollar threshold ("over $1,000" has a comma and is
#: refused; "over $900" is not) and a date other than the entry's own
#: measured_on. Those are read by a person at review, not by this lint.

#: Exact coverage phrases allowed past COVERAGE_PROSE_REFUSALS, by table, each
#: with its reason. An exception whose phrase is no longer in the prose is an
#: error, so a fixed note cannot leave a stale permission behind. A per-run cap
#: or a cited threshold is a code constant quoted as a rule, not a measurement,
#: which is the one shape a count may take here.
COVERAGE_PROSE_EXCEPTIONS: dict[str, dict[str, str]] = {
    "bill_subjects": {"up to 2,000 a run": "the API request cap per run (build_bill_subjects), a rule"},
    "committee_meetings": {"at most 1,000 a run": "MAX_DETAILS_PER_RUN (build_congress_index), a rule"},
    "record_issues": {"up to 1,000 a run": "MAX_DETAILS_PER_RUN (build_congress_index), a rule"},
    "committees": {"Since 2026-09-26": "the day the walk scope changed (committee-fixes-2026-09-26), not a measurement"},
}


def _measurement_date_rule(measured_on: str) -> tuple[str, re.Pattern[str]]:
    """The entry's own ``measured_on`` as a standalone date; inside a receipt name it is a citation."""
    return (
        "the measured_on date it was measured on",
        re.compile(rf"(?<![\w/-]){re.escape(measured_on)}(?![\w/-])"),
    )


def coverage_prose_errors(table: str, coverage: str, measured_on: str = "") -> list[str]:
    """Refuse decaying facts in one table's coverage prose, less its declared exceptions.

    ``measured_on`` is the entry's own measurement date: restated in the
    sentence it reads as the live end bound, so it is refused like a count.
    """
    errors = []
    text = " ".join(coverage.split())
    for phrase in COVERAGE_PROSE_EXCEPTIONS.get(table, {}):
        if phrase not in text:
            errors.append(f"[{table}] coverage exception no longer matches its prose: {phrase!r}")
        text = text.replace(phrase, " ")
    refusals = COVERAGE_PROSE_REFUSALS + ((_measurement_date_rule(measured_on),) if measured_on else ())
    for what, pattern in refusals:
        errors.extend(
            f"[{table}] coverage states {what} ({match.group(0)!r}); a coverage statement names no "
            "measurement: the server states live counts and pins, and a dated measurement belongs in the "
            "output ledger or the data_quality note"
            for match in pattern.finditer(text)
        )
    return errors


#: A data-quality note may state a measurement, but only with what it was
#: measured at: a date, a pin, a snapshot id, a receipt or a generation. A bare
#: count there is the same decaying fact as one in coverage ("33,373 rows carry
#: a posted_date before 1990", on a table rebuilt daily, 2026-10-02).
_DATA_QUALITY_ANCHOR = re.compile(
    r"\b\d{4}-\d{2}-\d{2}\b|\b[0-9a-f]{8}…|snapshot_[0-9a-f]{6,}|\breceipt|\bgeneration", re.IGNORECASE
)
_DATA_QUALITY_RULES = {what: pattern for what, pattern in COVERAGE_PROSE_REFUSALS}


def data_quality_prose_errors(table: str, data_quality: str) -> list[str]:
    """Refuse a publication claim, and a count no date, pin, snapshot, receipt or generation anchors."""
    text = " ".join(data_quality.split())
    errors = [
        f"[{table}] data_quality states a publication-status claim ({match.group(0)!r}); publication is the "
        "server's fact"
        for match in _DATA_QUALITY_RULES["a publication-status claim"].finditer(text)
    ]
    if not _DATA_QUALITY_ANCHOR.search(text):
        errors.extend(
            f"[{table}] data_quality states {what} ({match.group(0)!r}) with no date, pin, snapshot, receipt or "
            "generation it was measured at"
            for what in ("a row count", "a count")
            for match in _DATA_QUALITY_RULES[what].finditer(text)
        )
    return errors


def _prose_sentences(entry: dict | None) -> list[tuple[str, str]]:
    """``(field, sentence)`` for each sentence of one entry's prose; ``field`` as ``pinned_sentences`` names it."""
    fields = {name: (entry or {}).get(name) for name in ("summary", "coverage", "data_quality")}
    fields |= {f"columns.{name}": text for name, text in ((entry or {}).get("columns") or {}).items()}
    return [
        (field, sentence)
        for field, text in fields.items()
        for sentence in re.split(r"(?<=\.)\s+", " ".join(str(text or "").split()))
    ]


def pinned_sentences(tables: dict) -> list[tuple[str, str, str, str]]:
    """``(table, field, pin, sentence)`` for each sentence of the dictionary prose that names a pin.

    A pin is what COVERAGE_PROSE_REFUSALS calls a snapshot id or a digest or generation pin, cut to the
    form the live index states (``snapshot_`` and 8 hex digits, or 8 hex digits), so the release-time
    prose pass (``scripts/check_ledger_pins.py``) can say which no longer name the live data. ``field`` is
    ``summary``, ``coverage``, ``data_quality`` or ``columns.<name>``; pass the file's own entries, not
    ``load_descriptions``', so a contract sentence this repository cannot edit is not listed.
    """
    found = []
    for table, entry in tables.items():
        for field, sentence in _prose_sentences(entry):
            for what in ("a snapshot id", "a digest or generation pin"):
                for match in _DATA_QUALITY_RULES[what].finditer(sentence):
                    pin = match.group(0).rstrip("…")
                    pin = pin[: len("snapshot_") + 8] if pin.startswith("snapshot_") else pin[:8]
                    found.append((table, field, pin, sentence))
    return found


#: A sentence that sends a reader to a table's ETL receipt. Owner decision, 2026-10-05: one page explains
#: receipts (``data_dictionary/etl_receipts.md``, rendered to ``docs/tables/etl_receipts.md`` and served by
#: ``describe_table('etl_receipts')``), and a table mentions them only where its receipt holds something a reader
#: wants (a source link, a column people queried), in one sentence, and not at all before the table has
#: published in the native layout. That day every summary of a table with a field policy ended with one
#: paragraph of pipeline vocabulary, most of them on tables with no receipts yet, and 22 FEC summaries written
#: the same morning said a value "is kept in the receipt" of a family that had published none.
#:
#: The pattern reads the receipt table's name, "ETL receipt", a table's name before "receipts", and a receipt
#: said to keep or hold something. It cannot see a pointer phrased another way ("the processing record has it"),
#: and it does not judge whether what the receipt holds is wanted: both are read at review. "receipt" alone is
#: not matched, because FEC prose uses it for money a committee received and the notes cite maintainers'
#: evidence folders as receipts (59 such sentences on 2026-10-05, none of them read as a pointer). A view's
#: summary is a table note too, but it is declared in ``relationship_views`` and the dictionary check does not
#: read it: ``tests/test_data_dictionary.py`` holds the views to the one-sentence half.
RECEIPT_POINTER = re.compile(
    r"etl_receipts|\bETL receipts?\b|\b[a-z0-9]+(?:_[a-z0-9]+)+ receipts\b"
    r"|\b(?:kept|held|stored|retained|recorded|lives?|remains?|is|are) in (?:(?:the|its|their|each|a) )?"
    r"(?:[\w']+ ){0,2}?receipts?\b(?! (?:date|schedule|record|notice|line)s?\b)"
    r"|\b(?:the|its|their|each) (?:[\w']+ )?receipts? (?:keeps?|holds?|carr(?:y|ies))\b",
    re.IGNORECASE,
)


def receipt_pointers(entry: dict | None) -> list[str]:
    """The field of each sentence in one table's prose that RECEIPT_POINTER reads as a pointer to its receipt."""
    return [field for field, sentence in _prose_sentences(entry) if RECEIPT_POINTER.search(sentence)]


def receipt_pointer_errors(table: str, entry: dict | None) -> list[str]:
    """Refuse a table that points readers to its receipt in more than one sentence."""
    fields = receipt_pointers(entry)
    if len(fields) <= 1:
        return []
    return [
        f"[{table}] points readers to its receipt in {len(fields)} sentences ({', '.join(fields)}); a table says "
        "what its receipt holds in one sentence, and etl_receipts explains the rest"
    ]


def published_receipt_datasets(index: dict) -> set[str]:
    """Every dataset the publication index shows published with receipts: the tables that have converted."""
    families = index["families"].values()
    return {name for family in families for name in family.get("etlReceipts", {}).get("datasets", ())}


def receipt_index_errors(descriptions: dict, published: Iterable[str], with_receipts: set[str]) -> list[str]:
    """Refuse a pointer to a receipt on a table the publication index shows published without receipts.

    Whether a table has converted is the index's fact and no declaration's: a field policy says only that the
    table publishes natively at its next run. So this half of the rule has no offline form, as
    ``kind_index_errors`` has none for a row count. ``published`` names the tables the publisher holds
    (:func:`published_row_counts`) and ``with_receipts`` is :func:`published_receipt_datasets`. Like the kind, it
    follows actual publication: it cannot see a pointer on a table this publisher does not hold at all.
    """
    return [
        f"[{table}] {fields[0]} points readers to its receipt, but the publication index shows this table "
        "published without receipts; remove the sentence until the table has published in the native layout"
        for table in sorted(set(published) - with_receipts)
        if (fields := receipt_pointers(descriptions.get(table)))
    ]


#: A data-quality note that stands in for a fix in the vendored spicy-docs wheel ends "(interim until
#: spicy-docs > <version>)". spicy-regs cannot override one contract column's sentence, so such a note
#: corrects it from the table level, and without an expiry it outlives the release that fixes the sentence
#: (round 4, 2026-10-03: three contract sentences the data contradicted, and the margin-note reader, were
#: fixed only on an unreleased spicy-docs branch). Once the installed wheel is past the stated version the
#: check refuses the note: delete it if the wheel fixed what it covers, or restate the marker if not.
INTERIM_MARKER = re.compile(r"\(interim until spicy-docs > ([^\s()]+)\)")
_INTERIM_WORDS = re.compile(r"\binterim until\b", re.IGNORECASE)


def installed_spicy_docs() -> str:
    """The spicy-docs version this environment installed, which ``uv run --frozen`` takes from the lock."""
    from importlib.metadata import version

    return version("spicy-docs")


def _wheel_past(installed: str, stated: str) -> bool:
    """Whether ``installed`` is a later spicy-docs than ``stated``.

    A local label names a branch build, not an order (``+laws.…`` and ``+billslane.…`` do not compare), so
    another local build of the same release counts as later, and a plain release does not.
    """
    from packaging.version import Version

    have, want = Version(installed), Version(stated)
    if have.public != want.public:
        return Version(have.public) > Version(want.public)
    return have.local is not None and have.local != want.local


def interim_note_errors(table: str, data_quality: str, installed: str) -> list[str]:
    """Refuse an interim note the installed wheel has moved past, and one whose expiry cannot be read."""
    from packaging.version import InvalidVersion

    text = " ".join(data_quality.split())
    stated = INTERIM_MARKER.findall(text)
    errors = []
    if len(stated) != len(_INTERIM_WORDS.findall(text)):
        errors.append(f"[{table}] data_quality has an interim note whose expiry does not read "
                      "'(interim until spicy-docs > <version>)'")
    for version in stated:
        try:
            past = _wheel_past(installed, version)
        except InvalidVersion:
            errors.append(f"[{table}] data_quality interim note names no valid spicy-docs version ({version!r})")
            continue
        if past:
            errors.append(f"[{table}] data_quality interim note expired: spicy-docs {installed} is installed, past "
                          f"{version}; delete the note if the wheel fixed it, or restate its marker")
    return errors


#: The tables hosted from spicy-docs' table contracts. Their columns and their
#: per-column prose both come from ``spicy_docs.schemas.TABLE_CONTRACTS``, so a
#: column added upstream fails ``spicy-regs-dict check`` here until the release
#: is adopted — it is never restated in this repository. ``congress_bills`` is
#: in this list and is also the one table that predates it: the contract keeps
#: its first ten columns in their published order and appends the rest.
SCORECARD_TABLES: tuple[str, ...] = (
    "scorecard_publishers",
    "scorecards",
    "scorecard_snapshots",
    "scorecard_methodologies",
    "scorecard_metrics",
    "scorecard_items",
    "scorecard_metric_items",
    "scorecard_metric_components",
    "scorecard_members",
    "scorecard_member_ratings",
    "scorecard_member_item_results",
)


CONTRACT_TABLES: tuple[str, ...] = (
    *SCORECARD_TABLES,
    "congress_bills",
    "bill_actions",
    "bill_committees",
    "bill_committee_activities",
    "bill_cosponsors",
    "bill_publisher_summaries",
    "bill_versions",
    "bill_sections",
    "section_diffs",
    "section_diff_items",
    "financial_changes",
    "section_classifications",
    "bill_summaries",
    "diff_summaries",
    "public_activity_events",
    "amendments",
    "press_releases",
    "roll_call_votes",
    "member_votes",
    "members",
    "member_terms",
    "member_party_affiliations",
    "committee_reports",
    "committee_report_cbo_letters",
    "report_sections",
    "hearing_transcripts",
    "hearing_bill_links",
    "cbo_cost_estimates",
    # Every item of CBO's per-Congress feeds, a spicy-docs contract from 0.54.0; the bill family writes it.
    "cbo_feed_items",
    # GAO's legal decisions, a spicy-docs contract from 0.54.0 (DRY X1); the gao-reports rollup writes it.
    "gao_decisions",
    # A8/A9 (laws and rosters): the laws and committee-rosters rollups.
    "laws",
    "law_sections",
    "law_code_sections",
    "table3_records",
    "committees",
    "committee_assignments",
    "committee_directory_entries",
    # The Federal Register rollup (spicy-docs 0.42.0).
    "federal_register",
    # The FEC bulk committee master, every cycle (spicy-docs 0.43.0, decision 53).
    "fec_committee_history",
    # The FEC bulk candidate master, every cycle.
    "fec_candidate_history",
    # The Regulations.gov attributes the thin documents/dockets/comments lack, typed (decisions 65-67); the
    # regulations ETL writes them and the refresh's base families publish them. comment_attributes is seeded by
    # the comment re-read (fill-comment-fields attributes).
    "document_attributes",
    "docket_attributes",
    "comment_attributes",
    # The Congress.gov index tables (gaps A5, A7, A10), each written by its own
    # rollup in pipelines/rollups/congress_index.py.
    "house_communications",
    "committee_meetings",
    "record_issues",
    "treaties",
    "nominations",
    # The two PDF-only families (spicy-docs 0.23.0). ``document_citations`` is
    # the shared link table both GovInfo families write into, with one owner:
    # the print-citations rollup.
    "house_activity_reports",
    "budget_volumes",
    "bill_committee_actions",
    "document_citations",
    "senate_expenditures",
    # The native legal-reference tables (spicy-docs 0.52.0), their columns, identities and reading the wheel's.
    "native_legal_references",
    "native_legal_reference_reads",
    # GAO's open-recommendations export, accumulated daily (spicy-docs 0.53.0).
    "gao_recommendations",
)


#: The materialized rulemaking dataset's public tables, in its stage order. They
#: publish under their own snapshot pointer (``publication.SNAPSHOT_POINTER``),
#: not the publication index. Listed literally; test_generation_mcp pins them to
#: the pipeline's ``published_outputs``.
RULEMAKING_TABLES: tuple[str, ...] = (
    "rule_targets",
    "proceedings",
    "regulatory_agenda_items",
    "agenda_item_proceedings",
    "comment_periods",
    "rulemaking_lifecycles",
    "lifecycle_events",
    "agency_lifecycle_stats",
)

# These are supported retained observation tables, not a publication claim.
# Exact Arrow union bytes and DuckDB types are qualified together in the checked
# schema resource; the MCP consumes generated table_metadata.json only.
FEC_TYPED_TABLES: tuple[str, ...] = (
    "fec_account_transfers",
    "fec_agency_mapping_dispositions",
    "fec_agency_report_documents",
    "fec_agency_report_text",
    "fec_agency_reports",
    "fec_allocated_disbursements",
    "fec_allocation_bases",
    "fec_api_response_controls",
    "fec_audit_findings",
    "fec_bundled_contributions",
    "fec_candidate_api_observations",
    "fec_collection_selection",
    "fec_committee_master_observations",
    "fec_committee_observations",
    "fec_communication_costs",
    "fec_contribution_aggregates",
    "fec_coordinated_party_expenditures",
    "fec_debts",
    "fec_disbursements",
    "fec_electioneering_communications",
    "fec_filing_definition_evidence",
    "fec_filing_definitions",
    "fec_filing_header_associations",
    "fec_filing_links",
    "fec_filing_report_observations",
    "fec_filing_text_observations",
    "fec_filings",
    "fec_historical_ie_statistics",
    "fec_inaugural_donations",
    "fec_independent_expenditures",
    "fec_intercommittee_transactions",
    "fec_legal_documents",
    "fec_legal_events",
    "fec_legal_matters",
    "fec_legal_parties",
    "fec_loan_guarantors",
    "fec_loan_terms",
    "fec_loans",
    "fec_lobbyist_registrations",
    "fec_oversight_recommendations",
    "fec_postgres_committee_history_observations",
    "fec_quality_notices",
    "fec_receipts",
    "fec_record_evidence",
    "fec_registration_statements",
    "fec_report_metrics",
    "fec_reported_financial_summaries",
    "fec_reference_filing_feed_items",
    "fec_research_meeting_observations",
    "fec_reference_source_pages",
)


# Display order for the dictionary. The first three are the core record types;
# the rest are derived rollups. This is the full public R2 surface.
TABLES: tuple[str, ...] = (
    "dockets",
    "documents",
    "comments",
    "comments_index",
    "feed_summary",
    "agency_stats",
    "agency_monthly_volume",
    "fr_docket_links",
    "discovery_signals",
    "cfr_sections",
    "congress_bills",
    "bill_subjects",
    "unified_agenda",
    *RULEMAKING_TABLES,
    "sam_entities",
    "lobbying_filings",
    "lobbying_activities",
    "lobbying_activity_lobbyists",
    "lobbying_foreign_entities",
    "lobbying_affiliated_organizations",
    "lobbying_conviction_disclosures",
    "lobbying_registrants",
    "lobbying_clients",
    "fec_committees",
    "fec_source_catalog",
    "fec_collections",
    "fec_source_records",
    "fec_relationships",
    *FEC_TYPED_TABLES,
    "org_committee_links",
    "gao_reports",
    "crs_reports",
    "court_dockets",
    "court_docket_groups",
    "court_opinion_clusters",
    "court_citations",
    "court_citation_map",
    "court_parentheticals",
    "court_opinions",
    "court_opinion_pdf_extractions",
    "member_vote_terms",
    "scorecard_member_links",
    "scorecard_item_links",
    "scorecard_action_comparisons",
    "usaspending_recipients",
    "cms_part_d_drug_spending",
    "cms_medicaid_drug_spending",
    "cms_part_b_drug_spending",
    "grants_opportunity_observations",
    "ofac_entity_observations",
    "ofac_snapshots",
    "executive_order_observations",
    "house_disbursements",
    "house_disbursements_summary",
    "house_disbursement_pdf_rows",
    "house_disbursement_pdf_pages",
    "senate_disbursements",
    "senate_disbursement_pages",
    *DISBURSEMENT_SUMMARY_TABLES,
    "fec_financial_profiles",
    "treasury_debt_observations",
    "treasury_statement_periods",
    "treasury_statement_classification_cells",
    "census_acs_county_observations",
    "fcc_proceedings",
    "fcc_filings",
    # The hosted tables, minus congress_bills — it predates them and keeps its
    # position above, so appending CONTRACT_TABLES wholesale would list it twice.
    *(name for name in CONTRACT_TABLES if name != "congress_bills"),
    # Published, but not contract tables: the bill-family rollup's own
    # processing state, and the vote references the roll-call rollup joins
    # against. Their columns are this repository's, so expected_schemas()
    # takes them from the transform and their prose is inline in descriptions.yaml.
    "bill_family_archives",
    "bill_vote_references",
    "bill_family_backfills",
    "bill_family_backfill_walks",
    "committee_report_reads",
    "document_citation_reads",
)


LEGACY_TABLES = TABLES
TABLES = subject_tables(TABLES)

# Tables the MCP server (list_sources / describe_table / query_sql) exposes.
# This must equal spicy_regs.mcp_server.TABLES; a test enforces it so the docs'
# "queryable via MCP" flag can't drift from what the server actually serves.
MCP_QUERYABLE: frozenset[str] = frozenset(
    {
        "court_opinion_pdf_extractions",
        "native_legal_references",
        "native_legal_reference_reads",
        "dockets",
        "documents",
        "comments",
        "comments_index",
        "feed_summary",
        "agency_stats",
        "agency_monthly_volume",
        "fr_docket_links",
        "discovery_signals",
        "cfr_sections",
        "congress_bills",
        "bill_subjects",
        "unified_agenda",
        "federal_register",
        "sam_entities",
        "lobbying_filings",
        "lobbying_activities",
        "lobbying_activity_lobbyists",
        "lobbying_foreign_entities",
        "lobbying_affiliated_organizations",
        "lobbying_conviction_disclosures",
        "lobbying_registrants",
        "lobbying_clients",
        "fec_committees",
        "fec_source_catalog",
        "fec_collections",
        "fec_source_records",
        "fec_relationships",
        *FEC_TYPED_TABLES,
        "org_committee_links",
        "gao_reports",
        "crs_reports",
        "court_dockets",
        "court_docket_groups",
        "court_opinion_clusters",
        "court_citations",
        "court_citation_map",
        "court_parentheticals",
        "court_opinions",
        "usaspending_recipients",
        "cms_part_d_drug_spending",
        "cms_medicaid_drug_spending",
        "cms_part_b_drug_spending",
        "grants_opportunity_observations",
        "ofac_entity_observations",
        "ofac_snapshots",
        "executive_order_observations",
        "house_disbursements",
        "house_disbursements_summary",
        "house_disbursement_pdf_rows",
        "house_disbursement_pdf_pages",
        "senate_disbursements",
        "senate_disbursement_pages",
        *DISBURSEMENT_SUMMARY_TABLES,
        "fec_financial_profiles",
        "treasury_debt_observations",
        "treasury_statement_periods",
        "treasury_statement_classification_cells",
        "census_acs_county_observations",
        "fcc_proceedings",
        "fcc_filings",
        "bill_family_archives",
        "bill_vote_references",
        "bill_family_backfills",
        "bill_family_backfill_walks",
        "committee_report_reads",
        "document_citation_reads",
        "member_vote_terms",
        "scorecard_member_links",
        "scorecard_item_links",
        "scorecard_action_comparisons",
        *RULEMAKING_TABLES,
        *CONTRACT_TABLES,
    }
)
MCP_QUERYABLE = frozenset(subject_tables(MCP_QUERYABLE))


# Schemas for the derived rollups whose SQL is their only declaration: they mirror
# src/spicy_regs/transforms/{build_feed_summary,build_agency_rollups,build_fr_docket_links,
# build_discovery_signals}.py and sources/iceberg.py (_build_comments_index). A table whose
# transform declares its columns is read from that declaration in expected_schemas(), never
# restated here. Types are DuckDB type names, matching what a DESCRIBE of the published
# parquet returns (see `check --source r2`).
DERIVED_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "comments_index": [
        ("agency_code", "VARCHAR"),
        ("docket_id", "VARCHAR"),
        ("year", "BIGINT"),
        ("month", "BIGINT"),
        ("row_count", "BIGINT"),
    ],
    "feed_summary": [
        ("docket_id", "VARCHAR"),
        ("agency_code", "VARCHAR"),
        ("title", "VARCHAR"),
        ("docket_type", "VARCHAR"),
        ("modify_date", "VARCHAR"),
        ("abstract", "VARCHAR"),
        ("comment_count", "BIGINT"),
        ("comment_end_date", "VARCHAR"),
        ("date_created", "VARCHAR"),
    ],
    "agency_stats": [
        ("agency_code", "VARCHAR"),
        ("docket_count", "BIGINT"),
        ("document_count", "BIGINT"),
        ("comment_count", "BIGINT"),
    ],
    "agency_monthly_volume": [
        ("agency_code", "VARCHAR"),
        ("year", "BIGINT"),
        ("month", "BIGINT"),
        ("document_type", "VARCHAR"),
        ("document_count", "BIGINT"),
    ],
    # Built by build_fr_docket_links: one row per document–docket pair the Register prints in
    # docket_ids_json or states in its regulations.gov link, carrying FR display columns.
    "fr_docket_links": [
        ("docket_id", "VARCHAR"),
        ("docket_source_ordinal", "BIGINT"),
        ("normalized_docket_candidates_json", "VARCHAR"),
        ("docket_normalization_rule", "VARCHAR"),
        ("document_number", "VARCHAR"),
        ("title", "VARCHAR"),
        ("abstract", "VARCHAR"),
        ("document_type", "VARCHAR"),
        ("subtype", "VARCHAR"),
        ("publication_date", "VARCHAR"),
        ("effective_on", "VARCHAR"),
        ("comments_close_on", "VARCHAR"),
        ("signing_date", "VARCHAR"),
        ("agency_slugs", "VARCHAR"),
        ("docket_ids_json", "VARCHAR"),
        ("regulation_id_numbers_json", "VARCHAR"),
        ("html_url", "VARCHAR"),
        ("pdf_url", "VARCHAR"),
        ("executive_order_number", "VARCHAR"),
        ("link_source", "VARCHAR"),
    ],
    # Built by build_discovery_signals from documents.parquet; CURRENT_DATE-relative,
    # so rows change on every rebuild. One row per spiking agency.
    "discovery_signals": [
        ("agency_code", "VARCHAR"),
        ("recent_30d", "BIGINT"),
        ("baseline", "DOUBLE"),
        ("ratio", "DOUBLE"),
    ],
}

@lru_cache(maxsize=1)
def _contracts() -> dict:
    """``TABLE_CONTRACTS`` from the installed spicy-docs wheel.

    Imported lazily, not at module import, so the base CLI and MCP installs
    keep working without the ``source-readers`` group — ``vendor/README.md``
    states that they do not require it. The dictionary commands do: the column
    tuples and the per-column prose for the hosted tables are the wheel's, and
    generating them from anything else would be this repository restating a
    contract it does not own.
    """
    try:
        from spicy_docs.schemas import TABLE_CONTRACTS
    except ModuleNotFoundError as exc:  # pragma: no cover - install-shape guard
        raise ModuleNotFoundError(
            "The data dictionary reads the hosted tables' columns and prose from "
            "spicy-docs. Install the source-readers group: uv sync --frozen"
        ) from exc
    return dict(TABLE_CONTRACTS)


def contract_schemas() -> dict[str, list[tuple[str, str]]]:
    """``{table: [(column, DuckDB type), ...]}`` for every contract-hosted table.

    The type is the contract's (VARCHAR unless it states one, as the Regulations.gov attribute tables do,
    decision 67), spelled as DuckDB describes the written Parquet (``contract_types.DESCRIBED``).
    """
    from spicy_regs.contract_types import described_columns

    contracts = _contracts()
    return {name: described_columns(contracts[name]) for name in CONTRACT_TABLES}


def contract_column_prose(table: str) -> dict[str, str]:
    """Source column prose, including this repository's added Record enrichment."""
    descriptions = dict(_contracts()[table].descriptions)
    if table == "native_legal_reference_reads":
        # The producer spells these counts as text; the public main table uses
        # exact integers. Keep that storage meaning beside the source prose.
        descriptions.update({
            "source_bytes": "Exact retained XML byte count.",
            "occurrence_count": "Number of observations produced by this completed selected-shape read, including zero.",
        })
    if table == "house_communications":
        # SpicyDocs shapes publisher-decomposed rows with these slots empty.
        # This builder also fills them from a uniquely qualified printed entry.
        descriptions.update({
            "record_package_id": "The exact Congressional Record package that printed the communication. "
                "May also be filled on a Congress.gov API row from a unique captured entry with matching Congress, "
                "communication type and number. Missing, ambiguous or incomplete reads leave it empty.",
            "record_granule_id": "The exact House executive-communications granule that printed the entry. "
                "Paired with record_package_id; repeated qualifying entries do not supply a scalar locator.",
            "record_entry_text": "The entry's printed text, retained beside the original Congress.gov fields. "
                "An API row keeps its original abstract, report nature and source route when this field is filled.",
        })
    return descriptions


def contract_grain(table: str) -> str:
    """The contract's one-sentence grain, used as the table summary's first clause."""
    return _contracts()[table].grain


@lru_cache(maxsize=256)
def _former_spellings(table: str | None = None) -> re.Pattern[str]:
    """Every ``<column>_json`` no table or receipt declares while ``<column>`` is declared: a list's former name."""
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA

    policies = subject_policies()
    selected = [policies[table]] if table in policies else policies.values()
    declared = {name for policy in selected for name in policy.subject_schema.names}
    declared |= set(RECEIPT_SCHEMA.names)
    former = sorted(name + "_json" for name in declared if name + "_json" not in declared)
    return re.compile(r"\b(" + "|".join(former) + r")\b")


def native_spelling(prose: str, table: str | None = None) -> str:
    """Name each native list by its column where carried prose still uses its former serialized spelling.

    A contract's sentences describe the columns as spicy-docs emits them, lists serialized under ``<column>_json``.
    The native layout publishes the list as ``<column>``, so a sentence naming a sibling list is respelled, here
    only, for every reader of the prose; the sentence itself stays the contract's.
    """
    return _former_spellings(table).sub(lambda match: match.group(1).removesuffix("_json"), prose)


def fec_typed_schemas() -> dict[str, list[tuple[str, str]]]:
    """Read the supported producer declaration without importing its producers.

    This resource retains exact Arrow schema bytes and their digest beside the
    DuckDB spelling measured from those bytes. The dictionary tests reconcile
    both and check producer schema compatibility. The baseline receipt describes the original selected union; schema revisions
    are support declarations and require their own output qualification. It makes
    no assertion that any table is currently published or fully populated.
    """
    path = DICTIONARY_INPUTS / "fec_typed_schemas.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("format_version") != 2 or set(document.get("tables", {})) != set(FEC_TYPED_TABLES):
        raise ValueError("Retained FEC schema declaration differs from the supported table set")
    result = {}
    for table, entry in document["tables"].items():
        columns = entry["columns"]
        if (not columns or any(not isinstance(pair, list) or len(pair) != 2
                               or not all(isinstance(value, str) and value for value in pair) for pair in columns)
                or len({pair[0] for pair in columns}) != len(columns)):
            raise ValueError(f"Invalid retained FEC declared columns: {table}")
        result[table] = [tuple(pair) for pair in columns]
    return result


def expected_schemas(*, native: bool = True) -> dict[str, list[tuple[str, str]]]:
    """Return ``{table: [(column, type_label), ...]}`` for all tables (offline)."""
    schemas: dict[str, list[tuple[str, str]]] = {}
    from_contracts = contract_schemas()
    from_fec = fec_typed_schemas()
    from spicy_regs.contract_types import described_schema
    from spicy_regs.transforms.build_cms_part_d import SCHEMA as CMS_PART_D_SCHEMA
    from spicy_regs.transforms.build_cms_medicaid import SCHEMA as CMS_MEDICAID_SCHEMA
    from spicy_regs.transforms.build_cms_part_b import SCHEMA as CMS_PART_B_SCHEMA
    from spicy_regs.transforms.build_treasury_fiscal import SCHEMAS as TREASURY_SCHEMAS
    from spicy_regs.transforms.build_grants_opportunities import SCHEMA as GRANTS_SCHEMA
    from spicy_regs.transforms.build_ofac import SCHEMAS as OFAC_SCHEMAS
    from spicy_regs.transforms.build_executive_orders import SCHEMA as EXECUTIVE_ORDER_SCHEMA
    from spicy_regs.schemas.house_disbursements import SCHEMAS as HOUSE_DISBURSEMENT_SCHEMAS
    from spicy_regs.schemas.senate_disbursements import SCHEMAS as SENATE_DISBURSEMENT_SCHEMAS
    from spicy_regs.schemas.house_disbursement_pages import SCHEMAS as HOUSE_PDF_SCHEMAS
    from spicy_regs.schemas.disbursement_summaries import SCHEMAS as DISBURSEMENT_SUMMARY_SCHEMAS
    from spicy_regs.transforms.build_fec_financial_profiles import SCHEMA as FEC_PROFILE_SCHEMA
    from spicy_regs.transforms.build_census_acs import SCHEMA as CENSUS_ACS_SCHEMA
    from spicy_regs.transforms.build_agency_lifecycle_stats import SCHEMA as AGENCY_LIFECYCLE_STATS_SCHEMA
    from spicy_regs.transforms.build_comment_periods import COLUMNS as COMMENT_PERIOD_COLUMNS
    from spicy_regs.transforms.build_lifecycles import EVENT_SCHEMA, LIFECYCLE_SCHEMA
    from spicy_regs.transforms.build_proceedings import COLUMNS as PROCEEDING_COLUMNS
    from spicy_regs.transforms.build_regulatory_agenda import ITEM_COLUMNS, RELATIONSHIP_COLUMNS
    from spicy_regs.transforms.build_rule_targets import COLUMNS as RULE_TARGET_COLUMNS
    from spicy_regs.transforms.build_fec_observations import COLLECTION_SCHEMA, RECORD_COLUMNS
    from spicy_regs.transforms.build_fec_source_catalog import COLUMNS as FEC_CATALOG_COLUMNS
    from spicy_regs.transforms.fec_relationships import COLUMNS as FEC_RELATIONSHIP_COLUMNS
    from spicy_regs.transforms.enrich_bill_subjects import COLUMNS as BILL_SUBJECT_COLUMNS
    from spicy_regs.transforms.build_court_opinion_clusters import COLUMNS as COURT_CLUSTER_COLUMNS
    from spicy_regs.transforms.build_court_opinion_clusters import INPUT_COLUMNS as COURT_CLUSTER_INPUT_COLUMNS
    from spicy_regs.transforms.build_courtlistener import PUBLISHED_COLUMNS as COURT_DOCKET_COLUMNS
    from spicy_regs.transforms.build_court_bulk_tables import CITATION_MAP, CITATIONS, OPINIONS, PARENTHETICALS
    from spicy_regs.transforms.build_court_pdf_extractions import COLUMNS as COURT_PDF_COLUMNS
    from spicy_regs.scorecards.resolution import LINK_COLUMNS as SCORECARD_LINK_COLUMNS
    from spicy_regs.scorecards.action_comparisons import TABLE as COMPARISON_TABLE, COLUMNS as COMPARISON_COLUMNS
    from spicy_regs.transforms.build_member_vote_terms import COLUMNS as MEMBER_VOTE_TERM_COLUMNS
    from spicy_regs.transforms.build_sam_entities import COLUMNS as SAM_COLUMNS
    from spicy_regs.transforms.build_lobbying_filings import (
        ACTIVITY_COLUMNS as LOBBYING_ACTIVITY_COLUMNS,
        AFFILIATED_ORGANIZATION_COLUMNS as LOBBYING_AFFILIATED_ORGANIZATION_COLUMNS,
        CONVICTION_DISCLOSURE_COLUMNS as LOBBYING_CONVICTION_DISCLOSURE_COLUMNS,
        FOREIGN_ENTITY_COLUMNS as LOBBYING_FOREIGN_ENTITY_COLUMNS,
        COLUMNS as LOBBYING_COLUMNS,
        CLIENT_COLUMNS as LOBBYING_CLIENT_COLUMNS,
        LOBBYIST_COLUMNS as LOBBYING_LOBBYIST_COLUMNS,
        REGISTRANT_COLUMNS as LOBBYING_REGISTRANT_COLUMNS,
    )
    from spicy_regs.transforms.build_cfr_sections import COLUMNS as CFR_SECTION_COLUMNS
    from spicy_regs.transforms.build_unified_agenda import COLUMNS as UNIFIED_AGENDA_COLUMNS
    from spicy_regs.transforms.build_fec_committees import COLUMNS as FEC_COMMITTEE_COLUMNS
    # gao_reports types its three counts; its Arrow schema is the one declaration that says so.
    from spicy_regs.transforms.build_gao_reports import _SCHEMA as GAO_REPORT_SCHEMA
    from spicy_regs.transforms.build_crs_reports import COLUMNS as CRS_REPORT_COLUMNS
    from spicy_regs.transforms.build_court_docket_groups import SCHEMA as COURT_DOCKET_GROUP_SCHEMA
    from spicy_regs.transforms.build_usaspending_recipients import COLUMNS as USASPENDING_COLUMNS
    from spicy_regs.transforms.build_fcc_ecfs import (
        FILING_COLUMNS as FCC_FILING_COLUMNS,
        PROCEEDING_COLUMNS as FCC_PROCEEDING_COLUMNS,
    )
    from spicy_regs.transforms.build_bill_family import (
        ARCHIVE_COLUMNS,
        BACKFILL_COLUMNS,
        BACKFILL_WALK_COLUMNS,
        VOTE_REFERENCE_COLUMNS,
    )
    from spicy_regs.transforms.committee_report_reads import READ_COLUMNS as COMMITTEE_REPORT_READ_COLUMNS
    from spicy_regs.transforms.held_citations import READS_COLUMNS as CITATION_READ_COLUMNS
    from spicy_regs.transforms.build_org_committee_links import COLUMNS as ORG_COMMITTEE_LINK_COLUMNS

    builder_columns = {
        **SCORECARD_LINK_COLUMNS,
        COMPARISON_TABLE: COMPARISON_COLUMNS,
        "fec_source_catalog": FEC_CATALOG_COLUMNS,
        "fec_source_records": RECORD_COLUMNS,
        "fec_relationships": FEC_RELATIONSHIP_COLUMNS,
        "bill_subjects": BILL_SUBJECT_COLUMNS,
        "court_opinion_clusters": COURT_CLUSTER_COLUMNS,
        "court_dockets": COURT_DOCKET_COLUMNS,
        "court_citations": CITATIONS.columns,
        "court_citation_map": CITATION_MAP.columns,
        "court_parentheticals": PARENTHETICALS.columns,
        "court_opinions": OPINIONS.columns,
        "court_opinion_pdf_extractions": COURT_PDF_COLUMNS,
        "member_vote_terms": MEMBER_VOTE_TERM_COLUMNS,
        "sam_entities": SAM_COLUMNS,
        "lobbying_filings": LOBBYING_COLUMNS,
        "lobbying_activities": LOBBYING_ACTIVITY_COLUMNS,
        "lobbying_activity_lobbyists": LOBBYING_LOBBYIST_COLUMNS,
        "lobbying_foreign_entities": LOBBYING_FOREIGN_ENTITY_COLUMNS,
        "lobbying_affiliated_organizations": LOBBYING_AFFILIATED_ORGANIZATION_COLUMNS,
        "lobbying_conviction_disclosures": LOBBYING_CONVICTION_DISCLOSURE_COLUMNS,
        "lobbying_registrants": LOBBYING_REGISTRANT_COLUMNS,
        "lobbying_clients": LOBBYING_CLIENT_COLUMNS,
        "rule_targets": RULE_TARGET_COLUMNS,
        "proceedings": PROCEEDING_COLUMNS,
        "regulatory_agenda_items": ITEM_COLUMNS,
        "agenda_item_proceedings": RELATIONSHIP_COLUMNS,
        "comment_periods": COMMENT_PERIOD_COLUMNS,
        "cfr_sections": CFR_SECTION_COLUMNS,
        "unified_agenda": UNIFIED_AGENDA_COLUMNS,
        "fec_committees": FEC_COMMITTEE_COLUMNS,
        "crs_reports": CRS_REPORT_COLUMNS,
        "usaspending_recipients": USASPENDING_COLUMNS,
        "fcc_proceedings": FCC_PROCEEDING_COLUMNS,
        "fcc_filings": FCC_FILING_COLUMNS,
        "bill_family_archives": ARCHIVE_COLUMNS,
        "bill_vote_references": VOTE_REFERENCE_COLUMNS,
        "bill_family_backfills": BACKFILL_COLUMNS,
        "bill_family_backfill_walks": BACKFILL_WALK_COLUMNS,
        "committee_report_reads": COMMITTEE_REPORT_READ_COLUMNS,
        "document_citation_reads": CITATION_READ_COLUMNS,
    }
    # Writers that type their columns natively (DATE, INTEGER, BOOLEAN), spelled as DuckDB describes the file.
    builder_schemas = {
        "cms_part_d_drug_spending": CMS_PART_D_SCHEMA,
        "cms_medicaid_drug_spending": CMS_MEDICAID_SCHEMA,
        "cms_part_b_drug_spending": CMS_PART_B_SCHEMA,
        **TREASURY_SCHEMAS,
        "grants_opportunity_observations": GRANTS_SCHEMA,
        **OFAC_SCHEMAS,
        "executive_order_observations": EXECUTIVE_ORDER_SCHEMA,
        **HOUSE_DISBURSEMENT_SCHEMAS,
        **SENATE_DISBURSEMENT_SCHEMAS,
        **HOUSE_PDF_SCHEMAS,
        **DISBURSEMENT_SUMMARY_SCHEMAS,
        "fec_financial_profiles": FEC_PROFILE_SCHEMA,
        "census_acs_county_observations": CENSUS_ACS_SCHEMA,
        "fec_collections": COLLECTION_SCHEMA,
        "rulemaking_lifecycles": LIFECYCLE_SCHEMA,
        "lifecycle_events": EVENT_SCHEMA,
        "agency_lifecycle_stats": AGENCY_LIFECYCLE_STATS_SCHEMA,
        "gao_reports": GAO_REPORT_SCHEMA,
        "court_docket_groups": COURT_DOCKET_GROUP_SCHEMA,
    }
    # Writers whose SQL states the DuckDB type itself, as (column, type) pairs.
    builder_pairs = {
        "org_committee_links": ORG_COMMITTEE_LINK_COLUMNS,
    }
    if not native:
        builder_columns.update({
            'court_opinion_clusters': tuple(column for column in COURT_CLUSTER_INPUT_COLUMNS
                                           if column != 'raw_source_record'),
            'court_citations': CITATIONS.input_columns,
            'court_citation_map': CITATION_MAP.input_columns,
            'court_parentheticals': PARENTHETICALS.input_columns,
        })
    installed = subject_policies() if native else {}
    for name in TABLES if native else LEGACY_TABLES:
        if name in installed:
            schemas[name] = described_schema(installed[name].subject_schema)
        elif name in builder_columns:
            schemas[name] = [(column, "VARCHAR") for column in builder_columns[name]]
        elif name in builder_schemas:
            schemas[name] = described_schema(builder_schemas[name])
        elif name in builder_pairs:
            schemas[name] = list(builder_pairs[name])
        elif name in from_fec:
            schemas[name] = from_fec[name]
        elif name in from_contracts:
            schemas[name] = list(from_contracts[name])
        elif name in RECORD_TYPES:
            rt = RECORD_TYPES[name]
            schemas[name] = [(col, rt.sql_type(col)) for col in rt.schema]
            if name == "comments":
                schemas[name] += [(col, "VARCHAR") for col in COMMENT_MIRROR_COLUMNS]
            if name == "documents":
                schemas[name] += [(col, "VARCHAR") for col in DOCUMENT_FAMILY_COLUMNS]
        elif name in DERIVED_SCHEMAS:
            schemas[name] = list(DERIVED_SCHEMAS[name])
        else:  # pragma: no cover - guards against TABLES/registry drift
            raise KeyError(f"No schema known for table {name!r}")
    return schemas


# --------------------------------------------------------------------------- #
# Live schema discovery (DuckDB DESCRIBE over R2 or a local parquet directory).
# --------------------------------------------------------------------------- #
class SchemaDiscoveryError(OSError):
    """Incomplete live reads, retaining the schemas that could be checked."""

    def __init__(self, schemas: dict[str, list[tuple[str, str]]], failures: list[str]) -> None:
        self.schemas = schemas
        super().__init__("\n".join(failures))


def discover_schemas(source: str, base: str | None = None, *, index: dict | None = None) -> dict[str, list[tuple[str, str]]]:
    """Discover schemas from published parquet via DuckDB ``DESCRIBE``.

    ``source`` is ``"r2"`` (remote https bucket; needs the httpfs extension) or
    ``"local"`` (a directory of ``<table>.parquet`` files). Mirrors the
    connection recipe in :mod:`spicy_regs.mcp_server`: a remote table resolves
    through the publication index, then the rulemaking snapshot pointer, then
    its bare legacy key.
    """
    import duckdb

    if source == "r2":
        from spicy_regs.sources.publication import published_urls

        base_url = resolve_r2_base_url(base)
        urls = published_urls(base_url, index=index)
        names = tuple(dict.fromkeys(("dockets", "documents", "comments", "comments_index", *urls)))

        def url_for(name: str) -> list[str]:
            return urls.get(name, [f"{base_url}/{name}.parquet"])

    elif source == "local":
        base_dir = Path(base or "./spicy-regs-data")
        names = TABLES

        def url_for(name: str) -> list[str]:
            return [str(base_dir / f"{name}.parquet")]

    else:
        raise ValueError(f"Unknown source {source!r}; expected 'r2' or 'local'")

    schemas: dict[str, list[tuple[str, str]]] = {}
    failures: list[str] = []
    with duckdb.connect() as con:
        con.execute(f"SET home_directory='{tempfile.gettempdir()}'")
        if source == "r2":
            load_public_http(con)
        for name in names:
            try:
                rows = con.execute(f"DESCRIBE SELECT * FROM {parquet_scan(url_for(name))}").fetchall()
                schemas[name] = [(row[0], row[1]) for row in rows]
            except (duckdb.IOException, duckdb.HTTPException, OSError) as exc:
                failures.append(f"[{name}] {exc}")
    if failures:
        raise SchemaDiscoveryError(schemas, failures)
    return schemas


# --------------------------------------------------------------------------- #
# Descriptions file.
# --------------------------------------------------------------------------- #
#: A descriptions.yaml entry naming this source reads its per-column prose from
#: the installed spicy-docs contract instead of listing it inline.
COLUMNS_FROM_SPICY_DOCS = "spicy_docs"


def load_curated_descriptions(path: Path = DEFAULT_DESCRIPTIONS) -> dict:
    """Read the saved dictionary entries before resolving columns or filtering tables."""
    import yaml

    with path.open(encoding="utf-8") as fh:
        return (yaml.safe_load(fh) or {}).get("tables", {})


def load_descriptions(path: Path = DEFAULT_DESCRIPTIONS, *, native: bool = True) -> dict:
    """The curated descriptions, with every ``columns_from`` marker resolved.

    ``columns_from: spicy_docs`` means "the per-column sentences for this table
    are the contract's". Every one of them is, and copying them here would
    create a second copy to keep in step with no mechanism
    keeping it there. The label, coverage statement, ``measured_on`` and summary stay
    hand-written, because what this repository publishes and when it last
    measured it are its own facts, not the wheel's.

    Resolving here rather than at each call site means ``check``, ``generate``
    and ``catalog`` all see the same entry, and none of them has to know the
    marker exists.
    """
    tables = load_curated_descriptions(path)
    for name, entry in tables.items():
        if not entry or entry.get("columns_from") != COLUMNS_FROM_SPICY_DOCS:
            continue
        inline = entry.get("columns") or {}
        if inline:
            raise ValueError(f"[{name}] declares columns_from: {COLUMNS_FROM_SPICY_DOCS} and also lists columns inline")
        entry["columns"] = contract_column_prose(name)
    # Subject prose is generated from explicit family field maps. Source-only
    # processing datasets remain discoverable through receipt declarations.
    # Nothing about receipts is added to a summary here (owner decision, 2026-10-05): one page explains them,
    # and what a table says about its own receipt is written in its own notes.
    subject_prose = DICTIONARY_INPUTS / "subject_descriptions.json"
    overrides = json.loads(subject_prose.read_text()) if subject_prose.exists() else {}
    for name, policy in (subject_policies().items() if native else ()):
        if policy.receipt_only:
            tables.pop(name, None)
            continue
        entry = tables.setdefault(name, {})
        if name in overrides:
            if entry.get("legacy_columns"):
                entry["legacy_column_descriptions"] = {
                    column: entry["columns"][column] for column in entry["legacy_columns"]
                }
            entry["columns"] = overrides[name]["columns"]
        entry["identity_columns"] = list(policy.identity_fields)
    # A selected receiptless FEC generation still exposes the captured source
    # fields. Keep their qualified meanings independently of current native
    # fields, whose versioned interpretation can legitimately change.
    legacy_path = DICTIONARY_INPUTS / "fec_legacy_column_descriptions.json"
    legacy = json.loads(legacy_path.read_text(encoding="utf-8"))
    for name, meanings in legacy["tables"].items():
        if name in tables:
            tables[name]["legacy_column_descriptions"] = meanings
            if not native:
                tables[name]["columns"] = meanings
    return tables


# --------------------------------------------------------------------------- #
# Reconciliation.
# --------------------------------------------------------------------------- #
def selected_dictionary_definitions(index: dict, names: Iterable[str], descriptions_path: Path):
    """Use earlier declarations only for a selected owner explicitly without receipts.

    Unknown owners and any declared receipt member keep native checks. No live
    field or type is inferred as a supported declaration.
    """
    from spicy_regs.sources.publication import table_owner

    descriptions = load_descriptions(descriptions_path)
    schemas = expected_schemas()
    legacy_names = {name for name in names
                    if (owner := table_owner(index, name + ".parquet")) is not None
                    and "etlReceipts" not in owner[1]}
    if legacy_names:
        legacy_descriptions = load_descriptions(descriptions_path, native=False)
        legacy_schemas = expected_schemas(native=False)
        for name in legacy_names:
            if name in legacy_descriptions:
                descriptions[name] = legacy_descriptions[name]
            if name in legacy_schemas:
                schemas[name] = legacy_schemas[name]
        # These fixed producer declarations describe the earlier receipt-free
        # FEC generations. Current native definitions remain the default.
        if any(name.startswith("fec_") for name in legacy_names):
            path = DICTIONARY_INPUTS / "fec_historical_definitions.json"
            historical = json.loads(path.read_text(encoding="utf-8"))["tables"]
            for name in legacy_names & historical.keys():
                entry = historical[name]
                columns, meanings = entry["columns"], entry["columnDescriptions"]
                if (not columns or any(not isinstance(pair, list) or len(pair) != 2
                                       or not all(isinstance(value, str) and value for value in pair)
                                       for pair in columns)
                        or len({pair[0] for pair in columns}) != len(columns)
                        or {pair[0] for pair in columns} != set(meanings)
                        or any(not isinstance(text, str) or not text.strip() for text in meanings.values())):
                    raise ValueError(f"Invalid historical FEC dictionary declaration: {name}")
                schemas[name] = [tuple(pair) for pair in columns]
                if name not in descriptions:
                    # A retired table (ss-j4vz B10) keeps its table prose only here, for the
                    # earlier generations that still hold it.
                    if "description" not in entry:
                        raise ValueError(f"Historical FEC table has no description: {name}")
                    descriptions[name] = {key: value for key, value in entry["description"].items() if key != "retired"}
                descriptions[name]["columns"] = meanings
    return descriptions, schemas


def accept_pending_policy_columns(index: dict, base_url: str, descriptions: dict, expected: dict, live: dict, *,
                                  load_root=None) -> tuple[dict, dict, list[str]]:
    """Leave out of a live check the columns a table's installed policy declares and its selected generation predates.

    A column promotion merges before the nightly that publishes it, so until then the live file lacks columns the
    dictionary describes (six for the two Register tables on 2026-11-10; the same on 2026-11-09 for bills). Such a
    column is declared, not yet published: reported as a notice, not a failure. It is recognised from the
    generation itself, never from a list: the family root states the policy the table was written under, and only
    when that is a declared earlier policy of the installed one (``receipt_policies``) are the columns the
    installed policy adds to it set aside. Every other difference stays an error: a column both policies hold that
    the file lacks, a type, a table without receipts, a root stating a policy that is not a declared earlier one.
    Roots are read only for tables that lack an expected column, one read per family.
    """
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.etl_receipts import receipt_policies
    from spicy_regs.sources import publication

    load_root = load_root or publication.load_family_root
    installed, roots, notices = installed_policies(), {}, []
    descriptions, expected = dict(descriptions), dict(expected)
    for table in sorted(set(expected) & set(live)):
        missing = {name for name, _ in expected[table]} - {name for name, _ in live[table]}
        owner = publication.table_owner(index, table + ".parquet")
        if not missing or table not in installed or owner is None or "etlReceipts" not in owner[1]:
            continue
        family, entry = owner
        if family not in roots:
            roots[family] = load_root(base_url, entry)[1]
        stated = roots[family].get("spec", {}).get("etlReceipts", {}).get("policies", [])
        written = next((p for p in receipt_policies(installed[table])[1:]
                        if any(p.descriptor() == declared for declared in stated)), None)
        if written is None:
            continue
        pending = sorted(missing - set(written.subject_schema.names))
        if not pending:
            continue
        notices.append(f"[{table}] declared, not yet published: {', '.join(pending)} (the selected generation "
                       f"states {written.policy_version}; installed is {installed[table].policy_version})")
        expected[table] = [pair for pair in expected[table] if pair[0] not in pending]
        if table in descriptions:
            columns = descriptions[table].get("columns") or {}
            descriptions[table] = {**descriptions[table],
                                   "columns": {name: text for name, text in columns.items() if name not in pending}}
    return descriptions, expected, notices


def _reconcile_columns(
    table: str,
    left_label: str,
    left_cols: list[str],
    right_label: str,
    right_cols: list[str],
) -> list[str]:
    """Return human-readable drift errors comparing two column-name lists."""
    errors: list[str] = []
    left, right = set(left_cols), set(right_cols)
    for col in sorted(left - right):
        errors.append(f"[{table}] column {col!r} in {left_label} but missing from {right_label}")
    for col in sorted(right - left):
        errors.append(f"[{table}] column {col!r} in {right_label} but missing from {left_label}")
    return errors


def check_descriptions(
    schemas: dict[str, list[tuple[str, str]]],
    descriptions: dict,
) -> list[str]:
    """Reconcile a schema map against the curated descriptions. Returns errors."""
    errors: list[str] = []
    installed = installed_spicy_docs()
    schema_tables = set(schemas)
    desc_tables = set(descriptions)
    for table in sorted(schema_tables - desc_tables):
        errors.append(f"[{table}] table has a schema but no entry in descriptions.yaml")
    for table in sorted(desc_tables - schema_tables):
        errors.append(f"[{table}] described in descriptions.yaml but is not a known table")
    for table in sorted(set(COVERAGE_PROSE_EXCEPTIONS) - set(LEGACY_TABLES)):
        errors.append(f"[{table}] has a coverage exception but is not a known table")

    for table in sorted(schema_tables & desc_tables):
        schema_cols = [c for c, _ in schemas[table]]
        entry = descriptions[table] or {}
        if not (entry.get("summary") or "").strip():
            errors.append(f"[{table}] missing a 'summary' in descriptions.yaml")
        if not (entry.get("label") or "").strip():
            errors.append(f"[{table}] missing a 'label' in descriptions.yaml")
        if not (entry.get("coverage") or "").strip():
            errors.append(f"[{table}] missing a 'coverage' statement in descriptions.yaml")
        if entry.get("subject") not in SUBJECTS:
            errors.append(f"[{table}] 'subject' must be one of {', '.join(SUBJECTS)}, not {entry.get('subject')!r}")
        measured_on = str(entry.get("measured_on") or "").strip()
        errors.extend(coverage_prose_errors(table, entry.get("coverage") or "", measured_on))
        errors.extend(data_quality_prose_errors(table, entry.get("data_quality") or ""))
        errors.extend(interim_note_errors(table, entry.get("data_quality") or "", installed))
        errors.extend(receipt_pointer_errors(table, entry))
        if not measured_on:
            errors.append(f"[{table}] missing 'measured_on' beside its coverage statement")
        else:
            try:
                date.fromisoformat(measured_on)
            except ValueError:
                errors.append(f"[{table}] 'measured_on' is not an ISO date: {measured_on!r}")
        if "data_quality" in entry and not (entry.get("data_quality") or "").strip():
            errors.append(f"[{table}] has an empty 'data_quality' note in descriptions.yaml")
        desc_cols = list((entry.get("columns") or {}).keys())
        identity = entry.get("identity_columns", [])
        if not isinstance(identity, list) or any(column not in schema_cols for column in identity):
            errors.append(f"[{table}] 'identity_columns' must list declared columns")
        errors.extend(_reconcile_columns(table, "schema", schema_cols, "descriptions.yaml", desc_cols))
        for col in schema_cols:
            text = (entry.get("columns") or {}).get(col)
            if col in desc_cols and not (text or "").strip():
                errors.append(f"[{table}.{col}] has an empty description")
    return errors


def published_row_counts(base_url: str, index: dict | None = None) -> dict[str, int]:
    """Each published table's pinned row count: the index's families and the rulemaking snapshot's manifest.

    ``index`` is the publication index where the caller has already read it.
    """
    from spicy_regs.sources.publication import load_index, load_rulemaking_snapshot

    base = base_url.rstrip("/")
    rows = {
        key.removesuffix(".parquet"): int(table["rows"])
        for family in (index or load_index(base))["families"].values()
        for key, table in family["tables"].items()
    }
    snapshot = load_rulemaking_snapshot(base)
    if snapshot is not None:
        rows.update({key.removesuffix(".parquet"): int(table["rows"]) for key, table in snapshot["tables"].items()})
    return rows


def kind_index_errors(descriptions: dict, rows: dict[str, int]) -> list[str]:
    """Hold each declared coverage kind to the pinned index: no rows means Empty, and Empty means no rows.

    Where the index can be read, the kind is its fact, not the author's: six
    0-row tables read "Sampled" or "Not a range" on 2026-10-02, so a count over
    them read as a thin sample rather than as zero by construction. The prose
    prefix stays the committed source so the offline build is reproducible; this
    check is what refuses a prefix the index contradicts.
    """
    errors = []
    for table, count in sorted(rows.items()):
        entry = descriptions.get(table)
        if not entry:
            continue
        declared = coverage_kind(entry.get("coverage") or "")
        if (count == 0) != (declared == "empty"):
            expected = "Empty" if count == 0 else "a populated kind"
            errors.append(
                f"[{table}] coverage kind {declared!r} contradicts the published index ({count:,} rows); "
                f"the index says {expected}"
            )
    return errors


def check_schema_drift(
    expected: dict[str, list[tuple[str, str]]],
    live: dict[str, list[tuple[str, str]]],
) -> list[str]:
    """Reconcile the in-code expected schema against a live (parquet) schema: its columns, then each one's type.

    Names alone would pass a DATE column published as VARCHAR, so a typed
    declaration (the lifecycle and attribute tables) is held to the file too.
    """
    errors: list[str] = []
    for table in sorted(set(expected) | set(live)):
        exp_types, live_types = dict(expected.get(table, [])), dict(live.get(table, []))
        errors.extend(_reconcile_columns(table, "in-code schema", list(exp_types), "live parquet", list(live_types)))
        errors.extend(
            f"[{table}.{column}] in-code type {exp_types[column]} but live parquet {live_types[column]}"
            for column in exp_types
            if column in live_types and exp_types[column] != live_types[column]
        )
    return errors


# --------------------------------------------------------------------------- #
# Markdown generation.
# --------------------------------------------------------------------------- #
_GENERATED_BANNER = (
    "<!-- Generated by `spicy-regs-dict generate`. Do not edit by hand. "
    "Edit data_dictionary/descriptions.yaml or the schema, then regenerate. -->"
)


def table_labels(descriptions: dict) -> dict[str, str]:
    """Return ``{table: short display label}``.

    The label names the collection for someone deciding whether it holds court
    cases, bills or comments. It lives here, beside the summary the table pages
    already render, so a catalog and a docs page cannot name the same class two
    different ways.
    """
    return {table: (entry or {}).get("label", "") for table, entry in descriptions.items()}


def table_coverage(descriptions: dict) -> dict[str, dict[str, str]]:
    """Return ``{table: {label, coverage, data_quality, summary}}`` for a catalog.

    ``coverage`` is a written statement, not a computed min/max, because a
    computed range lies for three of these classes in three different ways: a
    fifty-three-day ingest window reads as a coverage claim about the
    publisher's archive, a rotating-window ingest reads as even density it does
    not have, and two tables carry publisher dates in the year 0000. Each
    statement opens by naming which kind it is.

    These are statements about supported tables and retained measurements.
    Neither a declaration nor its measurement date establishes that an output
    is publicly available. Publication requires a separate artifact observation;
    inclusion in a search index is that consumer's own fact.
    """
    return {
        table: {
            "label": (entry or {}).get("label", ""),
            "coverage": (entry or {}).get("coverage", ""),
            "measured_on": str((entry or {}).get("measured_on", "")),
            "data_quality": (entry or {}).get("data_quality", ""),
            "summary": (entry or {}).get("summary", ""),
        }
        for table, entry in descriptions.items()
    }


def _render_table_page(
    table: str,
    columns: list[tuple[str, str]],
    entry: dict,
) -> str:
    """Render one table's docs page: banner, label, coverage stamp, column table."""
    summary = (entry.get("summary") or "").strip()
    col_desc = entry.get("columns") or {}
    pk = None
    rt = RECORD_TYPES.get(table)
    if rt is not None:
        pk = rt.dedup_key

    label = (entry.get("label") or "").strip()
    lines = [_GENERATED_BANNER, "", f"# `{table}`", ""]
    if label:
        lines += [f"**{label}**", ""]
    if summary:
        lines += [summary, ""]
    coverage = (entry.get("coverage") or "").strip()
    if coverage:
        measured_on = str(entry.get("measured_on") or "").strip()
        stamp = f" *(measured {measured_on})*" if measured_on else ""
        lines += [f"**Coverage.** {coverage}{stamp}", ""]
    data_quality = (entry.get("data_quality") or "").strip()
    if data_quality:
        lines += [f"**Data quality.** {data_quality}", ""]
    queryable = "Configured" if table in MCP_QUERYABLE else "Not configured"
    where = f", in the snapshot that `{SNAPSHOT_POINTER}` names" if table in RULEMAKING_TABLES else ""
    lines += [
        f"- **Parquet file:** `{table}.parquet`{where}",
        f"- **MCP `query_sql` support:** {queryable}; requires an available artifact.",
        "- **Publication status:** Not established by this schema page or its measurement date.",
        "- **Row count:** Not stated here; the MCP `describe_table` reply gives the live count under `publication`.",
    ]
    if pk:
        lines.append(f"- **Primary / dedup key:** `{pk}`")
    lines += ["", *_column_table(columns, col_desc, pk), ""]
    return "\n".join(lines)


def _column_table(columns: list[tuple[str, str]], descriptions: dict, pk: str | None = None) -> list[str]:
    """The Markdown column table of a table page, one line per column."""
    lines = ["| Column | Type | Description |", "| --- | --- | --- |"]
    for col, col_type in columns:
        desc = (descriptions.get(col) or "").replace("|", "\\|").strip()
        marker = " 🔑" if col == pk else ""
        lines.append(f"| `{col}`{marker} | `{col_type}` | {desc} |")
    return lines


#: The one page that explains receipts, in plain words, for a reader who is not an engineer. Its text is
#: hand-written in this file; ``COLUMNS_MARK`` is where the declared columns go. A table's own notes point at it
#: and do not explain receipts again.
RECEIPT_GUIDE = REPO_ROOT / "data_dictionary" / "etl_receipts.md"
COLUMNS_MARK = "<!-- columns -->"


def receipt_guide(columns: str) -> str:
    """The receipt guide with ``columns`` set where it describes what a receipt contains."""
    guide = RECEIPT_GUIDE.read_text(encoding="utf-8").strip()
    if guide.count(COLUMNS_MARK) != 1:
        raise ValueError(f"{RECEIPT_GUIDE.name} must mark where the columns go exactly once ({COLUMNS_MARK})")
    return guide.replace(COLUMNS_MARK, columns)


def render_receipts_page(receipt: dict) -> str:
    """Render ``docs/tables/etl_receipts.md``: the guide around the receipt table's declared columns."""
    columns = [(column["column_name"], column["column_type"]) for column in receipt["columns"]]
    meanings = {column["column_name"]: column["description"] for column in receipt["columns"]}
    banner = (
        "<!-- Generated by `spicy-regs-dict generate`. Do not edit by hand. Edit "
        f"data_dictionary/{RECEIPT_GUIDE.name} (the text) or receipt_metadata in src/spicy_regs/data_dictionary.py "
        "(the columns), then regenerate. -->"
    )
    guide = receipt_guide("\n".join(_column_table(columns, meanings)))
    return "\n".join([banner, "", "# `etl_receipts`", "", f"**{receipt['label']}**", "", guide, ""])


def generate(
    descriptions: dict,
    schemas: dict[str, list[tuple[str, str]]],
    out_dir: Path = DEFAULT_DOCS_TABLES_DIR,
) -> list[Path]:
    """Render one Markdown page per table. Returns the written paths."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for table in TABLES:
        entry = descriptions.get(table, {}) or {}
        page = _render_table_page(table, schemas[table], entry)
        path = out_dir / f"{table}.md"
        path.write_text(page, encoding="utf-8")
        written.append(path)
    return written


# --------------------------------------------------------------------------- #
# CLI.
# --------------------------------------------------------------------------- #
def _schemas_for_source(source: str, base: str | None) -> dict[str, list[tuple[str, str]]]:
    if source == "schema":
        return expected_schemas()
    return discover_schemas(source, base)


#: Exit code for "the live source could not be read", distinct from drift.
#: A caller gating on this check needs to tell a transient unreachable bucket
#: from a real disagreement between what we declare and what we publish;
#: collapsing both into 1 makes the check either fail on outages or, with a
#: `|| true`, unable to fail at all.
EXIT_SOURCE_UNREACHABLE = 3


def cmd_check(args: argparse.Namespace) -> int:
    """Reconcile descriptions against the chosen schema source and report drift.

    Exit 1 on drift; a live source that cannot be read exits EXIT_SOURCE_UNREACHABLE (3), not 1.
    """
    import httpx

    descriptions = load_descriptions(Path(args.descriptions))
    errors: list[str] = []
    unreadable = False

    if args.source == "schema":
        errors += check_descriptions(expected_schemas(), descriptions)
    else:
        index = None
        try:
            if args.source == "r2":
                from spicy_regs.sources.publication import load_index

                base_url = resolve_r2_base_url(args.base)
                index = load_index(base_url)
            live = (discover_schemas(args.source, args.base, index=index) if args.source == "r2"
                    else discover_schemas(args.source, args.base))
        except SchemaDiscoveryError as exc:
            live = exc.schemas
            unreadable = True
            print(f"! Some live schemas could not be read:\n{exc}", file=sys.stderr)
        except (duckdb.IOException, duckdb.HTTPException, httpx.HTTPError, OSError, PublicationError) as exc:
            # Only the "could not read it" failures, and a pointer that cannot name
            # one publication. A malformed parquet or a bad query is a real
            # problem and must not be reported as an outage.
            print(f"! Could not read the live {args.source} schema: {exc}", file=sys.stderr)
            print("  Drift was NOT checked. This is not a pass.", file=sys.stderr)
            return EXIT_SOURCE_UNREACHABLE
        # Descriptions must cover the live schema, and the in-code registry the
        # docs are generated from must match the live schema too.
        # Supported but unpublished tables still receive offline checks. The
        # live check follows actual publication, including unknown active tables.
        expected = expected_schemas()
        if index is not None:
            descriptions, expected = selected_dictionary_definitions(index, live, Path(args.descriptions))
            try:
                descriptions, expected, notices = accept_pending_policy_columns(
                    index, resolve_r2_base_url(args.base), descriptions, expected, live)
                for notice in notices:
                    print(f"  notice: {notice}")
            except (httpx.HTTPError, OSError, PublicationError) as exc:
                print(f"! Could not read a published family root: {exc}", file=sys.stderr)
                unreadable = True
        errors += check_descriptions(live, {name: value for name, value in descriptions.items() if name in live})
        expected = {name: value for name, value in expected.items() if name in live}
        errors += check_schema_drift(expected, live)
        if args.source == "r2":
            # The kind is derived from the pinned index where the index can be read; offline it is the prose prefix.
            # Whether a table has receipts to point at is the same index's fact, with no offline fallback.
            from spicy_regs.sources.publication import load_index

            try:
                base_url = resolve_r2_base_url(args.base)
                if index is None:
                    raise PublicationError("No captured publication selection for live schema verification")
                if load_index(base_url) != index:
                    raise PublicationError("Publication selection changed during live schema verification")
                rows = published_row_counts(base_url, index)
                errors += kind_index_errors(descriptions, rows)
                errors += receipt_index_errors(descriptions, rows, published_receipt_datasets(index))
            except (httpx.HTTPError, OSError, PublicationError) as exc:
                print(f"! Could not read the published row counts: {exc}", file=sys.stderr)
                unreadable = True

    from spicy_regs import receipt_field_declarations

    stale = qualification_errors() + joins_errors() + receipt_field_declarations.record_errors()
    if errors or stale:
        print(f"✗ Data dictionary check failed ({len(errors) + len(stale)} issue(s)):", file=sys.stderr)
        for err in errors + stale:
            print(f"  - {err}", file=sys.stderr)
        if errors:
            print(
                "\nUpdate data_dictionary/descriptions.yaml (and the table's schema declaration "
                "if the schema changed; see expected_schemas) so they line up.",
                file=sys.stderr,
            )
        return 1
    if unreadable:
        print("Live schema verification is incomplete; this is not a pass.", file=sys.stderr)
        return EXIT_SOURCE_UNREACHABLE
    count = len(descriptions) if args.source == "schema" else len(live)
    print(f"✓ Data dictionary check passed ({count} tables, source={args.source}).")
    return 0


def qualification_bytes() -> bytes:
    """The MCP qualification record freshly built from the output ledger; OSError or ValueError when it cannot be."""
    return catalog_bytes(output_ledger.qualification_record(output_ledger.LEDGER.read_text(encoding="utf-8")))


def qualification_errors() -> list[str]:
    """Refuse a bundled qualification record that differs from a fresh build of the output ledger.

    The MCP server reports ledger dispositions from this copy, so a stale one
    would describe audits the ledger no longer states.
    """
    record = output_ledger.RECORD
    try:
        fresh = qualification_bytes()
    except (OSError, ValueError) as exc:
        return [f"{output_ledger.LEDGER_NAME} cannot build the qualification record: {exc}"]
    if not record.is_file() or record.read_bytes() != fresh:
        return [f"{record.name} is stale relative to {output_ledger.LEDGER_NAME}; run 'uv run spicy-regs-dict generate'"]
    return []


def joins_bytes() -> bytes:
    """The declared cross-table joins as the bundled ``table_joins.json`` states them."""
    return catalog_bytes(table_joins.joins_record())


def joins_errors() -> list[str]:
    """Refuse a join naming an undeclared table or column, or a bundled copy that differs from the declarations."""
    errors = table_joins.declaration_errors(expected_schemas())
    record = table_joins.RECORD
    if not record.is_file() or record.read_bytes() != joins_bytes():
        errors.append(f"{record.name} is stale relative to spicy_regs.table_joins; run 'uv run spicy-regs-dict generate'")
    return errors


def build_catalog(descriptions: dict, schemas: dict[str, list[tuple[str, str]]]) -> dict:
    """Return the schema catalog: one supported class per entry, in TABLES order.

    This is the declaration side of a readable catalog, and it exists as a file
    because the consumer cannot import this package. It describes the tables
    this checkout supports. A separate observation of the actual output is
    required to establish publication; local runs and declarations do not.

    It deliberately does **not** say whether a class is searchable. That is the
    serving side's fact, and a reader must derive it by aggregating over its own
    index rather than trusting this document — a catalog that certifies its own
    coverage is not a check. ``MCP_QUERYABLE`` identifies configured table
    support, not current availability, and is not exported here.

    ``measured_on`` is a manually maintained date attached to the coverage
    statement. Some statements describe source measurements, some describe
    local bounded runs, and some remain design statements. Read the associated
    prose and receipt to distinguish them. The date is neither a publication
    timestamp nor an automatic freshness or quality check.

    ``kind`` is the coverage statement's kind as a token. The prose still opens
    by naming it for a person, but a consumer must not have to parse a sentence
    to branch on it: the opening word carries a trailing period or comma
    depending on the phrasing, so a whitespace split fails on seven of the
    twenty-four classes.
    """
    from spicy_regs.fec_query_catalog import table_category

    coverage = table_coverage(descriptions)
    return {
        "format_version": CATALOG_FORMAT_VERSION,
        "declares": "supported table schemas; publication and search availability are verified separately",
        "views": list(build_fec_child_metadata(descriptions, schemas).values()),
        "etl_receipts": receipt_metadata(),
        "classes": [
            {
                "table": table,
                "label": coverage[table]["label"],
                **({"row_unit": descriptions[table]["row_unit"]} if descriptions[table].get("row_unit") else {}),
                **({"category": descriptions[table]["category"]} if descriptions[table].get("category") else {}),
                **({"category": table_category(table)} if table_category(table) else {}),
                "summary": coverage[table]["summary"],
                "coverage": coverage[table]["coverage"],
                "kind": coverage_kind(coverage[table]["coverage"]),
                "measured_on": coverage[table]["measured_on"],
                "data_quality": coverage[table]["data_quality"] or None,
                "columns": [name for name, _ in schemas[table]],
            }
            for table in TABLES
        ],
    }


def build_fec_child_metadata(descriptions, schemas):
    """Bind empty declared parent schemas to document child rows without reading data."""
    from spicy_regs.relationship_views import FEC_QUERY_VIEWS
    from spicy_regs.fec_query_catalog import child_column_descriptions
    from spicy_regs.relationship_views.core import quoted

    result = {}
    with duckdb.connect() as con:
        parents = {table for spec in FEC_QUERY_VIEWS for table in spec.required}
        for table in sorted(parents):
            con.execute(f'CREATE TABLE {quoted(table)} (' + ', '.join(
                f'{quoted(name)} {dtype}' for name, dtype in schemas[table]) + ')')
        for spec in FEC_QUERY_VIEWS:
            described = con.execute('DESCRIBE ' + spec.query({})).fetchall()
            meanings = child_column_descriptions(spec, described, descriptions)
            result[spec.name] = {
                'table': spec.name, 'label': spec.name.replace('_', ' '), 'summary': spec.meaning,
                'grain': spec.meaning, 'kind': 'derived', 'category': spec.category, 'subject': 'campaign_finance',
                'view_role': spec.role, 'source_tables': list(spec.required),
                'rule_version': spec.rule_version, 'identity_columns': list(spec.identity_columns),
                'column_descriptions': meanings,
                'columns': [{'column_name': row[0], 'column_type': row[1], 'description': meanings[row[0]]}
                            for row in described],
            }
    return result


def receipt_metadata():
    """Describe the receipt table in plain words; each sentence states what ``etl_receipts`` does.

    The words are for a reader who is not an engineer (owner decision, 2026-10-05): a row, its key, a build and
    a fingerprint, where the code says subject, identity, generation and digest. ``RECEIPT_GUIDE`` explains
    receipts at length; these are the catalog's short statements and the meaning of each column.
    """
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA
    from spicy_regs.contract_types import described_schema
    prose = {
        "receipt_id": "This receipt's own fingerprint: a SHA-256 digest of all its other columns, so a receipt "
                      "that was changed or damaged no longer matches its id.",
        "dataset": "The table this receipt belongs to, by name (`crs_reports`). A few datasets have no table of "
                   "their own and exist only as receipts.",
        "policy_version": "Version of the rule that says which of the table's fields are published as columns and "
                          "which are kept in the receipt.",
        "generation_id": "The build that published this receipt. A table and its receipts are always published "
                         "together as one build, and this names it.",
        "record_id": "A SHA-256 fingerprint of the table's name and the row's key. Empty on a receipt that states "
                     "no key.",
        "subject_version": "A SHA-256 fingerprint of the row's published values. It changes when any of them "
                           "changes, which ties a receipt to one exact version of its row. Empty on a receipt that "
                           "has no row.",
        "identity_json": "The row's key: each key column's name and its value, in the typed form shown under \"How "
                         "to find the receipt for a row\". Empty on a receipt that states no key.",
        "attempt_id": "Which processing attempt produced the receipt. A failed attempt, and one that produced no "
                      "row, have one too.",
        "outcome": "`accepted`: the record became the row this receipt goes with. `observed`: something was read "
                   "and recorded, and no row goes with it (a dataset that has no table, a read that produced no "
                   "row, or a row since removed). `rejected`, `refused` or `error`: the record did not become a "
                   "row, and none was made up for it; each table's pipeline chooses among the three words. All "
                   "five describe our processing, never the publisher's own status for the record.",
        "processor": "Name and version of the program, or reading rule, that produced the receipt.",
        "witnesses": "What was read to produce the receipt, in order, a source read twice listed twice. For each: "
                     "its identifier (`source_id`), its address (`source_uri`), a SHA-256 fingerprint of its content "
                     "(`sha256`), the place within it (`locator`) and, for a stored copy, which copy "
                     "(`body_version`). Every receipt names at least one source, with a fingerprint or a "
                     "stored-copy version.",
        "processing_json": "The fields this table keeps out of its columns, by name, in the typed form shown under "
                           "\"How to read a value out of a receipt\". For many tables that is the whole record as the "
                           "pipeline held it before conversion (`raw_record`); for others, named fields such as a "
                           "source link. On a receipt for a record that did not become a row, it holds the fields "
                           "that were read.",
        "diagnostic_json": "Notes about the processing itself, in the same typed form, such as which earlier "
                           "build's receipt for this row was carried forward and what that receipt held.",
    }
    return {"table": "etl_receipts", "subject": None, "label": "ETL receipts", "category": "processing_evidence", "kind": "not_a_range",
            "summary": "The paperwork behind each row: what was read to make it, how the processing went, and what "
                       "was kept out of the table's columns.",
            "grain": "One receipt for each row of a table that has receipts, plus one for each record that was read "
                     "and recorded without becoming a row.",
            "identity_columns": ["receipt_id"], "member": "etl_receipts.parquet",
            "selection": "A table and its receipts are published together as one build. Read both from the same entry "
                         "of the publication index; a receipts file from another build does not match the table's "
                         "rows.",
            "columns": [{"column_name": name, "column_type": dtype, "description": prose[name]} for name, dtype in described_schema(RECEIPT_SCHEMA)],
            "datasets": [{"dataset": name, "receipt_only": p.receipt_only, "policy_version": p.policy_version,
                          "identity_columns": list(p.identity_fields), "processing_fields": list(p.receipt_fields)}
                         for name,p in subject_policies().items()]}


def build_mcp_metadata(descriptions: dict, schemas: dict[str, list[tuple[str, str]]]) -> dict:
    """Bundle dictionary meaning for MCP without a runtime source-reader dependency.

    The public catalog keeps its existing format for downstream consumers. This
    package resource adds field prose, expected types and declared row identity
    from the same inputs. It describes supported output, not observed publication.
    """
    from spicy_regs.aggregate_checks import AGGREGATES
    from spicy_regs.fec_query_catalog import table_category

    classes = build_catalog(descriptions, schemas)["classes"]
    result = {}
    for entry in classes:
        table = entry["table"]
        description = descriptions[table]
        contract = _contracts().get(table) if table in CONTRACT_TABLES else None
        record_type = RECORD_TYPES.get(table)
        policy = subject_policies().get(table)
        identity = (
            list(policy.identity_fields) if policy is not None else list(contract.identity)
            if contract is not None
            else [record_type.dedup_key]
            if record_type is not None
            else description.get("identity_columns", [])
        )
        result[table] = {
            **entry,
            "subject": description["subject"],
            **({"category": table_category(table)} if table_category(table) else {}),
            "grain": description.get("grain") or (contract.grain if contract is not None else None),
            "identity_columns": identity,
            "columns": [
                {
                    "column_name": name,
                    "column_type": dtype,
                    "description": description["columns"][name],
                }
                for name, dtype in schemas[table]
            ],
        }
        if description.get("legacy_column_descriptions"):
            result[table]["legacy_column_descriptions"] = description["legacy_column_descriptions"]
        if policy is not None and not policy.receipt_only:
            # A generation written under any admitted subject schema holds the columns they all share. Serving
            # (no pyarrow) reads the current columns outside that common set to count such a generation native.
            # Schemas are usually nested (additive columns); a policy that moved columns out (committee_reports
            # legislative-documents/2) is not, and the common set still names what every admitted generation holds.
            from spicy_regs.etl_receipts import receipt_policies

            admitted = [set(p.subject_schema.names) for p in receipt_policies(policy) if not p.receipt_only]
            common = set.intersection(*admitted)
            later = [name for name in policy.subject_schema.names if name not in common]
            if later:
                result[table]["later_policy_columns"] = later
        checks = [{"name": check.name, "role": "output" if table == check.output else "input",
                   "grain": list(check.grain), "population": check.population,
                   "selection_policy": "Matching declared input bytes or one materialized/catalog export snapshot; "
                   "fixed comment URLs require unchanged ETags around the read. Each check selects its own pins.",
                   "command": f"uv run --frozen python scripts/check_table_joins.py --aggregate {check.name}",
                   "status": "available_check_not_a_live_measurement"}
                  for check in AGGREGATES if table in (check.output, *check.inputs)]
        if checks:
            result[table]["aggregate_checks"] = checks
    # The publisher's documented values and labels, parsed from the pinned captures; describe_table shows them
    # only with detail=true, since the LDA and FEC lists alone run to several kilobytes.
    from spicy_regs.sources.source_domains import DEFAULT_SOURCE_DOMAIN_DIR, documented_values

    for table, columns in documented_values(REPO_ROOT / DEFAULT_SOURCE_DOMAIN_DIR).items():
        if table in result:
            result[table]["documented_values"] = columns
    result.update(build_fec_child_metadata(descriptions, schemas))
    # describe_table('etl_receipts') is the receipts page for a reader who asks the server, so it carries the guide.
    result["etl_receipts"] = {
        **receipt_metadata(),
        "guide": receipt_guide("Each column is described under `columns` in this reply."),
    }
    return result


def catalog_bytes(document: dict) -> bytes:
    """Serialize the catalog to its one canonical byte form.

    A vendored contract is pinned by digest, so there must be exactly one byte
    string for a given document. Key order is insertion order, which follows
    ``TABLES``; indent and separators are fixed here rather than at the call
    site so the digest cannot move because someone passed a different flag.
    """
    return json.dumps(document, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"


def cmd_catalog(args: argparse.Namespace) -> int:
    """Write catalog.json plus its sha256 sidecar, refusing (exit 1) when descriptions disagree with the schema."""
    descriptions = load_descriptions(Path(args.descriptions))
    schemas = _schemas_for_source(args.source, args.base)
    errors = check_descriptions(schemas, descriptions)
    if errors:
        print("✗ Refusing to write a catalog that disagrees with the schema.", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1
    out = Path(args.out) if args.out else DEFAULT_CATALOG_PATH
    document = build_catalog(descriptions, schemas)
    payload = catalog_bytes(document)
    out.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    digest_path = Path(args.digest) if args.digest else DEFAULT_CATALOG_DIGEST_PATH
    # Sidecar, not a field inside the document: a digest carried by the thing it
    # certifies proves nothing. A consumer verifies the bytes against this.
    digest_path.write_text(f"{digest}  {out.name}\n", encoding="utf-8")
    print(f"✓ Wrote {len(document['classes'])} class declaration(s) to {out}.")
    print(f"  sha256 {digest}")
    print(f"  digest recorded in {digest_path}")
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    """Render the table pages, refusing (exit 1) when descriptions are out of sync with the schema.

    Writing the default docs dir also refreshes catalog.json (+ .sha256), table_metadata.json,
    table_qualification.json (from the output ledger), table_joins.json (from table_joins) and
    receipt_fields.json (from receipt_field_declarations).
    """
    descriptions = load_descriptions(Path(args.descriptions))
    schemas = _schemas_for_source(args.source, args.base)
    # Fail rather than emit a dictionary that disagrees with itself.
    errors = check_descriptions(schemas, descriptions)
    if errors:
        print("✗ Refusing to generate: descriptions are out of sync with the schema.", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1
    out_dir = Path(args.out_dir) if args.out_dir else DEFAULT_DOCS_TABLES_DIR
    written = generate(descriptions, schemas, out_dir)
    print(f"✓ Wrote {len(written)} table page(s) to {out_dir} (source={args.source}).")
    for path in written:
        print(f"  - {path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path}")
    # The catalog is the same derivation from the same two inputs, and two tests
    # require the committed copy to equal a fresh build. Writing it here means a
    # contributor who edits descriptions.yaml and regenerates cannot leave it
    # stale by not knowing a third command exists.
    if out_dir == DEFAULT_DOCS_TABLES_DIR:
        payload = catalog_bytes(build_catalog(descriptions, schemas))
        DEFAULT_CATALOG_PATH.write_bytes(payload)
        DEFAULT_CATALOG_DIGEST_PATH.write_text(
            f"{hashlib.sha256(payload).hexdigest()}  {DEFAULT_CATALOG_PATH.name}\n", encoding="utf-8"
        )
        print(f"  - {DEFAULT_CATALOG_PATH.relative_to(REPO_ROOT)} (+ .sha256)")
        DEFAULT_MCP_METADATA_PATH.write_bytes(catalog_bytes(build_mcp_metadata(descriptions, schemas)))
        print(f"  - {DEFAULT_MCP_METADATA_PATH.relative_to(REPO_ROOT)}")
        (out_dir / "etl_receipts.md").write_text(render_receipts_page(receipt_metadata()), encoding="utf-8")
        for name, policy in subject_policies().items():
            if policy.receipt_only:
                # A dataset with no table: its page only says so and points at the one page that explains receipts.
                (out_dir / f"{name}.md").write_text(
                    _GENERATED_BANNER + f"\n\n# `{name}`\n\n"
                    f"`{name}` has no table of its own. What the pipeline recorded for it is kept only as receipts: "
                    f"the rows of [ETL receipts](etl_receipts.md) with `dataset = '{name}'`.\n",
                    encoding="utf-8")

        try:
            output_ledger.RECORD.write_bytes(qualification_bytes())
        except (OSError, ValueError) as exc:
            print(f"✗ {output_ledger.LEDGER_NAME} cannot build the qualification record: {exc}", file=sys.stderr)
            return 1
        print(f"  - {output_ledger.RECORD.relative_to(REPO_ROOT)}")
        table_joins.RECORD.write_bytes(joins_bytes())
        print(f"  - {table_joins.RECORD.relative_to(REPO_ROOT)}")
        from spicy_regs import receipt_field_declarations

        receipt_field_declarations.RECORD.write_bytes(receipt_field_declarations.record_bytes())
        print(f"  - {receipt_field_declarations.RECORD.relative_to(REPO_ROOT)}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spicy-regs-dict",
        description="Generate and validate the Spicy Regs data dictionary.",
    )
    parser.add_argument(
        "--descriptions",
        default=str(DEFAULT_DESCRIPTIONS),
        help=f"Path to descriptions.yaml (default: {DEFAULT_DESCRIPTIONS})",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_check = sub.add_parser("check", help="Reconcile descriptions and schema; exit non-zero on drift")
    p_check.add_argument(
        "--source",
        choices=["schema", "r2", "local"],
        default="schema",
        help="schema = in-code (offline, default); r2/local = live parquet via DuckDB DESCRIBE",
    )
    p_check.add_argument(
        "--base",
        default=None,
        help="R2 base URL (source=r2) or local parquet dir (source=local)",
    )
    p_check.set_defaults(func=cmd_check)

    p_gen = sub.add_parser("generate", help="Render docs/tables/*.md from the schema + descriptions")
    p_gen.add_argument(
        "--source",
        choices=["schema", "r2", "local"],
        default="schema",
        help="Where to read column names/types from (default: in-code schema, offline)",
    )
    p_gen.add_argument("--base", default=None, help="R2 base URL or local parquet dir")
    p_gen.add_argument("--out-dir", default=None, help=f"Output dir (default: {DEFAULT_DOCS_TABLES_DIR})")
    p_gen.set_defaults(func=cmd_generate)

    p_cat = sub.add_parser("catalog", help="Write the machine-readable class declaration (catalog.json)")
    p_cat.add_argument("--source", choices=["schema", "r2", "local"], default="schema")
    p_cat.add_argument("--base", default=None, help="R2 base URL or local parquet dir")
    p_cat.add_argument("--out", default=None, help=f"Output path (default: {DEFAULT_CATALOG_PATH})")
    p_cat.add_argument("--digest", default=None, help=f"Digest sidecar (default: {DEFAULT_CATALOG_DIGEST_PATH})")
    p_cat.set_defaults(func=cmd_catalog)
    return parser


def main(argv: list[str] | None = None) -> None:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
